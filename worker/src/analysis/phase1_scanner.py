import logging
import time
from datetime import datetime, timezone # Dodano do obsługi dat
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from .. import models # Dodano import modeli do bezpiecznego zapisu
from .utils import (
    append_scan_log, update_scan_progress, safe_float, 
    standardize_df_columns, calculate_atr
)

logger = logging.getLogger(__name__)

# === KONFIGURACJA FILTRÓW FAZY 1 (SMALL CAP / BUDGET) ===
# Parametry dostosowane do mniejszego kapitału (zgodnie z Twoim życzeniem).

MIN_PRICE = 0.70           # Min: $0.70 (Łapiemy groszówki z potencjałem)
MAX_PRICE = 20.00          # Max: $20.00 (Dostępne cenowo dla mniejszego konta)
MIN_AVG_VOLUME = 500000    # Min: 500k akcji (Podstawowa płynność)
MIN_DOLLAR_VOLUME = 700000 # Min: 700k USD obrotu (Wystarczające dla detalu)
MIN_ATR_PERCENT = 0.02     # Min: 2% zmienności (Szukamy ruchu)

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    Faza 1: Skaner EOD (End-Of-Day).
    Wersja naprawiona (ORM Insert) z filtrami dla Small/Mid Caps.
    """
    logger.info("Uruchamianie Fazy 1: EOD Scan (Budget Focus: $0.50 - $20.00)...")
    append_scan_log(session, f"Faza 1: Start (FIX). Kryteria: Cena ${MIN_PRICE}-${MAX_PRICE}, Obrót > ${MIN_DOLLAR_VOLUME/1000:.0f}k, ATR > {MIN_ATR_PERCENT*100}%")

    # 1. Czyszczenie tabeli kandydatów (Bezpieczne usuwanie)
    try:
        session.query(models.Phase1Candidate).delete()
        session.commit()
        logger.info("Wyczyszczono tabelę phase1_candidates.")
    except Exception as e:
        logger.error(f"Błąd czyszczenia bazy: {e}", exc_info=True)
        session.rollback()
        return [] 

    # 2. Pobranie listy tickerów
    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        total_tickers = len(all_tickers)
        logger.info(f"Pobrano {total_tickers} spółek do przeskanowania.")
    except Exception as e:
        logger.error(f"Błąd pobierania tickerów: {e}", exc_info=True)
        return []

    if not all_tickers:
        append_scan_log(session, "BŁĄD: Pusta lista spółek w bazie.")
        return []

    final_candidate_tickers = []
    
    # 3. Pętla Skanowania
    for processed_count, ticker in enumerate(all_tickers):
        
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        if processed_count % 10 == 0: 
            update_scan_progress(session, processed_count, total_tickers)

        try:
            # A. Pobierz dane dzienne
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue
            
            # B. Konwersja do DataFrame
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)

            if len(daily_df) < 21:
                continue

            # C. Wyciągnij kluczowe dane
            latest_candle = daily_df.iloc[-1]
            current_price = float(latest_candle['close'])
            current_volume = float(latest_candle['volume'])
            
            if pd.isna(current_price) or current_price <= 0:
                continue

            # ==========================================================
            # === SITO 1: CENA (BUDGET) ===
            # ==========================================================
            if not (MIN_PRICE <= current_price <= MAX_PRICE):
                continue

            # ==========================================================
            # === SITO 2: PŁYNNOŚĆ ===
            # ==========================================================
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            if pd.isna(avg_volume) or avg_volume < MIN_AVG_VOLUME:
                continue
                
            avg_dollar_volume = avg_volume * current_price
            if avg_dollar_volume < MIN_DOLLAR_VOLUME:
                continue

            # ==========================================================
            # === SITO 3: ZMIENNOŚĆ (ATR) ===
            # ==========================================================
            atr_series = calculate_atr(daily_df, period=14)
            if atr_series.empty or pd.isna(atr_series.iloc[-1]):
                continue
            
            current_atr = atr_series.iloc[-1]
            atr_percent = (current_atr / current_price)
            
            if atr_percent < MIN_ATR_PERCENT:
                continue 
            
            # ==========================================================
            # === SITO 4: PRE-FLIGHT DATA CHECK ===
            # ==========================================================
            try:
                check_data = api_client.get_intraday(ticker, interval='60min', outputsize='compact')
                if not check_data or 'Time Series (60min)' not in check_data:
                    continue
            except Exception:
                continue

            # ==========================================================
            # === KWALIFIKACJA I ZAPIS (ORM FIX) ===
            # ==========================================================
            
            log_msg = (f"KWALIFIKACJA (F1): {ticker} | Cena: ${current_price:.2f} | "
                       f"Obrót: ${avg_dollar_volume/1000:.1f}k | ATR: {atr_percent:.1%}")
            append_scan_log(session, log_msg)
            logger.info(log_msg)
            
            # Używamy obiektu ORM zamiast raw SQL -> TO NAPRAWIA BŁĄD INSERTU
            new_candidate = models.Phase1Candidate(
                ticker=ticker,
                price=current_price,
                volume=int(current_volume),
                change_percent=0.0, # Placeholder
                score=int(avg_dollar_volume / 10000),
                analysis_date=datetime.now(timezone.utc)
            )
            
            session.add(new_candidate)
            session.commit()
            
            final_candidate_tickers.append(ticker)

        except Exception as e:
            logger.error(f"Błąd w Fazie 1 dla {ticker}: {e}")
            session.rollback()
    
    update_scan_progress(session, total_tickers, total_tickers)

    final_msg = f"Faza 1 zakończona. Wynik (Budget): {len(final_candidate_tickers)} kandydatów."
    logger.info(final_msg)
    append_scan_log(session, final_msg)
    
    return final_candidate_tickers
