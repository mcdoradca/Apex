import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..config import Phase1Config
from .utils import (
    append_scan_log, update_scan_progress, safe_float, 
    standardize_df_columns, calculate_atr
)

logger = logging.getLogger(__name__)

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    PRZEBUDOWANA FAZA 1: Skanowanie oparte na danych historycznych (EOD).
    Iteruje po każdym tickerze i używa get_daily_adjusted (zamiast get_bulk_quotes),
    ponieważ filtry RVol i ATR wymagają danych historycznych.
    
    ZAPISUJE WYNIKI POJEDYNCZO (jak Faza 2), aby zapewnić odporność na błędy.
    """
    logger.info("Running Phase 1: EOD Historical Data Scan (Robust Save Logic)...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania EOD (Logika Fazy 2)...")

    # ==================================================================
    # === POPRAWKA (Twoja sugestia): Czyszczenie tabeli na początku ===
    # ==================================================================
    try:
        # Czyścimy tylko kandydatów z bieżącego dnia (jeśli uruchamiamy ponownie)
        # Stare dane są czyszczone przez main.py
        session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date >= CURRENT_DATE"))
        session.commit()
        logger.info("Cleared today's Phase 1 candidates to prevent duplicates.")
    except Exception as e:
        logger.error(f"Failed to clear Phase 1 table: {e}", exc_info=True)
        session.rollback()
        # Kontynuujemy, ale możemy mieć duplikaty
    # ==================================================================

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        total_tickers = len(all_tickers)
        logger.info(f"Found {total_tickers} tickers to process one-by-one using EOD data.")
        append_scan_log(session, f"Znaleziono {total_tickers} spółek w bazie do skanowania EOD.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}", exc_info=True)
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można pobrać listy spółek z bazy: {e}")
        return []

    if not all_tickers:
        logger.warning("Ticker list from database is empty. Phase 1 cannot proceed.")
        append_scan_log(session, "BŁĄD: Lista spółek do skanowania jest pusta.")
        return []

    # Lista tickerów, która zostanie przekazana do Fazy 2
    final_candidate_tickers = []
    
    for processed_count, ticker in enumerate(all_tickers):
        
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        if processed_count % 20 == 0: 
            update_scan_progress(session, processed_count, total_tickers)

        try:
            # 1. Pobierz dane EOD (compact wystarczy na 100 dni)
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue
            
            # 2. Standaryzuj dane
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)

            # 3. Sprawdź, czy mamy wystarczająco danych
            if len(daily_df) < 22:
                continue

            # 4. Wyodrębnij dane
            latest_candle = daily_df.iloc[-1]
            prev_candle = daily_df.iloc[-2]

            current_price = latest_candle['close']
            current_volume = latest_candle['volume']
            prev_close = prev_candle['close']
            
            if current_price is None or current_volume is None or prev_close is None or prev_close == 0:
                continue

            change_percent = ((current_price - prev_close) / prev_close) * 100

            # 5. Zastosuj FILTRY PODSTAWOWE (z config.py)
            if not (Phase1Config.MIN_PRICE <= current_price <= Phase1Config.MAX_PRICE):
                continue
            
            if current_volume < Phase1Config.MIN_VOLUME:
                continue
            if change_percent < Phase1Config.MIN_DAY_CHANGE_PERCENT:
                continue
                
            # 6. Zastosuj FILTRY ZAAWANSOWANE (RVol i ATR)
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            if avg_volume == 0: continue
            
            volume_ratio = current_volume / avg_volume
            if volume_ratio < Phase1Config.MIN_VOLUME_RATIO:
                continue

            atr_series = calculate_atr(daily_df, period=14)
            if atr_series.empty or pd.isna(atr_series.iloc[-1]):
                continue
            
            latest_atr = atr_series.iloc[-1]
            if latest_atr == 0 or current_price == 0:
                continue
                
            atr_percent = (latest_atr / current_price)
            if atr_percent > Phase1Config.MAX_VOLATILITY_ATR_PERCENT:
                continue
            
            # 7. KWALIFIKACJA
            log_msg = f"Kwalifikacja (F1): {ticker} (Cena: {current_price:.2f}, VolRatio: {volume_ratio:.2f}, ATR%: {atr_percent:.2%})"
            append_scan_log(session, log_msg)
            
            # ==================================================================
            # === POPRAWKA (Twoja sugestia): Logika zapisu skopiowana z Fazy 2 ===
            # Zapisujemy kandydata NATYCHMIAST, aby uniknąć błędów wsadowych
            # ==================================================================
            candidate_data = {
                'ticker': ticker, 
                'price': current_price,
                'volume': int(current_volume),
                'change_percent': change_percent,
                'score': 1 
            }
            
            insert_stmt = text("""
                INSERT INTO phase1_candidates (ticker, price, volume, change_percent, score, analysis_date)
                VALUES (:ticker, :price, :volume, :change_percent, :score, NOW())
            """)
            session.execute(insert_stmt, [candidate_data])
            session.commit() # Zapisujemy tego JEDNEGO kandydata
            # ==================================================================
            
            # Dodajemy ticker do listy, którą przekażemy do Fazy 2
            final_candidate_tickers.append(ticker)

        except Exception as e:
            logger.error(f"Error in Phase 1 EOD processing for {ticker}: {e}", exc_info=True)
            session.rollback() # Wycofaj błąd tylko dla tego jednego tickera
    
    # Zakończ postęp
    update_scan_progress(session, total_tickers, total_tickers)

    logger.info(f"Phase 1 (EOD Scan) completed. Found {len(final_candidate_tickers)} final candidates.")
    append_scan_log(session, f"Faza 1 (Skan EOD) zakończona. Znaleziono {len(final_candidate_tickers)} ostatecznych kandydatów.")
    
    # Zwracamy listę tickerów do Fazy 2
    return final_candidate_tickers
