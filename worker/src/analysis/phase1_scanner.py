import logging
import time # <-- Dodano import 'time'
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..config import Phase1Config
# KROK 2 ZMIANA: Importujemy nowe funkcje analityczne
from .utils import (
    append_scan_log, update_scan_progress, safe_float, 
    standardize_df_columns, calculate_atr
)

logger = logging.getLogger(__name__)

# USUNIĘTO: funkcja _parse_bulk_quotes_csv, ponieważ nie używamy już get_bulk_quotes w tej fazie.

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    PRZEBUDOWANA FAZA 1: Skanowanie oparte na danych historycznych (EOD).
    Iteruje po każdym tickerze i używa get_daily_adjusted (zamiast get_bulk_quotes),
    aby poprawnie analizować dane EOD o 02:30 CET.
    """
    logger.info("Running Phase 1: EOD Historical Data Scan (REBUILT LOGIC)...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania EOD (nowa logika)...")

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

    # --- Skanowanie EOD (Ticker po tickerze) ---
    final_candidates = []
    
    for processed_count, ticker in enumerate(all_tickers):
        
        # Sprawdzenie pauzy (bez zmian)
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        # Aktualizacja postępu (bez zmian)
        if processed_count % 20 == 0: # Aktualizuj co 20 tickerów, aby nie obciążać bazy
            update_scan_progress(session, processed_count, total_tickers)

        try:
            # 1. Pobierz dane EOD (compact wystarczy na 100 dni)
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                # To jest normalne dla wielu tickerów, nie logujemy jako błąd
                continue
            
            # 2. Standaryzuj dane (z utils)
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)

            # 3. Sprawdź, czy mamy wystarczająco danych do analizy (np. dla RVol 20 dni + ATR 14 dni)
            if len(daily_df) < 22: # Potrzebujemy 21 dni dla RVol + 1 na zmianę ceny
                continue

            # 4. Wyodrębnij dane z ostatniej świecy (ostatniego dnia handlowego)
            latest_candle = daily_df.iloc[-1]
            prev_candle = daily_df.iloc[-2]

            current_price = latest_candle['close']
            current_volume = latest_candle['volume']
            prev_close = prev_candle['close']
            
            if current_price is None or current_volume is None or prev_close is None or prev_close == 0:
                continue

            change_percent = ((current_price - prev_close) / prev_close) * 100

            # 5. Zastosuj FILTRY PODSTAWOWE (cena, wolumen, zmiana%)
            if not (Phase1Config.MIN_PRICE <= current_price <= Phase1Config.MAX_PRICE):
                continue
            if current_volume < Phase1Config.MIN_VOLUME:
                continue
            if change_percent < Phase1Config.MIN_DAY_CHANGE_PERCENT:
                continue
                
            # Jeśli doszliśmy tutaj, ticker przeszedł filtry podstawowe
            # logger.info(f"[F1 Skan EOD] {ticker} przeszedł filtry podstawowe. Rozpoczynanie analizy zaawansowanej...")

            # 6. Zastosuj FILTRY ZAAWANSOWANE (Wolumen Względny i ATR%)
            
            # Weryfikacja Wolumenu Względnego (RVol)
            # Średni wolumen z 20 poprzednich dni
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            
            if avg_volume == 0: continue
            volume_ratio = current_volume / avg_volume
            
            if volume_ratio < Phase1Config.MIN_VOLUME_RATIO:
                continue

            # Weryfikacja ATR% (Zmienność)
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
            # Jeśli ticker przeszedł WSZYSTKIE filtry:
            log_msg = f"Kwalifikacja (F1): {ticker} (Cena: {current_price:.2f}, VolRatio: {volume_ratio:.2f}, ATR%: {atr_percent:.2%})"
            append_scan_log(session, log_msg)
            
            final_candidates.append({
                'ticker': ticker, 
                'price': current_price,
                'volume': int(current_volume),
                'change_percent': change_percent,
                'score': 1 # Wynik jest teraz binarny (przeszedł lub nie)
            })

        except Exception as e:
            logger.error(f"Error in Phase 1 EOD processing for {ticker}: {e}", exc_info=True)
    
    # Zakończ postęp
    update_scan_progress(session, total_tickers, total_tickers)

    logger.info(f"Phase 1 (EOD Scan) completed. Found {len(final_candidates)} final candidates.")
    append_scan_log(session, f"Faza 1 (Skan EOD) zakończona. Znaleziono {len(final_candidates)} ostatecznych kandydatów.")
    
    if final_candidates:
        try:
            # Używamy tej samej logiki zapisu do bazy co wcześniej
            insert_stmt = text("""
                INSERT INTO phase1_candidates (ticker, price, volume, change_percent, score, analysis_date)
                VALUES (:ticker, :price, :volume, :change_percent, :score, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    change_percent = EXCLUDED.change_percent,
                    score = EXCLUDED.score,
                    analysis_date = EXCLUDED.analysis_date;
            """)
            session.execute(insert_stmt, final_candidates)
            session.commit()
            append_scan_log(session, f"Zapisano {len(final_candidates)} kandydatów Fazy 1 w bazie danych.")
        except Exception as e:
            logger.error(f"Failed to save candidates to database: {e}", exc_info=True)
            session.rollback()
    
    return [c['ticker'] for c in final_candidates]
