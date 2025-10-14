import logging
import csv
from io import StringIO
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..config import Phase1Config
from .utils import append_scan_log, update_scan_progress, safe_float

logger = logging.getLogger(__name__)

def _parse_bulk_quotes_csv(csv_text: str) -> dict:
    """Przetwarza odpowiedź CSV z BULK_QUOTES na słownik danych."""
    if not csv_text or "symbol" not in csv_text:
        return {}
    
    # Użycie StringIO, aby traktować tekst jak plik
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    
    data_dict = {}
    for row in reader:
        ticker = row.get('symbol')
        if not ticker:
            continue
        
        data_dict[ticker] = {
            'price': safe_float(row.get('latest_price')),
            'volume': safe_float(row.get('volume')),
            'change_percent': safe_float(row.get('change_percent'))
        }
    return data_dict

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    Przeprowadza skanowanie Fazy 1 w wersji 2.0 (hybrydowej), aby znaleźć
    kandydatów do dalszej analizy.
    """
    logger.info("Running Phase 1 v2.0: Hybrid Momentum Scan...")
    append_scan_log(session, "Faza 1 (v2.0): Rozpoczynanie hybrydowego skanowania momentum...")

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        total_tickers = len(all_tickers)
        logger.info(f"Found {total_tickers} total tickers in database to process.")
        append_scan_log(session, f"Znaleziono {total_tickers} spółek w bazie do przeskanowania.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}", exc_info=True)
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można pobrać listy spółek z bazy danych: {e}")
        return []

    if not all_tickers:
        logger.warning("Ticker list from database is empty. Phase 1 cannot proceed.")
        append_scan_log(session, "BŁĄD: Lista spółek do skanowania jest pusta.")
        return []

    # --- ETAP 1: Szybkie skanowanie blokowe ---
    logger.info("--- STARTING STAGE 1: BULK DATA GATHERING & PRE-FILTERING ---")
    append_scan_log(session, "Etap 1: Szybkie skanowanie blokowe...")
    
    pre_candidates = []
    chunk_size = 100 
    
    for i in range(0, total_tickers, chunk_size):
        chunk = all_tickers[i:i + chunk_size]
        try:
            bulk_data_csv = api_client.get_bulk_quotes(chunk)
            if not bulk_data_csv:
                continue

            parsed_data = _parse_bulk_quotes_csv(bulk_data_csv)

            for ticker, data in parsed_data.items():
                price = data.get('price')
                volume = data.get('volume')
                change_percent = data.get('change_percent')

                if not all([price, volume, change_percent]):
                    continue

                # Wstępne filtrowanie
                price_ok = Phase1Config.MIN_PRICE <= price <= Phase1Config.MAX_PRICE
                volume_ok = volume >= Phase1Config.MIN_VOLUME
                change_ok = change_percent >= Phase1Config.MIN_DAY_CHANGE_PERCENT

                if price_ok and volume_ok and change_ok:
                    pre_candidates.append(ticker)
                    
        except Exception as e:
            logger.error(f"Error processing bulk chunk starting with {chunk[0]}: {e}", exc_info=True)
        
        update_scan_progress(session, min(i + chunk_size, total_tickers), total_tickers)

    logger.info(f"Phase 1 (Stage 1) completed. Found {len(pre_candidates)} pre-candidates.")
    append_scan_log(session, f"Etap 1 zakończony. Znaleziono {len(pre_candidates)} wstępnych kandydatów.")

    if not pre_candidates:
        logger.info("No pre-candidates found after Stage 1. Halting Phase 1.")
        append_scan_log(session, "Brak kandydatów po etapie 1. Zakończono Fazę 1.")
        return []

    # --- ETAP 2: Głęboka analiza zaawansowana ---
    logger.info("--- STARTING STAGE 2: ADVANCED ANALYSIS (ATR & RELATIVE STRENGTH) ---")
    append_scan_log(session, "Etap 2: Głęboka analiza zaawansowana...")
    
    final_candidates = []
    
    # Pobierz dane dla QQQ (benchmark) raz, aby zaoszczędzić zapytania
    try:
        qqq_data = api_client.get_daily_adjusted('QQQ', outputsize='compact')
        if not qqq_data:
            logger.error("Could not fetch QQQ data for relative strength analysis. Halting Stage 2.")
            append_scan_log(session, "BŁĄD: Nie można pobrać danych dla QQQ. Przerwano Etap 2.")
            return []
    except Exception as e:
        logger.error(f"Critical error fetching QQQ data: {e}", exc_info=True)
        return []

    for ticker in pre_candidates:
        try:
            # Weryfikacja ATR%
            atr_data = api_client.get_atr(ticker)
            price_data = api_client.get_daily_adjusted(ticker, outputsize='compact')

            if not atr_data or not price_data or 'Time Series (Daily)' not in price_data:
                continue
            
            latest_date = sorted(price_data['Time Series (Daily)'].keys())[0]
            latest_price = safe_float(price_data['Time Series (Daily)'][latest_date]['4. close'])
            
            latest_atr_date = sorted(atr_data['Technical Analysis: ATR'].keys())[0]
            latest_atr = safe_float(atr_data['Technical Analysis: ATR'][latest_atr_date]['ATR'])

            if not latest_price or not latest_atr or latest_price == 0:
                continue
            
            atr_percent = (latest_atr / latest_price)
            atr_ok = atr_percent <= Phase1Config.MAX_VOLATILITY_ATR_PERCENT

            if not atr_ok:
                continue

            # Weryfikacja siły względnej (jeśli jest wymagana)
            # Na razie ten warunek jest pominięty, zgodnie z planem, aby go dodać później
            # relative_strength_ok = True 
            
            # W przyszłości dodamy tutaj logikę obliczania siły względnej vs QQQ
            
            final_candidates.append(ticker)
            log_msg = f"Kwalifikacja: {ticker} (ATR%: {atr_percent:.2%})"
            logger.info(log_msg)
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error in Stage 2 processing for {ticker}: {e}", exc_info=True)

    logger.info(f"Phase 1 (Stage 2) completed. Found {len(final_candidates)} final candidates.")
    append_scan_log(session, f"Etap 2 zakończony. Znaleziono {len(final_candidates)} ostatecznych kandydatów.")
    
    # Zapisz ostatecznych kandydatów do bazy danych
    if final_candidates:
        try:
            # Najpierw wyczyść starych kandydatów
            session.execute(text("DELETE FROM phase1_candidates"))
            
            # Przygotuj dane do wstawienia
            candidates_to_insert = [{'ticker': ticker, 'score': 1} for ticker in final_candidates]
            
            insert_stmt = text("INSERT INTO phase1_candidates (ticker, score, added_at) VALUES (:ticker, :score, NOW())")
            session.execute(insert_stmt, candidates_to_insert)
            session.commit()
            logger.info(f"Successfully saved {len(final_candidates)} candidates to the database.")
            append_scan_log(session, f"Zapisano {len(final_candidates)} kandydatów w bazie danych.")
        except Exception as e:
            logger.error(f"Failed to save candidates to database: {e}", exc_info=True)
            session.rollback()

    final_log_msg = f"Faza 1 (v2.0) zakończona. Znaleziono {len(final_candidates)} ostatecznych kandydatów."
    logger.info(final_log_msg)
    append_scan_log(session, final_log_msg)
    
    return final_candidates

