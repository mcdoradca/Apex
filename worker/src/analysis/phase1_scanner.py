import time
import logging
import csv
from io import StringIO
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from ..config import (MIN_PRICE, MAX_PRICE, MIN_VOLUME, MIN_DAY_CHANGE_PERCENT,
                      MIN_VOLUME_RATIO, MAX_VOLATILITY_ATR_PERCENT, MIN_RELATIVE_STRENGTH)
from .utils import update_scan_progress, append_scan_log, safe_float

logger = logging.getLogger(__name__)

def _chunk_list(lst, n):
    """Dzieli listę na mniejsze części."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def run_scan(session: Session, get_current_state, api_client: AlphaVantageClient) -> list[str]:
    """
    WERSJA DIAGNOSTYCZNA:
    Celem tej funkcji jest wyłącznie zalogowanie danych otrzymanych z API
    w celu kalibracji filtrów. Nie wyłania ona żadnych kandydatów.
    """
    logger.info("Running Phase 1 v2.0: Hybrid Momentum Scan (DIAGNOSTIC MODE)...")
    append_scan_log(session, "Faza 1 (DIAG): Zbieranie danych wywiadowczych...")
    
    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        logger.info(f"[DIAG] Found {len(all_tickers)} tickers in database.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}")
        raise Exception("Diagnostic run failed at DB fetch stage.")

    if not all_tickers:
        logger.warning("Ticker list is empty. Halting diagnostic.")
        raise Exception("Diagnostic run halted. Ticker list is empty.")

    logger.info("--- STARTING STAGE 1: BULK DATA GATHERING ---")
    
    total_chunks = (len(all_tickers) + 99) // 100
    processed_chunks = 0

    for chunk in _chunk_list(all_tickers, 100):
        try:
            bulk_data_csv = api_client.get_bulk_quotes(chunk)
            if not bulk_data_csv or "symbol,timestamp" not in bulk_data_csv:
                logger.warning(f"[DIAG] No valid bulk data for chunk starting with {chunk[0]}")
                continue

            reader = csv.DictReader(StringIO(bulk_data_csv))
            
            logger.info(f"--- [DIAG] Analyzing Chunk {processed_chunks + 1}/{total_chunks} (starts with {chunk[0]}) ---")
            
            for i, row in enumerate(reader):
                # Logujemy dane dla kilku pierwszych spółek z każdego bloku, aby nie zalać logów
                if i < 5: 
                    symbol = row.get('symbol')
                    price = row.get('price', 'N/A')
                    volume = row.get('volume', 'N/A')
                    change_percent_raw = row.get('change_percent', 'N/A').replace('%','')
                    
                    logger.info(
                        f"[DIAG] Ticker: {symbol} | "
                        f"Price: {price} | "
                        f"Volume: {volume} | "
                        f"Change %: {change_percent_raw}"
                    )

        except Exception as e:
            logger.error(f"Error processing bulk chunk in DIAG mode for {chunk[0]}: {e}")
        finally:
            processed_chunks += 1
            # Krótka pauza, aby nie przeciążyć API
            time.sleep(1) 

    logger.info("--- DIAGNOSTIC MODE FINISHED ---")
    append_scan_log(session, "Tryb diagnostyczny zakończony. Analiza logów jest wymagana.")
    
    # W trybie diagnostycznym zawsze przerywamy cykl, rzucając wyjątek
    raise Exception("Diagnostic run complete. Halting cycle as intended to allow for log analysis.")

