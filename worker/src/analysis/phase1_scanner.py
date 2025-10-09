import time
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from ..config import MIN_PRICE, MAX_PRICE, MIN_VOLUME, MIN_DAY_CHANGE_PERCENT
from .utils import update_scan_progress, append_scan_log

logger = logging.getLogger(__name__)

def run_scan(session: Session, get_current_state, api_client: AlphaVantageClient) -> list[str]:
    """Skanuje cały rynek w poszukiwaniu spółek o znaczącej aktywności, używając zapytań blokowych."""
    logger.info("Running Phase 1: Market Impulse Scan (Batch Mode)...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania rynku (tryb blokowy)...")

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}")
        return []
        
    total_companies = len(all_tickers)
    update_scan_progress(session, 0, total_companies)

    candidate_tickers = []
    processed_count = 0
    ticker_chunks = [all_tickers[i:i + 100] for i in range(0, len(all_tickers), 100)]

    for chunk in ticker_chunks:
        if get_current_state() == 'PAUSED':
            logger.info("Phase 1 paused.")
            append_scan_log(session, "Skanowanie wstrzymane przez użytkownika.")
            while get_current_state() == 'PAUSED': time.sleep(1)
            logger.info("Phase 1 resumed.")
            append_scan_log(session, "Skanowanie wznowione.")

        try:
            batch_data = api_client.get_batch_quotes(chunk)
            if not batch_data or 'Stock Quotes' not in batch_data:
                logger.warning(f"No data or invalid format for batch: {chunk}")
                processed_count += len(chunk)
                continue

            quotes = batch_data['Stock Quotes']
            for quote in quotes:
                try:
                    price = float(quote.get('2. price', 0))
                    volume = int(quote.get('3. volume', 0))
                    change_percent_str = quote.get('5. change percent', '0%').strip('%')
                    change_percent = float(change_percent_str)
                    ticker = quote.get('1. symbol')

                    if not ticker: continue
                    
                    if (MIN_PRICE <= price <= MAX_PRICE and
                        volume >= MIN_VOLUME and
                        change_percent >= MIN_DAY_CHANGE_PERCENT):
                        candidate_tickers.append(ticker)
                        log_msg = f"Kwalifikacja: {ticker} (Cena: ${price:.2f}, Zmiana: {change_percent:.2f}%, Wolumen: {volume:,})"
                        append_scan_log(session, log_msg)
                
                except (ValueError, KeyError, TypeError) as e:
                    logger.warning(f"Could not parse quote for '{quote.get('1. symbol', 'N/A')}': {e}. Data: {quote}")
                finally:
                    processed_count += 1
        except Exception as e:
            logger.error(f"Failed to process a batch of tickers in Phase 1: {e}")
            processed_count += len(chunk)
        finally:
            update_scan_progress(session, processed_count, total_companies)
    
    final_log = f"Faza 1 zakończona. Znaleziono {len(candidate_tickers)} kandydatów."
    logger.info(final_log)
    append_scan_log(session, final_log)
    return candidate_tickers

