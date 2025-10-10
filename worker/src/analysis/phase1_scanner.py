import time
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from ..config import MIN_PRICE, MAX_PRICE, MIN_VOLUME, MIN_DAY_CHANGE_PERCENT
from .utils import update_scan_progress, append_scan_log, safe_float

logger = logging.getLogger(__name__)

def run_scan(session: Session, get_current_state, api_client: AlphaVantageClient) -> list[str]:
    """
    Skanuje rynek, iterując po każdej spółce indywidualnie, aby pobrać dane historyczne
    i poprawnie obliczyć zmianę procentową, zgodnie z planem API.
    """
    logger.info("Running Phase 1: Market Impulse Scan (Individual Mode)...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania rynku...")

    try:
        # Pobranie tickerów z bazy danych
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}")
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można pobrać listy spółek z bazy danych: {e}")
        return []
        
    total_companies = len(all_tickers)
    update_scan_progress(session, 0, total_companies)

    candidate_tickers = []
    processed_count = 0

    for ticker in all_tickers:
        if get_current_state() == 'PAUSED':
            logger.info("Phase 1 paused.")
            append_scan_log(session, "Skanowanie wstrzymane przez użytkownika.")
            while get_current_state() == 'PAUSED':
                time.sleep(1)
            logger.info("Phase 1 resumed.")
            append_scan_log(session, "Skanowanie wznowione.")

        try:
            # Użycie poprawnego endpointu: TIME_SERIES_DAILY_ADJUSTED
            daily_data = api_client.get_daily_adjusted(ticker, outputsize='compact')

            if not daily_data:
                logger.warning(f"No data received from API for ticker {ticker}. Skipping.")
                continue

            time_series = daily_data.get('Time Series (Daily)')
            if not time_series or len(time_series) < 2:
                continue

            dates = sorted(time_series.keys(), reverse=True)
            latest_day_data = time_series[dates[0]]
            previous_day_data = time_series[dates[1]]

            price = safe_float(latest_day_data.get('4. close'))
            volume = safe_float(latest_day_data.get('6. volume'))
            prev_close = safe_float(previous_day_data.get('4. close'))

            if not all([price, volume, prev_close]) or prev_close == 0:
                continue
            
            change_percent = ((price - prev_close) / prev_close) * 100

            # Zastosowanie kryteriów filtrowania
            if (MIN_PRICE <= price <= MAX_PRICE and
                volume >= MIN_VOLUME and
                change_percent >= MIN_DAY_CHANGE_PERCENT):
                candidate_tickers.append(ticker)
                log_msg = f"Kwalifikacja: {ticker} (Cena: ${price:.2f}, Zmiana: {change_percent:.2f}%, Wolumen: {int(volume):,})"
                append_scan_log(session, log_msg)
        
        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 1: {e}")
        finally:
            processed_count += 1
            if processed_count % 10 == 0:
                update_scan_progress(session, processed_count, total_companies)
    
    update_scan_progress(session, total_companies, total_companies)
    final_log = f"Faza 1 zakończona. Znaleziono {len(candidate_tickers)} kandydatów."
    logger.info(final_log)
    append_scan_log(session, final_log)
    
    return candidate_tickers

