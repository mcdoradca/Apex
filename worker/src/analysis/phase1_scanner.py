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
    Skanuje rynek z ostatecznym, rygorystycznym filtrem jakościowym, aby do Fazy 2
    przekazywać wyłącznie standardowe akcje.
    """
    logger.info("Running Phase 1: Market Impulse Scan (Enhanced Diagnostic Mode)...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania (Tryb Diagnostyczny+)...")

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        logger.info(f"[DIAG] Found {len(all_tickers)} tickers in the 'companies' table to process.")
        append_scan_log(session, f"Znaleziono {len(all_tickers)} spółek w bazie do przeskanowania.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}")
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można pobrać listy spółek z bazy danych: {e}")
        return []
        
    if not all_tickers:
        logger.warning("[DIAG] Ticker list is empty. Phase 1 cannot proceed.")
        append_scan_log(session, "BŁĄD: Lista spółek do skanowania jest pusta. Sprawdź tabelę 'companies'.")
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
            # --- POPRAWKA: Rygorystyczny filtr jakości tickera ---
            # Sprawdzamy ticker na samym początku, aby uniknąć niepotrzebnych zapytań API.
            # Przepuszczamy tylko standardowe tickery (1-5 wielkich liter), co eliminuje
            # warranty, ETFy, prawa do akcji i inne niestandardowe instrumenty.
            is_standard_stock = 1 <= len(ticker) <= 5 and ticker.isalpha() and ticker.isupper()
            if not is_standard_stock:
                logger.info(f"[DIAG] Skipping non-standard ticker: {ticker}")
                processed_count += 1
                if processed_count % 50 == 0: # Rzadsze aktualizowanie postępu przy pomijaniu
                    update_scan_progress(session, processed_count, total_companies)
                continue
            # --- KONIEC POPRAWKI ---

            daily_data = api_client.get_daily_adjusted(ticker, outputsize='compact')

            if not daily_data:
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

            price_ok = MIN_PRICE <= price <= MAX_PRICE
            volume_ok = volume >= MIN_VOLUME
            change_ok = change_percent >= MIN_DAY_CHANGE_PERCENT
            
            if price_ok and volume_ok and change_ok:
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
    final_log = f"Faza 1 zakończona. Znaleziono {len(candidate_tickers)} kandydatów (po rygorystycznym filtrowaniu)."
    logger.info(final_log)
    append_scan_log(session, final_log)
    
    return candidate_tickers
