import re
import time
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text, delete
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from ..config import MIN_PRICE, MAX_PRICE, MIN_VOLUME, MIN_DAY_CHANGE_PERCENT
from .utils import update_scan_progress, append_scan_log, safe_float
# --- POPRAWKA ARCHITEKTONICZNA ---
# Usunięto błędny import `from ..models import Phase1Candidate`.
# Worker nie musi znać definicji modelu, aby zapisywać dane.
# Będziemy używać "surowego" polecenia SQL (text), co jest prawidłowym podejściem.
# --- KONIEC POPRAWKI ---

logger = logging.getLogger(__name__)

def run_scan(session: Session, get_current_state, api_client: AlphaVantageClient) -> list[str]:
    logger.info("Running Phase 1: Market Impulse Scan...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania...")

    # Czyszczenie starych kandydatów przed nowym skanowaniem
    try:
        # Używamy `text` do wykonania polecenia SQL na podstawie nazwy tabeli
        session.execute(text("DELETE FROM phase1_candidates"))
        session.commit()
        append_scan_log(session, "Wyczyszczono listę kandydatów z poprzedniego cyklu.")
    except Exception as e:
        logger.error(f"Could not clear old phase 1 candidates: {e}")
        session.rollback()

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        logger.info(f"Found {len(all_tickers)} tickers in the 'companies' table to process.")
        append_scan_log(session, f"Znaleziono {len(all_tickers)} spółek w bazie do przeskanowania.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}")
        return []
        
    if not all_tickers:
        logger.warning("Ticker list is empty. Phase 1 cannot proceed.")
        return []

    total_companies = len(all_tickers)
    update_scan_progress(session, 0, total_companies)

    candidate_tickers = []
    candidates_to_insert = [] # Lista do zapisu partiami
    processed_count = 0
    
    ticker_format_regex = re.compile(r'^[A-Z]{1,5}$')

    for ticker in all_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        if not ticker_format_regex.match(ticker):
            processed_count += 1
            continue

        try:
            daily_data = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not daily_data: continue

            time_series = daily_data.get('Time Series (Daily)')
            if not time_series or len(time_series) < 2: continue

            dates = sorted(time_series.keys(), reverse=True)
            price = safe_float(time_series[dates[0]].get('4. close'))
            volume = safe_float(time_series[dates[0]].get('6. volume'))
            prev_close = safe_float(time_series[dates[1]].get('4. close'))

            if not all([price, volume, prev_close]) or prev_close == 0: continue
            
            change_percent = ((price - prev_close) / prev_close) * 100

            if (MIN_PRICE <= price <= MAX_PRICE and 
                volume >= MIN_VOLUME and 
                change_percent >= MIN_DAY_CHANGE_PERCENT):
                
                candidate_tickers.append(ticker)
                candidates_to_insert.append({
                    "ticker": ticker,
                    "price": price,
                    "change_percent": change_percent,
                    "volume": int(volume),
                    "score": int(change_percent) 
                })
                log_msg = f"Kwalifikacja: {ticker} (Cena: ${price:.2f}, Zmiana: {change_percent:.2f}%)"
                append_scan_log(session, log_msg)
        
        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 1: {e}")
        finally:
            processed_count += 1
            if processed_count % 50 == 0:
                update_scan_progress(session, processed_count, total_companies)
    
    # --- ZAPIS DO BAZY DANYCH ZA POMOCĄ SUROWEGO SQL ---
    if candidates_to_insert:
        try:
            # Używamy `ON CONFLICT DO UPDATE` (UPSERT)
            stmt = text("""
                INSERT INTO phase1_candidates (ticker, price, change_percent, volume, score, analysis_date)
                VALUES (:ticker, :price, :change_percent, :volume, :score, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    price = EXCLUDED.price,
                    change_percent = EXCLUDED.change_percent,
                    volume = EXCLUDED.volume,
                    score = EXCLUDED.score,
                    analysis_date = NOW();
            """)
            session.execute(stmt, candidates_to_insert)
            session.commit()
            logger.info(f"Successfully saved {len(candidates_to_insert)} candidates to the database.")
        except Exception as e:
            logger.error(f"Could not save phase 1 candidates to database: {e}")
            session.rollback()

    update_scan_progress(session, total_companies, total_companies)
    final_log = f"Faza 1 zakończona. Znaleziono {len(candidate_tickers)} kandydatów."
    logger.info(final_log)
    append_scan_log(session, final_log)
    
    return candidate_tickers

