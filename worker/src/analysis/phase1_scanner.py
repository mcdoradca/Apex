import logging
import csv
from io import StringIO
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date
from ..config import Phase1Config
from .utils import append_scan_log, update_scan_progress, safe_float, get_performance

logger = logging.getLogger(__name__)

def _parse_bulk_quotes_csv(csv_text: str) -> dict:
    if not csv_text or "symbol" not in csv_text: return {}
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    data_dict = {}
    for row in reader:
        ticker = row.get('symbol')
        if not ticker: continue
        data_dict[ticker] = {
            'price': safe_float(row.get('latest_price')),
            'volume': int(safe_float(row.get('volume')) or 0),
            'change_percent': safe_float(row.get('change_percent'))
        }
    return data_dict

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    logger.info("Running Phase 1: Advanced Momentum Scan...")
    append_scan_log(session, "Faza 1: Rozpoczynanie zaawansowanego skanowania momentum...")

    try:
        all_tickers = [row[0] for row in session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()]
        total_tickers = len(all_tickers)
        logger.info(f"Found {total_tickers} tickers to process.")
        append_scan_log(session, f"Znaleziono {total_tickers} spółek do skanowania.")
    except Exception as e:
        logger.error(f"Could not fetch companies: {e}", exc_info=True)
        return []

    # --- Etap 1: Szybkie skanowanie blokowe ---
    append_scan_log(session, "Etap 1: Skanowanie blokowe (cena, wolumen, zmiana)...")
    pre_candidates_data = {}
    chunk_size = 100 
    
    for i in range(0, total_tickers, chunk_size):
        chunk = all_tickers[i:i + chunk_size]
        try:
            bulk_data_csv = api_client.get_bulk_quotes(chunk)
            if not bulk_data_csv: continue
            parsed_data = _parse_bulk_quotes_csv(bulk_data_csv)

            for ticker, data in parsed_data.items():
                if not all(data.values()): continue
                if (Phase1Config.MIN_PRICE <= data['price'] <= Phase1Config.MAX_PRICE and
                    data['volume'] >= Phase1Config.MIN_VOLUME and
                    data['change_percent'] >= Phase1Config.MIN_DAY_CHANGE_PERCENT):
                    pre_candidates_data[ticker] = data
        except Exception as e:
            logger.error(f"Error processing bulk chunk starting with {chunk[0]}: {e}")
        update_scan_progress(session, min(i + chunk_size, total_tickers), total_tickers)

    logger.info(f"Stage 1 found {len(pre_candidates_data)} pre-candidates.")
    append_scan_log(session, f"Etap 1 zakończony. Znaleziono {len(pre_candidates_data)} wstępnych kandydatów.")
    if not pre_candidates_data: return []

    # --- Etap 2: Głęboka analiza (ATR, Siła Względna) ---
    append_scan_log(session, "Etap 2: Głęboka analiza (ATR, Siła Względna)...")
    final_candidates_to_insert = []
    
    try:
        qqq_data = api_client.get_daily_adjusted('QQQ', outputsize='compact')
        qqq_perf = get_performance(qqq_data, 5)
        if qqq_perf is None: raise Exception("Could not calculate QQQ performance.")
    except Exception as e:
        logger.error(f"Critical error fetching QQQ data: {e}", exc_info=True)
        return []

    processed_deep = 0
    total_deep = len(pre_candidates_data)
    update_scan_progress(session, 0, total_deep)

    for ticker, base_data in pre_candidates_data.items():
        try:
            # 1. Weryfikacja ATR%
            atr_data = api_client.get_atr(ticker)
            price_data = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not atr_data or not price_data: continue

            latest_atr = safe_float(list(atr_data['Technical Analysis: ATR'].values())[0]['ATR'])
            if not latest_atr or base_data['price'] == 0: continue
            
            atr_percent = (latest_atr / base_data['price'])
            if atr_percent > Phase1Config.MAX_VOLATILITY_ATR_PERCENT: continue
            
            # 2. Weryfikacja Siły Względnej
            ticker_perf = get_performance(price_data, 5)
            if ticker_perf is None or ticker_perf < (qqq_perf * Phase1Config.MIN_RELATIVE_STRENGTH): continue

            # Jeśli przeszedł, dodaj do listy finalnej
            final_candidates_to_insert.append({
                "ticker": ticker,
                "price": base_data['price'],
                "change_percent": base_data['change_percent'],
                "volume": base_data['volume'],
                "score": 1, # Placeholder
                "analysis_date": date.today()
            })
            append_scan_log(session, f"Kwalifikacja: {ticker} (ATR%: {atr_percent:.2%}, Perf: {ticker_perf:.2f}%)")
        except Exception as e:
            logger.warning(f"Error in deep analysis for {ticker}: {e}")
        finally:
            processed_deep += 1
            update_scan_progress(session, processed_deep, total_deep)

    if final_candidates_to_insert:
        try:
            session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date = :today"), {'today': date.today()})
            stmt = text("""
                INSERT INTO phase1_candidates (ticker, price, change_percent, volume, score, analysis_date)
                VALUES (:ticker, :price, :change_percent, :volume, :score, :analysis_date)
            """)
            session.execute(stmt, final_candidates_to_insert)
            session.commit()
            append_scan_log(session, f"Zapisano {len(final_candidates_to_insert)} kandydatów Fazy 1 w bazie danych.")
        except Exception as e:
            logger.error(f"Failed to save P1 candidates: {e}", exc_info=True)
            session.rollback()

    final_tickers = [c['ticker'] for c in final_candidates_to_insert]
    append_scan_log(session, f"Faza 1 zakończona. Znaleziono {len(final_tickers)} ostatecznych kandydatów.")
    return final_tickers
