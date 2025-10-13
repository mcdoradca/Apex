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

def _calculate_relative_strength(ticker_data, qqq_data, days=5):
    try:
        ticker_series = ticker_data['Time Series (Daily)']
        qqq_series = qqq_data['Time Series (Daily)']
        
        ticker_dates = sorted(ticker_series.keys(), reverse=True)
        qqq_dates = sorted(qqq_series.keys(), reverse=True)

        if len(ticker_dates) < days or len(qqq_dates) < days:
            return None

        ticker_end_price = safe_float(ticker_series[ticker_dates[0]]['4. close'])
        ticker_start_price = safe_float(ticker_series[ticker_dates[days-1]]['4. close'])
        
        qqq_end_price = safe_float(qqq_series[qqq_dates[0]]['4. close'])
        qqq_start_price = safe_float(qqq_series[qqq_dates[days-1]]['4. close'])

        if not all([ticker_start_price, ticker_end_price, qqq_start_price, qqq_end_price]) or ticker_start_price == 0 or qqq_start_price == 0:
            return None

        ticker_perf = (ticker_end_price - ticker_start_price) / ticker_start_price
        qqq_perf = (qqq_end_price - qqq_start_price) / qqq_start_price
        
        return ticker_perf / qqq_perf if qqq_perf != 0 else float('inf')
    except Exception as e:
        logger.error(f"Error calculating relative strength for {ticker_data.get('Meta Data', {}).get('2. Symbol', 'N/A')}: {e}")
        return None

def run_scan(session: Session, get_current_state, api_client: AlphaVantageClient) -> list[str]:
    logger.info("Running Phase 1 v2.0: Hybrid Momentum Scan...")
    append_scan_log(session, "Faza 1 (v2.0): Rozpoczynanie hybrydowego skanowania momentum...")
    
    session.execute(text("DELETE FROM phase1_candidates"))
    session.commit()

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        logger.info(f"Found {len(all_tickers)} total tickers in database to process.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}")
        return []

    if not all_tickers:
        logger.warning("Ticker list is empty. Phase 1 cannot proceed.")
        return []

    total_companies = len(all_tickers)
    update_scan_progress(session, 0, total_companies)

    # --- ETAP 1: SZYBKIE SKANOWANIE BLOKOWE ---
    pre_candidates = []
    processed_count = 0
    
    append_scan_log(session, "Etap 1: Szybkie skanowanie blokowe...")
    for chunk in _chunk_list(all_tickers, 100):
        if get_current_state() == 'PAUSED':
             while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            bulk_data_csv = api_client.get_bulk_quotes(chunk)
            if not bulk_data_csv or "timestamp" not in bulk_data_csv:
                logger.warning(f"No valid bulk data for chunk starting with {chunk[0]}")
                continue

            reader = csv.DictReader(StringIO(bulk_data_csv))
            for row in reader:
                price = safe_float(row.get('price'))
                volume = safe_float(row.get('volume'))
                change_percent = safe_float(row.get('change_percent', '0').rstrip('%'))

                if not all(v is not None for v in [price, volume, change_percent]):
                    continue
                
                if (MIN_PRICE <= price <= MAX_PRICE and 
                    volume >= MIN_VOLUME and 
                    change_percent >= MIN_DAY_CHANGE_PERCENT):
                    pre_candidates.append(row['symbol'])

        except Exception as e:
            logger.error(f"Error processing bulk chunk starting with {chunk[0]}: {e}")
        finally:
            processed_count += len(chunk)
            update_scan_progress(session, processed_count, total_companies)

    append_scan_log(session, f"Etap 1 zakończony. Znaleziono {len(pre_candidates)} wstępnych kandydatów.")
    logger.info(f"Phase 1 (Stage 1) completed. Found {len(pre_candidates)} pre-candidates.")

    # --- ETAP 2: GŁĘBOKA ANALIZA ZAAWANSOWANA ---
    final_candidates_data = []
    total_pre_candidates = len(pre_candidates)
    
    append_scan_log(session, "Etap 2: Głęboka analiza zaawansowana...")
    qqq_data = api_client.get_daily_adjusted('QQQ', outputsize='compact')
    
    for i, ticker in enumerate(pre_candidates):
        update_scan_progress(session, i + 1, total_pre_candidates) # Postęp dla etapu 2
        if get_current_state() == 'PAUSED':
             while get_current_state() == 'PAUSED': time.sleep(1)
        
        try:
            daily_data = api_client.get_daily_adjusted(ticker, outputsize='compact')
            atr_data = api_client.get_atr(ticker)
            
            if not daily_data or not atr_data or not qqq_data:
                append_scan_log(session, f"Pominięto {ticker}: Brak kompletnych danych zaawansowanych.")
                continue

            series = daily_data.get('Time Series (Daily)')
            if not series or len(series) < 21: continue
            
            dates = sorted(series.keys(), reverse=True)
            latest_volume = safe_float(series[dates[0]].get('6. volume'))
            avg_volume = sum(safe_float(series[d].get('6. volume', 0)) for d in dates[1:21]) / 20
            
            if not latest_volume or avg_volume is None or avg_volume == 0: continue
            volume_ratio = latest_volume / avg_volume
            if volume_ratio < MIN_VOLUME_RATIO:
                continue

            latest_price = safe_float(series[dates[0]].get('4. close'))
            atr_series = atr_data.get('Technical Analysis: ATR')
            if not latest_price or not atr_series: continue
            latest_atr = safe_float(list(atr_series.values())[0]['ATR'])

            if not latest_atr or latest_price == 0: continue
            atr_percent = latest_atr / latest_price
            if atr_percent > MAX_VOLATILITY_ATR_PERCENT:
                continue

            relative_strength = _calculate_relative_strength(daily_data, qqq_data)
            if relative_strength is None or relative_strength < MIN_RELATIVE_STRENGTH:
                continue

            score = 10 # Tymczasowy score
            final_candidates_data.append({'ticker': ticker, 'score': score})
            append_scan_log(session, f"Kwalifikacja (Etap 2): {ticker} (VolRatio: {volume_ratio:.2f}, ATR%: {atr_percent:.2%}, RS: {relative_strength:.2f})")

        except Exception as e:
            logger.error(f"Error in deep analysis for {ticker}: {e}")

    logger.info(f"Phase 1 (Stage 2) completed. Found {len(final_candidates_data)} final candidates.")
    
    if final_candidates_data:
        stmt = text("""
            INSERT INTO phase1_candidates (ticker, score, analysis_date)
            VALUES (:ticker, :score, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
            score = EXCLUDED.score, analysis_date = NOW();
        """)
        session.execute(stmt, final_candidates_data)
        session.commit()

    final_log = f"Faza 1 (v2.0) zakończona. Znaleziono {len(final_candidates_data)} ostatecznych kandydatów."
    logger.info(final_log)
    append_scan_log(session, final_log)
    
    return [c['ticker'] for c in final_candidates_data]

