import logging
import csv
import time
from io import StringIO
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from ..config import Phase1Config
from .utils import append_scan_log, update_scan_progress, safe_float, get_performance

logger = logging.getLogger(__name__)

def _parse_bulk_quotes_csv(csv_text: str) -> dict:
    """Przetwarza odpowiedź CSV z BULK_QUOTES na słownik danych, poprawnie obsługując formaty."""
    # Modyfikacja 4: Logowanie na początku funkcji
    logger.info(f"[DIAGNOSTYKA] Otrzymano CSV do parsowania (pierwsze 200 znaków): {csv_text[:200]}")
    
    if not csv_text or "symbol" not in csv_text:
        logger.warning("[DIAGNOSTYKA] Otrzymane dane CSV są puste lub nie zawierają nagłówka 'symbol'.")
        return {}
    
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    
    data_dict = {}
    for row in reader:
        ticker = row.get('symbol')
        if not ticker:
            continue
        
        change_percent_str = row.get('change_percentage') or row.get('change_percent')
        
        if isinstance(change_percent_str, str):
            change_percent_val = safe_float(change_percent_str.strip().replace('%', ''))
        else:
            change_percent_val = safe_float(change_percent_str)

        data_dict[ticker] = {
            'price': safe_float(row.get('price')),
            'volume': safe_float(row.get('volume')),
            'change_percent': change_percent_val
        }
    return data_dict

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """Przeprowadza skanowanie Fazy 1 zgodnie z dokumentem optymalizacji."""
    logger.info("Running Phase 1: Advanced Momentum Scan...")
    append_scan_log(session, "Faza 1: Rozpoczynanie zaawansowanego skanowania momentum...")

    all_tickers = [row[0] for row in session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()]
    total_tickers = len(all_tickers)
    logger.info(f"Found {total_tickers} tickers to process.")
    append_scan_log(session, f"Znaleziono {total_tickers} spółek w bazie do przeskanowania.")

    if not all_tickers:
        return []

    # --- Etap 1: Szybkie filtrowanie blokowe ---
    append_scan_log(session, "Etap 1: Szybkie filtrowanie (cena, wolumen, zmiana)...")
    pre_candidates_data = {}
    chunk_size = 100 
    
    # Zmienne do logowania diagnostycznego
    detailed_log_count = 0
    # Modyfikacja 1: Zwiększona liczba logowanych odrzuceń
    max_detailed_logs = 50 

    for i in range(0, total_tickers, chunk_size):
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)
            
        chunk = all_tickers[i:i + chunk_size]
        bulk_data_csv = api_client.get_bulk_quotes(chunk)
        
        # Modyfikacja 3: Logowanie, gdy bulk_data_csv jest puste
        if not bulk_data_csv:
            logger.warning(f"[DIAGNOSTYKA] Nie otrzymano danych z API dla chunka zaczynającego się od {chunk[0]}.")
            continue

        parsed_data = _parse_bulk_quotes_csv(bulk_data_csv)
        
        # Modyfikacja 2: Logowanie, gdy chunk nie przetwarza żadnych tickerów
        if not parsed_data:
            logger.warning(f"[DIAGNOSTYKA] Parsowanie danych dla chunka {chunk[0]} zwróciło pusty wynik.")
            continue


        for ticker, data in parsed_data.items():
            price = data.get('price')
            volume = data.get('volume')
            change_percent = data.get('change_percent')

            # --- POCZĄTEK LOGOWANIA DIAGNOSTYCZNEGO ---
            rejection_reasons = []

            if not price or not isinstance(price, (int, float)):
                rejection_reasons.append(f"Invalid Price ({price})")
            elif not (Phase1Config.MIN_PRICE <= price <= Phase1Config.MAX_PRICE):
                rejection_reasons.append(f"Price {price} not in [{Phase1Config.MIN_PRICE}, {Phase1Config.MAX_PRICE}]")

            if not volume or not isinstance(volume, (int, float)):
                rejection_reasons.append(f"Invalid Volume ({volume})")
            elif volume < Phase1Config.MIN_VOLUME:
                rejection_reasons.append(f"Volume {int(volume)} < {Phase1Config.MIN_VOLUME}")

            if change_percent is None or not isinstance(change_percent, (int, float)):
                 rejection_reasons.append(f"Invalid Change ({change_percent})")
            elif change_percent < Phase1Config.MIN_DAY_CHANGE_PERCENT:
                rejection_reasons.append(f"Change {change_percent:.2f}% < {Phase1Config.MIN_DAY_CHANGE_PERCENT}%")
            
            if rejection_reasons:
                if detailed_log_count < max_detailed_logs:
                    logger.info(f"[DIAGNOSTYKA] Odrzucono {ticker}: {'; '.join(rejection_reasons)}")
                    detailed_log_count += 1
                continue
            # --- KONIEC LOGOWANIA DIAGNOSTYCZNEGO ---

            pre_candidates_data[ticker] = data
        
        update_scan_progress(session, min(i + chunk_size, total_tickers), total_tickers)

    logger.info(f"Stage 1 found {len(pre_candidates_data)} pre-candidates.")
    append_scan_log(session, f"Etap 1 zakończony. Znaleziono {len(pre_candidates_data)} wstępnych kandydatów.")

    if not pre_candidates_data:
        return []

    # --- Etap 2: Głęboka analiza zaawansowana ---
    append_scan_log(session, "Etap 2: Głęboka analiza (Wolumen Względny, ATR%, Siła Względna)...")
    final_candidates_data = []
    
    try:
        qqq_data = api_client.get_daily_adjusted('QQQ', outputsize='compact')
        if not qqq_data:
            raise Exception("Could not fetch QQQ data for relative strength analysis.")
    except Exception as e:
        logger.error(f"Critical error fetching QQQ data: {e}", exc_info=True)
        append_scan_log(session, "BŁĄD KRYTYCZNY: Nie można pobrać danych dla QQQ.")
        return []
        
    qqq_perf = get_performance(qqq_data, 5)
    if qqq_perf is None:
        append_scan_log(session, "BŁĄD: Nie można obliczyć 5-dniowej stopy zwrotu dla QQQ.")
        return []

    pre_candidate_tickers = list(pre_candidates_data.keys())
    for idx, ticker in enumerate(pre_candidate_tickers):
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)
        
        try:
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue

            daily_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index').astype(float)
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df = daily_df.sort_index()

            # 1. Wolumen względny
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            current_volume = pre_candidates_data[ticker]['volume']
            if avg_volume == 0: continue
            volume_ratio = current_volume / avg_volume
            if volume_ratio < Phase1Config.MIN_VOLUME_RATIO:
                continue
            
            # 2. ATR%
            atr_data_raw = api_client.get_atr(ticker, time_period=14)
            if not atr_data_raw or 'Technical Analysis: ATR' not in atr_data_raw: continue
            latest_atr = safe_float(list(atr_data_raw['Technical Analysis: ATR'].values())[0]['ATR'])
            current_price = pre_candidates_data[ticker]['price']
            if not latest_atr or current_price == 0: continue
            atr_percent = latest_atr / current_price
            if atr_percent > Phase1Config.MAX_VOLATILITY_ATR_PERCENT:
                continue

            # 3. Siła względna
            ticker_perf = get_performance(price_data_raw, 5)
            if ticker_perf is None or qqq_perf is None: 
                 continue
            if ticker_perf < (qqq_perf * Phase1Config.MIN_RELATIVE_STRENGTH):
                continue
            
            final_candidates_data.append({
                "ticker": ticker,
                "price": current_price,
                "change_percent": pre_candidates_data[ticker]['change_percent'],
                "volume": current_volume,
                "score": int(volume_ratio) 
            })
            append_scan_log(session, f"Kwalifikacja (F1): {ticker} (VolRatio: {volume_ratio:.2f}, ATR%: {atr_percent:.2%}, Perf: {ticker_perf:.2f}%)")

        except Exception as e:
            logger.error(f"Error in Stage 2 processing for {ticker}: {e}", exc_info=True)
        
        update_scan_progress(session, idx + 1, len(pre_candidate_tickers))

    # --- Zapis do bazy danych ---
    if final_candidates_data:
        try:
            with session.begin_nested():
                insert_stmt = text("""
                    INSERT INTO phase1_candidates (ticker, price, change_percent, volume, score, analysis_date)
                    VALUES (:ticker, :price, :change_percent, :volume, :score, :analysis_date)
                    ON CONFLICT (ticker) DO UPDATE SET
                        price = EXCLUDED.price,
                        change_percent = EXCLUDED.change_percent,
                        volume = EXCLUDED.volume,
                        score = EXCLUDED.score,
                        analysis_date = EXCLUDED.analysis_date;
                """)
                
                today = datetime.now(timezone.utc)
                records_to_insert = [dict(item, analysis_date=today) for item in final_candidates_data]

                if records_to_insert:
                    session.execute(insert_stmt, records_to_insert)
            
            session.commit()
            append_scan_log(session, f"Zapisano {len(final_candidates_data)} kandydatów Fazy 1 w bazie danych.")
        except Exception as e:
            logger.error(f"Failed to save Phase 1 candidates: {e}", exc_info=True)
            session.rollback()

    final_log_msg = f"Faza 1 zakończona. Znaleziono {len(final_candidates_data)} ostatecznych kandydatów."
    logger.info(final_log_msg)
    append_scan_log(session, final_log_msg)
    
    return [item['ticker'] for item in final_candidates_data]

