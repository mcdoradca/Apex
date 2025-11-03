import logging
import csv
from io import StringIO
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

def _parse_bulk_quotes_csv(csv_text: str) -> dict:
    """Przetwarza odpowiedź CSV z BULK_QUOTES na słownik danych."""
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
        
        # Poprawka: Odczyt ceny z 'close'
        data_dict[ticker] = {
            'price': safe_float(row.get('close')),
            'volume': safe_float(row.get('volume')),
            'change_percent': safe_float(row.get('change_percent'))
        }
    return data_dict

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    Przeprowadza skanowanie Fazy 1.
    """
    logger.info("Running Phase 1: Advanced Momentum Scan...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania Advanced Momentum...")

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        total_tickers = len(all_tickers)
        logger.info(f"Found {total_tickers} tickers to process.")
        append_scan_log(session, f"Znaleziono {total_tickers} spółek w bazie do skanowania.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}", exc_info=True)
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można pobrać listy spółek z bazy: {e}")
        return []

    if not all_tickers:
        logger.warning("Ticker list from database is empty. Phase 1 cannot proceed.")
        append_scan_log(session, "BŁĄD: Lista spółek do skanowania jest pusta.")
        return []

    # --- ETAP 1: Szybkie skanowanie blokowe ---
    append_scan_log(session, "Etap 1: Szybkie skanowanie blokowe...")
    pre_candidates = []
    chunk_size = 100 
    
    detailed_logs_count = 0
    # Modyfikacja 1: Zwiększona liczba logów
    max_detailed_logs = 50

    for i in range(0, total_tickers, chunk_size):
        chunk = all_tickers[i:i + chunk_size]
        try:
            bulk_data_csv = api_client.get_bulk_quotes(chunk)
            if not bulk_data_csv:
                # Modyfikacja 3: Logowanie pustej odpowiedzi CSV
                logger.warning(f"[DIAGNOSTYKA] Nie otrzymano danych CSV dla chunka zaczynającego się od {chunk[0]}.")
                continue

            parsed_data = _parse_bulk_quotes_csv(bulk_data_csv)

            # Modyfikacja 2: Logowanie pustego wyniku parsowania
            if not parsed_data:
                logger.warning(f"[DIAGNOSTYKA] Parsowanie danych dla chunka {chunk[0]} zwróciło pusty wynik.")
                continue

            for ticker in chunk:
                data = parsed_data.get(ticker)
                if not data:
                    continue

                price = data.get('price')
                volume = data.get('volume')
                change_percent = data.get('change_percent')

                is_ok = True
                reasons = []

                if price is None:
                    is_ok = False
                    reasons.append("Invalid Price (None)")
                elif not (Phase1Config.MIN_PRICE <= price <= Phase1Config.MAX_PRICE):
                    is_ok = False
                    reasons.append(f"Price {price:.2f} out of range")

                if volume is None:
                    is_ok = False
                    reasons.append("Invalid Volume (None)")
                elif volume < Phase1Config.MIN_VOLUME:
                    is_ok = False
                    reasons.append(f"Volume {int(volume)} < {Phase1Config.MIN_VOLUME}")

                if change_percent is None:
                    is_ok = False
                elif change_percent < Phase1Config.MIN_DAY_CHANGE_PERCENT:
                    is_ok = False
                    reasons.append(f"Change {change_percent:.2f}% < {Phase1Config.MIN_DAY_CHANGE_PERCENT}%")

                if is_ok:
                    pre_candidates.append({
                        'ticker': ticker,
                        'price': price,
                        'volume': volume,
                        'change_percent': change_percent
                    })
                elif detailed_logs_count < max_detailed_logs:
                    logger.info(f"[DIAGNOSTYKA] Odrzucono {ticker}: {'; '.join(reasons)}")
                    detailed_logs_count += 1
                    
        except Exception as e:
            logger.error(f"Error processing bulk chunk starting with {chunk[0]}: {e}", exc_info=True)
        
        update_scan_progress(session, min(i + chunk_size, total_tickers), total_tickers)

    logger.info(f"Stage 1 found {len(pre_candidates)} pre-candidates.")
    append_scan_log(session, f"Etap 1 zakończony. Znaleziono {len(pre_candidates)} wstępnych kandydatów.")

    if not pre_candidates:
        append_scan_log(session, "Brak kandydatów po etapie 1. Zakończono Fazę 1.")
        return []

    # --- ETAP 2: Głęboka analiza zaawansowana ---
    append_scan_log(session, "Etap 2: Głęboka analiza zaawansowana (Wolumen Względny i ATR)...")
    
    final_candidates = []
    
    for candidate in pre_candidates:
        ticker = candidate['ticker']
        try:
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue
            
            # KROK 2 ZMIANA: Używamy standaryzatora z utils
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)


            # Weryfikacja Wolumenu Względnego
            if len(daily_df) < 22: continue
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            current_volume = candidate['volume']
            
            if avg_volume == 0: continue
            volume_ratio = current_volume / avg_volume
            volume_ratio_ok = volume_ratio >= Phase1Config.MIN_VOLUME_RATIO

            if not volume_ratio_ok: continue

            # KROK 2 ZMIANA: Weryfikacja ATR% (Lokalne obliczenia)
            # Usuwamy wywołanie API: api_client.get_atr(ticker)
            
            # Obliczamy ATR lokalnie
            atr_series = calculate_atr(daily_df, period=14)
            if atr_series.empty or pd.isna(atr_series.iloc[-1]):
                logger.warning(f"Nie można obliczyć lokalnego ATR dla {ticker}.")
                continue
            
            latest_atr = atr_series.iloc[-1]
            current_price = candidate['price']

            if not current_price or not latest_atr or current_price == 0:
                continue
            
            atr_percent = (latest_atr / current_price)
            atr_ok = atr_percent <= Phase1Config.MAX_VOLATILITY_ATR_PERCENT

            if not atr_ok: continue
            
            final_candidates.append(candidate)
            log_msg = f"Kwalifikacja (F1): {ticker} (VolRatio: {volume_ratio:.2f}, ATR%: {atr_percent:.2%})"
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error in Stage 2 processing for {ticker}: {e}", exc_info=True)


    logger.info(f"Phase 1 (Stage 2) completed. Found {len(final_candidates)} final candidates.")
    append_scan_log(session, f"Faza 1 zakończona. Znaleziono {len(final_candidates)} ostatecznych kandydatów.")
    
    if final_candidates:
        try:
            # Przygotuj dane do wstawienia
            candidates_to_insert = [
                {
                    'ticker': c['ticker'], 
                    'price': c['price'],
                    'volume': c['volume'],
                    'change_percent': c['change_percent'],
                    'score': 1 
                } for c in final_candidates
            ]
            
            # ==================================================================
            #  POPRAWKA INSPEKCYJNA (zgodna z sugestią użytkownika i Fazą 2)
            #  Zmieniono zwykły INSERT na INSERT ... ON CONFLICT DO UPDATE.
            #  To zapobiega błędowi Primary Key, gdy kandydat z wczoraj
            #  pojawia się także dzisiaj, a czyszczenie nie usunęło starych danych.
            # ==================================================================
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
            session.execute(insert_stmt, candidates_to_insert)
            session.commit()
            append_scan_log(session, f"Zapisano {len(final_candidates)} kandydatów Fazy 1 w bazie danych.")
        except Exception as e:
            logger.error(f"Failed to save candidates to database: {e}", exc_info=True)
            session.rollback()
    
    return [c['ticker'] for c in final_candidates]
