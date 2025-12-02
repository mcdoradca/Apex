import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Importy narzędziowe
from .utils import (
    append_scan_log, update_scan_progress, safe_float, 
    standardize_df_columns, get_raw_data_with_cache 
)

logger = logging.getLogger(__name__)

# Parametry Fazy X (BioX)
MIN_PRICE = 0.50
MAX_PRICE = 5.00 
PUMP_THRESHOLD_PERCENT = 0.50 # 50% wzrostu w 1 dzień

# Słowa kluczowe do identyfikacji sektora Biotech w bazie danych
BIOTECH_KEYWORDS = [
    'Biotechnology', 'Pharmaceutical', 'Health Care', 'Life Sciences', 
    'Medical', 'Therapeutics', 'Biosciences', 'Oncology', 'Genomics'
]

def run_phasex_scan(session: Session, api_client) -> List[str]:
    """
    Skaner Fazy X: BioX Hunter (V2.0 - Safe Upsert).
    Wyszukuje tanie spółki biotech z historią gwałtownych wzrostów (pomp).
    """
    logger.info("Running Phase X: BioX Scanner (Pump Hunter)...")
    append_scan_log(session, "Faza X (BioX): Start. Poszukiwanie historycznych pomp >50% w sektorze Biotech.")

    # 1. Pobieranie listy spółek z sektora
    try:
        # Budujemy zapytanie SQL z filtrem tekstowym na sektor/branżę
        sector_filters = " OR ".join([f"industry LIKE '%{k}%' OR sector LIKE '%{k}%'" for k in BIOTECH_KEYWORDS])
        query = text(f"SELECT ticker FROM companies WHERE {sector_filters}")
        
        rows = session.execute(query).fetchall()
        initial_tickers = [r[0] for r in rows]
        
        if not initial_tickers:
            append_scan_log(session, "Faza X: Nie znaleziono spółek pasujących do kryteriów sektora Biotech.")
            return []
            
        logger.info(f"Faza X: Znaleziono {len(initial_tickers)} spółek w sektorze Biotech. Rozpoczynanie analizy...")
        
    except Exception as e:
        logger.error(f"Faza X: Błąd pobierania listy spółek: {e}", exc_info=True)
        return []

    candidates_buffer = []
    BATCH_SIZE = 50
    processed_count = 0
    found_count = 0
    
    # Opcjonalne czyszczenie (nie jest krytyczne przy Upsert, ale utrzymuje porządek)
    try:
        session.execute(text("DELETE FROM phasex_candidates"))
        session.commit()
    except Exception:
        session.rollback()

    start_time = time.time()

    # 2. Główna pętla analizy
    for ticker in initial_tickers:
        processed_count += 1
        if processed_count % 50 == 0:
             update_scan_progress(session, processed_count, len(initial_tickers))
             time.sleep(0.1) # Throttle dla ochrony bazy

        try:
            # Pobieramy historię (Full Outputsize, aby objąć rok)
            # Używamy cache 24h, bo historia nie zmienia się tak szybko
            price_data_raw = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=24, 
                outputsize='full'
            )

            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue

            df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            df = standardize_df_columns(df)
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            
            # Analiza ostatniego roku (252 dni sesyjne to ok. rok kalendarzowy)
            one_year_ago = datetime.now() - timedelta(days=365)
            df_1y = df[df.index >= one_year_ago].copy()
            
            if df_1y.empty: continue

            # Filtr Ceny Aktualnej
            last_close = df_1y['close'].iloc[-1]
            if not (MIN_PRICE <= last_close <= MAX_PRICE):
                continue 

            # Detekcja Pomp (>50% wzrostu)
            df_1y['prev_close'] = df_1y['close'].shift(1)
            # Zmiana Intraday (High vs Open)
            df_1y['intraday_change'] = (df_1y['high'] - df_1y['open']) / df_1y['open']
            # Zmiana Sesyjna (Close vs Prev Close)
            df_1y['session_change'] = (df_1y['close'] - df_1y['prev_close']) / df_1y['prev_close']
            
            pump_mask = (df_1y['intraday_change'] >= PUMP_THRESHOLD_PERCENT) | (df_1y['session_change'] >= PUMP_THRESHOLD_PERCENT)
            pumps = df_1y[pump_mask]
            
            pump_count = len(pumps)
            
            # Jeśli znaleziono pompy -> Kandydat
            if pump_count > 0:
                last_pump = pumps.iloc[-1]
                last_pump_date = last_pump.name.date()
                max_pump_pct = max(last_pump['intraday_change'], last_pump['session_change']) * 100
                avg_vol = int(df_1y['volume'].mean())

                candidates_buffer.append({
                    'ticker': ticker,
                    'price': float(last_close),
                    'volume_avg': avg_vol,
                    'pump_count_1y': int(pump_count),
                    'last_pump_date': last_pump_date,
                    'last_pump_percent': float(max_pump_pct)
                })
                found_count += 1
                
                # Zapis paczkami
                if len(candidates_buffer) >= BATCH_SIZE:
                    _save_phasex_batch_upsert(session, candidates_buffer)
                    candidates_buffer = []

        except Exception as e:
            # Ignorujemy błędy pojedynczych tickerów, żeby nie przerywać pętli
            continue

    # Zapisz pozostałych kandydatów
    if candidates_buffer:
        _save_phasex_batch_upsert(session, candidates_buffer)

    summary = f"🏁 Faza X (BioX): Zakończono. Przeanalizowano {processed_count}. Znaleziono {found_count} kandydatów."
    logger.info(summary)
    append_scan_log(session, summary)
    
    # Pobierz finalną listę z bazy
    final_list = session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()
    return [r[0] for r in final_list]

def _save_phasex_batch_upsert(session: Session, data: list):
    """
    Zapisuje dane używając UPSERT (ON CONFLICT DO UPDATE).
    Kluczowe dla stabilności bazy danych - zapobiega błędom duplikatów.
    """
    if not data: return
    
    try:
        # Składnia PostgreSQL dla bezpiecznego zapisu/aktualizacji
        stmt = text("""
            INSERT INTO phasex_candidates (
                ticker, price, volume_avg, pump_count_1y, last_pump_date, last_pump_percent, analysis_date
            ) VALUES (
                :ticker, :price, :volume_avg, :pump_count_1y, :last_pump_date, :last_pump_percent, NOW()
            )
            ON CONFLICT (ticker) DO UPDATE SET
                price = EXCLUDED.price,
                volume_avg = EXCLUDED.volume_avg,
                pump_count_1y = EXCLUDED.pump_count_1y,
                last_pump_date = EXCLUDED.last_pump_date,
                last_pump_percent = EXCLUDED.last_pump_percent,
                analysis_date = NOW();
        """)
        session.execute(stmt, data)
        session.commit()
        
        # Opcjonalne logowanie co paczkę
        # tickers_str = ", ".join([d['ticker'] for d in data])
        # append_scan_log(session, f"🧪 BioX: Zapisano/Zaktualizowano {len(data)}: {tickers_str}")
        
        time.sleep(0.1) # Krótki oddech dla bazy
    except Exception as e:
        logger.error(f"Faza X: Błąd zapisu batcha: {e}", exc_info=True)
        session.rollback()
