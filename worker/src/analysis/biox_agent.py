import logging
import time
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, select, func
import pandas as pd 
import numpy as np 

# Modele bazy danych
from ..models import PhaseXCandidate

# Importy narzędziowe
from .utils import (
    append_scan_log, 
    get_raw_data_with_cache,
    standardize_df_columns
)

logger = logging.getLogger(__name__)

# ==================================================================
# AGENT BIOX (Faza X) - Cleaned & Optimized (Split-Aware)
# ==================================================================

# ==================================================================
# CZĘŚĆ 1: LIVE MONITOR (ZDEPRECJONOWANY)
# ==================================================================

def run_biox_live_monitor(session: Session, api_client):
    """
    Strażnik BioX (Newsy).
    Delegowany do news_agent.py.
    """
    pass

# ==================================================================
# CZĘŚĆ 2: HISTORICAL AUDIT (PUMP HUNTER)
# ==================================================================

def run_historical_catalyst_scan(session: Session, api_client, candidates: list = None):
    """
    Analiza Wsteczna dla Fazy X (BioX Audit).
    Szuka historycznych 'pomp' cenowych (>20%) w ciągu ostatniego roku.
    POPRAWKA: Obsługa Reverse Splits poprzez użycie Adjusted Close.
    """
    logger.info("BioX Audit: Uruchamianie analizy historycznej pomp...")
    append_scan_log(session, "🧬 BioX Audit: Analiza historii cen w poszukiwaniu pomp >20% (Filtr Splitów)...")

    # 1. Wybór kandydatów
    tickers_to_check = []
    
    if candidates and len(candidates) > 0:
        logger.info(f"BioX Audit: Otrzymano {len(candidates)} kandydatów bezpośrednio ze Skanera.")
        tickers_to_check = candidates
    else:
        try:
            stmt = text("""
                SELECT ticker FROM phasex_candidates 
                WHERE last_pump_date IS NULL 
                OR analysis_date < (NOW() - INTERVAL '24 hours')
                ORDER BY ticker
            """)
            tickers_to_check = [r[0] for r in session.execute(stmt).fetchall()]
        except Exception as e:
            logger.error(f"BioX Audit: Błąd pobierania z bazy: {e}")
            return

    if not tickers_to_check:
        append_scan_log(session, "BioX Audit: Brak kandydatów do sprawdzenia (wszyscy aktualni).")
        return

    logger.info(f"BioX Audit: {len(tickers_to_check)} tickerów w kolejce do analizy technicznej.")
    
    processed = 0
    updated_count = 0     
    pumps_found_count = 0 
    
    for ticker in tickers_to_check:
        try:
            # 2. Dane dzienne
            # Pobieramy DAILY_ADJUSTED, które zawiera zarówno 'close' (raw) jak i 'adjusted close'
            raw_data = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=24, outputsize='full'
            )
            
            if not raw_data or 'Time Series (Daily)' not in raw_data:
                continue
            
            df = standardize_df_columns(pd.DataFrame.from_dict(raw_data['Time Series (Daily)'], orient='index'))
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            
            one_year_ago = datetime.now() - timedelta(days=365)
            df_1y = df[df.index >= one_year_ago].copy()
            
            if df_1y.empty: continue

            # 3. Szukamy pomp (>20%) - LOGIKA "SPLIT-AWARE"
            
            # Konwersja kolumn na liczby (na wypadek stringów)
            cols_to_numeric = ['open', 'high', 'low', 'close', 'adjusted close']
            for col in cols_to_numeric:
                if col in df_1y.columns:
                    df_1y[col] = pd.to_numeric(df_1y[col], errors='coerce')

            # Obliczanie zmian
            
            # A. Pump Intraday: (High - Open) / Open
            # Tutaj używamy RAW (open/high), bo splity rzadko zdarzają się w trakcie sesji, 
            # a adjusted open/high często nie są dostępne wprost.
            df_1y['open'] = df_1y['open'].replace(0, np.nan)
            df_1y['pump_intraday'] = ((df_1y['high'] - df_1y['open']) / df_1y['open']).fillna(0.0)
            
            # B. Pump Session: Używamy ADJUSTED CLOSE!
            # To eliminuje problem Reverse Splits (sztucznych pomp 5000%)
            if 'adjusted close' in df_1y.columns:
                df_1y['prev_adj_close'] = df_1y['adjusted close'].shift(1).replace(0, np.nan)
                df_1y['pump_session'] = ((df_1y['adjusted close'] - df_1y['prev_adj_close']) / df_1y['prev_adj_close']).fillna(0.0)
            else:
                # Fallback jeśli brak adjusted (mało prawdopodobne przy tym endpoincie)
                df_1y['prev_close'] = df_1y['close'].shift(1).replace(0, np.nan)
                df_1y['pump_session'] = ((df_1y['close'] - df_1y['prev_close']) / df_1y['prev_close']).fillna(0.0)
            
            pump_threshold = 0.20
            
            # Wykrywanie pomp
            pumps = df_1y[
                (df_1y['pump_intraday'] >= pump_threshold) | 
                (df_1y['pump_session'] >= pump_threshold)
            ]
            
            pump_count = len(pumps)
            last_pump_date = None
            last_pump_percent = 0.0
            
            if pump_count > 0:
                pumps_found_count += 1
                last_pump_row = pumps.iloc[-1] # Bierzemy ostatnią chronologicznie
                
                if pd.notna(last_pump_row.name):
                    last_pump_date = last_pump_row.name.date()
                
                # Wybieramy większą z dwóch wartości (Intraday vs Session)
                max_pump = max(last_pump_row['pump_intraday'], last_pump_row['pump_session'])
                
                if pd.isna(max_pump) or np.isinf(max_pump):
                    last_pump_percent = 0.0
                else:
                    last_pump_percent = round(float(max_pump) * 100, 2)

            # 4. Aktualizacja w bazie
            update_stmt = text("""
                UPDATE phasex_candidates 
                SET pump_count_1y = :count, 
                    last_pump_date = :date, 
                    last_pump_percent = :percent,
                    analysis_date = NOW()
                WHERE ticker = :ticker
            """)
            
            session.execute(update_stmt, {
                'count': pump_count,
                'date': last_pump_date,
                'percent': last_pump_percent,
                'ticker': ticker
            })
            session.commit()
            updated_count += 1
            
        except Exception as e:
            logger.error(f"BioX Audit Error for {ticker}: {e}")
            session.rollback()
            continue
        
        processed += 1
        if processed % 20 == 0:
            logger.info(f"BioX Audit: Przetworzono {processed}/{len(tickers_to_check)}.")
            time.sleep(0.1)

    summary = f"🏁 BioX Audit: Zakończono (Split-Aware). Przeanalizowano: {updated_count}, Znaleziono Pomp: {pumps_found_count}."
    logger.info(summary)
    append_scan_log(session, summary)
