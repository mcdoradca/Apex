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

# Parametry Fazy X (Hardcoded zgodnie z życzeniem, ale można by je wydzielić)
MIN_PRICE = 0.50
MAX_PRICE = 4.00
PUMP_THRESHOLD_PERCENT = 0.50 # 50% wzrostu

# Słowa kluczowe do identyfikacji sektora Biotech w bazie danych
BIOTECH_KEYWORDS = [
    'Biotechnology', 'Pharmaceutical', 'Health Care', 'Life Sciences', 
    'Medical', 'Therapeutics', 'Biosciences', 'Oncology'
]

def run_phasex_scan(session: Session, api_client) -> List[str]:
    """
    Skaner Fazy X: BioX Hunter.
    1. Filtruje tanie spółki bio (0.5-4$).
    2. Analizuje historię pod kątem "pomp" (>50% w 1 dzień).
    3. Zapisuje kandydatów do 'phasex_candidates'.
    """
    logger.info("Running Phase X: BioX Scanner (Pump Hunter)...")
    append_scan_log(session, "Faza X (BioX): Start. Poszukiwanie historycznych pomp >50% w sektorze Biotech.")

    # 1. Pobierz listę potencjalnych spółek (sektor + cena) z tabeli 'companies'
    # (Cenę sprawdzamy wstępnie, o ile mamy ją w bazie, ale dokładną weryfikację zrobimy na danych historycznych)
    try:
        # Budujemy zapytanie SQL z filtrem tekstowym na sektor/branżę
        sector_filters = " OR ".join([f"industry LIKE '%{k}%' OR sector LIKE '%{k}%'" for k in BIOTECH_KEYWORDS])
        query = text(f"SELECT ticker FROM companies WHERE {sector_filters}")
        
        rows = session.execute(query).fetchall()
        initial_tickers = [r[0] for r in rows]
        
        if not initial_tickers:
            append_scan_log(session, "Faza X: Nie znaleziono spółek pasujących do kryteriów sektora Biotech.")
            return []
            
        logger.info(f"Faza X: Znaleziono {len(initial_tickers)} spółek w sektorze Biotech. Rozpoczynanie analizy cenowej...")
        
    except Exception as e:
        logger.error(f"Faza X: Błąd pobierania listy spółek: {e}", exc_info=True)
        return []

    # Przygotowanie do zapisu (Batch Mode dla bezpieczeństwa bazy)
    candidates_buffer = []
    BATCH_SIZE = 50
    processed_count = 0
    found_count = 0
    
    # Czyścimy tabelę przed nowym skanem
    session.execute(text("DELETE FROM phasex_candidates"))
    session.commit()

    start_time = time.time()

    for ticker in initial_tickers:
        processed_count += 1
        if processed_count % 50 == 0:
             update_scan_progress(session, processed_count, len(initial_tickers))
             time.sleep(0.1) # Throttle

        try:
            # 2. Pobierz pełną historię cenową (Daily Full - żeby znaleźć pompy z roku)
            price_data_raw = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=24, # Dane historyczne nie muszą być super świeże co minutę
                outputsize='full'
            )

            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue

            # Konwersja do DataFrame
            df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            df = standardize_df_columns(df)
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True) # Od najstarszych do najnowszych
            
            # Bierzemy ostatni rok (252 dni sesyjne)
            one_year_ago = datetime.now() - timedelta(days=365)
            df_1y = df[df.index >= one_year_ago].copy()
            
            if df_1y.empty: continue

            # 3. Sprawdź kryterium ceny AKTUALNEJ (0.5 - 4$)
            last_close = df_1y['close'].iloc[-1]
            if not (MIN_PRICE <= last_close <= MAX_PRICE):
                continue # Odrzucamy, bo cena nie pasuje do strategii Penny Stock

            # 4. Detekcja POMP (Data Mining)
            # Definicja pompy: (High - Open) / Open >= 0.50  LUB  (Close - PrevClose) / PrevClose >= 0.50
            # Liczymy to wektorowo w Pandas dla szybkości
            
            df_1y['prev_close'] = df_1y['close'].shift(1)
            df_1y['intraday_change'] = (df_1y['high'] - df_1y['open']) / df_1y['open']
            df_1y['session_change'] = (df_1y['close'] - df_1y['prev_close']) / df_1y['prev_close']
            
            # Maska pomp
            pump_mask = (df_1y['intraday_change'] >= PUMP_THRESHOLD_PERCENT) | (df_1y['session_change'] >= PUMP_THRESHOLD_PERCENT)
            pumps = df_1y[pump_mask]
            
            pump_count = len(pumps)
            
            # Jeśli spółka miała przynajmniej jedną pompę w roku -> jest kandydatem
            if pump_count > 0:
                last_pump = pumps.iloc[-1]
                last_pump_date = last_pump.name.date()
                
                # Wybieramy większą zmianę (intraday vs session) jako "wielkość pompy"
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
                
                # Flush Batch
                if len(candidates_buffer) >= BATCH_SIZE:
                    _save_phasex_batch(session, candidates_buffer)
                    candidates_buffer = []

        except Exception as e:
            # logger.error(f"Faza X: Błąd analizy {ticker}: {e}")
            continue

    # Zapisz resztki
    if candidates_buffer:
        _save_phasex_batch(session, candidates_buffer)

    summary = f"🏁 Faza X (BioX): Zakończono. Przeanalizowano {processed_count} spółek. Znaleziono {found_count} 'Wybuchowych' kandydatów."
    logger.info(summary)
    append_scan_log(session, summary)
    
    # Zwracamy listę tickerów (dla np. następnego kroku - szukania newsów)
    # Pobieramy z bazy, żeby mieć pewność co do zapisanych danych
    final_list = session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()
    return [r[0] for r in final_list]

def _save_phasex_batch(session: Session, data: list):
    try:
        stmt = text("""
            INSERT INTO phasex_candidates (
                ticker, price, volume_avg, pump_count_1y, last_pump_date, last_pump_percent, analysis_date
            ) VALUES (
                :ticker, :price, :volume_avg, :pump_count_1y, :last_pump_date, :last_pump_percent, NOW()
            )
        """)
        session.execute(stmt, data)
        session.commit()
        
        tickers_str = ", ".join([d['ticker'] for d in data])
        append_scan_log(session, f"🧪 BioX: Zapisano paczkę {len(data)}: {tickers_str}")
        time.sleep(0.5) # Safety throttle
    except Exception as e:
        logger.error(f"Faza X: Błąd zapisu batcha: {e}", exc_info=True)
        session.rollback()
        time.sleep(2)
