import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Importy narzędziowe z wnętrza aplikacji
from .utils import (
    append_scan_log, update_scan_progress, safe_float, 
    standardize_df_columns, get_raw_data_with_cache 
)

logger = logging.getLogger(__name__)

# === KONFIGURACJA KRYTERIÓW FAZY X (BIOX) ===
# Zgodnie z Twoim wymaganiem: Biotech + Penny Stocks (0.5$ - 4.0$)
MIN_PRICE = 0.50
MAX_PRICE = 4.00 
PUMP_THRESHOLD_PERCENT = 0.20 # Próg do statystyk historycznych (20% wzrostu intraday)

# Słowa kluczowe do identyfikacji sektora Biotech w bazie danych
BIOTECH_KEYWORDS = [
    'Biotechnology', 'Pharmaceutical', 'Health Care', 'Life Sciences', 
    'Medical', 'Therapeutics', 'Biosciences', 'Oncology', 'Genomics',
    'Drug', 'Bio'
]

def run_phasex_scan(session: Session, api_client) -> List[str]:
    """
    Skaner Fazy X: BioX Hunter (Fixed Logic).
    Wyszukuje spółki biotechnologiczne w przedziale 0.5$-4.0$, tworząc listę obserwacyjną
    dla Agenta Newsowego (sprawdzanie co 5 min) oraz dla Backtestu.
    """
    logger.info("Running Phase X: BioX Scanner (Criteria: Biotech, $0.5-$4.0)...")
    append_scan_log(session, "Faza X (BioX): Start selekcji. Kryteria: Biotech, Cena 0.5$-4.0$.")

    # 1. Pobieranie listy spółek z sektora (szeroki lejek)
    try:
        # Budujemy zapytanie SQL z filtrem tekstowym na sektor/branżę
        # Używamy ILIKE dla ignorowania wielkości liter (PostgreSQL)
        sector_filters = " OR ".join([f"industry ILIKE '%{k}%' OR sector ILIKE '%{k}%'" for k in BIOTECH_KEYWORDS])
        query = text(f"SELECT ticker FROM companies WHERE {sector_filters}")
        
        rows = session.execute(query).fetchall()
        initial_tickers = [r[0] for r in rows]
        
        if not initial_tickers:
            append_scan_log(session, "Faza X: Błąd. Nie znaleziono żadnych spółek pasujących do sektora Biotech w bazie.")
            return []
            
        logger.info(f"Faza X: Znaleziono {len(initial_tickers)} spółek w sektorze Biotech. Filtrowanie cenowe...")
        
    except Exception as e:
        logger.error(f"Faza X: Błąd pobierania listy spółek z bazy: {e}", exc_info=True)
        return []

    candidates_buffer = []
    BATCH_SIZE = 50
    processed_count = 0
    found_count = 0
    
    # Czyścimy tabelę kandydatów przed nowym skanem, aby lista była zawsze świeża
    # (Zostawiamy to, bo to pełny skan EOD/Uruchamiany ręcznie)
    try:
        session.execute(text("DELETE FROM phasex_candidates"))
        session.commit()
    except Exception:
        session.rollback()

    start_time = time.time()

    # 2. Główna pętla analizy (Filtrowanie po cenie + Statystyki historyczne)
    for ticker in initial_tickers:
        processed_count += 1
        if processed_count % 50 == 0:
             update_scan_progress(session, processed_count, len(initial_tickers))
             # Krótki sleep, żeby nie zabić bazy przy szybkim iterowaniu
             time.sleep(0.05) 

        try:
            # Pobieramy historię (Compact wystarczy do sprawdzenia bieżącej ceny, 
            # ale potrzebujemy Full do statystyk pomp z ostatniego roku dla Backtestu)
            price_data_raw = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=24, # Cache 24h jest OK dla skanera bazowego
                outputsize='full'
            )

            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue

            df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            df = standardize_df_columns(df)
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            
            if df.empty: continue

            # === KRYTERIUM 1: CENA (0.50$ - 4.00$) ===
            last_close = df['close'].iloc[-1]
            if not (MIN_PRICE <= last_close <= MAX_PRICE):
                continue 

            # === STATYSTYKI DO BACKTESTU (Historia Pomp) ===
            # Obliczamy to teraz, aby mieć gotowe dane do wyświetlenia w UI i do backtestu
            one_year_ago = datetime.now() - timedelta(days=365)
            df_1y = df[df.index >= one_year_ago].copy()
            
            pump_count = 0
            last_pump_date = None
            max_pump_pct = 0.0
            avg_vol = 0

            if not df_1y.empty:
                df_1y['prev_close'] = df_1y['close'].shift(1)
                # Zmiana Intraday (High vs Open) - siła wybicia w trakcie sesji
                df_1y['intraday_change'] = (df_1y['high'] - df_1y['open']) / df_1y['open']
                # Zmiana Sesyjna (Close vs Prev Close) - siła zamknięcia
                df_1y['session_change'] = (df_1y['close'] - df_1y['prev_close']) / df_1y['prev_close']
                
                # Definicja pompy: wzrost > 20% (zgodnie z Twoim opisem backtestu)
                pump_mask = (df_1y['intraday_change'] >= PUMP_THRESHOLD_PERCENT) | (df_1y['session_change'] >= PUMP_THRESHOLD_PERCENT)
                pumps = df_1y[pump_mask]
                
                pump_count = len(pumps)
                avg_vol = int(df_1y['volume'].mean()) if not df_1y['volume'].empty else 0
                
                if pump_count > 0:
                    last_pump = pumps.iloc[-1]
                    last_pump_date = last_pump.name.date()
                    # Zapisujemy największy ruch danego dnia
                    max_pump_pct = max(last_pump['intraday_change'], last_pump['session_change']) * 100

            # === DODANIE DO LISTY KANDYDATÓW ===
            candidates_buffer.append({
                'ticker': ticker,
                'price': float(last_close),
                'volume_avg': avg_vol,
                'pump_count_1y': int(pump_count),
                'last_pump_date': last_pump_date,
                'last_pump_percent': float(max_pump_pct)
            })
            found_count += 1
            
            # Zapis paczkami (Batch Insert/Upsert)
            if len(candidates_buffer) >= BATCH_SIZE:
                _save_phasex_batch_upsert(session, candidates_buffer)
                candidates_buffer = []

        except Exception as e:
            # Logujemy błąd, ale nie przerywamy pętli dla jednego tickera
            # logger.warning(f"Faza X: Błąd analizy {ticker}: {e}")
            continue

    # Zapisz pozostałych kandydatów z bufora
    if candidates_buffer:
        _save_phasex_batch_upsert(session, candidates_buffer)

    summary = f"🏁 Faza X (BioX): Zakończono. Przeanalizowano {processed_count}. Znaleziono {found_count} spółek Biotech (0.5$-4.0$)."
    logger.info(summary)
    append_scan_log(session, summary)
    
    # Zwracamy listę tickerów (np. dla kolejnych kroków w pipeline, jeśli będą potrzebne)
    # W tym modelu dane są już w bazie 'phasex_candidates', skąd pobierze je News Agent.
    final_list_rows = session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()
    return [r[0] for r in final_list_rows]

def _save_phasex_batch_upsert(session: Session, data: list):
    """
    Zapisuje dane używając UPSERT (ON CONFLICT DO UPDATE).
    Gwarantuje, że lista kandydatów jest zawsze aktualna.
    """
    if not data: return
    
    try:
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
        
    except Exception as e:
        logger.error(f"Faza X: Błąd zapisu batcha do bazy: {e}", exc_info=True)
        session.rollback()
