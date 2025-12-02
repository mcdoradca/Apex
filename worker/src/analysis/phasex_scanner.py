import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List

# Importujemy model Company do aktualizacji sektorów
from ..models import Company

# Importy narzędziowe z wnętrza aplikacji
from .utils import (
    append_scan_log, update_scan_progress, 
    standardize_df_columns, get_raw_data_with_cache 
)

logger = logging.getLogger(__name__)

# === KONFIGURACJA KRYTERIÓW FAZY X (BIOX) ===
MIN_PRICE = 0.50
MAX_PRICE = 4.00 

# Słowa kluczowe do identyfikacji sektora Biotech
BIOTECH_KEYWORDS = [
    'Biotechnology', 'Pharmaceutical', 'Health Care', 'Life Sciences', 
    'Medical', 'Therapeutics', 'Biosciences', 'Oncology', 'Genomics',
    'Drug', 'Bio'
]

def _is_biotech(sector: str, industry: str) -> bool:
    """Sprawdza czy sektor/branża pasuje do Biotech."""
    if not sector or not industry: return False
    combined = (str(sector) + " " + str(industry)).lower()
    for k in BIOTECH_KEYWORDS:
        if k.lower() in combined:
            return True
    return False

def _update_company_sector(session: Session, api_client, ticker: str) -> tuple[str, str]:
    """
    Pobiera dane fundamentalne (Overview) z API i aktualizuje bazę danych.
    Zwraca (sector, industry).
    """
    try:
        overview = api_client.get_company_overview(ticker)
        if not overview: return 'N/A', 'N/A'
        
        sector = overview.get('Sector', 'N/A')
        industry = overview.get('Industry', 'N/A')
        
        # Aktualizacja w bazie
        session.query(Company).filter(Company.ticker == ticker).update({
            Company.sector: sector,
            Company.industry: industry
        })
        session.commit()
        return sector, industry
    except Exception as e:
        logger.warning(f"Błąd aktualizacji sektora dla {ticker}: {e}")
        return 'N/A', 'N/A'

def run_phasex_scan(session: Session, api_client) -> List[str]:
    """
    Skaner Fazy X: BioX Hunter (Pure Logic).
    Wyszukuje spółki biotechnologiczne w przedziale 0.5$-4.0$.
    Bez zbędnej analizy historycznych pomp w tym kroku.
    """
    logger.info("Running Phase X: BioX Scanner (Pure Criteria: Biotech, $0.5-$4.0)...")
    append_scan_log(session, "Faza X (BioX): Start. Kryteria: Biotech, Cena 0.5$-4.0$.")

    # 1. Próba pobrania tickerów z poprawnym sektorem
    sector_filters = " OR ".join([f"industry ILIKE '%{k}%' OR sector ILIKE '%{k}%'" for k in BIOTECH_KEYWORDS])
    query = text(f"SELECT ticker, sector, industry FROM companies WHERE {sector_filters}")
    
    rows = session.execute(query).fetchall()
    initial_candidates = {r[0]: (r[1], r[2]) for r in rows} 
    
    # Tryb Odkrywania (Discovery Mode)
    discovery_mode = False
    if len(initial_candidates) < 50:
        logger.warning("Faza X: Mało spółek Biotech w bazie. Uruchamiam TRYB ODKRYWANIA.")
        append_scan_log(session, "Faza X: Tryb Odkrywania (Skanowanie całego rynku)...")
        
        all_query = text("SELECT ticker, sector, industry FROM companies")
        all_rows = session.execute(all_query).fetchall()
        initial_candidates = {r[0]: (r[1], r[2]) for r in all_rows}
        discovery_mode = True
    
    tickers_to_scan = list(initial_candidates.keys())
    logger.info(f"Faza X: Do przeanalizowania {len(tickers_to_scan)} spółek.")

    candidates_buffer = []
    BATCH_SIZE = 50
    processed_count = 0
    found_count = 0
    
    # Czyścimy tabelę kandydatów
    try:
        session.execute(text("DELETE FROM phasex_candidates"))
        session.commit()
    except Exception:
        session.rollback()

    # 2. Główna pętla analizy
    for ticker in tickers_to_scan:
        processed_count += 1
        if processed_count % 20 == 0:
             update_scan_progress(session, processed_count, len(tickers_to_scan))
        
        try:
            # A. SZYBKI FILTR CENOWY
            price_data_raw = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=24, 
                outputsize='compact' # Wystarczy compact do sprawdzenia bieżącej ceny
            )

            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue

            # Szybkie wyciągnięcie ostatniej ceny bez pełnego parsowania DataFrame (optymalizacja)
            ts = price_data_raw['Time Series (Daily)']
            last_date = sorted(ts.keys())[-1]
            last_close = float(ts[last_date].get('4. close', 0))
            volume = int(ts[last_date].get('6. volume', 0)) # lub '5. volume' w zależności od endpointu

            # KRYTERIUM: Cena 0.50$ - 4.00$
            if not (MIN_PRICE <= last_close <= MAX_PRICE):
                continue 

            # B. WERYFIKACJA SEKTORA
            sec, ind = initial_candidates.get(ticker, ('N/A', 'N/A'))
            
            if discovery_mode and (not sec or sec == 'N/A' or ind == 'N/A'):
                sec, ind = _update_company_sector(session, api_client, ticker)
                time.sleep(1.2) 
            
            if not _is_biotech(sec, ind):
                continue

            # === SUKCES: Mamy Biotech w dobrej cenie! ===
            
            # C. Dodanie do bufora (Bez logiki pomp)
            candidates_buffer.append({
                'ticker': ticker,
                'price': float(last_close),
                'volume_avg': volume, # Używamy bieżącego wolumenu jako proxy
                'pump_count_1y': 0, # Placeholder (usunięto logikę)
                'last_pump_date': None,
                'last_pump_percent': 0.0
            })
            found_count += 1
            
            if len(candidates_buffer) >= BATCH_SIZE:
                _save_phasex_batch_upsert(session, candidates_buffer)
                candidates_buffer = []

        except Exception as e:
            continue

    if candidates_buffer:
        _save_phasex_batch_upsert(session, candidates_buffer)

    summary = f"🏁 Faza X (BioX): Zakończono. Przeskanowano {processed_count}. Zidentyfikowano {found_count} spółek Biotech (0.5$-4.0$)."
    logger.info(summary)
    append_scan_log(session, summary)
    
    final_list_rows = session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()
    return [r[0] for r in final_list_rows]

def _save_phasex_batch_upsert(session: Session, data: list):
    """Bezpieczny zapis kandydatów (Upsert)."""
    if not data: return
    try:
        # Zapisujemy podstawowe dane, reszta kolumn (o pompach) zostaje pusta/domyślna
        stmt = text("""
            INSERT INTO phasex_candidates (
                ticker, price, volume_avg, pump_count_1y, last_pump_date, last_pump_percent, analysis_date
            ) VALUES (
                :ticker, :price, :volume_avg, 0, NULL, 0.0, NOW()
            )
            ON CONFLICT (ticker) DO UPDATE SET
                price = EXCLUDED.price,
                volume_avg = EXCLUDED.volume_avg,
                analysis_date = NOW();
        """)
        session.execute(stmt, data)
        session.commit()
    except Exception as e:
        logger.error(f"Faza X: Błąd zapisu batcha: {e}")
        session.rollback()
