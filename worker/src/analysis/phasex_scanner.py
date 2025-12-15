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
    standardize_df_columns, get_raw_data_with_cache,
    update_system_control
)

logger = logging.getLogger(__name__)

# === KONFIGURACJA KRYTERIÓW FAZY X (BIOX) ===
# ZAKTUALIZOWANO: Zwiększono zakres cenowy, aby łapać spółki jak RADX (start z ~$4.26)
MIN_PRICE = 0.20   # Obniżono dolny próg
MAX_PRICE = 25.00  # Podniesiono górny próg (wcześniej 4.00) - łapiemy Small/Mid Cap Biotech

# Słowa kluczowe do identyfikacji sektora Biotech
BIOTECH_KEYWORDS = [
    'Biotechnology', 'Pharmaceutical', 'Health Care', 'Life Sciences', 
    'Medical', 'Therapeutics', 'Biosciences', 'Oncology', 'Genomics',
    'Drug', 'Bio', 'Immuno', 'Cell', 'Gene', 'Theranostics' # Dodano Theranostics
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
        # To zapytanie kosztuje limit API, używane tylko gdy cena pasuje a sektor nieznany
        overview = api_client.get_company_overview(ticker)
        
        # Obsługa błędów API / pustych odpowiedzi
        if not overview: 
            return 'N/A', 'N/A'
        
        sector = overview.get('Sector', 'N/A')
        industry = overview.get('Industry', 'N/A')
        
        # Aktualizacja w bazie (aby nie pytać ponownie przy następnym skanie)
        if sector != 'N/A':
            session.execute(
                text("UPDATE companies SET sector=:s, industry=:i, last_updated=NOW() WHERE ticker=:t"),
                {'s': sector, 'i': industry, 't': ticker}
            )
            session.commit()
            
        return sector, industry
    except Exception as e:
        logger.warning(f"Błąd aktualizacji sektora dla {ticker}: {e}")
        return 'N/A', 'N/A'

def run_phasex_scan(session: Session, api_client) -> List[str]:
    """
    Skaner Fazy X: BioX Hunter (Brute Force Mode).
    Przechodzi przez WSZYSTKIE spółki w bazie (alfabetycznie).
    1. Sprawdza cenę (Cache/API).
    2. Jeśli cena OK -> Weryfikuje sektor (DB -> API Fallback).
    3. Jeśli Biotech -> Zapisuje.
    """
    logger.info("Running Phase X: BioX Scanner (Full Market Scan)...")
    append_scan_log(session, f"Faza X (BioX): Start pełnego skanowania rynku. Cel: Biotech ${MIN_PRICE}-${MAX_PRICE}.")

    # 1. Pobieramy WSZYSTKIE tickery, posortowane alfabetycznie (dla porządku w logach)
    # Pobieramy też sektor, żeby wiedzieć czy musimy pytać API
    try:
        query = text("SELECT ticker, sector, industry FROM companies ORDER BY ticker ASC")
        rows = session.execute(query).fetchall()
        # Mapa: ticker -> {'sector': ..., 'industry': ...}
        all_companies = {r[0]: {'s': r[1], 'i': r[2]} for r in rows}
    except Exception as e:
        logger.error(f"Faza X: Krytyczny błąd bazy danych: {e}")
        return []
    
    total_tickers = len(all_companies)
    tickers_list = list(all_companies.keys())
    
    if total_tickers == 0:
        append_scan_log(session, "Faza X BŁĄD: Tabela 'companies' jest pusta! Uruchom Data Initializer.")
        return []

    logger.info(f"Faza X: Załadowano {total_tickers} tickerów do sprawdzenia.")
    
    candidates_buffer = []
    # BATCH_SIZE = 20 # Mniejszy batch, częstszy zapis (usunięto nieużywaną zmienną lokalną, używamy 5 w pętli)
    processed_count = 0
    passed_price = 0
    found_count = 0
    
    # Czyścimy tabelę kandydatów na starcie, żeby mieć czysty obraz
    # UWAGA: To usuwa stare wyniki, więc skan musi przejść całość, żeby odzyskać listę.
    try:
        session.execute(text("DELETE FROM phasex_candidates"))
        session.commit()
    except Exception:
        session.rollback()

    # start_time = time.time() # Nieużywane

    # 2. Główna pętla
    for ticker in tickers_list:
        processed_count += 1
        
        # Aktualizacja postępu w UI co 10 sztuk
        if processed_count % 10 == 0:
             update_scan_progress(session, processed_count, total_tickers)
             # Log w konsoli co 50 sztuk
             if processed_count % 50 == 0:
                 logger.info(f"Faza X: Przetworzono {processed_count}/{total_tickers} (Znaleziono: {found_count})")

        try:
            # === KROK A: CENA (Najpierw, bo to odsiewa 90% rynku) ===
            # Pobieramy dane dzienne (Compact wystarczy do ceny bieżącej)
            # expiry_hours=24 -> jeśli mamy dane z wczoraj, to ok, nie pytamy API
            price_data_raw = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=24, 
                outputsize='compact' 
            )

            # Jeśli brak danych (API limit, błąd sieci, błąd tickera) -> Skip
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue

            # Szybkie wyciągnięcie ostatniej ceny (bez pełnego parsowania pandasa dla szybkości)
            ts = price_data_raw['Time Series (Daily)']
            # Sortujemy daty, bierzemy ostatnią
            dates = sorted(list(ts.keys()))
            if not dates: continue
            
            last_date = dates[-1]
            last_candle = ts[last_date]
            
            # Close price (Adjusted lub raw)
            raw_close = float(last_candle.get('4. close', 0))
            volume = int(last_candle.get('6. volume', 0))

            # FILTR CENOWY (Rozszerzony)
            if not (MIN_PRICE <= raw_close <= MAX_PRICE):
                continue 
            
            passed_price += 1

            # === KROK B: SEKTOR (Tylko jeśli cena pasuje) ===
            sector = all_companies[ticker]['s']
            industry = all_companies[ticker]['i']
            
            # Jeśli w bazie brak danych -> Pytamy API (Overview)
            if not sector or sector == 'N/A' or not industry or industry == 'N/A':
                sector, industry = _update_company_sector(session, api_client, ticker)
                # Mały sleep po callu do API Overview, żeby nie zabić limitu
                time.sleep(1.0) 

            # Czy to Biotech?
            if not _is_biotech(sector, industry):
                continue

            # === KROK C: MAMY KANDYDATA! ===
            
            candidates_buffer.append({
                'ticker': ticker,
                'price': raw_close,
                'volume_avg': volume,
                # Pola historyczne (puste, bo to czysty skaner, agent historii je wypełni/zweryfikuje w innym procesie)
                'pump_count_1y': 0, 
                'last_pump_date': None,
                'last_pump_percent': 0.0
            })
            found_count += 1
            
            # Zapisz natychmiast po znalezieniu (lub małymi paczkami), żebyś widział wynik od razu
            if len(candidates_buffer) >= 5:
                _save_phasex_batch_upsert(session, candidates_buffer)
                candidates_buffer = []

        except Exception as e:
            continue

    # Zapisz resztę z bufora na koniec
    if candidates_buffer:
        _save_phasex_batch_upsert(session, candidates_buffer)

    update_scan_progress(session, total_tickers, total_tickers)
    
    summary = f"🏁 Faza X (BioX): Koniec. Przeanalizowano: {total_tickers}. Pasowało cenowo (${MIN_PRICE}-${MAX_PRICE}): {passed_price}. Wynik Biotech: {found_count}."
    logger.info(summary)
    append_scan_log(session, summary)
    
    final_list_rows = session.execute(text("SELECT ticker FROM phasex_candidates ORDER BY ticker")).fetchall()
    return [r[0] for r in final_list_rows]

def _save_phasex_batch_upsert(session: Session, data: list):
    """Bezpieczny zapis kandydatów (Upsert)."""
    if not data: return
    try:
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
