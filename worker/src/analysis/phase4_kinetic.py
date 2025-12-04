import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone

# Importy modeli i narzędzi
from .. import models
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    append_scan_log, 
    update_scan_progress, 
    standardize_df_columns, 
    update_system_control
)
from .aqm_v4_logic import analyze_intraday_kinetics

logger = logging.getLogger(__name__)

# === KONFIGURACJA SKANERA H4 ===
BATCH_SIZE = 10       # Mniejszy batch, bo zapytania intraday są ciężkie (dużo danych)
THROTTLE_DELAY = 1.5  # Opóźnienie między tickerami (sekundy) dla bezpieczeństwa API

def run_phase4_scan(session: Session, api_client: AlphaVantageClient):
    """
    Główna pętla skanera Fazy 4: Kinetic Alpha.
    Skanuje rynek pod kątem "Petard" (akcji z dużą liczbą strzałów intraday).
    """
    start_msg = "🚀 FAZA 4 (H4): Start Skanowania Kinetic Alpha (Pulse Hunter)..."
    logger.info(start_msg)
    append_scan_log(session, start_msg)
    update_system_control(session, 'current_phase', 'PHASE_4_KINETIC')

    try:
        # 1. Pobierz listę tickerów do sprawdzenia
        # Na początek bierzemy kandydatów z Fazy 1 (już przesianych fundamentalnie/cenowo)
        # ORAZ kandydatów z Fazy X (BioX), bo oni też mogą być petardami.
        # To oszczędzi API przed skanowaniem martwych śmieci.
        
        tickers_p1 = [r[0] for r in session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()]
        tickers_px = [r[0] for r in session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()]
        
        # Unikalna lista
        tickers_to_scan = list(set(tickers_p1 + tickers_px))
        
        # Jeśli lista jest pusta, bierzemy top 100 z companies (fallback)
        if not tickers_to_scan:
            append_scan_log(session, "Faza 4: Brak kandydatów z F1/FX. Pobieranie próbki z bazy...")
            tickers_to_scan = [r[0] for r in session.execute(text("SELECT ticker FROM companies LIMIT 100")).fetchall()]

        total_tickers = len(tickers_to_scan)
        logger.info(f"Faza 4: Załadowano {total_tickers} tickerów do analizy kinetycznej.")
        append_scan_log(session, f"Faza 4: Celów do analizy: {total_tickers}")

        # Czyścimy tabelę wyników przed nowym skanem (świeży ranking)
        session.execute(text("DELETE FROM phase4_candidates"))
        session.commit()

        processed_count = 0
        candidates_buffer = []
        
        # 2. Główna pętla skanowania
        for ticker in tickers_to_scan:
            processed_count += 1
            
            # Raportowanie postępu
            if processed_count % 5 == 0:
                update_scan_progress(session, processed_count, total_tickers)
                logger.info(f"Faza 4: Postęp {processed_count}/{total_tickers}")

            try:
                # A. Pobierz dane Intraday (5min, full = 30 dni)
                # To jest kluczowy moment - zapytanie do API
                # Używamy interval='5min' dla precyzji, outputsize='full' dla historii
                raw_data = api_client.get_intraday(ticker, interval='5min', outputsize='full')
                
                if not raw_data or 'Time Series (5min)' not in raw_data:
                    # Brak danych lub błąd API - pomiń
                    continue

                # B. Przetwórz dane do DataFrame
                df = pd.DataFrame.from_dict(raw_data['Time Series (5min)'], orient='index')
                df = standardize_df_columns(df) # Zamienia '1. open' na 'open' i typy na float
                
                # C. Uruchom "Mózg" (Pulse Hunter)
                # Ta funkcja (z Kroku 3) policzy strzały, elasticity itp.
                kinetics = analyze_intraday_kinetics(df)
                
                # D. Filtr Wstępny (Odrzuć "Leniwych Żołnierzy")
                # Jeśli spółka nie miała ANI JEDNEGO strzału w 30 dni, szkoda miejsca w bazie
                if kinetics['total_2pct_shots'] == 0:
                    continue

                # E. Przygotuj rekord do zapisu
                # Pobieramy ostatnią cenę z danych intraday
                last_price = df['close'].iloc[0] if not df.empty else 0.0

                candidates_buffer.append({
                    'ticker': ticker,
                    'price': float(last_price),
                    'kinetic_score': kinetics['kinetic_score'],
                    'elasticity': float(kinetics['elasticity']),
                    'shots_30d': kinetics['total_2pct_shots'], # To pole w bazie nazywa się shots_30d
                    'avg_intraday_volatility': float(kinetics['avg_intraday_volatility']),
                    'max_daily_shots': kinetics['max_daily_shots'],
                    'total_2pct_shots_ytd': kinetics['total_2pct_shots'], # Na razie 30d = YTD (uproszczenie API)
                    'avg_swing_size': float(kinetics['avg_swing_size']),
                    'hard_floor_violations': kinetics['hard_floor_violations'],
                    'last_shot_date': kinetics['last_shot_date']
                })

                # F. Zapisz batch (jeśli bufor pełny)
                if len(candidates_buffer) >= BATCH_SIZE:
                    _save_phase4_batch(session, candidates_buffer)
                    candidates_buffer = []
                
                # G. Pacing (ochrona API)
                time.sleep(THROTTLE_DELAY)

            except Exception as e:
                logger.error(f"Faza 4: Błąd analizy dla {ticker}: {e}")
                continue

        # 3. Zapisz resztę bufora na koniec
        if candidates_buffer:
            _save_phase4_batch(session, candidates_buffer)

        update_scan_progress(session, total_tickers, total_tickers)
        
        # 4. Podsumowanie
        final_count = session.query(models.Phase4Candidate).count()
        success_msg = f"🏁 FAZA 4 ZAKOŃCZONA. Przeanalizowano: {total_tickers}. Znaleziono Petard: {final_count}."
        logger.info(success_msg)
        append_scan_log(session, success_msg)

    except Exception as e:
        logger.error(f"Faza 4: Błąd krytyczny skanera: {e}", exc_info=True)
        append_scan_log(session, f"Faza 4 BŁĄD: {str(e)}")
    finally:
        update_system_control(session, 'worker_status', 'IDLE')
        update_system_control(session, 'current_phase', 'NONE')

def _save_phase4_batch(session: Session, data: list):
    """
    Pomocnicza funkcja do zapisu grupowego (INSERT).
    """
    if not data: return
    try:
        # Używamy modelu SQLAlchemy do zapisu (bulk_insert_mappings jest szybkie)
        session.bulk_insert_mappings(models.Phase4Candidate, data)
        session.commit()
        logger.info(f"Faza 4: Zapisano {len(data)} kandydatów.")
    except Exception as e:
        logger.error(f"Faza 4: Błąd zapisu batcha: {e}")
        session.rollback()
