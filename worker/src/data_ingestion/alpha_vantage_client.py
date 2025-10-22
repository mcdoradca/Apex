import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import text

from .models import Base
from .database import get_db_session, engine

from .analysis import phase1_scanner, phase2_engine, phase3_sniper, ai_agents, utils
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .data_ingestion.data_initializer import initialize_database_if_empty


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not API_KEY:
    logger.critical("ALPHAVANTAGE_API_KEY environment variable not set. Exiting.")
    sys.exit(1)

current_state = "IDLE"
api_client = AlphaVantageClient(api_key=API_KEY)

def handle_ai_analysis_request(session):
    """Sprawdza i wykonuje nową analizę AI na żądanie."""
    ticker_to_analyze = utils.get_system_control_value(session, 'ai_analysis_request')
    if ticker_to_analyze and ticker_to_analyze not in ['NONE', 'PROCESSING']:
        logger.info(f"AI analysis request received for: {ticker_to_analyze}.")
        # 1. Natychmiast ustawiamy flagę na PROCESSING, aby API wiedziało, że zaczęliśmy
        utils.update_system_control(session, 'ai_analysis_request', 'PROCESSING')
        
        # 2. Natychmiast zapisujemy status PROCESSING do tabeli wyników
        processing_result = {"status": "PROCESSING", "message": "Rozpoczynanie analizy przez agentów AI...", "ticker": ticker_to_analyze}
        stmt_processing = text("""
            INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated) 
            VALUES (:ticker, :data, NOW()) 
            ON CONFLICT (ticker) DO UPDATE SET 
                analysis_data = EXCLUDED.analysis_data, 
                last_updated = NOW();
        """)
        try:
            session.execute(stmt_processing, {'ticker': ticker_to_analyze, 'data': json.dumps(processing_result)})
            session.commit()
            logger.info(f"Set initial PROCESSING status for {ticker_to_analyze} analysis.")
        except Exception as e:
            logger.error(f"Error setting initial PROCESSING status for {ticker_to_analyze}: {e}", exc_info=True)
            session.rollback()
            # Ustawiamy flagę z powrotem na NONE, aby umożliwić ponowne zlecenie
            utils.update_system_control(session, 'ai_analysis_request', 'NONE')
            return # Przerywamy, jeśli nie udało się zapisać statusu

        # 3. Teraz dopiero uruchamiamy właściwą analizę w tle (jeśli to możliwe) lub blokująco
        try:
            results = ai_agents.run_ai_analysis(ticker_to_analyze, api_client)
            # Używamy UPDATE zamiast INSERT ON CONFLICT, bo rekord już istnieje
            stmt_done = text("""
                UPDATE ai_analysis_results 
                SET analysis_data = :data, last_updated = NOW() 
                WHERE ticker = :ticker;
            """)
            session.execute(stmt_done, {'ticker': ticker_to_analyze, 'data': json.dumps(results)})
            session.commit()
            logger.info(f"Successfully saved AI analysis for {ticker_to_analyze}.")
        except Exception as e:
            logger.error(f"Error during AI analysis for {ticker_to_analyze}: {e}", exc_info=True)
            error_result = {"status": "ERROR", "message": str(e), "ticker": ticker_to_analyze}
            # Używamy UPDATE zamiast INSERT ON CONFLICT
            stmt_err = text("""
                UPDATE ai_analysis_results 
                SET analysis_data = :data, last_updated = NOW() 
                WHERE ticker = :ticker;
            """)
            session.execute(stmt_err, {'ticker': ticker_to_analyze, 'data': json.dumps(error_result)})
            session.commit()
        finally:
             # 4. Na koniec czyścimy flagę żądania
             utils.update_system_control(session, 'ai_analysis_request', 'NONE')


def run_full_analysis_cycle():
    global current_state
    session = get_db_session()
    try:
        logger.info("Cleaning tables before new analysis cycle...")
        session.execute(text("DELETE FROM phase2_results WHERE analysis_date = CURRENT_DATE;"))
        session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date >= CURRENT_DATE;"))
        session.commit()
        logger.info("Daily tables cleaned. Proceeding with analysis.")
    except Exception as e:
        logger.error(f"Could not clean tables before run: {e}", exc_info=True)
        session.rollback()
 
    if utils.get_system_control_value(session, 'worker_status') == 'RUNNING':
        logger.warning("Analysis cycle already in progress. Skipping scheduled run.")
        session.close()
        return

    try:
        logger.info("Starting full analysis cycle...")
        current_state = "RUNNING"
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.update_system_control(session, 'scan_log', '')
        # UWAGA: Potencjalnie ryzykowna operacja, jeśli sygnały mają być długoterminowe
        # Rozważ zmianę logiki, jeśli sygnały mają przetrwać dłużej niż jeden cykl
        # session.execute(text("UPDATE trading_signals SET status = 'EXPIRED' WHERE status = 'ACTIVE'"))
        # session.commit()
        # logger.info("Old active signals marked as expired.") 
        # Na razie zakomentowane, aby uniknąć niechcianego wygaszania
        
        utils.append_scan_log(session, "Rozpoczynanie nowego cyklu analizy...")
        
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            logger.warning("Phase 1 found no candidates. Halting cycle.") # Zmieniono na warning
            utils.append_scan_log(session, "Faza 1 nie znalazła kandydatów. Zakończono cykl.")
            # Nie rzucamy wyjątku, aby cykl mógł się normalnie zakończyć
        else:
            utils.update_system_control(session, 'current_phase', 'PHASE_2')
            qualified_tickers = phase2_engine.run_analysis(session, candidate_tickers, lambda: current_state, api_client)
            if not qualified_tickers:
                logger.warning("Phase 2 qualified no stocks. Halting cycle.") # Zmieniono na warning
                utils.append_scan_log(session, "Faza 2 nie zakwalifikowała spółek. Zakończono cykl.")
                 # Nie rzucamy wyjątku
            else:
                utils.update_system_control(session, 'current_phase', 'PHASE_3')
                phase3_sniper.run_tactical_planning(session, qualified_tickers, lambda: current_state, api_client)

        utils.append_scan_log(session, "Cykl analizy zakończony.") # Zmieniono komunikat
    except Exception as e:
        logger.error(f"An error occurred during the analysis: {e}", exc_info=True)
        utils.update_system_control(session, 'worker_status', 'ERROR')
        utils.append_scan_log(session, f"BŁĄD KRYTYCZNY podczas cyklu: {e}")
    finally:
        current_state = "IDLE"
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')
        utils.update_system_control(session, 'scan_progress_processed', '0')
        utils.update_system_control(session, 'scan_progress_total', '0')
        session.close() # Zamknięcie sesji w bloku finally

def main_loop():
    global current_state
    logger.info("Worker started. Initializing...")
    
    # Inicjalizacja poza pętlą
    try:
        with get_db_session() as session:
            logger.info("Verifying database tables for Worker...")
            Base.metadata.create_all(bind=engine)
            logger.info("Database tables verified.")
            initialize_database_if_empty(session, api_client)
            # Ustawienie początkowych wartości kontrolnych
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'worker_command', 'NONE')
            utils.update_system_control(session, 'ai_analysis_request', 'NONE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'system_alert', 'NONE')
            utils.report_heartbeat(session)
    except Exception as init_error:
        logger.critical(f"FATAL: Error during worker initialization: {init_error}", exc_info=True)
        # Rozważ wyjście z aplikacji, jeśli inicjalizacja zawiedzie
        # sys.exit(1) 
        pass # Lub kontynuuj, jeśli błąd nie jest krytyczny

    # Konfiguracja harmonogramu
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    # Monitorowanie wejść co minutę
    schedule.every(1).minute.do(lambda: phase3_sniper.monitor_entry_triggers(get_db_session(), api_client))
    
    logger.info(f"Scheduled job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Real-Time Entry Trigger Monitor scheduled every 1 minute.")

    # Główna pętla
    while True:
        session = None # Resetuj sesję na początku każdej iteracji
        try:
            session = get_db_session() # Otwórz nową sesję
            
            command_triggered_run, new_state = utils.check_for_commands(session, current_state)
            current_state = new_state

            if command_triggered_run:
                # Uruchomienie cyklu w odpowiedzi na komendę
                run_full_analysis_cycle() # Ta funkcja zarządza własną sesją
                session.close() # Zamknij sesję pętli, bo cykl użył swojej
                session = None # Oznacz sesję jako zamkniętą
            else:
                # Normalne operacje pętli
                if current_state != "PAUSED":
                    handle_ai_analysis_request(session)
                    schedule.run_pending()
                
                utils.report_heartbeat(session)

        except Exception as loop_error:
            logger.error(f"Error in main worker loop: {loop_error}", exc_info=True)
            if session:
                session.rollback() # Wycofaj zmiany, jeśli wystąpił błąd w tej sesji
        finally:
            if session:
                 session.close() # Zawsze zamykaj sesję na końcu iteracji pętli
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        main_loop()
    else:
        logger.critical("Worker cannot start because database connection was not established.")

