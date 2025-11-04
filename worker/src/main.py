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

# KROK 1: Importujemy nasz nowy monitor
from .analysis import (
    phase1_scanner, 
    phase2_engine, 
    phase3_sniper, 
    ai_agents, 
    utils,
    catalyst_monitor # <-- NOWY IMPORT
)
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
        utils.update_system_control(session, 'ai_analysis_request', 'PROCESSING')
        
        temp_result = {"status": "PROCESSING", "message": "Rozpoczynanie analizy przez agentów AI..."}
        stmt_temp = text("INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated) VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();")
        session.execute(stmt_temp, {'ticker': ticker_to_analyze, 'data': json.dumps(temp_result)})
        session.commit()

        try:
            # ==================================================================
            #  OSTATECZNA POPRAWKA: Usunięto "_optimized" z nazwy funkcji
            # ==================================================================
            results = ai_agents.run_ai_analysis(ticker_to_analyze, api_client)
            # ==================================================================
            
            stmt = text("INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated) VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();")
            session.execute(stmt, {'ticker': ticker_to_analyze, 'data': json.dumps(results)})
            session.commit()
            logger.info(f"Successfully saved AI analysis for {ticker_to_analyze}.")
        except Exception as e:
            logger.error(f"Error during AI analysis for {ticker_to_analyze}: {e}", exc_info=True)
            error_result = {"status": "ERROR", "message": str(e), "ticker": ticker_to_analyze}
            stmt_err = text("INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated) VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();")
            session.execute(stmt_err, {'ticker': ticker_to_analyze, 'data': json.dumps(error_result)})
            session.commit()
        finally:
             utils.update_system_control(session, 'ai_analysis_request', 'NONE')


def run_full_analysis_cycle():
    global current_state
    session = get_db_session()
    try:
        logger.info("Cleaning tables before new analysis cycle...")
        session.execute(text("DELETE FROM phase2_results WHERE analysis_date = CURRENT_DATE;"))
        session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date >= CURRENT_DATE;"))
        # Czyścimy stare wiadomości, aby umożliwić ponowną analizę
        session.execute(text("DELETE FROM processed_news WHERE processed_at < NOW() - INTERVAL '3 days';"))
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
        session.execute(text("UPDATE trading_signals SET status = 'EXPIRED' WHERE status = 'ACTIVE'"))
        session.commit()
        logger.info("Old active signals marked as expired.")
        
        utils.append_scan_log(session, "Rozpoczynanie nowego cyklu analizy...")
        
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            raise Exception("Phase 1 found no candidates. Halting cycle.")

        utils.update_system_control(session, 'current_phase', 'PHASE_2')
        # Zmieniona nazwa zmiennej, aby odzwierciedlić pełne dane
        qualified_data = phase2_engine.run_analysis(session, candidate_tickers, lambda: current_state, api_client)
        if not qualified_data:
            raise Exception("Phase 2 qualified no stocks. Halting cycle.")

        utils.update_system_control(session, 'current_phase', 'PHASE_3')
        # Przekazujemy pełne dane (ticker, df) do Fazy 3
        phase3_sniper.run_tactical_planning(session, qualified_data, lambda: current_state, api_client)

        utils.append_scan_log(session, "Cykl analizy zakończony pomyślnie.")
    except Exception as e:
        logger.error(f"An error occurred during the analysis: {e}", exc_info=True)
        utils.update_system_control(session, 'worker_status', 'ERROR')
        utils.append_scan_log(session, f"BŁĄD KRYTYCZNY: {e}")
    finally:
        current_state = "IDLE"
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')
        utils.update_system_control(session, 'scan_progress_processed', '0')
        utils.update_system_control(session, 'scan_progress_total', '0')
        session.close()

def main_loop():
    global current_state
    logger.info("Worker started. Initializing...")
    
    with get_db_session() as session:
        logger.info("Verifying database tables for Worker...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified.")

        initialize_database_if_empty(session, api_client)
        
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    
    # ZMIANA: Z 1 minuty na 15 sekund
    schedule.every(15).seconds.do(lambda: phase3_sniper.monitor_entry_triggers(get_db_session(), api_client))
    
    # KROK 3: Dodajemy harmonogram dla Agencji Prasowej (co 5 minut)
    schedule.every(5).minutes.do(lambda: catalyst_monitor.run_catalyst_check(get_db_session()))

    
    logger.info(f"Scheduled job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Real-Time Entry Trigger Monitor scheduled every 15 seconds.")
    logger.info("Catalyst News Monitor scheduled every 5 minutes.") # <-- NOWY LOG


    with get_db_session() as initial_session:
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        utils.update_system_control(initial_session, 'worker_command', 'NONE')
        utils.update_system_control(initial_session, 'ai_analysis_request', 'NONE')
        utils.update_system_control(initial_session, 'current_phase', 'NONE')
        utils.update_system_control(initial_session, 'system_alert', 'NONE')
        utils.report_heartbeat(initial_session)

    while True:
        with get_db_session() as session:
            try:
                command_triggered_run, new_state = utils.check_for_commands(session, current_state)
                current_state = new_state

                if command_triggered_run:
                    run_full_analysis_cycle()
                
                if current_state != "PAUSED":
                    handle_ai_analysis_request(session)
                    schedule.run_pending()
                
                utils.report_heartbeat(session)
            except Exception as loop_error:
                logger.error(f"Error in main worker loop: {loop_error}", exc_info=True)
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        main_loop()
    else:
        logger.critical("Worker cannot start because database connection was not established.")

