import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import text

# --- KRYTYCZNA ZMIANA: Importujemy `models` z API, aby Worker mógł tworzyć tabele ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'api', 'src')))
from api import models

from .analysis import phase1_scanner, phase2_engine, phase3_sniper, on_demand_analyzer, utils
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .data_ingestion.data_initializer import initialize_database_if_empty
from .database import get_db_session, engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not API_KEY:
    logger.critical("ALPHAVANTAGE_API_KEY environment variable not set. Exiting.")
    sys.exit(1)

current_state = "IDLE"
api_client = AlphaVantageClient(api_key=API_KEY)


def handle_on_demand_requests(session):
    # Ta funkcja pozostaje bez zmian
    ticker_to_analyze = utils.get_system_control_value(session, 'on_demand_request')
    if ticker_to_analyze and ticker_to_analyze not in ['NONE', 'PROCESSING']:
        logger.info(f"On-demand request received for: {ticker_to_analyze}.")
        utils.update_system_control(session, 'on_demand_request', 'PROCESSING')
        try:
            results = on_demand_analyzer.perform_full_analysis(ticker_to_analyze, api_client)
            stmt = text("INSERT INTO on_demand_results (ticker, analysis_data, last_updated) VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();")
            session.execute(stmt, {'ticker': ticker_to_analyze, 'data': json.dumps(results)})
            session.commit()
            logger.info(f"Successfully saved on-demand analysis for {ticker_to_analyze}.")
        except Exception as e:
            logger.error(f"Error during on-demand analysis for {ticker_to_analyze}: {e}", exc_info=True)
            error_result = {"error": True, "message": str(e), "ticker": ticker_to_analyze}
            stmt = text("INSERT INTO on_demand_results (ticker, analysis_data, last_updated) VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();")
            session.execute(stmt, {'ticker': ticker_to_analyze, 'data': json.dumps(error_result)})
            session.commit()
        finally:
             utils.update_system_control(session, 'on_demand_request', 'NONE')

    predator_ticker = utils.get_system_control_value(session, 'phase3_on_demand_request')
    if predator_ticker and predator_ticker not in ['NONE', 'PROCESSING']:
        logger.info(f"Phase 3 on-demand ('Predator') request for: {predator_ticker}.")
        utils.update_system_control(session, 'phase3_on_demand_request', 'PROCESSING')
        try:
            results = phase3_sniper.plan_trade_on_demand(predator_ticker, api_client)
            stmt = text("INSERT INTO phase3_on_demand_results (ticker, analysis_data, last_updated) VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();")
            session.execute(stmt, {'ticker': predator_ticker, 'data': json.dumps(results)})
            session.commit()
            logger.info(f"Successfully saved Phase 3 on-demand analysis for {predator_ticker}.")
        except Exception as e:
            logger.error(f"Error during Phase 3 on-demand for {predator_ticker}: {e}", exc_info=True)
            error_result = {"error": True, "message": str(e), "ticker": predator_ticker}
            stmt = text("INSERT INTO phase3_on_demand_results (ticker, analysis_data, last_updated) VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();")
            session.execute(stmt, {'ticker': predator_ticker, 'data': json.dumps(error_result)})
            session.commit()
        finally:
            utils.update_system_control(session, 'phase3_on_demand_request', 'NONE')


def run_full_analysis_cycle():
    # Ta funkcja pozostaje bez zmian
    global current_state
    session = get_db_session()
    if utils.get_system_control_value(session, 'worker_status') == 'RUNNING':
        logger.warning("Analysis cycle already in progress. Skipping scheduled run.")
        session.close()
        return
    try:
        logger.info("Starting full analysis cycle...")
        current_state = "RUNNING"
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        logger.info("Clearing old scan data...")
        utils.update_system_control(session, 'scan_log', '')
        session.execute(text("DELETE FROM phase1_candidates"))
        session.execute(text("DELETE FROM phase2_results"))
        session.execute(text("UPDATE trading_signals SET status = 'EXPIRED' WHERE status = 'ACTIVE'"))
        session.commit()
        logger.info("Old data cleared successfully.")
        utils.append_scan_log(session, "Rozpoczynanie nowego cyklu analizy...")
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            raise Exception("Phase 1 found no candidates. Halting cycle.")
        utils.update_system_control(session, 'current_phase', 'PHASE_2')
        qualified_tickers = phase2_engine.run_analysis(session, candidate_tickers, lambda: current_state, api_client)
        if not qualified_tickers:
            raise Exception("Phase 2 qualified no stocks. Halting cycle.")
        utils.update_system_control(session, 'current_phase', 'PHASE_3')
        phase3_sniper.run_tactical_planning(session, qualified_tickers, lambda: current_state, api_client)
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
    
    # --- KRYTYCZNA ZMIANA: Tworzenie tabel PRZED rozpoczęciem inicjalizacji danych ---
    try:
        logger.info("Worker is taking responsibility for creating database tables...")
        models.Base.metadata.create_all(bind=engine)
        logger.info("Database tables created/verified successfully by the Worker.")
    except Exception as e:
        logger.critical(f"FATAL: Worker failed to create database tables: {e}", exc_info=True)
        sys.exit(1) # Zatrzymujemy workera, jeśli nie może utworzyć tabel
        
    with get_db_session() as session:
        initialize_database_if_empty(session, api_client)
        
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    schedule.every(15).minutes.do(lambda: phase3_sniper.monitor_pending_signals(get_db_session(), api_client))
    
    logger.info(f"Scheduled job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Pending signals monitor scheduled every 15 minutes.")

    with get_db_session() as initial_session:
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        utils.update_system_control(initial_session, 'worker_command', 'NONE')
        utils.update_system_control(initial_session, 'on_demand_request', 'NONE')
        utils.update_system_control(initial_session, 'phase3_on_demand_request', 'NONE')
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
                    handle_on_demand_requests(session)
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
