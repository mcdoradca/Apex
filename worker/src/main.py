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

from .analysis import (
    phase1_scanner, 
    phase2_engine, 
    phase3_sniper, 
    ai_agents, 
    utils,
    catalyst_monitor 
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

# ==================================================================
#  POPRAWKA: Przywrócenie definicji `api_client` i `current_state`
# ==================================================================
current_state = "IDLE"
api_client = AlphaVantageClient(api_key=API_KEY)

# === Logika Strażnika dla Catalyst Monitor ===
catalyst_monitor_running = False
TICKERS_PER_BATCH = 1

def run_catalyst_monitor_job():
    """
    Uruchamia zadanie monitora wiadomości z blokadą, aby zapobiec
    współbieżnym uruchomieniom, które wyczerpują limity API.
    Logika "Karuzeli": Przetwarza tylko małą paczkę tickerów przy każdym uruchomieniu.
    """
    global catalyst_monitor_running
    if catalyst_monitor_running:
        logger.warning("Catalyst monitor job already running. Skipping this cycle.")
        return

    catalyst_monitor_running = True
    logger.info("Starting catalyst monitor job (Batch)...")
    
    session = get_db_session()
    try:
        # 1. Pobierz *wszystkie* tickery do monitorowania
        all_tickers_to_monitor = catalyst_monitor.get_tickers_to_monitor(session)
        if not all_tickers_to_monitor:
            logger.info("CatalystMonitor: Brak tickerów Fazy 3 do monitorowania wiadomości.")
            return

        total_tickers = len(all_tickers_to_monitor)

        # 2. Pobierz ostatni indeks, od którego skończyliśmy
        last_index_str = utils.get_system_control_value(session, 'catalyst_monitor_last_index')
        last_index = int(last_index_str) if last_index_str else 0

        # 3. Wybierz paczkę (np. 1 ticker) do przetworzenia TERAZ
        tickers_to_process = []
        next_index = last_index
        for _ in range(TICKERS_PER_BATCH):
            if next_index >= total_tickers:
                next_index = 0 # Wróć na początek listy
            
            if total_tickers > 0: # Upewnij się, że lista nie jest pusta
                tickers_to_process.append(all_tickers_to_monitor[next_index])
                next_index += 1
        
        logger.info(f"CatalystMonitor: Przetwarzanie paczki {len(tickers_to_process)}/{total_tickers} tickerów: {', '.join(tickers_to_process)}")

        # 4. Przetwórz *tylko* tę małą paczkę
        for ticker in tickers_to_process:
            # Sprawdź status rynku PRZED wykonaniem zapytania do API
            market_info = utils.get_market_status_and_time(api_client)
            if market_info.get("status") in ["MARKET_CLOSED", "UNKNOWN"]:
                 logger.info(f"CatalystMonitor: Rynek zamknięty. Pomijanie sprawdzania newsów dla {ticker}.")
                 continue # Pomiń ten ticker, ale pętla harmonogramu działa dalej

            # Jeśli rynek jest otwarty, sprawdzamy newsy
            catalyst_monitor.run_check_for_single_ticker(session, ticker)

        # 5. Zapisz nowy indeks na następne uruchomienie
        utils.update_system_control(session, 'catalyst_monitor_last_index', str(next_index))

    except Exception as e:
        logger.error(f"Error in catalyst monitor job: {e}", exc_info=True)
    finally:
        catalyst_monitor_running = False
        session.close()
        logger.info("Catalyst monitor job finished.")


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
            results = ai_agents.run_ai_analysis(ticker_to_analyze, api_client)
            
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
        
        # ==================================================================
        #  Logika "Week Tradingu": Ta linia jest wykomentowana, aby
        #  sygnały Fazy 3 były trwałe.
        # ==================================================================
        # session.execute(text("UPDATE trading_signals SET status = 'EXPIRED' WHERE status = 'ACTIVE'"))
        # session.commit()
        # logger.info("Old active signals marked as expired.")
        logger.info("Skipping automatic expiration of signals to support 'week trading' logic.")
        # ==================================================================
        
        utils.append_scan_log(session, "Rozpoczynanie nowego cyklu analizy...")
        
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            raise Exception("Phase 1 found no candidates. Halting cycle.")

        utils.update_system_control(session, 'current_phase', 'PHASE_2')
        qualified_data = phase2_engine.run_analysis(session, candidate_tickers, lambda: current_state, api_client)
        if not qualified_data:
            raise Exception("Phase 2 qualified no stocks. Halting cycle.")

        utils.update_system_control(session, 'current_phase', 'PHASE_3')
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

        # Wywołanie initialize_database_if_empty z poprawnie zdefiniowanym api_client
        initialize_database_if_empty(session, api_client)
        
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    
    schedule.every(15).seconds.do(lambda: phase3_sniper.monitor_entry_triggers(get_db_session(), api_client))
    
    # Logika "Cierpliwego Strażnika"
    schedule.every(10).minutes.do(run_catalyst_monitor_job)

    
    logger.info(f"Scheduled job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Real-Time Entry Trigger Monitor scheduled every 15 seconds.")
    logger.info(f"Catalyst News Monitor scheduled every 10 minutes (processing {TICKERS_PER_BATCH} ticker(s) per run).")


    with get_db_session() as initial_session:
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        utils.update_system_control(initial_session, 'worker_command', 'NONE')
        utils.update_system_control(initial_session, 'ai_analysis_request', 'NONE')
        utils.update_system_control(initial_session, 'current_phase', 'NONE')
        utils.update_system_control(initial_session, 'system_alert', 'NONE')
        # Inicjalizacja wskaźnika "karuzeli"
        if utils.get_system_control_value(initial_session, 'catalyst_monitor_last_index') is None:
            utils.update_system_control(initial_session, 'catalyst_monitor_last_index', '0')
        
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
                
                # ==================================================================
                #  POPRAWKA BŁĘDU (Literówka)
                # ==================================================================
                utils.report_heartbeat(session) # Poprawiono z `report_heartkey`
            except Exception as loop_error:
                logger.error(f"Error in main worker loop: {loop_error}", exc_info=True)
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        main_loop()
    else:
        logger.critical("Worker cannot start because database connection was not established.")

