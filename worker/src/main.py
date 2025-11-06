import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone, timedelta # Dodano timedelta
from dotenv import load_dotenv
from sqlalchemy import text, select, func

from .models import Base
from .database import get_db_session, engine

# KROK 1: Importujemy nasz nowy monitor
from .analysis import (
    phase1_scanner, 
    phase2_engine, 
    phase3_sniper, 
    ai_agents, 
    utils,
    news_agent, # <-- ZMIANA: Import nowego Agenta (Kategoria 2)
    phase0_macro_agent # <-- POPRAWKA: Import Fazy 0
)
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .data_ingestion.data_initializer import initialize_database_if_empty

# USUNIĘTO: Zmienna TICKERS_PER_BATCH nie jest już potrzebna
# USUNIĘTO: Zmienna catalyst_monitor_running nie jest już potrzebna

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
            results = ai_agents.run_ai_analysis(session, ticker_to_analyze, api_client)
            
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
        # Czyścimy tylko przestarzałe dane Fazy 1 i Fazy 2
        session.execute(text("DELETE FROM phase2_results WHERE analysis_date < CURRENT_DATE - INTERVAL '1 day';"))
        session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date < CURRENT_DATE - INTERVAL '1 day';"))
        # Czyścimy stare wiadomości, aby umożliwić ponowną analizę
        session.execute(text("DELETE FROM processed_news WHERE processed_at < NOW() - INTERVAL '3 days';"))
        # Czyścimy unieważnione sygnały starsze niż 3 dni
        session.execute(text("DELETE FROM trading_signals WHERE status = 'INVALIDATED' AND generation_date < NOW() - INTERVAL '3 days';"))
        
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
        # ==================================================================
        # POPRAWKA 1 (Problem 2): Uruchomienie Agenta Fazy 0 (Makro)
        # ==================================================================
        logger.info("Starting Phase 0: Macro Agent...")
        utils.update_system_control(session, 'current_phase', 'PHASE_0')
        utils.append_scan_log(session, "Faza 0: Uruchamianie Agenta Makro...")
        
        macro_sentiment = phase0_macro_agent.run_macro_analysis(session, api_client)
        
        if macro_sentiment == 'RISK_OFF':
            logger.warning("Phase 0 returned RISK_OFF. Halting full analysis cycle.")
            utils.append_scan_log(session, "Faza 0: RISK_OFF. Skanowanie EOD wstrzymane.")
            # Zakończ cykl, ale ustaw status na IDLE (to nie jest błąd)
            current_state = "IDLE"
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            session.close()
            return
        
        logger.info("Phase 0 returned RISK_ON. Proceeding with scan.")
        utils.append_scan_log(session, "Faza 0: RISK_ON. Warunki sprzyjające, kontynuacja skanowania.")
        # ==================================================================
        # Koniec Poprawki 1
        # ==================================================================

        logger.info("Checking market status before starting Phase 1 scan...")
        market_info = utils.get_market_status_and_time(api_client)
        market_status = market_info.get("status")

        # Logika "Strażnika Rynku" dla nocnego skanowania EOD
        # Logika Fazy 1 została przebudowana, aby używać danych EOD (get_daily_adjusted)
        # Oznacza to, że może działać *po* zamknięciu rynku.
        # Musimy jednak zapewnić, że dane EOD z danego dnia są już dostępne.
        # Uruchamianie o 02:30 CET (po 20:30 ET) powinno być bezpieczne.
        # Dodajemy kontrolę, aby nie uruchamiać ręcznie w środku dnia.
        
        # Pobieramy aktualny czas w NY
        now_ny = utils.get_current_NY_datetime()
        ny_hour = now_ny.hour
        
        # Sprawdzamy, czy polecenie startu przyszło ręcznie (przez przycisk)
        is_manual_start = utils.get_system_control_value(session, 'worker_command') == 'START_REQUESTED'

        # Zezwalaj na start tylko w nocy (gdy dane EOD są gotowe) lub gdy rynek jest otwarty
        # (na potrzeby testów lub ręcznego uruchomienia w ciągu dnia)
        # Godziny 2:00 - 4:00 CET (20:00 - 22:00 ET) to idealne okno nocne
        is_eod_window = (now_ny.hour >= 20 or now_ny.hour < 4) 
        
        if market_status not in ["MARKET_OPEN", "PRE_MARKET", "AFTER_MARKET"] and not is_eod_window:
            logger.warning(f"Market status is {market_status} and it's outside EOD window. Full analysis cycle (Phase 1) will not run.")
            utils.append_scan_log(session, f"Skanowanie Fazy 1 wstrzymane. Rynek jest {market_status} (poza oknem EOD).")
            current_state = "IDLE"
            utils.update_system_control(session, 'worker_status', 'IDLE')
            session.close()
            return 
        
        logger.info(f"Market status is {market_status} (lub okno EOD). Proceeding with analysis cycle.")

        logger.info("Starting full analysis cycle...")
        current_state = "RUNNING"
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.update_system_control(session, 'scan_log', '')
        
        logger.info("Trwałe sygnały Fazy 3 są aktywne (nie wygasają co noc).")
        
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


# ==================================================================
# KROK 3 (KAT. 2): Usunięcie starej funkcji 'run_catalyst_monitor_job'
# Cała ta funkcja została zastąpiona przez 'news_agent.py'
# ==================================================================
# USUNIĘTO: def run_catalyst_monitor_job(): ...


def main_loop():
    global current_state, api_client
    logger.info("Worker started. Initializing...")
    
    with get_db_session() as session:
        logger.info("Verifying database tables for Worker...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified.")

        initialize_database_if_empty(session, api_client)
        
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    
    # POPRAWKA 2 (Problem 5: Latencja): Monitor cen (Strażnik SL/TP) - co 10 sekund (było 15)
    schedule.every(10).seconds.do(lambda: phase3_sniper.monitor_entry_triggers(get_db_session(), api_client))
    
    # ==================================================================
    # KROK 3 (KAT. 2): Aktywacja nowego "Ultra Agenta Newsowego"
    # Zastępujemy stare wywołanie 'run_catalyst_monitor_job'
    # ==================================================================
    # POPRAWKA 3 (Problem 1: Częstotliwość): Uruchamiamy agenta newsowego co 2 minuty (było 5)
    schedule.every(2).minutes.do(lambda: news_agent.run_news_agent_cycle(get_db_session(), api_client))
    
    logger.info(f"Scheduled job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Real-Time Entry Trigger Monitor scheduled every 10 seconds.") # <-- ZMIANA: Nowy log
    logger.info("Ultra News Agent (Kategoria 2) scheduled every 2 minutes.") # <-- ZMIANA: Nowy log


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
        logger.critical("Could not connect to database on startup. Worker exiting.")
        sys.exit(1)
