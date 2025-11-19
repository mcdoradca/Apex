import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone, timedelta 
from dotenv import load_dotenv
from sqlalchemy import text, select, func

from .models import Base
from .database import get_db_session, engine

# KROK 1: Importujemy tylko niezbędne moduły
from .analysis import (
    phase1_scanner, 
    ai_agents, 
    utils,
    news_agent,
    phase0_macro_agent,
    virtual_agent,
    backtest_engine, 
    ai_optimizer,
    h3_deep_dive_agent
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

# ==================================================================
# === OBSŁUGA ZLECEŃ SPECJALNYCH (Backtest / AI / Deep Dive) ===
# ==================================================================

def handle_backtest_request(session, api_client) -> str:
    period_to_test = utils.get_system_control_value(session, 'backtest_request') 
    
    if period_to_test and period_to_test not in ['NONE', 'PROCESSING']:
        logger.warning(f"Zlecenie Backtestu Historycznego otrzymane dla: {period_to_test}.")
        
        params_json_str = utils.get_system_control_value(session, 'backtest_parameters')
        params = {}
        if params_json_str and params_json_str != '{}':
            try:
                params = json.loads(params_json_str)
                logger.info(f"Wczytano niestandardowe parametry backtestu: {params}")
            except json.JSONDecodeError as e:
                logger.error(f"Błąd parsowania parametrów backtestu JSON: {e}")
                utils.append_scan_log(session, "BŁĄD: Nie można wczytać parametrów H3. Użyto domyślnych.")
                params = {}

        utils.update_system_control(session, 'worker_status', 'BUSY_BACKTEST')
        utils.update_system_control(session, 'current_phase', 'BACKTESTING')
        utils.update_system_control(session, 'backtest_request', 'PROCESSING')
        utils.append_scan_log(session, f"Rozpoczynanie Backtestu Historycznego dla '{period_to_test}'...")

        try:
            backtest_engine.run_historical_backtest(session, api_client, period_to_test, parameters=params) 
            logger.info(f"Backtest Historyczny dla {period_to_test} zakończony pomyślnie.")
            utils.append_scan_log(session, f"Backtest Historyczny dla '{period_to_test}' zakończony.")
        except Exception as e:
            logger.error(f"Krytyczny błąd podczas Backtestu Historycznego dla {period_to_test}: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD KRYTYCZNY Backtestu: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'backtest_request', 'NONE')
            return 'IDLE'

    elif period_to_test == 'PROCESSING':
        return 'BUSY'
        
    return 'IDLE'

def handle_ai_optimizer_request(session) -> str:
    request_status = utils.get_system_control_value(session, 'ai_optimizer_request') 
    
    if request_status and request_status == 'REQUESTED':
        logger.warning("🤖 Zlecenie Mega Agenta AI otrzymane. Rozpoczynanie...")
        
        utils.update_system_control(session, 'worker_status', 'BUSY_AI_OPTIMIZER')
        utils.update_system_control(session, 'current_phase', 'AI_OPTIMIZING')
        utils.update_system_control(session, 'ai_optimizer_request', 'PROCESSING')
        utils.append_scan_log(session, "Rozpoczynanie analizy przez Mega Agenta AI...")

        try:
            ai_optimizer.run_ai_optimization_analysis(session)
            logger.info("🤖 Analiza Mega Agenta AI zakończona pomyślnie.")
            utils.append_scan_log(session, "🤖 Analiza Mega Agenta AI zakończona.")
        except Exception as e:
            logger.error(f"Krytyczny błąd podczas analizy Mega Agenta AI: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD KRYTYCZNY Mega Agenta: {e}")
            utils.update_system_control(session, 'ai_optimizer_report', f"BŁĄD KRYTYCZNY: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'ai_optimizer_request', 'NONE')
            return 'IDLE'

    elif request_status == 'PROCESSING':
        return 'BUSY'
        
    return 'IDLE'

def handle_h3_deep_dive_request(session) -> str:
    year_to_analyze_str = utils.get_system_control_value(session, 'h3_deep_dive_request') 
    
    if year_to_analyze_str and year_to_analyze_str not in ['NONE', 'PROCESSING']:
        try:
            year_to_analyze = int(year_to_analyze_str)
            logger.warning(f"Zlecenie H3 Deep Dive otrzymane dla roku: {year_to_analyze}.")
        except ValueError:
            logger.error(f"Otrzymano nieprawidłową wartość dla H3 Deep Dive: {year_to_analyze_str}")
            utils.update_system_control(session, 'h3_deep_dive_report', f"BŁĄD: Otrzymano nieprawidłowy rok")
            utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')
            return 'IDLE'

        utils.update_system_control(session, 'worker_status', 'BUSY_DEEP_DIVE')
        utils.update_system_control(session, 'current_phase', 'DEEP_DIVE_H3')
        utils.update_system_control(session, 'h3_deep_dive_request', 'PROCESSING')
        utils.append_scan_log(session, f"Rozpoczynanie analizy H3 Deep Dive dla roku '{year_to_analyze}'...")

        try:
            h3_deep_dive_agent.run_h3_deep_dive_analysis(session, year_to_analyze)
            logger.info(f"Analiza H3 Deep Dive dla {year_to_analyze} zakończona pomyślnie.")
            utils.append_scan_log(session, f"Analiza H3 Deep Dive dla '{year_to_analyze}' zakończona.")
        except Exception as e:
            logger.error(f"Krytyczny błąd podczas analizy H3 Deep Dive: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD KRYTYCZNY H3 Deep Dive: {e}")
            utils.update_system_control(session, 'h3_deep_dive_report', f"BŁĄD KRYTYCZNY: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')
            return 'IDLE'

    elif year_to_analyze_str == 'PROCESSING':
        return 'BUSY'
        
    return 'IDLE'


# ==================================================================
# === GŁÓWNA PĘTLA ANALITYCZNA (EOD) ===
# ==================================================================
def run_full_analysis_cycle():
    """
    Uruchamia cykliczne skanowanie.
    OBECNIE: Tylko Faza 0 (Makro) i Faza 1 (Skaner).
    """
    global current_state
    utils.clear_alert_memory_cache()
    logger.info("Telegram alert memory cache cleared for the new 24h cycle.")
    
    session = get_db_session()
    try:
        # Sprawdź czy worker nie jest zajęty zadaniami specjalnymi
        worker_status = utils.get_system_control_value(session, 'worker_status')
        if worker_status not in ['IDLE', 'ERROR']:
            logger.warning(f"Analysis cycle skipped because worker is busy: {worker_status}")
            session.close()
            return
        
        logger.info("Cleaning tables and expiring old setups before new analysis cycle...")
        
        # Czyścimy stare dane Fazy 1
        session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date < CURRENT_DATE - INTERVAL '1 day';"))
        session.execute(text("DELETE FROM processed_news WHERE processed_at < NOW() - INTERVAL '3 days';"))
        
        # Usuń stare sygnały niebędące częścią historii transakcji
        session.execute(text("""
            DELETE FROM trading_signals 
            WHERE status NOT IN ('ACTIVE', 'PENDING') 
            AND generation_date < NOW() - INTERVAL '3 days';
        """))
        
        session.commit()
        logger.info("Daily tables cleaned. Proceeding with analysis.")
    except Exception as e:
        logger.error(f"Could not clean tables before run: {e}", exc_info=True)
        session.rollback()
 
    try:
        # --- FAZA 0: MAKRO ---
        logger.info("Starting Phase 0: Macro Agent...")
        utils.update_system_control(session, 'current_phase', 'PHASE_0')
        utils.append_scan_log(session, "Faza 0: Uruchamianie Agenta Makro...")
        
        macro_sentiment = phase0_macro_agent.run_macro_analysis(session, api_client)
        
        if macro_sentiment == 'RISK_OFF':
            logger.warning("Phase 0 returned RISK_OFF. Halting full analysis cycle.")
            utils.append_scan_log(session, "Faza 0: RISK_OFF. Skanowanie EOD wstrzymane.")
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            session.close()
            return
        
        logger.info("Phase 0 returned RISK_ON. Proceeding with scan.")
        utils.append_scan_log(session, "Faza 0: RISK_ON. Warunki sprzyjające, kontynuacja skanowania.")

        # --- FAZA 1: SKANER (Istotne dla H3) ---
        logger.info("Starting Phase 1 Scan...")
        current_state = "RUNNING"
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.append_scan_log(session, "Rozpoczynanie cyklu analizy (Faza 1)...")
        
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        
        if not candidate_tickers:
            utils.append_scan_log(session, "Faza 1 zakończona. Nie znaleziono kandydatów.")
        else:
            utils.append_scan_log(session, f"Faza 1 zakończona. Znaleziono {len(candidate_tickers)} kandydatów (Baza dla H3).")

        # --- KONIEC CYKLU (Fazy 2 i 3 zostały usunięte) ---
        logger.info("Analysis cycle completed (Phase 0 + Phase 1).")
        utils.append_scan_log(session, "Cykl skanowania EOD zakończony.")

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
    global current_state, api_client
    logger.info("Worker started. Initializing...")
    
    with get_db_session() as session:
        logger.info("Verifying database tables for Worker...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified.")

        initialize_database_if_empty(session, api_client)
        
    # Harmonogram EOD (Faza 0 + 1)
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    
    # Agenci Tła
    schedule.every(2).minutes.do(lambda: news_agent.run_news_agent_cycle(get_db_session(), api_client))
    schedule.every().day.at("23:00", "Europe/Warsaw").do(lambda: virtual_agent.run_virtual_trade_monitor(get_db_session(), api_client))
    
    logger.info(f"Scheduled EOD scan set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Ultra News Agent scheduled every 2 minutes.")
    logger.info("Virtual Agent Monitor scheduled every day at 23:00 CET.")

    with get_db_session() as initial_session:
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        utils.update_system_control(initial_session, 'worker_command', 'NONE')
        utils.update_system_control(initial_session, 'current_phase', 'NONE')
        utils.update_system_control(initial_session, 'system_alert', 'NONE')
        utils.update_system_control(initial_session, 'backtest_request', 'NONE')
        utils.update_system_control(initial_session, 'ai_optimizer_request', 'NONE')
        utils.update_system_control(initial_session, 'ai_optimizer_report', 'NONE')
        utils.update_system_control(initial_session, 'h3_deep_dive_request', 'NONE')
        utils.update_system_control(initial_session, 'h3_deep_dive_report', 'NONE')
        utils.report_heartbeat(initial_session)

    while True:
        with get_db_session() as session:
            try:
                # Obsługa Zleceń Specjalnych
                handle_backtest_request(session, api_client)
                handle_ai_optimizer_request(session)
                handle_h3_deep_dive_request(session)
                
                # Sprawdź status
                worker_status = utils.get_system_control_value(session, 'worker_status')
                if worker_status.startswith('BUSY_'):
                    utils.report_heartbeat(session) 
                    time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)
                    continue 
                
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
