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

# Importujemy moduły (w tym nowy phase3_sniper)
from .analysis import (
    phase1_scanner, 
    phase3_sniper, # <-- To jest nasz nowy H3 Live Engine
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

# --- Funkcje obsługi zleceń (Backtest, AI, Deep Dive) ---
# (Skopiuj te funkcje 1:1 z poprzedniej wersji pliku main.py, one się nie zmieniają)
# Dla oszczędności miejsca wklejam tylko definicje.
def handle_backtest_request(session, api_client) -> str:
    # ... (Kod bez zmian) ...
    period_to_test = utils.get_system_control_value(session, 'backtest_request') 
    if period_to_test and period_to_test not in ['NONE', 'PROCESSING']:
        logger.warning(f"Zlecenie Backtestu: {period_to_test}")
        params = {}
        try:
            p_str = utils.get_system_control_value(session, 'backtest_parameters')
            if p_str and p_str != '{}': params = json.loads(p_str)
        except: pass
        utils.update_system_control(session, 'worker_status', 'BUSY_BACKTEST')
        utils.update_system_control(session, 'current_phase', 'BACKTESTING')
        utils.update_system_control(session, 'backtest_request', 'PROCESSING')
        try:
            backtest_engine.run_historical_backtest(session, api_client, period_to_test, parameters=params)
        except Exception as e: logger.error(f"Błąd Backtestu: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'backtest_request', 'NONE')
            return 'IDLE'
    elif period_to_test == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def handle_ai_optimizer_request(session) -> str:
    # ... (Kod bez zmian - skopiuj z poprzedniej odpowiedzi) ...
    status = utils.get_system_control_value(session, 'ai_optimizer_request')
    if status == 'REQUESTED':
        utils.update_system_control(session, 'worker_status', 'BUSY_AI_OPTIMIZER')
        utils.update_system_control(session, 'ai_optimizer_request', 'PROCESSING')
        try: ai_optimizer.run_ai_optimization_analysis(session)
        except: pass
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'ai_optimizer_request', 'NONE')
            return 'IDLE'
    elif status == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def handle_h3_deep_dive_request(session) -> str:
    # ... (Kod bez zmian - skopiuj z poprzedniej odpowiedzi) ...
    req = utils.get_system_control_value(session, 'h3_deep_dive_request')
    if req and req not in ['NONE', 'PROCESSING']:
        utils.update_system_control(session, 'worker_status', 'BUSY_DEEP_DIVE')
        utils.update_system_control(session, 'h3_deep_dive_request', 'PROCESSING')
        try: h3_deep_dive_agent.run_h3_deep_dive_analysis(session, int(req))
        except: pass
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')
            return 'IDLE'
    elif req == 'PROCESSING': return 'BUSY'
    return 'IDLE'


# ==================================================================
# === GŁÓWNA PĘTLA ANALITYCZNA (EOD) ===
# ==================================================================
def run_full_analysis_cycle():
    """
    Uruchamia cykliczne skanowanie.
    FAZY: Faza 0 (Makro) -> Faza 1 (Skaner) -> Faza 3 (H3 Live).
    """
    global current_state
    utils.clear_alert_memory_cache()
    logger.info("Rozpoczynanie cyklu EOD. Pamięć alertów wyczyszczona.")
    
    session = get_db_session()
    try:
        worker_status = utils.get_system_control_value(session, 'worker_status')
        if worker_status not in ['IDLE', 'ERROR']:
            logger.warning(f"Cykl pominięty, worker zajęty: {worker_status}")
            session.close()
            return
        
        # Czyszczenie tabel
        logger.info("Czyszczenie tabel dziennych...")
        session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date < CURRENT_DATE - INTERVAL '1 day';"))
        session.execute(text("DELETE FROM processed_news WHERE processed_at < NOW() - INTERVAL '3 days';"))
        session.execute(text("DELETE FROM trading_signals WHERE status NOT IN ('ACTIVE', 'PENDING') AND generation_date < NOW() - INTERVAL '3 days';"))
        session.commit()
 
    try:
        # --- FAZA 0: MAKRO ---
        utils.update_system_control(session, 'current_phase', 'PHASE_0')
        macro_sentiment = phase0_macro_agent.run_macro_analysis(session, api_client)
        
        if macro_sentiment == 'RISK_OFF':
            utils.append_scan_log(session, "Faza 0: RISK_OFF. Skanowanie wstrzymane.")
            utils.update_system_control(session, 'worker_status', 'IDLE')
            session.close()
            return
        
        utils.append_scan_log(session, "Faza 0: RISK_ON. Uruchamianie skanera.")

        # --- FAZA 1: SKANER (Kandydaci) ---
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        
        if not candidate_tickers:
            utils.append_scan_log(session, "Faza 1: Brak kandydatów. Koniec cyklu.")
        else:
            utils.append_scan_log(session, f"Faza 1 zakończona. {len(candidate_tickers)} kandydatów przekazanych do H3.")
            
            # --- FAZA 3: H3 LIVE ENGINE (NOWOŚĆ) ---
            utils.update_system_control(session, 'current_phase', 'PHASE_3_H3_LIVE')
            # Przekazujemy listę tickerów bezpośrednio do nowego silnika H3
            phase3_sniper.run_h3_live_scan(session, candidate_tickers, api_client)

        logger.info("Cykl EOD zakończony (F0 -> F1 -> F3).")
        utils.append_scan_log(session, "Cykl analizy zakończony.")

    except Exception as e:
        logger.error(f"Błąd cyklu EOD: {e}", exc_info=True)
        utils.update_system_control(session, 'worker_status', 'ERROR')
        utils.append_scan_log(session, f"BŁĄD KRYTYCZNY: {e}")
    finally:
        current_state = "IDLE"
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')
        session.close()


def main_loop():
    global current_state, api_client
    logger.info("Worker started.")
    
    with get_db_session() as session:
        Base.metadata.create_all(bind=engine)
        initialize_database_if_empty(session, api_client)
        
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    schedule.every(2).minutes.do(lambda: news_agent.run_news_agent_cycle(get_db_session(), api_client))
    schedule.every().day.at("23:00", "Europe/Warsaw").do(lambda: virtual_agent.run_virtual_trade_monitor(get_db_session(), api_client))
    
    logger.info(f"Harmonogram: EOD scan o {ANALYSIS_SCHEDULE_TIME_CET} CET.")

    with get_db_session() as initial_session:
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        # ... (Reszta resetów flag - bez zmian) ...

    while True:
        with get_db_session() as session:
            try:
                handle_backtest_request(session, api_client)
                handle_ai_optimizer_request(session)
                handle_h3_deep_dive_request(session)
                
                if utils.get_system_control_value(session, 'worker_status').startswith('BUSY_'):
                    utils.report_heartbeat(session)
                    time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)
                    continue
                
                schedule.run_pending()
                utils.report_heartbeat(session) 
            except Exception as e:
                logger.error(f"Loop error: {e}")
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main_loop()
