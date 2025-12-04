import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone, timedelta 
from dotenv import load_dotenv
from sqlalchemy import text, select, func

from .models import Base, OptimizationJob 
from .database import get_db_session, engine
# Importujemy najpierw tylko to co niezbędne do migracji
from .data_ingestion.data_initializer import initialize_database_if_empty
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS

# Reszta importów
from .analysis import (
    phase1_scanner, phase3_sniper, ai_agents, utils, news_agent,
    phase0_macro_agent, virtual_agent, backtest_engine, ai_optimizer, h3_deep_dive_agent,
    signal_monitor, apex_optimizer, phasex_scanner, biox_agent, recheck_agent,
    # === IMPORT SKANERA FAZY 4 ===
    phase4_kinetic 
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)
load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY: sys.exit(1)

current_state = "IDLE" 
api_client = AlphaVantageClient(api_key=API_KEY)

# === STRAŻNIK PROCESÓW ===
def can_run_background_task():
    global current_state
    HEAVY_DUTY_STATES = [
        'RUNNING', 
        'BUSY_BACKTEST', 
        'BUSY_OPTIMIZING', 
        'BUSY_AI_OPTIMIZER', 
        'BUSY_DEEP_DIVE',
        'PHASE_1_SCAN', 
        'PHASE_3_LIVE',
        'PHASE_X_SCAN',
        # === NOWY STAN CIĘŻKI ===
        'PHASE_4_KINETIC'
    ]
    if any(s in current_state for s in HEAVY_DUTY_STATES):
        return False
    return True

# === WRAPPERY DLA ZADAŃ W TLE ===

def safe_run_news_agent():
    if can_run_background_task():
        with get_db_session() as session:
            try:
                news_agent.run_news_agent_cycle(session, api_client)
            except Exception as e:
                logger.error(f"News Agent Error: {e}")

def safe_run_signal_monitor():
    if can_run_background_task():
        with get_db_session() as session:
            try:
                signal_monitor.run_signal_monitor_cycle(session, api_client)
            except Exception as e:
                logger.error(f"Signal Monitor Error: {e}")

def safe_run_virtual_agent():
    if can_run_background_task():
        with get_db_session() as session:
            try:
                virtual_agent.run_virtual_trade_monitor(session, api_client)
            except Exception as e:
                logger.error(f"Virtual Agent Error: {e}")

def safe_run_biox_monitor():
    if can_run_background_task():
        with get_db_session() as session:
            try:
                biox_agent.run_biox_live_monitor(session, api_client)
            except Exception as e:
                logger.error(f"BioX Monitor Error: {e}")

def safe_run_recheck_audit():
    if can_run_background_task():
        with get_db_session() as session:
            try:
                recheck_agent.run_recheck_audit_cycle(session)
            except Exception as e:
                logger.error(f"Re-check Agent Error: {e}")

# === GŁÓWNE PROCESY ===

def run_phase_1_cycle(session):
    global current_state
    session.rollback() 
    try:
        logger.info("Starting Phase 1 Cycle (Macro + Scan)...")
        utils.append_scan_log(session, ">>> Rozpoczynanie Fazy 1...") 
        
        current_state = 'RUNNING'
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        
        utils.update_system_control(session, 'current_phase', 'PHASE_0_MACRO')
        macro_sentiment = phase0_macro_agent.run_macro_analysis(session, api_client)
        if macro_sentiment == 'RISK_OFF':
            utils.append_scan_log(session, "Faza 0: RISK_OFF. Skanowanie przerwane.")
            return

        utils.append_scan_log(session, "Czyszczenie tabeli kandydatów Fazy 1...")
        session.execute(text("DELETE FROM phase1_candidates"))
        session.commit()

        utils.update_system_control(session, 'current_phase', 'PHASE_1_SCAN')
        candidates = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        
        if candidates:
            utils.append_scan_log(session, f"Faza 1 zakończona. Znaleziono {len(candidates)} kandydatów.")
        else:
            utils.append_scan_log(session, "Faza 1: Brak kandydatów.")
            
    except Exception as e:
        logger.error(f"Error in Phase 1 Cycle: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD Fazy 1: {e}")
    finally:
        current_state = 'IDLE'
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

def run_phase_3_cycle(session):
    global current_state
    session.rollback()
    try:
        logger.info("Starting Phase 3 Cycle (H3 Live)...")
        
        current_state = 'RUNNING'
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        
        params_json = utils.get_system_control_value(session, 'h3_live_parameters')
        params = {}
        if params_json and params_json != '{}':
             try: params = json.loads(params_json)
             except: pass

        utils.append_scan_log(session, f">>> Rozpoczynanie Fazy 3. Params: {params}")
        
        candidates_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        candidates = [row[0] for row in candidates_rows]
        
        if not candidates:
            utils.append_scan_log(session, "BŁĄD: Brak kandydatów z Fazy 1.")
            return

        utils.update_system_control(session, 'current_phase', 'PHASE_3_LIVE')
        phase3_sniper.run_h3_live_scan(session, candidates, api_client, parameters=params)
        
        utils.append_scan_log(session, "Faza 3 zakończona.")
        
    except Exception as e:
        logger.error(f"Error in Phase 3 Cycle: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD Fazy 3: {e}")
    finally:
        current_state = 'IDLE'
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

def run_phase_x_cycle(session):
    global current_state
    session.rollback()
    try:
        logger.info("Starting Phase X Cycle (BioX)...")
        current_state = 'RUNNING'
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.update_system_control(session, 'current_phase', 'PHASE_X_SCAN')
        utils.append_scan_log(session, ">>> Start Fazy X (BioX Pump Hunter)...")
        
        found_candidates = phasex_scanner.run_phasex_scan(session, api_client)
        
        utils.update_system_control(session, 'current_phase', 'PHASE_X_AUDIT')
        
        biox_agent.run_historical_catalyst_scan(session, api_client, candidates=found_candidates)
        
        utils.append_scan_log(session, "Faza X zakończona pomyślnie.")

    except Exception as e:
        logger.error(f"Error in Phase X Cycle: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD Fazy X: {e}")
    finally:
        current_state = 'IDLE'
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

# === NOWA FUNKCJA WRAPPER DLA H4 ===
def run_phase_4_cycle(session):
    global current_state
    session.rollback()
    try:
        logger.info("Starting Phase 4 Cycle (Kinetic Alpha)...")
        current_state = 'RUNNING'
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.update_system_control(session, 'current_phase', 'PHASE_4_KINETIC')
        utils.append_scan_log(session, ">>> Start Fazy 4 (H4: Kinetic Alpha)...")
        
        # Uruchomienie skanera z nowego modułu
        phase4_kinetic.run_phase4_scan(session, api_client)
        
        utils.append_scan_log(session, "Faza 4 zakończona pomyślnie.")

    except Exception as e:
        logger.error(f"Error in Phase 4 Cycle: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD Fazy 4: {e}")
    finally:
        current_state = 'IDLE'
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

def run_full_analysis_cycle():
    with get_db_session() as session:
        run_phase_1_cycle(session)
        run_phase_3_cycle(session)

# === OBSŁUGA ZLECEŃ ZEWNĘTRZNYCH ===

def handle_backtest_request(session, api_client) -> str:
    req = utils.get_system_control_value(session, 'backtest_request')
    if req and req not in ['NONE', 'PROCESSING']:
        utils.update_system_control(session, 'worker_status', 'BUSY_BACKTEST')
        utils.update_system_control(session, 'current_phase', 'BACKTESTING')
        utils.update_system_control(session, 'backtest_request', 'PROCESSING')
        
        params = {}
        try: params = json.loads(utils.get_system_control_value(session, 'backtest_parameters') or '{}')
        except: pass
        
        try: backtest_engine.run_historical_backtest(session, api_client, req, parameters=params)
        except Exception as e: logger.error(f"BT Error: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'backtest_request', 'NONE')
            return 'IDLE'
    elif req == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def handle_ai_optimizer_request(session) -> str:
    req = utils.get_system_control_value(session, 'ai_optimizer_request')
    if req == 'REQUESTED':
        utils.update_system_control(session, 'worker_status', 'BUSY_AI_OPTIMIZER')
        utils.update_system_control(session, 'current_phase', 'AI_ANALYSIS')
        utils.update_system_control(session, 'ai_optimizer_request', 'PROCESSING')
        try: ai_optimizer.run_ai_optimization_analysis(session)
        except: pass
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'ai_optimizer_request', 'NONE')
            return 'IDLE'
    elif req == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def handle_h3_deep_dive_request(session) -> str:
    req = utils.get_system_control_value(session, 'h3_deep_dive_request')
    if req and req not in ['NONE', 'PROCESSING']:
        utils.update_system_control(session, 'worker_status', 'BUSY_DEEP_DIVE')
        utils.update_system_control(session, 'current_phase', 'DEEP_DIVE')
        utils.update_system_control(session, 'h3_deep_dive_request', 'PROCESSING')
        try: h3_deep_dive_agent.run_h3_deep_dive_analysis(session, int(req))
        except: pass
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')
            return 'IDLE'
    elif req == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def handle_optimization_request(session) -> str:
    job_id = utils.get_system_control_value(session, 'optimization_request')
    if job_id and job_id not in ['NONE', 'PROCESSING']:
        logger.info(f"Optimization Job: {job_id}")
        utils.update_system_control(session, 'worker_status', 'BUSY_OPTIMIZING')
        utils.update_system_control(session, 'current_phase', 'QUANTUM_OPT')
        utils.update_system_control(session, 'optimization_request', 'PROCESSING')
        
        try:
            job = session.query(OptimizationJob).filter(OptimizationJob.id == job_id).first()
            if job:
                optimizer = apex_optimizer.QuantumOptimizer(session, job_id, job.target_year)
                optimizer.run(n_trials=job.total_trials)
                utils.append_scan_log(session, f"Optymalizacja zakończona. Wynik: {job.best_score}")
        except Exception as e:
            logger.error(f"Optimization Error: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD OPTYMALIZACJI: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'optimization_request', 'NONE')
            return 'IDLE'
    elif job_id == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def main_loop():
    global current_state, api_client
    logger.info("Worker main loop started with FORCED DATABASE INIT.")
    
    try:
        with get_db_session() as session:
            logger.info("Executing Pre-Flight Database Check & Migration...")
            Base.metadata.create_all(bind=engine)
            initialize_database_if_empty(session, api_client)
            logger.info("Pre-Flight Check Completed Successfully.")
    except Exception as e:
        logger.critical(f"CRITICAL STARTUP ERROR: Database initialization failed: {e}", exc_info=True)
        time.sleep(5)
    
    # Schedule
    schedule.every(2).minutes.do(safe_run_news_agent)
    schedule.every().day.at("23:00", "Europe/Warsaw").do(safe_run_virtual_agent)
    schedule.every(3).seconds.do(safe_run_signal_monitor)
    schedule.every(5).minutes.do(safe_run_biox_monitor)
    schedule.every(10).minutes.do(safe_run_recheck_audit)

    try:
        with get_db_session() as initial_session:
            utils.update_system_control(initial_session, 'worker_status', 'IDLE')
            utils.update_system_control(initial_session, 'current_phase', 'NONE')
            utils.update_system_control(initial_session, 'worker_command', 'NONE')
            utils.report_heartbeat(initial_session)
            utils.append_scan_log(initial_session, "SYSTEM: Worker Gotowy (H4 Active).")
    except Exception as e:
        logger.error(f"Startup status init failed: {e}")

    while True:
        with get_db_session() as session:
            try:
                run_action, new_state = utils.check_for_commands(session, current_state)
                if new_state != current_state:
                    current_state = new_state

                if run_action == "FULL_RUN": run_full_analysis_cycle()
                elif run_action == "PHASE_1_RUN": run_phase_1_cycle(session)
                elif run_action == "PHASE_3_RUN": run_phase_3_cycle(session)
                elif run_action == "PHASE_X_RUN": run_phase_x_cycle(session)
                # === OBSŁUGA KOMENDY H4 ===
                elif run_action == "PHASE_4_RUN": 
                    # Sprawdź komendę w utils.py - musimy tam dodać mapowanie 'START_PHASE_4_REQUESTED'
                    run_phase_4_cycle(session)
                
                status = 'IDLE'
                if current_state == 'IDLE':
                    status = handle_backtest_request(session, api_client)
                if current_state == 'IDLE' and status == 'IDLE':
                    status = handle_ai_optimizer_request(session)
                if current_state == 'IDLE' and status == 'IDLE':
                    status = handle_h3_deep_dive_request(session)
                if current_state == 'IDLE' and status == 'IDLE':
                    status = handle_optimization_request(session)
                
                if status != 'IDLE':
                    current_state = 'BUSY'

                schedule.run_pending()
                utils.report_heartbeat(session) 
                
            except Exception as e:
                logger.error(f"Loop error: {e}")
                current_state = 'IDLE'
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        main_loop()
    else:
        sys.exit(1)
