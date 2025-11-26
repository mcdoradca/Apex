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
from .analysis import (
    phase1_scanner, phase3_sniper, ai_agents, utils, news_agent,
    phase0_macro_agent, virtual_agent, backtest_engine, ai_optimizer, h3_deep_dive_agent,
    signal_monitor, apex_optimizer 
)
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .data_ingestion.data_initializer import initialize_database_if_empty

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)
load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY: sys.exit(1)
current_state = "IDLE"
api_client = AlphaVantageClient(api_key=API_KEY)

def run_phase_1_cycle(session):
    session.rollback() 
    try:
        logger.info("Starting Phase 1 Cycle (Macro + Scan)...")
        utils.append_scan_log(session, ">>> Rozpoczynanie Fazy 1 (Skanowanie rynku)...") 
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.update_system_control(session, 'current_phase', 'PHASE_0')
        macro_sentiment = phase0_macro_agent.run_macro_analysis(session, api_client)
        if macro_sentiment == 'RISK_OFF':
            utils.append_scan_log(session, "Faza 0: RISK_OFF. Skanowanie przerwane.")
            return

        session.execute(text("DELETE FROM phase1_candidates")) 
        session.commit()
        
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        candidates = phase1_scanner.run_scan(session, lambda: "RUNNING", api_client)
        
        if candidates:
            utils.append_scan_log(session, f"Faza 1 zakończona. Znaleziono {len(candidates)} kandydatów.")
        else:
            utils.append_scan_log(session, "Faza 1: Brak kandydatów.")
            
    except Exception as e:
        logger.error(f"Error in Phase 1 Cycle: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD Fazy 1: {e}")
    finally:
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

def run_phase_3_cycle(session):
    session.rollback()
    try:
        logger.info("Starting Phase 3 Cycle (H3 Live)...")
        
        params_json = utils.get_system_control_value(session, 'h3_live_parameters')
        params = {}
        if params_json and params_json != '{}':
             try: params = json.loads(params_json)
             except: pass

        utils.append_scan_log(session, f">>> Rozpoczynanie Fazy 3 (Szukanie sygnałów H3). Params: {params}")
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        
        candidates_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        candidates = [row[0] for row in candidates_rows]
        
        if not candidates:
            utils.append_scan_log(session, "BŁĄD: Brak kandydatów z Fazy 1. Uruchom najpierw Skanowanie F1.")
            return

        utils.update_system_control(session, 'current_phase', 'PHASE_3_H3_LIVE')
        phase3_sniper.run_h3_live_scan(session, candidates, api_client, parameters=params)
        
        utils.append_scan_log(session, "Faza 3 zakończona.")
        
    except Exception as e:
        logger.error(f"Error in Phase 3 Cycle: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD Fazy 3: {e}")
    finally:
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

def run_full_analysis_cycle():
    with get_db_session() as session:
        run_phase_1_cycle(session)
        run_phase_3_cycle(session)

def handle_backtest_request(session, api_client) -> str:
    req = utils.get_system_control_value(session, 'backtest_request')
    if req and req not in ['NONE', 'PROCESSING']:
        utils.update_system_control(session, 'worker_status', 'BUSY_BACKTEST')
        utils.update_system_control(session, 'backtest_request', 'PROCESSING')
        params = {}
        try: params = json.loads(utils.get_system_control_value(session, 'backtest_parameters') or '{}')
        except: pass
        try: backtest_engine.run_historical_backtest(session, api_client, req, parameters=params)
        except Exception as e: logger.error(f"BT Error: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'backtest_request', 'NONE')
            return 'IDLE'
    elif req == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def handle_ai_optimizer_request(session) -> str:
    req = utils.get_system_control_value(session, 'ai_optimizer_request')
    if req == 'REQUESTED':
        utils.update_system_control(session, 'worker_status', 'BUSY_AI_OPTIMIZER')
        utils.update_system_control(session, 'ai_optimizer_request', 'PROCESSING')
        try: ai_optimizer.run_ai_optimization_analysis(session)
        except: pass
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'ai_optimizer_request', 'NONE')
            return 'IDLE'
    elif req == 'PROCESSING': return 'BUSY'
    return 'IDLE'

def handle_h3_deep_dive_request(session) -> str:
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

def handle_optimization_request(session) -> str:
    """Obsługuje zlecenia optymalizacji Apex V4/V5/V6/V7 (Optuna)."""
    job_id = utils.get_system_control_value(session, 'optimization_request')
    
    if job_id and job_id not in ['NONE', 'PROCESSING']:
        logger.info(f"Otrzymano zlecenie optymalizacji: {job_id}")
        utils.update_system_control(session, 'worker_status', 'BUSY_OPTIMIZING')
        utils.update_system_control(session, 'optimization_request', 'PROCESSING')
        
        try:
            job = session.query(OptimizationJob).filter(OptimizationJob.id == job_id).first()
            if not job:
                logger.error(f"Optimization Job {job_id} not found in DB.")
                return 'IDLE'
            
            # Uruchom Quantum Optimizer (V7 Turbo)
            optimizer = apex_optimizer.QuantumOptimizer(session, job_id, job.target_year)
            optimizer.run(n_trials=job.total_trials)
            
            utils.append_scan_log(session, f"Optymalizacja V7 zakończona. Wynik: {job.best_score}")

        except Exception as e:
            logger.error(f"Critical Optimization Error: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD OPTYMALIZACJI: {e}")
        finally:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'optimization_request', 'NONE')
            return 'IDLE'
            
    elif job_id == 'PROCESSING': 
        return 'BUSY'
        
    return 'IDLE'

def main_loop():
    global current_state, api_client
    logger.info("Worker started.")
    with get_db_session() as session:
        Base.metadata.create_all(bind=engine)
        initialize_database_if_empty(session, api_client)
    
    # === MODYFIKACJA: TRYB MANUALNY ===
    # Zakomentowano automatyczne skanowanie. 
    # Użytkownik sam wywoła "Skanuj F1" z poziomu UI, kiedy będzie gotowy.
    
    # schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    
    # Agenci pomocniczy i strażnicy POZOSTAJĄ AKTYWNI
    # Agent Newsowy co 2 minuty (to jest bezpieczne, tylko nasłuchuje)
    schedule.every(2).minutes.do(lambda: news_agent.run_news_agent_cycle(get_db_session(), api_client))
    
    # Monitor Wirtualnego Agenta (zamykanie starych pozycji)
    schedule.every().day.at("23:00", "Europe/Warsaw").do(lambda: virtual_agent.run_virtual_trade_monitor(get_db_session(), api_client))

    # Strażnik Sygnałów (H3 Live) - monitoruje SL/TP dla aktywnych sygnałów
    schedule.every(1).minutes.do(lambda: signal_monitor.run_signal_monitor_cycle(get_db_session(), api_client))

    with get_db_session() as initial_session:
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        utils.update_system_control(initial_session, 'worker_command', 'NONE')
        utils.report_heartbeat(initial_session)
        # Informacja w logach, że jesteśmy w trybie manualnym
        utils.append_scan_log(initial_session, "SYSTEM: Tryb Manualny Aktywny. Automatyczne skanowanie nocne wyłączone.")

    while True:
        with get_db_session() as session:
            try:
                run_action, new_state = utils.check_for_commands(session, current_state)
                current_state = new_state

                if run_action == "FULL_RUN": run_full_analysis_cycle()
                elif run_action == "PHASE_1_RUN": run_phase_1_cycle(session)
                elif run_action == "PHASE_3_RUN": run_phase_3_cycle(session)
                
                # Sprawdzanie wszystkich typów zadań
                status = handle_backtest_request(session, api_client)
                if status == 'IDLE': status = handle_ai_optimizer_request(session)
                if status == 'IDLE': status = handle_h3_deep_dive_request(session)
                if status == 'IDLE': status = handle_optimization_request(session)
                
                worker_status = utils.get_system_control_value(session, 'worker_status')
                if worker_status.startswith('BUSY_'):
                    utils.report_heartbeat(session)
                    time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)
                    continue
                
                schedule.run_pending()
                utils.report_heartbeat(session) 
            except Exception as e:
                logger.error(f"Loop error: {e}")
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        main_loop()
    else:
        sys.exit(1)
