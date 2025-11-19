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
from .analysis import (
    phase1_scanner, phase3_sniper, ai_agents, utils, news_agent,
    phase0_macro_agent, virtual_agent, backtest_engine, ai_optimizer, h3_deep_dive_agent
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

# --- FUNKCJE POMOCNICZE DLA FAZ ---

def run_phase_1_cycle(session):
    """Uruchamia tylko Faze 0 (Makro) i Faze 1 (Skaner)."""
    try:
        logger.info("Starting Phase 1 Cycle (Macro + Scan)...")
        utils.append_scan_log(session, "Rozpoczynanie Fazy 1 (Skanowanie rynku)...")
        utils.update_system_control(session, 'worker_status', 'RUNNING')

        # Faza 0
        utils.update_system_control(session, 'current_phase', 'PHASE_0')
        macro_sentiment = phase0_macro_agent.run_macro_analysis(session, api_client)
        if macro_sentiment == 'RISK_OFF':
            utils.append_scan_log(session, "Faza 0: RISK_OFF. Skanowanie przerwane.")
            return

        # Faza 1 (czyścimy stare wyniki F1)
        session.execute(text("DELETE FROM phase1_candidates")) # Resetujemy kandydatów przed nowym skanem
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
    """Uruchamia tylko Faze 3 (H3 Live) na podstawie istniejących kandydatów."""
    try:
        logger.info("Starting Phase 3 Cycle (H3 Live)...")
        utils.append_scan_log(session, "Rozpoczynanie Fazy 3 (Szukanie sygnałów H3)...")
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        
        # Pobieramy kandydatów z bazy
        candidates_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        candidates = [row[0] for row in candidates_rows]
        
        if not candidates:
            utils.append_scan_log(session, "BŁĄD: Brak kandydatów z Fazy 1. Uruchom najpierw Skanowanie F1.")
            return

        utils.update_system_control(session, 'current_phase', 'PHASE_3_H3_LIVE')
        phase3_sniper.run_h3_live_scan(session, candidates, api_client)
        
        utils.append_scan_log(session, "Faza 3 zakończona.")
        
    except Exception as e:
        logger.error(f"Error in Phase 3 Cycle: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD Fazy 3: {e}")
    finally:
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

def run_full_analysis_cycle():
    """Uruchamia pełny cykl (F1 -> F3)."""
    with get_db_session() as session:
        run_phase_1_cycle(session)
        # Jeśli Faza 1 się udała (są kandydaci), Faza 3 pobierze ich z bazy
        run_phase_3_cycle(session)

# --- HANDLERY ZLECEŃ (Backtest, AI, Deep Dive) ---
# (Skopiuj te 3 funkcje: handle_backtest_request, handle_ai_optimizer_request, handle_h3_deep_dive_request
# z poprzedniej wersji main.py - one pozostają bez zmian, wklejam skrót dla oszczędności miejsca)
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

# --- MAIN LOOP ---
def main_loop():
    global current_state, api_client
    logger.info("Worker started.")
    with get_db_session() as session:
        Base.metadata.create_all(bind=engine)
        initialize_database_if_empty(session, api_client)
    
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    schedule.every(2).minutes.do(lambda: news_agent.run_news_agent_cycle(get_db_session(), api_client))
    schedule.every().day.at("23:00", "Europe/Warsaw").do(lambda: virtual_agent.run_virtual_trade_monitor(get_db_session(), api_client))

    with get_db_session() as initial_session:
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        utils.update_system_control(initial_session, 'worker_command', 'NONE')
        utils.report_heartbeat(initial_session)

    while True:
        with get_db_session() as session:
            try:
                # 1. Sprawdź komendy (F1 / F3 / FULL)
                run_action, new_state = utils.check_for_commands(session, current_state)
                current_state = new_state

                if run_action == "FULL_RUN":
                    run_full_analysis_cycle()
                elif run_action == "PHASE_1_RUN":
                    run_phase_1_cycle(session)
                elif run_action == "PHASE_3_RUN":
                    run_phase_3_cycle(session)
                
                # 2. Zadania Specjalne
                if handle_backtest_request(session, api_client) == 'BUSY': pass
                elif handle_ai_optimizer_request(session) == 'BUSY': pass
                elif handle_h3_deep_dive_request(session) == 'BUSY': pass
                
                # 3. Schedule i Heartbeat
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
    main_loop()
