import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone, timedelta 
from dotenv import load_dotenv
from sqlalchemy import text

# Importy bazodanowe (Unified)
from .models import Base, OptimizationJob 
from .database import get_db_session, engine
from .data_ingestion.data_initializer import initialize_database_if_empty
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .config import COMMAND_CHECK_INTERVAL_SECONDS

# Importy analityczne
from .analysis import (
    phase1_scanner, phase3_sniper, utils, news_agent,
    phase0_macro_agent, virtual_agent, backtest_engine, ai_optimizer, 
    h3_deep_dive_agent, signal_monitor, apex_optimizer, phasex_scanner, 
    biox_agent, recheck_agent, phase4_kinetic
)

# Konfiguracja Loggera
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)
load_dotenv()

# Sprawdzenie klucza API
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    logger.critical("ALPHAVANTAGE_API_KEY not found. Worker exiting.")
    sys.exit(1)

# Globalne instancje
api_client = AlphaVantageClient(api_key=API_KEY)
current_state = "IDLE" 

# === ZARZĄDCA STANU (RESOURCE GOVERNOR) ===
# Definiuje tryby pracy Workera
MODE_MONITORING = "MONITORING"   # Newsy + Tło (Niskie zużycie API)
MODE_OPERATION = "OPERATION"     # Skanery (F1, F3, F4, FX) / Optymalizacja (Wysokie zużycie API - Wyłączność)

active_mode = MODE_MONITORING 

def run_monitoring_tasks(session):
    """
    Tryb Wartownika: Utrzymuje przy życiu lekkie procesy tła.
    Działa w pętli głównej (jeden obrót na wywołanie).
    Przerywany natychmiast, gdy pojawi się zlecenie priorytetowe.
    """
    global current_state
    
    # 1. Zadania w tle (Schedule) - Newsy, Re-check, Wirtualny Portfel
    # Uruchamiamy je tylko w trybie monitoringu
    try:
        schedule.run_pending()
    except Exception as e:
        logger.error(f"Schedule Error: {e}")

def execute_high_priority_operation(session, operation_func, *args, **kwargs):
    """
    Tryb Operacji: "Odcięcie Tlenu".
    Zawiesza monitoring, wykonuje ciężkie zadanie (Skaner), a potem przywraca system.
    """
    global active_mode, current_state
    
    logger.info(">>> PRZEŁĄCZANIE TRYBU: MONITORING -> OPERACJA (High Priority)")
    active_mode = MODE_OPERATION
    
    utils.update_system_control(session, 'worker_status', 'BUSY_OPERATION')
    utils.append_scan_log(session, "SYSTEM: Wstrzymanie monitoringu. Start operacji priorytetowej...")
    
    start_time = time.time()
    
    try:
        # 2. Wykonaj Operację (Skan/Optymalizacja)
        operation_func(session, *args, **kwargs)
        
    except Exception as e:
        logger.error(f"Critical Operation Error: {e}", exc_info=True)
        utils.append_scan_log(session, f"BŁĄD KRYTYCZNY OPERACJI: {e}")
        
    finally:
        # 3. Przywróć system do życia (Resuscytacja)
        duration = time.time() - start_time
        logger.info(f"<<< OPERACJA ZAKOŃCZONA ({duration:.1f}s). POWRÓT DO MONITORINGU.")
        utils.append_scan_log(session, f"SYSTEM: Operacja zakończona. Wznawianie monitoringu.")
        
        active_mode = MODE_MONITORING
        current_state = "IDLE" # Reset stanu
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')

# === WRAPPERY ZADAŃ TŁA (Dla Schedule - Bezpieczeństwo) ===

def safe_run_news_agent():
    # Tylko w trybie monitoringu (żeby nie marnować API podczas skanu)
    if active_mode == MODE_MONITORING:
        with get_db_session() as session:
            try: news_agent.run_news_agent_cycle(session, api_client)
            except: pass

def safe_run_signal_monitor():
    if active_mode == MODE_MONITORING:
        with get_db_session() as session:
            try: signal_monitor.run_signal_monitor_cycle(session, api_client)
            except: pass

def safe_run_virtual_agent():
    # Ten agent może działać zawsze, bo operuje głównie na bazie danych
    with get_db_session() as session:
        try: virtual_agent.run_virtual_trade_monitor(session, api_client)
        except: pass

def safe_run_biox_monitor():
    if active_mode == MODE_MONITORING:
        with get_db_session() as session:
            try: biox_agent.run_biox_live_monitor(session, api_client)
            except: pass

def safe_run_recheck_audit():
    if active_mode == MODE_MONITORING:
        with get_db_session() as session:
            try: recheck_agent.run_recheck_audit_cycle(session)
            except: pass

# === OBSŁUGA ZLECEŃ (HANDLERS) ===

def run_phase_1_task(session):
    utils.update_system_control(session, 'current_phase', 'PHASE_1_SCAN')
    phase0_macro_agent.run_macro_analysis(session, api_client)
    session.execute(text("DELETE FROM phase1_candidates"))
    session.commit()
    phase1_scanner.run_scan(session, lambda: "RUNNING", api_client)

def run_phase_3_task(session):
    utils.update_system_control(session, 'current_phase', 'PHASE_3_SNIPER')
    params_json = utils.get_system_control_value(session, 'h3_live_parameters')
    params = json.loads(params_json) if params_json else {}
    
    # Pobieramy kandydatów z Fazy 1
    candidates = [r[0] for r in session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()]
    
    # Fallback: Jeśli F1 pusta, weź próbkę z companies
    if not candidates: 
        candidates = [r[0] for r in session.execute(text("SELECT ticker FROM companies LIMIT 50")).fetchall()]
        
    phase3_sniper.run_h3_live_scan(session, candidates, api_client, parameters=params)

def run_phase_x_task(session):
    utils.update_system_control(session, 'current_phase', 'PHASE_X_SCAN')
    cands = phasex_scanner.run_phasex_scan(session, api_client)
    # Po znalezieniu kandydatów, uruchom audit historyczny
    biox_agent.run_historical_catalyst_scan(session, api_client, candidates=cands)

def run_phase_4_task(session):
    utils.update_system_control(session, 'current_phase', 'PHASE_4_KINETIC')
    phase4_kinetic.run_phase4_scan(session, api_client)

def run_backtest_task(session):
    req = utils.get_system_control_value(session, 'backtest_request')
    params = json.loads(utils.get_system_control_value(session, 'backtest_parameters') or '{}')
    utils.update_system_control(session, 'current_phase', 'BACKTESTING')
    backtest_engine.run_historical_backtest(session, api_client, req, parameters=params)
    utils.update_system_control(session, 'backtest_request', 'NONE')

def run_ai_optimizer_task(session):
    utils.update_system_control(session, 'current_phase', 'AI_ANALYSIS')
    ai_optimizer.run_ai_optimization_analysis(session)
    utils.update_system_control(session, 'ai_optimizer_request', 'NONE')

def run_h3_deep_dive_task(session):
    req = utils.get_system_control_value(session, 'h3_deep_dive_request')
    utils.update_system_control(session, 'current_phase', 'DEEP_DIVE')
    h3_deep_dive_agent.run_h3_deep_dive_analysis(session, int(req))
    utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')

def run_optimization_task(session):
    job_id = utils.get_system_control_value(session, 'optimization_request')
    utils.update_system_control(session, 'current_phase', 'QUANTUM_OPT')
    job = session.query(OptimizationJob).filter(OptimizationJob.id == job_id).first()
    if job:
        # Optimizer tworzy własną sesję wewnątrz, przekazujemy tylko ID
        optimizer = apex_optimizer.QuantumOptimizer(session, job_id, job.target_year)
        optimizer.run(n_trials=job.total_trials)
    utils.update_system_control(session, 'optimization_request', 'NONE')


# === GŁÓWNA PĘTLA WORKERA ===

def main_loop():
    global current_state, api_client, active_mode
    logger.info("Worker V6.0 (Resource Governor) STARTED.")
    
    # Inicjalizacja bazy
    try:
        with get_db_session() as session:
            initialize_database_if_empty(session, api_client)
            utils.append_scan_log(session, "SYSTEM: Worker Uruchomiony. Tryb: MONITORING.")
            # Reset flagi pauzy na starcie
            utils.update_system_control(session, 'worker_status', 'IDLE')
    except Exception as e:
        logger.error(f"Startup Error: {e}")
        time.sleep(5)

    # Harmonogram zadań tła (działają tylko w trybie Monitoring)
    schedule.every(2).minutes.do(safe_run_news_agent)
    schedule.every(10).seconds.do(safe_run_signal_monitor) # Częste sprawdzanie sygnałów
    schedule.every(5).minutes.do(safe_run_biox_monitor)
    schedule.every(15).minutes.do(safe_run_recheck_audit)
    schedule.every().day.at("23:00", "Europe/Warsaw").do(safe_run_virtual_agent)

    while True:
        with get_db_session() as session:
            try:
                # 1. Sprawdź, czy są jakieś ROZKAZY od użytkownika (Priorytet Absolutny)
                cmd = utils.get_system_control_value(session, 'worker_command')
                
                # Zmienne pomocnicze do wykrywania zleceń asynchronicznych
                backtest_req = utils.get_system_control_value(session, 'backtest_request')
                ai_req = utils.get_system_control_value(session, 'ai_optimizer_request')
                deep_dive_req = utils.get_system_control_value(session, 'h3_deep_dive_request')
                opt_req = utils.get_system_control_value(session, 'optimization_request')

                operation_to_run = None
                
                # A. Mapowanie komend na funkcje (Explicit Commands)
                if cmd == "START_PHASE_1_REQUESTED": operation_to_run = run_phase_1_task
                elif cmd == "START_PHASE_3_REQUESTED": operation_to_run = run_phase_3_task
                elif cmd == "START_PHASE_X_REQUESTED": operation_to_run = run_phase_x_task
                elif cmd == "START_PHASE_4_REQUESTED": operation_to_run = run_phase_4_task
                
                # B. Mapowanie żądań analitycznych (Async Jobs)
                elif backtest_req and backtest_req not in ['NONE', 'PROCESSING']: operation_to_run = run_backtest_task
                elif ai_req == 'REQUESTED': operation_to_run = run_ai_optimizer_task
                elif deep_dive_req and deep_dive_req not in ['NONE', 'PROCESSING']: operation_to_run = run_h3_deep_dive_task
                elif opt_req and opt_req not in ['NONE', 'PROCESSING']: operation_to_run = run_optimization_task

                elif cmd == "PAUSE_REQUESTED":
                    utils.update_system_control(session, 'worker_status', 'PAUSED')
                    utils.update_system_control(session, 'worker_command', 'NONE')
                    utils.append_scan_log(session, "SYSTEM: Zatrzymano pracę (PAUSE).")
                    time.sleep(2)
                    continue # Pomiń resztę pętli, czekaj na wznowienie

                elif cmd == "RESUME_REQUESTED":
                    utils.update_system_control(session, 'worker_status', 'IDLE')
                    utils.update_system_control(session, 'worker_command', 'NONE')
                    utils.append_scan_log(session, "SYSTEM: Wznowiono pracę.")
                    continue

                # 2. DECYZJA: ALBO OPERACJA, ALBO MONITORING
                if operation_to_run:
                    # Czyścimy flagę komendy, aby nie uruchomić jej dwa razy
                    if cmd and "REQUESTED" in cmd:
                        utils.update_system_control(session, 'worker_command', 'NONE')
                    
                    # Wykonujemy operację z "Odcięciem Tlenu"
                    execute_high_priority_operation(session, operation_to_run)
                
                else:
                    # Brak zadań specjalnych -> Działa WARTOWNIK (Tło)
                    current_status_val = utils.get_system_control_value(session, 'worker_status')
                    
                    # Jeśli nie jesteśmy spauzowani ani zajęci inną operacją
                    if current_status_val != 'PAUSED' and not str(current_status_val).startswith('BUSY'):
                        run_monitoring_tasks(session)
                    
                # Raportowanie życia workera
                utils.report_heartbeat(session)

            except Exception as e:
                logger.error(f"Main Loop Error: {e}", exc_info=True)
                time.sleep(5) # Odczekaj chwilę po błędzie krytycznym

        # Krótki sleep, żeby nie zarżnąć CPU
        time.sleep(0.5)

if __name__ == "__main__":
    main_loop()
