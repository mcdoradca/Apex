import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone, timedelta 
from dotenv import load_dotenv
from sqlalchemy import text

# === IMPORTY BAZODANOWE (Unified) ===
from .models import Base, OptimizationJob 
from .database import get_db_session, engine
from .data_ingestion.data_initializer import initialize_database_if_empty
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .config import COMMAND_CHECK_INTERVAL_SECONDS

# === IMPORTY ANALITYCZNE (Moduły Strategii) ===
from .analysis import (
    phase1_scanner, phase3_sniper, utils, news_agent,
    phase0_macro_agent, virtual_agent, backtest_engine, ai_optimizer, 
    h3_deep_dive_agent, signal_monitor, apex_optimizer, phasex_scanner, 
    biox_agent, recheck_agent, phase4_kinetic, phase_sdar
)

# Konfiguracja Loggera
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)
load_dotenv()

# Sprawdzenie klucza API (Krytyczne dla działania Alpha Vantage)
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    logger.critical("ALPHAVANTAGE_API_KEY not found. Worker exiting.")
    sys.exit(1)

# Globalne instancje
api_client = AlphaVantageClient(api_key=API_KEY)
current_state = "IDLE" 

# === ZARZĄDCA STANU (RESOURCE GOVERNOR) ===
# Definiuje tryby pracy Workera w celu ochrony limitów API (Traffic Shaping)
MODE_MONITORING = "MONITORING"   # Newsy + Tło (Niskie zużycie API - nasłuchiwanie)
MODE_OPERATION = "OPERATION"     # Skanery (F1, F3, F4, FX) / Optymalizacja (Wysokie zużycie - Wyłączność)

active_mode = MODE_MONITORING 

def run_monitoring_tasks(session):
    """
    Tryb Wartownika: Utrzymuje przy życiu lekkie procesy tła.
    Działa w pętli głównej (jeden obrót na wywołanie).
    Przerywany natychmiast, gdy pojawi się zlecenie priorytetowe.
    """
    global current_state
    
    # 1. Zadania w tle (Schedule) - Newsy, Re-check, Wirtualny Portfel
    # Uruchamiamy je tylko w trybie monitoringu, aby nie zatykać kolejki API podczas skanowania
    try:
        schedule.run_pending()
    except Exception as e:
        logger.error(f"Schedule Error: {e}")

def execute_high_priority_operation(session, operation_func, *args, **kwargs):
    """
    Tryb Operacji: "Odcięcie Tlenu" dla tła.
    Zawiesza monitoring, wykonuje ciężkie zadanie (Skaner), a potem przywraca system.
    Zapewnia, że Skaner ma 100% dostępnych zasobów API.
    """
    global active_mode, current_state
    
    logger.info(">>> PRZEŁĄCZANIE TRYBU: MONITORING -> OPERACJA (High Priority)")
    active_mode = MODE_OPERATION
    
    utils.update_system_control(session, 'worker_status', 'BUSY_OPERATION')
    utils.append_scan_log(session, "SYSTEM: Wstrzymanie monitoringu. Start operacji priorytetowej...")
    
    start_time = time.time()
    
    try:
        # 2. Wykonaj Operację (Skan/Optymalizacja/SDAR)
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
            try: 
                news_agent.run_news_agent_cycle(session, api_client)
            except Exception as e:
                # Logujemy błędy, zamiast je połykać, aby wiedzieć dlaczego alerty nie dochodzą
                logger.error(f"News Agent Error (Schedule): {e}", exc_info=True)

def safe_run_signal_monitor():
    if active_mode == MODE_MONITORING:
        with get_db_session() as session:
            try: signal_monitor.run_signal_monitor_cycle(session, api_client)
            except: pass

def safe_run_virtual_agent():
    # Ten agent może działać zawsze, bo operuje głównie na bazie danych (Virtual Portfolio)
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
    """
    Uruchamia Fazę 1 (Skaner Rynku).
    NOWOŚĆ: Zintegrowana z Fazą 0 (Bezpiecznik Nasdaq).
    """
    utils.update_system_control(session, 'current_phase', 'PHASE_1_SCAN')
    
    # === KROK 0: BEZPIECZNIK NASDAQ (Guardrail) ===
    # Zanim wydamy zasoby na skanowanie setek spółek, sprawdzamy czy rynek w ogóle pozwala na handel.
    # Jeśli QQQ jest pod SMA200 (Bessa) -> przerywamy.
    market_status = phase0_macro_agent.run_macro_analysis(session, api_client)
    
    if market_status == "RISK_OFF":
        msg = "⛔ SKAN FAZY 1 PRZERWANY: Nasdaq (QQQ) jest w trybie RISK_OFF (Bessa/Wysokie Stopy)."
        logger.warning(msg)
        utils.append_scan_log(session, msg)
        return # STOP - Oszczędzamy API i kapitał
        
    # Jeśli RISK_ON -> kontynuujemy normalny skan
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

def run_sdar_task(session):
    """
    Uruchamia System Detekcji Anomalii Rynkowych (SDAR - Nowa Idea).
    Bada korelację sentymentu (News) i wolumenu (SAI) dla najlepszych kandydatów.
    
    WAŻNE: Respektuje flagę RISK_OFF z Fazy 0. Nie szukamy perełek w trakcie tsunami.
    """
    utils.update_system_control(session, 'current_phase', 'SDAR_ANOMALY_HUNT')
    
    # 1. Sprawdzenie bezpiecznika globalnego (zapisanego w bazie przez F0)
    market_status = utils.get_system_control_value(session, 'market_status')
    
    # 2. Jeśli status nieznany lub stary, odśwież go "na gorąco"
    if not market_status:
        market_status = phase0_macro_agent.run_macro_analysis(session, api_client)

    # 3. Decyzja Strategiczna
    if market_status == 'RISK_OFF':
        msg = "⛔ SDAR ZABLOKOWANY: Nasdaq jest w trybie RISK_OFF. Szukanie Longów jest zbyt ryzykowne."
        logger.warning(msg)
        utils.append_scan_log(session, msg)
        return

    # 4. Uruchomienie Silnika (tylko w trybie RISK_ON)
    analyzer = phase_sdar.SDARAnalyzer(session, api_client)
    # ZMIANA: Zdjęcie limitu 50 spółek - Skanujemy wszystko z Fazy 1
    analyzer.run_sdar_cycle(limit=None) 

def run_backtest_task(session):
    req = utils.get_system_control_value(session, 'backtest_request')
    params = json.loads(utils.get_system_control_value(session, 'backtest_parameters') or '{}')
    utils.update_system_control(session, 'current_phase', 'BACKTESTING')
    backtest_engine.run_historical_backtest(session, api_client, req, parameters=params)
    utils.update_system_control(session, 'backtest_request', 'NONE')

def run_ai_optimizer_task(session):
    # DISABLED: Funkcja wyłączona (Usunięcie zależności Gemini/LLM)
    utils.update_system_control(session, 'current_phase', 'AI_ANALYSIS_DISABLED')
    # ai_optimizer.run_ai_optimization_analysis(session)
    utils.update_system_control(session, 'ai_optimizer_request', 'NONE')
    logger.info("Skipping AI Optimizer Task (AI Agents Disabled).")

def run_h3_deep_dive_task(session):
    # DISABLED: Funkcja wyłączona (Usunięcie zależności Gemini/LLM)
    # req = utils.get_system_control_value(session, 'h3_deep_dive_request')
    utils.update_system_control(session, 'current_phase', 'DEEP_DIVE_DISABLED')
    # h3_deep_dive_agent.run_h3_deep_dive_analysis(session, int(req))
    utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')
    logger.info("Skipping Deep Dive Task (AI Agents Disabled).")

def run_optimization_task(session):
    job_id = utils.get_system_control_value(session, 'optimization_request')
    utils.update_system_control(session, 'current_phase', 'QUANTUM_OPT')
    job = session.query(OptimizationJob).filter(OptimizationJob.id == job_id).first()
    if job:
        # Optimizer tworzy własną sesję wewnątrz, przekazujemy tylko ID
        optimizer = apex_optimizer.QuantumOptimizer(session, job_id, job.target_year)
        optimizer.run(n_trials=job.total_trials)
    utils.update_system_control(session, 'optimization_request', 'NONE')


# === GŁÓWNA PĘTLA WORKERA (THE HEARTBEAT) ===

def main_loop():
    global current_state, api_client, active_mode
    logger.info("Worker V6.3 (Real Money Nasdaq Edition) STARTED.")
    
    # Inicjalizacja bazy i systemu
    try:
        with get_db_session() as session:
            initialize_database_if_empty(session, api_client)
            utils.append_scan_log(session, "SYSTEM: Worker Uruchomiony. Tryb: REAL MONEY (SDAR + Nasdaq Guard).")
            # Reset flagi pauzy na starcie
            utils.update_system_control(session, 'worker_status', 'IDLE')
            
            # Startowy check makro (żeby system wiedział od razu, co się dzieje na Nasdaq)
            phase0_macro_agent.run_macro_analysis(session, api_client)
            
    except Exception as e:
        logger.error(f"Startup Error: {e}")
        time.sleep(5)

    # === HARMONOGRAM ZADAŃ (Background Jobs) ===
    # Zadania działają tylko w trybie MONITORING, aby nie kolidować ze skanami
    
    # Newsy co 5 minut (zgodnie z limitem zapytań)
    schedule.every(5).minutes.do(safe_run_news_agent)
    
    # Monitor sygnałów (bardzo częsty, dla szybkiej reakcji)
    schedule.every(10).seconds.do(safe_run_signal_monitor)
    
    # Inne monitory
    schedule.every(5).minutes.do(safe_run_biox_monitor)
    schedule.every(15).minutes.do(safe_run_recheck_audit)
    
    # Odświeżanie portfela co minutę
    schedule.every(1).minutes.do(safe_run_virtual_agent) 

    while True:
        with get_db_session() as session:
            try:
                # 1. Sprawdź, czy są jakieś ROZKAZY od użytkownika (Priorytet Absolutny)
                cmd = utils.get_system_control_value(session, 'worker_command')
                
                # Zmienne pomocnicze do wykrywania zleceń asynchronicznych (UI Requests)
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
                elif cmd == "START_SDAR_REQUESTED": operation_to_run = run_sdar_task # TRIGGER SDAR
                
                # B. Mapowanie żądań analitycznych (Async Jobs)
                elif backtest_req and backtest_req not in ['NONE', 'PROCESSING']: operation_to_run = run_backtest_task
                
                # Wyłączone funkcje AI (Stuby)
                elif ai_req == 'REQUESTED': 
                    operation_to_run = run_ai_optimizer_task # Uruchomi wersję DISABLED
                elif deep_dive_req and deep_dive_req not in ['NONE', 'PROCESSING']: 
                    operation_to_run = run_h3_deep_dive_task # Uruchomi wersję DISABLED
                
                elif opt_req and opt_req not in ['NONE', 'PROCESSING']: operation_to_run = run_optimization_task

                # C. Obsługa Pauzy
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
                    
                    # Wykonujemy operację z "Odcięciem Tlenu" (Tryb OPERATION)
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

        # Krótki sleep, żeby nie zarżnąć CPU w pętli while
        time.sleep(0.5)

if __name__ == "__main__":
    main_loop()
