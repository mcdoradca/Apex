import os
import time
import logging
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Import modułów analitycznych
from .analysis import phase1_scanner, phase2_engine, phase3_sniper
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .config import COMMAND_CHECK_INTERVAL_SECONDS

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Ładowanie zmiennych środowiskowych z pliku .env
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not DATABASE_URL or not ALPHAVANTAGE_API_KEY:
    raise ValueError("DATABASE_URL and ALPHAVANTAGE_API_KEY environment variables must be set.")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Wewnętrzny stan workera, niezależny od bazy danych
current_state = "IDLE"

def get_db_session():
    return SessionLocal()

def update_system_control(session, key, value):
    """Ustandaryzowana funkcja do aktualizacji tabeli system_control."""
    try:
        stmt = text("""
            INSERT INTO system_control (key, value, updated_at)
            VALUES (:key, :value, :now)
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at;
        """)
        session.execute(stmt, {'key': key, 'value': str(value), 'now': datetime.now(timezone.utc)})
        session.commit()
    except Exception as e:
        session.rollback()
        logging.error(f"Error updating system_control for key {key}: {e}")

def check_for_commands(session):
    """Sprawdza i reaguje na polecenia z bazy danych."""
    global current_state
    try:
        command_row = session.execute(text("SELECT value FROM system_control WHERE key = 'worker_command'")).fetchone()
        command = command_row[0] if command_row else 'NONE'

        if command == "PAUSE_REQUESTED" and current_state == "RUNNING":
            current_state = "PAUSED"
            update_system_control(session, 'worker_status', 'PAUSED')
            update_system_control(session, 'worker_command', 'NONE') # Reset polecenia
            logging.info("Worker paused by command.")

        elif command == "RESUME_REQUESTED" and current_state == "PAUSED":
            current_state = "RUNNING"
            update_system_control(session, 'worker_status', 'RUNNING')
            update_system_control(session, 'worker_command', 'NONE') # Reset polecenia
            logging.info("Worker resumed by command.")

        elif command == "START_REQUESTED" and current_state == "IDLE":
            logging.info("Start command received, triggering immediate analysis cycle.")
            update_system_control(session, 'worker_command', 'NONE') # Reset polecenia
            return True # Zwracamy True, aby główna pętla wiedziała, że ma uruchomić cykl
        
    except Exception as e:
        logging.error(f"Error checking for commands: {e}")

    return False

def report_heartbeat(session):
    """Raportuje 'życie' workera do bazy danych."""
    now_utc_iso = datetime.now(timezone.utc).isoformat()
    update_system_control(session, 'last_heartbeat', now_utc_iso)

def run_full_analysis_cycle(api_client: AlphaVantageClient):
    """Główna funkcja orkiestrująca cały proces analityczny APEX."""
    global current_state
    session = get_db_session()

    status_row = session.execute(text("SELECT value FROM system_control WHERE key = 'worker_status'")).fetchone()
    if status_row and status_row[0] == 'RUNNING':
        logging.warning("Analysis cycle already in progress. Skipping run.")
        session.close()
        return

    try:
        logging.info(f"[{datetime.now()}] Starting full analysis cycle...")
        current_state = "RUNNING"
        update_system_control(session, 'worker_status', 'RUNNING')
        update_system_control(session, 'scan_log', f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] Rozpoczynanie cyklu analizy...")

        # --- FAZA 1 ---
        update_system_control(session, 'current_phase', 'PHASE_1')
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            raise Exception("Phase 1 did not yield any candidates.")

        # --- FAZA 2 ---
        update_system_control(session, 'current_phase', 'PHASE_2')
        qualified_tickers = phase2_engine.run_analysis(session, candidate_tickers, lambda: current_state, api_client)
        if not qualified_tickers:
            raise Exception("Phase 2 did not qualify any tickers for APEX Elita.")

        # --- FAZA 3 ---
        update_system_control(session, 'current_phase', 'PHASE_3')
        phase3_sniper.run_tactical_planning(session, qualified_tickers, lambda: current_state, api_client)
        
        logging.info(f"[{datetime.now()}] Full analysis cycle completed successfully.")
        update_system_control(session, 'scan_log', f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] Cykl analizy zakończony pomyślnie.")

    except Exception as e:
        error_message = f"An error occurred during analysis: {e}"
        logging.error(error_message, exc_info=True)
        update_system_control(session, 'worker_status', 'ERROR')
        update_system_control(session, 'scan_log', f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] BŁĄD: {e}")

    finally:
        current_state = "IDLE"
        update_system_control(session, 'worker_status', 'IDLE')
        update_system_control(session, 'current_phase', 'NONE')
        update_system_control(session, 'scan_progress_processed', '0')
        update_system_control(session, 'scan_progress_total', '0')
        session.close()

if __name__ == "__main__":
    logging.info("Worker started. Initializing...")
    
    # Inicjalizacja klienta API
    api_client = AlphaVantageClient(api_key=ALPHAVANTAGE_API_KEY)

    # USUNIĘTO AUTOMATYCZNY HARMONOGRAM URUCHAMIANIA ANALIZY.
    # Aplikacja czeka teraz na polecenie "START_REQUESTED" z API.
    logging.info("Worker is in manual mode. Waiting for 'start' command from the API.")
    
    # Inicjalizacja stanu w bazie danych przy starcie
    with get_db_session() as initial_session:
        update_system_control(initial_session, 'worker_status', 'IDLE')
        update_system_control(initial_session, 'worker_command', 'NONE')
        update_system_control(initial_session, 'current_phase', 'NONE')

    # Główna pętla sterująca
    logging.info("Entering main control loop...")
    while True:
        with get_db_session() as session:
            try:
                # Sprawdź polecenia i jeśli nadszedł START_REQUESTED, uruchom natychmiast
                if check_for_commands(session):
                    run_full_analysis_cycle(api_client=api_client)
                
                report_heartbeat(session)
            except Exception as loop_error:
                logging.error(f"Error in main worker loop: {loop_error}")
            
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS) # Używamy interwału z pliku config

