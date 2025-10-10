import logging
import sys
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List
from fastapi.middleware.cors import CORSMiddleware  # Import modułu CORS

from . import crud, models, schemas
from .database import get_db, engine, SessionLocal

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

# Utworzenie tabel w bazie danych, jeśli nie istnieją
try:
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully.")
except Exception as e:
    logger.critical(f"Failed to create database tables: {e}")
    sys.exit(1)

app = FastAPI(title="APEX Predator API", version="1.0.0")

# --- POCZĄTEK SEKCJI CORS ---
# Definicja adresów, które mogą odpytywać to API
origins = [
    "https://apex-predator-frontend.onrender.com",  # Adres Twojego frontendu na Render
    "http://localhost",
    "http://localhost:8080", # Przykładowy adres do lokalnego developmentu
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # Pozwól na zapytania z tych adresów
    allow_credentials=True,
    allow_methods=["*"],  # Pozwól na wszystkie metody (GET, POST, etc.)
    allow_headers=["*"],  # Pozwól na wszystkie nagłówki
)
# --- KONIEC SEKCJI CORS ---


@app.on_event("startup")
async def startup_event():
    # Inicjalizacja początkowych wartości w tabeli system_control, jeśli nie istnieją
    db = SessionLocal()
    try:
        initial_values = {
            'worker_status': 'IDLE',
            'worker_command': 'NONE',
            'current_phase': 'NONE',
            'scan_progress_processed': '0',
            'scan_progress_total': '0',
            'scan_log': 'Czekam na rozpoczęcie skanowania...',
            'last_heartbeat': datetime.now(timezone.utc).isoformat()
        }
        for key, value in initial_values.items():
            if not crud.get_system_control_value(db, key):
                crud.set_system_control_value(db, key, value)
        logger.info("Initial system control values verified.")
    except Exception as e:
        logger.error(f"Could not initialize system_control values: {e}")
    finally:
        db.close()


@app.post("/api/v1/worker/control/{action}", status_code=202, summary="Sterowanie Silnikiem Analitycznym")
def control_worker(action: str, db: Session = Depends(get_db)):
    """Wysyła polecenie sterujące do workera (start, pause, resume)."""
    allowed_actions = {
        "start": "START_REQUESTED",
        "pause": "PAUSE_REQUESTED",
        "resume": "RESUME_REQUESTED"
    }
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail=f"Invalid action. Allowed: {', '.join(allowed_actions.keys())}")
    
    command = allowed_actions[action]
    crud.set_system_control_value(db, key="worker_command", value=command)
    logger.info(f"Command '{action}' ({command}) sent to worker.")
    return {"message": f"Command '{action}' sent to worker."}

@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus, summary="Pobieranie Statusu Workera")
def get_worker_status(db: Session = Depends(get_db)):
    """Pobiera aktualny status, postęp i logi workera analitycznego."""
    try:
        status = crud.get_system_control_value(db, "worker_status") or "UNKNOWN"
        phase = crud.get_system_control_value(db, "current_phase") or "NONE"
        processed = int(crud.get_system_control_value(db, "scan_progress_processed") or 0)
        total = int(crud.get_system_control_value(db, "scan_progress_total") or 1)
        log = crud.get_system_control_value(db, "scan_log") or ""
        heartbeat_str = crud.get_system_control_value(db, "last_heartbeat")

        heartbeat_dt = datetime.fromisoformat(heartbeat_str) if heartbeat_str else datetime.now(timezone.utc)

        return {
            "status": status,
            "phase": phase,
            "progress": {"processed": processed, "total": total},
            "last_heartbeat_utc": heartbeat_dt,
            "log": log
        }
    except Exception as e:
        logger.error(f"Error fetching worker status: {e}")
        raise HTTPException(status_code=500, detail="Could not fetch worker status from the database.")


@app.get("/api/v1/signals/apex-elita", response_model=List[schemas.TradingSignal], summary="Pobieranie Aktywnych Sygnałów")
def get_apex_elita_signals(db: Session = Depends(get_db)):
    """Pobiera listę wszystkich aktywnych sygnałów transakcyjnych z APEX Elita."""
    try:
        signals = crud.get_active_signals(db)
        return signals
    except Exception as e:
        logger.error(f"Error fetching active signals: {e}")
        raise HTTPException(status_code=500, detail="Could not fetch active signals.")
