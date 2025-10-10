import logging
import sys
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict
from fastapi.middleware.cors import CORSMiddleware

from . import crud, models, schemas
from .database import get_db, engine, SessionLocal

# ... (reszta importów i konfiguracji bez zmian) ...
logging.basicConfig(level=logging.INFO, format='%(asctime=s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

try:
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully.")
except Exception as e:
    logger.critical(f"Failed to create database tables: {e}")
    sys.exit(1)

app = FastAPI(title="APEX Predator API", version="1.0.0")

origins = [
    "https://apex-predator-frontend.onrender.com",
    "http://localhost",
    "http://localhost:8080",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ... (@app.on_event("startup"), control_worker, get_worker_status, get_apex_elita_signals bez zmian) ...
@app.post("/api/v1/worker/control/{action}", status_code=202, summary="Sterowanie Silnikiem Analitycznym")
def control_worker(action: str, db: Session = Depends(get_db)):
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    logger.info(f"Command '{action}' ({command}) sent to worker.")
    return {"message": f"Command '{action}' sent to worker."}

@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus, summary="Pobieranie Statusu Workera")
def get_worker_status(db: Session = Depends(get_db)):
    try:
        status = crud.get_system_control_value(db, "worker_status") or "UNKNOWN"
        phase = crud.get_system_control_value(db, "current_phase") or "NONE"
        processed = int(crud.get_system_control_value(db, "scan_progress_processed") or 0)
        total = int(crud.get_system_control_value(db, "scan_progress_total") or 1)
        log = crud.get_system_control_value(db, "scan_log") or ""
        heartbeat_str = crud.get_system_control_value(db, "last_heartbeat")
        heartbeat_dt = datetime.fromisoformat(heartbeat_str) if heartbeat_str else datetime.now(timezone.utc)
        return {"status": status, "phase": phase, "progress": {"processed": processed, "total": total}, "last_heartbeat_utc": heartbeat_dt, "log": log}
    except Exception as e:
        logger.error(f"Error fetching worker status: {e}")
        raise HTTPException(status_code=500, detail="Could not fetch worker status from the database.")


@app.get("/api/v1/signals/apex-elita", response_model=List[schemas.TradingSignal], summary="Pobieranie Aktywnych Sygnałów")
def get_apex_elita_signals(db: Session = Depends(get_db)):
    try:
        return crud.get_active_signals(db)
    except Exception as e:
        logger.error(f"Error fetching active signals: {e}")
        raise HTTPException(status_code=500, detail="Could not fetch active signals.")

# --- ZAKTUALIZOWANE ENDPOINTY DO ANALIZY NA ŻĄDANIE ---

class TickerRequest(schemas.BaseModel):
    ticker: str

@app.post("/api/v1/analysis/on-demand", status_code=202, summary="Zlecenie Analizy Spółki")
def request_on_demand_analysis(request: TickerRequest, db: Session = Depends(get_db)):
    """Przyjmuje ticker i zapisuje go w bazie jako zadanie dla workera."""
    ticker = request.ticker.upper()
    logger.info(f"Received on-demand analysis request for {ticker}. Forwarding to worker.")
    
    # Zapisz polecenie dla workera
    crud.set_system_control_value(db, "on_demand_request", ticker)
    
    return {"message": f"Analysis request for {ticker} has been accepted and is being processed."}

@app.get("/api/v1/analysis/on-demand/result/{ticker}", summary="Pobieranie Wyników Analizy")
def get_on_demand_analysis_result(ticker: str, db: Session = Depends(get_db)) -> Optional[Dict[str, Any]]:
    """Sprawdza i zwraca gotowe wyniki analizy na żądanie."""
    ticker = ticker.upper()
    logger.info(f"Checking for on-demand analysis result for {ticker}.")
    
    result = crud.get_on_demand_result(db, ticker)
    
    if result:
        # Jeśli znaleziono wynik, usuń polecenie, aby worker go nie powtarzał
        crud.set_system_control_value(db, "on_demand_request", "NONE")
        return result
    
    # Jeśli nie ma wyniku, zwróć pustą odpowiedź, co frontend zinterpretuje jako "w toku"
    return None

