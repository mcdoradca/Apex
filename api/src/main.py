import logging
import sys
import json
from fastapi import FastAPI, Depends, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List

from . import crud, models, schemas
from .database import get_db, engine, SessionLocal

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

try:
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully.")
except Exception as e:
    logger.critical(f"Failed to create database tables: {e}")
    sys.exit(1)

app = FastAPI(title="APEX Predator API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://apex-predator-frontend.onrender.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    db = SessionLocal()
    try:
        initial_values = {
            'worker_status': 'IDLE', 'worker_command': 'NONE', 'current_phase': 'NONE',
            'scan_progress_processed': '0', 'scan_progress_total': '0',
            'scan_log': 'Czekam na rozpoczęcie skanowania...',
            'last_heartbeat': datetime.now(timezone.utc).isoformat(),
            'on_demand_request': 'NONE'
        }
        for key, value in initial_values.items():
            if not crud.get_system_control_value(db, key):
                crud.set_system_control_value(db, key, value)
        logger.info("Initial system control values verified.")
    except Exception as e:
        logger.error(f"Could not initialize system_control values: {e}")
    finally:
        db.close()

@app.post("/api/v1/worker/control/{action}", status_code=202)
@app.post("/api/v1/worker/control/{action}", status_code=202, summary="Sterowanie Silnikiem Analitycznym")
def control_worker(action: str, db: Session = Depends(get_db)):
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    # POPRAWKA: Definicja zmiennej command
    command = allowed_actions[action]
    crud.set_system_control_value(db, "worker_command", command)
    logger.info(f"Command '{action}' ({command}) sent to worker.")
    return {"message": f"Command '{action}' sent to worker."}

@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus, summary="Pobieranie Statusu Workera")
def get_worker_status(db: Session = Depends(get_db)):
    """Retrieves the current status, progress, and logs of the analysis worker."""
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

@app.post("/api/v1/analysis/on-demand", status_code=202)
def request_on_demand_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    """Accepts a request for on-demand analysis and forwards it to the worker via the database."""
    ticker = request.ticker.strip().upper()
    # POPRAWKA: Zmiana formatowania logu w celu uniknięcia konfliktu
    logger.info("Received on-demand analysis request for %s. Forwarding to worker.", ticker)
    crud.set_system_control_value(db, key="on_demand_request", value=ticker)
    return {"message": f"Analysis request for {ticker} accepted and queued."}

@app.get("/api/v1/analysis/on-demand/result/{ticker}")
def get_on_demand_result(ticker: str, db: Session = Depends(get_db)):
    """Polls for the result of a previously requested on-demand analysis."""
    result = crud.get_on_demand_result(db, ticker.strip().upper())
    if not result:
        return Response(status_code=204) # No content yet, tell the client to keep polling
    
    # POPRAWKA: Parsowanie stringa JSON z bazy danych na obiekt JSON
    return json.loads(result.analysis_data)

@app.get("/api/v1/signals/apex-elita", response_model=List[schemas.TradingSignal])
def get_apex_elita_signals(db: Session = Depends(get_db)):
    """Retrieves a list of all active trading signals from the APEX Elite."""
    try:
        return crud.get_active_signals(db)
    except Exception as e:
        logger.error(f"Error fetching active signals: {e}")
        raise HTTPException(status_code=500, detail="Could not fetch active signals.")



