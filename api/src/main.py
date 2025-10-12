import logging
import sys
from fastapi import FastAPI, Depends, HTTPException, Response
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List

# Zmiana nazwy `CORSMiddleware` na `AllowAllCORSMiddleware`, aby uniknąć konfliktu
from fastapi.middleware.cors import CORSMiddleware as AllowAllCORSMiddleware

from . import crud, models, schemas
from .database import get_db, engine, SessionLocal

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

try:
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to create database tables: {e}", exc_info=True)
    sys.exit(1)

app = FastAPI(title="APEX Predator API", version="1.0.0")

app.add_middleware(
    AllowAllCORSMiddleware,
    allow_origins=["*"], # Zezwól na wszystkie źródła
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    # ... reszta funkcji bez zmian ...
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
            if crud.get_system_control_value(db, key) is None:
                crud.set_system_control_value(db, key, value)
        logger.info("Initial system control values verified and seeded if necessary.")
    except Exception as e:
        logger.error(f"Could not initialize system_control values: {e}", exc_info=True)
    finally:
        db.close()


# --- NOWE ENDPOINTY DLA KANDYDATÓW FAZY 1 ---
@app.get("/api/v1/candidates/phase1", response_model=List[schemas.Phase1Candidate])
def get_phase1_candidates_endpoint(db: Session = Depends(get_db)):
    try:
        return crud.get_phase1_candidates(db)
    except Exception as e:
        logger.error(f"Error fetching phase 1 candidates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not fetch phase 1 candidates.")

@app.delete("/api/v1/candidates/phase1/{ticker}", status_code=200)
def delete_phase1_candidate_endpoint(ticker: str, db: Session = Depends(get_db)):
    try:
        return crud.delete_phase1_candidate(db, ticker.strip().upper())
    except Exception as e:
        logger.error(f"Error deleting phase 1 candidate {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not delete {ticker}.")

# --- ISTNIEJĄCE ENDPOINTY (BEZ ZMIAN) ---
@app.post("/api/v1/worker/control/{action}", status_code=202)
def control_worker(action: str, db: Session = Depends(get_db)):
    # ...
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    command = allowed_actions[action]
    crud.set_system_control_value(db, "worker_command", command)
    logger.info("Command '%s' (%s) sent to worker.", action, command)
    return {"message": f"Command '{action}' sent to worker."}


@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus)
def get_worker_status(db: Session = Depends(get_db)):
    # ...
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
        logger.error(f"Error fetching worker status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not fetch worker status from the database.")


@app.post("/api/v1/analysis/on-demand", status_code=202)
def request_on_demand_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    # ...
    ticker = request.ticker.strip().upper()
    logger.info("Received on-demand analysis request for %s. Forwarding to worker.", ticker)
    crud.set_system_control_value(db, key="on_demand_request", value=ticker)
    return {"message": f"Analysis request for {ticker} accepted and queued."}


@app.get("/api/v1/analysis/on-demand/result/{ticker}")
def get_on_demand_result(ticker: str, db: Session = Depends(get_db)):
    # ...
    analysis_result = crud.get_on_demand_result(db, ticker.strip().upper())
    if not analysis_result:
        return Response(status_code=204)
    return analysis_result


@app.get("/api/v1/signals/active", response_model=List[schemas.TradingSignal])
def get_apex_elita_signals(db: Session = Depends(get_db)):
    # ...
    try:
        return crud.get_active_signals(db)
    except Exception as e:
        logger.error(f"Error fetching active signals: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not fetch active signals.")


@app.get("/api/v1/scores/qualified", response_model=List[schemas.ApexScore])
def get_qualified_candidates(db: Session = Depends(get_db)):
    # ...
    try:
        return crud.get_qualified_stocks(db)
    except Exception as e:
        logger.error(f"Error fetching qualified stocks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not fetch qualified stocks.")

