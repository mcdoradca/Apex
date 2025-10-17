import logging
import sys
import os
from fastapi import FastAPI, Depends, HTTPException, Response, Query
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List, Optional

from fastapi.middleware.cors import CORSMiddleware as AllowAllCORSMiddleware

from . import crud, models, schemas
from .database import get_db, engine, SessionLocal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'worker', 'src')))
from data_ingestion.alpha_vantage_client import AlphaVantageClient

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
api_client = AlphaVantageClient(api_key=API_KEY) if API_KEY else None

try:
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to create database tables: {e}", exc_info=True)
    sys.exit(1)

app = FastAPI(title="APEX Predator API", version="2.1.0")

app.add_middleware(
    AllowAllCORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
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
            'ai_analysis_request': 'NONE',
            'system_alert': 'NONE'
        }
        for key, value in initial_values.items():
            if crud.get_system_control_value(db, key) is None:
                crud.set_system_control_value(db, key, value)
        logger.info("Initial system control values verified.")
    finally:
        db.close()

# --- ENDPOINTY ZWIĄZANE Z FAZAMI ANALIZY ---

@app.get("/api/v1/candidates/phase1", response_model=List[schemas.Phase1Candidate])
def get_phase1_candidates_endpoint(db: Session = Depends(get_db)):
    return crud.get_phase1_candidates(db)

@app.get("/api/v1/results/phase2", response_model=List[schemas.Phase2Result])
def get_phase2_results_endpoint(db: Session = Depends(get_db)):
    return crud.get_phase2_results(db)

@app.get("/api/v1/signals/phase3", response_model=List[schemas.TradingSignal])
def get_phase3_signals_endpoint(db: Session = Depends(get_db)):
    return crud.get_active_pending_and_watchlist_signals(db)

# --- ENDPOINTY ZBIORCZE I NA ŻĄDANIE ---

@app.get("/api/v1/details/{ticker}", response_model=schemas.ConsolidatedTickerDetails)
def get_consolidated_details(ticker: str, db: Session = Depends(get_db)):
    return crud.get_consolidated_details(db, ticker.strip().upper())

# --- NOWE ENDPOINTY DLA ANALIZY AI ---
@app.post("/api/v1/ai-analysis/request", status_code=202)
def request_ai_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    ticker = request.ticker.strip().upper()
    crud.set_system_control_value(db, key="ai_analysis_request", value=ticker)
    return {"message": f"AI analysis request for {ticker} accepted."}

@app.get("/api/v1/ai-analysis/result/{ticker}", response_model=Optional[schemas.AIAnalysisResult])
def get_ai_analysis_result(ticker: str, db: Session = Depends(get_db)):
    result = crud.get_ai_analysis_result(db, ticker.strip().upper())
    if not result:
        raise HTTPException(status_code=404, detail="Analysis not found or not ready.")
    return result

# --- NOWY ENDPOINT DLA WATCHLIST ---
@app.post("/api/v1/watchlist/{ticker}", status_code=201)
def add_to_watchlist(ticker: str, db: Session = Depends(get_db)):
    return crud.add_to_watchlist(db, ticker.strip().upper())

# --- ENDPOINTY KONTROLI I STATUSU ---

@app.post("/api/v1/worker/control/{action}", status_code=202)
def control_worker(action: str, db: Session = Depends(get_db)):
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    command = allowed_actions[action]
    crud.set_system_control_value(db, "worker_command", command)
    return {"message": f"Command '{action}' sent to worker."}


@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus)
def get_worker_status(db: Session = Depends(get_db)):
    try:
        status_data = {
            "status": crud.get_system_control_value(db, "worker_status") or "UNKNOWN",
            "phase": crud.get_system_control_value(db, "current_phase") or "NONE",
            "progress": {
                "processed": int(crud.get_system_control_value(db, "scan_progress_processed") or 0),
                "total": int(crud.get_system_control_value(db, "scan_progress_total") or 1)
            },
            "last_heartbeat_utc": crud.get_system_control_value(db, "last_heartbeat"),
            "log": crud.get_system_control_value(db, "scan_log") or ""
        }
        return schemas.WorkerStatus(**status_data)
    except Exception as e:
        logger.error(f"Error fetching worker status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not fetch worker status.")

@app.get("/api/v1/system/alert", response_model=schemas.SystemAlert)
def get_system_alert(db: Session = Depends(get_db)):
    alert_message = crud.get_system_control_value(db, "system_alert")
    if alert_message and alert_message != 'NONE':
        crud.set_system_control_value(db, "system_alert", "NONE")
        return schemas.SystemAlert(message=alert_message)
    return schemas.SystemAlert(message="NONE")
