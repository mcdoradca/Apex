import logging
import sys
import os
from fastapi import FastAPI, Depends, HTTPException, Response, Query
# ==========================================================
# KRYTYCZNA POPRAWKA: Importowanie mechanizmu CORS
# To jest ostateczne rozwiązanie błędu 'CORS policy'.
# ==========================================================
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List, Optional

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

app = FastAPI(title="APEX Predator API", version="2.1.0")

# ==============================================================================
# KRYTYCZNA POPRAWKA: Dodanie konfiguracji CORS
# Ten blok kodu mówi serwerowi API, aby akceptował zapytania z dowolnego źródła
# (w tym z Twojej aplikacji frontendowej na Render.com). To rozwiązuje błąd
# "has been blocked by CORS policy" widoczny w konsoli przeglądarki.
# ==============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Pozwala na dostęp z każdej domeny
    allow_credentials=True,
    allow_methods=["*"],  # Pozwala na wszystkie metody (GET, POST, etc.)
    allow_headers=["*"],  # Pozwala na wszystkie nagłówki
)


@app.on_event("startup")
async def startup_event():
    """Weryfikuje i ustawia początkowe wartości w tabeli system_control."""
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
    except Exception as e:
        logger.error(f"Could not initialize system_control values: {e}", exc_info=True)
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
    return crud.get_active_and_pending_signals(db)

# --- ENDPOINTY ANALIZY AI NA ŻĄDANIE ---

@app.post("/api/v1/ai-analysis/request", status_code=202, response_model=schemas.AIAnalysisRequestResponse)
def request_ai_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    """Zleca nową, pełną analizę AI dla danego tickera."""
    ticker = request.ticker.strip().upper()
    crud.set_system_control_value(db, key="ai_analysis_request", value=ticker)
    logger.info(f"AI analysis request for {ticker} has been sent to the worker.")
    return {"message": f"Analiza AI dla {ticker} została zlecona.", "ticker": ticker}

@app.get("/api/v1/ai-analysis/result/{ticker}", response_model=schemas.AIAnalysisResult)
def get_ai_analysis_result(ticker: str, db: Session = Depends(get_db)):
    """Pobiera wynik analizy AI dla danego tickera."""
    analysis_result = crud.get_ai_analysis_result(db, ticker.strip().upper())
    if not analysis_result:
        raise HTTPException(status_code=404, detail="Nie znaleziono wyniku analizy AI dla tego tickera.")
    return analysis_result


# --- ENDPOINTY KONTROLI I STATUSU ---

@app.post("/api/v1/worker/control/{action}", status_code=202)
def control_worker(action: str, db: Session = Depends(get_db)):
    """Steruje pracą workera (start, pause, resume)."""
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    command = allowed_actions[action]
    crud.set_system_control_value(db, "worker_command", command)
    logger.info(f"Command '{action}' ({command}) sent to worker.")
    return {"message": f"Command '{action}' sent to worker."}


@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus)
def get_worker_status(db: Session = Depends(get_db)):
    """Pobiera aktualny status workera."""
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
    """Pobiera i czyści globalny alert systemowy."""
    alert_message = crud.get_system_control_value(db, "system_alert")
    
    if alert_message and alert_message != 'NONE':
        crud.set_system_control_value(db, "system_alert", "NONE")
        return schemas.SystemAlert(message=alert_message)
        
    return schemas.SystemAlert(message="NONE")

