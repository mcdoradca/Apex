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
# Import klienta Alpha Vantage, aby API mogło pobierać ceny na żywo
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'worker', 'src')))
from data_ingestion.alpha_vantage_client import AlphaVantageClient

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# --- INICJALIZACJA KLIENTA API ---
# Klucz API jest przekazywany jako zmienna środowiskowa do usługi API na Render
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    logger.warning("ALPHAVANTAGE_API_KEY not found for live price endpoint. Endpoint will be disabled.")
    api_client = None
else:
    api_client = AlphaVantageClient(api_key=API_KEY)

try:
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to create database tables: {e}", exc_info=True)
    sys.exit(1)

app = FastAPI(title="APEX Predator API", version="2.0.0")

app.add_middleware(
    AllowAllCORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
            'on_demand_request': 'NONE',
            'phase3_on_demand_request': 'NONE',
            'system_alert': 'NONE' # DODANO: Inicjalizacja alertu
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

@app.delete("/api/v1/candidates/phase1/{ticker}", status_code=204)
def delete_phase1_candidate_endpoint(ticker: str, db: Session = Depends(get_db)):
    crud.delete_phase1_candidate(db, ticker.strip().upper())
    return Response(status_code=204)

@app.get("/api/v1/results/phase2", response_model=List[schemas.Phase2Result])
def get_phase2_results_endpoint(db: Session = Depends(get_db)):
    return crud.get_phase2_results(db)

@app.delete("/api/v1/results/phase2/{ticker}", status_code=204)
def delete_phase2_result_endpoint(ticker: str, db: Session = Depends(get_db)):
    crud.delete_phase2_result(db, ticker.strip().upper())
    return Response(status_code=204)

@app.get("/api/v1/signals/phase3", response_model=List[schemas.TradingSignal])
def get_phase3_signals_endpoint(db: Session = Depends(get_db)):
    # Zmieniono na pobieranie ACTIVE i PENDING sygnałów
    return crud.get_active_and_pending_signals(db)

@app.delete("/api/v1/signals/phase3/{signal_id}", status_code=204)
def delete_phase3_signal_endpoint(signal_id: int, db: Session = Depends(get_db)):
    if not crud.delete_trading_signal(db, signal_id):
        raise HTTPException(status_code=404, detail="Signal not found.")
    return Response(status_code=204)


# --- ENDPOINTY ZBIORCZE I NA ŻĄDANIE ---

@app.get("/api/v1/details/{ticker}", response_model=schemas.ConsolidatedTickerDetails)
def get_consolidated_details(ticker: str, db: Session = Depends(get_db)):
    """Pobiera skonsolidowane dane dla pojedynczego tickera."""
    return crud.get_consolidated_details(db, ticker.strip().upper())

@app.post("/api/v1/analysis/on-demand", status_code=202)
def request_on_demand_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    """Zleca ogólną analizę na żądanie."""
    ticker = request.ticker.strip().upper()
    crud.set_system_control_value(db, key="on_demand_request", value=ticker)
    return {"message": f"Analysis request for {ticker} accepted."}

@app.post("/api/v1/analysis/phase3-on-demand", status_code=202)
def request_phase3_on_demand_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    """Zleca analizę Fazy 3 na żądanie (Predator)."""
    ticker = request.ticker.strip().upper()
    crud.set_system_control_value(db, key="phase3_on_demand_request", value=ticker)
    return {"message": f"Phase 3 on-demand analysis for {ticker} accepted."}

@app.get("/api/v1/analysis/on-demand/result/{ticker}")
def get_on_demand_result(ticker: str, db: Session = Depends(get_db)):
    """Pobiera wynik ogólnej analizy na żądanie."""
    analysis_result = crud.get_on_demand_result(db, ticker.strip().upper())
    if not analysis_result:
        return Response(status_code=204) # No Content
    return analysis_result

@app.get("/api/v1/analysis/phase3-on-demand/result/{ticker}")
def get_phase3_on_demand_result(ticker: str, db: Session = Depends(get_db)):
    """Pobiera wynik analizy Fazy 3 na żądanie."""
    analysis_result = crud.get_phase3_on_demand_result(db, ticker.strip().upper())
    if not analysis_result:
        return Response(status_code=204)
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

# DODANO: Endpoint do pobierania globalnego alertu systemowego
@app.get("/api/v1/system/alert", response_model=schemas.SystemAlert)
def get_system_alert(db: Session = Depends(get_db)):
    """Pobiera i czyści globalny alert systemowy."""
    alert_message = crud.get_system_control_value(db, "system_alert")
    
    if alert_message and alert_message != 'NONE':
        # Czyści alert po odczycie przez frontend (ważne, żeby nie pokazywał się w kółko)
        crud.set_system_control_value(db, "system_alert", "NONE")
        return schemas.SystemAlert(message=alert_message)
        
    return schemas.SystemAlert(message="NONE")


# --- ENDPOINT CEN NA ŻYWO ---

@app.get("/api/v1/live-prices", response_model=List[schemas.LivePrice])
def get_live_prices(tickers: Optional[str] = Query(None), db: Session = Depends(get_db)):
    """Pobiera aktualne ceny dla listy tickerów."""
    if not api_client:
        raise HTTPException(status_code=503, detail="Live price service is not available (API key missing).")
    if not tickers:
        return []
    
    ticker_list = [t.strip().upper() for t in tickers.split(',')]
    if not ticker_list:
        return []

    try:
        # Używamy zaimplementowanej funkcji do pobierania live prices (działa w workwerze, więc importujemy stamtąd)
        from worker.src.analysis.phase1_scanner import _parse_bulk_quotes_csv
        bulk_data_csv = api_client.get_bulk_quotes(ticker_list)
        if not bulk_data_csv:
            return []
            
        parsed_data = _parse_bulk_quotes_csv(bulk_data_csv)
        
        # Konwersja na schemat LivePrice
        prices = []
        for ticker, data in parsed_data.items():
            if data['price'] is not None:
                prices.append(schemas.LivePrice(ticker=ticker, price=data['price']))
                
        return prices

    except Exception as e:
        logger.error(f"Error fetching live prices: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch live prices.")
