import logging
import sys
import os
import time
from fastapi import FastAPI, Depends, HTTPException, Response, Query
from sqlalchemy.orm import Sessionimport logging
import sys
import os
import time
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
            'phase3_on_demand_request': 'NONE'
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
    return crud.get_active_signals(db)

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
        prices = crud.get_live_prices_from_api(ticker_list, api_client)
        return prices
    except Exception as e:
        logger.error(f"Error fetching live prices: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch live prices.")


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
    db = SessionLocal()
    try:
        initial_values = {
            'worker_status': 'IDLE', 'worker_command': 'NONE', 'current_phase': 'NONE',
            'scan_progress_processed': '0', 'scan_progress_total': '0',
            'scan_log': 'Czekam na rozpoczęcie skanowania...',
            'last_heartbeat': datetime.now(timezone.utc).isoformat(),
            'on_demand_request': 'NONE',
            'phase3_on_demand_request': 'NONE' # Nowy klucz
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
    return crud.get_active_signals(db)

@app.delete("/api/v1/signals/phase3/{signal_id}", status_code=204)
def delete_phase3_signal_endpoint(signal_id: int, db: Session = Depends(get_db)):
    if not crud.delete_trading_signal(db, signal_id):
        raise HTTPException(status_code=404, detail="Signal not found.")
    return Response(status_code=204)


# --- NOWY, ZBIORCZY ENDPOINT DLA SZCZEGÓŁÓW ---
@app.get("/api/v1/details/{ticker}", response_model=schemas.ConsolidatedTickerDetails)
def get_consolidated_details(ticker: str, db: Session = Depends(get_db)):
    data = crud.get_consolidated_details(db, ticker.strip().upper())
    return data

# --- ENDPOINTY ANALIZY NA ŻĄDANIE ---

@app.post("/api/v1/analysis/on-demand", status_code=202)
def request_on_demand_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    ticker = request.ticker.strip().upper()
    crud.set_system_control_value(db, key="on_demand_request", value=ticker)
    return {"message": f"Analysis request for {ticker} queued."}

@app.get("/api/v1/analysis/on-demand/result/{ticker}")
def get_on_demand_result(ticker: str, db: Session = Depends(get_db)):
    result = crud.get_on_demand_result(db, ticker.strip().upper())
    if not result: return Response(status_code=204)
    return result

# NOWE ENDPOINTY DLA FAZY 3 NA ŻĄDANIE ("PREDATOR")
@app.post("/api/v1/analysis/phase3-on-demand", status_code=202)
def request_phase3_on_demand_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    ticker = request.ticker.strip().upper()
    crud.set_system_control_value(db, key="phase3_on_demand_request", value=ticker)
    return {"message": f"Phase 3 on-demand analysis for {ticker} queued."}

@app.get("/api/v1/analysis/phase3-on-demand/result/{ticker}")
def get_phase3_on_demand_result(ticker: str, db: Session = Depends(get_db)):
    result = crud.get_phase3_on_demand_result(db, ticker.strip().upper())
    if not result: return Response(status_code=204)
    return result

# --- NOWY ENDPOINT DO POBIERANIA CEN NA ŻYWO ---
@app.get("/api/v1/live-prices", response_model=schemas.LivePricesResponse)
def get_live_prices(tickers: Optional[List[str]] = Query(None), db: Session = Depends(get_db)):
    if not api_client:
        raise HTTPException(status_code=503, detail="Live price service is not available (API key missing).")
    if not tickers:
        return {"prices": {}}
    
    # Prosty cache w bazie danych, aby nie odpytywać API częściej niż co 5 sekund
    cache_key = 'live_prices_cache'
    cached_data = crud.get_system_control_value(db, cache_key)
    
    if cached_data:
        import json
        try:
            cached_prices, timestamp_str = json.loads(cached_data)
            timestamp = float(timestamp_str)
            if (time.time() - timestamp) < 5:
                prices_to_return = {t: cached_prices.get(t) for t in tickers}
                return {"prices": prices_to_return}
        except (json.JSONDecodeError, ValueError):
            logger.warning("Could not parse live prices cache. Refetching.")

    try:
        csv_data = api_client.get_bulk_quotes(tickers)
        if not csv_data:
            return {"prices": {t: None for t in tickers}}
        
        from io import StringIO
        import csv
        csv_file = StringIO(csv_data)
        reader = csv.DictReader(csv_file)
        prices = {row['symbol']: float(row['latest_price']) for row in reader if row.get('latest_price')}
        
        import json
        crud.set_system_control_value(db, cache_key, json.dumps([prices, time.time()]))

        return {"prices": prices}
    except Exception as e:
        logger.error(f"Error fetching live prices: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch live prices.")


# --- ENDPOINTY KONTROLI WORKERA (bez zmian) ---
@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus)
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
        logger.error(f"Error fetching worker status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not fetch worker status.")

@app.post("/api/v1/worker/control/{action}", status_code=202)
def control_worker(action: str, db: Session = Depends(get_db)):
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    command = allowed_actions[action]
    crud.set_system_control_value(db, "worker_command", command)
    logger.info("Command '%s' sent to worker.", action)
    return {"message": f"Command '{action}' sent to worker."}

