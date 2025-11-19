import os
import time
import logging
import sys
import json
from fastapi import FastAPI, Depends, HTTPException, Response, Query
from fastapi.responses import StreamingResponse
import io
import csv
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict

from . import crud, models, schemas
from .database import get_db, engine, SessionLocal
from .alpha_vantage_client import AlphaVantageClient

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

try:
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully.")
except Exception as e:
    logger.critical(f"FATAL: Failed to create database tables: {e}", exc_info=True)
    sys.exit(1)

app = FastAPI(title="APEX Predator API", version="2.4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_av_client = AlphaVantageClient()

@app.get("/", summary="Root endpoint confirming API is running")
def read_root_get():
    return {"status": "APEX Predator API is running"}

@app.head("/", summary="Health check endpoint for HEAD requests")
async def read_root_head():
    return Response(status_code=200)

@app.on_event("startup")
async def startup_event():
    db = SessionLocal()
    try:
        initial_values = {
            'worker_status': 'IDLE', 'worker_command': 'NONE', 'current_phase': 'NONE',
            'scan_progress_processed': '0', 'scan_progress_total': '0',
            'scan_log': 'Czekam na rozpoczęcie skanowania...',
            'last_heartbeat': datetime.now(timezone.utc).isoformat(),
            'system_alert': 'NONE',
            'backtest_request': 'NONE',
            'backtest_parameters': '{}',
            'ai_optimizer_request': 'NONE',
            'ai_optimizer_report': 'NONE',
            'h3_deep_dive_request': 'NONE',
            'h3_deep_dive_report': 'NONE'
        }
        for key, value in initial_values.items():
            if crud.get_system_control_value(db, key) is None:
                crud.set_system_control_value(db, key, value)
        logger.info("Initial system control values verified.")
    except Exception as e:
        logger.error(f"Could not initialize system_control values: {e}", exc_info=True)
    finally:
        db.close()

# --- ENDPOINTY PORTFELA ---
@app.post("/api/v1/portfolio/buy", response_model=schemas.PortfolioHolding, status_code=201)
def buy_stock(buy_request: schemas.BuyRequest, db: Session = Depends(get_db)):
    try:
        return crud.record_buy_transaction(db, buy_request)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error buy: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Server error.")

@app.post("/api/v1/portfolio/sell", response_model=Optional[schemas.PortfolioHolding], status_code=200)
def sell_stock(sell_request: schemas.SellRequest, db: Session = Depends(get_db)):
    try:
        return crud.record_sell_transaction(db, sell_request)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error sell: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Server error.")

@app.get("/api/v1/portfolio", response_model=List[schemas.PortfolioHolding])
def get_portfolio(db: Session = Depends(get_db)):
    return crud.get_portfolio_holdings(db)

@app.get("/api/v1/transactions", response_model=List[schemas.TransactionHistory])
def get_transactions(limit: int = Query(100), db: Session = Depends(get_db)):
    return crud.get_transaction_history(db, limit=limit)

# --- ENDPOINTY ANALIZY ---
@app.get("/api/v1/candidates/phase1", response_model=List[schemas.Phase1Candidate])
def get_phase1_candidates_endpoint(db: Session = Depends(get_db)):
    return crud.get_phase1_candidates(db)

@app.get("/api/v1/results/phase2", response_model=List[schemas.Phase2Result])
def get_phase2_results_endpoint(db: Session = Depends(get_db)):
    return crud.get_phase2_results(db)

@app.get("/api/v1/signals/phase3", response_model=List[schemas.TradingSignal])
def get_phase3_signals_endpoint(db: Session = Depends(get_db)):
    return crud.get_active_and_pending_signals(db)

@app.get("/api/v1/signals/discarded-count-24h", response_model=Dict[str, int])
def get_discarded_signals_count(db: Session = Depends(get_db)):
    return {"discarded_count_24h": crud.get_discarded_signals_count_24h(db)}

@app.get("/api/v1/export/trades.csv", response_class=StreamingResponse)
def export_virtual_trades(db: Session = Depends(get_db)):
    try:
        csv_generator = crud.stream_all_trades_as_csv(db)
        filename = f'apex_virtual_trades_export_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")}.csv'
        return StreamingResponse(csv_generator, media_type="text/csv", headers={'Content-Disposition': f'attachment; filename="{filename}"'})
    except Exception as e:
        logger.error(f"CSV export error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Server error: {e}")

@app.get("/api/v1/virtual-agent/report", response_model=schemas.VirtualAgentReport)
def get_virtual_agent_report_endpoint(page: int = 1, page_size: int = 200, db: Session = Depends(get_db)):
    return crud.get_virtual_agent_report(db, page, page_size)

@app.post("/api/v1/backtest/request", status_code=202)
def request_backtest(request: schemas.BacktestRequest, db: Session = Depends(get_db)):
    year_to_test = request.year.strip()
    if not (year_to_test.isdigit() and len(year_to_test) == 4):
         raise HTTPException(status_code=400, detail="Nieprawidłowy rok.")
    worker_status = crud.get_system_control_value(db, "worker_status")
    if worker_status.startswith('BUSY') or worker_status == 'RUNNING':
            raise HTTPException(status_code=409, detail="Worker zajęty.")
    
    try:
        if request.parameters:
            crud.set_system_control_value(db, key="backtest_parameters", value=json.dumps(request.parameters))
        else:
            crud.set_system_control_value(db, key="backtest_parameters", value="{}")
        crud.set_system_control_value(db, key="backtest_request", value=year_to_test)
        return {"message": f"Backtest {year_to_test} zlecony."}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Błąd serwera.")

@app.post("/api/v1/ai-optimizer/request", status_code=202)
def request_ai_optimizer(request: schemas.AIOptimizerRequest, db: Session = Depends(get_db)):
    worker_status = crud.get_system_control_value(db, "worker_status")
    if worker_status.startswith('BUSY') or worker_status == 'RUNNING':
            raise HTTPException(status_code=409, detail="Worker zajęty.")
    try:
        crud.set_system_control_value(db, "ai_optimizer_request", 'REQUESTED')
        crud.set_system_control_value(db, "ai_optimizer_report", 'PROCESSING')
        return {"message": "Zlecenie AI wysłane."}
    except Exception:
        raise HTTPException(status_code=500, detail="Błąd serwera.")

@app.get("/api/v1/ai-optimizer/report", response_model=schemas.AIOptimizerReport)
def get_ai_optimizer_report_endpoint(db: Session = Depends(get_db)):
    return crud.get_ai_optimizer_report(db)

@app.post("/api/v1/analysis/h3-deep-dive", status_code=202)
def request_h3_deep_dive(request: schemas.H3DeepDiveRequest, db: Session = Depends(get_db)):
    worker_status = crud.get_system_control_value(db, "worker_status")
    if worker_status.startswith('BUSY') or worker_status == 'RUNNING':
            raise HTTPException(status_code=409, detail="Worker zajęty.")
    try:
        crud.set_system_control_value(db, "h3_deep_dive_request", str(request.year))
        crud.set_system_control_value(db, "h3_deep_dive_report", 'PROCESSING') 
        return {"message": f"Deep Dive {request.year} zlecony."}
    except Exception:
        raise HTTPException(status_code=500, detail="Błąd serwera.")

@app.get("/api/v1/analysis/h3-deep-dive-report", response_model=schemas.H3DeepDiveReport)
def get_h3_deep_dive_report_endpoint(db: Session = Depends(get_db)):
    return crud.get_h3_deep_dive_report(db)

@app.post("/api/v1/watchlist/{ticker}", status_code=201, response_model=schemas.TradingSignal)
def add_to_watchlist(ticker: str, db: Session = Depends(get_db)):
    # ... (skrót, logika bez zmian) ...
    # (Aby zaoszczędzić miejsce, zakładam że logika add_to_watchlist pozostaje identyczna jak w poprzednich wersjach)
    try:
        stmt = text("""
            INSERT INTO trading_signals (ticker, generation_date, status, notes)
            VALUES (:ticker, NOW(), 'PENDING', 'Ręcznie dodany')
            ON CONFLICT (ticker) WHERE status IN ('ACTIVE', 'PENDING') DO UPDATE SET notes = 'Ponownie dodany' RETURNING *;
        """)
        result = db.execute(stmt, [{'ticker': ticker.strip().upper()}]).fetchone()
        db.commit()
        if not result:
            result = db.query(models.TradingSignal).filter(models.TradingSignal.ticker == ticker.strip().upper(), models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])).first()
        res_dict = dict(result._mapping) if result else {}
        res_dict['generation_date'] = res_dict['generation_date'].isoformat()
        return res_dict
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/quote/{ticker}")
def get_live_quote(ticker: str):
    try:
        return api_av_client.get_global_quote(ticker.strip().upper())
    except Exception:
        raise HTTPException(status_code=503, detail="Błąd AV.")

# --- ENDPOINTY KONTROLI ---

# ZMIANA: Dodano 'start_phase1' i 'start_phase3' do dozwolonych akcji
@app.post("/api/v1/worker/control/{action}", status_code=202)
def control_worker(action: str, db: Session = Depends(get_db)):
    allowed_actions = {
        "start": "START_REQUESTED", 
        "pause": "PAUSE_REQUESTED", 
        "resume": "RESUME_REQUESTED",
        "start_phase1": "START_PHASE_1_REQUESTED", # Nowa akcja
        "start_phase3": "START_PHASE_3_REQUESTED"  # Nowa akcja
    }
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    
    try:
        crud.set_system_control_value(db, "worker_command", allowed_actions[action])
        logger.info(f"Command '{action}' sent to worker.")
        return {"message": f"Command '{action}' sent."}
    except Exception as e:
        logger.error(f"Error sending command: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Server error.")

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
    except Exception:
        raise HTTPException(status_code=500, detail="Error.")

@app.get("/api/v1/system/alert", response_model=schemas.SystemAlert)
def get_system_alert(db: Session = Depends(get_db)):
    msg = crud.get_system_control_value(db, "system_alert")
    if msg and msg != 'NONE':
        crud.set_system_control_value(db, "system_alert", "NONE")
        return schemas.SystemAlert(message=msg)
    return schemas.SystemAlert(message="NONE")
