import logging
import sys
import json
import csv
from io import StringIO
from fastapi import FastAPI, Depends, HTTPException, Response, Query, Body
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any
from decimal import Decimal

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

app = FastAPI(title="APEX Predator API", version="5.0.1") # V5.0.1: Omni-Flux + Login Fix

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
    # === CRITICAL FIX: Słowo 'running' jest wymagane przez Frontend (js/app.js) ===
    return {"status": "APEX Predator API V5 is running (Omni-Flux Ready)"}

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
            'h3_deep_dive_report': 'NONE',
            'h3_live_parameters': '{}',
            'macro_sentiment': 'UNKNOWN',
            'optimization_request': 'NONE' 
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

@app.get("/api/v1/candidates/phasex", response_model=List[schemas.PhaseXCandidate])
def get_phasex_candidates_endpoint(db: Session = Depends(get_db)):
    return crud.get_phasex_candidates(db)

@app.get("/api/v1/candidates/phase4", response_model=List[schemas.Phase4Candidate])
def get_phase4_candidates_endpoint(db: Session = Depends(get_db)):
    """Pobiera listę kandydatów Kinetic Alpha (Petardy)."""
    try:
        return crud.get_phase4_candidates(db)
    except Exception as e:
        logger.error(f"Error fetching Phase 4 candidates: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Błąd pobierania danych Fazy 4.")

@app.get("/api/v1/results/phase2", response_model=List[schemas.Phase2Result])
def get_phase2_results_endpoint(db: Session = Depends(get_db)):
    return crud.get_phase2_results(db)

@app.get("/api/v1/signals/phase3", response_model=List[schemas.TradingSignal])
def get_phase3_signals_endpoint(db: Session = Depends(get_db)):
    # Ten endpoint zwraca wszystkie aktywne/oczekujące sygnały (H3 + Flux)
    # To poprawne dla głównego widoku sygnałów
    return crud.get_active_and_pending_signals(db)

@app.get("/api/v1/signal/{ticker}/details")
def get_signal_details_live(ticker: str, db: Session = Depends(get_db)):
    ticker = ticker.upper().strip()
    
    signal = db.query(models.TradingSignal).filter(
        models.TradingSignal.ticker == ticker,
        models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
    ).first()

    company = db.query(models.Company).filter(models.Company.ticker == ticker).first()
    live_quote = api_av_client.get_global_quote(ticker)
    
    if not signal and not company and not live_quote:
        raise HTTPException(status_code=404, detail=f"Nie znaleziono danych dla tickera {ticker}. Sprawdź poprawność symbolu.")

    market_status_raw = api_av_client.get_market_status()
    
    latest_news = db.query(models.ProcessedNews).filter(
        models.ProcessedNews.ticker == ticker
    ).order_by(models.ProcessedNews.processed_at.desc()).first()
    
    news_context = None
    if latest_news:
        news_context = {
            "sentiment": latest_news.sentiment,
            "headline": latest_news.headline,
            "url": latest_news.source_url,
            "processed_at": latest_news.processed_at.isoformat()
        }

    current_price = 0.0
    prev_close = 0.0
    change_percent = "0%"
    market_state = "UNKNOWN"
    price_source = "unknown"
    
    if live_quote:
        try:
            current_price = float(live_quote.get("05. price", 0))
            prev_close = float(live_quote.get("08. previous close", 0))
            change_percent = live_quote.get("10. change percent", "0%")
            price_source = live_quote.get("_price_source", "unknown")
        except: pass
        
    if market_status_raw:
        for m in market_status_raw.get("markets", []):
             if m.get("region") == "United States":
                 market_state = m.get("current_status", "Closed")
                 break

    setup_obj = {
        "entry_price": None, "stop_loss": None, "take_profit": None, 
        "risk_reward": None, "notes": None, "generation_date": None
    }
    
    validation_msg = "Tryb Podglądu (Brak Sygnału)"
    status_code = "WATCH_ONLY"
    is_valid_signal = False

    if signal:
        status_code = "VALID"
        is_valid_signal = True
        validation_msg = "Setup Aktywny"
        
        setup_obj = {
            "entry_price": float(signal.entry_price) if signal.entry_price else None,
            "stop_loss": float(signal.stop_loss) if signal.stop_loss else None,
            "take_profit": float(signal.take_profit) if signal.take_profit else None,
            "risk_reward": float(signal.risk_reward_ratio) if signal.risk_reward_ratio else None,
            "notes": signal.notes,
            "generation_date": signal.generation_date.isoformat()
        }
        
        if current_price > 0 and signal.stop_loss and signal.take_profit:
            sl = float(signal.stop_loss)
            tp = float(signal.take_profit)
            
            if current_price <= sl:
                is_valid_signal = False
                status_code = "INVALIDATED"
                validation_msg = f"SPALONY (Live): Cena {current_price} przebiła SL {sl}."
                signal.status = 'INVALIDATED'
                signal.notes = (signal.notes or "") + f" [AUTO-REMOVED by API Live Check]"
                signal.updated_at = datetime.now(timezone.utc)
                db.commit()
                
            elif current_price >= tp:
                is_valid_signal = False
                status_code = "COMPLETED"
                validation_msg = f"ZREALIZOWANY (Live): Cena {current_price} osiągnęła TP {tp}."

    response_data = {
        "status": status_code,
        "ticker": ticker,
        "company": {
            "name": company.company_name if company else ticker,
            "sector": company.sector if company else "N/A",
            "industry": company.industry if company else "N/A"
        },
        "market_data": {
            "current_price": current_price,
            "prev_close": prev_close,
            "change_percent": change_percent,
            "market_status": market_state,
            "price_source": price_source,
            "server_check_time": datetime.now(timezone.utc).isoformat()
        },
        "setup": setup_obj,
        "news_context": news_context,
        "validity": {
            "is_valid": is_valid_signal,
            "message": validation_msg
        }
    }
    
    return response_data

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

@app.get("/api/v1/virtual-agent/trade/{trade_id}/audit", response_model=Dict[str, Any])
def get_trade_audit_details(trade_id: int, db: Session = Depends(get_db)):
    """Pobiera szczegółowy raport audytu dla pojedynczej transakcji."""
    try:
        trade = db.query(models.VirtualTrade).filter(models.VirtualTrade.id == trade_id).first()
        if not trade:
            raise HTTPException(status_code=404, detail="Transakcja nie znaleziona.")
        
        return {
            "trade_id": trade.id,
            "ticker": trade.ticker,
            "ai_audit_report": trade.ai_audit_report,
            "ai_optimization_suggestion": trade.ai_optimization_suggestion,
            "expected_pf": float(trade.expected_profit_factor) if trade.expected_profit_factor else None,
            "actual_pl": float(trade.final_profit_loss_percent) if trade.final_profit_loss_percent else None
        }
    except Exception as e:
        logger.error(f"Błąd pobierania audytu: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Błąd serwera.")

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
    try:
        stmt = text("""
            INSERT INTO trading_signals (ticker, generation_date, status, notes)
            VALUES (:ticker, NOW(), 'PENDING', 'GHOST: Rozpoczęto śledzenie')
            ON CONFLICT (ticker) WHERE status IN ('ACTIVE', 'PENDING')
            DO UPDATE SET
                notes = trading_signals.notes || '\n[GHOST] Śledzenie aktywowane',
                updated_at = NOW()
            RETURNING *;
        """)
        result_proxy = db.execute(stmt, [{'ticker': ticker.strip().upper()}])
        result = result_proxy.fetchone()
        db.commit()

        if not result:
            existing = db.query(models.TradingSignal).filter(
                models.TradingSignal.ticker == ticker.strip().upper(),
                models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
            ).first()
            if not existing:
                 raise HTTPException(status_code=500, detail="Nie można było utworzyć ani pobrać sygnału po konflikcie.")
            result_dict = {c.name: getattr(existing, c.name) for c in existing.__table__.columns}
        else:
            result_dict = dict(result._mapping)

        result_dict['generation_date'] = result_dict['generation_date'].isoformat()
        if result_dict.get('signal_candle_timestamp'):
            result_dict['signal_candle_timestamp'] = result_dict['signal_candle_timestamp'].isoformat()
        return result_dict
    except Exception as e:
        db.rollback()
        logger.error(f"Błąd podczas dodawania do watchlist ({ticker}): {e}", exc_info=True)
        if "foreign key constraint" in str(e):
             raise HTTPException(status_code=400, detail=f"Ticker {ticker} nie istnieje w bazie danych 'companies'.")
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {str(e)}")

@app.get("/api/v1/quotes/bulk", response_model=List[Dict[str, Any]])
def get_bulk_quotes_endpoint(tickers: str = Query(..., description="Tickery oddzielone przecinkami (np. AAPL,MSFT)"), db: Session = Depends(get_db)):
    ticker_list = [t.strip().upper() for t in tickers.split(',') if t.strip()]
    if not ticker_list:
        return []
    try:
        csv_data = api_av_client.get_bulk_quotes(ticker_list)
        if not csv_data: return []
        results = []
        reader = csv.DictReader(StringIO(csv_data))
        for row in reader:
            formatted = {
                "01. symbol": row.get("symbol"),
                "02. open": row.get("open"),
                "03. high": row.get("high"),
                "04. low": row.get("low"),
                "05. price": row.get("close"),
                "06. volume": row.get("volume"),
                "08. previous close": row.get("previous_close"),
                "09. change": row.get("change"),
                "10. change percent": f'{row.get("change_percent")}%'
            }
            if row.get("extended_hours_quote"):
                 formatted["05. price"] = row.get("extended_hours_quote")
                 formatted["09. change"] = row.get("extended_hours_change")
                 formatted["10. change percent"] = f'{row.get("extended_hours_change_percent")}%'
                 formatted["_price_source"] = "extended_hours"
            results.append(formatted)
        return results
    except Exception as e:
        logger.error(f"Błąd w endpointcie Bulk Quotes: {e}", exc_info=True)
        return []

@app.get("/api/v1/quote/{ticker}")
def get_live_quote(ticker: str):
    try:
        return api_av_client.get_global_quote(ticker.strip().upper())
    except Exception:
        raise HTTPException(status_code=503, detail="Błąd AV.")

@app.post("/api/v1/optimization/start", status_code=202, response_model=schemas.OptimizationJob)
def start_optimization(request: schemas.OptimizationRequest, db: Session = Depends(get_db)):
    worker_status = crud.get_system_control_value(db, "worker_status")
    if worker_status.startswith('BUSY') or worker_status == 'RUNNING':
        raise HTTPException(status_code=409, detail="Worker jest obecnie zajęty.")

    try:
        new_job = crud.create_optimization_job(db, request)
        crud.set_system_control_value(db, "optimization_request", new_job.id)
        return new_job
    except Exception as e:
        logger.error(f"Błąd podczas startu optymalizacji: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Błąd serwera: {e}")

@app.get("/api/v1/optimization/results", response_model=Optional[schemas.OptimizationJobDetail])
def get_latest_optimization_results(db: Session = Depends(get_db)):
    try:
        job = crud.get_latest_optimization_job(db)
        if not job:
            return None
        trials = crud.get_optimization_trials(db, job.id)
        job_detail = schemas.OptimizationJobDetail.model_validate(job)
        job_detail.trials = [schemas.OptimizationTrial.model_validate(t) for t in trials]
        return job_detail
    except Exception as e:
        logger.error(f"Błąd pobierania wyników optymalizacji: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Błąd serwera.")

@app.post("/api/v1/worker/control/{action}", status_code=202)
def control_worker(action: str, params: Dict[str, Any] = Body(default=None), db: Session = Depends(get_db)):
    allowed_actions = {
        "start": "START_REQUESTED", 
        "pause": "PAUSE_REQUESTED", 
        "resume": "RESUME_REQUESTED",
        "start_phase1": "START_PHASE_1_REQUESTED", 
        "start_phase3": "START_PHASE_3_REQUESTED",
        "start_phasex": "START_PHASE_X_REQUESTED",
        "start_phase4": "START_PHASE_4_REQUESTED",
        # === NOWE POLECENIE DLA FAZY 5 ===
        "start_phase5": "START_PHASE_5_REQUESTED"
    }
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    
    try:
        if params:
            crud.set_system_control_value(db, "h3_live_parameters", json.dumps(params))
        else:
            crud.set_system_control_value(db, "h3_live_parameters", "{}")

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
