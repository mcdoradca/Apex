import logging
import sys
import os
# ZMIANA: Dodano Response z fastapi
from fastapi import FastAPI, Depends, HTTPException, Response, Query
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

app = FastAPI(title="APEX Predator API", version="2.3.1") # Wersja z poprawką HEAD

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"], # Upewnijmy się, że HEAD jest dozwolony (choć * zwykle to załatwia)
    allow_headers=["*"],
)

api_av_client = AlphaVantageClient()


# Poprawiony endpoint główny, obsługuje GET i HEAD
@app.get("/", summary="Root endpoint confirming API is running")
def read_root_get():
    """Podstawowy endpoint GET potwierdzający działanie API."""
    return {"status": "APEX Predator API is running"}

# ==========================================================
# === POPRAWKA INSPEKCYJNA (Crash Loop - próba 2) ===
# Dodano obsługę metody HEAD dla ścieżki głównej '/',
# aby zapobiec potencjalnym restartom przez mechanizmy Render,
# które mogą testować tę metodę.
# ==========================================================
@app.head("/", summary="Health check endpoint for HEAD requests")
async def read_root_head():
    """Podstawowy endpoint HEAD zwracający pustą odpowiedź 200 OK."""
    # Metoda HEAD powinna zwracać tylko nagłówki, bez ciała odpowiedzi.
    # Używamy pustej odpowiedzi Response z kodem 200.
    return Response(status_code=200)
# ==========================================================


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
        if not api_av_client.api_key:
             logger.warning("ALPHAVANTAGE_API_KEY environment variable is not set. The /quote endpoint will not work.")

    except Exception as e:
        logger.error(f"Could not initialize system_control values: {e}", exc_info=True)
    finally:
        db.close()

# --- ENDPOINTY PORTFELA I TRANSAKCJI ---
# (Reszta endpointów bez zmian)
@app.post("/api/v1/portfolio/buy", response_model=schemas.PortfolioHolding, status_code=201)
def buy_stock(buy_request: schemas.BuyRequest, db: Session = Depends(get_db)):
    try:
        logger.info(f"Otrzymano zlecenie zakupu: {buy_request.quantity} akcji {buy_request.ticker} po {buy_request.price_per_share}")
        holding = crud.record_buy_transaction(db, buy_request)
        logger.info(f"Zakup {buy_request.ticker} przetworzony. Nowy stan portfela: {holding.quantity} akcji, średnia cena: {holding.average_buy_price}")
        return holding
    except ValueError as ve:
        logger.warning(f"Błąd walidacji przy zakupie {buy_request.ticker}: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Nieoczekiwany błąd serwera przy zakupie {buy_request.ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas przetwarzania zakupu.")

@app.post("/api/v1/portfolio/sell", response_model=Optional[schemas.PortfolioHolding], status_code=200)
def sell_stock(sell_request: schemas.SellRequest, db: Session = Depends(get_db)):
    try:
        logger.info(f"Otrzymano zlecenie sprzedaży: {sell_request.quantity} akcji {sell_request.ticker} po {sell_request.price_per_share}")
        updated_holding = crud.record_sell_transaction(db, sell_request)
        if updated_holding:
            logger.info(f"Sprzedaż częściowa {sell_request.ticker} przetworzona. Pozostało: {updated_holding.quantity} akcji.")
            return updated_holding
        else:
            logger.info(f"Sprzedaż całkowita {sell_request.ticker} przetworzona. Pozycja zamknięta.")
            return None
    except ValueError as ve:
        logger.warning(f"Błąd walidacji przy sprzedaży {sell_request.ticker}: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Nieoczekiwany błąd serwera przy sprzedaży {sell_request.ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas przetwarzania sprzedaży.")

@app.get("/api/v1/portfolio", response_model=List[schemas.PortfolioHolding])
def get_portfolio(db: Session = Depends(get_db)):
    logger.info("Pobieranie aktualnego stanu portfela.")
    holdings = crud.get_portfolio_holdings(db)
    return holdings

@app.get("/api/v1/transactions", response_model=List[schemas.TransactionHistory])
def get_transactions(limit: int = Query(100, ge=1, le=1000, description="Liczba ostatnich transakcji do pobrania"), db: Session = Depends(get_db)):
    logger.info(f"Pobieranie historii ostatnich {limit} transakcji.")
    transactions = crud.get_transaction_history(db, limit=limit)
    return transactions

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

# ==========================================================
# KROK 4e (Licznik): Endpoint API do pobierania licznika
# ==========================================================
@app.get("/api/v1/signals/discarded-count-24h", response_model=Dict[str, int], summary="Pobiera liczbę sygnałów unieważnionych/zakończonych w ciągu ostatnich 24h")
def get_discarded_signals_count(db: Session = Depends(get_db)):
    """
    Zwraca liczbę sygnałów, które zmieniły status na 'INVALIDATED' 
    lub 'COMPLETED' w ciągu ostatnich 24 godzin.
    """
    try:
        count = crud.get_discarded_signals_count_24h(db)
        return {"discarded_count_24h": count}
    except Exception as e:
        logger.error(f"Error fetching discarded signals count: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Nie można pobrać licznika unieważnionych sygnałów.")
# ==========================================================


# ==========================================================
# KROK 5 (Wirtualny Agent): Endpoint API do pobierania raportu
# ==========================================================
@app.get("/api/v1/virtual-agent/report", response_model=schemas.VirtualAgentReport, summary="Pobiera pełny raport Wirtualnego Agenta")
def get_virtual_agent_report_endpoint(db: Session = Depends(get_db)):
    """
    Zwraca pełny raport Wirtualnego Agenta, zawierający zagregowane
    statystyki (Win Rate, P/L, Profit Factor) oraz listę
    wszystkich zamkniętych wirtualnych transakcji.
    """
    try:
        report = crud.get_virtual_agent_report(db)
        return report
    except Exception as e:
        logger.error(f"Error fetching virtual agent report: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Nie można pobrać raportu Wirtualnego Agenta.")
# ==========================================================


@app.post("/api/v1/watchlist/{ticker}", status_code=201, response_model=schemas.TradingSignal)
def add_to_watchlist(ticker: str, db: Session = Depends(get_db)):
    try:
        stmt = text("""
            INSERT INTO trading_signals (ticker, generation_date, status, notes)
            VALUES (:ticker, NOW(), 'PENDING', 'Ręcznie dodany do obserwowanych')
            ON CONFLICT (ticker) WHERE status IN ('ACTIVE', 'PENDING')
            DO UPDATE SET
                notes = 'Ręcznie dodany do obserwowanych (ponownie)'
            RETURNING *;
        """)
        params = [{'ticker': ticker.strip().upper()}]
        result_proxy = db.execute(stmt, params)
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


# --- ENDPOINTY ANALIZY AI NA ŻĄDANIE ---

@app.post("/api/v1/ai-analysis/request", status_code=202, response_model=schemas.AIAnalysisRequestResponse)
def request_ai_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    ticker = request.ticker.strip().upper()
    try:
        crud.delete_ai_analysis_result(db, ticker)
        crud.set_system_control_value(db, key="ai_analysis_request", value=ticker)
        logger.info(f"AI analysis request for {ticker} has been sent to the worker (previous result cleared).")
        return {"message": f"Analiza AI dla {ticker} została zlecona.", "ticker": ticker}
    except Exception as e:
        logger.error(f"Error processing AI analysis request for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas zlecania analizy.")


@app.get("/api/v1/ai-analysis/result/{ticker}", response_model=schemas.AIAnalysisResult)
def get_ai_analysis_result(ticker: str, db: Session = Depends(get_db)):
    analysis_result = crud.get_ai_analysis_result(db, ticker.strip().upper())
    if not analysis_result:
        current_request = crud.get_system_control_value(db, "ai_analysis_request")
        if current_request == ticker.strip().upper() or current_request == 'PROCESSING':
             return {"status": "PROCESSING", "message": "Analiza w toku..."}
        raise HTTPException(status_code=404, detail="Nie znaleziono wyniku analizy AI dla tego tickera ani nie jest ona przetwarzana.")
    return analysis_result

# Endpoint do pobierania ceny
@app.get("/api/v1/quote/{ticker}", response_model=Optional[Dict[str, Any]])
def get_live_quote(ticker: str):
    ticker = ticker.strip().upper()
    try:
        quote_data = api_av_client.get_global_quote(ticker)
        if not quote_data:
            logger.warning(f"No quote data received from Alpha Vantage for {ticker}.")
            return None
        return quote_data
    except Exception as e:
        logger.error(f"Error fetching live quote for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=503, detail=f"Błąd podczas pobierania ceny z Alpha Vantage: {e}")


# --- ENDPOINTY KONTROLI I STATUSU ---

@app.post("/api/v1/worker/control/{action}", status_code=202)
def control_worker(action: str, db: Session = Depends(get_db)):
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Invalid action.")
    command = allowed_actions[action]
    try:
        if action == "start":
            crud.set_system_control_value(db, "ai_analysis_request", 'NONE')
        crud.set_system_control_value(db, "worker_command", command)
        logger.info(f"Command '{action}' ({command}) sent to worker.")
        return {"message": f"Command '{action}' sent to worker."}
    except Exception as e:
        logger.error(f"Error sending command {action} to worker: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas wysyłania komendy.")


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
        raise HTTPException(status_code=500, detail="Nie można pobrać statusu workera.")

@app.get("/api/v1/system/alert", response_model=schemas.SystemAlert)
def get_system_alert(db: Session = Depends(get_db)):
    """Pobiera i czyści globalny alert systemowy."""
    alert_message = crud.get_system_control_value(db, "system_alert")

    if alert_message and alert_message != 'NONE':
         try:
            crud.set_system_control_value(db, "system_alert", "NONE")
            return schemas.SystemAlert(message=alert_message)
         except Exception as e:
              logger.error(f"Error clearing system alert: {e}", exc_info=True)
              return schemas.SystemAlert(message=alert_message)

    return schemas.SystemAlert(message="NONE")
