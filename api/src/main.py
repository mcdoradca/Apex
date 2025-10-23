import logging
import sys
import os
# Usunięto Response, bo nie jest już potrzebny w HEAD
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import text
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict

from . import crud, models, schemas
from .database import get_db, engine, SessionLocal
# POPRAWKA BŁĘDU #5: Usunięto import klienta AV
# from .alpha_vantage_client import AlphaVantageClient

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Weryfikacja/tworzenie tabel przy starcie
try:
    # Używamy Base z lokalnych modeli API
    models.Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created successfully by API service.")
except Exception as e:
    logger.critical(f"FATAL: Failed to create database tables: {e}", exc_info=True)
    sys.exit(1) # Zakończ, jeśli baza nie działa

app = FastAPI(title="APEX Predator API", version="3.0.0") # Wersja z refaktoryzacją cache

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Lub zawęź to do domeny frontendu w produkcji
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# POPRAWKA BŁĘDU #5: Usunięto instancję klienta AV
# api_av_client = AlphaVantageClient()


# Endpointy główne (GET i HEAD dla health check)
@app.get("/", summary="Root endpoint confirming API is running", tags=["System"])
def read_root_get():
    """Podstawowy endpoint GET potwierdzający działanie API."""
    return {"status": "APEX Predator API is running"}

@app.head("/", summary="Health check endpoint for HEAD requests", include_in_schema=False)
async def read_root_head():
    """Podstawowy endpoint HEAD zwracający pustą odpowiedź 200 OK (dla Render)."""
    # Zwracamy pusty string i kod 200, FastAPI zajmie się resztą
    return ""


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
        # UPSERT dla każdej wartości
        for key, value in initial_values.items():
            crud.set_system_control_value(db, key, value) # Ta funkcja robi UPSERT

        logger.info("Initial system control values verified/set.")
        # Usunięto ostrzeżenie o braku klucza AV, bo API go już nie używa

    except Exception as e:
        logger.error(f"Could not initialize system_control values during startup: {e}", exc_info=True)
    finally:
        db.close()

# --- ENDPOINTY PORTFELA I TRANSAKCJI ---

@app.post("/api/v1/portfolio/buy", response_model=schemas.PortfolioHolding, status_code=201, tags=["Portfolio"])
def buy_stock(buy_request: schemas.BuyRequest, db: Session = Depends(get_db)):
    """Rejestruje zakup akcji i aktualizuje portfel."""
    try:
        logger.info(f"Received BUY request: {buy_request.quantity} shares of {buy_request.ticker} at {buy_request.price_per_share}")
        # CRUD wykonuje walidację i logikę
        holding = crud.record_buy_transaction(db, buy_request)
        logger.info(f"BUY {buy_request.ticker} processed. New holding state: {holding.quantity} shares, avg price: {holding.average_buy_price}")
        return holding
    except ValueError as ve:
        logger.warning(f"Validation error during BUY {buy_request.ticker}: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Unexpected server error during BUY {buy_request.ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas przetwarzania zakupu.")

@app.post("/api/v1/portfolio/sell", response_model=Optional[schemas.PortfolioHolding], status_code=200, tags=["Portfolio"])
def sell_stock(sell_request: schemas.SellRequest, db: Session = Depends(get_db)):
    """Rejestruje sprzedaż akcji i aktualizuje portfel."""
    try:
        logger.info(f"Received SELL request: {sell_request.quantity} shares of {sell_request.ticker} at {sell_request.price_per_share}")
        updated_holding = crud.record_sell_transaction(db, sell_request)
        if updated_holding:
            logger.info(f"Partial SELL {sell_request.ticker} processed. Remaining: {updated_holding.quantity} shares.")
            return updated_holding
        else:
            logger.info(f"Full SELL {sell_request.ticker} processed. Position closed.")
            # Zwracamy pustą odpowiedź 204 No Content, gdy pozycja jest zamknięta
            # return None # Zwrócenie None powoduje błąd walidacji FastAPI, jeśli response_model nie dopuszcza None
            # Zmieniamy status code i zwracamy pusty content
            return Response(status_code=204)
    except ValueError as ve:
        logger.warning(f"Validation error during SELL {sell_request.ticker}: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Unexpected server error during SELL {sell_request.ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas przetwarzania sprzedaży.")

@app.get("/api/v1/portfolio", response_model=List[schemas.PortfolioHolding], tags=["Portfolio"])
def get_portfolio(db: Session = Depends(get_db)):
    """Pobiera aktualny stan portfela."""
    logger.info("Fetching current portfolio holdings.")
    holdings = crud.get_portfolio_holdings(db)
    return holdings

@app.get("/api/v1/transactions", response_model=List[schemas.TransactionHistory], tags=["Portfolio"])
def get_transactions(limit: int = Query(100, ge=1, le=1000, description="Liczba ostatnich transakcji do pobrania"), db: Session = Depends(get_db)):
    """Pobiera historię ostatnich transakcji."""
    logger.info(f"Fetching last {limit} transaction history entries.")
    transactions = crud.get_transaction_history(db, limit=limit)
    return transactions

# --- ENDPOINTY ZWIĄZANE Z FAZAMI ANALIZY ---

@app.get("/api/v1/candidates/phase1", response_model=List[schemas.Phase1Candidate], tags=["Analysis Results"])
def get_phase1_candidates_endpoint(db: Session = Depends(get_db)):
    """Pobiera kandydatów z Fazy 1 z bieżącego dnia."""
    return crud.get_phase1_candidates(db)

@app.get("/api/v1/results/phase2", response_model=List[schemas.Phase2Result], tags=["Analysis Results"])
def get_phase2_results_endpoint(db: Session = Depends(get_db)):
    """Pobiera zakwalifikowane wyniki z Fazy 2 z ostatniej analizy."""
    return crud.get_phase2_results(db)

@app.get("/api/v1/signals/phase3", response_model=List[schemas.TradingSignal], tags=["Analysis Results"])
def get_phase3_signals_endpoint(db: Session = Depends(get_db)):
    """Pobiera aktywne (ACTIVE), oczekujące (PENDING) i wyzwolone (TRIGGERED) sygnały Fazy 3."""
    # Używamy nowej nazwy funkcji CRUD
    return crud.get_active_pending_triggered_signals(db)

@app.post("/api/v1/watchlist/{ticker}", status_code=201, response_model=schemas.TradingSignal, tags=["Watchlist"])
def add_to_watchlist(ticker: str, db: Session = Depends(get_db)):
    """Dodaje ticker do obserwowanych (tworzy sygnał PENDING)."""
    ticker_upper = ticker.strip().upper()
    try:
        # Sprawdzenie, czy ticker istnieje w 'companies'
        company = db.query(models.Company).filter(models.Company.ticker == ticker_upper).first()
        if not company:
            raise HTTPException(status_code=404, detail=f"Ticker {ticker_upper} nie został znaleziony w bazie spółek.")

        # Używamy UPSERT do dodania lub zaktualizowania sygnału
        stmt = text("""
            INSERT INTO trading_signals (ticker, generation_date, status, notes)
            VALUES (:ticker, NOW(), 'PENDING', :notes)
            ON CONFLICT (ticker) WHERE status IN ('ACTIVE', 'PENDING', 'TRIGGERED')
            DO UPDATE SET
                notes = trading_signals.notes || ' | Re-added to watchlist'
            RETURNING *;
        """)
        params = {'ticker': ticker_upper, 'notes': 'Ręcznie dodany do obserwowanych'}
        result_proxy = db.execute(stmt, params)
        result = result_proxy.fetchone()
        db.commit()

        # Jeśli UPSERT nic nie zwrócił (bo np. status był EXPIRED), spróbuj pobrać
        if not result:
            existing = db.query(models.TradingSignal).filter(
                models.TradingSignal.ticker == ticker_upper,
                models.TradingSignal.status == 'PENDING' # Szukamy PENDING po potencjalnym UPSERT
            ).first()
            if not existing:
                 # Jeśli nadal nie ma, to coś poszło nie tak
                 logger.error(f"Failed to create or retrieve PENDING signal for {ticker_upper} after UPSERT attempt.")
                 raise HTTPException(status_code=500, detail="Nie można było utworzyć sygnału PENDING.")
            # Zwracamy istniejący (już zmapowany przez Pydantic)
            return existing
        else:
            # Zwracamy nowo utworzony/zaktualizowany (mapowanie Pydantic zadziała)
            # Potrzebujemy odświeżyć obiekt, aby załadować relacje, jeśli są
            # db.refresh(result) # Nie mamy obiektu ORM, tylko RowProxy
            # Zamiast refresh, pobieramy ponownie obiekt ORM
            created_signal = db.query(models.TradingSignal).filter(models.TradingSignal.id == result.id).first()
            return created_signal

    except HTTPException:
         db.rollback() # Wycofaj tylko jeśli był HTTPException (np. 404)
         raise # Rzuć dalej HTTPException
    except Exception as e:
        db.rollback() # Wycofaj przy każdym innym błędzie
        logger.error(f"Error adding {ticker_upper} to watchlist: {e}", exc_info=True)
        # Unikamy rzucania 500, jeśli błąd wynika z ograniczenia unikalności (już istnieje)
        if "duplicate key value violates unique constraint" in str(e) or "uq_active_pending_ticker" in str(e):
             # Próbujemy pobrać istniejący sygnał
             existing = db.query(models.TradingSignal).filter(
                 models.TradingSignal.ticker == ticker_upper,
                 models.TradingSignal.status.in_(['ACTIVE', 'PENDING', 'TRIGGERED'])
             ).first()
             if existing:
                 logger.info(f"Ticker {ticker_upper} already exists in watchlist/signals. Returning existing.")
                 return existing
             else:
                  # To nie powinno się zdarzyć, ale zabezpieczamy
                 raise HTTPException(status_code=409, detail=f"Ticker {ticker_upper} już istnieje w aktywnych sygnałach lub wystąpił konflikt.")
        raise HTTPException(status_code=500, detail=f"Wewnętrzny błąd serwera podczas dodawania do obserwowanych: {str(e)}")


# --- ENDPOINTY ANALIZY AI NA ŻĄDANIE ---

@app.post("/api/v1/ai-analysis/request", status_code=202, response_model=schemas.AIAnalysisRequestResponse, tags=["AI Analysis"])
def request_ai_analysis(request: schemas.OnDemandRequest, db: Session = Depends(get_db)):
    """Zleca workerowi wykonanie pełnej analizy AI dla danego tickera."""
    ticker = request.ticker.strip().upper()
    try:
        # 1. Sprawdź, czy ticker istnieje w bazie firm
        company = db.query(models.Company).filter(models.Company.ticker == ticker).first()
        if not company:
            raise HTTPException(status_code=404, detail=f"Ticker {ticker} nie został znaleziony w bazie spółek.")

        # 2. Sprawdź, czy analiza nie jest już w toku
        current_request = crud.get_system_control_value(db, "ai_analysis_request")
        if current_request == ticker or current_request == 'PROCESSING':
            logger.warning(f"AI analysis for {ticker} is already in progress or requested.")
            # Zwracamy 202, ale z informacją, że już trwa
            return {"message": f"Analiza AI dla {ticker} jest już w toku lub została zlecona.", "ticker": ticker}

        # 3. Wyczyść stary wynik (jeśli istnieje) i zleć nową analizę
        crud.delete_ai_analysis_result(db, ticker) # Ta funkcja ma własną obsługę błędów
        crud.set_system_control_value(db, key="ai_analysis_request", value=ticker) # Ta funkcja robi commit
        logger.info(f"AI analysis request for {ticker} sent to worker (previous result cleared).")
        return {"message": f"Analiza AI dla {ticker} została zlecona.", "ticker": ticker}

    except HTTPException:
        raise # Przekaż dalej wyjątki HTTP (np. 404)
    except Exception as e:
        logger.error(f"Error processing AI analysis request for {ticker}: {e}", exc_info=True)
        # Wycofaj zmiany, jeśli set_system_control_value zawiodło
        try: db.rollback()
        except: pass
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas zlecania analizy.")


@app.get("/api/v1/ai-analysis/result/{ticker}", response_model=schemas.AIAnalysisResult, tags=["AI Analysis"])
def get_ai_analysis_result(ticker: str, db: Session = Depends(get_db)):
    """Pobiera wynik ostatniej analizy AI dla tickera (jeśli istnieje)."""
    ticker_upper = ticker.strip().upper()
    analysis_result_data = crud.get_ai_analysis_result(db, ticker_upper)

    if not analysis_result_data:
        # Sprawdź, czy zlecenie dla tego tickera jest aktywne
        current_request = crud.get_system_control_value(db, "ai_analysis_request")
        # Jeśli zlecono TEN ticker LUB worker jest ogólnie zajęty przetwarzaniem
        if current_request == ticker_upper or current_request == 'PROCESSING':
             # Zwracamy status PROCESSING ręcznie
             return {"status": "PROCESSING", "message": "Analiza w toku..."}
        # Jeśli nie ma wyniku i nie ma aktywnego zlecenia -> 404
        raise HTTPException(status_code=404, detail=f"Nie znaleziono wyniku analizy AI dla {ticker_upper} ani nie jest ona przetwarzana.")

    # Mamy dane, walidujemy je przez Pydantic i zwracamy
    # CRUD zwraca dict, Pydantic go zwaliduje
    try:
        return schemas.AIAnalysisResult(**analysis_result_data)
    except Exception as e_val:
        logger.error(f"Validation error for stored AI analysis data ({ticker_upper}): {e_val}")
        # Zwracamy błąd, jeśli dane w bazie są niekompatybilne ze schematem
        return {"status": "ERROR", "message": "Błąd wewnętrzny: Nieprawidłowy format zapisanych danych analizy."}


# === POPRAWKA BŁĘDU #5: Zmieniony Endpoint Ceny ===
@app.get("/api/v1/quote/{ticker}", response_model=schemas.LiveQuoteDetails, tags=["Market Data"])
def get_live_quote_from_cache(ticker: str, db: Session = Depends(get_db)):
    """Pobiera najnowsze dane cenowe dla tickera z wewnętrznej pamięci podręcznej (bazy danych)."""
    ticker = ticker.strip().upper()
    try:
        # Używamy nowej funkcji CRUD do odczytu z tabeli live_price_cache
        cached_data = crud.get_live_price_from_cache(db, ticker)

        if not cached_data:
            logger.warning(f"No cached quote data found for {ticker}.")
            # Sprawdźmy, czy ticker w ogóle istnieje w bazie firm
            company = db.query(models.Company).filter(models.Company.ticker == ticker).first()
            if not company:
                 raise HTTPException(status_code=404, detail=f"Ticker {ticker} nie został znaleziony w bazie spółek.")
            else:
                 # Ticker istnieje, ale nie ma ceny w cache - może worker jeszcze nie zdążył?
                 raise HTTPException(status_code=404, detail=f"Brak danych cenowych w pamięci podręcznej dla {ticker}. Spróbuj ponownie za chwilę.")

        # Mamy dane z cache (są już dict), walidujemy je przez Pydantic i zwracamy
        # Pydantic użyje aliasu 'market_status' dla pola 'market_status_internal'
        return schemas.LiveQuoteDetails(**cached_data)

    except HTTPException:
        raise # Przekaż dalej wyjątki HTTP (np. 404)
    except Exception as e:
        logger.error(f"Error fetching live quote from cache for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Wewnętrzny błąd serwera podczas pobierania ceny z pamięci podręcznej.")


# --- ENDPOINTY KONTROLI I STATUSU WORKERA ---

@app.post("/api/v1/worker/control/{action}", status_code=202, tags=["Worker Control"])
def control_worker(action: str, db: Session = Depends(get_db)):
    """Wysyła polecenie sterujące do workera (start, pause, resume)."""
    allowed_actions = {"start": "START_REQUESTED", "pause": "PAUSE_REQUESTED", "resume": "RESUME_REQUESTED"}
    if action not in allowed_actions:
        raise HTTPException(status_code=400, detail="Nieprawidłowa akcja. Dozwolone: start, pause, resume.")

    command = allowed_actions[action]
    try:
        # Przy starcie czyścimy też zlecenie AI, na wszelki wypadek
        if action == "start":
            crud.set_system_control_value(db, "ai_analysis_request", 'NONE')
        # Wysyłamy polecenie
        crud.set_system_control_value(db, "worker_command", command) # Ta funkcja robi commit
        logger.info(f"Command '{action}' ({command}) sent to worker.")
        return {"message": f"Polecenie '{action}' wysłane do workera."}
    except Exception as e:
        logger.error(f"Error sending command {action} to worker: {e}", exc_info=True)
        # Wycofaj zmiany, jeśli set_system_control_value zawiodło
        try: db.rollback()
        except: pass
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera podczas wysyłania komendy.")


@app.get("/api/v1/worker/status", response_model=schemas.WorkerStatus, tags=["Worker Control"])
def get_worker_status(db: Session = Depends(get_db)):
    """Pobiera aktualny status workera z bazy danych."""
    try:
        # Pobieramy wszystkie wartości jednym zapytaniem dla wydajności
        keys_to_fetch = ['worker_status', 'current_phase', 'scan_progress_processed', 'scan_progress_total', 'last_heartbeat', 'scan_log']
        query = db.query(models.SystemControl).filter(models.SystemControl.key.in_(keys_to_fetch))
        results = {row.key: row.value for row in query.all()}

        # Przygotowujemy dane odpowiedzi z wartościami domyślnymi
        status_data = {
            "status": results.get("worker_status", "UNKNOWN"),
            "phase": results.get("current_phase", "NONE"),
            "progress": {
                "processed": int(results.get("scan_progress_processed", 0)),
                # Zapewniamy, że total nie jest 0, aby uniknąć dzielenia przez zero w UI
                "total": int(results.get("scan_progress_total", 1) or 1)
            },
            # Zwracamy ISO format string
            "last_heartbeat_utc": results.get("last_heartbeat", ""),
            "log": results.get("scan_log", "")
        }
        # Walidacja przez Pydantic przed zwróceniem
        return schemas.WorkerStatus(**status_data)
    except Exception as e:
        logger.error(f"Error fetching worker status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Nie można pobrać statusu workera z bazy danych.")

@app.get("/api/v1/system/alert", response_model=schemas.SystemAlert, tags=["System"])
def get_system_alert(db: Session = Depends(get_db)):
    """Pobiera aktualny alert systemowy i oznacza go jako przeczytany (ustawia na 'NONE')."""
    alert_message = crud.get_system_control_value(db, "system_alert")

    # Jeśli alert istnieje i nie jest 'NONE'
    if alert_message and alert_message != 'NONE':
         try:
            # Ustawiamy z powrotem na 'NONE'
            crud.set_system_control_value(db, "system_alert", "NONE") # Ta funkcja robi commit
            # Zwracamy oryginalną wiadomość
            return schemas.SystemAlert(message=alert_message)
         except Exception as e:
              logger.error(f"Error clearing system alert: {e}", exc_info=True)
              # W razie błędu czyszczenia, nadal zwracamy alert, ale logujemy problem
              return schemas.SystemAlert(message=alert_message) # Lepiej pokazać alert niż go zgubić
    else:
        # Jeśli nie ma alertu lub jest 'NONE', zwracamy 'NONE'
        return schemas.SystemAlert(message="NONE")

