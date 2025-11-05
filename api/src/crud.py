from sqlalchemy.orm import Session
from sqlalchemy import text, desc, func, update, delete # Dodano update, delete
from . import models, schemas # Dodano import schemas
from typing import Optional, Any, Dict, List
# KROK 4d: Dodano 'timedelta' do obliczeń 24-godzinnych
from datetime import date, datetime, timezone, timedelta 
import logging
# NOWY IMPORT dla typów Decimal, które są lepsze do obliczeń finansowych
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)

# Funkcja pomocnicza do bezpiecznej konwersji na Decimal
def to_decimal(value, precision='0.0001') -> Optional[Decimal]:
    """Konwertuje float lub str na Decimal z określoną precyzją."""
    if value is None:
        return None
    try:
        # Używamy ROUND_HALF_UP dla standardowego zaokrąglania
        return Decimal(str(value)).quantize(Decimal(precision), rounding=ROUND_HALF_UP)
    except Exception:
        logger.error(f"Nie można przekonwertować {value} na Decimal.")
        return None

# ==========================================================
# === NOWE FUNKCJE CRUD DLA PORTFELA I TRANSAKCJI ===
# ==========================================================

def get_portfolio_holdings(db: Session) -> List[schemas.PortfolioHolding]:
    """
    Pobiera wszystkie aktualnie otwarte pozycje z portfela,
    dołączając docelową cenę (take_profit) z aktywnych lub oczekujących sygnałów.
    """
    # Wykonujemy zapytanie z LEFT OUTER JOIN
    results = db.query(
        models.PortfolioHolding,
        models.TradingSignal.take_profit
    ).outerjoin(
        models.TradingSignal,
        (models.PortfolioHolding.ticker == models.TradingSignal.ticker) &
        (models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])) # Łączymy tylko z istotnymi sygnałami
    ).order_by(models.PortfolioHolding.ticker).all()

    # Przetwarzamy wyniki
    holdings_with_tp = []
    for (holding, take_profit) in results:
        # Konwertujemy model ORM 'holding' na schemat Pydantic 'PortfolioHolding'
        # (dzięki config: from_attributes=True w schemas.py)
        holding_schema = schemas.PortfolioHolding.model_validate(holding)
        
        # Ręcznie ustawiamy dodatkowe pole 'take_profit'
        holding_schema.take_profit = float(take_profit) if take_profit is not None else None
        
        holdings_with_tp.append(holding_schema)
    
    return holdings_with_tp

def get_transaction_history(db: Session, limit: int = 100) -> List[models.TransactionHistory]:
    """Pobiera historię ostatnich transakcji."""
    return db.query(models.TransactionHistory).order_by(desc(models.TransactionHistory.transaction_date)).limit(limit).all()

def record_buy_transaction(db: Session, buy_request: schemas.BuyRequest) -> models.PortfolioHolding:
    """Rejestruje transakcję KUPNA i aktualizuje/tworzy pozycję w portfelu."""
    ticker = buy_request.ticker.strip().upper()
    quantity_bought = buy_request.quantity
    price_per_share = to_decimal(buy_request.price_per_share)

    if price_per_share is None:
        raise ValueError("Nieprawidłowa cena zakupu.")

    # === POPRAWKA Z POPRZEDNIEJ RUNDY: Zapewnienie istnienia tickera w tabeli 'companies' ===
    company_exists = db.query(models.Company).filter(models.Company.ticker == ticker).first()
    if not company_exists:
        logger.warning(f"Ticker {ticker} not found in 'companies' table. Adding it automatically to satisfy foreign key constraint.")
        new_company = models.Company(
            ticker=ticker,
            company_name=f"{ticker} (Dodany przez Portfel)",
            exchange="N/A",
            industry="N/A",
            sector="N/A"
        )
        db.add(new_company)
    # === KONIEC POPRAWKI ===

    # 1. Zapisz transakcję w historii
    db_history = models.TransactionHistory(
        ticker=ticker,
        transaction_type='BUY',
        quantity=quantity_bought,
        price_per_share=price_per_share,
        transaction_date=datetime.now(timezone.utc) # Używamy timezone - teraz działa dzięki importowi
    )
    db.add(db_history)
    # Celowo commitujemy historię osobno, aby była zapisana nawet jeśli aktualizacja portfela zawiedzie
    try:
        db.commit()
        db.refresh(db_history)
    except Exception as e:
        db.rollback()
        logger.error(f"Nie udało się zapisać transakcji BUY do historii dla {ticker}: {e}", exc_info=True)
        raise # Rzuć błąd dalej

    # 2. Zaktualizuj lub stwórz pozycję w portfelu
    holding = db.query(models.PortfolioHolding).filter(models.PortfolioHolding.ticker == ticker).with_for_update().first() # Blokujemy wiersz do aktualizacji

    if holding:
        # Aktualizuj istniejącą pozycję
        current_quantity = Decimal(holding.quantity)
        current_avg_price = Decimal(holding.average_buy_price)
        new_quantity_dec = Decimal(quantity_bought)

        # Oblicz nową średnią cenę ważoną
        total_cost_before = current_quantity * current_avg_price
        cost_of_new_shares = new_quantity_dec * price_per_share
        new_total_quantity = current_quantity + new_quantity_dec
        new_total_cost = total_cost_before + cost_of_new_shares

        if new_total_quantity == 0: # Teoretycznie niemożliwe przy zakupie, ale dla bezpieczeństwa
             new_average_price = Decimal(0)
        else:
             new_average_price = (new_total_cost / new_total_quantity).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

        holding.quantity = int(new_total_quantity) # Konwertuj z powrotem na int
        holding.average_buy_price = new_average_price
        holding.last_updated = datetime.now(timezone.utc)
    else:
        # Stwórz nową pozycję
        holding = models.PortfolioHolding(
            ticker=ticker,
            quantity=quantity_bought,
            average_buy_price=price_per_share,
            first_purchase_date=datetime.now(timezone.utc),
            last_updated=datetime.now(timezone.utc)
        )
        db.add(holding)

    try:
        db.commit()
        db.refresh(holding)
        return holding
    except Exception as e:
        db.rollback()
        logger.error(f"Nie udało się zaktualizować/stworzyć pozycji w portfelu dla {ticker} po zakupie: {e}", exc_info=True)
        # Wycofanie transakcji z historii? Można dodać taką logikę, jeśli to krytyczne.
        raise # Rzuć błąd dalej


def record_sell_transaction(db: Session, sell_request: schemas.SellRequest) -> Optional[models.PortfolioHolding]:
    """
    Rejestruje transakcję SPRZEDAŻY, aktualizuje pozycję w portfelu (lub ją usuwa)
    i zwraca zaktualizowaną pozycję lub None, jeśli została zamknięta.
    """
    ticker = sell_request.ticker.strip().upper()
    quantity_sold = sell_request.quantity
    price_per_share = to_decimal(sell_request.price_per_share)

    if price_per_share is None:
        raise ValueError("Nieprawidłowa cena sprzedaży.")

    # 1. Sprawdź, czy pozycja istnieje i czy mamy wystarczająco akcji
    holding = db.query(models.PortfolioHolding).filter(models.PortfolioHolding.ticker == ticker).with_for_update().first()

    if not holding:
        raise ValueError(f"Nie posiadasz akcji {ticker} w portfelu.")
    if holding.quantity < quantity_sold:
        raise ValueError(f"Próba sprzedaży {quantity_sold} akcji {ticker}, ale posiadasz tylko {holding.quantity}.")

    # 2. Oblicz zysk/stratę dla tej transakcji
    average_buy_price = Decimal(holding.average_buy_price)
    quantity_sold_dec = Decimal(quantity_sold)
    profit_loss = (price_per_share - average_buy_price) * quantity_sold_dec
    profit_loss = profit_loss.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) # Zaokrąglij do centów

    # 3. Zapisz transakcję w historii
    db_history = models.TransactionHistory(
        ticker=ticker,
        transaction_type='SELL',
        quantity=quantity_sold,
        price_per_share=price_per_share,
        transaction_date=datetime.now(timezone.utc), # Używamy timezone - teraz działa dzięki importowi
        profit_loss_usd=profit_loss # Zapisujemy obliczony P/L
    )
    db.add(db_history)
    try:
        db.commit()
        db.refresh(db_history)
    except Exception as e:
        db.rollback()
        logger.error(f"Nie udało się zapisać transakcji SELL do historii dla {ticker}: {e}", exc_info=True)
        raise

    # 4. Zaktualizuj lub usuń pozycję w portfelu
    remaining_quantity = holding.quantity - quantity_sold

    if remaining_quantity == 0:
        # Sprzedano całość - usuń pozycję z portfela
        db.delete(holding)
        logger.info(f"Pozycja {ticker} została zamknięta.")
        try:
             db.commit()
             return None # Zwracamy None, sygnalizując zamknięcie pozycji
        except Exception as e:
            db.rollback()
            logger.error(f"Nie udało się usunąć pozycji {ticker} z portfela po sprzedaży: {e}", exc_info=True)
            # Wycofanie transakcji z historii?
            raise
    else:
        # Sprzedaż częściowa - zaktualizuj ilość (średnia cena zakupu się nie zmienia)
        holding.quantity = remaining_quantity
        holding.last_updated = datetime.now(timezone.utc)
        try:
            db.commit()
            db.refresh(holding)
            logger.info(f"Pozycja {ticker} zaktualizowana po sprzedaży częściowej. Pozostało: {remaining_quantity} akcji.")
            return holding # Zwracamy zaktualizowaną pozycję
        except Exception as e:
            db.rollback()
            logger.error(f"Nie udało się zaktualizować pozycji {ticker} w portfelu po sprzedaży częściowej: {e}", exc_info=True)
            # Wycofanie transakcji z historii?
            raise


# ==========================================================
# === Istniejące funkcje CRUD (z drobnymi poprawkami) ===
# ==========================================================

def get_phase1_candidates(db: Session) -> List[Dict[str, Any]]:
    """Pobiera wszystkich kandydatów z Fazy 1 z najnowszego dnia analizy."""
    # Używamy >= CURRENT_DATE, bo analysis_date to timestamp
    candidates_from_db = db.query(models.Phase1Candidate).filter(
        models.Phase1Candidate.analysis_date >= func.current_date()
    ).order_by(models.Phase1Candidate.ticker).all() # Sortowanie alfabetyczne

    # Konwersja do słowników bezpośrednio w return
    return [
        {
            "ticker": c.ticker,
            "price": float(c.price) if c.price is not None else None,
            "change_percent": float(c.change_percent) if c.change_percent is not None else None,
            "volume": c.volume,
            "score": c.score,
            "analysis_date": c.analysis_date.isoformat() if c.analysis_date else None
        } for c in candidates_from_db
    ]

def get_phase2_results(db: Session) -> List[Dict[str, Any]]:
    """Pobiera wszystkie wyniki Fazy 2 (tylko zakwalifikowane) z najnowszego dnia analizy."""
    latest_date = db.query(func.max(models.Phase2Result.analysis_date)).scalar()
    if not latest_date:
        return []

    results_from_db = db.query(models.Phase2Result).filter(
        models.Phase2Result.analysis_date == latest_date,
        models.Phase2Result.is_qualified == True
    ).order_by(desc(models.Phase2Result.total_score)).all()

    return [
        {
            "ticker": r.ticker,
            "analysis_date": r.analysis_date.isoformat() if r.analysis_date else None,
            "catalyst_score": r.catalyst_score,
            "relative_strength_score": r.relative_strength_score,
            "energy_compression_score": r.energy_compression_score,
            "total_score": r.total_score,
            "is_qualified": r.is_qualified
        } for r in results_from_db
    ]

def get_active_and_pending_signals(db: Session) -> List[Dict[str, Any]]:
    """Pobiera aktywne i oczekujące sygnały (Wyniki Fazy 3)."""
    signals_from_db = db.query(models.TradingSignal).filter(
        models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
    ).order_by(models.TradingSignal.ticker).all() # Sortowanie alfabetyczne

    return [
        {
            "id": signal.id,
            "ticker": signal.ticker,
            "generation_date": signal.generation_date.isoformat() if signal.generation_date else None,
            "status": signal.status,
            "entry_price": float(signal.entry_price) if signal.entry_price is not None else None,
            "stop_loss": float(signal.stop_loss) if signal.stop_loss is not None else None,
            "take_profit": float(signal.take_profit) if signal.take_profit is not None else None,
            "risk_reward_ratio": float(signal.risk_reward_ratio) if signal.risk_reward_ratio is not None else None,
            "signal_candle_timestamp": signal.signal_candle_timestamp.isoformat() if signal.signal_candle_timestamp else None,
            "entry_zone_bottom": float(signal.entry_zone_bottom) if signal.entry_zone_bottom is not None else None,
            "entry_zone_top": float(signal.entry_zone_top) if signal.entry_zone_top is not None else None,
            "notes": signal.notes
        } for signal in signals_from_db
    ]

# ==================================================================
# KROK 4d (Licznik): Dodanie nowej funkcji do zliczania "wyrzuconych"
# ==================================================================
def get_discarded_signals_count_24h(db: Session) -> int:
    """
    Zlicza sygnały, które zostały unieważnione (INVALIDATED) lub
    zakończone (COMPLETED) w ciągu ostatnich 24 godzin.
    """
    try:
        # Definiujemy statusy "wyrzucone" (zakończone lub unieważnione)
        discarded_statuses = ['INVALIDATED', 'COMPLETED']
        
        # Obliczamy czas 24 godziny temu
        time_24_hours_ago = datetime.now(timezone.utc) - timedelta(days=1)

        # Budujemy zapytanie
        # Zliczamy sygnały, których status należy do "wyrzuconych"
        # ORAZ których data aktualizacji jest w ciągu ostatnich 24h
        count = db.query(func.count(models.TradingSignal.id)).filter(
            models.TradingSignal.status.in_(discarded_statuses),
            models.TradingSignal.updated_at >= time_24_hours_ago
        ).scalar()
        
        return count if count is not None else 0
        
    except Exception as e:
        logger.error(f"Error counting discarded signals: {e}", exc_info=True)
        return 0 # Zwróć 0 w przypadku błędu
# ==================================================================
# Koniec Krok 4d
# ==================================================================

def delete_phase1_candidate(db: Session, ticker: str):
    # Nieużywane?
    db.query(models.Phase1Candidate).filter(models.Phase1Candidate.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Candidate {ticker} from Phase 1 deleted."}

def delete_phase2_result(db: Session, ticker: str):
    # Nieużywane?
    db.query(models.Phase2Result).filter(models.Phase2Result.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Result {ticker} from Phase 2 deleted."}

def delete_trading_signal(db: Session, signal_id: int):
    # Zmieniamy status zamiast usuwać fizycznie?
    signal = db.query(models.TradingSignal).filter(models.TradingSignal.id == signal_id).first()
    if signal and signal.status in ['ACTIVE', 'PENDING']:
        signal.status = 'CANCELLED' # Używamy CANCELLED zamiast DELETED?
        signal.notes = (signal.notes or "") + " Ręcznie anulowany."
        db.commit()
        logger.info(f"Signal {signal_id} for {signal.ticker} marked as CANCELLED.")
        return {"message": f"Signal {signal_id} for {signal.ticker} marked as cancelled."}
    logger.warning(f"Signal {signal_id} not found or already closed/cancelled.")
    return None

def get_system_control_value(db: Session, key: str) -> Optional[str]:
    result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
    return result[0] if result else None

def set_system_control_value(db: Session, key: str, value: str):
    stmt = text("""
        INSERT INTO system_control (key, value, updated_at)
        VALUES (:key, :value, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = :value, updated_at = NOW();
    """)
    try:
        db.execute(stmt, [{'key': key, 'value': str(value)}]) # Upewnijmy się, że value jest stringiem
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error setting system control value for key {key}: {e}", exc_info=True)
        raise

def get_ai_analysis_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    result = db.query(models.AIAnalysisResult).filter(models.AIAnalysisResult.ticker == ticker).first()
    return result.analysis_data if result else None

def delete_ai_analysis_result(db: Session, ticker: str):
    """Usuwa istniejący wynik analizy AI dla danego tickera."""
    try:
        deleted_count = db.query(models.AIAnalysisResult).filter(models.AIAnalysisResult.ticker == ticker).delete(synchronize_session=False)
        db.commit()
        if deleted_count > 0:
             logger.info(f"Deleted previous AI analysis result for {ticker} to force refresh.")
    except Exception as e:
        logger.error(f"Error deleting AI analysis result for {ticker}: {e}", exc_info=True)
        db.rollback()
        # Nie rzucamy błędu, bo to nie jest krytyczne dla zlecenia analizy
