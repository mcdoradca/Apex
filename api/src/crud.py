from sqlalchemy.orm import Session
from sqlalchemy import text, desc, func, update, delete
from . import models, schemas
from typing import Optional, Any, Dict, List
from datetime import date, datetime, timezone
import logging
# Dodajemy import json do obsługi deserializacji
import json
# Używamy Decimal do precyzyjnych obliczeń finansowych
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)

# Funkcja pomocnicza do bezpiecznej konwersji na Decimal z 4 miejscami
def to_decimal(value, precision='0.0001') -> Optional[Decimal]:
    """Konwertuje float lub str na Decimal z określoną precyzją."""
    if value is None:
        return None
    try:
        # Używamy standardowego zaokrąglania
        return Decimal(str(value)).quantize(Decimal(precision), rounding=ROUND_HALF_UP)
    except Exception:
        logger.error(f"Nie można przekonwertować {value} na Decimal.")
        return None

# ==========================================================
# === CRUD DLA PORTFELA I TRANSAKCJI ===
# ==========================================================

def get_portfolio_holdings(db: Session) -> List[models.PortfolioHolding]:
    """Pobiera wszystkie aktualnie otwarte pozycje z portfela."""
    # Zwracamy obiekty ORM, Pydantic zajmie się konwersją
    return db.query(models.PortfolioHolding).order_by(models.PortfolioHolding.ticker).all()

def get_transaction_history(db: Session, limit: int = 100) -> List[models.TransactionHistory]:
    """Pobiera historię ostatnich transakcji."""
    # Zwracamy obiekty ORM
    return db.query(models.TransactionHistory).order_by(desc(models.TransactionHistory.transaction_date)).limit(limit).all()

def record_buy_transaction(db: Session, buy_request: schemas.BuyRequest) -> models.PortfolioHolding:
    """Rejestruje transakcję KUPNA i aktualizuje/tworzy pozycję w portfelu."""
    ticker = buy_request.ticker.strip().upper()
    quantity_bought = buy_request.quantity
    # Konwertujemy cenę z float (ze schematu) na Decimal do obliczeń
    price_per_share = to_decimal(buy_request.price_per_share)

    if price_per_share is None:
        raise ValueError("Nieprawidłowa cena zakupu.")

    # === Zapewnienie istnienia tickera w tabeli 'companies' ===
    company_exists = db.query(models.Company).filter(models.Company.ticker == ticker).first()
    if not company_exists:
        logger.warning(f"Ticker {ticker} not found in 'companies' table. Adding it automatically.")
        new_company = models.Company(
            ticker=ticker,
            company_name=f"{ticker} (Auto-Added)", # Można by to później uzupełnić
            exchange="N/A",
            industry="N/A",
            sector="N/A"
        )
        db.add(new_company)
        # Commitujemy od razu, aby spełnić ograniczenie klucza obcego PRZED zapisem transakcji
        try:
            db.commit()
            logger.info(f"Successfully auto-added company {ticker}.")
        except Exception as e_add_comp:
            db.rollback()
            logger.error(f"Nie udało się automatycznie dodać firmy {ticker}: {e_add_comp}")
            # Rzucamy błąd, bo transakcja nie może się powieść bez firmy
            raise ValueError(f"Ticker {ticker} nie istnieje i nie można go było automatycznie dodać.")

    # === Zapis transakcji do historii (w osobnej transakcji) ===
    db_history = models.TransactionHistory(
        ticker=ticker,
        transaction_type='BUY',
        quantity=quantity_bought,
        price_per_share=price_per_share, # Zapisujemy Decimal
        transaction_date=datetime.now(timezone.utc)
    )
    db.add(db_history)
    try:
        db.commit()
        db.refresh(db_history) # Odśwież, aby pobrać ID itp.
        logger.info(f"BUY transaction for {ticker} recorded in history (ID: {db_history.id}).")
    except Exception as e_hist:
        db.rollback()
        logger.error(f"Nie udało się zapisać transakcji BUY do historii dla {ticker}: {e_hist}", exc_info=True)
        raise # Rzuć błąd dalej, bo nie można kontynuować bez zapisu historii

    # === Aktualizacja/tworzenie pozycji w portfelu (w nowej transakcji) ===
    try:
        # Używamy .with_for_update() aby zablokować wiersz na czas aktualizacji
        holding = db.query(models.PortfolioHolding).filter(models.PortfolioHolding.ticker == ticker).with_for_update().first()

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

            if new_total_quantity == 0: # Teoretycznie niemożliwe przy zakupie
                 new_average_price = Decimal(0)
            else:
                 # Używamy precyzji 4 miejsc po przecinku dla średniej ceny
                 new_average_price = (new_total_cost / new_total_quantity).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)

            holding.quantity = int(new_total_quantity) # Konwertuj z powrotem na int
            holding.average_buy_price = new_average_price # Zapisujemy Decimal
            holding.last_updated = datetime.now(timezone.utc)
            logger.info(f"Updated existing holding for {ticker}.")
        else:
            # Stwórz nową pozycję
            holding = models.PortfolioHolding(
                ticker=ticker,
                quantity=quantity_bought,
                average_buy_price=price_per_share, # Zapisujemy Decimal
                first_purchase_date=datetime.now(timezone.utc),
                last_updated=datetime.now(timezone.utc)
            )
            db.add(holding)
            logger.info(f"Created new holding for {ticker}.")

        db.commit() # Zatwierdź zmiany w portfelu
        db.refresh(holding) # Odśwież obiekt holding
        return holding
    except Exception as e_portfolio:
        db.rollback() # Wycofaj zmiany w portfelu
        logger.error(f"Nie udało się zaktualizować/stworzyć pozycji w portfelu dla {ticker} po zakupie: {e_portfolio}", exc_info=True)
        # UWAGA: W tym momencie transakcja jest już w historii, ale portfel nie został zaktualizowany.
        # Należy rozważyć mechanizm kompensacyjny lub oznaczenie transakcji historycznej jako nieudanej.
        # Na razie rzucamy błąd dalej.
        raise


def record_sell_transaction(db: Session, sell_request: schemas.SellRequest) -> Optional[models.PortfolioHolding]:
    """
    Rejestruje transakcję SPRZEDAŻY, aktualizuje pozycję w portfelu (lub ją usuwa)
    i zwraca zaktualizowaną pozycję lub None, jeśli została zamknięta.
    Wykonuje operacje na historii i portfelu w jednej transakcji.
    """
    ticker = sell_request.ticker.strip().upper()
    quantity_sold = sell_request.quantity
    price_per_share = to_decimal(sell_request.price_per_share)

    if price_per_share is None:
        raise ValueError("Nieprawidłowa cena sprzedaży.")

    try:
        # Rozpoczynamy transakcję (domyślnie SQLAlchemy ORM działa w transakcji)
        holding = db.query(models.PortfolioHolding).filter(models.PortfolioHolding.ticker == ticker).with_for_update().first()

        # Walidacja
        if not holding:
            raise ValueError(f"Nie posiadasz akcji {ticker} w portfelu.")
        if holding.quantity < quantity_sold:
            raise ValueError(f"Próba sprzedaży {quantity_sold} akcji {ticker}, ale posiadasz tylko {holding.quantity}.")

        # Oblicz zysk/stratę
        average_buy_price = Decimal(holding.average_buy_price)
        quantity_sold_dec = Decimal(quantity_sold)
        profit_loss = (price_per_share - average_buy_price) * quantity_sold_dec
        # Zaokrąglij P/L do 2 miejsc po przecinku
        profit_loss = profit_loss.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        # Przygotuj wpis do historii
        db_history = models.TransactionHistory(
            ticker=ticker,
            transaction_type='SELL',
            quantity=quantity_sold,
            price_per_share=price_per_share, # Zapisujemy Decimal
            transaction_date=datetime.now(timezone.utc),
            profit_loss_usd=profit_loss # Zapisujemy Decimal
        )
        db.add(db_history)
        logger.info(f"Prepared history entry for SELL {ticker} (P/L: {profit_loss}).")

        # Zaktualizuj lub usuń pozycję w portfelu
        remaining_quantity = holding.quantity - quantity_sold

        updated_holding_ref = None # Zmienna do przechowania referencji zwracanego obiektu
        if remaining_quantity == 0:
            # Sprzedano całość - usuń pozycję
            db.delete(holding)
            logger.info(f"Position {ticker} marked for deletion (full sell).")
            # Nie ustawiamy updated_holding_ref, funkcja zwróci None
        else:
            # Sprzedaż częściowa - zaktualizuj ilość (średnia cena zakupu się nie zmienia)
            holding.quantity = remaining_quantity
            holding.last_updated = datetime.now(timezone.utc)
            updated_holding_ref = holding # Zachowujemy referencję do zwrócenia
            logger.info(f"Position {ticker} marked for update (partial sell). Remaining: {remaining_quantity}.")

        # Zatwierdzamy obie operacje (historia + portfel) atomowo
        db.commit()
        logger.info(f"SELL transaction for {ticker} committed successfully.")

        # Odświeżamy referencję PO commicie, jeśli istnieje (dla sprzedaży częściowej)
        if updated_holding_ref:
            try:
                db.refresh(updated_holding_ref)
                return updated_holding_ref
            except Exception as e_refresh:
                 # Jeśli odświeżenie zawiedzie (bardzo rzadkie), logujemy, ale kontynuujemy
                 logger.error(f"Failed to refresh holding object for {ticker} after partial sell commit: {e_refresh}")
                 # Zwracamy obiekt w stanie przed odświeżeniem - może być niekompletny
                 return updated_holding_ref
        else:
            return None # Dla sprzedaży całkowitej

    except Exception as e:
        db.rollback() # Wycofujemy obie zmiany (historia i portfel)
        logger.error(f"Nie udało się przetworzyć sprzedaży {ticker}: {e}", exc_info=True)
        # Rzucamy błąd dalej, aby endpoint zwrócił 500 lub odpowiedni kod błędu
        raise


# ==========================================================
# === CRUD DLA FAZ ANALIZY I SYSTEMU ===
# ==========================================================

def get_phase1_candidates(db: Session) -> List[models.Phase1Candidate]:
    """Pobiera wszystkich kandydatów z Fazy 1 z najnowszego dnia analizy."""
    # Używamy DATE(analysis_date) dla porównania z CURRENT_DATE
    candidates_from_db = db.query(models.Phase1Candidate).filter(
        func.date(models.Phase1Candidate.analysis_date) == func.current_date()
    ).order_by(models.Phase1Candidate.ticker).all()

    # Zwracamy listę obiektów ORM, Pydantic zajmie się konwersją
    return candidates_from_db


def get_phase2_results(db: Session) -> List[models.Phase2Result]:
    """Pobiera wszystkie wyniki Fazy 2 (tylko zakwalifikowane) z najnowszego dnia analizy."""
    latest_date = db.query(func.max(models.Phase2Result.analysis_date)).scalar()
    if not latest_date:
        return []

    results_from_db = db.query(models.Phase2Result).filter(
        models.Phase2Result.analysis_date == latest_date,
        models.Phase2Result.is_qualified == True
    ).order_by(desc(models.Phase2Result.total_score)).all()

    # Zwracamy listę obiektów ORM
    return results_from_db

# Zmieniono nazwę funkcji, aby odzwierciedlała pobieranie również TRIGGERED
def get_active_pending_triggered_signals(db: Session) -> List[models.TradingSignal]:
    """Pobiera aktywne (ACTIVE), oczekujące (PENDING) i wyzwolone (TRIGGERED) sygnały."""
    signals_from_db = db.query(models.TradingSignal).filter(
        models.TradingSignal.status.in_(['ACTIVE', 'PENDING', 'TRIGGERED'])
    ).order_by(models.TradingSignal.ticker).all() # Sortowanie alfabetyczne

    # Zwracamy listę obiektów ORM
    return signals_from_db


def get_system_control_value(db: Session, key: str) -> Optional[str]:
    """Odczytuje pojedynczą wartość z tabeli system_control."""
    try:
        result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting system_control value for key '{key}': {e}")
        # W przypadku bazy danych, lepiej rzucić błąd niż zwrócić None, co może być mylące
        raise

def set_system_control_value(db: Session, key: str, value: str):
    """Aktualizuje lub wstawia wartość w tabeli system_control (UPSERT)."""
    # Używamy MERGE dla standardowego SQL lub ON CONFLICT dla PostgreSQL
    # Tutaj użyjemy ON CONFLICT, zakładając PostgreSQL
    stmt = text("""
        INSERT INTO system_control (key, value, updated_at)
        VALUES (:key, :value, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = NOW();
    """)
    try:
        db.execute(stmt, [{'key': key, 'value': str(value)}]) # Upewnijmy się, że value jest stringiem
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error setting system control value for key '{key}': {e}", exc_info=True)
        # Rzucamy błąd dalej
        raise

def get_ai_analysis_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    """Pobiera wynik analizy AI jako słownik Python."""
    try:
        # analysis_data jest typu JSONB, SQLAlchemy >1.4 zwraca go jako dict
        result = db.query(models.AIAnalysisResult.analysis_data).filter(models.AIAnalysisResult.ticker == ticker).first()
        
        if not result:
            return None
        
        data = result[0]
        
        # === DODATKOWA POPRAWKA ODPORNOŚCI (API/CRUD) ===
        # Na wypadek, gdyby dane AI również były zapisane jako string
        if isinstance(data, str):
            logger.warning(f"AI Analysis data for {ticker} was stored as string. Attempting JSON load.")
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode cached string AI Analysis data for {ticker}.")
                return None
        
        return data
        
    except Exception as e:
         logger.error(f"Error getting AI analysis result for {ticker}: {e}")
         raise

def delete_ai_analysis_result(db: Session, ticker: str):
    """Usuwa istniejący wynik analizy AI dla danego tickera."""
    try:
        deleted_count = db.query(models.AIAnalysisResult).filter(models.AIAnalysisResult.ticker == ticker).delete(synchronize_session=False)
        db.commit()
        if deleted_count > 0:
             logger.info(f"Deleted previous AI analysis result for {ticker}.")
        # Jeśli nie było rekordu, to też jest OK, nie logujemy błędu
    except Exception as e:
        db.rollback() # Wycofaj zmiany w razie błędu
        logger.error(f"Error deleting AI analysis result for {ticker}: {e}", exc_info=True)
        # Nie rzucamy błędu, bo to nie jest krytyczne dla zlecenia analizy,
        # ale logujemy, żeby wiedzieć o problemie.


# === POPRAWKA BŁĘDU #5: Nowa funkcja do czytania cen z cache ===
# === POPRAWKA BŁĘDU (TypeError): Dodano odporność na dane typu string ===

def get_live_price_from_cache(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    """
    Pobiera najnowsze dane cenowe (jako dict) z tabeli cache.
    Zwraca słownik zgodny ze schematem schemas.LiveQuoteDetails (lub None).
    """
    try:
        result = db.query(models.LivePriceCache.quote_data).filter(models.LivePriceCache.ticker == ticker).first()
        
        if not result:
            return None
            
        data = result[0]
        
        # === POCZĄTEK POPRAWKI (API) ===
        # Jeśli dane z bazy są stringiem (z powodu błędu zapisu),
        # spróbuj je sparsować jako JSON.
        if isinstance(data, str):
            logger.warning(f"Cache data for {ticker} was stored as string. Attempting JSON load.")
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                logger.error(f"Failed to decode cached string data for {ticker}.")
                return None
        # === KONIEC POPRAWKI (API) ===
            
        # SQLAlchemy (jeśli dane są poprawnie zapisane jako JSONB) 
        # automatycznie zwróci 'data' jako dict.
        return data
        
    except Exception as e:
        logger.error(f"Error getting live price from cache for {ticker}: {e}")
        # Rzucamy błąd, aby endpoint API wiedział o problemie
        raise

