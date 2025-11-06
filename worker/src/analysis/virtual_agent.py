import logging
from sqlalchemy.orm import Session
from sqlalchemy import Row, text
from datetime import datetime, timezone, timedelta
# Importy dla parsowania CSV i zapytań API
import csv
from io import StringIO
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import safe_float

# Używamy modeli zdefiniowanych w głównym module
from .. import models

logger = logging.getLogger(__name__)

def _parse_setup_type_from_notes(notes: str) -> str:
    """Prosta funkcja pomocnicza do wyciągania typu setupu z notatki sygnału."""
    if not notes:
        return "UNKNOWN"
    notes_lower = notes.lower()
    if "fib" in notes_lower:
        return "FIB_H1"
    if "ema" in notes_lower:
        return "EMA_BOUNCE"
    if "breakout" in notes_lower or "wybicie" in notes_lower:
        return "BREAKOUT"
    return "OTHER"

def open_virtual_trade(session: Session, signal: Row):
    """
    Wywoływane, gdy sygnał PENDING przechodzi na ACTIVE.
    Tworzy nowy wpis w tabeli 'virtual_trades' do śledzenia.
    """
    signal_id = signal.id
    logger.info(f"[Virtual Agent] Otrzymano sygnał aktywacji dla signal_id: {signal_id} ({signal.ticker})")

    try:
        # Krok 1: Sprawdź, czy już nie otworzyliśmy tej transakcji (zabezpieczenie)
        existing_trade = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.signal_id == signal_id
        ).first()
        
        if existing_trade:
            logger.warning(f"[Virtual Agent] Wirtualna transakcja dla signal_id: {signal_id} już istnieje. Pomijanie.")
            return

        # Krok 2: Wyciągnij dane z sygnału
        setup_type = _parse_setup_type_from_notes(signal.notes)
        
        # Ustal cenę wejścia:
        # Dla Breakout/EMA to zdefiniowana 'entry_price'
        # Dla Fib to 'entry_zone_top' (górna granica strefy aktywacji)
        entry_price_for_trade = signal.entry_price if signal.entry_price is not None else signal.entry_zone_top

        # Walidacja: Nie możemy otworzyć transakcji bez ceny wejścia lub stop-lossa
        if entry_price_for_trade is None or signal.stop_loss is None:
            logger.error(f"[Virtual Agent] Nie można otworzyć wirtualnej transakcji dla {signal.ticker} (signal_id: {signal_id}). Brak ceny wejścia lub stop lossa.")
            return
            
        # Krok 3: Stwórz nowy obiekt VirtualTrade
        new_trade = models.VirtualTrade(
            signal_id=signal_id,
            ticker=signal.ticker,
            status='OPEN', # Nowa transakcja jest zawsze 'OPEN'
            setup_type=setup_type,
            entry_price=entry_price_for_trade,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            open_date=datetime.now(timezone.utc) # Zapisz dokładny czas aktywacji
            # close_date, close_price, final_p_l pozostają NULL
        )
        
        # Krok 4: Zapisz w bazie
        session.add(new_trade)
        session.commit()
        
        logger.info(f"✅ [Virtual Agent] Wirtualna transakcja OTWARTA dla {signal.ticker} @ {entry_price_for_trade:.2f} (Setup: {setup_type})")

    except Exception as e:
        logger.error(f"[Virtual Agent] Błąd krytyczny podczas otwierania wirtualnej transakcji dla {signal.ticker} (signal_id: {signal_id}): {e}", exc_info=True)
        session.rollback()

# ==================================================================
# === KROK 3: Implementacja Mózgu Wirtualnego Agenta ===
# ==================================================================

def _parse_bulk_quotes_for_virtual_agent(csv_text: str) -> dict:
    """
    Parsuje odpowiedź CSV z REALTIME_BULK_QUOTES i zwraca słownik
    mapujący ticker na cenę (price).
    """
    if not csv_text or "symbol" not in csv_text:
        logger.warning("[Virtual Agent] Otrzymane dane CSV (Bulk Quotes) są puste lub nieprawidłowe.")
        return {}
    
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    
    price_dict = {}
    for row in reader:
        ticker = row.get('symbol')
        price = safe_float(row.get('close')) # 'close' to aktualna cena w BULK
        if ticker and price is not None:
            price_dict[ticker] = price
    return price_dict

def run_virtual_trade_monitor(session: Session, api_client: AlphaVantageClient):
    """
    Główna funkcja monitorująca Wirtualnego Agenta (uruchamiana np. raz na dobę).
    Zamyka pozycje, które osiągnęły 7-dniowy horyzont czasowy
    lub zostały zamknięte przez Strażnika SL/TP.
    """
    logger.info("🤖 [Virtual Agent] Uruchamianie monitora dobowego (Krok 3)...")
    
    try:
        # Krok 1: Pobierz wszystkie otwarte wirtualne transakcje
        open_trades = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.status == 'OPEN'
        ).all()

        if not open_trades:
            logger.info("🤖 [Virtual Agent] Brak otwartych wirtualnych transakcji do analizy.")
            return

        logger.info(f"🤖 [Virtual Agent] Znaleziono {len(open_trades)} otwartych transakcji do weryfikacji.")

        # Krok 2: Sprawdź statusy powiązanych sygnałów (TP/SL)
        tickers_to_check_expiry = []
        now = datetime.now(timezone.utc)

        for trade in open_trades:
            signal = session.query(models.TradingSignal).filter(
                models.TradingSignal.id == trade.signal_id
            ).first()

            # --- Scenariusz A: Sygnał został zamknięty przez Strażnika (TP/SL) ---
            if signal and signal.status == 'COMPLETED':
                logger.info(f"🤖 [Virtual Agent] Transakcja {trade.ticker} (ID: {trade.id}) zamknięta przez Strażnika (TAKE PROFIT).")
                trade.status = 'CLOSED_TP'
                trade.close_date = signal.updated_at
                trade.close_price = signal.take_profit
            
            elif signal and signal.status == 'INVALIDATED':
                logger.info(f"🤖 [Virtual Agent] Transakcja {trade.ticker} (ID: {trade.id}) zamknięta przez Strażnika (STOP LOSS).")
                trade.status = 'CLOSED_SL'
                trade.close_date = signal.updated_at
                # Uwaga: zamykamy po cenie SL, nawet jeśli rynek otworzył się niżej (zgodnie z planem)
                trade.close_price = signal.stop_loss

            # --- Scenariusz B: Sygnał wciąż aktywny, ale wygasa (7 dni) ---
            elif (now - trade.open_date) > timedelta(days=7):
                logger.info(f"🤖 [Virtual Agent] Transakcja {trade.ticker} (ID: {trade.id}) wygasła (7 dni). Oznaczanie do zamknięcia rynkowego.")
                tickers_to_check_expiry.append(trade.ticker)
            
            # --- Scenariusz C: Sygnał osierocony (nie znaleziono w trading_signals) ---
            elif not signal:
                 logger.warning(f"🤖 [Virtual Agent] Transakcja {trade.ticker} (ID: {trade.id}) jest osierocona (brak sygnału). Oznaczanie do zamknięcia rynkowego.")
                 tickers_to_check_expiry.append(trade.ticker)
            
            # --- Obliczanie P/L dla zamkniętych transakcji ---
            if trade.status != 'OPEN' and trade.close_price is not None:
                # Oblicz P/L %
                try:
                    p_l_percent = ((trade.close_price - trade.entry_price) / trade.entry_price) * 100
                    trade.final_profit_loss_percent = p_l_percent
                except Exception as e:
                    logger.error(f"Błąd obliczania P/L dla {trade.ticker}: {e}")
                    trade.final_profit_loss_percent = 0 # Błąd (np. dzielenie przez zero)

        # Zapisz zmiany dla transakcji zamkniętych przez TP/SL
        session.commit()

        # Krok 3: Obsługa transakcji, które wygasły (Wymaga zapytania API)
        if tickers_to_check_expiry:
            logger.info(f"🤖 [Virtual Agent] Pobieranie aktualnych cen dla {len(tickers_to_check_expiry)} wygasłych transakcji...")
            
            unique_tickers = list(set(tickers_to_check_expiry))
            bulk_csv = api_client.get_bulk_quotes(unique_tickers)
            
            if not bulk_csv:
                logger.error("🤖 [Virtual Agent] Nie otrzymano cen z API dla wygasłych transakcji. Spróbuję ponownie jutro.")
                return

            parsed_prices = _parse_bulk_quotes_for_virtual_agent(bulk_csv)
            
            # Krok 4: Druga pętla - zamykanie wygasłych transakcji
            # (Musimy ponownie odpytać bazę, ponieważ `open_trades` jest nieaktualne po commicie)
            expired_trades = session.query(models.VirtualTrade).filter(
                models.VirtualTrade.status == 'OPEN',
                models.VirtualTrade.ticker.in_(unique_tickers)
            ).all()

            for trade in expired_trades:
                current_price = parsed_prices.get(trade.ticker)
                
                if current_price:
                    logger.info(f"🤖 [Virtual Agent] Zamykanie wygasłej transakcji {trade.ticker} (ID: {trade.id}) po cenie rynkowej {current_price:.2f}.")
                    trade.status = 'CLOSED_EXPIRED'
                    trade.close_date = now
                    trade.close_price = current_price
                    try:
                        p_l_percent = ((trade.close_price - trade.entry_price) / trade.entry_price) * 100
                        trade.final_profit_loss_percent = p_l_percent
                    except Exception as e:
                        logger.error(f"Błąd obliczania P/L dla wygasłego {trade.ticker}: {e}")
                        trade.final_profit_loss_percent = 0
                else:
                    logger.warning(f"🤖 [Virtual Agent] Nie znaleziono ceny dla wygasłej transakcji {trade.ticker}. Zostanie zamknięta jutro.")
            
            # Zapisz zmiany dla transakcji wygasłych
            session.commit()

        logger.info("🤖 [Virtual Agent] Monitor dobowy zakończył pracę.")
        
    except Exception as e:
        logger.error(f"🤖 [Virtual Agent] Błąd krytyczny w monitorze dobowym: {e}", exc_info=True)
        session.rollback()
