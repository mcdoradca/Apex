import logging
from sqlalchemy.orm import Session
from sqlalchemy import Row, text
from datetime import datetime, timezone

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
# === KROK 4 (Placeholder): Monitor Wirtualnego Agenta ===
# Ta funkcja będzie uruchamiana raz na dobę (przez main.py),
# aby zamknąć transakcje, które wygasły (po 7 dniach)
# lub zostały zamknięte przez monitory SL/TP.
# ==================================================================

def run_virtual_trade_monitor(session: Session, api_client):
    """
    Główna funkcja monitorująca Wirtualnego Agenta (uruchamiana np. raz na dobę).
    Zamyka pozycje, które osiągnęły 7-dniowy horyzont czasowy
    lub zostały zamknięte przez Strażnika SL/TP.
    """
    logger.info("[Virtual Agent] Uruchamianie monitora dobowego (Krok 4)...")
    
    try:
        # Na razie zostawiamy pustą implementację.
        # Wypełnimy ją w Kroku 4.
        logger.info("[Virtual Agent] Monitor dobowy jeszcze nie zaimplementowany (Krok 4).")
        pass
        
    except Exception as e:
        logger.error(f"[Virtual Agent] Błąd w monitorze dobowym: {e}", exc_info=True)
        session.rollback()
