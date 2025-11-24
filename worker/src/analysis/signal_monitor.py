import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from .. import models
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import send_telegram_alert, append_scan_log, safe_float

logger = logging.getLogger(__name__)

# Stała dla Trailing Stopu: ile ATR od szczytu ma być oddalony stop?
TRAILING_ATR_MULTIPLIER = 2.5 

def _update_linked_virtual_trade(session: Session, signal_id: int, close_price: float, exit_reason: str):
    """
    Pomocnicza funkcja do natychmiastowej synchronizacji Wirtualnego Portfela.
    Gdy Strażnik zamyka sygnał, zamykamy też powiązaną transakcję wirtualną.
    """
    try:
        virtual_trade = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.signal_id == signal_id,
            models.VirtualTrade.status == 'OPEN'
        ).first()

        if virtual_trade:
            # Mapowanie statusu sygnału na status transakcji
            vt_status = 'CLOSED_TP' if exit_reason == 'COMPLETED' else 'CLOSED_SL'
            if "TRAILING" in exit_reason: # Jeśli to był trailing stop, oznaczamy jako TP (zysk) lub SL (ochrona)
                 # Zazwyczaj Trailing to forma TP (ochrona zysku)
                 vt_status = 'CLOSED_TP' 

            virtual_trade.status = vt_status
            virtual_trade.close_price = close_price
            virtual_trade.close_date = datetime.now(timezone.utc)
            
            # Oblicz P/L %
            if virtual_trade.entry_price:
                p_l = ((close_price - float(virtual_trade.entry_price)) / float(virtual_trade.entry_price)) * 100
                virtual_trade.final_profit_loss_percent = p_l
            
            logger.info(f"Strażnik: Zsynchronizowano Wirtualną Transakcję ID {virtual_trade.id}. P/L: {virtual_trade.final_profit_loss_percent:.2f}%")

    except Exception as e:
        logger.error(f"Strażnik: Błąd synchronizacji wirtualnego portfela: {e}")

def run_signal_monitor_cycle(session: Session, api_client: AlphaVantageClient):
    """
    Cykl Strażnika Sygnałów (Signal Monitor) - V5.1 FIXED.
    
    Funkcje:
    - Trailing Stop (Chandelier Exit).
    - Hard TP/SL.
    - NOWOŚĆ: Synchronizacja czasu rzeczywistego z Wirtualnym Agentem (eliminacja rozbieżności).
    """
    logger.info("Uruchamianie cyklu Strażnika Sygnałów (V5.1 Sync)...")

    # 1. Pobierz aktywne i oczekujące sygnały
    signals = session.query(models.TradingSignal).filter(
        models.TradingSignal.status.in_(['PENDING', 'ACTIVE'])
    ).all()

    if not signals:
        logger.info("Strażnik: Brak aktywnych sygnałów do monitorowania.")
        return

    # 2. Pobierz listę tickerów
    tickers = [s.ticker for s in signals]
    
    # 3. Pobierz ceny LIVE (Bulk Request)
    bulk_csv = api_client.get_bulk_quotes(tickers)
    
    if not bulk_csv:
        logger.error("Strażnik: Nie udało się pobrać cen live (Bulk Quotes).")
        return

    live_prices = {}
    try:
        import csv
        from io import StringIO
        reader = csv.DictReader(StringIO(bulk_csv))
        for row in reader:
            symbol = row.get('symbol')
            price = safe_float(row.get('close'))
            if symbol and price:
                live_prices[symbol] = price
    except Exception as e:
        logger.error(f"Strażnik: Błąd parsowania CSV: {e}")
        return

    updates_count = 0
    
    for signal in signals:
        current_price = live_prices.get(signal.ticker)
        if not current_price: continue

        sl = float(signal.stop_loss) if signal.stop_loss else 0
        tp = float(signal.take_profit) if signal.take_profit else 0
        entry = float(signal.entry_price) if signal.entry_price else 0
        
        # Obsługa Trailing Stop
        highest_price = float(signal.highest_price_since_entry) if signal.highest_price_since_entry else 0.0
        if highest_price == 0 and entry > 0:
            highest_price = entry

        status_changed = False
        new_status = signal.status
        note_update = ""
        alert_msg = ""
        
        # Flaga do synchronizacji
        sync_virtual_portfolio = False

        # --- LOGIKA V5 (Trailing Stop) ---
        if signal.status == 'ACTIVE':
            if current_price > highest_price:
                highest_price = current_price
                signal.highest_price_since_entry = highest_price 
            
            if signal.is_trailing_active:
                initial_risk = entry - sl
                estimated_atr = initial_risk / 2.0 if initial_risk > 0 else (current_price * 0.02)
                dynamic_sl = highest_price - (estimated_atr * TRAILING_ATR_MULTIPLIER)
                
                if current_price <= dynamic_sl and current_price > sl:
                    new_status = 'COMPLETED' 
                    note_update = f"[TRAILING STOP] Cena ({current_price}) spadła poniżej dynamicznego SL ({dynamic_sl:.2f})."
                    alert_msg = f"🛡️ TRAILING STOP HIT: {signal.ticker}\nWyjście: {current_price}."
                    status_changed = True
                    sync_virtual_portfolio = True

        # --- LOGIKA STANDARDOWA ---
        if not status_changed:
            if current_price <= sl:
                new_status = 'INVALIDATED'
                note_update = f"[HARD SL] Cena ({current_price}) przebiła SL ({sl})."
                alert_msg = f"🛑 STOP LOSS: {signal.ticker}\nWyjście: {current_price}."
                status_changed = True
                sync_virtual_portfolio = True

            elif current_price >= tp:
                new_status = 'COMPLETED'
                note_update = f"[TP HIT] Cena ({current_price}) osiągnęła cel ({tp})."
                alert_msg = f"💰 TAKE PROFIT: {signal.ticker}\nCel: {current_price}."
                status_changed = True
                sync_virtual_portfolio = True

            elif signal.status == 'PENDING':
                if current_price >= entry:
                    new_status = 'ACTIVE'
                    note_update = f"[ENTRY] Cena ({current_price}) przebiła Entry ({entry})."
                    alert_msg = f"🚀 ENTRY: {signal.ticker}\nCena: {current_price}."
                    status_changed = True
                    signal.highest_price_since_entry = current_price
                    
                    # Tu opcjonalnie można by otwierać Virtual Trade automatycznie,
                    # ale zostawmy to VirtualAgentowi (lub dodajmy tu w przyszłości).

        # --- APLIKOWANIE ZMIAN ---
        if status_changed:
            logger.info(f"Strażnik: Aktualizacja {signal.ticker} -> {new_status}")
            
            signal.status = new_status
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
            signal.notes = f"{timestamp}: {note_update}\n{signal.notes or ''}"
            signal.updated_at = datetime.now(timezone.utc)
            
            updates_count += 1
            send_telegram_alert(alert_msg)
            append_scan_log(session, f"STRAŻNIK: {signal.ticker} -> {new_status}. Cena: {current_price}")
            
            # === FIX ROZBIEŻNOŚCI ===
            if sync_virtual_portfolio:
                _update_linked_virtual_trade(session, signal.id, current_price, new_status)
        
        elif signal.status == 'ACTIVE' and current_price > (float(signal.highest_price_since_entry or 0)):
             signal.highest_price_since_entry = current_price
             updates_count += 1

    if updates_count > 0:
        try:
            session.commit()
            logger.info(f"Strażnik: Zaktualizowano {updates_count} sygnałów (i zsynchronizowano portfel).")
        except Exception as e:
            logger.error(f"Strażnik: Błąd zapisu do bazy: {e}")
            session.rollback()
