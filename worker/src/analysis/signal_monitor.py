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
# Nowa stała: Bufor bezpieczeństwa przy starcie (0.2%)
STARTUP_GRACE_BUFFER = 0.002 

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
            if "TRAILING" in exit_reason: 
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
    Cykl Strażnika Sygnałów (Signal Monitor) - V6: TTL + RR GUARD + BURNOUT + INSTANT KILL FIX.
    """
    logger.info("Uruchamianie cyklu Strażnika Sygnałów (V6 + Cleaner)...")

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
    
    live_prices = {}
    if bulk_csv:
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
            return # Jeśli nie mamy cen, nie robimy nic (bezpieczeństwo)

    updates_count = 0
    now_utc = datetime.now(timezone.utc)
    
    for signal in signals:
        # === 1. CLEANER: Time-To-Live (Wygaśnięcie Czasowe) ===
        if signal.expiration_date and now_utc > signal.expiration_date.replace(tzinfo=timezone.utc):
            signal.status = 'EXPIRED'
            signal.notes = (signal.notes or "") + f" [EXPIRED: Czas minął]"
            signal.updated_at = now_utc
            append_scan_log(session, f"🗑️ STRAŻNIK: {signal.ticker} WYGASŁ (TTL). Usunięto z listy.")
            updates_count += 1
            continue 

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
        sync_virtual_portfolio = False

        # === 2. CLEANER: PENDING SIGNALS LOGIC (Burnout & RR Guard) ===
        if signal.status == 'PENDING':
            # A. BURNOUT: Czy cena spadła poniżej SL przed wejściem?
            if current_price <= sl:
                # --- FIX: OCHRONA PRZED INSTANT KILL ---
                is_buy_stop = "BUY_STOP" in (signal.notes or "")
                should_kill = True
                
                if is_buy_stop:
                    # Dla BUY_STOP: Cena poniżej SL przed wejściem NIE unieważnia setupu (czekamy na wybicie).
                    should_kill = False
                else:
                    # Dla innych: Dajemy mały bufor (Grace Period) na spread/szum
                    sl_with_grace = sl * (1.0 - STARTUP_GRACE_BUFFER)
                    if current_price > sl_with_grace:
                        should_kill = False # Uratowany przez bufor

                if should_kill:
                    new_status = 'INVALIDATED'
                    note_update = f"[BURNT] Cena {current_price:.2f} przebiła SL {sl:.2f} przed aktywacją."
                    append_scan_log(session, f"🔥 STRAŻNIK: {signal.ticker} SPALONY (Cena < SL przed wejściem).")
                    status_changed = True
            
            # B. ACTIVATION & RR GUARD: Czy cena przebiła Entry?
            elif current_price >= entry:
                # Sprawdź R:R przy obecnej cenie (ochrona przed Gap Up)
                potential_profit = tp - current_price
                potential_risk = current_price - sl
                
                current_rr = 0
                if potential_risk > 0:
                    current_rr = potential_profit / potential_risk
                
                # Jeśli R:R jest fatalny (np. < 1.0), nie wchodzimy
                if current_rr < 1.0:
                    new_status = 'INVALIDATED'
                    note_update = f"[RR REJECT] Cena otwarcia {current_price:.2f} zbyt wysoka. RR spadł do {current_rr:.2f}."
                    append_scan_log(session, f"⛔ STRAŻNIK: {signal.ticker} ODRZUCONY PRZY WEJŚCIU (Słaby R:R: {current_rr:.2f}).")
                    status_changed = True
                else:
                    # Normalna aktywacja
                    new_status = 'ACTIVE'
                    note_update = f"[ENTRY] Cena ({current_price:.2f}) przebiła Entry ({entry:.2f})."
                    alert_msg = f"🚀 ENTRY: {signal.ticker}\nCena: {current_price:.2f}."
                    status_changed = True
                    signal.highest_price_since_entry = current_price
                    # Tu opcjonalnie: auto-otwarcie Virtual Trade

        # === 3. ACTIVE SIGNALS LOGIC (Standard Monitoring) ===
        elif signal.status == 'ACTIVE':
            if current_price > highest_price:
                highest_price = current_price
                signal.highest_price_since_entry = highest_price 
            
            # A. Trailing Stop
            if signal.is_trailing_active:
                initial_risk = entry - sl
                estimated_atr = initial_risk / 2.0 if initial_risk > 0 else (current_price * 0.02)
                dynamic_sl = highest_price - (estimated_atr * TRAILING_ATR_MULTIPLIER)
                
                if current_price <= dynamic_sl and current_price > sl:
                    new_status = 'COMPLETED' 
                    note_update = f"[TRAILING STOP] Cena ({current_price:.2f}) spadła poniżej dynamicznego SL ({dynamic_sl:.2f})."
                    alert_msg = f"🛡️ TRAILING STOP HIT: {signal.ticker}\nWyjście: {current_price:.2f}."
                    status_changed = True
                    sync_virtual_portfolio = True

            # B. Hard SL / TP
            if not status_changed:
                if current_price <= sl:
                    new_status = 'INVALIDATED'
                    note_update = f"[HARD SL] Cena ({current_price:.2f}) przebiła SL ({sl:.2f})."
                    alert_msg = f"🛑 STOP LOSS: {signal.ticker}\nWyjście: {current_price:.2f}."
                    status_changed = True
                    sync_virtual_portfolio = True

                elif current_price >= tp:
                    new_status = 'COMPLETED'
                    note_update = f"[TP HIT] Cena ({current_price:.2f}) osiągnęła cel ({tp:.2f})."
                    alert_msg = f"💰 TAKE PROFIT: {signal.ticker}\nCel: {current_price:.2f}."
                    status_changed = True
                    sync_virtual_portfolio = True

        # === APLIKOWANIE ZMIAN ===
        if status_changed:
            logger.info(f"Strażnik: Aktualizacja {signal.ticker} -> {new_status}")
            
            signal.status = new_status
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
            signal.notes = f"{timestamp}: {note_update}\n{signal.notes or ''}"
            signal.updated_at = datetime.now(timezone.utc)
            
            updates_count += 1
            if alert_msg: send_telegram_alert(alert_msg)
            # Logowanie tylko istotnych zmian statusu do UI
            if new_status in ['ACTIVE', 'COMPLETED', 'INVALIDATED']:
                 append_scan_log(session, f"STRAŻNIK: {signal.ticker} -> {new_status}. Cena: {current_price:.2f}")
            
            if sync_virtual_portfolio:
                _update_linked_virtual_trade(session, signal.id, current_price, new_status)
        
        elif signal.status == 'ACTIVE' and current_price > (float(signal.highest_price_since_entry or 0)):
             signal.highest_price_since_entry = current_price
             updates_count += 1

    if updates_count > 0:
        try:
            session.commit()
            logger.info(f"Strażnik: Zaktualizowano {updates_count} sygnałów.")
        except Exception as e:
            logger.error(f"Strażnik: Błąd zapisu do bazy: {e}")
            session.rollback()
