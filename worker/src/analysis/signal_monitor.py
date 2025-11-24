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
# 2.5 - 3.0 to standard dla Swing Tradingu. Daje oddech, ale chroni zysk.
TRAILING_ATR_MULTIPLIER = 2.5 

def run_signal_monitor_cycle(session: Session, api_client: AlphaVantageClient):
    """
    Cykl Strażnika Sygnałów (Signal Monitor) - V5 UPGRADE.
    
    Funkcje V5:
    - Obsługa Trailing Stop (Chandelier Exit).
    - Śledzenie 'highest_price_since_entry'.
    - Dynamiczne zamykanie pozycji.
    """
    logger.info("Uruchamianie cyklu Strażnika Sygnałów (V5 Trailing Stop)...")

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
        
        # V5: Obsługa Trailing Stop
        highest_price = float(signal.highest_price_since_entry) if signal.highest_price_since_entry else 0.0
        
        # Jeśli najwyższa cena nie jest ustawiona, a mamy cenę wejścia, zacznij od wejścia
        if highest_price == 0 and entry > 0:
            highest_price = entry

        status_changed = False
        new_status = signal.status
        note_update = ""
        alert_msg = ""

        # --- LOGIKA V5 (Trailing Stop) ---
        
        if signal.status == 'ACTIVE':
            # 1. Aktualizacja szczytu (High Watermark)
            if current_price > highest_price:
                highest_price = current_price
                signal.highest_price_since_entry = highest_price # Zapisz nowy szczyt
                # (Nie commitujemy jeszcze, zrobimy to zbiorczo na końcu)
            
            # 2. Sprawdzenie warunku Trailing Stop
            if signal.is_trailing_active:
                # Obliczamy przybliżony ATR z różnicy Entry-SL (zakładamy, że SL był ustawiony np. na 2 ATR)
                # To heurystyka, bo nie mamy pełnego ATR w bazie sygnałów, ale działa.
                # SL_distance = Entry - SL. Jeśli to było 2 ATR, to 1 ATR = SL_distance / 2.
                
                initial_risk = entry - sl
                estimated_atr = initial_risk / 2.0 if initial_risk > 0 else (current_price * 0.02) # Fallback 2%
                
                # Dynamiczny Stop Loss (Chandelier Exit)
                dynamic_sl = highest_price - (estimated_atr * TRAILING_ATR_MULTIPLIER)
                
                # Jeśli cena spadła poniżej dynamicznego SL (ale jest powyżej sztywnego SL)
                if current_price <= dynamic_sl and current_price > sl:
                    new_status = 'COMPLETED' # Traktujemy to jako realizację zysku (lub ochronę kapitału)
                    note_update = f"[TRAILING STOP] Cena ({current_price}) spadła poniżej dynamicznego SL ({dynamic_sl:.2f}). Szczyt był: {highest_price}."
                    alert_msg = f"🛡️ TRAILING STOP HIT: {signal.ticker}\nWyjście ochronne: {current_price}.\nObroniono zysk z poziomu {highest_price}."
                    status_changed = True

        # --- LOGIKA STANDARDOWA (Hard TP/SL) ---

        if not status_changed:
            if current_price <= sl:
                new_status = 'INVALIDATED'
                note_update = f"[HARD SL] Cena ({current_price}) przebiła SL ({sl})."
                alert_msg = f"🛑 STOP LOSS ALERT: {signal.ticker}\nCena spadła do {current_price} (SL: {sl})."
                status_changed = True

            elif current_price >= tp:
                new_status = 'COMPLETED'
                note_update = f"[TP HIT] Cena ({current_price}) osiągnęła cel ({tp})."
                alert_msg = f"💰 TAKE PROFIT ALERT: {signal.ticker}\nCel osiągnięty! Cena: {current_price}."
                status_changed = True

            elif signal.status == 'PENDING':
                if current_price >= entry:
                    new_status = 'ACTIVE'
                    note_update = f"[ENTRY] Cena ({current_price}) przebiła Entry ({entry}). AKTYWACJA."
                    alert_msg = f"🚀 ENTRY ALERT: {signal.ticker}\nSetup AKTYWNY (Cena: {current_price}).\nTrailing Stop włączony."
                    status_changed = True
                    
                    # Przy aktywacji inicjujemy 'highest_price'
                    signal.highest_price_since_entry = current_price

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
        
        # Nawet jeśli status się nie zmienił, zapisz nowy 'highest_price' jeśli wzrósł
        elif signal.status == 'ACTIVE' and current_price > (float(signal.highest_price_since_entry or 0)):
             signal.highest_price_since_entry = current_price
             updates_count += 1 # Wymuś commit, żeby zapisać nowy szczyt

    if updates_count > 0:
        try:
            session.commit()
            logger.info(f"Strażnik: Zaktualizowano {updates_count} sygnałów.")
        except Exception as e:
            logger.error(f"Strażnik: Błąd zapisu do bazy: {e}")
            session.rollback()
