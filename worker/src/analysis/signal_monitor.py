import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from .. import models
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import send_telegram_alert, append_scan_log, safe_float

logger = logging.getLogger(__name__)

def run_signal_monitor_cycle(session: Session, api_client: AlphaVantageClient):
    """
    Cykl Strażnika Sygnałów (Signal Monitor).
    Sprawdza WSZYSTKIE aktywne i oczekujące sygnały w bazie danych pod kątem
    realizacji TP, SL lub wejścia (Entry). Działa w tle.
    """
    logger.info("Uruchamianie cyklu Strażnika Sygnałów (Signal Monitor)...")

    # 1. Pobierz aktywne i oczekujące sygnały
    signals = session.query(models.TradingSignal).filter(
        models.TradingSignal.status.in_(['PENDING', 'ACTIVE'])
    ).all()

    if not signals:
        logger.info("Strażnik: Brak aktywnych sygnałów do monitorowania.")
        return

    # 2. Pobierz listę tickerów do sprawdzenia
    tickers = [s.ticker for s in signals]
    
    # 3. Pobierz ceny LIVE (Bulk Request dla oszczędności API)
    # Worker ma limit 145 req/min, więc bulk jest tu idealny.
    logger.info(f"Strażnik: Pobieranie cen live dla {len(tickers)} tickerów...")
    
    bulk_csv = api_client.get_bulk_quotes(tickers)
    
    if not bulk_csv:
        logger.error("Strażnik: Nie udało się pobrać cen live (Bulk Quotes). Pomijanie cyklu.")
        return

    # Parsowanie CSV do słownika {ticker: current_price}
    live_prices = {}
    try:
        import csv
        from io import StringIO
        reader = csv.DictReader(StringIO(bulk_csv))
        for row in reader:
            symbol = row.get('symbol')
            price = safe_float(row.get('close')) # W Bulk Quotes 'close' to current price
            if symbol and price:
                live_prices[symbol] = price
    except Exception as e:
        logger.error(f"Strażnik: Błąd parsowania CSV: {e}")
        return

    # 4. Analiza każdego sygnału
    updates_count = 0
    
    for signal in signals:
        current_price = live_prices.get(signal.ticker)
        if not current_price:
            continue

        # Konwersja na float dla obliczeń
        sl = float(signal.stop_loss) if signal.stop_loss else 0
        tp = float(signal.take_profit) if signal.take_profit else 0
        entry = float(signal.entry_price) if signal.entry_price else 0
        
        status_changed = False
        new_status = signal.status
        note_update = ""
        alert_msg = ""

        # --- LOGIKA STRAŻNIKA ---

        # A. Sprawdzenie STOP LOSS (Ochrona Kapitału)
        # Jeśli cena spadła poniżej SL -> Setup spalony
        if current_price <= sl:
            new_status = 'INVALIDATED'
            note_update = f"[AUTO-WATCHDOG] Cena ({current_price}) przebiła SL ({sl})."
            alert_msg = f"🛑 STOP LOSS ALERT: {signal.ticker}\nCena spadła do {current_price} (SL: {sl}).\nSygnał unieważniony."
            status_changed = True

        # B. Sprawdzenie TAKE PROFIT (Realizacja Zysku)
        elif current_price >= tp:
            new_status = 'COMPLETED'
            note_update = f"[AUTO-WATCHDOG] Cena ({current_price}) osiągnęła TP ({tp})."
            alert_msg = f"💰 TAKE PROFIT ALERT: {signal.ticker}\nCel osiągnięty! Cena: {current_price}.\nZaksięguj zysk."
            status_changed = True

        # C. Sprawdzenie AKTYWACJI (Pending -> Active)
        # Jeśli sygnał był PENDING, a cena weszła w strefę wejścia lub przebiła entry
        elif signal.status == 'PENDING':
            # Zakładamy wejście na wybicie (Breakout) lub w strefie
            # Uproszczenie: Jeśli cena jest powyżej Entry (dla Longa), to weszliśmy
            if current_price >= entry:
                new_status = 'ACTIVE'
                note_update = f"[AUTO-WATCHDOG] Cena ({current_price}) przebiła Entry ({entry}). Sygnał AKTYWNY."
                alert_msg = f"🚀 ENTRY ALERT: {signal.ticker}\nCena aktywacji osiągnięta: {current_price}.\nSetup AKTYWNY."
                status_changed = True

        # D. Ostrzeżenie o bliskości SL (Danger Zone) - opcjonalne, bez zmiany statusu
        # Jeśli cena jest < 1% od SL i sygnał jest aktywny
        elif signal.status == 'ACTIVE' and (current_price - sl) / sl < 0.01:
            # To nie zmienia statusu w bazie, ale wysyła powiadomienie (można dodać logikę, by nie spamować)
            pass 

        # --- APLIKOWANIE ZMIAN ---
        if status_changed:
            logger.info(f"Strażnik: Aktualizacja {signal.ticker} -> {new_status}")
            
            signal.status = new_status
            current_notes = signal.notes or ""
            # Dodaj notatkę na początku, z datą
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')
            signal.notes = f"{timestamp}: {note_update}\n{current_notes}"
            signal.updated_at = datetime.now(timezone.utc)
            
            updates_count += 1
            
            # Wyślij powiadomienie na Telegram
            send_telegram_alert(alert_msg)
            
            # Log systemowy
            append_scan_log(session, f"STRAŻNIK: {signal.ticker} -> {new_status}. Cena: {current_price}")

    if updates_count > 0:
        try:
            session.commit()
            logger.info(f"Strażnik: Zaktualizowano {updates_count} sygnałów.")
        except Exception as e:
            logger.error(f"Strażnik: Błąd zapisu do bazy: {e}")
            session.rollback()
