import logging
import re
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
    if "biox" in notes_lower:
        return "BIOX_PUMP"
    if "aqm" in notes_lower:
        return "AQM_V4"
    if "h3" in notes_lower:
        return "H3_SNIPER"
    if "fib" in notes_lower:
        return "FIB_H1"
    if "ema" in notes_lower:
        return "EMA_BOUNCE"
    return "OTHER"

def _parse_metrics_from_notes(notes: str) -> dict:
    """
    Próbuje odzyskać kluczowe metryki (np. AQM Score) z tekstu notatki,
    ponieważ tabela TradingSignal nie przechowuje ich w osobnych kolumnach.
    """
    metrics = {}
    if not notes: return metrics
    
    # Szukamy wzorców typu "AQM: 0.85" lub "AQM H3:0.85"
    aqm_match = re.search(r'AQM(?: H3)?:?\s*([0-9\.]+)', notes)
    if aqm_match:
        try:
            metrics['metric_aqm_score_h3'] = float(aqm_match.group(1))
        except: pass
        
    return metrics

def open_virtual_trade(session: Session, signal: models.TradingSignal):
    """
    Wywoływane, gdy sygnał PENDING przechodzi na ACTIVE.
    Tworzy nowy wpis w tabeli 'virtual_trades' do śledzenia.
    """
    signal_id = signal.id
    # logger.info(f"[Virtual Agent] Próba otwarcia transakcji dla signal_id: {signal_id} ({signal.ticker})")

    try:
        # Krok 1: Sprawdź, czy już nie otworzyliśmy tej transakcji (zabezpieczenie)
        existing_trade = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.signal_id == signal_id
        ).first()
        
        if existing_trade:
            return

        # Krok 2: Wyciągnij dane z sygnału
        setup_type = _parse_setup_type_from_notes(signal.notes)
        parsed_metrics = _parse_metrics_from_notes(signal.notes)
        
        # Ustal cenę wejścia:
        # Jeśli sygnał jest ACTIVE i ma highest_price_since_entry, to znaczy że wejście nastąpiło.
        # Dla uproszczenia wirtualnego, używamy zdefiniowanej entry_price (idealne wykonanie)
        # lub entry_zone_top w ostateczności.
        entry_price_for_trade = signal.entry_price if signal.entry_price is not None else signal.entry_zone_top

        # Walidacja: Nie możemy otworzyć transakcji bez ceny wejścia lub stop-lossa
        if entry_price_for_trade is None or signal.stop_loss is None:
            logger.error(f"[Virtual Agent] Nie można otworzyć wirtualnej transakcji dla {signal.ticker} (signal_id: {signal_id}). Brak ceny wejścia lub stop lossa.")
            return
            
        # Krok 3: Stwórz nowy obiekt VirtualTrade
        # === INTEGRACJA RE-CHECK: PRZEKAZUJEMY OCZEKIWANIA ===
        new_trade = models.VirtualTrade(
            signal_id=signal_id,
            ticker=signal.ticker,
            status='OPEN', 
            setup_type=setup_type,
            entry_price=entry_price_for_trade,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            open_date=datetime.now(timezone.utc),
            
            # Przekazujemy "Cyrograf" dla Agenta Re-check
            expected_profit_factor=signal.expected_profit_factor,
            expected_win_rate=signal.expected_win_rate,
            
            # Przekazujemy odzyskane metryki
            metric_aqm_score_h3=parsed_metrics.get('metric_aqm_score_h3')
        )
        
        # Krok 4: Zapisz w bazie
        session.add(new_trade)
        session.commit()
        
        logger.info(f"✅ [Virtual Agent] Wirtualna transakcja OTWARTA dla {signal.ticker} @ {entry_price_for_trade:.2f}. Exp.PF: {signal.expected_profit_factor}")

    except Exception as e:
        logger.error(f"[Virtual Agent] Błąd krytyczny podczas otwierania wirtualnej transakcji dla {signal.ticker}: {e}", exc_info=True)
        session.rollback()

# ==================================================================
# === Mózg Wirtualnego Agenta (Monitor) ===
# ==================================================================

def _parse_bulk_quotes_for_virtual_agent(csv_text: str) -> dict:
    """
    Parsuje odpowiedź CSV z REALTIME_BULK_QUOTES i zwraca słownik
    mapujący ticker na cenę (price).
    """
    if not csv_text or "symbol" not in csv_text:
        return {}
    
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    
    price_dict = {}
    for row in reader:
        ticker = row.get('symbol')
        price = safe_float(row.get('close'))
        if ticker and price is not None:
            price_dict[ticker] = price
    return price_dict

def run_virtual_trade_monitor(session: Session, api_client: AlphaVantageClient):
    """
    Główna funkcja monitorująca Wirtualnego Agenta (uruchamiana np. raz na dobę lub częściej).
    1. Skanuje 'ACTIVE' signals i otwiera dla nich VirtualTrade (jeśli brak).
    2. Zamyka pozycje, które osiągnęły horyzont czasowy lub zostały zamknięte przez Strażnika.
    """
    logger.info("🤖 [Virtual Agent] Uruchamianie monitora (Auto-Entry + Exit)...")
    
    try:
        # === ETAP 0: AUTO-ENTRY (Otwieranie pozycji dla aktywnych sygnałów) ===
        # Znajdź sygnały ACTIVE, które nie mają jeszcze wpisu w virtual_trades
        # Robimy to w Pythonie dla bezpieczeństwa logiki
        active_signals = session.query(models.TradingSignal).filter(
            models.TradingSignal.status == 'ACTIVE'
        ).all()
        
        for sig in active_signals:
            open_virtual_trade(session, sig)

        # === ETAP 1: MONITOROWANIE OTWARTYCH POZYCJI ===
        open_trades = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.status == 'OPEN'
        ).all()

        if not open_trades:
            # logger.info("🤖 [Virtual Agent] Brak otwartych wirtualnych transakcji.")
            return

        # logger.info(f"🤖 [Virtual Agent] Monitorowanie {len(open_trades)} otwartych transakcji.")

        # Krok 2: Sprawdź statusy powiązanych sygnałów (TP/SL)
        tickers_to_check_expiry = []
        now = datetime.now(timezone.utc)

        for trade in open_trades:
            signal = session.query(models.TradingSignal).filter(
                models.TradingSignal.id == trade.signal_id
            ).first()

            # --- Scenariusz A: Sygnał został zamknięty przez Strażnika (TP/SL) ---
            if signal and signal.status == 'COMPLETED':
                logger.info(f"🤖 [Virtual Agent] Transakcja {trade.ticker} zamknięta przez Strażnika (TP).")
                trade.status = 'CLOSED_TP'
                trade.close_date = signal.updated_at
                trade.close_price = signal.take_profit # Zakładamy realizację po cenie TP
            
            elif signal and signal.status == 'INVALIDATED':
                logger.info(f"🤖 [Virtual Agent] Transakcja {trade.ticker} zamknięta przez Strażnika (SL).")
                trade.status = 'CLOSED_SL'
                trade.close_date = signal.updated_at
                trade.close_price = signal.stop_loss # Zakładamy realizację po cenie SL

            elif signal and signal.status == 'EXPIRED':
                 logger.info(f"🤖 [Virtual Agent] Transakcja {trade.ticker} wygasła (TTL). Zamykanie rynkowe.")
                 tickers_to_check_expiry.append(trade.ticker)

            # --- Scenariusz B: Sygnał wciąż aktywny, ale wygasa (Fallback 7 dni jeśli brak expiration_date) ---
            elif (now - trade.open_date) > timedelta(days=14): # Bezpiecznik 14 dni
                logger.info(f"🤖 [Virtual Agent] Transakcja {trade.ticker} wygasła (Hard Limit).")
                tickers_to_check_expiry.append(trade.ticker)
            
            # --- Scenariusz C: Sygnał osierocony ---
            elif not signal:
                 logger.warning(f"🤖 [Virtual Agent] Transakcja {trade.ticker} osierocona.")
                 tickers_to_check_expiry.append(trade.ticker)
            
            # --- Obliczanie P/L dla zamkniętych przez Signal Monitor ---
            if trade.status != 'OPEN' and trade.close_price is not None:
                try:
                    p_l_percent = ((trade.close_price - trade.entry_price) / trade.entry_price) * 100
                    trade.final_profit_loss_percent = p_l_percent
                except: trade.final_profit_loss_percent = 0

        session.commit()

        # Krok 3: Obsługa transakcji, które wygasły (Wymaga zapytania API o aktualną cenę)
        if tickers_to_check_expiry:
            logger.info(f"🤖 [Virtual Agent] Pobieranie cen dla {len(tickers_to_check_expiry)} wygasających transakcji...")
            unique_tickers = list(set(tickers_to_check_expiry))
            bulk_csv = api_client.get_bulk_quotes(unique_tickers)
            
            if bulk_csv:
                parsed_prices = _parse_bulk_quotes_for_virtual_agent(bulk_csv)
                
                # Ponowne odpytanie bazy, aby mieć świeże obiekty po commit
                expired_trades = session.query(models.VirtualTrade).filter(
                    models.VirtualTrade.status == 'OPEN',
                    models.VirtualTrade.ticker.in_(unique_tickers)
                ).all()

                for trade in expired_trades:
                    current_price = parsed_prices.get(trade.ticker)
                    if current_price:
                        logger.info(f"🤖 [Virtual Agent] Zamykanie wygasłej {trade.ticker} po {current_price:.2f}.")
                        trade.status = 'CLOSED_EXPIRED'
                        trade.close_date = now
                        trade.close_price = current_price
                        try:
                            p_l_percent = ((trade.close_price - trade.entry_price) / trade.entry_price) * 100
                            trade.final_profit_loss_percent = p_l_percent
                        except: trade.final_profit_loss_percent = 0
            
            session.commit()

        # logger.info("🤖 [Virtual Agent] Monitor zakończył pracę.")
        
    except Exception as e:
        logger.error(f"🤖 [Virtual Agent] Błąd krytyczny: {e}", exc_info=True)
        session.rollback()
