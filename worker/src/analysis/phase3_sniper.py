import logging
import time
import pandas as pd
# KROK 7 ZMIANA: Dodajemy importy do parsowania CSV
import csv
from io import StringIO
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from pandas import Series as pd_Series
from typing import List, Tuple

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    update_scan_progress, append_scan_log, safe_float, 
    update_system_control, get_market_status_and_time,
    calculate_ema, standardize_df_columns,
    # ==================================================================
    # KROK 2 (KAT. 1): Import funkcji alertów Telegram
    # ==================================================================
    send_telegram_alert,
    # ==================================================================
    # === POPRAWKA (TimeoutError): Import funkcji do sprawdzania statusu ===
    # ==================================================================
    get_system_control_value
)
from ..config import Phase3Config
# ==================================================================
# KROK 2 (Wirtualny Agent): Import nowego modułu
# ==================================================================
from . import virtual_agent
# ==================================================================

logger = logging.getLogger(__name__)

# KROK 7 ZMIANA: Dodajemy parser CSV (skopiowany z phase1_scanner.py dla spójności)
def _parse_bulk_quotes_csv(csv_text: str) -> dict:
# ... (bez zmian) ...
    """Przetwarza odpowiedź CSV z REALTIME_BULK_QUOTES na słownik danych."""
    if not csv_text or "symbol" not in csv_text:
        logger.warning("[Monitor F3] Otrzymane dane CSV (Bulk Quotes) są puste lub nieprawidłowe.")
        return {}
    
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    
    data_dict = {}
    for row in reader:
        ticker = row.get('symbol')
        if not ticker:
            continue
        
        # Endpoint REALTIME_BULK_QUOTES używa 'close' jako aktualnej ceny
        data_dict[ticker] = {
            'price': safe_float(row.get('close')),
            'volume': safe_float(row.get('volume')),
        }
    return data_dict


# --- SEKCJA SKANERA NOCNEGO (EOD) ---

def _find_breakout_setup(daily_df: pd.DataFrame, min_consolidation_days=5, breakout_atr_multiplier=1.0) -> dict | None:
    try:
        if len(daily_df) < min_consolidation_days + 2: return None
        high_low = daily_df['high'] - daily_df['low']
        high_close = (daily_df['high'] - daily_df['close'].shift()).abs()
        low_close = (daily_df['low'] - daily_df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = calculate_ema(tr, 14) # Używamy funkcji z utils
        current_atr = atr.iloc[-1]
        if current_atr == 0: return None
        consolidation_df = daily_df.iloc[-(min_consolidation_days + 1):-1]
        consolidation_high = consolidation_df['high'].max()
        consolidation_low = consolidation_df['low'].min()
        consolidation_range = consolidation_high - consolidation_low
        is_consolidating = consolidation_range < (2 * atr.iloc[-2]) # Sprawdź, czy zakres jest mniejszy niż 2x ATR
        latest_candle = daily_df.iloc[-1]
        is_breakout = latest_candle['close'] > consolidation_high
        is_strong_breakout = latest_candle['close'] > (consolidation_high + breakout_atr_multiplier * current_atr)
        
        if is_consolidating and is_breakout and is_strong_breakout:
            logger.info(f"Breakout setup found for {daily_df.index[-1]}")
            
            # ==================================================================
            # ZMIANA (Sugestia AI): Używamy mnożnika z nowej konfiguracji
            # ==================================================================
            stop_loss = consolidation_high - (Phase3Config.Breakout.ATR_MULTIPLIER_FOR_SL * current_atr)
            
            return {
                "setup_type": "BREAKOUT",
                "entry_price": latest_candle['high'] + 0.01,
                "stop_loss": stop_loss,
                "consolidation_high": consolidation_high,
                "atr": current_atr
            }
        return None
    except Exception as e:
        logger.error(f"Error in _find_breakout_setup: {e}")
        return None

def _find_ema_bounce_setup(daily_df: pd.DataFrame) -> dict | None:
    try:
        # ==================================================================
        # ZMIANA (Sugestia AI): Używamy okresu EMA z nowej konfiguracji
        # ==================================================================
        ema_period = Phase3Config.EmaBounce.EMA_PERIOD
        # ==================================================================

        if len(daily_df) < ema_period + 3: return None
        daily_df['ema'] = calculate_ema(daily_df['close'], ema_period) # Używamy funkcji z utils
        is_ema_rising = daily_df['ema'].iloc[-1] > daily_df['ema'].iloc[-2] > daily_df['ema'].iloc[-3]
        latest_candle = daily_df.iloc[-1]
        prev_candle = daily_df.iloc[-2]
        latest_ema = daily_df['ema'].iloc[-1]
        touched_ema = (prev_candle['low'] <= daily_df['ema'].iloc[-2] * 1.01) or \
                      (latest_candle['open'] <= latest_ema * 1.01)
        closed_above_ema = latest_candle['close'] > latest_ema
        is_bullish_candle = latest_candle['close'] > latest_candle['open']
        
        if is_ema_rising and touched_ema and closed_above_ema and is_bullish_candle:
             
             # --- Obliczanie ATR ---
             high_low = daily_df['high'] - daily_df['low']
             high_close = (daily_df['high'] - daily_df['close'].shift()).abs()
             low_close = (daily_df['low'] - daily_df['close'].shift()).abs()
             tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
             atr = calculate_ema(tr, 14).iloc[-1]
             if atr == 0 or latest_candle['close'] == 0:
                 return None # Błąd danych
             
             # ==================================================================
             # ZMIANA (Sugestia AI 2): Filtr minimalnej zmienności ATR
             # ==================================================================
             atr_percent = atr / latest_candle['close']
             if atr_percent < Phase3Config.EmaBounce.MIN_ATR_PERCENT_FILTER:
                 logger.info(f"EMA Bounce dla {daily_df.index[-1]} pominięty. ATR% ({atr_percent:.2%}) jest poniżej progu {Phase3Config.EmaBounce.MIN_ATR_PERCENT_FILTER:.2%}")
                 return None
             # ==================================================================

             logger.info(f"EMA Bounce setup found for {daily_df.index[-1]}")
             
             # ==================================================================
             # ZMIANA (Sugestia AI): Używamy mnożnika z nowej konfiguracji
             # ==================================================================
             stop_loss = latest_candle['low'] - (Phase3Config.EmaBounce.ATR_MULTIPLIER_FOR_SL * atr)
             
             return {
                 "setup_type": "EMA_BOUNCE",
                 "entry_price": latest_candle['high'] + 0.01,
                 "stop_loss": stop_loss,
                 "ema_value": latest_ema,
                 "atr": atr
             }
        return None
    except Exception as e:
        logger.error(f"Error in _find_ema_bounce_setup: {e}")
        return None

def find_end_of_day_setup(ticker: str, daily_df: pd.DataFrame) -> dict:
    if daily_df.empty or len(daily_df) < 21:
         return {"signal": False, "reason": "Niewystarczająca historia danych dziennych (otrzymana z Fazy 2)."}
    current_price = daily_df['close'].iloc[-1]
    
    breakout_setup = _find_breakout_setup(daily_df)
    if breakout_setup:
        risk = breakout_setup['entry_price'] - breakout_setup['stop_loss']
        if risk <= 0: return {"signal": False, "reason": "Błąd kalkulacji ryzyka (Breakout)."}
        
        # ==================================================================
        # ZMIANA (Sugestia AI 1): Używamy R/R specyficznego dla Breakout
        # ==================================================================
        rr_ratio = Phase3Config.Breakout.TARGET_RR_RATIO
        take_profit = breakout_setup['entry_price'] + (rr_ratio * risk)
        # ==================================================================
        
        return {
            "signal": True, "status": "PENDING",
            "ticker": ticker,
            "entry_price": float(breakout_setup['entry_price']),
            "stop_loss": float(breakout_setup['stop_loss']),
            "take_profit": float(take_profit),
            "risk_reward_ratio": rr_ratio, # Zapisujemy poprawny R/R
            "notes": f"Setup EOD (OCZEKUJĄCY): Breakout z konsolidacji. Opór: {breakout_setup['consolidation_high']:.2f}."
        }
        
    ema_bounce_setup = _find_ema_bounce_setup(daily_df)
    if ema_bounce_setup:
        risk = ema_bounce_setup['entry_price'] - ema_bounce_setup['stop_loss']
        if risk <= 0: return {"signal": False, "reason": "Błąd kalkulacji ryzyka (EMA Bounce)."}
        
        # ==================================================================
        # ZMIANA (Sugestia AI 1): Używamy R/R specyficznego dla EMA Bounce
        # ==================================================================
        rr_ratio = Phase3Config.EmaBounce.TARGET_RR_RATIO
        take_profit = ema_bounce_setup['entry_price'] + (rr_ratio * risk)
        # ==================================================================
        
        return {
            "signal": True, "status": "PENDING",
            "ticker": ticker,
            "entry_price": float(ema_bounce_setup['entry_price']),
            "stop_loss": float(ema_bounce_setup['stop_loss']),
            "take_profit": float(take_profit),
            "risk_reward_ratio": rr_ratio,
            "notes": f"Setup EOD (OCZEKUJĄCY): Odbicie od rosnącej EMA{Phase3Config.EmaBounce.EMA_PERIOD}. EMA={ema_bounce_setup['ema_value']:.2f}."
        }
        
    impulse_result = _find_impulse_and_fib_zone(daily_df)
    if impulse_result:
        is_in_zone = impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"]
        if is_in_zone:
            take_profit = float(impulse_result['impulse_high'])
            stop_loss = float(impulse_result['impulse_low'])
            
            # Oblicz R/R dla Fiba (używając górnej granicy strefy jako wejścia)
            entry_activation_price = impulse_result['entry_zone_top']
            risk = entry_activation_price - stop_loss
            reward = take_profit - entry_activation_price
            
            # Unikaj dzielenia przez zero
            if risk <= 0:
                rr_ratio = 0.0
                logger.warning(f"Obliczanie R/R dla Fib {ticker} nie powiodło się (ryzyko <= 0).")
            else:
                rr_ratio = reward / risk

            return {
                "signal": True, "status": "PENDING",
                "ticker": ticker,
                "entry_zone_bottom": float(impulse_result['entry_zone_bottom']),
                "entry_zone_top": float(impulse_result['entry_zone_top']),
                "stop_loss": stop_loss, 
                "take_profit": take_profit,
                "risk_reward_ratio": rr_ratio, # Zapisujemy obliczony R/R
                "notes": f"Setup EOD (OCZEKUJĄCY): Cena ({current_price:.2f}) w strefie Fib. Oczekuje na sygnał intraday H1."
            }
        else:
             return {"signal": False, "reason": f"Fib: Cena ({current_price:.2f}) poza strefą."}
             
    return {"signal": False, "reason": "Brak setupu EOD (Fib/Breakout/EMA Bounce)."}

def run_tactical_planning(session: Session, qualified_data: List[Tuple[str, pd.DataFrame]], get_current_state, api_client: AlphaVantageClient):
# ... (bez zmian) ...
    logger.info("Running Phase 3: End-of-Day Tactical Planning...")
    append_scan_log(session, "Faza 3: Skanowanie EOD w poszukiwaniu setupów...")
    successful_setups = 0
    for ticker, daily_df in qualified_data:
        try:
            trade_setup = find_end_of_day_setup(ticker, daily_df)
            if trade_setup.get("signal"):
                successful_setups += 1
                stmt = text("""
                    INSERT INTO trading_signals (
                        ticker, generation_date, status, 
                        entry_price, stop_loss, take_profit, risk_reward_ratio, 
                        notes, entry_zone_bottom, entry_zone_top,
                        updated_at 
                    )
                    VALUES (
                        :ticker, NOW(), :status, 
                        :entry, :sl, :tp, :rr, 
                        :notes, :ezb, :ezt,
                        NOW()
                    )
                    ON CONFLICT (ticker) WHERE status IN ('ACTIVE', 'PENDING')
                    DO UPDATE SET 
                        status = EXCLUDED.status, 
                        generation_date = EXCLUDED.generation_date, 
                        entry_price = EXCLUDED.entry_price, 
                        stop_loss = EXCLUDED.stop_loss, 
                        take_profit = EXCLUDED.take_profit, 
                        risk_reward_ratio = EXCLUDED.risk_reward_ratio, 
                        notes = EXCLUDED.notes, 
                        entry_zone_bottom = EXCLUDED.entry_zone_bottom, 
                        entry_zone_top = EXCLUDED.entry_zone_top,
                        updated_at = EXCLUDED.updated_at;
                """)
                params = {
                    'ticker': ticker,
                    'status': trade_setup['status'],
                    'entry': trade_setup.get('entry_price'),
                    'sl': trade_setup.get('stop_loss'),
                    'tp': trade_setup.get('take_profit'),
                    'rr': trade_setup.get('risk_reward_ratio'),
                    'notes': trade_setup.get('notes'),
                    'ezb': trade_setup.get('entry_zone_bottom'),
                    'ezt': trade_setup.get('entry_zone_top')
                }
                session.execute(stmt, [params])
                session.commit()
                log_prefix = f"NOWY SYGNAŁ (F3): {ticker} [{trade_setup['status']}]"
                log_message = f"{log_prefix} | {trade_setup.get('notes', 'Brak notatek.')}"
                append_scan_log(session, log_message)
                
                # POPRAWKA (Problem 3): Alert EOD jest teraz mniej pilny, bo sygnał jest PENDING
                if trade_setup['status'] == 'PENDING':
                    # Nie generujemy już alertu 'system_alert', aby uniknąć fałszywych alarmów w nocy.
                    # Alert wygeneruje monitor czasu rzeczywistego, gdy cena faktycznie wejdzie w strefę.
                    logger.info(f"Sygnał {ticker} zapisany jako PENDING. Monitor RT przejmie obserwację.")
                elif trade_setup['status'] == 'ACTIVE': 
                    # Ta logika jest zachowana na wypadek, gdyby jakaś strategia *celowo* generowała ACTIVE
                    alert_msg = f"NOWY SYGNAł AKTYWNY (EOD): {ticker} gotowy do wejścia!"
                    update_system_control(session, 'system_alert', alert_msg)
            else:
                append_scan_log(session, f"INFO (F3): {ticker} - {trade_setup.get('reason')}")
        except Exception as e:
            logger.error(f"Error in Phase 3 EOD scan for {ticker}: {e}", exc_info=True)
            session.rollback()
    append_scan_log(session, f"Faza 3 (Skaner EOD) zakończony. Znaleziono {successful_setups} setupów.")


# --- SEKCJA MONITORA CZASU RZECZYWISTEGO ---

# ==================================================================
# NOWA FUNKCJA (KROK 1): Helper do sprawdzania potwierdzenia H1
# ==================================================================
def _check_h1_confirmation(ticker: str, api_client: AlphaVantageClient) -> bool:
# ... (bez zmian) ...
    """
    Pobiera dane H1 (60min) i sprawdza, czy ostatnia zamknięta świeca
    zamknęła się powyżej rosnącej 9-okresowej EMA H1.
    """
    try:
        # Pobieramy dane intraday (H1)
        h1_data_raw = api_client.get_intraday(ticker, interval='60min', outputsize='compact', extended_hours=False)
        if not h1_data_raw or 'Time Series (60min)' not in h1_data_raw:
            logger.warning(f"[Monitor Fib H1] Brak danych H1 dla {ticker}.")
            return False
        
        # Przetwarzamy dane
        df = pd.DataFrame.from_dict(h1_data_raw['Time Series (60min)'], orient='index')
        df = standardize_df_columns(df) # Sortuje rosnąco i konwertuje na liczby
        
        if len(df) < 11: # Potrzebujemy 9 dla EMA + 2 do sprawdzenia (ostatnia zamknięta i ta przed nią)
            logger.warning(f"[Monitor Fib H1] Za mało danych H1 dla {ticker} (tylko {len(df)} świec).")
            return False

        # Obliczamy EMA(9) na danych H1
        ema9 = calculate_ema(df['close'], 9)
        
        # Sprawdzamy OSTATNIĄ ZAMKNIĘTĄ ŚWIECĘ (indeks -2),
        # ponieważ świeca -1 może się jeszcze tworzyć.
        last_closed_candle = df.iloc[-2]
        last_closed_ema = ema9.iloc[-2]
        prev_ema = ema9.iloc[-3]
        
        # Definicja sygnału:
        # 1. EMA(9) na H1 musi rosnąć (ostatnia zamknięta > poprzednia)
        is_ema_rising = last_closed_ema > prev_ema
        # 2. Ostatnia zamknięta świeca H1 musi zamknąć się POWYŻEJ EMA(9)
        is_closed_above_ema = last_closed_candle['close'] > last_closed_ema
        
        if is_ema_rising and is_closed_above_ema:
            logger.info(f"[Monitor Fib H1] Potwierdzenie H1 ZNALEZIONE dla {ticker}.")
            return True
        else:
            logger.info(f"[Monitor Fib H1] Brak potwierdzenia H1 dla {ticker} (EMA rośnie: {is_ema_rising}, Zamknięcie > EMA: {is_closed_above_ema}).")
            return False
            
    except Exception as e:
        logger.error(f"[Monitor Fib H1] Błąd podczas sprawdzania potwierdzenia H1 dla {ticker}: {e}", exc_info=True)
        return False
# ==================================================================
# KONIEC NOWEJ FUNKCJI
# ==================================================================


# ==================================================================
# NOWA FUNKCJA (KROK 1): Monitor potwierdzeń Fib H1 (Wolny)
# ==================================================================
def monitor_fib_confirmations(session: Session, api_client: AlphaVantageClient):
# ... (bez zmian) ...
    """
    Wolniejszy monitor (uruchamiany np. co 15 minut), który sprawdza
    potwierdzenia H1 dla sygnałów Fib (PENDING), które są w strefie wejścia.
    """
    
    # ==================================================================
    # === POPRAWKA (TimeoutError): Dodanie blokady (lock) ===
    # ==================================================================
    try:
        worker_status = get_system_control_value(session, 'worker_status')
        if worker_status and worker_status.startswith('BUSY_'):
            logger.info(f"[Monitor Fib H1] Worker jest zajęty ({worker_status}). Pomijanie cyklu, aby zapobiec blokadzie DB.")
            return # Zakończ natychmiast, zwalniając połączenie
    except Exception as e:
        logger.error(f"[Monitor Fib H1] Krytyczny błąd podczas sprawdzania statusu workera: {e}", exc_info=True)
        return # Nie kontynuuj, jeśli nie możemy sprawdzić statusu
    # ==================================================================
    
    market_info = get_market_status_and_time(api_client)
    market_status = market_info.get("status")
    
    # Ten monitor może działać rzadziej, ale też tylko gdy rynek jest aktywny
    if market_status not in ["MARKET_OPEN", "PRE_MARKET", "AFTER_MARKET"]:
        logger.info(f"Market is {market_status}. Skipping H1 Fib Confirmation Monitor.")
        return
        
    logger.info("Running H1 Fib Confirmation Monitor (Slow Monitor)...")
    
    try:
        # Krok 1: Znajdź wszystkie sygnały PENDING, które są setupami Fib
        # (Rozpoznajemy je po tym, że entry_zone_top NIE JEST NULLEM, a entry_price JEST NULLEM)
        fib_signals_rows = session.execute(text("""
            SELECT * FROM trading_signals 
            WHERE status = 'PENDING' 
              AND entry_zone_top IS NOT NULL 
              AND entry_price IS NULL
        """)).fetchall() # <-- ZMIANA: Pobieramy * (wszystkie kolumny)

        if not fib_signals_rows:
            logger.info("[Monitor Fib H1] Brak sygnałów Fib (PENDING) do monitorowania.")
            return

        tickers_to_monitor = [row.ticker for row in fib_signals_rows]
        logger.info(f"[Monitor Fib H1] Monitorowanie {len(tickers_to_monitor)} sygnałów Fib: {', '.join(tickers_to_monitor)}")

        # Krok 2: Pobierz aktualne ceny dla tych tickerów (1 zapytanie API)
        bulk_data_csv = api_client.get_bulk_quotes(tickers_to_monitor)
        if not bulk_data_csv:
            logger.warning("[Monitor Fib H1] Nie można pobrać danych bulk quote dla monitorowania Fib.")
            return

        parsed_data = _parse_bulk_quotes_csv(bulk_data_csv)
        if not parsed_data:
            logger.warning("[Monitor Fib H1] Nie udało się sparsować danych bulk quote.")
            return

        # Krok 3: Iteruj i sprawdzaj
        for signal in fib_signals_rows:
            ticker = signal.ticker
            quote_data = parsed_data.get(ticker)
            
            if not quote_data or quote_data.get('price') is None:
                logger.warning(f"[Monitor Fib H1] Brak ceny dla {ticker} w odpowiedzi bulk.")
                continue

            current_price = float(quote_data['price'])
            zone_bottom = float(signal.entry_zone_bottom)
            zone_top = float(signal.entry_zone_top)

            # Krok 4: Sprawdź, czy cena jest w strefie Fib
            if zone_bottom <= current_price <= zone_top:
                logger.info(f"[Monitor Fib H1] {ticker} jest w strefie Fib ({current_price:.2f}). Sprawdzanie potwierdzenia H1...")
                
                # Krok 5: TYLKO jeśli jest w strefie, wykonaj drogie zapytanie H1 (N zapytań API)
                try:
                    has_confirmation = _check_h1_confirmation(ticker, api_client)
                    
                    if has_confirmation:
                        logger.warning(f"[Monitor Fib H1] POTWIERDZENIE H1 ZNALEZIONE dla {ticker}! Aktywowanie sygnału.")
                        
                        # Krok 6: Aktywuj sygnał
                        update_stmt = text("""
                            UPDATE trading_signals 
                            SET status = 'ACTIVE', 
                                notes = :notes, 
                                updated_at = NOW() 
                            WHERE id = :signal_id
                        """)
                        session.execute(update_stmt, {
                            'signal_id': signal.id,
                            'notes': f"Sygnał Fib aktywowany. Potwierdzenie H1 (cena {current_price:.2f} w strefie)."
                        })
                        session.commit()
                        
                        # ==================================================================
                        # KROK 2 (Wirtualny Agent): Uruchomienie "Wirtualnego Zakupu"
                        # ==================================================================
                        try:
                            # Przekazujemy cały obiekt 'signal' (Row proxy)
                            virtual_agent.open_virtual_trade(session, signal)
                        except Exception as e:
                            logger.error(f"[Virtual Agent] Nie udało się uruchomić open_virtual_trade dla {ticker} (Fib): {e}", exc_info=True)
                        # ==================================================================

                        # Krok 7: Wyślij alerty
                        alert_msg = f"ALARM CENOWY (Fib H1): {ticker} ({current_price:.2f}) potwierdził setup H1 w strefie Fib!"
                        update_system_control(session, 'system_alert', alert_msg)
                        send_telegram_alert(f"🔔 ALARM CENOWY (Fib H1) 🔔\n{alert_msg}")
                    
                except Exception as e:
                    logger.error(f"[Monitor Fib H1] Błąd podczas przetwarzania H1 dla {ticker}: {e}", exc_info=True)
                    # Kontynuuj pętlę, nie zatrzymuj monitora
            
            # Jeśli cena nie jest w strefie, po prostu zignoruj i sprawdź następnym razem

    except Exception as e:
        logger.error(f"Krytyczny błąd w monitorze Fib H1: {e}", exc_info=True)
        session.rollback()
# ==================================================================
# KONIEC NOWEGO MONITORA
# ==================================================================


def monitor_entry_triggers(session: Session, api_client: AlphaVantageClient):
# ... (bez zmian) ...
    """
    Zoptymalizowany monitor (SZYBKI - co 10s), który używa JEDNEGO zapytania blokowego.
    TERAZ OBSŁUGUJE TYLKO:
    1. Osiągnięcia Take Profit (Wszystkie)
    2. Osiągnięcia Stop Loss (Wszystkie)
    3. Unieważnienia "zużytych" setupów (Wszystkie PENDING)
    4. Osiągnięcia ceny wejścia (TYLKO Breakout / EMA)
    """
    
    # ==================================================================
    # === POPRAWKA (TimeoutError): Dodanie blokady (lock) ===
    # ==================================================================
    try:
        worker_status = get_system_control_value(session, 'worker_status')
        if worker_status and worker_status.startswith('BUSY_'):
            logger.info(f"[Monitor Szybki] Worker jest zajęty ({worker_status}). Pomijanie cyklu, aby zapobiec blokadzie DB.")
            return # Zakończ natychmiast, zwalniając połączenie
    except Exception as e:
        logger.error(f"[Monitor Szybki] Krytyczny błąd podczas sprawdzania statusu workera: {e}", exc_info=True)
        return # Nie kontynuuj, jeśli nie możemy sprawdzić statusu
    # ==================================================================

    market_info = get_market_status_and_time(api_client)
    
    market_status = market_info.get("status")
    if market_status not in ["MARKET_OPEN", "PRE_MARKET", "AFTER_MARKET"]:
        logger.info(f"Market is {market_status}. Skipping Entry Trigger Monitor (Fast).")
        return
        
    logger.info("Running Real-Time Entry Trigger Monitor (Fast - SL/TP/Entry)...")
    
    # ==================================================================
    # KROK 2 POPRAWKI (LOGIKA): Pobieramy teraz *WSZYSTKIE* pola
    # ==================================================================
    all_signals_rows = session.execute(text("""
        SELECT * FROM trading_signals WHERE status IN ('ACTIVE', 'PENDING')
    """)).fetchall() # <-- ZMIANA: Pobieramy * (wszystkie kolumny)
    
    if not all_signals_rows:
        logger.info("No ACTIVE or PENDING signals to monitor.")
        return

    tickers_to_monitor = [row.ticker for row in all_signals_rows]
    logger.info(f"Monitoring {len(tickers_to_monitor)} tickers using 1 bulk request (Fast Monitor).")
    
    try:
        bulk_data_csv = api_client.get_bulk_quotes(tickers_to_monitor)
        if not bulk_data_csv:
            logger.warning("Could not get bulk quote data for monitoring (API error or empty response).")
            return

        parsed_data = _parse_bulk_quotes_csv(bulk_data_csv)
        if not parsed_data:
            logger.warning("Failed to parse bulk quote data.")
            return

        # Teraz iterujemy po sygnałach i sprawdzamy ceny z pobranych danych
        for signal_row in all_signals_rows:
            ticker = signal_row.ticker
            
            quote_data_from_bulk = parsed_data.get(ticker)
            if not quote_data_from_bulk:
                logger.warning(f"No price data for {ticker} in bulk response.")
                continue
            
            current_price = quote_data_from_bulk.get('price')
            if current_price is None:
                continue

            current_price = float(current_price) # Upewnijmy się, że to liczba

            # === POBRANIE KLUCZOWYCH WARTOŚCI Z SYGNAŁU ===
            stop_loss_price = float(signal_row.stop_loss) if signal_row.stop_loss is not None else None
            take_profit_price = float(signal_row.take_profit) if signal_row.take_profit is not None else None
            
            # POPRAWKA (Problem 3): Logika ceny wejścia musi obsługiwać setupy (Breakout/EMA) i (Fib)
            # Dla Breakout/EMA: entry_price
            # Dla Fib: entry_zone_top (chcemy wejść, gdy cena spadnie *do* strefy)
            entry_price_target = None
            if signal_row.entry_price is not None:
                entry_price_target = float(signal_row.entry_price) # Dla Breakout/EMA
            elif signal_row.entry_zone_top is not None:
                entry_price_target = float(signal_row.entry_zone_top) # Dla Fib
            

            # ==================================================================
            # KROK 3 i 4c POPRAWKI (LOGIKA): Monitor Take Profit
            # ==================================================================
            if take_profit_price is not None and current_price >= take_profit_price:
                logger.warning(f"TAKE PROFIT: {ticker} cena LIVE ({current_price}) osiągnęła cel ({take_profit_price}). Zamykanie sygnału.")
                
                # Krok 4c: Dodano ", updated_at = NOW()"
                update_stmt = text("UPDATE trading_signals SET status = 'COMPLETED', notes = :notes, updated_at = NOW() WHERE id = :signal_id")
                session.execute(update_stmt, {
                    'signal_id': signal_row.id,
                    'notes': f"Sygnał zakończony (TAKE PROFIT). Cena LIVE {current_price} >= Cel {take_profit_price}."
                })
                session.commit()
                
                alert_msg = f"TAKE PROFIT: {ticker} ({current_price:.2f}) osiągnął cenę docelową ({take_profit_price:.2f}). Sygnał zakończony."
                update_system_control(session, 'system_alert', alert_msg)
                # ==================================================================
                # KROK 2 (KAT. 1): Wysyłanie alertu na Telegram
                # ==================================================================
                send_telegram_alert(f"✅ TAKE PROFIT ✅\n{alert_msg}")
                # ==================================================================
                
                continue # Przejdź do następnego tickera, ten jest zakończony
            # === Koniec Logiki Take Profit ===


            # ==================================================================
            # KROK 4c POPRAWKI (LOGIKA): Monitor Stop Loss (Strażnik)
            # ==================================================================
            if stop_loss_price is not None and current_price <= stop_loss_price:
                # CENA JEST PONIŻEJ STOP LOSSA!
                logger.warning(f"STOP LOSS (Strażnik): {ticker} cena LIVE ({current_price}) spadła PONIŻEJ Stop Loss ({stop_loss_price}). Unieważnianie setupu.")
                
                # Krok 4c: Dodano ", updated_at = NOW()"
                update_stmt = text("UPDATE trading_signals SET status = 'INVALIDATED', notes = :notes, updated_at = NOW() WHERE id = :signal_id")
                session.execute(update_stmt, {
                    'signal_id': signal_row.id,
                    'notes': f"Setup automatycznie unieważniony (STOP LOSS). Cena LIVE {current_price} <= SL {stop_loss_price}."
                })
                session.commit()
                
                alert_msg = f"STOP LOSS: {ticker} ({current_price:.2f}) spadł poniżej SL ({stop_loss_price:.2f}). Setup unieważniony."
                update_system_control(session, 'system_alert', alert_msg)
                # ==================================================================
                # KROK 2 (KAT. 1): Wysyłanie alertu na Telegram
                # ==================================================================
                send_telegram_alert(f"🛑 STOP LOSS 🛑\n{alert_msg}")
                # ==================================================================
                
                continue # Przejdź do następnego tickera, ten jest już nieważny
            # === Koniec Logiki Stop Loss ===


            # === Logika Wejścia (uruchomi się tylko, jeśli TP i SL nie zostały trafione) ===
            if entry_price_target is None:
                continue
            
            # ==================================================================
            # KROK 3 i 4c POPRAWKI (LOGIKA): Monitor "Zużycia" Setupu (Problem CSTL)
            # ==================================================================
            # Sprawdzamy tylko sygnały PENDING (OCZEKUJĄCE)
            if signal_row.status == 'PENDING':
                # Definiujemy "zużycie" jako sytuację, gdy cena przeskoczyła 
                # poziom wejścia i jest już blisko Take Profit (np. > 30% drogi do TP)
                # To zapobiega wejściu w pozycję ze złym R/R.
                
                if take_profit_price is not None:
                    # Dla setupów Fib, cena startowa jest wyższa niż cel, więc range jest negatywny.
                    # Dla Breakout/EMA, cena startowa jest niższa. Musimy obsłużyć oba.
                    
                    # Użyjmy ceny wejścia dla Breakout/EMA, a strefy dla Fib
                    entry_activation_price = float(signal_row.entry_price) if signal_row.entry_price is not None else float(signal_row.entry_zone_top)
                    
                    # Sprawdź, czy mamy poprawne dane do obliczeń
                    if entry_activation_price is None or entry_activation_price == 0:
                        continue # Nie można obliczyć "zużycia"
                        
                    full_range = take_profit_price - entry_activation_price
                    
                    if full_range > 0: # Tylko dla setupów long (Breakout/EMA/Fib)
                        
                        # Unikaj dzielenia przez zero
                        if full_range == 0:
                            continue

                        gap_percent = (current_price - entry_activation_price) / full_range
                        
                        # Jeśli cena jest już 30% drogi do Take Profit, a my jeszcze nie weszliśmy
                        if gap_percent > 0.30:
                            logger.warning(f"ZUŻYTY SETUP: {ticker} cena LIVE ({current_price}) jest zbyt daleko od wejścia ({entry_activation_price}). Unieważnianie.")
                            
                            # Krok 4c: Dodano ", updated_at = NOW()"
                            update_stmt = text("UPDATE trading_signals SET status = 'INVALIDATED', notes = :notes, updated_at = NOW() WHERE id = :signal_id")
                            session.execute(update_stmt, {
                                'signal_id': signal_row.id,
                                'notes': f"Setup unieważniony (ZUŻYTY). Cena LIVE ({current_price}) zbyt daleko od wejścia ({entry_activation_price})."
                            })
                            session.commit()
                            
                            continue # Przejdź do następnego tickera
            # === Koniec Logiki "Zużycia" ===
            
            
            # ==================================================================
            # ZMODYFIKOWANA LOGIKA ALARMU CENOWEGO (KROK 1)
            # ==================================================================
            
            # GŁÓWNY WARUNEK: Czy aktualna cena jest PONIŻEJ (lub na) ceny wejścia?
            # Dla Breakout/EMA (long) chcemy, aby cena była >= entry_price_target
            # Dla Fib (long) chcemy, aby cena była <= entry_price_target (strefa)
            
            # Scenariusz 1: Breakout/EMA (mają 'entry_price')
            if signal_row.entry_price is not None:
                if current_price >= entry_price_target and signal_row.status == 'PENDING':
                    logger.info(f"ALARM CENOWY (Breakout/EMA): {ticker} cena LIVE ({current_price}) jest w strefie wejścia (>= {entry_price_target}).")
                    logger.info(f"Promowanie sygnału dla {ticker} z PENDING na ACTIVE.")
                    
                    # Krok 4c: Dodano ", updated_at = NOW()"
                    update_stmt = text("UPDATE trading_signals SET status = 'ACTIVE', updated_at = NOW() WHERE id = :signal_id")
                    session.execute(update_stmt, {'signal_id': signal_row.id})
                    session.commit() # Commitujemy od razu zmianę statusu

                    # ==================================================================
                    # KROK 2 (Wirtualny Agent): Uruchomienie "Wirtualnego Zakupu"
                    # ==================================================================
                    try:
                        # Przekazujemy cały obiekt 'signal_row' (Row proxy)
                        virtual_agent.open_virtual_trade(session, signal_row)
                    except Exception as e:
                        logger.error(f"[Virtual Agent] Nie udało się uruchomić open_virtual_trade dla {ticker} (Breakout/EMA): {e}", exc_info=True)
                    # ==================================================================
                    
                    # Zawsze generuj alert, gdy cena jest w strefie wejścia
                    alert_msg = f"ALARM CENOWY: {ticker} ({current_price:.2f}) osiągnął strefę wejścia!"
                    update_system_control(session, 'system_alert', alert_msg)
                    # ==================================================================
                    # KROK 2 (KAT. 1): Wysyłanie alertu na Telegram
                    # ==================================================================
                    send_telegram_alert(f"🔔 ALARM CENOWY 🔔\n{alert_msg}")
                    # ==================================================================
            
            # Scenariusz 2: Fib (nie ma 'entry_price', ale ma 'entry_zone_top')
            elif signal_row.entry_zone_top is not None:
                # Dla Fib, ten monitor (Szybki) nic nie robi.
                # Czeka na monitor H1 (Wolny), który zajmie się aktywacją.
                if current_price <= entry_price_target: # Cena jest w strefie
                    logger.info(f"[Monitor Szybki] INFO: {ticker} ({current_price:.2f}) jest w strefie Fib. Oczekiwanie na monitor H1.")
                # Celowo nie robimy nic i nie wysyłamy alertu
        
    except Exception as e:
        logger.error(f"Error during bulk monitoring: {e}", exc_info=True)
        session.rollback()


def _find_impulse_and_fib_zone(daily_df: pd.DataFrame) -> dict | None:
# ... (bez zmian) ...
    try:
        if len(daily_df) < 21: return None
        recent_df = daily_df.iloc[-21:]
        low_point_price = recent_df['low'].min()
        low_point_date_loc = recent_df['low'].idxmin()
        if low_point_date_loc not in recent_df.index:
             logger.warning(f"Cannot find low point date {low_point_date_loc} in recent_df index for Fib calculation.")
             return None
        df_after_low = recent_df[recent_df.index > low_point_date_loc]
        if df_after_low.empty: return None
        high_point_price = df_after_low['high'].max()
        if low_point_price <= 0: return None
        impulse_strength = (high_point_price - low_point_price) / low_point_price
        if impulse_strength < 0.10: # Wymagamy minimum 10% impulsu
            logger.info(f"Impuls dla {daily_df.index[-1]} zbyt słaby ({impulse_strength:.2%}).")
            return None
            
        logger.info(f"Znaleziono silny impuls ({impulse_strength:.2%}) dla {daily_df.index[-1]} od {low_point_date_loc}.")
        return {
            "impulse_high": high_point_price,
            "impulse_low": low_point_price,
            "entry_zone_top": high_point_price - 0.382 * (high_point_price - low_point_price),
            "entry_zone_bottom": high_point_price - 0.618 * (high_point_price - low_point_price)
        }
    except Exception as e:
        logger.error(f"Error in _find_impulse_and_fib_zone: {e}", exc_info=True)
        return None
