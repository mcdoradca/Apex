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
    send_telegram_alert
)
from ..config import Phase3Config

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
# (Ta część jest już zoptymalizowana i pozostaje bez zmian)

def _find_breakout_setup(daily_df: pd.DataFrame, min_consolidation_days=5, breakout_atr_multiplier=1.0) -> dict | None:
# ... (bez zmian) ...
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
            # KROK 1 POPRAWKI (STRATEGIA): Zmiana mnożnika ATR dla SL
            # ==================================================================
            return {
                "setup_type": "BREAKOUT",
                "entry_price": latest_candle['high'] + 0.01,
                "stop_loss": consolidation_high - (0.7 * current_atr), # ZMIANA z 0.5 na 0.7
                "consolidation_high": consolidation_high,
                "atr": current_atr
            }
        return None
    except Exception as e:
        logger.error(f"Error in _find_breakout_setup: {e}")
        return None

def _find_ema_bounce_setup(daily_df: pd.DataFrame, ema_period=9) -> dict | None:
# ... (bez zmian) ...
    try:
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
             logger.info(f"EMA Bounce setup found for {daily_df.index[-1]}")
             high_low = daily_df['high'] - daily_df['low']
             high_close = (daily_df['high'] - daily_df['close'].shift()).abs()
             low_close = (daily_df['low'] - daily_df['close'].shift()).abs()
             tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
             atr = calculate_ema(tr, 14).iloc[-1]
             # ==================================================================
             # KROK 1 POPRAWKI (STRATEGIA): Zmiana mnożnika ATR dla SL
             # ==================================================================
             return {
                 "setup_type": "EMA_BOUNCE",
                 "entry_price": latest_candle['high'] + 0.01,
                 "stop_loss": latest_candle['low'] - (0.7 * atr), # ZMIANA z 0.5 na 0.7
                 "ema_value": latest_ema,
                 "atr": atr
             }
        return None
    except Exception as e:
        logger.error(f"Error in _find_ema_bounce_setup: {e}")
        return None

def find_end_of_day_setup(ticker: str, daily_df: pd.DataFrame) -> dict:
# ... (bez zmian) ...
    if daily_df.empty or len(daily_df) < 21:
         return {"signal": False, "reason": "Niewystarczająca historia danych dziennych (otrzymana z Fazy 2)."}
    current_price = daily_df['close'].iloc[-1]
    breakout_setup = _find_breakout_setup(daily_df)
    if breakout_setup:
        risk = breakout_setup['entry_price'] - breakout_setup['stop_loss']
        if risk <= 0: return {"signal": False, "reason": "Błąd kalkulacji ryzyka (Breakout)."}
        take_profit = breakout_setup['entry_price'] + (Phase3Config.TARGET_RR_RATIO * risk)
        return {
            "signal": True, "status": "PENDING", # <-- POPRAWKA (Problem 3): Zmiana z "ACTIVE" na "PENDING"
            "ticker": ticker,
            "entry_price": float(breakout_setup['entry_price']),
            "stop_loss": float(breakout_setup['stop_loss']),
            "take_profit": float(take_profit),
            "risk_reward_ratio": Phase3Config.TARGET_RR_RATIO,
            "notes": f"Setup EOD (OCZEKUJĄCY): Wybicie z konsolidacji. Opór: {breakout_setup['consolidation_high']:.2f}." # <-- Notatka zaktualizowana
        }
    ema_bounce_setup = _find_ema_bounce_setup(daily_df)
    if ema_bounce_setup:
        risk = ema_bounce_setup['entry_price'] - ema_bounce_setup['stop_loss']
        if risk <= 0: return {"signal": False, "reason": "Błąd kalkulacji ryzyka (EMA Bounce)."}
        take_profit = ema_bounce_setup['entry_price'] + (Phase3Config.TARGET_RR_RATIO * risk)
        return {
            "signal": True, "status": "PENDING", # <-- POPRAWKA (Problem 3): Zmiana z "ACTIVE" na "PENDING"
            "ticker": ticker,
            "entry_price": float(ema_bounce_setup['entry_price']),
            "stop_loss": float(ema_bounce_setup['stop_loss']),
            "take_profit": float(take_profit),
            "risk_reward_ratio": Phase3Config.TARGET_RR_RATIO,
            "notes": f"Setup EOD (OCZEKUJĄCY): Odbicie od rosnącej EMA{Phase3Config.EMA_PERIOD}. EMA={ema_bounce_setup['ema_value']:.2f}." # <-- Notatka zaktualizowana
        }
    impulse_result = _find_impulse_and_fib_zone(daily_df)
    if impulse_result:
        is_in_zone = impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"]
        if is_in_zone:
            take_profit = float(impulse_result['impulse_high'])
            return {
                "signal": True, "status": "PENDING",
                "ticker": ticker,
                "entry_zone_bottom": float(impulse_result['entry_zone_bottom']),
                "entry_zone_top": float(impulse_result['entry_zone_top']),
                "take_profit": take_profit,
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
                    alert_msg = f"NOWY SYGNAŁ AKTYWNY (EOD): {ticker} gotowy do wejścia!"
                    update_system_control(session, 'system_alert', alert_msg)
            else:
                append_scan_log(session, f"INFO (F3): {ticker} - {trade_setup.get('reason')}")
        except Exception as e:
            logger.error(f"Error in Phase 3 EOD scan for {ticker}: {e}", exc_info=True)
            session.rollback()
    append_scan_log(session, f"Faza 3 (Skaner EOD) zakończona. Znaleziono {successful_setups} setupów.")


# --- SEKCJA MONITORA CZASU RZECZYWISTEGO ---

def monitor_entry_triggers(session: Session, api_client: AlphaVantageClient):
# ... (bez zmian) ...
    """
    Zoptymalizowany monitor, który używa JEDNEGO zapytania blokowego do sprawdzenia
    WSZYSTKICH sygnałów Fazy 3 (PENDING i ACTIVE) pod kątem:
    1. Osiągnięcia Take Profit (NOWE)
    2. Osiągnięcia Stop Loss (istniejące)
    3. Unieważnienia "zużytych" setupów (NOWE)
    4. Osiągnięcia ceny wejścia (istniejące)
    """
    market_info = get_market_status_and_time(api_client)
    
    market_status = market_info.get("status")
    if market_status not in ["MARKET_OPEN", "PRE_MARKET", "AFTER_MARKET"]:
        logger.info(f"Market is {market_status}. Skipping Entry Trigger Monitor.")
        return
        
    logger.info("Running Real-Time Entry Trigger Monitor (Optimized)...")
    
    # ==================================================================
    # KROK 2 POPRAWKI (LOGIKA): Pobieramy teraz *WSZYSTKIE* pola
    # ==================================================================
    all_signals_rows = session.execute(text("""
        SELECT id, ticker, status, entry_price, entry_zone_bottom, entry_zone_top, stop_loss, take_profit 
        FROM trading_signals WHERE status IN ('ACTIVE', 'PENDING')
    """)).fetchall()
    
    if not all_signals_rows:
        logger.info("No ACTIVE or PENDING signals to monitor.")
        return

    tickers_to_monitor = [row.ticker for row in all_signals_rows]
    logger.info(f"Monitoring {len(tickers_to_monitor)} tickers using 1 bulk request.")
    
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
                    full_range = take_profit_price - entry_price_target
                    if full_range > 0: # Upewnij się, że nie dzielimy przez zero
                        gap_percent = (current_price - entry_price_target) / full_range
                        
                        # Jeśli cena jest już 30% drogi do Take Profit, a my jeszcze nie weszliśmy
                        if gap_percent > 0.30:
                            logger.warning(f"ZUŻYTY SETUP: {ticker} cena LIVE ({current_price}) jest zbyt daleko od wejścia ({entry_price_target}). Unieważnianie.")
                            
                            # Krok 4c: Dodano ", updated_at = NOW()"
                            update_stmt = text("UPDATE trading_signals SET status = 'INVALIDATED', notes = :notes, updated_at = NOW() WHERE id = :signal_id")
                            session.execute(update_stmt, {
                                'signal_id': signal_row.id,
                                'notes': f"Setup unieważniony (ZUŻYTY). Cena LIVE ({current_price}) zbyt daleko od wejścia ({entry_price_target})."
                            })
                            session.commit()
                            
                            continue # Przejdź do następnego tickera
            # === Koniec Logiki "Zużycia" ===
            
            
            # === Logika Alarmu Cenowego (Główny warunek wejścia) ===
            # Ta logika uruchomi się tylko, jeśli:
            # 1. Nie trafiono TP
            # 2. Nie trafiono SL
            # 3. Setup PENDING nie został "zużyty"
            
            # GŁÓWNY WARUNEK: Czy aktualna cena jest PONIŻEJ (lub na) ceny wejścia?
            # (Dla setupów 'long' chcemy kupić po cenie X lub taniej)
            # POPRAWKA (Problem 3): Ta logika zadziała teraz dla PENDING
            if current_price <= entry_price_target:
                
                # Jeśli sygnał był PENDING, promuj go na ACTIVE
                if signal_row.status == 'PENDING':
                    logger.info(f"ALARM CENOWY: {ticker} cena LIVE ({current_price}) jest w strefie wejścia (<= {entry_price_target}).")
                    logger.info(f"Promowanie sygnału dla {ticker} z PENDING na ACTIVE.")
                    
                    # Krok 4c: Dodano ", updated_at = NOW()"
                    update_stmt = text("UPDATE trading_signals SET status = 'ACTIVE', updated_at = NOW() WHERE id = :signal_id")
                    session.execute(update_stmt, {'signal_id': signal_row.id})
                    session.commit() # Commitujemy od razu zmianę statusu
                    
                    # Zawsze generuj alert, gdy cena jest w strefie wejścia
                    alert_msg = f"ALARM CENOWY: {ticker} ({current_price:.2f}) osiągnął strefę wejścia!"
                    update_system_control(session, 'system_alert', alert_msg)
                    # ==================================================================
                    # KROK 2 (KAT. 1): Wysyłanie alertu na Telegram
                    # ==================================================================
                    send_telegram_alert(f"🔔 ALARM CENOWY 🔔\n{alert_msg}")
                    # ==================================================================
                
                # Jeśli status był już ACTIVE (np. cena ponownie spadła do strefy), nie wysyłaj alertu ponownie.
        
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
        if impulse_strength < 0.10:
            return None
        return {
            "impulse_high": high_point_price,
            "impulse_low": low_point_price,
            "entry_zone_top": high_point_price - 0.382 * (high_point_price - low_point_price),
            "entry_zone_bottom": high_point_price - 0.618 * (high_point_price - low_point_price)
        }
    except Exception as e:
        logger.error(f"Error in _find_impulse_and_fib_zone: {e}", exc_info=True)
        return None
