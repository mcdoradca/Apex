import logging
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
import pytz

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float, update_system_control
from ..config import Phase3Config

logger = logging.getLogger(__name__)

def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.split('. ')[-1] for col in df.columns]
    df['open'] = pd.to_numeric(df['open'], errors='coerce')
    df['high'] = pd.to_numeric(df['high'], errors='coerce')
    df['low'] = pd.to_numeric(df['low'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    return df

def is_market_open() -> bool:
    """Sprawdza, czy giełda NASDAQ jest otwarta."""
    tz = pytz.timezone('US/Eastern')
    now = datetime.now(tz)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0).time()
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0).time()
    return market_open <= now.time() <= market_close and now.weekday() < 5

def _find_impulse_and_fib_zone(daily_df: pd.DataFrame) -> dict | None:
    """Znajduje ostatni impuls i strefę korekty Fibonacciego na wykresie dziennym."""
    try:
        if len(daily_df) < 21: return None
        recent_df = daily_df.iloc[-21:]
        
        low_point_price = recent_df['low'].min()
        low_point_date = recent_df['low'].idxmin()
        
        df_after_low = recent_df[recent_df.index > low_point_date]
        if df_after_low.empty: return None
        
        high_point_price = df_after_low['high'].max()

        if (high_point_price - low_point_price) / low_point_price < 0.10: # Impuls min 10%
            return None
            
        return {
            "impulse_high": high_point_price,
            "impulse_low": low_point_price,
            "entry_zone_top": high_point_price - 0.382 * (high_point_price - low_point_price),
            "entry_zone_bottom": high_point_price - 0.618 * (high_point_price - low_point_price)
        }
    except Exception as e:
        logger.error(f"Error in _find_impulse_and_fib_zone: {e}")
        return None

def _find_entry_signal_candle(intraday_df: pd.DataFrame) -> pd.Series | None:
    """Znajduje byczą świecę sygnałową na wykresie godzinowym."""
    try:
        if intraday_df.empty: return None

        for _, candle in intraday_df.iloc[-8:].iterrows():
            is_bullish = candle['close'] > candle['open']
            is_in_upper_half = candle['close'] > (candle['high'] + candle['low']) / 2
            has_strong_body = (candle['close'] - candle['open']) > 0.3 * (candle['high'] - candle['low'])

            if is_bullish and is_in_upper_half and has_strong_body:
                return candle
        return None
    except Exception as e:
        logger.error(f"Error in _find_entry_signal_candle: {e}")
        return None

def find_end_of_day_setup(ticker: str, api_client: AlphaVantageClient) -> dict:
    """
    Funkcja dla SKANERA NOCNEGO. Analizuje tylko wykres DZIENNY w poszukiwaniu
    prawidłowej struktury (impuls + cena w strefie), aby stworzyć sygnał PENDING.
    """
    daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
    if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw:
        return {"signal": False, "reason": "Brak danych dziennych."}

    daily_df = pd.DataFrame.from_dict(daily_data_raw['Time Series (Daily)'], orient='index')
    daily_df = standardize_df_columns(daily_df)
    daily_df.sort_index(inplace=True)

    impulse_result = _find_impulse_and_fib_zone(daily_df)
    if not impulse_result:
        return {"signal": False, "reason": "Brak impulsu >10% w ostatnich 21 dniach."}
    
    current_price = daily_df['close'].iloc[-1]
    is_in_zone = impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"]
    
    if not is_in_zone:
        return {"signal": False, "reason": f"Cena ({current_price:.2f}) poza strefą Fib."}

    return {
        "signal": True, "status": "PENDING",
        "ticker": ticker,
        "entry_zone_bottom": float(impulse_result['entry_zone_bottom']),
        "entry_zone_top": float(impulse_result['entry_zone_top']),
        "take_profit": float(impulse_result['impulse_high']),
        "notes": f"Setup EOD prawidłowy. Cena ({current_price:.2f}) w strefie. Oczekuje na sygnał intraday."
    }

def plan_trade_on_demand(ticker: str, api_client: AlphaVantageClient) -> dict:
    """
    Funkcja dla MONITORA (w trakcie sesji). Bierze sygnał PENDING i poluje na 
    świecę godzinową, liczy R/R i promuje do ACTIVE.
    """
    # 1. Pobierz dane dzienne i godzinowe
    daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
    intraday_data_raw = api_client.get_intraday(ticker, interval='60min')
    atr_data_raw = api_client.get_atr(ticker)

    if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw or \
       not intraday_data_raw or 'Time Series (60min)' not in intraday_data_raw or \
       not atr_data_raw or 'Technical Analysis: ATR' not in atr_data_raw:
        return {"signal": False, "reason": "Brak kompletnych danych (D1, H1, ATR)."}

    daily_df = pd.DataFrame.from_dict(daily_data_raw['Time Series (Daily)'], orient='index')
    daily_df = standardize_df_columns(daily_df)
    daily_df.sort_index(inplace=True)

    intraday_df = pd.DataFrame.from_dict(intraday_data_raw['Time Series (60min)'], orient='index')
    intraday_df = standardize_df_columns(intraday_df)
    intraday_df.sort_index(inplace=True)

    # 2. Znajdź impuls i strefę (tak jak robi to skaner nocny)
    impulse_result = _find_impulse_and_fib_zone(daily_df)
    if not impulse_result:
        return {"signal": False, "reason": "Struktura impulsu zanikła."}

    # 3. Sprawdź, czy cena jest w strefie i czy jest świeca sygnałowa
    current_price = intraday_df['close'].iloc[-1]
    is_in_zone = impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"]
    if not is_in_zone:
        return {"signal": True, "status": "PENDING", "reason": f"Cena ({current_price:.2f}) wyszła ze strefy."}

    signal_candle = _find_entry_signal_candle(intraday_df)
    if signal_candle is None:
        return {"signal": True, "status": "PENDING", "reason": "Cena w strefie, brak świecy sygnałowej H1."}

    # 4. Jeśli wszystko się zgadza, oblicz plan transakcji oparty na ATR
    try:
        latest_atr_date = sorted(atr_data_raw['Technical Analysis: ATR'].keys(), reverse=True)[0]
        latest_atr = safe_float(atr_data_raw['Technical Analysis: ATR'][latest_atr_date]['ATR'])
        if not latest_atr:
            return {"signal": False, "reason": "Błąd odczytu ATR."}
    except (IndexError, KeyError):
        return {"signal": False, "reason": "Błąd odczytu ATR."}

    entry_price = signal_candle['high'] + 0.01
    stop_loss = entry_price - (Phase3Config.ATR_MULTIPLIER_FOR_SL * latest_atr)
    take_profit = impulse_result['impulse_high']
    
    potential_risk = entry_price - stop_loss
    potential_profit = take_profit - entry_price
    
    if potential_risk <= 0:
        return {"signal": False, "reason": "Błąd kalkulacji: Ryzyko zerowe lub ujemne."}
    
    risk_reward_ratio = potential_profit / potential_risk

    if risk_reward_ratio < Phase3Config.MIN_RISK_REWARD_RATIO:
        return {
            "signal": True, "status": "PENDING",
            "reason": f"Sygnał wejścia, ale R/R ({risk_reward_ratio:.2f}) poniżej progu ({Phase3Config.MIN_RISK_REWARD_RATIO})."
        }

    return {
        "signal": True, "status": "ACTIVE",
        "ticker": ticker,
        "entry_price": float(entry_price),
        "stop_loss": float(stop_loss),
        "take_profit": float(take_profit),
        "risk_reward_ratio": float(risk_reward_ratio),
        "notes": f"SYGNAŁ AKTYWNY. Bycza świeca z {signal_candle.name} w strefie Fib. SL oparty na ATR ({latest_atr:.2f})."
    }

def run_tactical_planning(session: Session, qualified_tickers: list[str], get_current_state, api_client: AlphaVantageClient):
    """Główna funkcja dla SKANERA NOCNEGO."""
    logger.info("Running Phase 3: End-of-Day Tactical Planning...")
    append_scan_log(session, "Faza 3: Skanowanie EOD w poszukiwaniu setupów...")
    
    for ticker in qualified_tickers:
        try:
            # Używamy funkcji do analizy końca dnia
            trade_setup = find_end_of_day_setup(ticker, api_client)
            
            if trade_setup.get("signal"):
                stmt = text("""
                    INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, notes, entry_zone_bottom, entry_zone_top)
                    VALUES (:ticker, NOW(), :status, NULL, NULL, :tp, NULL, :notes, :ezb, :ezt)
                    ON CONFLICT (ticker) DO UPDATE SET 
                        status = EXCLUDED.status, generation_date = EXCLUDED.generation_date, take_profit = EXCLUDED.take_profit,
                        notes = EXCLUDED.notes, entry_zone_bottom = EXCLUDED.entry_zone_bottom, entry_zone_top = EXCLUDED.entry_zone_top;
                """)
                session.execute(stmt, {
                    'ticker': ticker, 
                    'status': trade_setup['status'],
                    'tp': trade_setup.get('take_profit'), 
                    'notes': trade_setup.get('notes'),
                    'ezb': trade_setup.get('entry_zone_bottom'),
                    'ezt': trade_setup.get('entry_zone_top')
                })
                session.commit()
                append_scan_log(session, f"PENDING (F3): {ticker} dodany do listy obserwacyjnej.")
            else:
                append_scan_log(session, f"INFO (F3): {ticker} - {trade_setup.get('reason')}")

        except Exception as e:
            logger.error(f"Error in Phase 3 for {ticker}: {e}", exc_info=True)
            session.rollback()
            
    append_scan_log(session, "Faza 3 (Skaner EOD) zakończona.")

def monitor_pending_signals(session: Session, api_client: AlphaVantageClient):
    """Główna funkcja dla MONITORA (w trakcie sesji)."""
    if not is_market_open():
        logger.info("Market is closed. Skipping pending signals monitor.")
        return
        
    logger.info("Market is open. Running pending signals monitor...")
    
    pending_signals = session.execute(text("SELECT ticker FROM trading_signals WHERE status = 'PENDING'")).fetchall()
    
    for signal in pending_signals:
        ticker = signal[0]
        try:
            # Używamy pełnej logiki "na żądanie" do analizy
            trade_plan = plan_trade_on_demand(ticker, api_client)
            
            if trade_plan.get("status") == "ACTIVE":
                update_stmt = text("""
                    UPDATE trading_signals 
                    SET status = 'ACTIVE', 
                        entry_price = :entry, stop_loss = :sl, take_profit = :tp, risk_reward_ratio = :rr, 
                        notes = :notes, generation_date = NOW()
                    WHERE ticker = :ticker;
                """)
                session.execute(update_stmt, {
                    'ticker': ticker,
                    'entry': trade_plan['entry_price'],
                    'sl': trade_plan['stop_loss'],
                    'tp': trade_plan['take_profit'],
                    'rr': trade_plan['risk_reward_ratio'],
                    'notes': trade_plan['notes']
                })
                session.commit()

                alert_msg = f"SYGNAŁ AKTYWNY: {ticker} gotowy do wejścia! R/R={trade_plan['risk_reward_ratio']:.2f}"
                update_system_control(session, 'system_alert', alert_msg)
                logger.info(alert_msg)
            else:
                logger.info(f"Monitor: {ticker} - {trade_plan.get('reason', 'Nadal PENDING.')}")
        
        except Exception as e:
            logger.error(f"Error monitoring PENDING signal for {ticker}: {e}", exc_info=True)
            session.rollback()

