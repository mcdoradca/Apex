import logging
import time
import json
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float
from ..config import MIN_RISK_REWARD_RATIO, ATR_STOP_LOSS_MULTIPLIER

logger = logging.getLogger(__name__)

def _find_impulse_and_fib_zone(daily_data: dict, atr_value: float) -> tuple[float, float, float, float] | None:
    """Znajduje impuls i strefę Fibonacciego, używając ATR do walidacji impulsu."""
    try:
        time_series = daily_data.get('Time Series (Daily)')
        if not time_series or len(time_series) < 21: return None

        dates = sorted(time_series.keys(), reverse=True)[:21]
        prices = {d: {k.split(' ')[1]: float(v) for k, v in time_series[d].items()} for d in dates}

        low_point_date = min(dates, key=lambda d: prices[d]['low'])
        low_point_price = prices[low_point_date]['low']
        
        dates_after_low = [d for d in dates if d > low_point_date]
        if not dates_after_low: return None
        
        high_point_date = max(dates_after_low, key=lambda d: prices[d]['high'])
        high_point_price = prices[high_point_date]['high']
        
        # Ulepszenie: Sprawdź, czy ruch to impuls > 1.0 * ATR
        if (high_point_price - low_point_price) < (1.0 * atr_value):
            return None
            
        fib_50 = high_point_price - 0.5 * (high_point_price - low_point_price)
        fib_61_8 = high_point_price - 0.618 * (high_point_price - low_point_price)
        
        return high_point_price, low_point_price, fib_50, fib_61_8
    except Exception as e:
        logger.error(f"Error in _find_impulse_and_fib_zone: {e}")
        return None

def _find_entry_signal_candle(intraday_data: dict) -> dict | None:
    try:
        time_series = intraday_data.get('Time Series (60min)')
        if not time_series: return None
        # Szukamy w ostatnich 8 świecach (8 godzin handlu)
        for dt, values in list(time_series.items())[:8]:
            o, h, l, c = safe_float(values.get('1. open')), safe_float(values.get('2. high')), safe_float(values.get('3. low')), safe_float(values.get('4. close'))
            if not all([o, h, l, c]): continue

            is_bullish, is_in_upper_half, has_strong_body = c > o, c > (h + l) / 2, (c - o) > 0.3 * (h - l)
            if is_bullish and is_in_upper_half and has_strong_body:
                return {**values, 'datetime': dt}
        return None
    except Exception as e:
        logger.error(f"Error in _find_entry_signal_candle: {e}")
        return None

def _generate_trade_plan(ticker: str, api_client: AlphaVantageClient) -> dict:
    """Generuje plan taktyczny dla pojedynczego tickera. Zwraca status i szczegóły."""
    try:
        daily_data = api_client.get_daily_adjusted(ticker, 'compact')
        atr_data = api_client.get_atr(ticker)
        if not daily_data or not atr_data:
            return {"status": "NO_SIGNAL", "reason": "Brak kompletnych danych dziennych lub ATR."}

        latest_atr = safe_float(list(atr_data['Technical Analysis: ATR'].values())[0]['ATR'])
        if not latest_atr:
            return {"status": "NO_SIGNAL", "reason": "Nie można odczytać wartości ATR."}

        impulse_result = _find_impulse_and_fib_zone(daily_data, latest_atr)
        if not impulse_result:
            return {"status": "NO_SIGNAL", "reason": "Brak znaczącego impulsu (>1.0 ATR) w ciągu ostatnich 21 sesji."}
        
        impulse_high, impulse_low, entry_zone_top, entry_zone_bottom = impulse_result
        current_price = safe_float(list(daily_data['Time Series (Daily)'].values())[0]['4. close'])
        if not (entry_zone_bottom <= current_price <= entry_zone_top):
            return {"status": "NO_SIGNAL", "reason": f"Cena ({current_price:.2f}) poza strefą wejścia Fib ({entry_zone_bottom:.2f} - {entry_zone_top:.2f})."}
        
        intraday_data = api_client.get_intraday(ticker)
        signal_candle = _find_entry_signal_candle(intraday_data)
        if not signal_candle:
            return {"status": "NO_SIGNAL", "reason": "W strefie wejścia, ale brak byczej świecy sygnałowej w ostatnich 8 godzinach."}

        # Dynamiczne zarządzanie ryzykiem oparte na ATR
        entry_price = current_price
        stop_loss = entry_price - (ATR_STOP_LOSS_MULTIPLIER * latest_atr)
        take_profit = entry_price + (MIN_RISK_REWARD_RATIO * (entry_price - stop_loss))
        
        potential_risk = entry_price - stop_loss
        potential_profit = take_profit - entry_price
        risk_reward_ratio = potential_profit / potential_risk if potential_risk > 0 else 0

        if risk_reward_ratio < MIN_RISK_REWARD_RATIO:
            return {"status": "NO_SIGNAL", "reason": f"Znaleziono setup, ale R/R ({risk_reward_ratio:.2f}) jest zbyt niskie."}

        return {
            "status": "SIGNAL_FOUND",
            "entry_price": entry_price, "stop_loss": stop_loss, "take_profit": take_profit,
            "risk_reward_ratio": risk_reward_ratio,
            "signal_candle_timestamp": signal_candle['datetime'],
            "details": {"atr": latest_atr, "atr_multiplier": ATR_STOP_LOSS_MULTIPLIER}
        }
    except Exception as e:
        logger.error(f"Error generating trade plan for {ticker}: {e}")
        return {"status": "ERROR", "reason": str(e)}

def plan_trade_on_demand(ticker: str, api_client: AlphaVantageClient) -> dict:
    """Funkcja obsługująca Fazy 3 na żądanie."""
    logger.info(f"Running Phase 3 On-Demand for {ticker}...")
    return _generate_trade_plan(ticker, api_client)

def run_tactical_planning(session: Session, qualified_tickers: list[str], get_current_state, api_client: AlphaVantageClient):
    logger.info("Running Phase 3: Sniper Agent Tactical Planning...")
    append_scan_log(session, "Faza 3: Generowanie planów taktycznych...")
    
    total_qualified = len(qualified_tickers)
    update_scan_progress(session, 0, total_qualified)
    processed_count = 0

    for ticker in qualified_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        plan = _generate_trade_plan(ticker, api_client)

        if plan['status'] == 'SIGNAL_FOUND':
            stmt = text("""
                INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, signal_candle_timestamp, details_json)
                VALUES (:ticker, :gen_date, 'ACTIVE', :entry, :sl, :tp, :rr, :candle_ts, :details)
            """)
            candle_ts = datetime.strptime(plan['signal_candle_timestamp'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            session.execute(stmt, {
                'ticker': ticker, 'gen_date': datetime.now(timezone.utc), 
                'entry': plan['entry_price'], 'sl': plan['stop_loss'], 'tp': plan['take_profit'], 
                'rr': plan['risk_reward_ratio'], 'candle_ts': candle_ts,
                'details': json.dumps(plan['details'])
            })
            session.commit()
            log_msg = f"SYGNAŁ WYGENEROWANY dla {ticker}: Wejście={plan['entry_price']:.2f}, SL={plan['stop_loss']:.2f}, TP={plan['take_profit']:.2f}, R/R={plan['risk_reward_ratio']:.2f}"
            append_scan_log(session, log_msg)
        else:
            append_scan_log(session, f"{ticker}: {plan['reason']}")
        
        processed_count += 1
        update_scan_progress(session, processed_count, total_qualified)
            
    append_scan_log(session, "Faza 3 zakończona.")
