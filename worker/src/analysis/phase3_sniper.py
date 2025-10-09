import logging
import time
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float
from ..config import MIN_RISK_REWARD_RATIO

logger = logging.getLogger(__name__)

def _find_impulse_and_fib_zone(daily_data: dict) -> tuple[float, float, float, float] | None:
    try:
        time_series = daily_data.get('Time Series (Daily)')
        if not time_series or len(time_series) < 21: return None

        dates = sorted(time_series.keys(), reverse=True)[:21] # Ostatnie 21 sesji
        prices = {d: {k.split(' ')[1]: float(v) for k, v in time_series[d].items()} for d in dates}

        # Znajdź najniższy dołek i najwyższy szczyt w tym okresie
        low_point_date = min(dates, key=lambda d: prices[d]['low'])
        low_point_price = prices[low_point_date]['low']
        
        # Szukaj szczytu po dołku
        dates_after_low = [d for d in dates if d > low_point_date]
        if not dates_after_low: return None
        
        high_point_date = max(dates_after_low, key=lambda d: prices[d]['high'])
        high_point_price = prices[high_point_date]['high']
        
        # Sprawdź, czy ruch to impuls > 10%
        if (high_point_price - low_point_price) / low_point_price < 0.10:
            return None
            
        # Oblicz strefę Fibonacciego
        fib_50 = high_point_price - 0.5 * (high_point_price - low_point_price)
        fib_61_8 = high_point_price - 0.618 * (high_point_price - low_point_price)
        
        return high_point_price, low_point_price, fib_50, fib_61_8 # impulse_high, impulse_low, entry_zone_top, entry_zone_bottom
    except Exception as e:
        logger.error(f"Error in _find_impulse_and_fib_zone: {e}")
        return None


def _find_entry_signal_candle(intraday_data: dict) -> dict | None:
    try:
        time_series = intraday_data.get('Time Series (60min)')
        if not time_series: return None

        for dt, values in time_series.items():
            o = safe_float(values.get('1. open'))
            h = safe_float(values.get('2. high'))
            l = safe_float(values.get('3. low'))
            c = safe_float(values.get('4. close'))

            if not all([o, h, l, c]): continue

            is_bullish = c > o
            is_in_upper_half = c > (h + l) / 2
            has_strong_body = (c - o) > 0.3 * (h - l)

            if is_bullish and is_in_upper_half and has_strong_body:
                return {**values, 'datetime': dt} # Zwróć pasującą świecę wraz z datą
        return None
    except Exception as e:
        logger.error(f"Error in _find_entry_signal_candle: {e}")
        return None

def run_tactical_planning(session: Session, qualified_tickers: list[str], get_current_state, api_client: AlphaVantageClient):
    logger.info("Running Phase 3: Sniper Agent Tactical Planning...")
    append_scan_log(session, "Faza 3: Generowanie planów taktycznych...")
    
    total_qualified = len(qualified_tickers)
    update_scan_progress(session, 0, total_qualified)
    processed_count = 0

    for ticker in qualified_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            daily_data = api_client.get_daily_adjusted(ticker, 'compact')
            if not daily_data: continue

            impulse_result = _find_impulse_and_fib_zone(daily_data)
            if not impulse_result:
                append_scan_log(session, f"{ticker}: Brak wystarczającego impulsu do analizy taktycznej.")
                continue
            
            impulse_high, impulse_low, entry_zone_top, entry_zone_bottom = impulse_result
            current_price = safe_float(list(daily_data['Time Series (Daily)'].values())[0]['4. close'])
            if not (entry_zone_bottom <= current_price <= entry_zone_top):
                append_scan_log(session, f"{ticker}: Cena ({current_price:.2f}) poza strefą wejścia Fib ({entry_zone_bottom:.2f} - {entry_zone_top:.2f}). Obserwuj.")
                continue
            
            intraday_data = api_client.get_intraday(ticker)
            if not intraday_data: continue
            
            signal_candle = _find_entry_signal_candle(intraday_data)
            if not signal_candle:
                append_scan_log(session, f"{ticker}: W strefie wejścia, ale brak świecy sygnałowej. Obserwuj.")
                continue

            entry_price = safe_float(signal_candle.get('2. high')) + 0.01
            stop_loss = safe_float(signal_candle.get('3. low')) - 0.01
            take_profit = impulse_high
            
            if entry_price is None or stop_loss is None or take_profit is None or stop_loss >= entry_price:
                continue
                
            potential_risk = entry_price - stop_loss
            potential_profit = take_profit - entry_price
            if potential_risk == 0: continue
            
            risk_reward_ratio = potential_profit / potential_risk

            if risk_reward_ratio >= MIN_RISK_REWARD_RATIO:
                stmt = text("""
                    INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, signal_candle_timestamp)
                    VALUES (:ticker, :gen_date, 'ACTIVE', :entry, :sl, :tp, :rr, :candle_ts)
                """)
                candle_ts = datetime.strptime(signal_candle['datetime'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                session.execute(stmt, {'ticker': ticker, 'gen_date': datetime.now(timezone.utc), 'entry': entry_price, 'sl': stop_loss, 
                                       'tp': take_profit, 'rr': risk_reward_ratio, 'candle_ts': candle_ts})
                session.commit()
                log_msg = f"SYGNAŁ WYGENEROWANY dla {ticker}: Wejście={entry_price:.2f}, SL={stop_loss:.2f}, TP={take_profit:.2f}, R/R={risk_reward_ratio:.2f}"
                append_scan_log(session, log_msg)
            else:
                append_scan_log(session, f"{ticker}: Znaleziono sygnał, ale R/R ({risk_reward_ratio:.2f}) jest zbyt niskie.")

        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 3: {e}")
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_qualified)
            
    final_log = "Faza 3 zakończona. Zakończono generowanie planów taktycznych."
    logger.info(final_log)
    append_scan_log(session, final_log)

