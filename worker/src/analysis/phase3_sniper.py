import logging
import time
import json
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float
from ..config import Phase3Config

logger = logging.getLogger(__name__)

def _find_atr_and_impulse(daily_df: pd.DataFrame) -> dict | None:
    try:
        if len(daily_df) < 21: return None

        latest_atr = daily_df['atr'].iloc[-1]
        
        # Znajdź dołek w ostatnich 21 sesjach
        recent_period = daily_df.iloc[-21:]
        low_point_price = recent_period['low'].min()
        low_point_date = recent_period['low'].idxmin()

        # Znajdź szczyt po dołku
        high_after_low = recent_period[recent_period.index > low_point_date]
        if high_after_low.empty: return None
        
        high_point_price = high_after_low['high'].max()
        
        # Sprawdź, czy ruch to impuls > 1.0 * ATR
        impulse_value = high_point_price - low_point_price
        if impulse_value < (1.0 * latest_atr):
            return {
                "status": "NO_IMPULSE",
                "message": f"Brak wystarczającego impulsu. Ruch {impulse_value:.2f} < wymagany {latest_atr:.2f} (1.0*ATR)."
            }

        return {
            "status": "IMPULSE_FOUND",
            "impulse_high": high_point_price,
            "impulse_low": low_point_price,
            "latest_atr": latest_atr,
            "current_price": daily_df['close'].iloc[-1]
        }
    except Exception as e:
        logger.error(f"Error in _find_atr_and_impulse: {e}", exc_info=True)
        return None

def _find_entry_signal_candle(intraday_data: dict) -> dict | None:
    try:
        time_series_key = next((key for key in intraday_data if 'Time Series' in key), None)
        if not time_series_key: return None
        time_series = intraday_data[time_series_key]

        cleaned_data = {}
        for dt, values in time_series.items():
            cleaned_values = {key.split(' ')[1]: float(val) for key, val in values.items()}
            cleaned_data[pd.to_datetime(dt)] = cleaned_values
        
        intraday_df = pd.DataFrame.from_dict(cleaned_data, orient='index').sort_index(ascending=False)

        for index, row in intraday_df.iterrows():
            o, h, l, c = row['open'], row['high'], row['low'], row['close']
            is_bullish = c > o
            is_in_upper_half = c > (h + l) / 2
            has_strong_body = (c - o) > 0.3 * (h - l)

            if is_bullish and is_in_upper_half and has_strong_body:
                return {"datetime": index.strftime('%Y-%m-%d %H:%M:%S'), "high": h, "low": l}
        return None
    except Exception as e:
        logger.error(f"Error in _find_entry_signal_candle: {e}", exc_info=True)
        return None

def run_phase3_on_demand(session: Session, ticker: str, api_client: AlphaVantageClient):
    """Wykonuje analizę Fazy 3 na żądanie dla pojedynczej spółki."""
    logger.info(f"Running Phase 3 On-Demand for {ticker}...")
    
    result_payload = {"status": "ERROR", "message": "Wystąpił nieoczekiwany błąd."}
    try:
        daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
        atr_data_raw = api_client.get_atr(ticker)

        if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw or \
           not atr_data_raw or 'Technical Analysis: ATR' not in atr_data_raw:
            raise Exception("Brak kompletnych danych dziennych lub ATR do analizy.")
        
        # Tworzenie DataFrame i łączenie danych
        daily_series = daily_data_raw['Time Series (Daily)']
        daily_cleaned = {pd.to_datetime(d): {k.split(' ')[1]: float(v) for k,v in vals.items()} for d,vals in daily_series.items()}
        daily_df = pd.DataFrame.from_dict(daily_cleaned, orient='index').sort_index()

        atr_series = atr_data_raw['Technical Analysis: ATR']
        atr_cleaned = {pd.to_datetime(d): float(vals['ATR']) for d, vals in atr_series.items()}
        atr_df = pd.DataFrame.from_dict(atr_cleaned, orient='index', columns=['atr']).sort_index()
        
        combined_df = daily_df.join(atr_df, how='inner')

        impulse_result = _find_atr_and_impulse(combined_df)

        if not impulse_result or impulse_result["status"] == "NO_IMPULSE":
            result_payload = impulse_result or {"status": "NO_IMPULSE", "message": "Nie zidentyfikowano formacji impulsowej."}
        else:
            intraday_data = api_client.get_intraday(ticker)
            signal_candle = _find_entry_signal_candle(intraday_data) if intraday_data else None

            if not signal_candle:
                result_payload = {"status": "NO_SIGNAL_CANDLE", "message": "Znaleziono impuls, ale brak świecy sygnałowej w danych intraday. Obserwuj."}
            else:
                entry_price = signal_candle['high'] + 0.01
                stop_loss = entry_price - (Phase3Config.ATR_STOP_LOSS_MULTIPLIER * impulse_result['latest_atr'])
                take_profit = entry_price + (Phase3Config.ATR_STOP_LOSS_MULTIPLIER * impulse_result['latest_atr'] * Phase3Config.MIN_RISK_REWARD_RATIO)
                
                result_payload = {
                    "status": "SIGNAL_GENERATED",
                    "ticker": ticker,
                    "entry_price": round(entry_price, 2),
                    "stop_loss": round(stop_loss, 2),
                    "take_profit": round(take_profit, 2),
                    "risk_reward_ratio": Phase3Config.MIN_RISK_REWARD_RATIO,
                    "signal_candle_timestamp": signal_candle['datetime']
                }
    except Exception as e:
        logger.error(f"Error in on-demand Phase 3 for {ticker}: {e}", exc_info=True)
        result_payload = {"status": "ERROR", "message": str(e)}
    finally:
        # Zapisz wynik do bazy danych
        stmt = text("""
            INSERT INTO phase3_on_demand_results (ticker, analysis_data, last_updated)
            VALUES (:ticker, :data, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
            analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
        """)
        session.execute(stmt, {'ticker': ticker, 'data': json.dumps(result_payload)})
        session.commit()
        # Zresetuj żądanie
        from .utils import update_system_control # Uniknięcie cyklicznego importu
        update_system_control(session, 'phase3_on_demand_request', 'NONE')
        logger.info(f"Saved on-demand Phase 3 result for {ticker}.")


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
            daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
            atr_data_raw = api_client.get_atr(ticker)
            if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw or \
               not atr_data_raw or 'Technical Analysis: ATR' not in atr_data_raw:
                append_scan_log(session, f"{ticker}: Brak kompletnych danych do analizy Fazy 3.")
                continue
            
            daily_series = daily_data_raw['Time Series (Daily)']
            daily_cleaned = {pd.to_datetime(d): {k.split(' ')[1]: float(v) for k,v in vals.items()} for d,vals in daily_series.items()}
            daily_df = pd.DataFrame.from_dict(daily_cleaned, orient='index').sort_index()

            atr_series = atr_data_raw['Technical Analysis: ATR']
            atr_cleaned = {pd.to_datetime(d): float(vals['ATR']) for d, vals in atr_series.items()}
            atr_df = pd.DataFrame.from_dict(atr_cleaned, orient='index', columns=['atr']).sort_index()
            
            combined_df = daily_df.join(atr_df, how='inner')

            impulse_result = _find_atr_and_impulse(combined_df)
            if not impulse_result or impulse_result["status"] != "IMPULSE_FOUND":
                log_msg = f"{ticker}: " + (impulse_result["message"] if impulse_result else "Brak impulsu.")
                append_scan_log(session, log_msg)
                continue
            
            intraday_data = api_client.get_intraday(ticker)
            signal_candle = _find_entry_signal_candle(intraday_data) if intraday_data else None
            
            if not signal_candle:
                append_scan_log(session, f"{ticker}: W strefie wejścia, ale brak świecy sygnałowej. Obserwuj.")
                continue

            entry_price = signal_candle['high'] + 0.01
            stop_loss = entry_price - (Phase3Config.ATR_STOP_LOSS_MULTIPLIER * impulse_result['latest_atr'])
            take_profit = entry_price + (Phase3Config.ATR_STOP_LOSS_MULTIPLIER * impulse_result['latest_atr'] * Phase3Config.MIN_RISK_REWARD_RATIO)

            stmt = text("""
                INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, signal_candle_timestamp)
                VALUES (:ticker, :gen_date, 'ACTIVE', :entry, :sl, :tp, :rr, :candle_ts)
            """)
            candle_ts = datetime.strptime(signal_candle['datetime'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            session.execute(stmt, {'ticker': ticker, 'gen_date': datetime.now(timezone.utc), 'entry': entry_price, 'sl': stop_loss, 
                                   'tp': take_profit, 'rr': Phase3Config.MIN_RISK_REWARD_RATIO, 'candle_ts': candle_ts})
            session.commit()
            log_msg = f"SYGNAŁ WYGENEROWANY dla {ticker}: Wejście={entry_price:.2f}, SL={stop_loss:.2f}, TP={take_profit:.2f}, R/R={Phase3Config.MIN_RISK_REWARD_RATIO:.2f}"
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 3: {e}", exc_info=True)
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_qualified)
            
    final_log = "Faza 3 zakończona. Zakończono generowanie planów taktycznych."
    logger.info(final_log)
    append_scan_log(session, final_log)

