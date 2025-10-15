import logging
import time
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float
from ..config import Phase3Config

logger = logging.getLogger(__name__)

def _find_impulse_and_fib_zone(daily_df: pd.DataFrame) -> dict | None:
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
            "entry_zone_top": high_point_price - 0.5 * (high_point_price - low_point_price),
            "entry_zone_bottom": high_point_price - 0.618 * (high_point_price - low_point_price)
        }
    except Exception as e:
        logger.error(f"Error in _find_impulse_and_fib_zone: {e}")
        return None

def _find_entry_signal_candle(intraday_df: pd.DataFrame) -> pd.Series | None:
    try:
        if intraday_df.empty: return None

        for _, candle in intraday_df.iterrows():
            is_bullish = candle['close'] > candle['open']
            is_in_upper_half = candle['close'] > (candle['high'] + candle['low']) / 2
            has_strong_body = (candle['close'] - candle['open']) > 0.3 * (candle['high'] - candle['low'])

            if is_bullish and is_in_upper_half and has_strong_body:
                return candle
        return None
    except Exception as e:
        logger.error(f"Error in _find_entry_signal_candle: {e}")
        return None

def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.split('. ')[-1] for col in df.columns]
    return df.apply(pd.to_numeric)

# Nowa funkcja dla analizy "Predator" na żądanie
def plan_trade_on_demand(ticker: str, api_client: AlphaVantageClient) -> dict:
    logger.info(f"[Predator] Running on-demand analysis for {ticker}")
    
    daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
    if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw:
        return {"signal": False, "reason": "Brak wystarczających danych historycznych (dziennych)."}

    daily_df = pd.DataFrame.from_dict(daily_data_raw['Time Series (Daily)'], orient='index')
    daily_df = standardize_df_columns(daily_df)
    daily_df.sort_index(inplace=True)

    impulse_result = _find_impulse_and_fib_zone(daily_df)
    if not impulse_result:
        return {"signal": False, "reason": "Nie znaleziono wyraźnego impulsu wzrostowego (>10%) w ciągu ostatnich 21 sesji."}

    current_price = daily_df['close'].iloc[-1]
    if not (impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"]):
        return {
            "signal": False, 
            "reason": f"Cena ({current_price:.2f}) jest poza optymalną strefą wejścia Fibonacciego ({impulse_result['entry_zone_bottom']:.2f} - {impulse_result['entry_zone_top']:.2f}). Obserwuj."
        }

    # Jeśli jest w strefie, szukamy świecy sygnałowej
    intraday_data_raw = api_client.get_intraday(ticker, interval='60min')
    if not intraday_data_raw or 'Time Series (60min)' not in intraday_data_raw:
         return {"signal": False, "reason": "Brak danych intraday (60min) do znalezienia sygnału wejścia."}

    intraday_df = pd.DataFrame.from_dict(intraday_data_raw['Time Series (60min)'], orient='index')
    intraday_df = standardize_df_columns(intraday_df)
    intraday_df.sort_index(inplace=True)

    signal_candle = _find_entry_signal_candle(intraday_df.iloc[-8:]) # Sprawdzamy ostatnie 8h
    if signal_candle is None:
        return {"signal": False, "reason": "Spółka w strefie wejścia, ale w ciągu ostatnich 8 godzin nie pojawiła się silna bycza świeca sygnałowa. Obserwuj."}

    entry_price = signal_candle['high'] + 0.01
    stop_loss = signal_candle['low'] - 0.01
    take_profit = impulse_result['impulse_high']
    
    potential_risk = entry_price - stop_loss
    potential_profit = take_profit - entry_price
    if potential_risk == 0:
        return {"signal": False, "reason": "Błąd kalkulacji: Ryzyko równe zero."}
    
    risk_reward_ratio = potential_profit / potential_risk

    if risk_reward_ratio < Phase3Config.MIN_RISK_REWARD_RATIO:
        return {
            "signal": False,
            "reason": f"Znaleziono sygnał wejścia, ale stosunek zysku do ryzyka ({risk_reward_ratio:.2f}) jest poniżej progu ({Phase3Config.MIN_RISK_REWARD_RATIO})."
        }

    return {
        "signal": True,
        "ticker": ticker,
        "entry_price": round(entry_price, 2),
        "stop_loss": round(stop_loss, 2),
        "take_profit": round(take_profit, 2),
        "risk_reward_ratio": round(risk_reward_ratio, 2),
        "notes": f"Sygnał wygenerowany na podstawie byczej świecy z {signal_candle.name} w strefie korekty impulsu."
    }


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
            trade_plan = plan_trade_on_demand(ticker, api_client)
            
            if trade_plan.get("signal"):
                stmt = text("""
                    INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, signal_candle_timestamp, notes)
                    VALUES (:ticker, :gen_date, 'ACTIVE', :entry, :sl, :tp, :rr, :candle_ts, :notes)
                """)
                candle_ts_str = trade_plan['notes'].split(' z ')[-1].split(' w ')[0]
                candle_ts = datetime.strptime(candle_ts_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

                session.execute(stmt, {
                    'ticker': ticker, 
                    'gen_date': datetime.now(timezone.utc), 
                    'entry': trade_plan['entry_price'], 
                    'sl': trade_plan['stop_loss'], 
                    'tp': trade_plan['take_profit'], 
                    'rr': trade_plan['risk_reward_ratio'], 
                    'candle_ts': candle_ts,
                    'notes': trade_plan['notes']
                })
                session.commit()
                log_msg = f"SYGNAŁ (F3): Wygenerowano dla {ticker}: Wejście={trade_plan['entry_price']:.2f}, SL={trade_plan['stop_loss']:.2f}, TP={trade_plan['take_profit']:.2f}"
                append_scan_log(session, log_msg)
            else:
                append_scan_log(session, f"INFO (F3): {ticker} - {trade_plan.get('reason')}")

        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 3: {e}", exc_info=True)
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_qualified)
            
    append_scan_log(session, "Faza 3 zakończona. Zakończono generowanie planów taktycznych.")

