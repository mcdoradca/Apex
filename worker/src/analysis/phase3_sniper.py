import logging
import time
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float
from ..config import Phase3Config

logger = logging.getLogger(__name__)

# --- ZAAWANSOWANE FUNKCJE ANALITYCZNE DLA FAZY 3 ---

def _find_significant_impulse(daily_df: pd.DataFrame, latest_atr: float) -> tuple[float, float] | None:
    """Identyfikuje ostatni znaczący impuls cenowy (> 1.0 * ATR) w ciągu ostatnich 21 sesji."""
    try:
        recent_data = daily_df.iloc[-21:]
        low_point_price = recent_data['low'].min()
        low_point_date = recent_data['low'].idxmin()

        data_after_low = recent_data[recent_data.index > low_point_date]
        if data_after_low.empty:
            return None

        high_point_price = data_after_low['high'].max()
        
        # Warunek: Wyzwalacz dostosowany do zmienności
        if (high_point_price - low_point_price) > (1.0 * latest_atr):
            return high_point_price, low_point_price # impulse_high, impulse_low
        return None
    except Exception as e:
        logger.error(f"Error in _find_significant_impulse: {e}", exc_info=True)
        return None

def run_tactical_planning(session: Session, qualified_tickers: list[str], get_current_state, api_client: AlphaVantageClient):
    """Przeprowadza pełną analizę Fazy 3 dla zakwalifikowanych kandydatów."""
    logger.info("Running Phase 3: Sniper Agent Tactical Planning (v2.0)...")
    append_scan_log(session, "Faza 3 (v2.0): Generowanie planów taktycznych...")
    
    total_qualified = len(qualified_tickers)
    update_scan_progress(session, 0, total_qualified)
    processed_count = 0

    # Wyczyść stare aktywne sygnały przed nową analizą
    try:
        session.query(models.TradingSignal).filter(models.TradingSignal.status == 'ACTIVE').delete()
        session.commit()
    except Exception as e:
        logger.error(f"Could not clear old active signals: {e}")
        session.rollback()

    for ticker in qualified_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        analysis_result = _generate_signal_for_ticker(ticker, api_client)
        
        if analysis_result.get("signal_generated"):
            signal_data = analysis_result["data"]
            stmt = text("""
                INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, signal_details)
                VALUES (:ticker, :gen_date, 'ACTIVE', :entry, :sl, :tp, :rr, :details)
            """)
            session.execute(stmt, {
                'ticker': ticker, 
                'gen_date': datetime.now(timezone.utc), 
                'entry': signal_data['entry_price'], 
                'sl': signal_data['stop_loss'], 
                'tp': signal_data['take_profit'], 
                'rr': signal_data['risk_reward_ratio'],
                'details': analysis_result['log']
            })
            session.commit()
        
        append_scan_log(session, analysis_result["log"])

        processed_count += 1
        update_scan_progress(session, processed_count, total_qualified)
            
    final_log = "Faza 3 zakończona. Zakończono generowanie planów taktycznych."
    logger.info(final_log)
    append_scan_log(session, final_log)


def run_phase3_on_demand(ticker: str, api_client: AlphaVantageClient) -> dict:
    """Uruchamia analizę Fazy 3 na żądanie dla pojedynczego tickera."""
    logger.info(f"Running Phase 3 On-Demand for ticker: {ticker}")
    return _generate_signal_for_ticker(ticker, api_client)


def _generate_signal_for_ticker(ticker: str, api_client: AlphaVantageClient) -> dict:
    """Centralna funkcja generująca sygnał dla pojedynczego tickera (zarówno dla skanu, jak i na żądanie)."""
    try:
        # Pobierz dane dzienne i ATR
        daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
        atr_data_raw = api_client.get_atr(ticker, time_period=14)

        if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw or not atr_data_raw or 'Technical Analysis: ATR' not in atr_data_raw:
            return {"signal_generated": False, "log": f"{ticker}: Pomięty (F3) - brak danych dziennych lub ATR."}

        daily_df = pd.DataFrame.from_dict(daily_data_raw['Time Series (Daily)'], orient='index').astype(float)
        daily_df.index = pd.to_datetime(daily_df.index)
        daily_df = daily_df.sort_index()
        
        latest_atr = list(atr_data_raw['Technical Analysis: ATR'].values())[0]['ATR']
        latest_atr = safe_float(latest_atr)
        if not latest_atr:
             return {"signal_generated": False, "log": f"{ticker}: Pomięty (F3) - nie można odczytać ATR."}

        # 1. Znajdź znaczący impuls
        impulse_result = _find_significant_impulse(daily_df, latest_atr)
        if not impulse_result:
            return {"signal_generated": False, "log": f"{ticker}: Brak znaczącego impulsu (>1.0 ATR) do analizy taktycznej."}
        
        impulse_high, impulse_low = impulse_result
        
        # 2. Zdefiniuj logikę wejścia i ryzyka dynamicznie na podstawie ATR
        entry_price = daily_df['close'].iloc[-1]
        stop_loss = entry_price - (Phase3Config.ATR_MULTIPLIER_FOR_SL * latest_atr)
        
        # Cel zysku ustawiony na szczyt impulsu
        take_profit = impulse_high
        
        if stop_loss >= entry_price:
            return {"signal_generated": False, "log": f"{ticker}: Błąd kalkulacji SL (F3). SL > cena wejścia."}
            
        potential_risk = entry_price - stop_loss
        potential_profit = take_profit - entry_price

        if potential_risk <= 0 or potential_profit <= 0:
            return {"signal_generated": False, "log": f"{ticker}: Brak potencjału zysku lub nieprawidłowe ryzyko."}
        
        risk_reward_ratio = potential_profit / potential_risk

        # 3. Sprawdź, czy sygnał spełnia kryteria
        if risk_reward_ratio >= Phase3Config.MIN_RISK_REWARD_RATIO:
            log_message = f"SYGNAŁ (F3) dla {ticker}: Wejście={entry_price:.2f}, SL={stop_loss:.2f}, TP={take_profit:.2f}, R/R={risk_reward_ratio:.2f}"
            return {
                "signal_generated": True,
                "log": log_message,
                "data": {
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "risk_reward_ratio": risk_reward_ratio
                }
            }
        else:
            log_message = f"{ticker}: Znaleziono setup, ale R/R ({risk_reward_ratio:.2f}) jest zbyt niskie (poniżej {Phase3Config.MIN_RISK_REWARD_RATIO})."
            return {"signal_generated": False, "log": log_message}

    except Exception as e:
        error_message = f"Krytyczny błąd podczas analizy Fazy 3 dla {ticker}: {e}"
        logger.error(error_message, exc_info=True)
        return {"signal_generated": False, "log": error_message}
