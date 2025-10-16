import logging
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from pytz import timezone as pytz_timezone
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float, update_system_control
from ..config import Phase3Config

logger = logging.getLogger(__name__)

def _is_market_open():
    """Sprawdza, czy giełda w USA jest otwarta."""
    ny_time = datetime.now(pytz_timezone('America/New_York'))
    # Sprawdzamy, czy jest to dzień roboczy (poniedziałek=0, niedziela=6)
    if ny_time.weekday() >= 5:
        return False
    # Sprawdzamy godziny handlu
    return ny_time.time() >= datetime.strptime("09:30", "%H:%M").time() and ny_time.time() <= datetime.strptime("16:00", "%H:%M").time()


def _find_impulse_and_fib_zone(daily_df: pd.DataFrame) -> dict | None:
    try:
        if len(daily_df) < 21: return None
        recent_df = daily_df.iloc[-21:]
        low_point_price = recent_df['low'].min()
        low_point_date = recent_df['low'].idxmin()
        df_after_low = recent_df[recent_df.index > low_point_date]
        if df_after_low.empty: return None
        high_point_price = df_after_low['high'].max()

        if (high_point_price - low_point_price) / low_point_price < 0.10:
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

def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.split('. ')[-1] for col in df.columns]
    return df.apply(pd.to_numeric)

def plan_trade_on_demand(ticker: str, api_client: AlphaVantageClient) -> dict:
    logger.info(f"[Predator] Running analysis for {ticker}")
    daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
    if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw:
        return {"signal": False, "reason": "Brak danych dziennych."}

    daily_df = pd.DataFrame.from_dict(daily_data_raw['Time Series (Daily)'], orient='index')
    daily_df = standardize_df_columns(daily_df)
    daily_df.sort_index(inplace=True)
    impulse_result = _find_impulse_and_fib_zone(daily_df)
    if not impulse_result:
        return {"signal": False, "reason": "Brak impulsu >10%."}

    current_price = daily_df['close'].iloc[-1]
    is_in_zone = (impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"])
    
    # --- ZMIANA LOGIKI: Jeśli nie jest w strefie, od razu zwracamy sygnał PENDING do monitorowania strefy ---
    if not is_in_zone:
        return {
            "signal": True, "status": "PENDING", "ticker": ticker,
            "entry_zone_bottom": round(impulse_result['entry_zone_bottom'], 2),
            "entry_zone_top": round(impulse_result['entry_zone_top'], 2),
            "take_profit": round(impulse_result['impulse_high'], 2),
            "notes": f"Oczekiwanie na wejście ceny ({current_price:.2f}) do strefy."
        }

    # --- Jeśli jest w strefie, kontynuujemy analizę świecy i R/R ---
    intraday_data_raw = api_client.get_intraday(ticker, interval='60min')
    if not intraday_data_raw or 'Time Series (60min)' not in intraday_data_raw:
         return {"signal": False, "reason": "Brak danych 60min."}

    intraday_df = pd.DataFrame.from_dict(intraday_data_raw['Time Series (60min)'], orient='index')
    intraday_df = standardize_df_columns(intraday_df)
    intraday_df.sort_index(inplace=True)
    signal_candle = _find_entry_signal_candle(intraday_df)
    if signal_candle is None:
        return {"signal": False, "reason": "Cena w strefie, brak byczej świecy sygnałowej."}

    atr_data_raw = api_client.get_atr(ticker)
    if not atr_data_raw or 'Technical Analysis: ATR' not in atr_data_raw:
        return {"signal": False, "reason": "Brak danych ATR."}
    
    try:
        latest_atr_date = sorted(atr_data_raw['Technical Analysis: ATR'].keys())[-1]
        latest_atr = safe_float(atr_data_raw['Technical Analysis: ATR'][latest_atr_date]['ATR'])
        if not latest_atr: raise ValueError("ATR is null.")
    except (IndexError, KeyError, ValueError) as e:
        return {"signal": False, "reason": f"Błąd odczytu ATR: {e}"}

    entry_price = signal_candle['high'] + 0.01
    stop_loss = entry_price - (Phase3Config.ATR_MULTIPLIER_FOR_SL * latest_atr)
    take_profit = impulse_result['impulse_high']
    potential_risk = entry_price - stop_loss
    potential_profit = take_profit - entry_price
    
    if potential_risk <= 0: return {"signal": False, "reason": "Ryzyko (ATR) <= 0."}
    
    risk_reward_ratio = potential_profit / potential_risk

    # --- KLUCZOWA ZMIANA: Jeśli R/R jest za niskie, zwracamy sygnał PENDING do monitorowania R/R ---
    if risk_reward_ratio < Phase3Config.MIN_RISK_REWARD_RATIO:
        return {
            "signal": True, "status": "PENDING", "ticker": ticker,
            "entry_zone_bottom": round(impulse_result['entry_zone_bottom'], 2),
            "entry_zone_top": round(impulse_result['entry_zone_top'], 2),
            "take_profit": round(take_profit, 2),
            "notes": f"Oczekiwanie na poprawę R/R. Aktualne: {risk_reward_ratio:.2f} (próg: {Phase3Config.MIN_RISK_REWARD_RATIO})."
        }

    # --- Jeśli wszystko jest OK, zwracamy sygnał ACTIVE ---
    return {
        "signal": True, "status": "ACTIVE", "ticker": ticker,
        "entry_price": round(entry_price, 2), "stop_loss": round(stop_loss, 2),
        "take_profit": round(take_profit, 2), "risk_reward_ratio": round(risk_reward_ratio, 2),
        "notes": f"SYGNAŁ AKTYWNY. Bycza świeca z {signal_candle.name}. SL oparty na ATR({latest_atr:.2f})."
    }

def run_tactical_planning(session: Session, qualified_tickers: list[str], get_current_state, api_client: AlphaVantageClient):
    logger.info("Running Phase 3: Tactical Planning and Watchlist Generation...")
    append_scan_log(session, "Faza 3: Generowanie planów i listy obserwacyjnej...")
    total_qualified = len(qualified_tickers)
    update_scan_progress(session, 0, total_qualified)
    processed_count = 0

    for ticker in qualified_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            trade_plan = plan_trade_on_demand(ticker, api_client)
            if not trade_plan.get("signal"):
                append_scan_log(session, f"INFO (F3): {ticker} - {trade_plan.get('reason')}")
                continue

            status = trade_plan.get("status", "PENDING")
            stmt = text("""
                INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, notes, entry_zone_bottom, entry_zone_top)
                VALUES (:ticker, NOW(), :status, :entry, :sl, :tp, :rr, :notes, :ezb, :ezt)
                ON CONFLICT (ticker) DO UPDATE SET 
                    status = EXCLUDED.status, generation_date = EXCLUDED.generation_date, entry_price = EXCLUDED.entry_price,
                    stop_loss = EXCLUDED.stop_loss, take_profit = EXCLUDED.take_profit, risk_reward_ratio = EXCLUDED.risk_reward_ratio,
                    notes = EXCLUDED.notes, entry_zone_bottom = EXCLUDED.entry_zone_bottom, entry_zone_top = EXCLUDED.entry_zone_top;
            """)
            session.execute(stmt, {
                'ticker': ticker, 'status': status, 'notes': trade_plan.get('notes'),
                'entry': trade_plan.get('entry_price'), 'sl': trade_plan.get('stop_loss'),
                'tp': trade_plan.get('take_profit'), 'rr': trade_plan.get('risk_reward_ratio'),
                'ezb': trade_plan.get('entry_zone_bottom'), 'ezt': trade_plan.get('entry_zone_top')
            })
            session.commit()
            append_scan_log(session, f"{status} (F3): Dodano {ticker} do planu. Powód: {trade_plan.get('notes')}")

        except Exception as e:
            logger.error(f"Error in Phase 3 for {ticker}: {e}", exc_info=True)
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_qualified)
            
    append_scan_log(session, "Faza 3 zakończona.")

def monitor_pending_signals(session: Session, api_client: AlphaVantageClient):
    """
    Uruchamia ponowną, pełną analizę dla wszystkich sygnałów PENDING.
    Działa tylko w godzinach otwarcia giełdy.
    """
    if not _is_market_open():
        logger.info("Market is closed. Skipping pending signals monitor.")
        return
        
    logger.info("Market is open. Running pending signals monitor...")
    
    try:
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        delete_stmt = text("DELETE FROM trading_signals WHERE status = 'PENDING' AND generation_date < :seven_days_ago;")
        result = session.execute(delete_stmt, {'seven_days_ago': seven_days_ago})
        if result.rowcount > 0:
            update_system_control(session, 'system_alert', f"USUNIĘTO {result.rowcount} starych sygnałów PENDING (>7 dni).")
            logger.info(f"Deleted {result.rowcount} expired PENDING signals.")
            session.commit()
    except Exception as e:
        logger.error(f"Error deleting expired signals: {e}", exc_info=True)
        session.rollback()

    pending_tickers = [row[0] for row in session.execute(text("SELECT ticker FROM trading_signals WHERE status = 'PENDING'")).fetchall()]
    if not pending_tickers:
        logger.info("No PENDING signals to monitor.")
        return

    logger.info(f"Re-analyzing {len(pending_tickers)} PENDING tickers: {', '.join(pending_tickers)}")
        
    for ticker in pending_tickers:
        try:
            # Uruchom pełną analizę ponownie
            trade_plan = plan_trade_on_demand(ticker, api_client)
            
            # Jeśli nowa analiza zwraca sygnał ACTIVE, zaktualizuj bazę i wyślij alert
            if trade_plan.get('status') == 'ACTIVE':
                update_stmt = text("""
                    UPDATE trading_signals 
                    SET status = 'ACTIVE', entry_price = :entry, stop_loss = :sl, 
                        take_profit = :tp, risk_reward_ratio = :rr, 
                        notes = :notes, generation_date = NOW()
                    WHERE ticker = :ticker AND status = 'PENDING';
                """)
                session.execute(update_stmt, {
                    'ticker': ticker, 'notes': trade_plan['notes'],
                    'entry': trade_plan['entry_price'], 'sl': trade_plan['stop_loss'],
                    'tp': trade_plan['take_profit'], 'rr': trade_plan['risk_reward_ratio']
                })
                session.commit()

                alert_msg = f"SYGNAŁ AKTYWOWANY: {ticker} spełnił warunki! Wejście: ${trade_plan['entry_price']:.2f}, R/R: {trade_plan['risk_reward_ratio']:.2f}"
                update_system_control(session, 'system_alert', alert_msg)
                logger.info(alert_msg)
        
        except Exception as e:
            logger.error(f"Error monitoring PENDING signal for {ticker}: {e}", exc_info=True)
            session.rollback()

