import logging
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float, update_system_control, get_system_control_value
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
    """Funkcja generująca plan handlowy dla Fazy 3 (Predator) na żądanie."""
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
    
    # 1. Sprawdzenie, czy cena JEST w strefie
    is_in_zone = (impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"])

    # 2. Jeśli jest w strefie, szukamy świecy sygnałowej
    if is_in_zone:
        intraday_data_raw = api_client.get_intraday(ticker, interval='60min')
        if not intraday_data_raw or 'Time Series (60min)' not in intraday_data_raw:
             return {"signal": False, "reason": "Cena w strefie, ale brak danych intraday (60min) do znalezienia sygnału wejścia."}

        intraday_df = pd.DataFrame.from_dict(intraday_data_raw['Time Series (60min)'], orient='index')
        intraday_df = standardize_df_columns(intraday_df)
        intraday_df.sort_index(inplace=True)

        signal_candle = _find_entry_signal_candle(intraday_df)
        if signal_candle is None:
            return {"signal": False, "reason": "Cena w strefie, ale nie pojawiła się silna bycza świeca sygnałowa (ostatnie 8h). Obserwuj."}

        # Kalkulacja R/R
        entry_price = signal_candle['high'] + 0.01
        # Używamy impulselow jako potencjalnego SL dla pełnego planu (mimo że ATR jest lepsze, trzymamy się logiki z Faz 1/3)
        stop_loss = impulse_result['impulse_low'] 
        take_profit = impulse_result['impulse_high']
        
        potential_risk = entry_price - stop_loss
        potential_profit = take_profit - entry_price
        
        if potential_risk <= 0:
            return {"signal": False, "reason": "Błąd kalkulacji: Ryzyko równe zero lub negatywne."}
        
        risk_reward_ratio = potential_profit / potential_risk

        if risk_reward_ratio < Phase3Config.MIN_RISK_REWARD_RATIO:
            return {
                "signal": False,
                "reason": f"Sygnał wejścia, ale R/R ({risk_reward_ratio:.2f}) jest poniżej progu ({Phase3Config.MIN_RISK_REWARD_RATIO})."
            }

        return {
            "signal": True,
            "ticker": ticker,
            "entry_price": round(entry_price, 2),
            "stop_loss": round(stop_loss, 2),
            "take_profit": round(take_profit, 2),
            "risk_reward_ratio": round(risk_reward_ratio, 2),
            "status": "ACTIVE", # Bezpośredni sygnał do wejścia
            "notes": f"SYGNAŁ AKTYWNY. Bycza świeca z {signal_candle.name} w strefie korekty."
        }
    
    # 3. Jeśli cena JEST poza strefą (monitoring) - zwracamy status PENDING
    entry_reason = f"Cena ({current_price:.2f}) jest poza optymalną strefą wejścia Fibonacciego ({impulse_result['entry_zone_bottom']:.2f} - {impulse_result['entry_zone_top']:.2f})."
    
    return {
        "signal": True,
        "ticker": ticker,
        "entry_zone_bottom": round(impulse_result['entry_zone_bottom'], 2),
        "entry_zone_top": round(impulse_result['entry_zone_top'], 2),
        "current_price": round(current_price, 2),
        "take_profit": round(impulse_result['impulse_high'], 2),
        "status": "PENDING", # Oczekujący na wejście do strefy
        "notes": entry_reason + " Monitorowanie aktywne."
    }


def run_tactical_planning(session: Session, qualified_tickers: list[str], get_current_state, api_client: AlphaVantageClient):
    """Generuje plany taktyczne dla wszystkich zakwalifikowanych tickerów."""
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
                # Używamy statusu z planu (ACTIVE lub PENDING)
                status = trade_plan.get("status", "PENDING")
                notes = trade_plan.get("notes")
                
                # Używamy UPSERT, aby zawsze aktualizować sygnał, jeśli już istnieje
                stmt = text("""
                    INSERT INTO trading_signals (ticker, generation_date, status, entry_price, stop_loss, take_profit, risk_reward_ratio, notes, entry_zone_bottom, entry_zone_top)
                    VALUES (:ticker, NOW(), :status, :entry, :sl, :tp, :rr, :notes, :ezb, :ezt)
                    ON CONFLICT (ticker) DO UPDATE 
                    SET status = EXCLUDED.status, generation_date = EXCLUDED.generation_date, entry_price = EXCLUDED.entry_price,
                        stop_loss = EXCLUDED.stop_loss, take_profit = EXCLUDED.take_profit, risk_reward_ratio = EXCLUDED.risk_reward_ratio,
                        notes = EXCLUDED.notes, entry_zone_bottom = EXCLUDED.entry_zone_bottom, entry_zone_top = EXCLUDED.entry_zone_top;
                """)
                
                # Dostosowanie danych do wstawienia w zależności od statusu
                entry_price = trade_plan.get('entry_price')
                stop_loss = trade_plan.get('stop_loss')
                risk_reward_ratio = trade_plan.get('risk_reward_ratio')
                
                session.execute(stmt, {
                    'ticker': ticker, 
                    'status': status,
                    'entry': entry_price, 
                    'sl': stop_loss, 
                    'tp': trade_plan.get('take_profit'), 
                    'rr': risk_reward_ratio, 
                    'notes': notes,
                    'ezb': trade_plan.get('entry_zone_bottom'),
                    'ezt': trade_plan.get('entry_zone_top')
                })
                session.commit()
                
                log_msg = f"{status} (F3): Wygenerowano plan dla {ticker}. "
                if status == 'ACTIVE':
                    log_msg += f"Wejście={entry_price:.2f}, SL={stop_loss:.2f}, TP={trade_plan['take_profit']:.2f}"
                else:
                    log_msg += f"Monitorowanie strefy: {trade_plan['entry_zone_bottom']:.2f} - {trade_plan['entry_zone_top']:.2f}"
                    
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


def monitor_pending_signals(session: Session, api_client: AlphaVantageClient):
    """
    Sprawdza sygnały ze statusem PENDING, czy weszły w strefę wejścia (ACTIVE),
    oraz usuwa te, które są PENDING dłużej niż 7 dni.
    """
    logger.info("Running pending signals monitor...")
    
    # 1. Usuwanie przeterminowanych sygnałów (starszych niż 7 dni)
    try:
        seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
        delete_stmt = text("""
            DELETE FROM trading_signals 
            WHERE status = 'PENDING' AND generation_date < :seven_days_ago;
        """)
        result = session.execute(delete_stmt, {'seven_days_ago': seven_days_ago})
        if result.rowcount > 0:
            update_system_control(session, 'system_alert', f"USUNIĘTO {result.rowcount} starych sygnałów PENDING (>7 dni).")
            logger.info(f"Deleted {result.rowcount} expired PENDING signals.")
            session.commit()
    except Exception as e:
        logger.error(f"Error deleting expired signals: {e}", exc_info=True)
        session.rollback()

    # 2. Sprawdzanie aktywności sygnałów PENDING
    pending_signals = session.execute(text("SELECT ticker, entry_zone_bottom, entry_zone_top FROM trading_signals WHERE status = 'PENDING'")).fetchall()
    
    if not pending_signals:
        logger.info("No PENDING signals to monitor.")
        return

    tickers_to_check = [s[0] for s in pending_signals]
    
    # Pobieranie cen na żywo dla monitorowanych tickerów
    try:
        live_prices = api_client.get_bulk_quotes(tickers_to_check)
        if not live_prices:
            logger.warning("Could not retrieve live prices for PENDING monitor.")
            return
            
        # Użycie funkcji pomocniczej do parsowania CSV (w Phase 1)
        from .phase1_scanner import _parse_bulk_quotes_csv
        parsed_data = _parse_bulk_quotes_csv(live_prices)
    except Exception as e:
        logger.error(f"Error fetching/parsing live prices for monitor: {e}", exc_info=True)
        return
        
    for ticker, ezb, ezt in pending_signals:
        try:
            price_data = parsed_data.get(ticker)
            current_price = safe_float(price_data.get('price')) if price_data else None
            
            if current_price and ezb and ezt:
                # Weryfikacja wejścia w strefę (current_price JEST w strefie)
                if float(ezb) <= current_price <= float(ezt):
                    # Zmiana statusu na ACTIVE i generowanie alertu
                    
                    # Logika: Ponawiamy pełną analizę PREDATOR na żądanie (Faza 3), 
                    # aby wygenerować dokładne SL/TP oparte na świecy sygnałowej
                    trade_plan = plan_trade_on_demand(ticker, api_client)
                    
                    if trade_plan.get('status') == 'ACTIVE':
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
                            'notes': trade_plan['notes'] + " (AKTYWOWANY PRZEZ MONITORING)"
                        })
                        session.commit()

                        alert_msg = f"SYGNAŁ AKTYWNY: {ticker} wszedł w strefę! Generowany plan: Wejście={trade_plan['entry_price']:.2f}, R/R={trade_plan['risk_reward_ratio']:.2f}"
                        # Używamy system_alert do wyświetlenia alertu na frontendzie
                        update_system_control(session, 'system_alert', alert_msg)
                        logger.info(alert_msg)
                    else:
                        # Może być w strefie, ale nie ma świecy sygnałowej. Zostawiamy PENDING
                        logger.info(f"TICKER {ticker} w strefie, ale nie znaleziono świecy sygnałowej. Pozostaje PENDING.")
        
        except Exception as e:
            logger.error(f"Error monitoring signal for {ticker}: {e}", exc_info=True)
            session.rollback()


# Nowa kolumna musi być dodana w models.py (w Twoim projekcie API)
# Przekazano te kolumny w wcześniejszym pliku:
# entry_zone_bottom = Column(NUMERIC(12, 2))
# entry_zone_top = Column(NUMERIC(12, 2))
