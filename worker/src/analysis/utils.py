import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta 
import pytz
import pandas as pd
import numpy as np
from pandas import Series as pd_Series
from typing import Optional, Dict, Any, Tuple

import os
import requests
from urllib.parse import quote_plus
import hashlib 
import json

from .. import models
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CACHE_EXPIRY_DAYS = 7 

if not TELEGRAM_BOT_TOKEN:
    logger.warning("TELEGRAM_BOT_TOKEN not found. Telegram alerts are DISABLED.")
if not TELEGRAM_CHAT_ID:
    logger.warning("TELEGRAM_CHAT_ID not found. Telegram alerts are DISABLED.")

def get_raw_data_with_cache(
    session: Session, 
    api_client: AlphaVantageClient, 
    ticker: str, 
    data_type: str, 
    api_func: str, 
    expiry_hours: Optional[int] = None, 
    **kwargs
) -> Dict[str, Any]:
    try:
        cache_entry = session.query(models.AlphaVantageCache).filter(
            models.AlphaVantageCache.ticker == ticker,
            models.AlphaVantageCache.data_type == data_type
        ).first()
        now = datetime.now(timezone.utc)
        if cache_entry:
            if expiry_hours is not None:
                is_fresh = (now - cache_entry.last_fetched) < timedelta(hours=expiry_hours)
            else:
                is_fresh = (now - cache_entry.last_fetched) < timedelta(days=CACHE_EXPIRY_DAYS)
            if is_fresh and cache_entry.raw_data_json:
                return cache_entry.raw_data_json 
    except Exception: pass

    client_method = getattr(api_client, api_func, None)
    if not client_method: return {}
    if api_func == 'get_news_sentiment': kwargs['ticker'] = ticker
    elif api_func == 'get_bulk_quotes': kwargs['symbols'] = [ticker]
    else: kwargs['symbol'] = ticker
    try: raw_data = client_method(**kwargs)
    except TypeError: return {}
    
    if not raw_data or raw_data.get("Error Message") or raw_data.get("Information"): return {}

    try:
        upsert_stmt = text("""
            INSERT INTO alpha_vantage_cache (ticker, data_type, raw_data_json, last_fetched)
            VALUES (:ticker, :data_type, :raw_data, NOW())
            ON CONFLICT (ticker, data_type) DO UPDATE SET raw_data_json = :raw_data, last_fetched = NOW();
        """)
        session.execute(upsert_stmt, {'ticker': ticker, 'data_type': data_type, 'raw_data': json.dumps(raw_data)})
        session.commit()
    except Exception: session.rollback()
    return raw_data

_sent_alert_hashes = set()
def clear_alert_memory_cache():
    global _sent_alert_hashes
    _sent_alert_hashes = set()

def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    message_hash = hashlib.sha256(message.encode('utf-8')).hexdigest()
    if message_hash in _sent_alert_hashes: return
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&text={quote_plus(message)}", timeout=5)
        _sent_alert_hashes.add(message_hash)
    except Exception: pass

def get_market_status_and_time(api_client) -> dict:
    try:
        # Uproszczona wersja bez pytz (zależność opcjonalna) lub używamy timezone.utc
        now = datetime.now(timezone.utc)
        # ... (logika statusu)
        return {"status": "UNKNOWN", "time_ny": now.strftime('%H:%M UTC'), "date_ny": now.strftime('%Y-%m-%d')}
    except: return {"status": "UNKNOWN", "time_ny": "N/A", "date_ny": "N/A"}

def update_system_control(session: Session, key: str, value: str):
    try:
        session.execute(text("INSERT INTO system_control (key, value, updated_at) VALUES (:key, :value, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();"), [{'key': key, 'value': str(value)}])
        session.commit()
    except Exception: session.rollback()

def get_system_control_value(session: Session, key: str) -> str | None:
    try:
        res = session.execute(text("SELECT value FROM system_control WHERE key = :key"), {'key': key}).fetchone()
        return res[0] if res else None
    except: return None

def update_scan_progress(session: Session, processed: int, total: int):
    update_system_control(session, 'scan_progress_processed', str(processed))
    update_system_control(session, 'scan_progress_total', str(total))

def append_scan_log(session: Session, message: str):
    try:
        curr = get_system_control_value(session, 'scan_log') or ""
        new_log = (f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {message}\n" + curr)[:15000]
        update_system_control(session, 'scan_log', new_log)
    except: pass

def clear_scan_log(session: Session):
    update_system_control(session, 'scan_log', '')

# ZMIANA: Zwracamy typ komendy (str) zamiast boola
def check_for_commands(session: Session, current_state: str) -> tuple[str, str]:
    cmd = get_system_control_value(session, 'worker_command')
    if cmd == "START_REQUESTED":
        update_system_control(session, 'worker_command', 'NONE')
        return "FULL_RUN", current_state
    if cmd == "START_PHASE_1_REQUESTED":
        update_system_control(session, 'worker_command', 'NONE')
        return "PHASE_1_RUN", current_state
    if cmd == "START_PHASE_3_REQUESTED":
        update_system_control(session, 'worker_command', 'NONE')
        return "PHASE_3_RUN", current_state
    if cmd == "PAUSE_REQUESTED":
        update_system_control(session, 'worker_status', 'PAUSED')
        update_system_control(session, 'worker_command', 'NONE')
        return "NONE", "PAUSED"
    if cmd == "RESUME_REQUESTED":
        update_system_control(session, 'worker_status', 'RUNNING')
        update_system_control(session, 'worker_command', 'NONE')
        return "NONE", "RUNNING"
    return "NONE", current_state

def report_heartbeat(session: Session):
    update_system_control(session, 'last_heartbeat', datetime.now(timezone.utc).isoformat())

def safe_float(value) -> float | None:
    if value is None: return None
    try: return float(str(value).replace(',', '').replace('%', ''))
    except: return None

def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    mapping = {'1. open':'open', '2. high':'high', '3. low':'low', '4. close':'close', '5. vwap':'vwap', '5. volume':'volume', '6. volume':'volume', '7. adjusted close':'adjusted close'}
    df.rename(columns=lambda c: mapping.get(c, c.split('. ')[-1]), inplace=True)
    for c in ['open','high','low','close','volume','adjusted close','vwap']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce')
    return df.sort_index()

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd_Series:
    if df.empty or len(df) < period: return pd.Series(dtype=float)
    hl = df['high'] - df['low']
    hc = (df['high'] - df['close'].shift()).abs()
    lc = (df['low'] - df['close'].shift()).abs()
    return pd.concat([hl, hc, lc], axis=1).max(axis=1).ewm(span=period, adjust=False).mean()

def calculate_ema(series: pd_Series, period: int) -> pd_Series:
    return series.ewm(span=period, adjust=False).mean()

def _safe_float_convert(value: Any) -> float | None:
    if value is None: return None
    try: return float(value)
    except: return None

def _resolve_trade(historical_data: pd.DataFrame, entry_index: int, setup: Dict[str, Any], max_hold_days: int, year: str, direction: str) -> models.VirtualTrade | None:
    try:
        entry_price = setup['entry_price']
        stop_loss = setup['stop_loss']
        take_profit = setup['take_profit']
        close_price = entry_price
        status = 'CLOSED_EXPIRED'
        for i in range(0, max_hold_days): 
            curr_idx = entry_index + i
            if curr_idx >= len(historical_data):
                close_price = historical_data.iloc[-1]['close']
                break
            candle = historical_data.iloc[curr_idx]
            if direction == 'LONG':
                if candle['low'] <= stop_loss:
                    close_price = stop_loss
                    status = 'CLOSED_SL'
                    break
                if candle['high'] >= take_profit:
                    close_price = take_profit
                    status = 'CLOSED_TP'
                    break
        else:
            final_idx = min(entry_index + max_hold_days - 1, len(historical_data) - 1)
            close_price = historical_data.iloc[final_idx]['close']
            status = 'CLOSED_EXPIRED'
        p_l_percent = 0.0 if entry_price == 0 else ((close_price - entry_price) / entry_price) * 100
        return models.VirtualTrade(
            ticker=setup['ticker'], status=status, setup_type=f"BACKTEST_{year}_{setup['setup_type']}",
            entry_price=float(entry_price), stop_loss=float(stop_loss), take_profit=float(take_profit),
            open_date=historical_data.index[entry_index].to_pydatetime(),
            close_date=historical_data.iloc[min(entry_index + max_hold_days - 1, len(historical_data) - 1)].name.to_pydatetime(),
            close_price=float(close_price), final_profit_loss_percent=float(p_l_percent),
            metric_atr_14=_safe_float_convert(setup.get('metric_atr_14')),
            metric_time_dilation=_safe_float_convert(setup.get('metric_time_dilation')),
            metric_price_gravity=_safe_float_convert(setup.get('metric_price_gravity')),
            metric_td_percentile_90=_safe_float_convert(setup.get('metric_td_percentile_90')),
            metric_pg_percentile_90=_safe_float_convert(setup.get('metric_pg_percentile_90')),
            metric_inst_sync=_safe_float_convert(setup.get('metric_inst_sync')),
            metric_retail_herding=_safe_float_convert(setup.get('metric_retail_herding')),
            metric_aqm_score_h3=_safe_float_convert(setup.get('metric_aqm_score_h3')),
            metric_aqm_percentile_95=_safe_float_convert(setup.get('metric_aqm_percentile_95')),
            metric_J_norm=_safe_float_convert(setup.get('metric_J_norm')),
            metric_nabla_sq_norm=_safe_float_convert(setup.get('metric_nabla_sq_norm')),
            metric_m_sq_norm=_safe_float_convert(setup.get('metric_m_sq_norm')),
            metric_J=_safe_float_convert(setup.get('metric_J')),
            metric_J_threshold_2sigma=_safe_float_convert(setup.get('metric_J_threshold_2sigma'))
        )
    except Exception as e:
        logger.error(f"[Backtest Utils] Błąd transakcji: {e}")
        return None
