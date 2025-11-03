import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
import pytz

logger = logging.getLogger(__name__)

# === NOWE FUNKCJE KROK 1: Lokalna analityka (oszczędzanie API) ===

def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standaryzuje nazwy kolumn i konwertuje na typy numeryczne."""
    if df.empty:
        return df
    
    # Sprawdź, czy kolumny już są w formacie '1. open'
    if any(col.endswith('. open') for col in df.columns):
        try:
             df.columns = [col.split('. ')[-1] for col in df.columns]
        except Exception as e:
            logger.error(f"Error standardizing columns: {e}. Columns: {df.columns}")
    
    # Kopiujemy, aby uniknąć SettingWithCopyWarning
    df_copy = df.copy()
    
    # Konwertuj kluczowe kolumny na numeryczne
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
            
    df_copy.sort_index(inplace=True)
    return df_copy

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Oblicza Average True Range (ATR) lokalnie."""
    if df.empty or len(df) < period:
        return pd.Series(dtype=float)
        
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    # Używamy ewm (Exponential Weighted Moving Average) do obliczenia ATR
    atr = tr.ewm(span=period, adjust=False, min_periods=period).mean()
    return atr

def calculate_rsi(df: pd.DataFrame, period: int = 9) -> pd.Series:
    """Oblicza Relative Strength Index (RSI) lokalnie."""
    if df.empty or len(df) < period + 1:
        return pd.Series(dtype=float)

    delta = df['close'].diff(1)
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Używamy ewm do wygładzenia
    avg_gain = gain.ewm(span=period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(span=period, adjust=False, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_bbands(df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> (pd.Series, pd.Series, pd.Series):
    """Oblicza Wstęgi Bollingera (BBands) lokalnie."""
    if df.empty or len(df) < period:
        return pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)

    middle_band = df['close'].rolling(window=period).mean()
    rolling_std = df['close'].rolling(window=period).std()
    
    upper_band = middle_band + (rolling_std * std_dev)
    lower_band = middle_band - (rolling_std * std_dev)
    
    return upper_band, middle_band, lower_band

# === ISTNIEJĄCE FUNKCJE (bez zmian) ===

def get_market_status_and_time(api_client) -> dict:
    """
    Sprawdza status giełdy NASDAQ używając dedykowanego endpointu API
    i zwraca czas w Nowym Jorku.
    """
    # 1. Zawsze pobieraj aktualny czas dla celów wyświetlania
    try:
        tz = pytz.timezone('US/Eastern')
        now_ny = datetime.now(tz)
        time_ny_str = now_ny.strftime('%H:%M:%S ET')
        date_ny_str = now_ny.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error getting New York time: {e}")
        time_ny_str = "N/A"
        date_ny_str = "N/A"

    # 2. Pobierz oficjalny status rynku z API
    try:
        status_data = api_client.get_market_status()
        if status_data and status_data.get('markets'):
            us_market = next((m for m in status_data['markets'] if m.get('region') == 'United States'), None)
            if us_market:
                api_status = us_market.get('current_status', 'unknown').lower()
                
                # 3. Przetłumacz status API na wewnętrzny status aplikacji
                status_map = {
                    "open": "MARKET_OPEN",
                    "closed": "MARKET_CLOSED",
                    "pre-market": "PRE_MARKET",
                    "post-market": "AFTER_MARKET"
                }
                app_status = status_map.get(api_status, "UNKNOWN")
                
                return {"status": app_status, "time_ny": time_ny_str, "date_ny": date_ny_str}
        
        logger.warning("Could not determine market status from API response.")
        return {"status": "UNKNOWN", "time_ny": time_ny_str, "date_ny": date_ny_str}

    except Exception as e:
        logger.error(f"Error getting market status from API: {e}")
        return {"status": "UNKNOWN", "time_ny": time_ny_str, "date_ny": date_ny_str}

def update_system_control(session: Session, key: str, value: str):
    """Aktualizuje lub wstawia wartość w tabeli system_control (UPSERT)."""
    try:
        stmt = text("""
            INSERT INTO system_control (key, value, updated_at)
            VALUES (:key, :value, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = NOW();
        """)
        session.execute(stmt, [{'key': key, 'value': str(value)}])
        session.commit()
    except Exception as e:
        logger.error(f"Error updating system_control for key {key}: {e}")
        session.rollback()

def get_system_control_value(session: Session, key: str) -> str | None:
    """Odczytuje pojedynczą wartość z tabeli system_control."""
    try:
        result = session.execute(text("SELECT value FROM system_control WHERE key = :key"), {'key': key}).fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting system_control value for key {key}: {e}")
        return None

def update_scan_progress(session: Session, processed: int, total: int):
    """Aktualizuje postęp skanowania w bazie danych."""
    update_system_control(session, 'scan_progress_processed', str(processed))
    update_system_control(session, 'scan_progress_total', str(total))

def append_scan_log(session: Session, message: str):
    """Dodaje nową linię do logu skanowania w bazie danych."""
    try:
        current_log_result = session.execute(text("SELECT value FROM system_control WHERE key = 'scan_log'")).fetchone()
        current_log = current_log_result[0] if current_log_result else ""
        
        timestamp = datetime.now(timezone.utc).strftime('%H:%M:%S')
        log_message = f"[{timestamp}] {message}\n"
        
        new_log = log_message + current_log
        
        if len(new_log) > 15000:
            new_log = new_log[:15000]

        stmt = text("""
            UPDATE system_control
            SET value = :new_log
            WHERE key = 'scan_log';
        """)
        session.execute(stmt, {'new_log': new_log})
        session.commit()
    except Exception as e:
        logger.error(f"Error appending to scan_log: {e}")
        session.rollback()


def clear_scan_log(session: Session):
    """Czyści log skanowania w bazie danych."""
    update_system_control(session, 'scan_log', '')


def check_for_commands(session: Session, current_state: str) -> tuple[bool, str]:
    """Sprawdza i reaguje na polecenia z bazy danych."""
    command = get_system_control_value(session, 'worker_command')
    should_run_now = False
    new_state = current_state

    if command == "START_REQUESTED":
        logger.info("Start command received, triggering immediate analysis cycle.")
        update_system_control(session, 'worker_command', 'NONE')
        should_run_now = True
    elif command == "PAUSE_REQUESTED" and current_state == "RUNNING":
        new_state = "PAUSED"
        update_system_control(session, 'worker_status', 'PAUSED')
        update_system_control(session, 'worker_command', 'NONE')
        logger.info("Worker paused by command.")
    elif command == "RESUME_REQUESTED" and current_state == "PAUSED":
        new_state = "RUNNING"
        update_system_control(session, 'worker_status', 'RUNNING')
        update_system_control(session, 'worker_command', 'NONE')
        logger.info("Worker resumed by command.")
        
    return should_run_now, new_state

def report_heartbeat(session: Session):
    """Raportuje 'życie' workera do bazy danych."""
    update_system_control(session, 'last_heartheart', datetime.now(timezone.utc).isoformat())

def safe_float(value) -> float | None:
    """Bezpiecznie konwertuje wartość na float, usuwając po drodze przecinki."""
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(',', '').replace('%', '')
        return float(value)
    except (ValueError, TypeError):
        return None

def get_performance(data: dict, days: int) -> float | None:
    """Oblicza zwrot procentowy w danym okresie na podstawie słownika."""
    try:
        time_series = data.get('Time Series (Daily)')
        if not time_series or len(time_series) < days + 1:
            return None
        
        dates = sorted(time_series.keys(), reverse=True)
        
        end_price = safe_float(time_series[dates[0]]['4. close'])
        start_price = safe_float(time_series[dates[days]]['4. close'])
        
        if start_price is None or end_price is None or start_price == 0:
            return None
        
        return ((end_price - start_price) / start_price) * 100
    except (IndexError, KeyError, TypeError) as e:
        logger.warning(f"Could not calculate performance: {e}")
        return None
