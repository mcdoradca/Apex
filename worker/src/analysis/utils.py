import logging
from sqlalchemy.orm import Session
from sqlalchemy import text, Row
from datetime import datetime, timezone, timedelta # Dodano timedelta
import pytz
import pandas as pd
import numpy as np
from pandas import Series as pd_Series
from typing import Optional, Tuple, Dict, Any # Dodano Dict, Any

# Importy dla Telegrama
import os
import requests
from urllib.parse import quote_plus
import hashlib 
import json # <-- NOWY IMPORT dla cache JSON

# Importujemy nowy model cache z Workera
from .. import models
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient


logger = logging.getLogger(__name__)

# ==================================================================
# KROK 1 (KAT. 1): Konfiguracja kluczy Telegrama (Bez zmian)
# ==================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
CACHE_EXPIRY_DAYS = 7 # <-- NOWA STAŁA: Czas ważności cache

if not TELEGRAM_BOT_TOKEN:
    logger.warning("TELEGRAM_BOT_TOKEN not found. Telegram alerts are DISABLED.")
if not TELEGRAM_CHAT_ID:
    logger.warning("TELEGRAM_CHAT_ID not found. Telegram alerts are DISABLED.")
# ==================================================================

# ==================================================================
# KROK 2 (Cache): NOWA FUNKCJA CACHE'OWANIA (OGÓLNA)
# ==================================================================
def get_raw_data_with_cache(
    session: Session, 
    api_client: AlphaVantageClient, 
    ticker: str, 
    data_type: str, 
    api_func: str, 
    **kwargs
) -> Dict[str, Any]:
    """
    Sprawdza, czy dane są w cache i są świeże. Jeśli nie, pobiera z API, zapisuje i zwraca.
    Zwraca surowy słownik (Dict[str, Any]) lub pusty słownik ({}) w przypadku błędu.
    """
    
    # 1. PRÓBA ODCZYTU Z CACHE
    try:
        cache_entry = session.query(models.AlphaVantageCache).filter(
            models.AlphaVantageCache.ticker == ticker,
            models.AlphaVantageCache.data_type == data_type
        ).first()

        now = datetime.now(timezone.utc)
        
        if cache_entry:
            # DANE ZNALEZIONE: Sprawdź, czy są świeże (7 dni)
            is_fresh = (now - cache_entry.last_fetched) < timedelta(days=CACHE_EXPIRY_DAYS)
            
            if is_fresh:
                # Jeśli jest to JSON, zwróć słownik
                return json.loads(cache_entry.raw_data_json)
                
            logger.warning(f"[Cache UTILS] Dane {data_type} dla {ticker} są nieaktualne. Pobieranie z API.")

    except Exception as e:
        logger.error(f"[Cache UTILS] Błąd odczytu z cache dla {ticker} ({data_type}): {e}", exc_info=True)
        # W razie błędu parsowania lub innego błędu DB, idziemy dalej do API

    # 2. POBIERANIE Z API
    
    # Wyszukaj odpowiednią metodę w kliencie AlphaVantage (musi to być funkcja w AlphaVantageClient)
    client_method = getattr(api_client, api_func, None)
    if not client_method:
        logger.error(f"[Cache UTILS] Nieznana funkcja API: {api_func}")
        return {}
        
    # === KRYTYCZNA POPRAWKA BŁĘDU ARGUMENTU ===
    # Zmieniamy nazwę zmiennej 'ticker' na 'symbol' podczas wywołania funkcji klienta,
    # ponieważ funkcje AlphaVantageClient oczekują parametru 'symbol'.
    raw_data = client_method(symbol=ticker, **kwargs)

    if not raw_data or raw_data.get("Error Message") or raw_data.get("Information"):
        logger.warning(f"[Cache UTILS] API zwróciło błąd lub puste dane dla {ticker} ({data_type}).")
        # Awaryjny powrót do nieświeżego cache
        if 'cache_entry' in locals() and cache_entry:
            logger.warning(f"[Cache UTILS] Awaryjny powrót do nieświeżego cache dla {ticker} ({data_type}).")
            return json.loads(cache_entry.raw_data_json)
        return {}

    # 3. ZAPIS DO CACHE (UPSERT)
    try:
        raw_data_json_str = json.dumps(raw_data)
        
        # Używamy UPSERT do zapisu
        upsert_stmt = text("""
            INSERT INTO alpha_vantage_cache (ticker, data_type, raw_data_json, last_fetched)
            VALUES (:ticker, :data_type, :raw_data, NOW())
            ON CONFLICT (ticker, data_type) DO UPDATE
            SET raw_data_json = :raw_data, last_fetched = NOW();
        """)
        session.execute(upsert_stmt, {
            'ticker': ticker,
            'data_type': data_type,
            'raw_data': raw_data_json_str
        })
        session.commit()
        logger.info(f"[Cache UTILS] Pomyślnie zapisano nowe dane {data_type} dla {ticker} do DB.")
        
    except Exception as e:
        logger.error(f"[Cache UTILS] Błąd zapisu do cache dla {ticker} ({data_type}): {e}", exc_info=True)
        session.rollback()
        
    return raw_data
# ==================================================================
# KONIEC NOWEJ FUNKCJI CACHE
# ==================================================================


# --- PONIŻEJ TYLKO ZMIANY NAWIĄZUJĄCE DO IMPORTU W INNYCH PLIKACH ---

_sent_alert_hashes = set()

def clear_alert_memory_cache():
# ... (bez zmian) ...
    """Czyści pamięć podręczną wysłanych alertów Telegrama."""
    global _sent_alert_hashes
    logger.info(f"Czyszczenie pamięci podręcznej alertów. Usunięto {len(_sent_alert_hashes)} wpisów.")
    _sent_alert_hashes = set()

def send_telegram_alert(message: str):
# ... (bez zmian) ...
    """
    Wysyła sformatowaną wiadomość do zdefiniowanego czatu na Telegramie.
    NOWA LOGIKA: Wysyła wiadomość only wtedy, jeśli nie została wysłana 
    wcześniej w tym cyklu (od ostatniego czyszczenia pamięci).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        # Nie wysyłaj, jeśli nie skonfigurowano (ostrzeżenie już poszło przy starcie)
        return

    # 1. Stwórz skrót (hash) wiadomości, aby ją unikalnie zidentyfikować
    message_hash = hashlib.sha256(message.encode('utf-8')).hexdigest()

    # 2. Sprawdź, czy ten skrót jest już w naszej pamięci podręcznej
    if message_hash in _sent_alert_hashes:
        logger.info(f"Alert został już wysłany (pomijanie duplikatu): {message[:50]}...")
        return # Nie wysyłaj ponownie tej samej wiadomości
    
    # 3. Jeśli nie, wyślij wiadomość i dodaj skrót do pamięci
    logger.info(f"Wysyłanie NOWEGO alertu Telegram: {message[:50]}...")

    # Kodowanie wiadomości, aby była bezpieczna dla URL (obsługa spacji, nowych linii, itp.)
    encoded_message = quote_plus(message)
    
    # Formatowanie URL
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage?chat_id={TELEGRAM_CHAT_ID}&text={encoded_message}"
    
    try:
        # Użyj niskiego timeoutu (5s), aby nie blokować pętli workera
        response = requests.get(url, timeout=5)
        response.raise_for_status() # Sprawdź błędy HTTP (np. 400, 404, 500)
        
        if response.json().get('ok'):
            logger.info(f"Pomyślnie wysłano alert Telegram: {message[:50]}...")
            # 4. Dodaj do pamięci TYLKO po pomyślnym wysłaniu
            _sent_alert_hashes.add(message_hash)
        else:
            logger.error(f"Telegram API zwrócił błąd: {response.text}")
    except requests.exceptions.RequestException as e:
        # Złap błędy sieciowe (timeout, brak połączenia)
        logger.error(f"Nie można wysłać alertu Telegram: {e}")
    except Exception as e:
        # Złap inne błędy (np. JSON decode error)
        logger.error(f"Nieoczekiwany błąd podczas wysyłania alertu Telegram: {e}")

def get_current_NY_datetime() -> datetime:
# ... (bez zmian) ...
    """Zwraca aktualny obiekt datetime dla strefy czasowej Nowego Jorku."""
    try:
        tz = pytz.timezone('US/Eastern')
        return datetime.now(tz)
    except Exception as e:
        logger.error(f"Error getting New York time: {e}", exc_info=True)
        # Zwróć czas UTC jako awaryjny
        return datetime.now(timezone.utc)

def get_market_status_and_time(api_client) -> dict:
# ... (bez zmian) ...
    """
    Sprawdza status giełdy NASDAQ używając dedykowanego endpointu API
    i zwraca czas w Nowym Jorku.
    """
    # 1. Zawsze pobieraj aktualny czas dla celów wyświetlania
    try:
        # ZMIANA: Używamy nowej, wydzielonej funkcji
        now_ny = get_current_NY_datetime()
        time_ny_str = now_ny.strftime('%H:%M:%S ET')
        date_ny_str = now_ny.strftime('%Y-%m-%d')
    except Exception as e:
        logger.error(f"Error formatting New York time: {e}")
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
# ... (bez zmian) ...
    """Aktualizuje lub wstawia wartość w tabeli system_control (UPSERT)."""
    try:
        # ==================================================================
        # === KLUCZOWA POPRAWKA BŁĘDU SQL (SyntaxError: 'NOW()') ===
        # Zmieniamy `NOW()` na jawną nazwę kolumny `updated_at`
        # ==================================================================
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
# ... (bez zmian) ...
    """Odczytuje pojedynczą wartość z tabeli system_control."""
    try:
        result = session.execute(text("SELECT value FROM system_control WHERE key = :key"), {'key': key}).fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting system_control value for key {key}: {e}")
        return None

def update_scan_progress(session: Session, processed: int, total: int):
# ... (bez zmian) ...
    """Aktualizuje postęp skanowania w bazie danych."""
    update_system_control(session, 'scan_progress_processed', str(processed))
    update_system_control(session, 'scan_progress_total', str(total))

def append_scan_log(session: Session, message: str):
# ... (bez zmian) ...
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
# ... (bez zmian) ...
    """Czyści log skanowania w bazie danych."""
    update_system_control(session, 'scan_log', '')


def check_for_commands(session: Session, current_state: str) -> tuple[bool, str]:
# ... (bez zmian) ...
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
# ... (bez zmian) ...
    """Raportuje 'życie' workera do bazy danych."""
    update_system_control(session, 'last_heartbeat', datetime.now(timezone.utc).isoformat())

def safe_float(value) -> float | None:
# ... (bez zmian) ...
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
# ... (bez zmian) ...
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

# --- NARZĘDZIA DO OPTYMALIZACJI API ---

def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
# ... (bez zmian) ...
    """Standaryzuje nazwy kolumn z API ('1. open' -> 'open') i konwertuje na typy numeryczne."""
    if df.empty:
        return df
    
    # Sprawdź, czy kolumny już są w poprawnym formacie
    if 'open' in df.columns and 'close' in df.columns:
        # Ten warunek jest teraz wystarczający. Kolumna 'vwap' zostanie
        # dodana RĘCZNIE w `backtest_engine.py`, jeśli brakuje jej w surowych danych.
        return df # Już przetworzone

    # ==================================================================
    # === POPRAWKA VWAP (Usunięcie niestabilnego mapowania) ===
    # Usuwamy mapowanie VWAP, ponieważ błąd 'KeyError: vwap' nadal występuje
    # (nawet przy poprawnym mapowaniu), co sugeruje, że API nie zwraca
    # tej kolumny konsekwentnie. Polegamy na AWARYJNYM obliczeniu w backtest_engine.py.
    # ==================================================================
    column_mapping = {
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        # TIME_SERIES_DAILY
        '5. volume': 'volume', 
        # TIME_SERIES_DAILY_ADJUSTED
        '6. volume': 'volume', 
        # Usuwamy niestabilne mapowania '5. vwap' i '6. vwap'
        '7. adjusted close': 'adjusted close', 
        '8. split coefficient': 'split coefficient'
    }

    # Zmieniamy nazwy kolumn na podstawie mapowania
    # Używamy rozdzielacza '. ' jako awaryjnego (fallback)
    df.rename(columns=lambda c: column_mapping.get(c, c.split('. ')[-1]), inplace=True)
    # ==================================================================

    # Konwertuj kluczowe kolumny na numeryczne
    # Włączamy 'vwap' do konwersji, ponieważ może istnieć w surowych danych (jeśli API go zwróci)
    # Jeśli go nie zwróci, zostanie obsłużony przez 'backtest_engine'
    for col in ['open', 'high', 'low', 'close', 'volume', 'adjusted close', 'vwap']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df.sort_index(inplace=True) # Upewnij się, że dane są posortowane od najstarszych do najnowszych
    return df

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd_Series:
# ... (bez zmian) ...
    """Oblicza ATR (Average True Range) na podstawie DataFrame OHLC."""
    if df.empty or len(df) < period:
        return pd.Series(dtype=float)
    
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    # Używamy EMA (ewm) do wygładzenia TR, co jest standardem dla ATR
    atr = tr.ewm(span=period, adjust=False).mean()
    return atr

def calculate_rsi(series: pd_Series, period: int = 14) -> pd_Series:
# ... (bez zmian) ...
    """Oblicza RSI (Relative Strength Index)."""
    if series.empty or len(series) < period:
        return pd.Series(dtype=float)
        
    delta = series.diff(1)
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_bbands(series: pd_Series, period: int = 20, num_std: int = 2) -> tuple:
# ... (bez zmian) ...
    """Oblicza Bollinger Bands (Środkowa, Górna, Dolna) oraz Szerokość Wstęgi (BBW)."""
    if series.empty or len(series) < period:
        return pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)

    middle_band = series.rolling(window=period).mean()
    std_dev = series.rolling(window=period).std()
    
    upper_band = middle_band + (std_dev * num_std)
    lower_band = middle_band - (std_dev * num_std)
    
    # Oblicz BBW (Szerokość Wstęg Bollingera) jako procent środkowej wstęgi
    bbw = (upper_band - lower_band) / middle_band
    
    return middle_band, upper_band, lower_band, bbw

# --- POPRAWKA: Dodanie brakującej funkcji ---
def calculate_ema(series: pd_Series, period: int) -> pd_Series:
# ... (bez zmian) ...
    """Oblicza Wykładniczą Średnią Kroczącą (EMA)."""
    if series.empty or len(series) < period:
        return pd.Series(dtype=float)
    return series.ewm(span=period, adjust=False).mean()


# ==================================================================
# === NOWA FUNKCJA (Dla "Strategy Battle Royale") ===
# ==================================================================
def calculate_macd(series: pd_Series, short_period=12, long_period=26, signal_period=9) -> tuple:
# ... (bez zmian) ...
    """
    Oblicza linię MACD (EMA(12) - EMA(26)) oraz linię Sygnału (EMA(9) z MACD).
    Zwraca (macd_line, signal_line).
    """
    if series.empty or len(series) < long_period:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    
    short_ema = calculate_ema(series, short_period)
    long_ema = calculate_ema(series, long_period)
    
    macd_line = short_ema - long_ema
    signal_line = calculate_ema(macd_line, signal_period)
    
    return macd_line, signal_line
# ==================================================================
# === KONIEC NOWEJ FUNKCJI ===
# ==================================================================


# ==================================================================
# === NOWE FUNKCJE DLA AQM (Volume Entropy Score, PDF str. 14-15) ===
# ==================================================================

def calculate_obv(df: pd.DataFrame) -> pd_Series:
# ... (bez zmian) ...
    """Oblicza On-Balance Volume (OBV) używając metody wektorowej."""
    if 'close' not in df.columns or 'volume' not in df.columns:
        logger.error("Brak kolumn 'close' lub 'volume' do obliczenia OBV.")
        return pd.Series(dtype=float)
    
    # Używamy numpy.sign() na różnicy ceny
    price_diff = df['close'].diff()
    direction = np.sign(price_diff).fillna(0) # 0 dla NaN (pierwszy wiersz) i dla braku zmiany
    
    # Mnożymy wolumen przez kierunek
    directional_volume = df['volume'] * direction
    
    # OBV to skumulowana suma
    return directional_volume.cumsum()

def calculate_ad(df: pd.DataFrame) -> pd_Series:
# ... (bez zmian) ...
    """Oblicza Linię Akumulacji/Dystrybucji (A/D Line)."""
    if not all(col in df.columns for col in ['high', 'low', 'close', 'volume']):
        logger.error("Brak kolumn 'high', 'low', 'close', 'volume' do obliczenia A/D.")
        return pd.Series(dtype=float)

    # 1. Money Flow Multiplier
    high_low_diff = df['high'] - df['low']
    
    # Użyj wektoryzacji numpy, aby uniknąć dzielenia przez zero
    # where(warunek, jeśli_prawda, jeśli_fałsz)
    mfm = np.where(
        high_low_diff == 0, 
        0.0, # Jeśli high == low, MFM = 0
        ((df['close'] - df['low']) - (df['high'] - df['close'])) / high_low_diff
    )
    
    # 2. Money Flow Volume
    mfv = mfm * df['volume']
    
    # 3. A/D Line (skumulowana suma)
    ad_line = mfv.cumsum()
    
    return ad_line

# ==================================================================
# === KONIEC NOWYCH FUNKCJI AQM ===
# ==================================================================
