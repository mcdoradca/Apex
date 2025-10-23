import logging
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from pandas import Series as pd_Series

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
# Usunięto nieużywany import get_market_status_and_time (Błąd #3)
from .utils import update_scan_progress, append_scan_log, safe_float, update_system_control
from ..config import Phase3Config

logger = logging.getLogger(__name__)

def calculate_ema(series: pd_Series, period: int) -> pd_Series:
    """Oblicza Wykładniczą Średnią Kroczącą (EMA)."""
    return series.ewm(span=period, adjust=False).mean()

def standardize_df_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Standaryzuje nazwy kolumn i konwertuje na typy numeryczne."""
    if df.empty:
        return df
    # Sprawdzenie czy kolumny już mają standardowe nazwy
    standard_cols = ['open', 'high', 'low', 'close', 'volume', 'adjusted close', 'dividend amount', 'split coefficient']
    if all(col in standard_cols for col in df.columns):
         # Tworzymy kopię, aby uniknąć SettingWithCopyWarning
         df = df.copy()
    else:
        # Próbujemy znormalizować nazwy w stylu Alpha Vantage
        try:
             # Używamy copy(), aby uniknąć modyfikacji oryginalnego DataFrame poza funkcją
             df = df.copy()
             df.columns = [col.split('. ')[-1] for col in df.columns]
        except Exception as e:
            logger.error(f"Error standardizing columns (might already be standard): {e}. Columns: {df.columns}")
            # Nadal tworzymy kopię
            df = df.copy()
    # Konwersja na typy numeryczne
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            # Używamy errors='coerce', aby zamienić błędy konwersji na NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def _find_breakout_setup(daily_df: pd.DataFrame, min_consolidation_days=5, breakout_atr_multiplier=1.0) -> dict | None:
    """Szuka wybicia ponad konsolidację na wykresie dziennym."""
    try:
        if len(daily_df) < min_consolidation_days + 2: return None
        # Obliczanie True Range (TR)
        high_low = daily_df['high'] - daily_df['low']
        high_close = (daily_df['high'] - daily_df['close'].shift()).abs()
        low_close = (daily_df['low'] - daily_df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        # Obliczanie Average True Range (ATR) używając EMA
        atr = calculate_ema(tr, 14) # Używamy 14-okresowego ATR
        current_atr = atr.iloc[-1]
        # Sprawdzenie czy ATR nie jest zerowe, aby uniknąć dzielenia przez zero
        if current_atr == 0: return None
        # Określenie okresu konsolidacji (ostatnie 'min_consolidation_days' dni przed ostatnią świecą)
        consolidation_df = daily_df.iloc[-(min_consolidation_days + 1):-1]
        # Znalezienie najwyższego High i najniższego Low w okresie konsolidacji
        consolidation_high = consolidation_df['high'].max()
        consolidation_low = consolidation_df['low'].min()
        consolidation_range = consolidation_high - consolidation_low
        # Sprawdzenie, czy zakres konsolidacji jest "wąski" (np. mniej niż 2x ATR)
        is_consolidating = consolidation_range < (2 * atr.iloc[-2]) # Używamy ATR z poprzedniego dnia
        # Ostatnia świeca
        latest_candle = daily_df.iloc[-1]
        # Sprawdzenie, czy ostatnia świeca zamknęła się powyżej szczytu konsolidacji
        is_breakout = latest_candle['close'] > consolidation_high
        # Sprawdzenie, czy wybicie jest "silne" (np. cena zamknięcia jest znacząco powyżej szczytu)
        is_strong_breakout = latest_candle['close'] > (consolidation_high + breakout_atr_multiplier * current_atr)

        if is_consolidating and is_breakout and is_strong_breakout:
            logger.info(f"Breakout setup found for ticker on {daily_df.index[-1]}") # Dodano logowanie daty
            entry_price = latest_candle['high'] + 0.01 # Wejście lekko powyżej High ostatniej świecy
            stop_loss = consolidation_high - (0.5 * current_atr) # Stop-Loss poniżej szczytu konsolidacji
            return {
                "setup_type": "BREAKOUT",
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "consolidation_high": consolidation_high,
                "atr": current_atr
            }
        return None
    except Exception as e:
        # Logujemy błąd ze śladem stosu dla lepszej diagnostyki
        logger.error(f"Error in _find_breakout_setup: {e}", exc_info=True)
        return None

def _find_ema_bounce_setup(daily_df: pd.DataFrame, ema_period=9) -> dict | None:
    """Szuka odbicia od rosnącej EMA na wykresie dziennym."""
    try:
        if len(daily_df) < ema_period + 3: return None
        # Oblicz EMA
        daily_df['ema'] = calculate_ema(daily_df['close'], ema_period)
        # Sprawdź, czy EMA rośnie (ostatnie 3 wartości)
        is_ema_rising = daily_df['ema'].iloc[-1] > daily_df['ema'].iloc[-2] > daily_df['ema'].iloc[-3]
        # Ostatnia i przedostatnia świeca
        latest_candle = daily_df.iloc[-1]
        prev_candle = daily_df.iloc[-2]
        latest_ema = daily_df['ema'].iloc[-1]
        # Sprawdź, czy cena "dotknęła" lub zbliżyła się do EMA (poprzedni Low lub obecny Open)
        touched_ema = (prev_candle['low'] <= daily_df['ema'].iloc[-2] * 1.01) or \
                      (latest_candle['open'] <= latest_ema * 1.01)
        # Sprawdź, czy zamknięcie jest powyżej EMA
        closed_above_ema = latest_candle['close'] > latest_ema
        # Sprawdź, czy ostatnia świeca jest bycza (zamknięcie > otwarcie)
        is_bullish_candle = latest_candle['close'] > latest_candle['open']

        if is_ema_rising and touched_ema and closed_above_ema and is_bullish_candle:
             logger.info(f"EMA Bounce setup found for ticker on {daily_df.index[-1]}") # Dodano logowanie daty
             # Oblicz ATR do ustawienia Stop-Loss
             high_low = daily_df['high'] - daily_df['low']
             high_close = (daily_df['high'] - daily_df['close'].shift()).abs()
             low_close = (daily_df['low'] - daily_df['close'].shift()).abs()
             tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
             atr = calculate_ema(tr, 14).iloc[-1]
             # Ustawienie wejścia i SL
             entry_price = latest_candle['high'] + 0.01
             stop_loss = latest_candle['low'] - (0.5 * atr)

             return {
                 "setup_type": "EMA_BOUNCE",
                 "entry_price": entry_price,
                 "stop_loss": stop_loss,
                 "ema_value": latest_ema,
                 "atr": atr
             }
        return None
    except Exception as e:
        logger.error(f"Error in _find_ema_bounce_setup: {e}", exc_info=True)
        return None

def _find_impulse_and_fib_zone(daily_df: pd.DataFrame) -> dict | None:
    """Identyfikuje ostatni impuls wzrostowy i oblicza strefę Fibo 0.382-0.618."""
    try:
        if len(daily_df) < 21: return None # Potrzebujemy trochę historii
        # Rozważamy ostatnie 21 dni handlowych (około miesiąca)
        recent_df = daily_df.iloc[-21:]
        # Znajdź najniższą cenę Low i jej datę w tym okresie
        low_point_price = recent_df['low'].min()
        low_point_date_loc = recent_df['low'].idxmin()
        # Sprawdzenie, czy data minimum jest w indeksie (powinna być)
        if low_point_date_loc not in recent_df.index:
             logger.warning(f"Cannot find low point date {low_point_date_loc} in recent_df index for Fib calculation.")
             return None
        # Wybierz dane *po* znalezionym minimum
        df_after_low = recent_df[recent_df.index > low_point_date_loc]
        # Jeśli nie ma danych po minimum (minimum było ostatniego dnia), to nie ma impulsu
        if df_after_low.empty: return None
        # Znajdź najwyższą cenę High w okresie po minimum
        high_point_price = df_after_low['high'].max()
        # Unikaj dzielenia przez zero lub ujemne ceny
        if low_point_price <= 0: return None
        # Oblicz siłę impulsu (np. wzrost o co najmniej 10%)
        impulse_strength = (high_point_price - low_point_price) / low_point_price
        if impulse_strength < 0.10: # Minimum 10% wzrostu dla impulsu
            return None
        # Oblicz strefę Fibo
        fib_range = high_point_price - low_point_price
        entry_zone_top = high_point_price - 0.382 * fib_range
        entry_zone_bottom = high_point_price - 0.618 * fib_range
        # Zwróć wyniki
        return {
            "impulse_high": high_point_price,
            "impulse_low": low_point_price,
            "entry_zone_top": entry_zone_top,
            "entry_zone_bottom": entry_zone_bottom
        }
    except Exception as e:
        logger.error(f"Error in _find_impulse_and_fib_zone: {e}", exc_info=True)
        return None


def find_end_of_day_setup(ticker: str, api_client: AlphaVantageClient) -> dict:
    """Szuka setupów Breakout, EMA Bounce lub Fib Zone na koniec dnia."""
    daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact') # Compact wystarczy do EOD
    if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw:
        return {"signal": False, "reason": "Brak danych dziennych."}
    # Standaryzacja i sortowanie danych
    daily_df = pd.DataFrame.from_dict(daily_data_raw['Time Series (Daily)'], orient='index')
    daily_df = standardize_df_columns(daily_df)
    daily_df.sort_index(inplace=True)
    # Sprawdzenie czy mamy wystarczająco danych
    if daily_df.empty or len(daily_df) < 21: # Potrzebne dla ATR, EMA i Fib
         return {"signal": False, "reason": "Niewystarczająca historia danych dziennych."}
    # Pobranie ostatniej ceny zamknięcia
    current_price = daily_df['close'].iloc[-1]
    # Sprawdzenie setupu Breakout
    breakout_setup = _find_breakout_setup(daily_df)
    if breakout_setup:
        risk = breakout_setup['entry_price'] - breakout_setup['stop_loss']
        if risk <= 0: return {"signal": False, "reason": "Błąd kalkulacji ryzyka (Breakout)."}
        # Używamy TARGET_RR_RATIO z konfiguracji
        take_profit = breakout_setup['entry_price'] + (Phase3Config.TARGET_RR_RATIO * risk)
        return {
            "signal": True, "status": "ACTIVE", # Breakout jest od razu aktywny
            "ticker": ticker,
            "entry_price": float(breakout_setup['entry_price']),
            "stop_loss": float(breakout_setup['stop_loss']),
            "take_profit": float(take_profit),
            "risk_reward_ratio": Phase3Config.TARGET_RR_RATIO,
            "notes": f"Setup EOD (AKTYWNY): Wybicie z konsolidacji. Opór: {breakout_setup['consolidation_high']:.2f}."
        }
    # Sprawdzenie setupu EMA Bounce
    ema_bounce_setup = _find_ema_bounce_setup(daily_df, ema_period=Phase3Config.EMA_PERIOD)
    if ema_bounce_setup:
        risk = ema_bounce_setup['entry_price'] - ema_bounce_setup['stop_loss']
        if risk <= 0: return {"signal": False, "reason": "Błąd kalkulacji ryzyka (EMA Bounce)."}
        take_profit = ema_bounce_setup['entry_price'] + (Phase3Config.TARGET_RR_RATIO * risk)
        return {
            "signal": True, "status": "ACTIVE", # EMA Bounce jest od razu aktywny
            "ticker": ticker,
            "entry_price": float(ema_bounce_setup['entry_price']),
            "stop_loss": float(ema_bounce_setup['stop_loss']),
            "take_profit": float(take_profit),
            "risk_reward_ratio": Phase3Config.TARGET_RR_RATIO,
            "notes": f"Setup EOD (AKTYWNY): Odbicie od rosnącej EMA{Phase3Config.EMA_PERIOD}. EMA={ema_bounce_setup['ema_value']:.2f}."
        }
    # Sprawdzenie setupu Fib Zone (Impuls + Retracement)
    impulse_result = _find_impulse_and_fib_zone(daily_df)
    if impulse_result:
        # Sprawdź, czy aktualna cena zamknięcia jest w strefie Fibo
        is_in_zone = impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"]
        if is_in_zone:
            # Dla Fibo, Take Profit jest na szczycie impulsu
            take_profit = float(impulse_result['impulse_high'])
            # Stop Loss (można zdefiniować np. poniżej dołka impulsu lub użyć ATR)
            # Na razie SL nie jest ustalany automatycznie dla PENDING
            return {
                "signal": True, "status": "PENDING", # Setup Fibo wymaga potwierdzenia intraday
                "ticker": ticker,
                "entry_zone_bottom": float(impulse_result['entry_zone_bottom']),
                "entry_zone_top": float(impulse_result['entry_zone_top']),
                "take_profit": take_profit,
                # Nie ustawiamy entry_price ani stop_loss, bo to zależy od sygnału intraday
                "notes": f"Setup EOD (OCZEKUJĄCY): Cena ({current_price:.2f}) w strefie Fib. Oczekuje na sygnał intraday."
            }
        else:
             # Cena poza strefą Fibo
             return {"signal": False, "reason": f"Fib: Cena ({current_price:.2f}) poza strefą [{impulse_result['entry_zone_bottom']:.2f}-{impulse_result['entry_zone_top']:.2f}]."}
    # Jeśli żaden setup nie został znaleziony
    return {"signal": False, "reason": "Brak setupu EOD (Fib/Breakout/EMA Bounce)."}

def run_tactical_planning(session: Session, qualified_tickers: list[str], get_current_state, api_client: AlphaVantageClient):
    """Główna funkcja dla SKANERA NOCNEGO (EOD)."""
    logger.info("Running Phase 3: End-of-Day Tactical Planning...")
    append_scan_log(session, "Faza 3: Skanowanie EOD w poszukiwaniu setupów...")
    successful_setups = 0
    total_tickers = len(qualified_tickers)
    processed_count = 0

    for ticker in qualified_tickers:
        # Sprawdzanie poleceń pauzy/wznowienia
        if get_current_state() == 'PAUSED':
            logger.info("Phase 3 (EOD Scan) paused.")
            while get_current_state() == 'PAUSED': time.sleep(1)
            logger.info("Phase 3 (EOD Scan) resumed.")

        try:
            trade_setup = find_end_of_day_setup(ticker, api_client)
            if trade_setup.get("signal"):
                successful_setups += 1
                # Używamy INSERT ... ON CONFLICT DO UPDATE, aby obsłużyć istniejące sygnały
                # (np. jeśli PENDING został znaleziony ponownie lub ACTIVE został znaleziony dla tickera, który był PENDING)
                stmt = text("""
                    INSERT INTO trading_signals (
                        ticker, generation_date, status,
                        entry_price, stop_loss, take_profit, risk_reward_ratio,
                        notes, entry_zone_bottom, entry_zone_top
                    )
                    VALUES (
                        :ticker, NOW(), :status,
                        :entry, :sl, :tp, :rr,
                        :notes, :ezb, :ezt
                    )
                    ON CONFLICT (ticker) WHERE status IN ('ACTIVE', 'PENDING', 'TRIGGERED') -- Zaktualizowano warunek konfliktu
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        generation_date = EXCLUDED.generation_date,
                        entry_price = EXCLUDED.entry_price,
                        stop_loss = EXCLUDED.stop_loss,
                        take_profit = EXCLUDED.take_profit,
                        risk_reward_ratio = EXCLUDED.risk_reward_ratio,
                        notes = EXCLUDED.notes,
                        entry_zone_bottom = EXCLUDED.entry_zone_bottom,
                        entry_zone_top = EXCLUDED.entry_zone_top;
                """)
                params = {
                    'ticker': ticker,
                    'status': trade_setup['status'],
                    'entry': trade_setup.get('entry_price'),
                    'sl': trade_setup.get('stop_loss'),
                    'tp': trade_setup.get('take_profit'),
                    'rr': trade_setup.get('risk_reward_ratio'),
                    'notes': trade_setup.get('notes'),
                    'ezb': trade_setup.get('entry_zone_bottom'),
                    'ezt': trade_setup.get('entry_zone_top')
                }
                session.execute(stmt, [params]) # Przekazujemy listę parametrów
                session.commit()
                # Logowanie i alerty
                log_prefix = f"NOWY SYGNAŁ (F3): {ticker} [{trade_setup['status']}]"
                log_message = f"{log_prefix} | {trade_setup.get('notes', 'Brak notatek.')}"
                append_scan_log(session, log_message)
                logger.info(log_message) # Dodano logowanie do konsoli
                # Alert tylko dla NOWYCH aktywnych sygnałów
                if trade_setup['status'] == 'ACTIVE':
                    alert_msg = f"NOWY SYGNAŁ AKTYWNY (EOD): {ticker} gotowy do wejścia!"
                    update_system_control(session, 'system_alert', alert_msg)
            else:
                # Logowanie informacji o braku setupu
                log_info_msg = f"INFO (F3): {ticker} - {trade_setup.get('reason')}"
                append_scan_log(session, log_info_msg)
                logger.info(log_info_msg) # Dodano logowanie do konsoli
        except Exception as e:
            logger.error(f"Error in Phase 3 EOD scan for {ticker}: {e}", exc_info=True)
            append_scan_log(session, f"BŁĄD: Nie udało się przetworzyć {ticker} w Fazie 3: {e}") # Logowanie błędu
            session.rollback()
        finally:
             processed_count += 1
             # Aktualizacja postępu Fazy 3
             update_scan_progress(session, processed_count, total_tickers)

    append_scan_log(session, f"Faza 3 (Skaner EOD) zakończona. Znaleziono {successful_setups} setupów.")


def monitor_entry_triggers(session: Session, api_client: AlphaVantageClient):
    """
    Ulepszony monitor, który sprawdza WSZYSTKIE sygnały Fazy 3 (PENDING i ACTIVE)
    pod kątem osiągnięcia ceny wejścia w czasie rzeczywistym.

    POPRAWKA BŁĘDU #4: Rozdzielono logikę dla PENDING (<= górna strefa) i ACTIVE (>= cena wejścia).
    Dodano zmianę statusu ACTIVE na TRIGGERED.
    """
    logger.info("Running Real-Time Entry Trigger Monitor for all Phase 3 signals...")

    try:
        all_signals_rows = session.execute(text("""
            SELECT id, ticker, status, entry_price, entry_zone_bottom, entry_zone_top, notes
            FROM trading_signals WHERE status IN ('ACTIVE', 'PENDING') -- Monitorujemy tylko te dwa statusy
        """)).fetchall()

        if not all_signals_rows:
            logger.info("No ACTIVE or PENDING signals to monitor.")
            return

        tickers_to_monitor = [row.ticker for row in all_signals_rows]
        logger.info(f"Monitoring {len(tickers_to_monitor)} tickers: {', '.join(tickers_to_monitor)}")

        first_run = True
        market_status_determined = "unknown" # Przechowuje status z pierwszego zapytania

        for signal_row in all_signals_rows:
            # Krótka pauza między tickerami, aby rozłożyć zapytania
            time.sleep(0.5)

            try:
                ticker = signal_row.ticker

                # Pobieramy najnowsze dane cenowe i status rynku
                quote_data = api_client.get_live_quote_details(ticker)

                # Sprawdzamy status rynku tylko raz
                if first_run:
                    market_status_determined = quote_data.get("market_status_internal", "CLOSED")
                    if market_status_determined == "CLOSED":
                         logger.info(f"Market is {market_status_determined}. Skipping Entry Trigger Monitor.")
                         # Jeśli rynek zamknięty, kończymy całe monitorowanie
                         return
                    first_run = False # Już sprawdziliśmy

                # Sprawdzamy, czy mamy cenę live
                current_price = quote_data.get('live_price')
                if current_price is None:
                    logger.warning(f"Could not get current live price for {ticker} during monitoring.")
                    continue

                # --- POPRAWKA BŁĘDU #4: Rozdzielenie logiki ---

                if signal_row.status == 'PENDING':
                    # Logika dla sygnałów PENDING (Fib Zone) - kupno na wsparciu
                    # Sprawdzamy, czy cena weszła DO strefy (jest poniżej górnej granicy)
                    entry_target_top = signal_row.entry_zone_top

                    if entry_target_top is not None and current_price <= float(entry_target_top):
                        logger.info(f"TRIGGER (PENDING->ACTIVE): {ticker} current price ({current_price:.2f}) is at or below Fib entry zone top ({entry_target_top:.2f}).") # Dodano formatowanie

                        # Promuj na ACTIVE
                        update_stmt = text("UPDATE trading_signals SET status = 'ACTIVE', notes = :notes WHERE id = :signal_id")
                        session.execute(update_stmt, {
                            'signal_id': signal_row.id,
                            'notes': (signal_row.notes or "") + f" | Aktywowany przez monitor intraday ({current_price:.2f})."
                        })
                        session.commit()

                        # Generuj alert
                        alert_msg = f"ALARM (Fib): {ticker} ({current_price:.2f}) wszedł w strefę wejścia!"
                        update_system_control(session, 'system_alert', alert_msg)
                        logger.info(f"Generated alert: {alert_msg}") # Dodatkowe logowanie alertu

                elif signal_row.status == 'ACTIVE':
                    # Logika dla sygnałów ACTIVE (Breakout/EMA) - kupno na wybiciu
                    entry_target_price = signal_row.entry_price

                    if entry_target_price is not None and current_price >= float(entry_target_price):
                        logger.info(f"TRIGGER (ACTIVE->TRIGGERED): {ticker} current price ({current_price:.2f}) is at or above Breakout/EMA entry price ({entry_target_price:.2f}).") # Dodano formatowanie

                        # Generuj alert
                        alert_msg = f"ALARM (Breakout/EMA): {ticker} ({current_price:.2f}) osiągnął cenę wejścia!"
                        update_system_control(session, 'system_alert', alert_msg)
                        logger.info(f"Generated alert: {alert_msg}") # Dodatkowe logowanie alertu

                        # Zmień status na 'TRIGGERED', aby nie alertować wielokrotnie
                        update_stmt = text("UPDATE trading_signals SET status = 'TRIGGERED', notes = :notes WHERE id = :signal_id")
                        session.execute(update_stmt, {
                            'signal_id': signal_row.id,
                            'notes': (signal_row.notes or "") + f" | Sygnał wejścia aktywowany ({current_price:.2f})."
                        })
                        session.commit()

            except Exception as e_ticker:
                # Błąd przetwarzania pojedynczego tickera - logujemy i kontynuujemy
                logger.error(f"Error monitoring trigger for {signal_row.ticker}: {e_ticker}", exc_info=True)
                session.rollback() # Wycofaj zmiany tylko dla tego tickera

    except Exception as e_main:
        # Błąd na poziomie pobierania listy sygnałów lub inny błąd główny
        logger.error(f"Critical error in monitor_entry_triggers main loop: {e_main}", exc_info=True)
        session.rollback() # Wycofaj potencjalne zmiany
    finally:
        # Upewnij się, że sesja jest zamknięta, nawet jeśli wystąpił błąd
        # (Chociaż get_db_session() w main_loop zwykle to robi)
        if session:
            session.close() # Dodatkowe zabezpieczenie

