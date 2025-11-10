import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from typing import List, Dict, Any

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    get_current_NY_datetime,
    append_scan_log,
    calculate_rsi,
    calculate_macd,
    # ==================================================================
    # === NOWY IMPORT (Strategy Battle Royale) ===
    # Potrzebujemy `calculate_atr` bezpośrednio tutaj
    # ==================================================================
    calculate_atr
)
# Importujemy *tylko* setup Breakout, EMA zrobimy lokalnie
from .phase3_sniper import _find_breakout_setup, _find_impulse_and_fib_zone
from .. import models
from ..config import Phase3Config

logger = logging.getLogger(__name__)

# ==================================================================
# === Środowisko Backtestingu ===
# ==================================================================

# Horyzont czasowy dla strategii
MAX_HOLD_DAYS_DEFAULT = 7

# ==================================================================
# === Silnik Symulacji ===
# ==================================================================

def _resolve_trade(historical_data: pd.DataFrame, entry_index: int, setup: Dict[str, Any], max_hold_days: int, year: str) -> models.VirtualTrade | None:
    """
    "Spogląda w przyszłość" (w danych historycznych), aby zobaczyć, jak
    dana transakcja by się zakończyła.
    """
    try:
        entry_price = setup['entry_price']
        stop_loss = setup['stop_loss']
        take_profit = setup['take_profit']
        
        # Iterujemy przez N kolejnych dni (świec) po wejściu
        for i in range(1, max_hold_days + 1):
            if entry_index + i >= len(historical_data):
                # Koniec danych historycznych, zamykamy po ostatniej cenie
                candle = historical_data.iloc[-1]
                close_price = candle['close']
                status = 'CLOSED_EXPIRED'
                break
            
            candle = historical_data.iloc[entry_index + i]
            day_low = candle['low']
            day_high = candle['high']

            # Sprawdzenie 1: Czy trafiono Stop Loss? (Sprawdzamy Low dnia)
            if day_low <= stop_loss:
                logger.info(f"    [Backtest] ZAMKNIĘCIE (SL): {candle.name.date()} @ {stop_loss:.2f}")
                close_price = stop_loss
                status = 'CLOSED_SL'
                break
                
            # Sprawdzenie 2: Czy trafiono Take Profit? (Sprawdzamy High dnia)
            if day_high >= take_profit:
                logger.info(f"    [Backtest] ZAMKNIĘCIE (TP): {candle.name.date()} @ {take_profit:.2f}")
                close_price = take_profit
                status = 'CLOSED_TP'
                break
        else:
            # Sprawdzenie 3: Jeśli pętla się zakończyła (minęło max_hold_days), zamykamy po cenie zamknięcia ostatniego dnia
            candle = historical_data.iloc[entry_index + max_hold_days]
            close_price = candle['close']
            status = 'CLOSED_EXPIRED'
            logger.info(f"    [Backtest] ZAMKNIĘCIE (Wygasło): {candle.name.date()} @ {close_price:.2f}")

        # Oblicz P/L %
        p_l_percent = ((close_price - entry_price) / entry_price) * 100
        
        trade = models.VirtualTrade(
            ticker=setup['ticker'],
            status=status,
            setup_type=f"BACKTEST_{year}_{setup['setup_type']}",
            entry_price=float(entry_price),
            stop_loss=float(stop_loss),
            take_profit=float(take_profit),
            open_date=historical_data.index[entry_index].to_pydatetime(), # Data znalezienia setupu
            close_date=candle.name.to_pydatetime(), # Data zamknięcia
            close_price=float(close_price),
            final_profit_loss_percent=float(p_l_percent)
        )
        
        return trade

    except Exception as e:
        logger.error(f"[Backtest] Błąd podczas rozwiązywania transakcji: {e}", exc_info=True)
        return None

# ==================================================================
# === NOWA FUNKCJA (Strategy Battle Royale) ===
# Kopiujemy logikę EMA Bounce z phase3_sniper, ale usuwamy z niej
# filtr ATR, aby mieć czystą bazę do testów.
# ==================================================================
def _find_base_ema_bounce(daily_df: pd.DataFrame) -> dict | None:
    """
    Znajduje *podstawowy* setup EMA Bounce (bez filtra ATR),
    aby służył jako grupa kontrolna do testowania filtrów.
    """
    try:
        ema_period = Phase3Config.EmaBounce.EMA_PERIOD
        if len(daily_df) < ema_period + 3: return None
        
        # Ta linia modyfikuje kopię (df_view), co jest bezpieczne
        daily_df['ema'] = calculate_ema(daily_df['close'], ema_period) 
        
        is_ema_rising = daily_df['ema'].iloc[-1] > daily_df['ema'].iloc[-2] > daily_df['ema'].iloc[-3]
        latest_candle = daily_df.iloc[-1]
        prev_candle = daily_df.iloc[-2]
        latest_ema = daily_df['ema'].iloc[-1]
        touched_ema = (prev_candle['low'] <= daily_df['ema'].iloc[-2] * 1.01) or \
                      (latest_candle['open'] <= latest_ema * 1.01)
        closed_above_ema = latest_candle['close'] > latest_ema
        is_bullish_candle = latest_candle['close'] > latest_candle['open']
        
        if is_ema_rising and touched_ema and closed_above_ema and is_bullish_candle:
             atr = calculate_atr(daily_df, 14).iloc[-1]
             if atr == 0: return None
             
             stop_loss = latest_candle['low'] - (Phase3Config.EmaBounce.ATR_MULTIPLIER_FOR_SL * atr)
             
             return {
                 "setup_type": "EMA_BOUNCE", # Nazwa bazowa
                 "entry_price": latest_candle['high'] + 0.01,
                 "stop_loss": stop_loss,
                 "atr_percent": (atr / latest_candle['close']) # Zwracamy ATR% do filtrowania
             }
        return None
    except Exception as e:
        logger.error(f"Error in _find_base_ema_bounce: {e}")
        return None
# ==================================================================


def _simulate_trades(session: Session, ticker: str, historical_data: pd.DataFrame, year: str):
    """
    Iteruje dzień po dniu przez historyczny DataFrame, szuka setupów
    i przekazuje je do rozwiązania.
    """
    logger.info(f"  [Backtest] Rozpoczynanie symulacji dla {ticker} (dni: {len(historical_data)})...")
    trades_found = 0
    
    # Obliczamy wskaźniki dla CAŁEGO historycznego DF tylko raz (bardzo wydajne)
    historical_data['rsi_14'] = calculate_rsi(historical_data['close'], period=14)
    historical_data['macd_line'], historical_data['signal_line'] = calculate_macd(historical_data['close'])

    # Zaczynamy od 50, aby mieć wystarczająco danych dla wskaźników (EMA, ATR)
    for i in range(50, len(historical_data)):
        # Tworzymy "widok" danych, który widziałby analityk danego dnia
        df_view = historical_data.iloc[i-50 : i].copy()
        
        # Pobieramy wskaźniki obliczone wcześniej dla *tego* dnia (indeks -1 w df_view)
        latest_indicators = df_view.iloc[-1]
        latest_rsi = latest_indicators['rsi_14']
        latest_macd = latest_indicators['macd_line']
        latest_signal = latest_indicators['signal_line']
        
        # --- TESTOWANIE STRATEGII EOD ---
        setups_to_test = []
        
        # ==================================================================
        # === ZMIANA (Strategy Battle Royale) ===
        # ==================================================================
        
        # === Test 1: EMA Bounce (Wszystkie warianty) ===
        # Używamy nowej, bazowej funkcji bez filtra ATR
        ema_setup = _find_base_ema_bounce(df_view) 
        
        if ema_setup:
            risk = ema_setup['entry_price'] - ema_setup['stop_loss']
            if risk > 0:
                rr_ratio = Phase3Config.EmaBounce.TARGET_RR_RATIO
                max_hold = Phase3Config.EmaBounce.MAX_HOLD_DAYS # <-- Sugestia AI #3
                
                # Przygotuj bazowy setup
                base_setup = {
                    "ticker": ticker,
                    "entry_price": ema_setup['entry_price'],
                    "stop_loss": ema_setup['stop_loss'],
                    "take_profit": ema_setup['entry_price'] + (rr_ratio * risk),
                    "max_hold_days": max_hold
                }
                
                # --- Wariant 1: Bazowa strategia (stara logika, dla porównania) ---
                # Zapisujemy ją, aby mieć punkt odniesienia
                setups_to_test.append({
                    **base_setup, 
                    "setup_type": "EMA_BOUNCE" 
                })

                # --- Wariant 2: Sugestia AI (Filtr ATR > 20%) ---
                if ema_setup['atr_percent'] > Phase3Config.EmaBounce.MIN_ATR_PERCENT_FILTER:
                    setups_to_test.append({
                        **base_setup,
                        "setup_type": "EMA_ATR_FILTER" # NOWA NAZWA
                    })
                
                # --- Wariant 3: Nasz Pomysł (Filtr RSI < 40) ---
                if latest_rsi < 40:
                    setups_to_test.append({
                        **base_setup,
                        "setup_type": "EMA_RSI_40", # NOWA NAZWA
                    })
                
                # --- Wariant 4: Nasz Pomysł (Filtr MACD Cross) ---
                if latest_macd > latest_signal:
                    setups_to_test.append({
                        **base_setup,
                        "setup_type": "EMA_MACD_CROSS", # NOWA NAZWA
                    })

        # === Test 2: Breakout (bez zmian) ===
        breakout_setup = _find_breakout_setup(df_view)
        if breakout_setup:
            risk = breakout_setup['entry_price'] - breakout_setup['stop_loss']
            if risk > 0:
                rr_ratio = Phase3Config.Breakout.TARGET_RR_RATIO # <-- Sugestia AI #1
                max_hold = Phase3Config.Breakout.MAX_HOLD_DAYS
                
                setups_to_test.append({
                    "ticker": ticker,
                    "setup_type": "BREAKOUT",
                    "entry_price": breakout_setup['entry_price'],
                    "stop_loss": breakout_setup['stop_loss'],
                    "take_profit": breakout_setup['entry_price'] + (rr_ratio * risk),
                    "max_hold_days": max_hold
                })
            
        # ==================================================================
        # === KONIEC ZMIAN ===
        # ==================================================================


        # Przetwórz znalezione setupy
        for setup_full in setups_to_test:
            
            log_date = historical_data.index[i].date()
            logger.info(f"    [Backtest] ZNALEZIONO SETUP: {ticker} | {log_date} | {setup_full['setup_type']}")
            
            # Rozwiąż transakcję (sprawdź przyszłość)
            trade_result = _resolve_trade(
                historical_data, 
                i, 
                setup_full, 
                setup_full['max_hold_days'], # Przekaż specyficzny czas trzymania
                year
            )
            
            if trade_result:
                session.add(trade_result)
                trades_found += 1

    # Zapisz wszystkie znalezione transakcje dla tego tickera
    if trades_found > 0:
        session.commit()
    logger.info(f"  [Backtest] Symulacja dla {ticker} zakończona. Znaleziono i zapisano {trades_found} transakcji.")


def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny dla
    zdefiniowanego okresu i listy tickerów.
    """
    
    try:
        if not (year.isdigit() and len(year) == 4):
            raise ValueError(f"Otrzymano nieprawidłowy rok: {year}")
        
        current_year = datetime.now(timezone.utc).year
        if int(year) > current_year:
             raise ValueError(f"Nie można testować przyszłości: {year}")
        
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
    except Exception as e:
        logger.error(f"[Backtest] Błąd walidacji roku: {e}", exc_info=True)
        append_scan_log(session, f"[Backtest] BŁĄD: Nieprawidłowy format roku: {year}")
        return
        
    log_msg = f"BACKTEST HISTORYCZNY: Rozpoczynanie testu dla roku '{year}' ({start_date} do {end_date})"
    
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # Wyczyść stare wyniki dla tego okresu testowego, aby uniknąć duplikatów
    try:
        # ZMIANA: Usuwamy teraz WSZYSTKIE strategie EMA, aby móc je przetestować od nowa
        period_prefix_base = f"BACKTEST_{year}_"
        prefixes_to_delete = [
            f"{period_prefix_base}EMA_BOUNCE",
            f"{period_prefix_base}EMA_RSI_40",
            f"{period_prefix_base}EMA_MACD_CROSS",
            f"{period_prefix_base}EMA_ATR_FILTER",
            f"{period_prefix_base}BREAKOUT" # Czyścimy też breakout
        ]
        
        logger.info(f"Czyszczenie starych wyników dla okresu: {year}...")
        
        # Używamy pętli do czyszczenia (bezpieczniejsze niż skomplikowany LIKE)
        for prefix in prefixes_to_delete:
            session.execute(
                text("DELETE FROM virtual_trades WHERE setup_type = :prefix"),
                {'prefix': prefix}
            )
        
        session.commit()
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()

    try:
        tickers_to_test_rows = session.execute(text("SELECT DISTINCT ticker FROM trading_signals ORDER BY ticker")).fetchall()
        tickers_to_test = [row[0] for row in tickers_to_test_rows]
        
        if not tickers_to_test:
            log_msg = f"[Backtest] BŁĄD: Brak tickerów na liście Fazy 3 (trading_signals) do przetestowania. Uruchom najpierw skanowanie EOD."
            logger.error(log_msg)
            append_scan_log(session, log_msg)
            return

        log_msg = f"[Backtest] Znaleziono {len(tickers_to_test)} tickerów z Fazy 3 do przetestowania historycznego (np. {tickers_to_test[0]}, {tickers_to_test[1]}, ...)."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów z Fazy 3: {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return


    for ticker in tickers_to_test:
        try:
            log_msg = f"[Backtest] Pobieranie pełnych danych historycznych dla {ticker}..."
            logger.info(log_msg)
            append_scan_log(session, log_msg)
            
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
            
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                logger.warning(f"[Backtest] Brak danych historycznych dla {ticker}. Pomijanie.")
                continue

            full_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            full_df = standardize_df_columns(full_df) # To sortuje rosnąco
            
            full_df.index = pd.to_datetime(full_df.index)
            
            historical_data_slice = full_df.loc[start_date:end_date]
            
            if historical_data_slice.empty or len(historical_data_slice) < 50:
                logger.warning(f"[Backtest] Niewystarczająca ilość danych dla {ticker} w okresie {year}. Pomijanie.")
                continue

            # Krok 4: Uruchom symulator
            _simulate_trades(session, ticker, historical_data_slice, year)

        except Exception as e:
            logger.error(f"[Backtest] Błąd krytyczny podczas przetwarzania {ticker}: {e}", exc_info=True)
            session.rollback()
            append_scan_log(session, f"BŁĄD Backtestu dla {ticker}: {e}")

    log_msg = f"BACKTEST HISTORYCZNY: Zakończono test dla roku '{year}'."
    logger.info(log_msg)
    append_scan_log(session, log_msg)
