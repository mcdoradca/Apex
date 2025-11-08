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
    append_scan_log
)
# Importujemy nasze istniejące strategie EOD do przetestowania
from .phase3_sniper import _find_ema_bounce_setup, _find_breakout_setup, _find_impulse_and_fib_zone
from .. import models
from ..config import Phase3Config

logger = logging.getLogger(__name__)

# ==================================================================
# === Środowisko Backtestingu ===
# ==================================================================

# ==================================================================
# ZMIANA (Dynamiczny Rok): Usunięcie statycznych okresów
# ==================================================================
# BACKTEST_PERIODS = {
#     "TRUMP_2019": ('2019-01-01', '2019-12-31'),
#     "BIDEN_2022": ('2022-01-01', '2022-12-31'),
# }
# ==================================================================

# Horyzont czasowy dla strategii (zgodnie z naszym celem 7 dni)
MAX_HOLD_DAYS = 7

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
            # Sprawdzenie 3: Jeśli pętla się zakończyła (minęło 7 dni), zamykamy po cenie zamknięcia ostatniego dnia
            candle = historical_data.iloc[entry_index + max_hold_days]
            close_price = candle['close']
            status = 'CLOSED_EXPIRED'
            logger.info(f"    [Backtest] ZAMKNIĘCIE (Wygasło): {candle.name.date()} @ {close_price:.2f}")

        # Oblicz P/L %
        p_l_percent = ((close_price - entry_price) / entry_price) * 100
        
        # ==================================================================
        # === POPRAWKA BŁĘDU (Problem "np.float64") ===
        # Konwertujemy wszystkie liczby z (potencjalnie) numpy.float64
        # na natywne typy Pythona (float) przed wysłaniem do bazy.
        # ==================================================================
        trade = models.VirtualTrade(
            ticker=setup['ticker'],
            status=status,
            # ZMIANA (Dynamiczny Rok): Tworzymy dynamiczny setup_type
            setup_type=f"BACKTEST_{year}_{setup['setup_type']}",
            entry_price=float(entry_price),
            stop_loss=float(stop_loss),
            take_profit=float(take_profit),
            open_date=historical_data.index[entry_index].to_pydatetime(), # Data znalezienia setupu
            close_date=candle.name.to_pydatetime(), # Data zamknięcia
            close_price=float(close_price),
            final_profit_loss_percent=float(p_l_percent)
        )
        # ==================================================================
        
        return trade

    except Exception as e:
        logger.error(f"[Backtest] Błąd podczas rozwiązywania transakcji: {e}", exc_info=True)
        return None


def _simulate_trades(session: Session, ticker: str, historical_data: pd.DataFrame, year: str):
    """
    Iteruje dzień po dniu przez historyczny DataFrame, szuka setupów
    i przekazuje je do rozwiązania.
    """
    logger.info(f"  [Backtest] Rozpoczynanie symulacji dla {ticker} (dni: {len(historical_data)})...")
    trades_found = 0
    
    # Zaczynamy od 50, aby mieć wystarczająco danych dla wskaźników (EMA, ATR)
    for i in range(50, len(historical_data)):
        # Tworzymy "widok" danych, który widziałby analityk danego dnia
        # (czyli wszystkie dane *do* tego dnia)
        
        # ==================================================================
        # NAPRAWA (Problem 2): Używamy .copy(), aby uniknąć SettingWithCopyWarning
        # ==================================================================
        df_view = historical_data.iloc[i-50 : i].copy()
        # ==================================================================
        
        # --- TESTOWANIE STRATEGII EOD ---
        # (Na razie pomijamy Fib H1, ponieważ wymagałoby to pobierania
        # historycznych danych intraday miesiąc po miesiącu, co jest
        # znacznie bardziej złożone i wolniejsze)
        
        setups_to_test = []
        
        # Test 1: EMA Bounce
        ema_setup = _find_ema_bounce_setup(df_view)
        if ema_setup:
            setups_to_test.append(ema_setup)

        # Test 2: Breakout
        breakout_setup = _find_breakout_setup(df_view)
        if breakout_setup:
            setups_to_test.append(breakout_setup)
            
        # ... W przyszłości możemy tu dodać `_find_impulse_and_fib_zone`
        # i specjalną logikę do pobierania H1 dla tego dnia ...

        # Przetwórz znalezione setupy
        for setup_base in setups_to_test:
            # Uzupełnij setup o dane, których _find_... nie zwraca
            # (Musimy odtworzyć logikę z `find_end_of_day_setup`)
            risk = setup_base['entry_price'] - setup_base['stop_loss']
            if risk <= 0:
                continue # Błędny setup
            
            setup_full = {
                "ticker": ticker,
                "setup_type": setup_base['setup_type'],
                "entry_price": setup_base['entry_price'],
                "stop_loss": setup_base['stop_loss'],
                "take_profit": setup_base['entry_price'] + (Phase3Config.TARGET_RR_RATIO * risk)
            }
            
            # ==================================================================
            # NAPRAWA (Problem 1): Używamy .name.date() zamiast .date()
            # Indeks to teraz obiekt DatetimeIndex, więc [i] da nam Timestamp
            # ==================================================================
            log_date = historical_data.index[i].date()
            logger.info(f"    [Backtest] ZNALEZIONO SETUP: {ticker} | {log_date} | {setup_full['setup_type']}")
            # ==================================================================
            
            # Rozwiąż transakcję (sprawdź przyszłość)
            trade_result = _resolve_trade(historical_data, i, setup_full, MAX_HOLD_DAYS, year)
            
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
    
    # ==================================================================
    # ZMIANA (Dynamiczny Rok): Walidacja i dynamiczne ustawianie dat
    # ==================================================================
    try:
        # Walidacja, czy 'year' to 4-cyfrowa liczba
        if not (year.isdigit() and len(year) == 4):
            raise ValueError(f"Otrzymano nieprawidłowy rok: {year}")
        
        # Sprawdzenie, czy rok nie jest z przyszłości
        current_year = datetime.now(timezone.utc).year
        if int(year) > current_year:
             raise ValueError(f"Nie można testować przyszłości: {year}")
        
        # Ustawiamy dynamicznie daty
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
    except Exception as e:
        logger.error(f"[Backtest] Błąd walidacji roku: {e}", exc_info=True)
        append_scan_log(session, f"[Backtest] BŁĄD: Nieprawidłowy format roku: {year}")
        return
        
    log_msg = f"BACKTEST HISTORYCZNY: Rozpoczynanie testu dla roku '{year}' ({start_date} do {end_date})"
    # ==================================================================
    
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # Wyczyść stare wyniki dla tego okresu testowego, aby uniknąć duplikatów
    try:
        # ZMIANA (Dynamiczny Rok): Używamy dynamicznego prefiksu
        period_prefix = f"BACKTEST_{year}_%"
        logger.info(f"Czyszczenie starych wyników dla okresu: {period_prefix}")
        session.execute(
            text("DELETE FROM virtual_trades WHERE setup_type LIKE :period"),
            {'period': period_prefix}
        )
        session.commit()
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()

    # ==================================================================
    # ZMIANA (Logika Doboru Spółek): Pobieramy listę tickerów z Fazy 3 (Sygnały)
    # ==================================================================
    try:
        # Pobieramy unikalne tickery z listy sygnałów (Twoje 33 spółki)
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
    # ==================================================================


    for ticker in tickers_to_test:
        try:
            log_msg = f"[Backtest] Pobieranie pełnych danych historycznych dla {ticker}..."
            logger.info(log_msg)
            append_scan_log(session, log_msg)
            
            # Krok 1: Pobierz PEŁNE dane EOD (20+ lat) - 1 zapytanie API
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
            
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                logger.warning(f"[Backtest] Brak danych historycznych dla {ticker}. Pomijanie.")
                continue

            # Krok 2: Przetwórz i posortuj dane
            full_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            full_df = standardize_df_columns(full_df) # To sortuje rosnąco
            
            # ==================================================================
            # NAPRAWA (Problem 1): Konwertujemy indeks na obiekty Datetime
            # ==================================================================
            full_df.index = pd.to_datetime(full_df.index)
            # ==================================================================
            
            # Krok 3: Wytnij tylko interesujący nas okres
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
