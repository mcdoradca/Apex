import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from typing import List, Dict, Any
# KROK 1: Rozszerzamy importy
import numpy as np
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    get_current_NY_datetime,
    append_scan_log,
    # === POPRAWKA: Dodanie brakującego importu ===
    update_scan_progress,
    # ============================================
    calculate_rsi,
    calculate_macd,
    calculate_atr,
    # === NOWE IMPORTY DLA AQM ===
    calculate_obv,
    calculate_ad
)
# KROK 1 (REWOLUCJA): Usunięto importy starych strategii
from .. import models
from ..config import Phase3Config

logger = logging.getLogger(__name__)

# ==================================================================
# === Środowisko Backtestingu ===
# ==================================================================

# Horyzont czasowy dla strategii
MAX_HOLD_DAYS_DEFAULT = 7

# Definicje progów AQM (z PDF str. 18)
AQM_THRESHOLDS = {
    'bull': 0.65,
    'volatile': 0.75,
    'bear': 0.85
}
# Definicje strategii AQM (z PDF str. 19)
AQM_STRATEGY_PARAMS = {
    'stop_loss_percent': 0.05, # Używamy 5% (środek z 3-5%)
    'target_1_percent': 0.08,  # Target 1 (8%)
    'target_2_percent': 0.12,  # Target 2 (12%)
    'max_hold_days': 7         # 3-7 dni
}


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
# === KROK 1 (REWOLUCJA): Usunięto funkcję _find_base_ema_bounce ===
# ==================================================================


# ==================================================================
# === NOWE FUNKCJE POMOCNICZE DLA BACKTESTU AQM ===
# ==================================================================

# Słownik cache dla danych makro (VIX, SPY) i sektorów
_backtest_cache = {
    "vix_data": None,
    "spy_data": None,
    "sector_map": {}
}

def _get_sector_for_ticker(session: Session, ticker: str) -> str:
    """Pobiera sektor dla tickera z bazy danych (z cache)."""
    if ticker not in _backtest_cache["sector_map"]:
        try:
            sector = session.execute(
                text("SELECT sector FROM companies WHERE ticker = :ticker"),
                {'ticker': ticker}
            ).scalar()
            _backtest_cache["sector_map"][ticker] = sector or "N/A"
        except Exception as e:
            logger.error(f"Nie udało się pobrać sektora dla {ticker}: {e}")
            _backtest_cache["sector_map"][ticker] = "N/A"
            
    return _backtest_cache["sector_map"][ticker]

def _detect_market_regime(vix_df: pd.DataFrame, spy_df: pd.DataFrame, current_date_str: str) -> str:
    """Wykrywa reżim rynkowy (z PDF str. 10 i 16) na podstawie danych VIX i SPY."""
    try:
        # Znajdź najbliższe dane dla VIX i SPY (nie mogą być z przyszłości)
        vix_row = vix_df[vix_df.index <= current_date_str].iloc[-1]
        spy_row = spy_df[spy_df.index <= current_date_str].iloc[-1]
        
        vix_price = vix_row['close']
        spy_price = spy_row['close']
        spy_sma_50 = spy_row['ema_50'] # Używamy EMA(50) jako przybliżenie SMA(50)
        spy_sma_200 = spy_row['ema_200'] # Używamy EMA(200) jako przybliżenie SMA(200)

        # Logika z PDF str. 16
        if vix_price < 18 and spy_price > spy_sma_200:
            return 'bull'
        elif vix_price > 25 or spy_price < spy_sma_50:
            return 'bear'
        else:
            return 'volatile'
            
    except IndexError:
        # Zdarza się na początku danych historycznych, gdy nie ma jeszcze danych VIX/SPY
        return 'volatile' # Bezpieczny domyślny
    except Exception as e:
        logger.error(f"Błąd podczas wykrywania reżimu dla {current_date_str}: {e}")
        return 'volatile'

def _calculate_aqm_score(
    df_view: pd.DataFrame, 
    weekly_df_view: pd.DataFrame, 
    sector: str, 
    market_regime: str
) -> (float, dict):
    """
    Oblicza AQM Score na podstawie danych EOD (zgodnie z logiką z PDF str. 13-17).
    Zwraca: (final_score, components_dict)
    """
    try:
        # --- 1. Quantum Prime Score (QPS) - Waga 40% ---
        # Sprawdzamy tylko dane Daily i Weekly (jak w backteście)
        
        # A. Dane Dzienne (Daily)
        d = df_view.iloc[-1] # Ostatni dzień
        trend_d = 0.0
        if d['close'] > d['ema_20']: trend_d += 0.25
        if d['close'] > d['ema_50']: trend_d += 0.25
        if d['ema_20'] > d['ema_50']: trend_d += 0.25
        if d['ema_50'] > d['ema_200']: trend_d += 0.25 # Razem 1.0
        
        momentum_d = 0.0
        if d['rsi_14'] > 50: momentum_d += 0.5
        if d['macd_line'] > d['signal_line']: momentum_d += 0.5 # Razem 1.0
        
        volatility_d = 0.3 # Domyślnie niska ocena
        if d['close'] > 0:
            atr_percent = d['atr_14'] / d['close']
            if atr_percent < 0.05: volatility_d = 1.0
            elif atr_percent < 0.08: volatility_d = 0.7 # Razem 1.0
        
        score_daily = (trend_d * 0.5) + (momentum_d * 0.3) + (volatility_d * 0.2)
        
        # B. Dane Tygodniowe (Weekly)
        w = weekly_df_view.iloc[-1]
        trend_w = 0.0
        if w['close'] > w['ema_20']: trend_w += 0.25
        if w['close'] > w['ema_50']: trend_w += 0.25
        if w['ema_20'] > w['ema_50']: trend_w += 0.25
        # (Brak EMA200 dla Weekly w tym widoku)
        
        momentum_w = 0.0
        if w['rsi_14'] > 50: momentum_w += 0.5
        if w['macd_line'] > w['signal_line']: momentum_w += 0.5
        
        score_weekly = (trend_w * 0.7) + (momentum_w * 0.3)
        
        # Używamy średniej, jak sugeruje logika "harmonic mean" (choć implementujemy zwykłą średnią)
        qps_score = (score_daily + score_weekly) / 2.0

        # --- 2. Volume Entropy Score (VES) - Waga 30% ---
        # Zgodnie z PDF str. 14-15, wagi (40/30/30) są *wewnątrz* komponentów
        
        obv_score = 0.0
        if d['obv'] > d['obv_20_ma']: obv_score += 0.4
        if d['obv'] > df_view.iloc[-2]['obv']: obv_score += 0.3 # Trend rosnący
        if d['obv_20_ma'] > d['obv_50_ma']: obv_score += 0.3 # Razem 1.0
        
        volume_score = 0.1 # Domyślnie
        if d['volume_20_ma'] > 0:
            volume_ratio = d['volume'] / d['volume_20_ma']
            if volume_ratio > 1.2: volume_score = 1.0 # Zmieniono na 1.0 (z 0.3)
            elif volume_ratio > 0.8: volume_score = 0.7 # Zmieniono na 0.7 (z 0.2)
        
        ad_score = 0.0
        if d['ad_line'] > d['ad_line_20_ma']: ad_score += 0.5 # Zmieniono na 0.5 (z 0.3)
        if d['ad_line'] > df_view.iloc[-2]['ad_line']: ad_score += 0.5 # Zmieniono na 0.5 (z 0.2)
        
        # Stosujemy wagi 40/30/30 (z PDF str. 14) do wyników (0-1)
        ves_score = (obv_score * 0.4) + (volume_score * 0.3) + (ad_score * 0.3)

        # --- 3. Market Regime Score (MRS) - Waga 20% ---
        # ==================================================================
        # === POPRAWKA LOGIKI "PUŁAPKI 2" (Bessa) ===
        # Zastępujemy błędną logikę PDF (kupowanie Utilities)
        # prawdziwym "hamulcem", który blokuje handel w bessie.
        # ==================================================================
        mrs_score = 0.0 # Domyślna niska ocena (kara)
        if market_regime == 'bull':
            if sector in ['Technology', 'Communication Services', 'Consumer Cyclical']:
                mrs_score = 0.7 # Nagroda za zgodność z hossą
            else:
                mrs_score = 0.3 # Mniejsza kara, jeśli spółka nie jest z tych sektorów
        elif market_regime == 'volatile':
            mrs_score = 0.5 # Neutralna ocena
        elif market_regime == 'bear':
            mrs_score = 0.0 # HAMULEC. Nie handlujemy long w bessie.
        # ==================================================================
        
        # --- 4. Temporal Coherence Score (TCS) - Waga 10% ---
        # Ignorujemy w backteście (zbyt skomplikowane do symulacji)
        tcs_score = 1.0 
        
        # ==================================================================
        # === POPRAWKA LOGIKI "PUŁAPKI 1" (Matematyka) ===
        # Używamy SUMY WAŻONEJ (40/30/20/10) zamiast iloczynu.
        # ==================================================================
        final_aqm_score = (qps_score * 0.40) + (ves_score * 0.30) + (mrs_score * 0.20) + (tcs_score * 0.10)
        # ==================================================================
        
        components = {
            "QPS": qps_score, "VES": ves_score, "MRS": mrs_score, "TCS": tcs_score
        }
        
        return final_aqm_score, components

    except Exception as e:
        # logger.error(f"Błąd obliczania AQM: {e}", exc_info=False) # Zbyt głośne
        return 0.0, {}

# ==================================================================


def _simulate_trades(session: Session, ticker: str, historical_data: pd.DataFrame, weekly_data: pd.DataFrame, year: str):
    """
    Iteruje dzień po dniu przez historyczny DataFrame, szuka setupów
    (STARYCH ORAZ NOWEGO AQM) i przekazuje je do rozwiązania.
    """
    # ZMIANA: Przeniesiono logowanie do pętli wyższego poziomu
    # logger.info(f"  [Backtest] Rozpoczynanie symulacji dla {ticker} (dni: {len(historical_data)})...")
    trades_found = 0
    
    # Pobierz sektor dla tego tickera (raz)
    ticker_sector = _get_sector_for_ticker(session, ticker)
    
    # Obliczamy wskaźniki dla CAŁEGO historycznego DF tylko raz (bardzo wydajne)
    historical_data['rsi_14'] = calculate_rsi(historical_data['close'], period=14)
    historical_data['macd_line'], historical_data['signal_line'] = calculate_macd(historical_data['close'])
    historical_data['atr_14'] = calculate_atr(historical_data, period=14)
    # Wskaźniki EMA
    historical_data['ema_20'] = calculate_ema(historical_data['close'], period=20)
    historical_data['ema_50'] = calculate_ema(historical_data['close'], period=50)
    historical_data['ema_200'] = calculate_ema(historical_data['close'], period=200)
    # Wskaźniki wolumenu
    historical_data['obv'] = calculate_obv(historical_data)
    historical_data['obv_20_ma'] = historical_data['obv'].rolling(window=20).mean()
    historical_data['obv_50_ma'] = historical_data['obv'].rolling(window=50).mean()
    historical_data['volume_20_ma'] = historical_data['volume'].rolling(window=20).mean()
    historical_data['ad_line'] = calculate_ad(historical_data)
    historical_data['ad_line_20_ma'] = historical_data['ad_line'].rolling(window=20).mean()
    
    # Przygotuj dane tygodniowe (potrzebne do AQM)
    weekly_data['rsi_14'] = calculate_rsi(weekly_data['close'], period=14)
    weekly_data['macd_line'], weekly_data['signal_line'] = calculate_macd(weekly_data['close'])
    weekly_data['ema_20'] = calculate_ema(weekly_data['close'], period=20)
    weekly_data['ema_50'] = calculate_ema(weekly_data['close'], period=50)


    # Zaczynamy od 200, aby mieć wystarczająco danych dla EMA 200
    for i in range(200, len(historical_data)):
        
        current_date = historical_data.index[i]
        current_date_str = current_date.strftime('%Y-%m-%d')

        # Tworzymy "widok" danych, który widziałby analityk danego dnia
        # (Obejmuje dane do dnia 'i' włącznie)
        df_view = historical_data.iloc[i-200 : i+1].copy()
        
        # Pobierz widok danych tygodniowych (do bieżącej daty)
        weekly_df_view = weekly_data[weekly_data.index <= current_date_str].iloc[-50:] # Ostatnie 50 tygodni
        if len(weekly_df_view) < 50: # Potrzebujemy danych dla wskaźników tygodniowych
            continue

        # --- TESTOWANIE STRATEGII EOD ---
        setups_to_test = []
        
        # ==================================================================
        # === KROK 1 (REWOLUCJA): Usunięcie starych strategii ===
        # Bloki testujące 'EMA_BOUNCE' i 'BREAKOUT' zostały usunięte.
        # ==================================================================
        
        
        # ==================================================================
        # === TEST TYLKO NOWEJ STRATEGII AQM ===
        # ==================================================================
        
        # 1. Wykryj reżim rynkowy (raz na dzień symulacji)
        market_regime = _detect_market_regime(_backtest_cache["vix_data"], _backtest_cache["spy_data"], current_date_str)
        
        # 2. Oblicz AQM Score
        aqm_score, components = _calculate_aqm_score(df_view, weekly_df_view, ticker_sector, market_regime)
        
        # 3. Sprawdź, czy przekracza próg dla danego reżimu
        threshold = AQM_THRESHOLDS.get(market_regime, 0.85) # Pobierz próg
        
        if aqm_score > threshold:
            latest_candle = df_view.iloc[-1]
            entry_price = latest_candle['close'] # Wejście po cenie zamknięcia (PDF str. 19)
            
            # Użyj strategii SL/TP z PDF
            sl_price = entry_price * (1 - AQM_STRATEGY_PARAMS['stop_loss_percent'])
            tp_price = entry_price * (1 + AQM_STRATEGY_PARAMS['target_1_percent']) # Celujemy w Target 1
            
            setups_to_test.append({
                "ticker": ticker,
                # Zapisujemy reżim w typie, aby móc je filtrować w raporcie
                "setup_type": f"AQM_SCORE_{market_regime.upper()}", 
                "entry_price": entry_price,
                "stop_loss": sl_price,
                "take_profit": tp_price,
                "max_hold_days": AQM_STRATEGY_PARAMS['max_hold_days']
            })

        # ==================================================================
        # === Koniec Testu AQM ===
        # ==================================================================


        # Przetwórz wszystkie znalezione setupy (teraz tylko AQM)
        for setup_full in setups_to_test:
            
            log_date = historical_data.index[i].date()
            # logger.info(f"    [Backtest] ZNALEZIONO SETUP: {ticker} | {log_date} | {setup_full['setup_type']}")
            
            # Rozwiąż transakcję (sprawdź przyszłość)
            trade_result = _resolve_trade(
                historical_data, 
                i, 
                setup_full, 
                setup_full['max_hold_days'],
                year
            )
            
            if trade_result:
                session.add(trade_result)
                trades_found += 1

    # Zapisz wszystkie znalezione transakcje dla tego tickera
    if trades_found > 0:
        session.commit()
    # ZMIANA: Usunięto logowanie z tego miejsca, aby było mniej "głośne"
    # logger.info(f"  [Backtest] Symulacja dla {ticker} zakończona. Znaleziono i zapisano {trades_found} transakcji.")


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

    # ==================================================================
    # === KROK 1 (REWOLUCJA): Rozszerzone Czyszczenie Bazy Danych ===
    # Usuwamy WSZYSTKIE stare strategie, aby baza była czysta
    # dla nowych wyników AQM.
    # ==================================================================
    try:
        # Definiujemy wzorzec LIKE (np. 'BACKTEST_2022_%')
        like_pattern = f"BACKTEST_{year}_%"
        
        logger.info(f"Czyszczenie WSZYSTKICH starych wyników dla wzorca: {like_pattern}...")
        
        # Wykonujemy jedno, globalne polecenie DELETE
        delete_stmt = text("DELETE FROM virtual_trades WHERE setup_type LIKE :pattern")
        result = session.execute(delete_stmt, {'pattern': like_pattern})
        
        session.commit()
        logger.info(f"Pomyślnie usunięto {result.rowcount} starych wpisów backtestu dla roku {year}.")
        
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()
    # ==================================================================
    # === KONIEC POPRAWKI CZYSZCZENIA ===
    # ==================================================================

    try:
        # ==================================================================
        # === ZMIANA KRYTYCZNA (NA TWOJE ŻĄDANIE) ===
        # Zmieniamy źródło danych z 'companies' (4133 spółki)
        # na 'phase1_candidates', aby drastycznie przyspieszyć test.
        # ==================================================================
        log_msg_tickers = "[Backtest] Pobieranie listy tickerów ze skanera 'phase1_candidates'..."
        logger.info(log_msg_tickers)
        append_scan_log(session, log_msg_tickers)
        
        # Używamy DISTINCT, aby pobrać unikalne tickery, jeśli Faza 1 działała wielokrotnie
        tickers_to_test_rows = session.execute(text("SELECT DISTINCT ticker FROM phase1_candidates ORDER BY ticker")).fetchall()
        tickers_to_test = [row[0] for row in tickers_to_test_rows]
        
        if not tickers_to_test:
            log_msg = f"[Backtest] BŁĄD: Tabela 'phase1_candidates' jest pusta. Uruchom najpierw skaner Fazy 1 (przycisk 'Start')."
            logger.error(log_msg)
            append_scan_log(session, log_msg)
            return

        log_msg = f"[Backtest] Znaleziono {len(tickers_to_test)} tickerów w 'phase1_candidates' do przetestowania historycznego (np. {tickers_to_test[0]}, ...)."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów z 'phase1_candidates': {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # ==================================================================
    # === NOWY KROK: Pobierz dane makro (VIX, SPY) raz ===
    # ==================================================================
    try:
        # === POPRAWKA: Zmieniamy 'VIX' na 'VXX' ===
        logger.info("[Backtest] Pobieranie danych makro (VXX, SPY) na potrzeby reżimu rynkowego...")
        
        # Pobieramy dane VXX (ETF śledzący VIX)
        vix_raw = api_client.get_daily_adjusted('VXX', outputsize='full')
        vix_df = pd.DataFrame.from_dict(vix_raw['Time Series (Daily)'], orient='index')
        vix_df = standardize_df_columns(vix_df)
        vix_df.index = pd.to_datetime(vix_df.index)
        _backtest_cache["vix_data"] = vix_df
        
        # Pobieramy dane SPY
        spy_raw = api_client.get_daily_adjusted('SPY', outputsize='full')
        spy_df = pd.DataFrame.from_dict(spy_raw['Time Series (Daily)'], orient='index')
        spy_df = standardize_df_columns(spy_df)
        spy_df.index = pd.to_datetime(spy_df.index)
        # Oblicz EMA dla SPY (potrzebne do detekcji reżimu)
        spy_df['ema_50'] = calculate_ema(spy_df['close'], period=50)
        spy_df['ema_200'] = calculate_ema(spy_df['close'], period=200)
        _backtest_cache["spy_data"] = spy_df
        
        logger.info("[Backtest] Dane makro VXX i SPY pomyślnie załadowane i zapisane w cache.")

    except Exception as e:
        log_msg = f"[Backtest] BŁĄD KRYTYCZNY: Nie można pobrać danych makro VXX/SPY: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return
    # ==================================================================


    total_count = len(tickers_to_test) # Całkowita liczba tickerów
    
    for i, ticker in enumerate(tickers_to_test):
        
        processed_count = i + 1 # Aktualny numer (zaczyna się od 1)

        try:
            log_msg = f"[Backtest] Przetwarzanie {ticker} ({processed_count}/{total_count})..."
            
            # === POPRAWKA UI ===
            # Aktualizuj logi i postęp UI co 10 tickerów (lub co 1, jeśli lista jest krótka)
            if processed_count % 10 == 0 or total_count < 50 or processed_count == total_count:
                logger.info(log_msg)
                append_scan_log(session, log_msg)
                # Wywołanie funkcji aktualizującej UI
                update_scan_progress(session, processed_count, total_count)
            # ===================
            
            # === ZMIANA: Pobieramy DANE DZIENNE i TYGODNIOWE ===
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
            weekly_data_raw = api_client.get_time_series_weekly(ticker, outputsize='full') # NOWE
            
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
               not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw:
                # logger.warning(f"[Backtest] Brak danych historycznych (Dziennych lub Tygodniowych) dla {ticker}. Pomijanie.")
                continue

            # Przetwarzanie danych DZIENNYCH
            full_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            full_df = standardize_df_columns(full_df) # To sortuje rosnąco
            full_df.index = pd.to_datetime(full_df.index)
            
            # Przetwarzanie danych TYGODNIOWYCH
            weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
            weekly_df = standardize_df_columns(weekly_df)
            weekly_df.index = pd.to_datetime(weekly_df.index)
            
            # Krojenie obu DF do okresu testowego
            historical_data_slice = full_df.loc[start_date:end_date]
            # Dla danych tygodniowych bierzemy szerszy zakres, aby mieć historię dla wskaźników
            weekly_data_slice = weekly_df.loc[:end_date] 
            
            if historical_data_slice.empty or len(historical_data_slice) < 200 or \
               weekly_data_slice.empty or len(weekly_data_slice) < 50:
                # logger.warning(f"[Backtest] Niewystarczająca ilość danych dla {ticker} w okresie {year} (D: {len(historical_data_slice)}, W: {len(weekly_data_slice)}). Pomijanie.")
                continue

            # Krok 4: Uruchom symulator (przekazujemy oba DataFrame)
            _simulate_trades(session, ticker, historical_data_slice, weekly_data_slice, year)

        except Exception as e:
            logger.error(f"[Backtest] Błąd krytyczny podczas przetwarzania {ticker}: {e}", exc_info=True)
            session.rollback()
            append_scan_log(session, f"BŁĄD Backtestu dla {ticker}: {e}")

    # === POPRAWKA UI ===
    # Ustaw ostateczny postęp na 100% po zakończeniu pętli
    update_scan_progress(session, total_count, total_count)
    # ===================

    log_msg = f"BACKTEST HISTORYCZNY: Zakończono test dla roku '{year}'."
    logger.info(log_msg)
    append_scan_log(session, log_msg)
