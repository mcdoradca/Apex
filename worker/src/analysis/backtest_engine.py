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
    calculate_rsi,
    calculate_macd,
    calculate_atr,
    # === NOWE IMPORTY DLA AQM ===
    calculate_obv,
    calculate_ad
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
        # (Logika z PDF str. 16)
        mrs_score = 0.3 # Domyślna niska ocena
        if market_regime == 'bull':
            if sector in ['Technology', 'Communication Services', 'Consumer Cyclical']:
                mrs_score = 0.7
        elif market_regime == 'bear':
            if sector in ['Utilities', 'Consumer Defensive', 'Healthcare']:
                mrs_score = 0.7
        else: # volatile
            mrs_score = 0.5 # Neutralna
        
        # --- 4. Temporal Coherence Score (TCS) - Waga 10% ---
        # Ignorujemy w backteście (zbyt skomplikowane do symulacji)
        tcs_score = 1.0 
        
        # --- Finalny Score (z PDF str 18 - mnożenie) ---
        final_aqm_score = qps_score * ves_score * mrs_score * tcs_score
        
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
    logger.info(f"  [Backtest] Rozpoczynanie symulacji dla {ticker} (dni: {len(historical_data)})...")
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
        # === TEST 1 i 2: Stare strategie (dla porównania) ===
        # ==================================================================
        
        # Test 1: EMA Bounce (Warianty)
        ema_setup = _find_base_ema_bounce(df_view) 
        if ema_setup:
            risk = ema_setup['entry_price'] - ema_setup['stop_loss']
            if risk > 0:
                base_setup = {
                    "ticker": ticker,
                    "entry_price": ema_setup['entry_price'],
                    "stop_loss": ema_setup['stop_loss'],
                    "take_profit": ema_setup['entry_price'] + (Phase3Config.EmaBounce.TARGET_RR_RATIO * risk),
                    "max_hold_days": Phase3Config.EmaBounce.MAX_HOLD_DAYS
                }
                setups_to_test.append({ **base_setup, "setup_type": "EMA_BOUNCE" })

        # Test 2: Breakout
        breakout_setup = _find_breakout_setup(df_view)
        if breakout_setup:
            risk = breakout_setup['entry_price'] - breakout_setup['stop_loss']
            if risk > 0:
                setups_to_test.append({
                    "ticker": ticker,
                    "setup_type": "BREAKOUT",
                    "entry_price": breakout_setup['entry_price'],
                    "stop_loss": breakout_setup['stop_loss'],
                    "take_profit": breakout_setup['entry_price'] + (Phase3Config.Breakout.TARGET_RR_RATIO * risk),
                    "max_hold_days": Phase3Config.Breakout.MAX_HOLD_DAYS
                })
        
        # ==================================================================
        # === TEST 3: Nowa strategia AQM (Prototyp walidacyjny) ===
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
        # === Koniec Testu 3 ===
        # ==================================================================


        # Przetwórz wszystkie znalezione setupy (stare i nowe)
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
        # ORAZ NOWE STRATEGIE AQM
        period_prefix_base = f"BACKTEST_{year}_"
        prefixes_to_delete = [
            f"{period_prefix_base}EMA_BOUNCE",
            f"{period_prefix_base}EMA_RSI_40",
            f"{period_prefix_base}EMA_MACD_CROSS",
            f"{period_prefix_base}EMA_ATR_FILTER",
            f"{period_prefix_base}BREAKOUT",
            # === NOWE TYPY STRATEGII AQM ===
            f"{period_prefix_base}AQM_SCORE_BULL",
            f"{period_prefix_base}AQM_SCORE_VOLATILE",
            f"{period_prefix_base}AQM_SCORE_BEAR"
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

    # ==================================================================
    # === NOWY KROK: Pobierz dane makro (VIX, SPY) raz ===
    # ==================================================================
    try:
        logger.info("[Backtest] Pobieranie danych makro (VIX, SPY) na potrzeby reżimu rynkowego...")
        
        # Pobieramy dane VIX
        vix_raw = api_client.get_daily_adjusted('VIX', outputsize='full')
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
        
        logger.info("[Backtest] Dane makro VIX i SPY pomyślnie załadowane i zapisane w cache.")

    except Exception as e:
        log_msg = f"[Backtest] BŁĄD KRYTYCZNY: Nie można pobrać danych makro VIX/SPY: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return
    # ==================================================================


    for ticker in tickers_to_test:
        try:
            log_msg = f"[Backtest] Pobieranie pełnych danych historycznych dla {ticker}..."
            logger.info(log_msg)
            append_scan_log(session, log_msg)
            
            # === ZMIANA: Pobieramy DANE DZIENNE i TYGODNIOWE ===
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
            weekly_data_raw = api_client.get_time_series_weekly(ticker, outputsize='full') # NOWE
            
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
               not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw:
                logger.warning(f"[Backtest] Brak danych historycznych (Dziennych lub Tygodniowych) dla {ticker}. Pomijanie.")
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
                logger.warning(f"[Backtest] Niewystarczająca ilość danych dla {ticker} w okresie {year} (D: {len(historical_data_slice)}, W: {len(weekly_data_slice)}). Pomijanie.")
                continue

            # Krok 4: Uruchom symulator (przekazujemy oba DataFrame)
            _simulate_trades(session, ticker, historical_data_slice, weekly_data_slice, year)

        except Exception as e:
            logger.error(f"[Backtest] Błąd krytyczny podczas przetwarzania {ticker}: {e}", exc_info=True)
            session.rollback()
            append_scan_log(session, f"BŁĄD Backtestu dla {ticker}: {e}")

    log_msg = f"BACKTEST HISTORYCZNY: Zakończono test dla roku '{year}'."
    logger.info(log_msg)
    append_scan_log(session, log_msg)
