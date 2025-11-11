import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
# KROK 1: Rozszerzamy importy
import numpy as np
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    get_current_NY_datetime,
    append_scan_log,
    update_scan_progress,
    calculate_rsi,
    calculate_macd,
    calculate_atr,
    calculate_obv,
    calculate_ad
)
from .. import models
# KROK 2b: Importujemy mapowanie sektorów z config
from ..config import Phase3Config, SECTOR_TO_ETF_MAP

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
# === KROK 2b (REWOLUCJA): Parametry dla Handlu Parami ===
# ==================================================================
PAIRS_STRATEGY_PARAMS = {
    'stop_loss_percent': 0.04, # Ciaśniejszy SL (4%)
    'target_percent': 0.08,    # Cel 8% (R:R 1:2)
    'max_hold_days': 5,        # Krótszy horyzont (5 dni)
    'sector_lookback_days': 21 # 1 miesiąc na siłę sektora
}
# ==================================================================


# ==================================================================
# === Silnik Symulacji ===
# ==================================================================

def _resolve_trade(historical_data: pd.DataFrame, entry_index: int, setup: Dict[str, Any], max_hold_days: int, year: str, direction: str) -> models.VirtualTrade | None:
    """
    "Spogląda w przyszłość" (w danych historycznych), aby zobaczyć, jak
    dana transakcja by się zakończyła.
    Obsługuje teraz transakcje 'LONG' i 'SHORT'.
    """
    try:
        entry_price = setup['entry_price']
        stop_loss = setup['stop_loss']
        take_profit = setup['take_profit']
        
        close_price = entry_price # Domyślna cena zamknięcia, jeśli nic się nie stanie
        status = 'CLOSED_EXPIRED' # Domyślny status
        candle = historical_data.iloc[entry_index] # Domyślna świeca (na wypadek błędu)

        if direction == 'LONG':
            # === LOGIKA DLA POZYCJI DŁUGIEJ (LONG) ===
            for i in range(1, max_hold_days + 1):
                if entry_index + i >= len(historical_data):
                    candle = historical_data.iloc[-1]
                    close_price = candle['close']
                    status = 'CLOSED_EXPIRED'
                    break
                
                candle = historical_data.iloc[entry_index + i]
                day_low = candle['low']
                day_high = candle['high']

                if day_low <= stop_loss:
                    close_price = stop_loss
                    status = 'CLOSED_SL'
                    break
                    
                if day_high >= take_profit:
                    close_price = take_profit
                    status = 'CLOSED_TP'
                    break
            else:
                final_index = min(entry_index + max_hold_days, len(historical_data) - 1)
                candle = historical_data.iloc[final_index]
                close_price = candle['close']
                status = 'CLOSED_EXPIRED'

            p_l_percent = ((close_price - entry_price) / entry_price) * 100
        
        elif direction == 'SHORT':
            # === LOGIKA DLA POZYCJI KRÓTKIEJ (SHORT) ===
            for i in range(1, max_hold_days + 1):
                if entry_index + i >= len(historical_data):
                    candle = historical_data.iloc[-1]
                    close_price = candle['close']
                    status = 'CLOSED_EXPIRED'
                    break
                
                candle = historical_data.iloc[entry_index + i]
                day_low = candle['low']
                day_high = candle['high']

                if day_high >= stop_loss:
                    close_price = stop_loss
                    status = 'CLOSED_SL'
                    break
                    
                if day_low <= take_profit:
                    close_price = take_profit
                    status = 'CLOSED_TP'
                    break
            else:
                final_index = min(entry_index + max_hold_days, len(historical_data) - 1)
                candle = historical_data.iloc[final_index]
                close_price = candle['close']
                status = 'CLOSED_EXPIRED'

            p_l_percent = ((entry_price - close_price) / entry_price) * 100
        
        else:
            logger.error(f"Nieznany kierunek transakcji: {direction}")
            return None

        
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
# === SŁOWNIK CACHE (PRZEBUDOWANY) ===
# ==================================================================

_backtest_cache = {
    "vix_data": None, # DF dla VXX
    "spy_data": None, # DF dla SPY
    "sector_etf_data": {}, # Klucz: 'XLK', Wartość: DF
    # === KROK 2b: Cache dla danych spółek ===
    "company_data": {}, # Klucz: 'AAPL', Wartość: {'daily': DF, 'weekly': DF, 'sector': 'Technology'}
    "tickers_by_sector": {} # Klucz: 'Technology', Wartość: ['AAPL', 'MSFT', ...]
}

def _get_ticker_data_from_cache(ticker: str) -> Optional[Dict[str, Any]]:
    """Pobiera dane spółki (dzienne, tygodniowe, sektor) z cache."""
    return _backtest_cache["company_data"].get(ticker)

def _get_tickers_in_sector(sector: str) -> List[str]:
    """Pobiera listę tickerów dla danego sektora z cache."""
    return _backtest_cache["tickers_by_sector"].get(sector, [])

def _get_sector_for_ticker(session: Session, ticker: str) -> str:
    """Pobiera sektor dla tickera z bazy danych (z cache)."""
    # Ta funkcja jest teraz używana tylko raz, podczas budowania cache
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
# ==================================================================


def _detect_market_regime(current_date_str: str) -> str:
    """Wykrywa reżim rynkowy na podstawie danych VXX i SPY z cache."""
    try:
        vix_df = _backtest_cache["vix_data"]
        spy_df = _backtest_cache["spy_data"]
        
        vix_row = vix_df[vix_df.index <= current_date_str].iloc[-1]
        spy_row = spy_df[spy_df.index <= current_date_str].iloc[-1]
        
        vix_price = vix_row['close']
        spy_price = spy_row['close']
        spy_sma_50 = spy_row['ema_50']
        spy_sma_200 = spy_row['ema_200']

        if vix_price < 18 and spy_price > spy_sma_200:
            return 'bull'
        elif vix_price > 25 or spy_price < spy_sma_50:
            return 'bear'
        else:
            return 'volatile'
            
    except IndexError:
        return 'volatile'
    except Exception as e:
        logger.error(f"Błąd podczas wykrywania reżimu dla {current_date_str}: {e}")
        return 'volatile'

def _calculate_sector_strength(sector_etf_ticker: str, current_date_str: str, lookback_days: int) -> float | None:
    """Oblicza siłę (momentum) sektora na podstawie zwrotu z ostatnich N dni."""
    try:
        sector_etf_df = _backtest_cache["sector_etf_data"].get(sector_etf_ticker)
        if sector_etf_df is None:
            return None
            
        relevant_data = sector_etf_df[sector_etf_df.index <= current_date_str]
        if len(relevant_data) < lookback_days + 1:
            return None
            
        price_now = relevant_data.iloc[-1]['close']
        price_then = relevant_data.iloc[-(lookback_days + 1)]['close']
        
        if price_then == 0:
            return None
            
        return ((price_now - price_then) / price_then) * 100
        
    except Exception as e:
        return None

# ==================================================================
# === LOGIKA STRATEGII 1: AQM (Long-Only) ===
# ==================================================================

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
        d = df_view.iloc[-1]
        trend_d = 0.0
        if d['close'] > d['ema_20']: trend_d += 0.25
        if d['close'] > d['ema_50']: trend_d += 0.25
        if d['ema_20'] > d['ema_50']: trend_d += 0.25
        if d['ema_50'] > d['ema_200']: trend_d += 0.25
        
        momentum_d = 0.0
        if d['rsi_14'] > 50: momentum_d += 0.5
        if d['macd_line'] > d['signal_line']: momentum_d += 0.5
        
        volatility_d = 0.3
        if d['close'] > 0:
            atr_percent = d['atr_14'] / d['close']
            if atr_percent < 0.05: volatility_d = 1.0
            elif atr_percent < 0.08: volatility_d = 0.7
        
        score_daily = (trend_d * 0.5) + (momentum_d * 0.3) + (volatility_d * 0.2)
        
        w = weekly_df_view.iloc[-1]
        trend_w = 0.0
        if w['close'] > w['ema_20']: trend_w += 0.25
        if w['close'] > w['ema_50']: trend_w += 0.25
        if w['ema_20'] > w['ema_50']: trend_w += 0.25
        
        momentum_w = 0.0
        if w['rsi_14'] > 50: momentum_w += 0.5
        if w['macd_line'] > w['signal_line']: momentum_w += 0.5
        
        score_weekly = (trend_w * 0.7) + (momentum_w * 0.3)
        qps_score = (score_daily + score_weekly) / 2.0

        # --- 2. Volume Entropy Score (VES) - Waga 30% ---
        obv_score = 0.0
        if d['obv'] > d['obv_20_ma']: obv_score += 0.4
        if d['obv'] > df_view.iloc[-2]['obv']: obv_score += 0.3
        if d['obv_20_ma'] > d['obv_50_ma']: obv_score += 0.3
        
        volume_score = 0.1
        if d['volume_20_ma'] > 0:
            volume_ratio = d['volume'] / d['volume_20_ma']
            if volume_ratio > 1.2: volume_score = 1.0
            elif volume_ratio > 0.8: volume_score = 0.7
        
        ad_score = 0.0
        if d['ad_line'] > d['ad_line_20_ma']: ad_score += 0.5
        if d['ad_line'] > df_view.iloc[-2]['ad_line']: ad_score += 0.5
        
        ves_score = (obv_score * 0.4) + (volume_score * 0.3) + (ad_score * 0.3)

        # --- 3. Market Regime Score (MRS) - Waga 20% ---
        mrs_score = 0.0
        if market_regime == 'bull':
            if sector in ['Technology', 'Communication Services', 'Consumer Cyclical']:
                mrs_score = 0.7
            else:
                mrs_score = 0.3
        elif market_regime == 'volatile':
            mrs_score = 0.5
        elif market_regime == 'bear':
            mrs_score = 0.0 # HAMULEC
        
        # --- 4. Temporal Coherence Score (TCS) - Waga 10% ---
        tcs_score = 1.0 # Uproszczenie
        
        # --- FINAŁ: Suma ważona ---
        final_aqm_score = (qps_score * 0.40) + (ves_score * 0.30) + (mrs_score * 0.20) + (tcs_score * 0.10)
        
        components = { "QPS": qps_score, "VES": ves_score, "MRS": mrs_score, "TCS": tcs_score }
        
        return final_aqm_score, components

    except Exception as e:
        return 0.0, {}

# ==================================================================
# === KROK 2b (REWOLUCJA): Logika Strategii 2: Handel Parami ===
# ==================================================================

def _find_best_long_candidate(tickers_in_sector: List[str], current_date_str: str) -> Optional[Tuple[str, float]]:
    """Przeszukuje spółki w danym sektorze, aby znaleźć najlepszego kandydata LONG (najwyższy AQM)."""
    best_score = 0.0
    best_ticker = None
    
    market_regime = _detect_market_regime(current_date_str) # Reżim jest ten sam dla wszystkich
    
    for ticker in tickers_in_sector:
        data = _get_ticker_data_from_cache(ticker)
        if not data:
            continue
            
        df_full = data['daily']
        weekly_df_full = data['weekly']
        
        # Tworzymy widok do dnia bieżącego
        df_view = df_full[df_full.index <= current_date_str].iloc[-200:]
        weekly_df_view = weekly_df_full[weekly_df_full.index <= current_date_str].iloc[-50:]
        
        if len(df_view) < 200 or len(weekly_df_view) < 50:
            continue
            
        # Obliczamy AQM (który jest naszą miarą "siły")
        aqm_score, _ = _calculate_aqm_score(df_view, weekly_df_view, data['sector'], market_regime)
        
        if aqm_score > best_score:
            best_score = aqm_score
            best_ticker = ticker
            
    if best_ticker and best_score > AQM_THRESHOLDS.get(market_regime, 0.85):
        return best_ticker, best_score
    return None

def _find_best_short_candidate(tickers_in_sector: List[str], current_date_str: str) -> Optional[Tuple[str, float]]:
    """
    Przeszukuje spółki w danym sektorze, aby znaleźć najlepszego kandydata SHORT.
    Używa nowej, "odwróconej" logiki (ADM - Adaptive Distribution Momentum).
    """
    best_score = 0.0 # Najwyższy wynik oznacza "najgorszą" spółkę
    best_ticker = None
    
    for ticker in tickers_in_sector:
        data = _get_ticker_data_from_cache(ticker)
        if not data:
            continue
            
        df_full = data['daily']
        df_view = df_full[df_full.index <= current_date_str].iloc[-200:]
        
        if len(df_view) < 200:
            continue
            
        d = df_view.iloc[-1]
        score = 0.0
        
        # 1. Trend (cena poniżej EMA)
        if d['close'] < d['ema_50'] and d['ema_20'] < d['ema_50']:
            score += 0.4
        # 2. Momentum (RSI słabe)
        if d['rsi_14'] < 40:
            score += 0.3
        # 3. Wolumen (Dystrybucja - OBV spada)
        if d['obv'] < d['obv_20_ma']:
            score += 0.3
            
        if score > best_score:
            best_score = score
            best_ticker = ticker
            
    if best_ticker and best_score > 0.7: # Wymagamy silnego sygnału SHORT (np. 0.4 + 0.3)
        return best_ticker, best_score
    return None

def _run_pairs_trading_strategy(
    session: Session, 
    year: str, 
    current_date: pd.Timestamp, 
    current_date_str: str
) -> List[models.VirtualTrade]:
    """
    Uruchamia pełną logikę Handlu Parami dla JEDNEGO DNIA.
    """
    trades_to_open = []
    
    # 1. Oblicz siłę wszystkich 11 sektorów
    sector_strengths = []
    for sector_name, etf_ticker in SECTOR_TO_ETF_MAP.items():
        strength = _calculate_sector_strength(
            etf_ticker, 
            current_date_str, 
            PAIRS_STRATEGY_PARAMS['sector_lookback_days']
        )
        if strength is not None:
            sector_strengths.append((sector_name, strength))
            
    if len(sector_strengths) < 2: # Potrzebujemy co najmniej 2 sektorów do porównania
        return []
        
    # 2. Posortuj, aby znaleźć najsilniejszy i najsłabszy
    sector_strengths.sort(key=lambda x: x[1], reverse=True)
    strongest_sector_name = sector_strengths[0][0]
    weakest_sector_name = sector_strengths[-1][0]

    # 3. Znajdź kandydatów w tych sektorach
    long_candidate_tuple = _find_best_long_candidate(
        _get_tickers_in_sector(strongest_sector_name), 
        current_date_str
    )
    
    short_candidate_tuple = _find_best_short_candidate(
        _get_tickers_in_sector(weakest_sector_name),
        current_date_str
    )
    
    # 4. Jeśli mamy parę, przygotuj transakcje
    if long_candidate_tuple and short_candidate_tuple:
        long_ticker, long_score = long_candidate_tuple
        short_ticker, short_score = short_candidate_tuple
        
        long_data = _get_ticker_data_from_cache(long_ticker)['daily']
        short_data = _get_ticker_data_from_cache(short_ticker)['daily']
        
        # Pobieramy indeks (numer wiersza) dla bieżącej daty
        long_entry_index = long_data.index.get_loc(current_date)
        short_entry_index = short_data.index.get_loc(current_date)
        
        long_entry_price = long_data.iloc[long_entry_index]['close']
        short_entry_price = short_data.iloc[short_entry_index]['close']

        # Przygotuj transakcję LONG
        setup_long = {
            "ticker": long_ticker,
            "setup_type": f"PAIRS_LONG (vs {short_ticker})",
            "entry_price": long_entry_price,
            "stop_loss": long_entry_price * (1 - PAIRS_STRATEGY_PARAMS['stop_loss_percent']),
            "take_profit": long_entry_price * (1 + PAIRS_STRATEGY_PARAMS['target_percent']),
        }
        trade_long = _resolve_trade(
            long_data, long_entry_index, setup_long, 
            PAIRS_STRATEGY_PARAMS['max_hold_days'], year, direction='LONG'
        )
        if trade_long:
            trades_to_open.append(trade_long)

        # Przygotuj transakcję SHORT
        setup_short = {
            "ticker": short_ticker,
            "setup_type": f"PAIRS_SHORT (vs {long_ticker})",
            "entry_price": short_entry_price,
            "stop_loss": short_entry_price * (1 + PAIRS_STRATEGY_PARAMS['stop_loss_percent']), # SL w górę
            "take_profit": short_entry_price * (1 - PAIRS_STRATEGY_PARAMS['target_percent']), # TP w dół
        }
        trade_short = _resolve_trade(
            short_data, short_entry_index, setup_short, 
            PAIRS_STRATEGY_PARAMS['max_hold_days'], year, direction='SHORT'
        )
        if trade_short:
            trades_to_open.append(trade_short)
            
    return trades_to_open

# ==================================================================
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (PRZEBUDOWANA) ===
# ==================================================================

def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny dla
    zdefiniowanego okresu i listy tickerów.
    
    PRZEBUDOWANA: Działa teraz w trybie "TOP-DOWN" (dzień po dniu).
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
        
    log_msg = f"BACKTEST HISTORYCZNY (TOP-DOWN): Rozpoczynanie testu dla roku '{year}' ({start_date} do {end_date})"
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # === KROK 1: Czyszczenie Bazy Danych (Logika z Rewolucji) ===
    try:
        like_pattern = f"BACKTEST_{year}_%"
        logger.info(f"Czyszczenie WSZYSTKICH starych wyników dla wzorca: {like_pattern}...")
        
        delete_stmt = text("DELETE FROM virtual_trades WHERE setup_type LIKE :pattern")
        result = session.execute(delete_stmt, {'pattern': like_pattern})
        
        session.commit()
        logger.info(f"Pomyślnie usunięto {result.rowcount} starych wpisów backtestu dla roku {year}.")
        
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()

    # === KROK 2: Pobieranie Listy Spółek (Logika z Rewolucji) ===
    try:
        log_msg_tickers = "[Backtest] Pobieranie listy tickerów ze skanera 'phase1_candidates'..."
        logger.info(log_msg_tickers)
        append_scan_log(session, log_msg_tickers)
        
        tickers_to_test_rows = session.execute(text("SELECT DISTINCT ticker FROM phase1_candidates ORDER BY ticker")).fetchall()
        tickers_to_test = [row[0] for row in tickers_to_test_rows]
        
        if not tickers_to_test:
            log_msg = f"[Backtest] BŁĄD: Tabela 'phase1_candidates' jest pusta. Uruchom najpierw skaner Fazy 1 (przycisk 'Start')."
            logger.error(log_msg)
            append_scan_log(session, log_msg)
            return

        log_msg = f"[Backtest] Znaleziono {len(tickers_to_test)} tickerów w 'phase1_candidates' do przetestowania."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów z 'phase1_candidates': {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 3: Budowanie Pamięci Podręcznej (Cache) ===
    # To jest intensywna operacja, która wykonuje się raz na początku.
    
    try:
        logger.info("[Backtest] Rozpoczynanie budowania pamięci podręcznej (Cache)...")
        # 1. Wyczyść stary cache
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["company_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}

        # 2. Pobierz dane Makro (VXX i SPY)
        logger.info("[Backtest] Cache: Ładowanie VXX i SPY...")
        vix_raw = api_client.get_daily_adjusted('VXX', outputsize='full')
        vix_df = pd.DataFrame.from_dict(vix_raw['Time Series (Daily)'], orient='index')
        vix_df = standardize_df_columns(vix_df)
        vix_df.index = pd.to_datetime(vix_df.index)
        _backtest_cache["vix_data"] = vix_df
        
        spy_raw = api_client.get_daily_adjusted('SPY', outputsize='full')
        spy_df = pd.DataFrame.from_dict(spy_raw['Time Series (Daily)'], orient='index')
        spy_df = standardize_df_columns(spy_df)
        spy_df.index = pd.to_datetime(spy_df.index)
        spy_df['ema_50'] = calculate_ema(spy_df['close'], period=50)
        spy_df['ema_200'] = calculate_ema(spy_df['close'], period=200)
        _backtest_cache["spy_data"] = spy_df
        
        # 3. Pobierz dane Sektorowe (ETF-y)
        logger.info("[Backtest] Cache: Ładowanie 11 sektorów ETF...")
        for sector_name, etf_ticker in SECTOR_TO_ETF_MAP.items():
            try:
                sector_raw = api_client.get_daily_adjusted(etf_ticker, outputsize='full')
                sector_df = pd.DataFrame.from_dict(sector_raw['Time Series (Daily)'], orient='index')
                sector_df = standardize_df_columns(sector_df)
                sector_df.index = pd.to_datetime(sector_df.index)
                _backtest_cache["sector_etf_data"][etf_ticker] = sector_df
            except Exception as e:
                logger.error(f"  > BŁĄD ładowania danych dla sektora {etf_ticker}: {e}")
        
        # 4. Pobierz dane dla wszystkich spółek z listy Fazy 1
        logger.info(f"[Backtest] Cache: Ładowanie danych dla {len(tickers_to_test)} spółek...")
        for i, ticker in enumerate(tickers_to_test):
            if i % 10 == 0:
                log_msg = f"[Backtest] Budowanie cache... ({i}/{len(tickers_to_test)})"
                append_scan_log(session, log_msg)
                update_scan_progress(session, i, len(tickers_to_test))
            
            try:
                price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
                weekly_data_raw = api_client.get_time_series_weekly(ticker, outputsize='full')
                
                if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
                   not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw:
                    continue

                daily_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
                daily_df = standardize_df_columns(daily_df)
                daily_df.index = pd.to_datetime(daily_df.index)
                
                weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
                weekly_df = standardize_df_columns(weekly_df)
                weekly_df.index = pd.to_datetime(weekly_df.index)

                # Oblicz wskaźniki dla spółki (raz)
                daily_df['rsi_14'] = calculate_rsi(daily_df['close'], period=14)
                daily_df['macd_line'], daily_df['signal_line'] = calculate_macd(daily_df['close'])
                daily_df['atr_14'] = calculate_atr(daily_df, period=14)
                daily_df['ema_20'] = calculate_ema(daily_df['close'], period=20)
                daily_df['ema_50'] = calculate_ema(daily_df['close'], period=50)
                daily_df['ema_200'] = calculate_ema(daily_df['close'], period=200)
                daily_df['obv'] = calculate_obv(daily_df)
                daily_df['obv_20_ma'] = daily_df['obv'].rolling(window=20).mean()
                daily_df['obv_50_ma'] = daily_df['obv'].rolling(window=50).mean()
                daily_df['volume_20_ma'] = daily_df['volume'].rolling(window=20).mean()
                daily_df['ad_line'] = calculate_ad(daily_df)
                daily_df['ad_line_20_ma'] = daily_df['ad_line'].rolling(window=20).mean()
                
                weekly_df['rsi_14'] = calculate_rsi(weekly_df['close'], period=14)
                weekly_df['macd_line'], weekly_df['signal_line'] = calculate_macd(weekly_df['close'])
                weekly_df['ema_20'] = calculate_ema(weekly_df['close'], period=20)
                weekly_df['ema_50'] = calculate_ema(weekly_df['close'], period=50)
                
                # Zdobądź i zapisz sektor
                sector = _get_sector_for_ticker(session, ticker)
                
                # Zapisz wszystko w cache
                _backtest_cache["company_data"][ticker] = {
                    "daily": daily_df,
                    "weekly": weekly_df,
                    "sector": sector
                }
                
                # Zbuduj mapę Sektor -> Ticker
                if sector not in _backtest_cache["tickers_by_sector"]:
                    _backtest_cache["tickers_by_sector"][sector] = []
                _backtest_cache["tickers_by_sector"][sector].append(ticker)

            except Exception as e:
                logger.error(f"[Backtest] Błąd budowania cache dla {ticker}: {e}", exc_info=True)
                
        logger.info("[Backtest] Budowanie pamięci podręcznej (Cache) zakończone.")
        append_scan_log(session, "[Backtest] Budowanie pamięci podręcznej (Cache) zakończone.")

    except Exception as e:
        log_msg = f"[Backtest] BŁĄD KRYTYCZNY podczas budowania cache: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # ==================================================================
    # === KROK 4: Główna pętla symulacji (TOP-DOWN) ===
    # ==================================================================
    
    # Znajdź wszystkie unikalne dni handlowe w danym roku na podstawie SPY
    all_trading_days = _backtest_cache["spy_data"].loc[start_date:end_date].index
    
    total_days = len(all_trading_days)
    trades_found_total = 0

    for i, current_date in enumerate(all_trading_days):
        
        current_date_str = current_date.strftime('%Y-%m-%d')
        
        # Pomiń pierwsze 200 dni roku, aby mieć pewność, że wskaźniki są "dojrzałe"
        if i < 200:
            continue
            
        if i % 10 == 0: # Aktualizuj UI co 10 dni
            log_msg = f"[Backtest] Symulowanie dnia {i}/{total_days} ({current_date_str})..."
            append_scan_log(session, log_msg)
            update_scan_progress(session, i, total_days)

        try:
            # 1. Wykryj reżim rynkowy (raz na dzień)
            market_regime = _detect_market_regime(current_date_str)
            
            new_trades = []

            # ==================================================
            # === URUCHOM STRATEGIĘ 1: AQM (Long-Only) ===
            # Uruchamiaj tylko w hossie, zgodnie z logiką hamulca
            # ==================================================
            if market_regime == 'bull':
                threshold = AQM_THRESHOLDS[market_regime]
                
                for ticker in tickers_to_test: # Iteruj po spółkach z Fazy 1
                    data = _get_ticker_data_from_cache(ticker)
                    if not data:
                        continue
                        
                    # Pobierz widoki danych
                    df_view = data['daily'][data['daily'].index <= current_date_str].iloc[-200:]
                    weekly_df_view = data['weekly'][data['weekly'].index <= current_date_str].iloc[-50:]
                    
                    if len(df_view) < 200 or len(weekly_df_view) < 50:
                        continue

                    aqm_score, _ = _calculate_aqm_score(df_view, weekly_df_view, data['sector'], market_regime)
                    
                    if aqm_score > threshold:
                        # ZNALEZIONO SETUP AQM
                        entry_index = df_view.index.get_loc(current_date)
                        entry_price = df_view.iloc[entry_index]['close']
                        
                        setup_aqm = {
                            "ticker": ticker,
                            "setup_type": f"AQM_SCORE_{market_regime.upper()}", 
                            "entry_price": entry_price,
                            "stop_loss": entry_price * (1 - AQM_STRATEGY_PARAMS['stop_loss_percent']),
                            "take_profit": entry_price * (1 + AQM_STRATEGY_PARAMS['target_1_percent']),
                        }
                        
                        trade = _resolve_trade(
                            data['daily'], entry_index, setup_aqm, 
                            AQM_STRATEGY_PARAMS['max_hold_days'], year, direction='LONG'
                        )
                        if trade:
                            new_trades.append(trade)

            # ==================================================
            # === URUCHOM STRATEGIĘ 2: Handel Parami ===
            # Uruchamiaj ZAWSZE (jest rynkowo-neutralna)
            # ==================================================
            pair_trades = _run_pairs_trading_strategy(session, year, current_date, current_date_str)
            if pair_trades:
                new_trades.extend(pair_trades)

            # Zapisz wszystkie transakcje znalezione TEGO DNIA
            if new_trades:
                session.add_all(new_trades)
                session.commit()
                trades_found_total += len(new_trades)

        except Exception as e:
            logger.error(f"[Backtest] Błąd krytyczny podczas symulacji dnia {current_date_str}: {e}", exc_info=True)
            session.rollback()

    # === KONIEC PĘTLI ROCZNEJ ===
    update_scan_progress(session, total_days, total_days)
    log_msg = f"BACKTEST HISTORYCZNY: Zakończono test dla roku '{year}'. Znaleziono łącznie {trades_found_total} transakcji."
    logger.info(log_msg)
    append_scan_log(session, log_msg)
