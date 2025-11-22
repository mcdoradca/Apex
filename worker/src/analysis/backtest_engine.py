import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Importy narzędziowe
from .utils import (
    get_raw_data_with_cache, 
    standardize_df_columns, 
    calculate_atr, 
    append_scan_log, 
    update_system_control,
    update_scan_progress
)

# Importy z nowych modułów analitycznych (V3)
from .aqm_v3_h2_loader import load_h2_data_into_cache
from .aqm_v3_h3_simulator import _simulate_trades_h3
from . import aqm_v3_metrics
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

logger = logging.getLogger(__name__)

def _calculate_time_dilation_series(ticker_df: pd.DataFrame, spy_df: pd.DataFrame, window: int = 20) -> pd.Series:
    """Oblicza serię Time Dilation (Zmienność Tickera / Zmienność SPY)."""
    try:
        if not isinstance(ticker_df.index, pd.DatetimeIndex): ticker_df.index = pd.to_datetime(ticker_df.index)
        if not isinstance(spy_df.index, pd.DatetimeIndex): spy_df.index = pd.to_datetime(spy_df.index)

        ticker_returns = ticker_df['close'].pct_change()
        spy_returns = spy_df['close'].pct_change()
        spy_returns = spy_returns.reindex(ticker_returns.index).ffill().fillna(0)

        ticker_std = ticker_returns.rolling(window=window).std()
        spy_std = spy_returns.rolling(window=window).std()
        
        time_dilation = ticker_std / spy_std.replace(0, np.nan)
        return time_dilation.fillna(0)
    except Exception:
        return pd.Series(0, index=ticker_df.index)

# ==============================================================================
# === NOWA SEKCJA: OPTYMALIZACJA BEZSTRATNA (RAM CACHING) ===
# ==============================================================================

def preload_optimization_data(session: Session, year: str) -> Dict[str, Any]:
    """
    [BEZSTRATNA OPTYMALIZACJA]
    Ładuje dane, oblicza niezmienne metryki (Invariant Metrics) i zwraca strukturę
    gotową do błyskawicznego przetwarzania w pętlach Optuny.
    
    Eliminuje narzut DB i rekalkulacji metryk o 99% w procesie optymalizacji.
    """
    logger.info(f"[Preload] Rozpoczynanie ładowania danych do RAM dla roku {year}...")
    api_client = AlphaVantageClient()
    
    # 1. Selekcja Tickerów (Ta sama logika co w backteście)
    phase1_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
    portfolio_rows = session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()
    tickers = list(set([r[0] for r in phase1_rows] + [r[0] for r in portfolio_rows]))
    
    if not tickers:
        fallback_rows = session.execute(text("SELECT ticker FROM companies LIMIT 50")).fetchall()
        tickers = [r[0] for r in fallback_rows]

    # 2. Benchmark SPY (Raz dla wszystkich)
    spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
    spy_df = pd.DataFrame()
    if spy_raw:
        spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
        spy_df.index = pd.to_datetime(spy_df.index)

    cache = {}
    Z_SCORE_WINDOW = 100
    HISTORY_BUFFER = 201
    
    # 3. Przetwarzanie Tickerów (Heavy Lifting - robimy to RAZ)
    for ticker in tickers:
        try:
            # A. Pobranie danych
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
            
            if not daily_raw or not daily_adj_raw: continue

            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            
            if len(daily_adj) < HISTORY_BUFFER: continue
            
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            daily_adj.index = pd.to_datetime(daily_adj.index)
            
            # Join OHLCV i Adjusted
            if 'high' in daily_ohlcv.columns:
                daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            else: continue
            
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            
            # B. Obliczenie Metryk Niezmiennych (Static Metrics)
            # Te obliczenia są kosztowne, ale niezależne od parametrów optymalizacji (np. mnożników TP/SL)
            
            # H1
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df).ffill().fillna(0)
            
            # H2
            h2_data = load_h2_data_into_cache(ticker, api_client, session)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')
            
            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            
            # Market Temp
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            
            # H3 Components
            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(df.index, fill_value=0)
                df['information_entropy'] = news_counts.rolling(window=10).sum()
                news_mean_200 = df['information_entropy'].rolling(200).mean()
                news_std_200 = df['information_entropy'].rolling(200).std()
                df['normalized_news'] = ((df['information_entropy'] - news_mean_200) / news_std_200).replace([np.inf, -np.inf], 0).fillna(0)
            else:
                df['information_entropy'] = 0.0
                df['normalized_news'] = 0.0
            
            df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
            df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
            df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
            df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
            
            # Score H3 - Prekalkulacja
            # Ponieważ wagi H3 są obecnie stałe (1.0), możemy obliczyć AQM Score raz.
            # Jedynie THRESHOLD (percentyl) zmienia się w pętli optymalizacyjnej.
            
            df['m_sq'] = df['normalized_volume'] + df['normalized_news']
            df['nabla_sq'] = df['price_gravity']
            S = df['information_entropy']
            Q = df['retail_herding']
            T = df['market_temperature']
            mu = df['institutional_sync']
            df['J'] = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
            df['J'] = df['J'].fillna(0)

            # Z-Scores
            j_mean = df['J'].rolling(window=Z_SCORE_WINDOW).mean()
            j_std = df['J'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
            df['J_norm'] = ((df['J'] - j_mean) / j_std).fillna(0)
            
            nabla_mean = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).mean()
            nabla_std = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
            df['nabla_sq_norm'] = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
            
            m_mean = df['m_sq'].rolling(window=Z_SCORE_WINDOW).mean()
            m_std = df['m_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
            df['m_sq_norm'] = ((df['m_sq'] - m_mean) / m_std).fillna(0)

            # Finalny Score H3 (Statyczny, bo wagi są 1.0)
            df['aqm_score_static'] = df['J_norm'] - df['nabla_sq_norm'] - df['m_sq_norm']

            # Minimalizacja zużycia RAM - zachowujemy tylko to, co niezbędne do symulacji
            lean_df = df[[
                'open', 'high', 'low', 'close', 
                'atr_14', 'aqm_score_static', 'm_sq_norm'
            ]].copy()
            
            # Sortowanie dla szybkości wyszukiwania
            lean_df.sort_index(inplace=True)
            
            cache[ticker] = lean_df
            
        except Exception as e:
            logger.error(f"[Preload] Błąd przetwarzania {ticker}: {e}")
            continue

    logger.info(f"[Preload] Zakończono. Załadowano dane dla {len(cache)} tickerów.")
    return cache


def run_optimization_simulation_fast(preloaded_data: Dict[str, pd.DataFrame], params: Dict[str, Any]) -> Dict[str, float]:
    """
    [FAST LOOP] Błyskawiczna symulacja na danych z pamięci RAM.
    Nie wykonuje ŻADNYCH zapytań do bazy ani ciężkich obliczeń metryk.
    """
    stats = { 'profit_factor': 0.0, 'total_trades': 0, 'win_rate': 0.0, 'net_profit': 0.0 }
    trades_results = []
    
    # Parametry dynamiczne (zmieniane przez Optunę)
    h3_percentile = float(params.get('h3_percentile', 0.95))
    h3_m_sq_threshold = float(params.get('h3_m_sq_threshold', -0.5))
    h3_min_score = float(params.get('h3_min_score', 0.0))
    h3_tp_mult = float(params.get('h3_tp_multiplier', 5.0))
    h3_sl_mult = float(params.get('h3_sl_multiplier', 2.0))
    h3_max_hold = int(params.get('h3_max_hold', 5))
    
    # Zakres dat (dla Multi-Period Validation)
    sim_start = params.get('simulation_start_date')
    sim_end = params.get('simulation_end_date')
    
    # Konwersja na Timestamp raz (dla wydajności)
    ts_start = pd.Timestamp(sim_start) if sim_start else None
    ts_end = pd.Timestamp(sim_end) if sim_end else None
    
    Z_SCORE_WINDOW = 100
    HISTORY_BUFFER = 201

    for ticker, df in preloaded_data.items():
        # Filtrowanie po dacie - szybki slice na indeksie
        if ts_start and ts_end:
            # Sprawdzenie zakresu bez kopiowania całego DF jeśli to możliwe
            # Ale musimy mieć historię do rolling quantile! 
            # Używamy całego DF do obliczeń, a pętlę ograniczamy indeksami.
            pass
        else:
            # Domyślnie cały dostępny zakres
            pass

        try:
            # Dynamiczne obliczenie progu (to jedyna ciężka operacja w pętli, ale Pandas robi to w C)
            # Używamy pre-kalkulowanego score
            threshold_series = df['aqm_score_static'].rolling(window=Z_SCORE_WINDOW).quantile(h3_percentile)
            
            # Określenie zakresu indeksów do iteracji
            start_idx = HISTORY_BUFFER
            if ts_start:
                # Znajdź indeks startowy
                search_idx = df.index.searchsorted(ts_start)
                start_idx = max(start_idx, search_idx)
            
            end_idx = len(df)
            if ts_end:
                search_idx = df.index.searchsorted(ts_end)
                end_idx = min(end_idx, search_idx)
            
            if start_idx >= end_idx: continue

            # Szybka pętla transakcyjna
            # Używamy .iloc dla szybkości (dostęp pozycyjny jest szybszy niż etykietowy)
            # Konwertujemy kolumny na numpy arrays dla ekstremalnej szybkości
            opens = df['open'].values
            highs = df['high'].values
            lows = df['low'].values
            closes = df['close'].values
            atrs = df['atr_14'].values
            scores = df['aqm_score_static'].values
            threshs = threshold_series.values
            ms = df['m_sq_norm'].values
            
            i = start_idx
            while i < end_idx - 1: # -1 bo potrzebujemy i+1 do wejścia
                
                # Logika wejścia (Vectorized lookups)
                if (scores[i] > threshs[i]) and \
                   (ms[i] < h3_m_sq_threshold) and \
                   (scores[i] > h3_min_score):
                    
                    entry_price = opens[i+1]
                    atr = atrs[i]
                    
                    if np.isnan(entry_price) or np.isnan(atr) or atr == 0:
                        i += 1; continue
                        
                    tp = entry_price + (h3_tp_mult * atr)
                    sl = entry_price - (h3_sl_mult * atr)
                    
                    pnl_percent = 0.0
                    
                    # Symulacja przebiegu transakcji (max hold)
                    for day_offset in range(h3_max_hold):
                        curr_day = i + 1 + day_offset
                        if curr_day >= len(df): break
                        
                        day_low = lows[curr_day]
                        day_high = highs[curr_day]
                        
                        # Sprawdzenie SL/TP
                        # Priorytet ma Low (SL) - pesymistyczne założenie
                        if day_low <= sl:
                            pnl_percent = ((sl - entry_price) / entry_price) * 100
                            break
                        if day_high >= tp:
                            pnl_percent = ((tp - entry_price) / entry_price) * 100
                            break
                        
                        # Wyjście czasowe
                        if day_offset == h3_max_hold - 1:
                            exit_price = closes[curr_day]
                            pnl_percent = ((exit_price - entry_price) / entry_price) * 100
                    
                    trades_results.append(pnl_percent)
                    
                    # Przeskocz okres trwania transakcji (uproszczenie: jedna pozycja na raz)
                    i += max(1, day_offset)
                
                i += 1

        except Exception:
            continue

    # Agregacja wyników (Szybka statystyka)
    if not trades_results:
        return stats

    # Używamy numpy do szybkiego sumowania
    results_np = np.array(trades_results)
    wins = results_np[results_np > 0]
    losses = results_np[results_np <= 0]
    
    total_win_sum = np.sum(wins) if len(wins) > 0 else 0.0
    total_loss_sum = np.abs(np.sum(losses)) if len(losses) > 0 else 0.0
    
    pf = total_win_sum / total_loss_sum if total_loss_sum > 0 else 0.0
    
    stats['profit_factor'] = float(pf)
    stats['total_trades'] = len(trades_results)
    stats['win_rate'] = (len(wins) / len(trades_results)) * 100
    stats['net_profit'] = float(np.sum(results_np))
    
    return stats

# ==============================================================================

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    """
    Uruchamia pełny backtest historyczny (z zapisem do bazy).
    OPTYMALIZACJA: Skanuje TYLKO spółki z Fazy 1 i Portfela, a nie cały rynek.
    """
    logger.info(f"[Backtest] Rozpoczynanie analizy historycznej dla roku {year}...")
    append_scan_log(session, f"BACKTEST: Uruchamianie symulacji dla roku {year}...")
    
    try:
        # === OPTYMALIZACJA SELEKCJI SPÓŁEK ===
        phase1_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        phase1_tickers = [r[0] for r in phase1_rows]
        
        portfolio_rows = session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()
        portfolio_tickers = [r[0] for r in portfolio_rows]
        
        tickers = list(set(phase1_tickers + portfolio_tickers))
        
        if not tickers:
            msg = "BACKTEST: Brak kandydatów w Fazie 1 i Portfelu. Pobieram Top 50 z bazy (tryb awaryjny)."
            logger.warning(msg)
            append_scan_log(session, msg)
            fallback_rows = session.execute(text("SELECT ticker FROM companies LIMIT 50")).fetchall()
            tickers = [r[0] for r in fallback_rows]
        
        logger.info(f"[Backtest] Wybrano {len(tickers)} tickerów do analizy (Faza 1 + Portfel).")
        append_scan_log(session, f"BACKTEST: Wybrano {len(tickers)} tickerów do analizy.")
        
        # === KROK PRE-A: Pobierz dane SPY (Benchmark) raz dla całej pętli ===
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        spy_df = pd.DataFrame()
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)

        total_tickers = len(tickers)
        update_scan_progress(session, 0, total_tickers)

        processed_count = 0
        trades_generated = 0
        
        Z_SCORE_WINDOW = 100
        MARKET_TEMP_WINDOW = 30
        
        for ticker in tickers:
            try:
                # === KROK A: Pobieranie Danych Historycznych (Cache + API) ===
                daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
                
                if not daily_raw or not daily_adj_raw: continue

                daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
                
                daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_adj.index = pd.to_datetime(daily_adj.index)
                
                if len(daily_adj) < 201: continue

                if 'high' in daily_ohlcv.columns:
                    daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
                else: continue

                df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
                close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
                
                # === KROK B: Obliczanie Metryk ===
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
                
                if not spy_df.empty:
                    df['time_dilation'] = _calculate_time_dilation_series(df, spy_df)
                else:
                    df['time_dilation'] = 0.0
                
                # Dane H2
                h2_data = load_h2_data_into_cache(ticker, api_client, session)
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                
                df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
                df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
                
                df['daily_returns'] = df['close'].pct_change()
                df['market_temperature'] = df['daily_returns'].rolling(window=MARKET_TEMP_WINDOW).std()
                
                # Metryki H3
                if not news_df.empty:
                    news_counts = news_df.groupby(news_df.index.date).size()
                    news_counts.index = pd.to_datetime(news_counts.index)
                    news_counts = news_counts.reindex(df.index, fill_value=0)
                    df['information_entropy'] = news_counts.rolling(window=10).sum()
                    news_mean_200 = df['information_entropy'].rolling(200).mean()
                    news_std_200 = df['information_entropy'].rolling(200).std()
                    df['normalized_news'] = ((df['information_entropy'] - news_mean_200) / news_std_200).replace([np.inf, -np.inf], 0).fillna(0)
                else:
                    df['information_entropy'] = 0.0
                    df['normalized_news'] = 0.0
                
                df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
                df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
                df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
                df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                
                # H3 Score Components
                df['m_sq'] = df['normalized_volume'] + df['normalized_news']
                df['nabla_sq'] = df['price_gravity']
                
                S = df['information_entropy']
                Q = df['retail_herding']
                T = df['market_temperature']
                mu = df['institutional_sync']
                df['J'] = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
                df['J'] = df['J'].fillna(0)

                j_mean = df['J'].rolling(window=Z_SCORE_WINDOW).mean()
                j_std = df['J'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                df['J_norm'] = ((df['J'] - j_mean) / j_std).replace([np.inf, -np.inf], 0).fillna(0)
                
                nabla_mean = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).mean()
                nabla_std = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                df['nabla_sq_norm'] = ((df['nabla_sq'] - nabla_mean) / nabla_std).replace([np.inf, -np.inf], 0).fillna(0)
                
                m_mean = df['m_sq'].rolling(window=Z_SCORE_WINDOW).mean()
                m_std = df['m_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                df['m_sq_norm'] = ((df['m_sq'] - m_mean) / m_std).replace([np.inf, -np.inf], 0).fillna(0)

                df['aqm_score_h3'] = df['J_norm'] - df['nabla_sq_norm'] - df['m_sq_norm']
                df['aqm_percentile_95'] = df['aqm_score_h3'].rolling(window=Z_SCORE_WINDOW).quantile(0.95)

                # Symulacja Transakcji
                sim_data = { "daily": df }
                trades = _simulate_trades_h3(session, ticker, sim_data, year, parameters)
                trades_generated += trades
                
                processed_count += 1
                
                # Logowanie Sukcesów
                if trades > 0:
                    append_scan_log(session, f"✨ BACKTEST: {ticker} -> Znaleziono {trades} wirtualnych setupów.")

                if processed_count % 5 == 0:
                    update_scan_progress(session, processed_count, total_tickers)
                    logger.info(f"[Backtest] {processed_count}/{total_tickers} ({ticker}). Transakcji: {trades_generated}")
                
                if processed_count % 20 == 0:
                    msg = f"Backtest: Przetworzono {processed_count}/{total_tickers} ({ticker})... Łącznie znaleziono: {trades_generated}"
                    append_scan_log(session, msg)

            except Exception as e:
                logger.error(f"Błąd backtestu dla {ticker}: {e}")
                continue

        update_scan_progress(session, total_tickers, total_tickers)
        append_scan_log(session, f"BACKTEST: Zakończono dla roku {year}. Wygenerowano {trades_generated} wirtualnych transakcji.")
        logger.info(f"[Backtest] Koniec. Łącznie transakcji: {trades_generated}")

    except Exception as e:
        error_msg = f"Krytyczny błąd Backtestu: {e}"
        logger.error(error_msg, exc_info=True)
        append_scan_log(session, error_msg)


def run_optimization_simulation(session: Session, year: str, params: dict) -> dict:
    """
    WRAPPER dla kompatybilności wstecznej.
    UWAGA: Używanie tej funkcji w pętli optymalizacyjnej jest POWOLNE.
    Należy używać preload_optimization_data + run_optimization_simulation_fast.
    """
    # Ta funkcja pozostaje jako fallback, ale idealnie powinna zostać zastąpiona przez wywołanie
    # 'fast' w warstwie wyższej (apex_optimizer).
    # Jeśli jednak zostanie wywołana bezpośrednio, wykonuje "starą", wolną logikę (load + calc + sim).
    
    # Dla uproszczenia i zachowania logiki, możemy tu użyć naszej nowej szybkiej logiki jednorazowo:
    cache = preload_optimization_data(session, year)
    return run_optimization_simulation_fast(cache, params)
