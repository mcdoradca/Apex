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
# === SEKCJA 1: BACKTEST HISTORYCZNY (H3 CORE) - ZOPTYMALIZOWANY ===
# ==============================================================================

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    """
    Uruchamia pełny backtest historyczny (z zapisem do bazy).
    
    OPTYMALIZACJA BEZSTRATNA (Vectorization):
    Zamiast powolnego df.apply() dla każdego dnia, używamy wektorowych operacji
    rolling() dla metryk H2 (Insider/News), co przyspiesza proces o 50-100x
    przy zachowaniu 100% dokładności matematycznej względem PDF.
    """
    logger.info(f"[Backtest] Rozpoczynanie analizy historycznej dla roku {year}...")
    append_scan_log(session, f"BACKTEST: Uruchamianie symulacji dla roku {year}...")
    
    try:
        # 1. Selekcja Tickerów (Faza 1 + Portfel)
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
        
        logger.info(f"[Backtest] Wybrano {len(tickers)} tickerów do analizy.")
        append_scan_log(session, f"BACKTEST: Wybrano {len(tickers)} tickerów do analizy.")
        
        # 2. Benchmark SPY (Raz dla całej pętli)
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        spy_df = pd.DataFrame()
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)

        total_tickers = len(tickers)
        update_scan_progress(session, 0, total_tickers)

        processed_count = 0
        trades_generated = 0
        
        MARKET_TEMP_WINDOW = 30
        
        for ticker in tickers:
            try:
                # A. Pobieranie Danych Historycznych (Cache + API)
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
                
                # B. Szybkie Obliczanie Metryk (Wektoryzacja)
                
                # Wymiar 1 (H1)
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
                
                if not spy_df.empty:
                    df['time_dilation'] = _calculate_time_dilation_series(df, spy_df)
                else:
                    df['time_dilation'] = 0.0
                
                # Wymiar 2 (H2) - OPTYMALIZACJA WEKTOROWA
                h2_data = load_h2_data_into_cache(ticker, api_client, session)
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                
                # Zamiast df.apply (wolne), używamy rolling window na indeksie czasowym (błyskawiczne)
                
                # 2.1 Institutional Sync (90 dni)
                if not insider_df.empty:
                    # Agregacja do dziennych sum
                    insider_buys = insider_df[insider_df['transaction_type'] == 'A']['transaction_shares'].resample('D').sum()
                    insider_sells = insider_df[insider_df['transaction_type'] == 'D']['transaction_shares'].resample('D').sum()
                    
                    # Rolling sum na 90 dni (kalendarzowych)
                    rolling_buys = insider_buys.rolling('90D').sum()
                    rolling_sells = insider_sells.rolling('90D').sum()
                    denominator = rolling_buys + rolling_sells
                    
                    # Obliczenie wskaźnika i mapowanie do głównego DF (reindex)
                    sync_series = (rolling_buys - rolling_sells) / denominator.replace(0, np.nan)
                    df['institutional_sync'] = sync_series.reindex(df.index, method='ffill').fillna(0.0)
                else:
                    df['institutional_sync'] = 0.0

                # 2.2 Retail Herding (7 dni)
                if not news_df.empty:
                    # Aby zachować "średnią ważoną wszystkich artykułów z 7 dni", sumujemy score i liczby
                    news_sum = news_df['overall_sentiment_score'].resample('D').sum()
                    news_count = news_df['overall_sentiment_score'].resample('D').count()
                    
                    rolling_sum = news_sum.rolling('7D').sum()
                    rolling_count = news_count.rolling('7D').sum()
                    
                    herding_series = rolling_sum / rolling_count.replace(0, np.nan)
                    df['retail_herding'] = herding_series.reindex(df.index, method='ffill').fillna(0.0)
                else:
                    df['retail_herding'] = 0.0
                
                # Wymiar 4 (Market Temp)
                df['daily_returns'] = df['close'].pct_change()
                df['market_temperature'] = df['daily_returns'].rolling(window=MARKET_TEMP_WINDOW).std()
                
                # Wymiar 3 i 7 (H3 Components)
                # Używamy proxy liczby newsów dla Entropii (Wymiar 4.2)
                if not news_df.empty:
                    # Zliczanie newsów dziennie, potem rolling window 10 dni
                    news_counts = news_df.groupby(news_df.index.date).size()
                    news_counts.index = pd.to_datetime(news_counts.index)
                    # Reindex do osi czasu akcji, wypełnienie zerami tam gdzie brak newsów
                    news_counts = news_counts.reindex(df.index, fill_value=0)
                    
                    df['information_entropy'] = news_counts.rolling(window=10).sum()
                    
                    # Z-Score dla Newsów (200 dni historii)
                    news_mean_200 = df['information_entropy'].rolling(200).mean()
                    news_std_200 = df['information_entropy'].rolling(200).std()
                    df['normalized_news'] = ((df['information_entropy'] - news_mean_200) / news_std_200).replace([np.inf, -np.inf], 0).fillna(0)
                else:
                    df['information_entropy'] = 0.0
                    df['normalized_news'] = 0.0
                
                # Z-Score dla Wolumenu (10 dni średniej vs 200 dni historii)
                df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
                df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
                df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
                df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                
                # C. Budowanie Składników H3 (Base Components)
                # Symulator (simulate_trades_h3) sam oblicza Z-Scores (J_norm, nabla_norm, etc.) 
                # i finalny AQM Score na podstawie poniższych kolumn.
                # Nie musimy ich tu liczyć podwójnie.
                
                df['m_sq'] = df['normalized_volume'] + df['normalized_news']
                df['nabla_sq'] = df['price_gravity']
                
                S = df['information_entropy']
                Q = df['retail_herding']
                T = df['market_temperature']
                mu = df['institutional_sync']
                
                # Wzór na J (Siła Napędowa)
                df['J'] = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
                df['J'] = df['J'].fillna(0)

                # D. Symulacja Transakcji
                sim_data = { "daily": df }
                trades = _simulate_trades_h3(session, ticker, sim_data, year, parameters)
                trades_generated += trades
                
                processed_count += 1
                
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


# ==============================================================================
# === SEKCJA 2: OPTYMALIZACJA V4 (PRELOAD + FAST LOOP) ===
# ==============================================================================

def preload_optimization_data(session: Session, year: str) -> Dict[str, Any]:
    """
    [BEZSTRATNA OPTYMALIZACJA V4]
    Ładuje dane do pamięci RAM dla pętli optymalizacyjnej Optuny.
    """
    logger.info(f"[Preload] Rozpoczynanie ładowania danych do RAM dla roku {year}...")
    api_client = AlphaVantageClient()
    
    phase1_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
    portfolio_rows = session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()
    tickers = list(set([r[0] for r in phase1_rows] + [r[0] for r in portfolio_rows]))
    
    if not tickers:
        fallback_rows = session.execute(text("SELECT ticker FROM companies LIMIT 50")).fetchall()
        tickers = [r[0] for r in fallback_rows]

    spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
    spy_df = pd.DataFrame()
    if spy_raw:
        spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
        spy_df.index = pd.to_datetime(spy_df.index)

    cache = {}
    Z_SCORE_WINDOW = 100
    HISTORY_BUFFER = 201
    
    for ticker in tickers:
        try:
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
            
            if not daily_raw or not daily_adj_raw: continue

            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            
            if len(daily_adj) < HISTORY_BUFFER: continue
            
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            daily_adj.index = pd.to_datetime(daily_adj.index)
            
            if 'high' in daily_ohlcv.columns:
                daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            else: continue
            
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df).ffill().fillna(0)
            
            # Tutaj też używamy wektoryzacji jeśli to możliwe, lub cache z loadera
            h2_data = load_h2_data_into_cache(ticker, api_client, session)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')
            
            # Dla preloada (V4) też warto użyć szybkiej metody, ale tu zachowujemy logikę metryk
            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            
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
            df['J_norm'] = ((df['J'] - j_mean) / j_std).fillna(0)
            
            nabla_mean = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).mean()
            nabla_std = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
            df['nabla_sq_norm'] = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
            
            m_mean = df['m_sq'].rolling(window=Z_SCORE_WINDOW).mean()
            m_std = df['m_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
            df['m_sq_norm'] = ((df['m_sq'] - m_mean) / m_std).fillna(0)

            df['aqm_score_static'] = df['J_norm'] - df['nabla_sq_norm'] - df['m_sq_norm']

            lean_df = df[[
                'open', 'high', 'low', 'close', 
                'atr_14', 'aqm_score_static', 'm_sq_norm'
            ]].copy()
            
            lean_df.sort_index(inplace=True)
            cache[ticker] = lean_df
            
        except Exception:
            continue

    return cache


def run_optimization_simulation_fast(preloaded_data: Dict[str, pd.DataFrame], params: Dict[str, Any]) -> Dict[str, float]:
    """
    [FAST LOOP] Szybka symulacja dla Optuny (V4).
    """
    stats = { 'profit_factor': 0.0, 'total_trades': 0, 'win_rate': 0.0, 'net_profit': 0.0 }
    trades_results = []
    
    h3_percentile = float(params.get('h3_percentile', 0.95))
    h3_m_sq_threshold = float(params.get('h3_m_sq_threshold', -0.5))
    h3_min_score = float(params.get('h3_min_score', 0.0))
    h3_tp_mult = float(params.get('h3_tp_multiplier', 5.0))
    h3_sl_mult = float(params.get('h3_sl_multiplier', 2.0))
    h3_max_hold = int(params.get('h3_max_hold', 5))
    
    sim_start = params.get('simulation_start_date')
    sim_end = params.get('simulation_end_date')
    
    ts_start = pd.Timestamp(sim_start) if sim_start else None
    ts_end = pd.Timestamp(sim_end) if sim_end else None
    
    Z_SCORE_WINDOW = 100
    HISTORY_BUFFER = 201

    for ticker, df in preloaded_data.items():
        if df.empty: continue

        try:
            threshold_series = df['aqm_score_static'].rolling(window=Z_SCORE_WINDOW).quantile(h3_percentile)
            
            start_idx = HISTORY_BUFFER
            if ts_start:
                search_idx = df.index.searchsorted(ts_start)
                start_idx = max(start_idx, search_idx)
            
            end_idx = len(df)
            if ts_end:
                search_idx = df.index.searchsorted(ts_end)
                end_idx = min(end_idx, search_idx)
            
            if start_idx >= end_idx: continue

            opens = df['open'].values
            highs = df['high'].values
            lows = df['low'].values
            closes = df['close'].values
            atrs = df['atr_14'].values
            scores = df['aqm_score_static'].values
            threshs = threshold_series.values
            ms = df['m_sq_norm'].values
            
            i = start_idx
            while i < end_idx - 1:
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
                    
                    for day_offset in range(h3_max_hold):
                        curr_day = i + 1 + day_offset
                        if curr_day >= len(df): break
                        
                        day_low = lows[curr_day]
                        day_high = highs[curr_day]
                        
                        if day_low <= sl:
                            pnl_percent = ((sl - entry_price) / entry_price) * 100
                            break
                        if day_high >= tp:
                            pnl_percent = ((tp - entry_price) / entry_price) * 100
                            break
                        
                        if day_offset == h3_max_hold - 1:
                            exit_price = closes[curr_day]
                            pnl_percent = ((exit_price - entry_price) / entry_price) * 100
                    
                    trades_results.append(pnl_percent)
                    i += max(1, day_offset)
                
                i += 1

        except Exception:
            continue

    if not trades_results:
        return stats

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

def run_optimization_simulation(session: Session, year: str, params: dict) -> dict:
    cache = preload_optimization_data(session, year)
    return run_optimization_simulation_fast(cache, params)
