import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

# Importy narzędziowe
from .utils import (
    get_raw_data_with_cache, 
    standardize_df_columns, 
    calculate_atr, 
    append_scan_log, 
    update_system_control
)

# Importy z nowych modułów analitycznych (V3)
from .aqm_v3_h2_loader import load_h2_data_into_cache
from .aqm_v3_h3_simulator import _simulate_trades_h3
from . import aqm_v3_metrics
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

logger = logging.getLogger(__name__)

# Funkcja pomocnicza do obliczania Time Dilation (lokalnie w silniku)
def _calculate_time_dilation_series(ticker_df: pd.DataFrame, spy_df: pd.DataFrame, window: int = 20) -> pd.Series:
    """Oblicza serię Time Dilation (Zmienność Tickera / Zmienność SPY)."""
    try:
        # Upewniamy się, że indeksy są datetime
        if not isinstance(ticker_df.index, pd.DatetimeIndex):
            ticker_df.index = pd.to_datetime(ticker_df.index)
        if not isinstance(spy_df.index, pd.DatetimeIndex):
            spy_df.index = pd.to_datetime(spy_df.index)

        # Oblicz zwroty
        ticker_returns = ticker_df['close'].pct_change()
        spy_returns = spy_df['close'].pct_change()
        
        # Dopasuj SPY do dat tickera (reindex)
        spy_returns = spy_returns.reindex(ticker_returns.index).ffill().fillna(0)

        # Oblicz zmienność kroczącą
        ticker_std = ticker_returns.rolling(window=window).std()
        spy_std = spy_returns.rolling(window=window).std()
        
        # Unikaj dzielenia przez zero
        time_dilation = ticker_std / spy_std.replace(0, np.nan)
        return time_dilation.fillna(0)
    except Exception as e:
        logger.warning(f"Błąd obliczania Time Dilation: {e}")
        return pd.Series(0, index=ticker_df.index)

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    """
    Uruchamia pełny backtest historyczny (z zapisem do bazy).
    """
    logger.info(f"[Backtest] Rozpoczynanie analizy historycznej dla roku {year}...")
    append_scan_log(session, f"BACKTEST: Uruchamianie symulacji dla roku {year}...")
    
    try:
        tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        tickers = [r[0] for r in tickers_rows]
        
        # === KROK PRE-A: Pobierz dane SPY (Benchmark) raz dla całej pętli ===
        # Time Dilation wymaga porównania z rynkiem.
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)
        else:
            logger.warning("Nie udało się pobrać danych SPY. Time Dilation będzie wynosić 0.")
            spy_df = pd.DataFrame()

        total_tickers = len(tickers)
        processed_count = 0
        trades_generated = 0
        
        Z_SCORE_WINDOW = 100
        MARKET_TEMP_WINDOW = 30
        
        for ticker in tickers:
            try:
                # === KROK A: Pobieranie Danych Historycznych ===
                daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
                
                if not daily_raw or not daily_adj_raw: continue

                daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
                
                daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_adj.index = pd.to_datetime(daily_adj.index)
                
                if len(daily_adj) < 201: continue

                if 'high' in daily_ohlcv.columns and 'low' in daily_ohlcv.columns and 'close' in daily_ohlcv.columns:
                    daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
                else: continue

                df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
                close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
                
                # === KROK B: Obliczanie Metryk ===
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
                
                # === NAPRAWA: Obliczanie Time Dilation ===
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
                
                # === KROK C: Konstrukcja Pola H3 ===
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

                sim_data = { "daily": df }
                trades = _simulate_trades_h3(session, ticker, sim_data, year, parameters)
                trades_generated += trades
                
                processed_count += 1
                if processed_count % 10 == 0:
                    logger.info(f"[Backtest] Przetworzono {processed_count}/{total_tickers} spółek. Znaleziono {trades_generated} transakcji.")

            except Exception as e:
                logger.error(f"Błąd backtestu dla {ticker}: {e}")
                continue

        append_scan_log(session, f"BACKTEST: Zakończono dla roku {year}. Wygenerowano {trades_generated} wirtualnych transakcji.")
        logger.info(f"[Backtest] Koniec. Łącznie transakcji: {trades_generated}")

    except Exception as e:
        error_msg = f"Krytyczny błąd Backtestu: {e}"
        logger.error(error_msg, exc_info=True)
        append_scan_log(session, error_msg)


def run_optimization_simulation(session: Session, year: str, params: dict) -> dict:
    """
    Tryb 'Lightweight' dla QuantumOptimizer.
    """
    api_client = AlphaVantageClient()
    
    stats = { 'profit_factor': 0.0, 'total_trades': 0, 'win_rate': 0.0, 'net_profit': 0.0 }
    trades_results = [] 
    
    try:
        tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        tickers = [r[0] for r in tickers_rows]
        
        # === KROK PRE-A: Pobierz dane SPY (Benchmark) ===
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)
        else:
            spy_df = pd.DataFrame()

        # Parametry z Optuny
        h3_percentile = params.get('h3_percentile', 0.95)
        h3_m_sq_threshold = params.get('h3_m_sq_threshold', -0.5)
        h3_min_score = params.get('h3_min_score', 0.0)
        h3_tp_mult = params.get('h3_tp_multiplier', 5.0)
        h3_sl_mult = params.get('h3_sl_multiplier', 2.0)
        h3_max_hold = int(params.get('h3_max_hold', 5))
        
        Z_SCORE_WINDOW = 100
        HISTORY_BUFFER = 201
        target_year_int = int(year)

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
                
                if daily_adj.index[-1].year < target_year_int: continue
                
                if 'high' in daily_ohlcv.columns:
                    daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
                else: continue
                
                df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
                close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
                
                # 2. Obliczanie podstawowych metryk
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                df['atr_14'] = calculate_atr(df).ffill().fillna(0)
                
                # === NAPRAWA: Obliczanie Time Dilation w symulacji ===
                if not spy_df.empty:
                    df['time_dilation'] = _calculate_time_dilation_series(df, spy_df)
                else:
                    df['time_dilation'] = 0.0

                # Ładowanie H2 (Insider/News)
                h2_data = load_h2_data_into_cache(ticker, api_client, session)
                
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                
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
                
                # Score H3
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
                j_norm = ((df['J'] - j_mean) / j_std).fillna(0)
                
                nabla_mean = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).mean()
                nabla_std = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                nabla_norm = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
                
                m_mean = df['m_sq'].rolling(window=Z_SCORE_WINDOW).mean()
                m_std = df['m_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)

                aqm_score_series = j_norm - nabla_norm - m_norm
                threshold_series = aqm_score_series.rolling(window=Z_SCORE_WINDOW).quantile(h3_percentile)

                # 3. Pętla symulacyjna (In-Memory)
                start_idx = max(HISTORY_BUFFER, df.index.searchsorted(pd.Timestamp(f"{target_year}-01-01")))
                end_idx = df.index.searchsorted(pd.Timestamp(f"{target_year}-12-31"))
                
                if start_idx >= len(df) or start_idx >= end_idx: continue

                i = start_idx
                while i < end_idx and i < len(df) - 1:
                    curr_score = aqm_score_series.iloc[i]
                    curr_thresh = threshold_series.iloc[i]
                    curr_m_norm = m_norm.iloc[i]
                    
                    if (curr_score > curr_thresh) and \
                       (curr_m_norm < h3_m_sq_threshold) and \
                       (curr_score > h3_min_score):
                       
                        try:
                            entry_candle = df.iloc[i+1]
                            entry_price = entry_candle['open']
                            atr = df.iloc[i]['atr_14']
                            
                            if pd.isna(entry_price) or pd.isna(atr) or atr == 0:
                                i += 1
                                continue
                                
                            tp = entry_price + (h3_tp_mult * atr)
                            sl = entry_price - (h3_sl_mult * atr)
                            
                            pnl_percent = 0.0
                            exit_price = entry_price 
                            
                            for day_offset in range(h3_max_hold):
                                if i + 1 + day_offset >= len(df): break
                                
                                day_candle = df.iloc[i + 1 + day_offset]
                                if day_candle['low'] <= sl:
                                    exit_price = sl
                                    pnl_percent = ((sl - entry_price) / entry_price) * 100
                                    break
                                if day_candle['high'] >= tp:
                                    exit_price = tp
                                    pnl_percent = ((tp - entry_price) / entry_price) * 100
                                    break
                                
                                if day_offset == h3_max_hold - 1:
                                    exit_price = day_candle['close']
                                    pnl_percent = ((exit_price - entry_price) / entry_price) * 100
                            
                            trades_results.append(pnl_percent)
                            i += max(1, day_offset)
                            
                        except Exception:
                            pass
                    
                    i += 1

            except Exception as e:
                continue

        if not trades_results:
            return stats 

        wins = [r for r in trades_results if r > 0]
        losses = [r for r in trades_results if r <= 0]
        
        total_win_sum = sum(wins)
        total_loss_sum = abs(sum(losses))
        
        pf = total_win_sum / total_loss_sum if total_loss_sum > 0 else 0.0
        win_rate = (len(wins) / len(trades_results)) * 100
        
        stats['profit_factor'] = pf
        stats['total_trades'] = len(trades_results)
        stats['win_rate'] = win_rate
        stats['net_profit'] = sum(trades_results)
        
        return stats

    except Exception as e:
        logger.error(f"QuantumSimulation Error: {e}", exc_info=True)
        return stats
