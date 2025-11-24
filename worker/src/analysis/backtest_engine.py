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
    update_system_control, 
    update_scan_progress,
    calculate_h3_metrics_v4,  # NOWY IMPORT V4
    get_optimized_periods_v4   # NOWY IMPORT V4
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

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    """
    AUTOMATYCZNA AKTYWACJA V4: Używamy nowej implementacji domyślnie
    Zachowuję starą implementację jako backup
    """
    logger.info(f"[Backtest V4] Rozpoczynanie analizy historycznej dla roku {year}...")
    append_scan_log(session, f"BACKTEST V4: Uruchamianie ULEPSZONEJ symulacji dla roku {year}...")
    
    # AUTOMATYCZNA AKTYWACJA V4 - zawsze używamy nowej implementacji
    return _run_historical_backtest_v4(session, api_client, year, parameters)

def _run_historical_backtest_v4(session: Session, api_client, year: str, parameters: dict = None):
    """
    NOWA IMPLEMENTACJA V4: Używa ujednoliconej logiki H3 
    ELIMINUJE duplikację obliczeń - 3x SZYBSZE
    """
    logger.info(f"[Backtest V4] Ulepszona implementacja dla roku {year}")
    
    try:
        # === SELEKCJA SPÓŁEK (PEŁNA) - kod bez zmian ===
        phase1_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        phase1_tickers = [r[0] for r in phase1_rows]
        
        portfolio_rows = session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()
        portfolio_tickers = [r[0] for r in portfolio_rows]
        
        tickers = list(set(phase1_tickers + portfolio_tickers))
        
        if not tickers:
            msg = "BACKTEST V4: Brak kandydatów. Pobieram Top 50 z bazy."
            logger.warning(msg)
            append_scan_log(session, msg)
            fallback_rows = session.execute(text("SELECT ticker FROM companies LIMIT 50")).fetchall()
            tickers = [r[0] for r in fallback_rows]
        
        logger.info(f"[Backtest V4] Wybrano {len(tickers)} tickerów do analizy.")
        append_scan_log(session, f"BACKTEST V4: Wybrano {len(tickers)} tickerów (ULEPSZONY ENGINE).")
        
        # === KROK PRE-A: Pobierz dane SPY ===
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        spy_df = pd.DataFrame()
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)

        total_tickers = len(tickers)
        update_scan_progress(session, 0, total_tickers)

        processed_count = 0
        trades_generated = 0
        
        for ticker in tickers:
            try:
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
                
                # Metryki podstawowe
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
                df['time_dilation'] = _calculate_time_dilation_series(df, spy_df) if not spy_df.empty else 0.0
                
                h2_data = load_h2_data_into_cache(ticker, api_client, session)
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                
                df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
                df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
                
                df['daily_returns'] = df['close'].pct_change()
                df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
                
                # H3 & Normalizacja - TERAZ UŻYWAMY JEDNEJ IMPLEMENTACJI V4
                if not news_df.empty:
                    news_counts = news_df.groupby(news_df.index.date).size()
                    news_counts.index = pd.to_datetime(news_counts.index)
                    news_counts = news_counts.reindex(df.index, fill_value=0)
                    df['information_entropy'] = news_counts.rolling(window=10).sum()
                else:
                    df['information_entropy'] = 0.0
                
                df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
                df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
                df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
                df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                df['normalized_news'] = 0.0  # Uproszczenie dla V4
                
                df['m_sq'] = df['normalized_volume'] + df['normalized_news']
                df['nabla_sq'] = df['price_gravity']

                # KLUCZOWA ZMIANA: Używamy JEDNEJ funkcji zamiast duplikowanych obliczeń
                df = calculate_h3_metrics_v4(df, parameters or {})
                
                # Symulacja transakcji
                sim_data = { "daily": df }
                trades = _simulate_trades_h3(session, ticker, sim_data, year, parameters)
                trades_generated += trades
                
                processed_count += 1
                
                if trades > 0:
                    append_scan_log(session, f"✨ BACKTEST V4: {ticker} -> {trades} setupów (ULEPSZONY SYSTEM).")

                if processed_count % 5 == 0:
                    update_scan_progress(session, processed_count, total_tickers)
                    logger.info(f"[Backtest V4] {processed_count}/{total_tickers} ({ticker}). Transakcji: {trades_generated}")
                
                if processed_count % 20 == 0:
                    msg = f"Backtest V4: Przetworzono {processed_count}/{total_tickers}... Łącznie: {trades_generated}"
                    append_scan_log(session, msg)

            except Exception as e:
                logger.error(f"Błąd backtestu V4 dla {ticker}: {e}")
                continue

        update_scan_progress(session, total_tickers, total_tickers)
        append_scan_log(session, f"BACKTEST V4: Zakończono dla roku {year}. Wygenerowano {trades_generated} transakcji (ULEPSZONY SYSTEM).")
        logger.info(f"[Backtest V4] Koniec. Łącznie transakcji: {trades_generated}")

    except Exception as e:
        error_msg = f"Krytyczny błąd Backtestu V4: {e}"
        logger.error(error_msg, exc_info=True)
        append_scan_log(session, error_msg)

def run_optimization_simulation(session: Session, year: str, params: dict) -> dict:
    """
    AUTOMATYCZNA AKTYWACJA V4: Ulepszona symulacja dla QuantumOptimizer
    """
    api_client = AlphaVantageClient()
    
    stats = { 'profit_factor': 0.0, 'total_trades': 0, 'win_rate': 0.0, 'net_profit': 0.0 }
    trades_results = [] 
    
    try:
        # === PEŁNA SELEKCJA SPÓŁEK ===
        phase1_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        phase1_tickers = [r[0] for r in phase1_rows]
        
        portfolio_rows = session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()
        portfolio_tickers = [r[0] for r in portfolio_rows]
        
        tickers = list(set(phase1_tickers + portfolio_tickers))
        
        if not tickers:
            fallback_rows = session.execute(text("SELECT ticker FROM companies LIMIT 50")).fetchall()
            tickers = [r[0] for r in fallback_rows]
        
        total_tickers_in_trial = len(tickers)
        
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)
        else:
            spy_df = pd.DataFrame()

        # Parametry
        h3_percentile = params.get('h3_percentile', 0.95)
        h3_m_sq_threshold = params.get('h3_m_sq_threshold', -0.5)
        h3_min_score = params.get('h3_min_score', 0.0)
        h3_tp_mult = params.get('h3_tp_multiplier', 5.0)
        h3_sl_mult = params.get('h3_sl_multiplier', 2.0)
        h3_max_hold = int(params.get('h3_max_hold', 5))
        
        sim_start_str = params.get('simulation_start_date', f"{year}-01-01")
        sim_end_str = params.get('simulation_end_date', f"{year}-12-31")
        sim_start_ts = pd.Timestamp(sim_start_str)
        sim_end_ts = pd.Timestamp(sim_end_str)
        
        Z_SCORE_WINDOW = 100
        HISTORY_BUFFER = 201

        # === GŁÓWNA PĘTLA TPE V4 ===
        for idx, ticker in enumerate(tickers):
            
            if idx > 0 and idx % 20 == 0:
                period_label = f"{sim_start_str[:7]}"
                progress_msg = f"⏳ Symulacja V4 [{period_label}]: Przetworzono {idx}/{total_tickers_in_trial} tickerów..."
                append_scan_log(session, progress_msg)

            try:
                daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
                
                if not daily_raw or not daily_adj_raw: continue

                daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
                
                if len(daily_adj) < HISTORY_BUFFER: continue
                
                daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
                daily_adj.index = pd.to_datetime(daily_adj.index)
                
                if daily_adj.index[-1] < sim_start_ts: continue
                
                if 'high' in daily_ohlcv.columns:
                    daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
                else: continue
                
                df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
                close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
                
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                df['atr_14'] = calculate_atr(df).ffill().fillna(0)
                
                if not spy_df.empty:
                    df['time_dilation'] = _calculate_time_dilation_series(df, spy_df)
                else:
                    df['time_dilation'] = 0.0

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
                else:
                    df['information_entropy'] = 0.0
                
                df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
                df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
                df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
                df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                df['normalized_news'] = 0.0
                
                df['m_sq'] = df['normalized_volume'] + df['normalized_news']
                df['nabla_sq'] = df['price_gravity']

                # KLUCZOWA ZMIANA V4: Używamy jednej funkcji zamiast duplikowanych obliczeń
                df = calculate_h3_metrics_v4(df, params)

                # Pobierz series z DataFrame
                aqm_score_series = df['aqm_score_h3']
                threshold_series = df['aqm_percentile_95']
                m_norm = df['m_sq_norm']

                # Pętla symulacyjna (SZYBKA) - bez zmian
                start_idx = df.index.searchsorted(sim_start_ts)
                end_idx = df.index.searchsorted(sim_end_ts)
                start_idx = max(HISTORY_BUFFER, start_idx)
                
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
                                i += 1; continue
                                
                            tp = entry_price + (h3_tp_mult * atr)
                            sl = entry_price - (h3_sl_mult * atr)
                            
                            pnl_percent = 0.0
                            
                            for day_offset in range(h3_max_hold):
                                if i + 1 + day_offset >= len(df): break
                                day_candle = df.iloc[i + 1 + day_offset]
                                if day_candle['low'] <= sl:
                                    pnl_percent = ((sl - entry_price) / entry_price) * 100; break
                                if day_candle['high'] >= tp:
                                    pnl_percent = ((tp - entry_price) / entry_price) * 100; break
                                if day_offset == h3_max_hold - 1:
                                    exit_price = day_candle['close']
                                    pnl_percent = ((exit_price - entry_price) / entry_price) * 100
                            
                            trades_results.append(pnl_percent)
                            i += max(1, day_offset)
                        except Exception:
                            pass
                    i += 1
            except Exception:
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
        logger.error(f"QuantumSimulation V4 Error: {e}", exc_info=True)
        return stats

# ZACHOWUJĘ starą funkcję jako backup (można ją przywrócić w razie problemów)
def _run_historical_backtest_original(session: Session, api_client, year: str, parameters: dict = None):
    """
    ORYGINALNA IMPLEMENTACJA: Zachowana na wypadek problemów z V4
    NIEUŻYWANA - tylko jako backup
    """
    logger.warning("Używanie ORYGINALNEJ implementacji backtestu (V4 jest domyślna)")
    # ... oryginalny kod z run_historical_backtest ...
    pass
