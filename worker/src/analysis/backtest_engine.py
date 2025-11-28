import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone

# Importy narzędziowe
from .utils import (
    get_raw_data_with_cache, 
    standardize_df_columns, 
    calculate_atr, 
    append_scan_log, 
    update_scan_progress,
    calculate_h3_metrics_v4,
    _resolve_trade # Używamy pomocniczej funkcji do rozwiązywania transakcji jeśli jest dostępna, lub zaimplementujemy in-line
)

# Importy analityczne (H2/H3)
from .aqm_v3_h2_loader import load_h2_data_into_cache
from . import aqm_v3_metrics

# Importy analityczne (AQM V4)
from . import aqm_v4_logic

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

logger = logging.getLogger(__name__)

# Pomocnicza funkcja do Time Dilation (dla H3)
def _calculate_time_dilation_series(ticker_df: pd.DataFrame, spy_df: pd.DataFrame, window: int = 20) -> pd.Series:
    try:
        if not isinstance(ticker_df.index, pd.DatetimeIndex): ticker_df.index = pd.to_datetime(ticker_df.index)
        if not isinstance(spy_df.index, pd.DatetimeIndex): spy_df.index = pd.to_datetime(spy_df.index)
        
        # Wyrównanie indeksów (ffill dla SPY)
        spy_aligned = spy_df['close'].reindex(ticker_df.index, method='ffill').fillna(0)
        
        ticker_returns = ticker_df['close'].pct_change()
        spy_returns = spy_aligned.pct_change().fillna(0)

        ticker_std = ticker_returns.rolling(window=window).std()
        spy_std = spy_returns.rolling(window=window).std()
        
        time_dilation = ticker_std / spy_std.replace(0, np.nan)
        return time_dilation.fillna(0)
    except Exception:
        return pd.Series(0, index=ticker_df.index)

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    """
    Główny wrapper uruchamiający backtest.
    """
    strategy_mode = 'H3'
    if parameters and parameters.get('strategy_mode') == 'AQM':
        strategy_mode = 'AQM'
        
    logger.info(f"[Backtest] Start analizy historycznej {year} (Strategia: {strategy_mode})...")
    append_scan_log(session, f"BACKTEST: Uruchamianie symulacji dla roku {year} (Strategia: {strategy_mode})...")
    
    return _run_historical_backtest_unified(session, api_client, year, parameters, strategy_mode)

def _run_historical_backtest_unified(session: Session, api_client, year: str, parameters: dict = None, strategy_mode: str = 'H3'):
    """
    Kompletny, ujednolicony silnik backtestu zawierający PEŁNĄ logikę H3 oraz AQM.
    """
    try:
        # ==============================================================================
        # 1. SELEKCJA UNIWERSUM
        # ==============================================================================
        phase1_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        tickers = [r[0] for r in phase1_rows]
        
        # Pobieramy też z portfela
        port_rows = session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()
        tickers += [r[0] for r in port_rows]
        tickers = list(set(tickers))
        
        if not tickers:
            logger.warning("Brak kandydatów Fazy 1. Pobieram próbkę z bazy.")
            tickers = [r[0] for r in session.execute(text("SELECT ticker FROM companies LIMIT 100")).fetchall()]
        
        logger.info(f"[Backtest] Wybrano {len(tickers)} tickerów do analizy.")
        
        # ==============================================================================
        # 2. PARAMETRY I DANE MAKRO
        # ==============================================================================
        params = parameters or {}
        
        # Wspólne parametry wyjścia
        tp_mult = float(params.get('h3_tp_multiplier', 5.0))
        sl_mult = float(params.get('h3_sl_multiplier', 2.0))
        max_hold = int(params.get('h3_max_hold', 5))
        
        setup_name_base = f"{strategy_mode}_BACKTEST"
        setup_name_suffix = str(params.get('setup_name', ''))
        if setup_name_suffix: setup_name_base += f"_{setup_name_suffix}"

        start_date_ts = pd.Timestamp(f"{year}-01-01").tz_localize(None)
        end_date_ts = pd.Timestamp(f"{year}-12-31").tz_localize(None)

        # Pobranie SPY (potrzebne dla H3 Time Dilation i AQM MRS)
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        spy_df = pd.DataFrame()
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index).tz_localize(None)
            spy_df.sort_index(inplace=True)

        # Kontekst Makro dla AQM
        macro_data = {'spy_df': spy_df, 'vix': 20.0, 'sector_trend': 0.0}

        total_tickers = len(tickers)
        processed_count = 0
        trades_generated = 0
        
        # ==============================================================================
        # 3. GŁÓWNA PĘTLA PRZETWARZANIA
        # ==============================================================================
        for ticker in tickers:
            try:
                # A. POBIERANIE DANYCH PODSTAWOWYCH (OHLCV)
                daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
                
                if not daily_raw: continue

                # Budowa DataFrame OHLCV
                ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
                ohlcv.index = pd.to_datetime(ohlcv.index).tz_localize(None)
                
                # Budowa DataFrame Adjusted (dla poprawnej historii)
                adj = pd.DataFrame()
                if daily_adj_raw:
                    adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
                    adj.index = pd.to_datetime(adj.index).tz_localize(None)
                
                # Łączenie (Preferujemy Adjusted dla analizy, OHLCV dla cen transakcyjnych)
                if not adj.empty:
                    # Dołączamy prawdziwe ceny otwarcia/zamknięcia do skorygowanych danych
                    df = adj.join(ohlcv[['open', 'high', 'low', 'close']], rsuffix='_raw')
                    # Używamy surowych cen do symulacji transakcji (bo takie były na rynku)
                    trade_open_col = 'open_raw' if 'open_raw' in df.columns else 'open'
                    trade_high_col = 'high_raw' if 'high_raw' in df.columns else 'high'
                    trade_low_col = 'low_raw' if 'low_raw' in df.columns else 'low'
                    trade_close_col = 'close_raw' if 'close_raw' in df.columns else 'close'
                else:
                    df = ohlcv
                    trade_open_col, trade_high_col, trade_low_col, trade_close_col = 'open', 'high', 'low', 'close'

                df.sort_index(inplace=True)
                if len(df) < 201: continue # Wymagany bufor
                
                df['atr_14'] = calculate_atr(df).ffill().fillna(0)

                # DataFrame z sygnałami
                signal_df = pd.DataFrame()

                # ==========================================================
                # ŚCIEŻKA 1: STRATEGIA H3 (Elite Sniper Logic) - PEŁNA WERSJA
                # ==========================================================
                if strategy_mode == 'H3':
                    # 1. Dane Wymiaru 2 (H2)
                    h2_data = load_h2_data_into_cache(ticker, api_client, session)
                    insider_df = h2_data.get('insider_df')
                    news_df = h2_data.get('news_df')
                    
                    # 2. Obliczanie metryk H2
                    df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
                    df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
                    
                    # 3. Metryki H1
                    df['price_gravity'] = (df['high'] + df['low'] + df['close']) / 3 / df['close'] - 1
                    df['time_dilation'] = _calculate_time_dilation_series(df, spy_df)
                    
                    # 4. Fizyka H3 (Entropy, Temp)
                    df['daily_returns'] = df['close'].pct_change()
                    df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
                    
                    if not news_df.empty:
                        # Przetwarzanie newsów dla entropii
                        if news_df.index.tz is not None: news_df.index = news_df.index.tz_localize(None)
                        nc = news_df.groupby(news_df.index.date).size()
                        nc.index = pd.to_datetime(nc.index)
                        nc = nc.reindex(df.index, fill_value=0)
                        df['information_entropy'] = nc.rolling(window=10).sum()
                    else:
                        df['information_entropy'] = 0.0
                    
                    # 5. Obliczanie AQM V3 Score (wzory pola)
                    # Masa
                    df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
                    df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
                    df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
                    df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                    df['m_sq'] = df['normalized_volume'] # (News component skipped in H3 optimizer version)
                    df['nabla_sq'] = df['price_gravity']

                    # Normalizacja
                    window_z = 100
                    mu_mean = df['institutional_sync'].rolling(window_z).mean()
                    mu_std = df['institutional_sync'].rolling(window_z).std().replace(0, 1)
                    df['mu_normalized'] = (df['institutional_sync'] - mu_mean) / mu_std
                    
                    df['retail_herding_capped'] = df['retail_herding'].clip(-1.0, 1.0)
                    
                    S = df['information_entropy']
                    Q = df['retail_herding_capped']
                    T = df['market_temperature'].replace(0, np.nan).fillna(0.0001)
                    
                    df['J'] = S - (Q / T) + (df['mu_normalized'] * 1.0)
                    
                    # Składniki znormalizowane
                    j_mean = df['J'].rolling(window_z).mean()
                    j_std = df['J'].rolling(window_z).std().replace(0, 1)
                    df['J_norm'] = (df['J'] - j_mean) / j_std
                    
                    nabla_mean = df['nabla_sq'].rolling(window_z).mean()
                    nabla_std = df['nabla_sq'].rolling(window_z).std().replace(0, 1)
                    df['nabla_sq_norm'] = (df['nabla_sq'] - nabla_mean) / nabla_std
                    
                    m_mean = df['m_sq'].rolling(window_z).mean()
                    m_std = df['m_sq'].rolling(window_z).std().replace(0, 1)
                    df['m_sq_norm'] = (df['m_sq'] - m_mean) / m_std
                    
                    # Final Score H3
                    df['aqm_score_h3'] = df['J_norm'] - df['nabla_sq_norm'] - df['m_sq_norm']
                    df['aqm_rank'] = df['aqm_score_h3'].rolling(window=100).rank(pct=True).fillna(0)
                    
                    # Filtrowanie sygnałów H3
                    h3_p = float(params.get('h3_percentile', 0.95))
                    h3_m = float(params.get('h3_m_sq_threshold', -0.5))
                    h3_min = float(params.get('h3_min_score', 0.0))
                    
                    # Maska sygnału
                    df['is_signal'] = (
                        (df['aqm_rank'] > h3_p) & 
                        (df['m_sq_norm'] < h3_m) & 
                        (df['aqm_score_h3'] > h3_min)
                    )
                    
                    # Przygotowanie DF do pętli symulacyjnej
                    # (zachowujemy kolumny potrzebne do logowania metryk)
                    signal_df = df

                # ==========================================================
                # ŚCIEŻKA 2: STRATEGIA AQM (Adaptive Quantum Momentum) - V4
                # ==========================================================
                elif strategy_mode == 'AQM':
                    # 1. Pobieranie danych specyficznych dla AQM
                    w_raw = get_raw_data_with_cache(session, api_client, ticker, 'WEEKLY_ADJUSTED', 'get_weekly_adjusted')
                    weekly_df = pd.DataFrame()
                    if w_raw: 
                        weekly_df = standardize_df_columns(pd.DataFrame.from_dict(w_raw.get('Weekly Adjusted Time Series', {}), orient='index'))
                        weekly_df.index = pd.to_datetime(weekly_df.index).tz_localize(None)
                    
                    obv_raw = get_raw_data_with_cache(session, api_client, ticker, 'OBV', 'get_obv')
                    obv_df = pd.DataFrame()
                    if obv_raw: 
                        obv_df = pd.DataFrame.from_dict(obv_raw.get('Technical Analysis: OBV', {}), orient='index')
                        obv_df.index = pd.to_datetime(obv_df.index).tz_localize(None)
                        obv_df.rename(columns={'OBV': 'OBV'}, inplace=True)

                    # 2. Obliczanie AQM V4 (Logic Core)
                    # Funkcja calculate_aqm_full_vector zwraca DF z kolumnami: aqm_score, qps, ves, mrs, tcs
                    aqm_metrics_df = aqm_v4_logic.calculate_aqm_full_vector(
                        df, weekly_df, pd.DataFrame(), obv_df, macro_data, None
                    )
                    
                    if not aqm_metrics_df.empty:
                        # Łączymy wyniki z głównym DF (po indeksie daty)
                        df = df.join(aqm_metrics_df[['aqm_score', 'qps', 'ves', 'mrs', 'tcs']], rsuffix='_dupl')
                        
                        # Parametry wejścia dla AQM
                        min_score = float(params.get('aqm_min_score', 0.8)) # Domyślnie wysoki próg
                        comp_min = float(params.get('aqm_component_min', 0.5))
                        
                        # Maska sygnału AQM
                        df['is_signal'] = (
                            (df['aqm_score'] > min_score) &
                            (df['qps'] > comp_min) &
                            (df['ves'] > comp_min) &
                            (df['mrs'] > comp_min)
                        )
                        signal_df = df
                    else:
                        signal_df = pd.DataFrame() # Brak danych AQM

                # ==============================================================================
                # 4. PĘTLA SYMULACYJNA (WSPÓLNA DLA OBU STRATEGII)
                # ==============================================================================
                
                if not signal_df.empty and 'is_signal' in signal_df.columns:
                    # Szukamy startu roku
                    sim_start_idx = signal_df.index.searchsorted(start_date_ts)
                    
                    i = sim_start_idx
                    # Iterujemy do przedostatniego dnia (żeby mieć D+1 na wejście)
                    while i < len(signal_df) - 1:
                        current_date = signal_df.index[i]
                        if current_date > end_date_ts: break
                        
                        # Sprawdzamy sygnał na zamknięciu dnia 'i'
                        if signal_df['is_signal'].iloc[i]:
                            row = signal_df.iloc[i]
                            
                            # WEJŚCIE: Na otwarciu dnia i+1
                            next_day_row = signal_df.iloc[i+1]
                            entry_price = next_day_row[trade_open_col]
                            
                            # Parametry transakcji
                            atr = row['atr_14']
                            if atr <= 0 or entry_price <= 0:
                                i += 1
                                continue
                                
                            tp_price = entry_price + (tp_mult * atr)
                            sl_price = entry_price - (sl_mult * atr)
                            
                            # Symulacja przebiegu transakcji (Hold Period)
                            trade_status = 'CLOSED_EXPIRED' # Domyślnie
                            close_price = entry_price
                            close_date = next_day_row.name
                            days_held = 0
                            
                            # Pętla sprawdzająca kolejne dni (max_hold)
                            for h in range(max_hold):
                                # Sprawdzamy dzień i+1+h
                                day_idx = i + 1 + h
                                if day_idx >= len(signal_df): 
                                    # Koniec danych - zamykamy na ostatnim Close
                                    close_price = signal_df.iloc[-1][trade_close_col]
                                    close_date = signal_df.index[-1]
                                    break
                                
                                day_candle = signal_df.iloc[day_idx]
                                d_open = day_candle[trade_open_col]
                                d_high = day_candle[trade_high_col]
                                d_low = day_candle[trade_low_col]
                                d_close = day_candle[trade_close_col]
                                
                                days_held += 1
                                close_date = day_candle.name
                                
                                # 1. Sprawdzenie luki na otwarciu (Gap Down poniżej SL)
                                if d_open <= sl_price:
                                    trade_status = 'CLOSED_SL'
                                    close_price = d_open
                                    break
                                
                                # 2. Sprawdzenie intraday Low (SL)
                                if d_low <= sl_price:
                                    trade_status = 'CLOSED_SL'
                                    close_price = sl_price
                                    break
                                
                                # 3. Sprawdzenie intraday High (TP)
                                if d_high >= tp_price:
                                    trade_status = 'CLOSED_TP'
                                    close_price = tp_price
                                    break
                                
                                # 4. Jeśli to ostatni dzień trzymania -> wyjście czasowe
                                if h == max_hold - 1:
                                    trade_status = 'CLOSED_EXPIRED'
                                    close_price = d_close
                                    break
                            
                            # Obliczenie wyniku
                            p_l_percent = ((close_price - entry_price) / entry_price) * 100
                            
                            # Logowanie metryk (zależnie od strategii)
                            metric_score = 0.0
                            if strategy_mode == 'H3':
                                metric_score = float(row.get('aqm_score_h3', 0))
                            elif strategy_mode == 'AQM':
                                metric_score = float(row.get('aqm_score', 0))

                            # Zapis do bazy
                            trade_data = {
                                "ticker": ticker,
                                "setup_type": setup_name_base,
                                "entry_price": float(entry_price),
                                "stop_loss": float(sl_price),
                                "take_profit": float(tp_price),
                                "metric_aqm_score_h3": metric_score,
                                "metric_atr_14": float(atr),
                                # Dodatkowe metryki diagnostyczne
                                "metric_J_norm": float(row.get('J_norm', 0)) if strategy_mode == 'H3' else float(row.get('qps', 0)), # Mapping QPS to J_norm slot for view
                                "metric_nabla_sq_norm": float(row.get('nabla_sq_norm', 0)) if strategy_mode == 'H3' else float(row.get('ves', 0)),
                                "metric_m_sq_norm": float(row.get('m_sq_norm', 0)) if strategy_mode == 'H3' else float(row.get('mrs', 0)),
                                
                                "status": trade_status,
                                "close_price": float(close_price),
                                "final_profit_loss_percent": float(p_l_percent),
                                "open_date": next_day_row.name,
                                "close_date": close_date
                            }
                            
                            vt = models.VirtualTrade(**trade_data)
                            session.add(vt)
                            trades_generated += 1
                            
                            # Przesuwamy główny licznik 'i' o czas trwania transakcji (żeby nie otwierać nowych w trakcie)
                            i += max(1, days_held)
                        else:
                            i += 1
                
                processed_count += 1
                if processed_count % 5 == 0: 
                    update_scan_progress(session, processed_count, total_tickers)
                
                if processed_count % 20 == 0:
                    session.commit() # Batch commit

            except Exception as e:
                logger.error(f"Błąd backtestu dla {ticker}: {e}")
                session.rollback()
                continue

        # Finalizacja
        session.commit()
        update_scan_progress(session, total_tickers, total_tickers)
        summary = f"BACKTEST: Zakończono dla roku {year}. Wygenerowano {trades_generated} transakcji."
        logger.info(summary)
        append_scan_log(session, summary)

    except Exception as e:
        err_msg = f"Krytyczny błąd Backtestu Unified: {e}"
        logger.error(err_msg, exc_info=True)
        append_scan_log(session, err_msg)
