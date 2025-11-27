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
    update_system_control, 
    update_scan_progress,
    calculate_h3_metrics_v4,
    _resolve_trade, 
    safe_float
)

# Importy z nowych modułów analitycznych
from .aqm_v3_h2_loader import load_h2_data_into_cache
from . import aqm_v3_metrics
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

logger = logging.getLogger(__name__)

def _calculate_time_dilation_series(ticker_df: pd.DataFrame, spy_df: pd.DataFrame, window: int = 20) -> pd.Series:
    """
    Oblicza serię Time Dilation (Zmienność Tickera / Zmienność SPY).
    Wymiar 1.1 z modelu H1.
    """
    try:
        if not isinstance(ticker_df.index, pd.DatetimeIndex): ticker_df.index = pd.to_datetime(ticker_df.index)
        if not isinstance(spy_df.index, pd.DatetimeIndex): spy_df.index = pd.to_datetime(spy_df.index)

        ticker_returns = ticker_df['close'].pct_change()
        spy_returns = spy_df['close'].pct_change()
        # Wypełniamy braki w SPY (np. święta)
        spy_returns = spy_returns.reindex(ticker_returns.index).ffill().fillna(0)

        ticker_std = ticker_returns.rolling(window=window).std()
        spy_std = spy_returns.rolling(window=window).std()
        
        # Unikamy dzielenia przez zero
        time_dilation = ticker_std / spy_std.replace(0, np.nan)
        return time_dilation.fillna(0)
    except Exception:
        return pd.Series(0, index=ticker_df.index)

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    """
    Wrapper uruchamiający główną logikę backtestu.
    """
    logger.info(f"[Backtest V4] Rozpoczynanie analizy historycznej dla roku {year}...")
    append_scan_log(session, f"BACKTEST V4: Uruchamianie PEŁNEJ symulacji dla roku {year} (Logic: No-News Mass + Phase 1 Universe)...")
    
    # Wywołanie właściwej funkcji logicznej
    return _run_historical_backtest_v4(session, api_client, year, parameters)

def _run_historical_backtest_v4(session: Session, api_client, year: str, parameters: dict = None):
    """
    Główny silnik backtestu (Wersja V4/V9).
    Zawiera pełną pętlę symulacyjną inline (bez zewnętrznych zależności logicznych).
    """
    logger.info(f"[Backtest V4] Ulepszona implementacja (Full Inline) dla roku {year}")
    
    try:
        # ==============================================================================
        # 1. SELEKCJA UNIWERSUM (Zgodna z filozofią użytkownika: Faza 1 jest podstawą)
        # ==============================================================================
        
        # Pobieramy kandydatów z Fazy 1
        phase1_rows = session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
        phase1_tickers = [r[0] for r in phase1_rows]
        
        # Pobieramy spółki z portfela (żeby też je sprawdzać)
        portfolio_rows = session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()
        portfolio_tickers = [r[0] for r in portfolio_rows]
        
        # Łączymy listy
        tickers = list(set(phase1_tickers + portfolio_tickers))
        
        # Fallback (Tylko jeśli baza jest pusta, np. po resecie)
        if not tickers:
            msg = "BACKTEST V4: Brak kandydatów Fazy 1. Pobieram Top 100 z bazy (Fallback)."
            logger.warning(msg)
            append_scan_log(session, msg)
            fallback_rows = session.execute(text("SELECT ticker FROM companies LIMIT 100")).fetchall()
            tickers = [r[0] for r in fallback_rows]
        
        logger.info(f"[Backtest V4] Wybrano {len(tickers)} tickerów do analizy.")
        append_scan_log(session, f"BACKTEST V4: Wybrano {len(tickers)} tickerów (Baza: Faza 1/Portfel).")
        
        # ==============================================================================
        # 2. DANE KONTEKSTOWE (SPY)
        # ==============================================================================
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        spy_df = pd.DataFrame()
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)

        # ==============================================================================
        # 3. PARAMETRY SYMULACJI
        # ==============================================================================
        params = parameters or {}
        
        h3_percentile = float(params.get('h3_percentile', 0.95))
        h3_m_sq_threshold = float(params.get('h3_m_sq_threshold', -0.5))
        h3_min_score = float(params.get('h3_min_score', 0.0))
        h3_tp_mult = float(params.get('h3_tp_multiplier', 5.0))
        h3_sl_mult = float(params.get('h3_sl_multiplier', 2.0))
        h3_max_hold = int(params.get('h3_max_hold', 5))
        setup_name_suffix = str(params.get('setup_name', 'H3_V4_INLINE'))

        # Ustalanie zakresu dat
        start_date_ts = pd.Timestamp(f"{year}-01-01").tz_localize(None)
        end_date_ts = pd.Timestamp(f"{year}-12-31").tz_localize(None)

        total_tickers = len(tickers)
        update_scan_progress(session, 0, total_tickers)

        processed_count = 0
        trades_generated = 0
        
        # ==============================================================================
        # 4. GŁÓWNA PĘTLA PRZETWARZANIA TICKERÓW
        # ==============================================================================
        for ticker in tickers:
            try:
                # A. POBIERANIE DANYCH
                # OHLCV (dane surowe, potrzebne do High/Low/Open)
                daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                # Adjusted (dane skorygowane, potrzebne do analizy długoterminowej)
                daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
                
                if not daily_raw or not daily_adj_raw: continue

                # B. PRZETWARZANIE DATAFRAME
                daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index).tz_localize(None)
                
                daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_adj.index = pd.to_datetime(daily_adj.index).tz_localize(None)
                
                if len(daily_adj) < 201: continue # Wymagany bufor historii

                # Łączenie danych (Left Join na Adjusted, dodajemy kolumny OHLCV)
                if 'high' in daily_ohlcv.columns:
                    daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
                else: continue

                df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'close', 'vwap_proxy']], rsuffix='_ohlcv')
                
                # Definicja kolumn cenowych do handlu (używamy _ohlcv, bo to realne ceny transakcyjne)
                trade_open_col = 'open_ohlcv' if 'open_ohlcv' in df.columns else 'open'
                trade_high_col = 'high_ohlcv' if 'high_ohlcv' in df.columns else 'high'
                trade_low_col = 'low_ohlcv' if 'low_ohlcv' in df.columns else 'low'
                trade_close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'

                # C. OBLICZANIE WSKAŹNIKÓW (FIZYKA H3)
                df['price_gravity'] = (df['vwap_proxy'] - df[trade_close_col]) / df[trade_close_col]
                df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
                df['time_dilation'] = _calculate_time_dilation_series(df, spy_df) if not spy_df.empty else 0.0
                
                # D. DANE H2 (SENTYMENT / INSIDER)
                h2_data = load_h2_data_into_cache(ticker, api_client, session)
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                
                df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
                df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
                
                df['daily_returns'] = df[trade_close_col].pct_change()
                df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
                
                if not news_df.empty:
                    news_counts = news_df.groupby(news_df.index.date).size()
                    news_counts.index = pd.to_datetime(news_counts.index)
                    news_counts = news_counts.reindex(df.index, fill_value=0)
                    df['information_entropy'] = news_counts.rolling(window=10).sum()
                else:
                    df['information_entropy'] = 0.0
                
                # E. OBLICZANIE AQM (WZORY POLA)
                df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
                df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
                df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
                df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                
                # !!! KLUCZOWE: WYMUSZONA SPÓJNOŚĆ Z OPTIMIZEREM !!!
                # Ignorujemy newsy w obliczeniach masy m^2.
                df['normalized_news'] = 0.0 
                
                df['m_sq'] = df['normalized_volume'] + df['normalized_news']
                df['nabla_sq'] = df['price_gravity']

                # Normalizacja Z-Score dla składników AQM (Okno 100 dni)
                window_z = 100
                
                # J (Information Flow)
                S = df['information_entropy']
                Q = df['retail_herding'].clip(-1.0, 1.0) # Capped
                T = df['market_temperature'].replace(0, np.nan).fillna(0.0001)
                
                # Institutional Sync Normalized
                mu_mean = df['institutional_sync'].rolling(window_z).mean()
                mu_std = df['institutional_sync'].rolling(window_z).std().replace(0, 1)
                mu_norm = (df['institutional_sync'] - mu_mean) / mu_std
                
                df['J'] = S - (Q / T) + (mu_norm * 1.0)
                
                j_mean = df['J'].rolling(window_z).mean()
                j_std = df['J'].rolling(window_z).std().replace(0, 1)
                df['J_norm'] = (df['J'] - j_mean) / j_std
                
                nabla_mean = df['nabla_sq'].rolling(window_z).mean()
                nabla_std = df['nabla_sq'].rolling(window_z).std().replace(0, 1)
                df['nabla_sq_norm'] = (df['nabla_sq'] - nabla_mean) / nabla_std
                
                m_mean = df['m_sq'].rolling(window_z).mean()
                m_std = df['m_sq'].rolling(window_z).std().replace(0, 1)
                df['m_sq_norm'] = (df['m_sq'] - m_mean) / m_std
                
                # Ostateczny AQM Score
                df['aqm_score_h3'] = df['J_norm'] - df['nabla_sq_norm'] - df['m_sq_norm']
                
                # Dynamiczny próg percentyla (Rolling 100)
                df['aqm_percentile_95'] = df['aqm_score_h3'].rolling(window=100).quantile(h3_percentile).fillna(0)

                # ==============================================================================
                # 5. PĘTLA SYMULACYJNA (INLINE) - Symulacja Dzień po Dniu
                # ==============================================================================
                
                # Znajdź indeks startowy dla roku
                sim_start_idx = df.index.searchsorted(start_date_ts)
                
                # Jeśli rok zaczyna się zbyt wcześnie w historii danych, pomijamy ticker (brak wskaźników)
                if sim_start_idx < 201: 
                    continue
                
                i = sim_start_idx
                while i < len(df) - 1:
                    current_date = df.index[i]
                    if current_date > end_date_ts: break
                    
                    # Pobieramy wartości sygnałowe z zamknięcia dnia D
                    row = df.iloc[i]
                    
                    aqm_score = row['aqm_score_h3']
                    threshold = row['aqm_percentile_95']
                    m_norm = row['m_sq_norm']
                    
                    # WARUNEK WEJŚCIA (Identyczny jak w Optimizerze)
                    is_signal = (
                        (aqm_score > threshold) and
                        (m_norm < h3_m_sq_threshold) and
                        (aqm_score > h3_min_score)
                    )
                    
                    if is_signal:
                        # Wchodzimy na otwarciu następnego dnia (D+1)
                        entry_candle = df.iloc[i + 1]
                        entry_price = entry_candle[trade_open_col]
                        atr = row['atr_14']
                        
                        # Walidacja danych wejściowych
                        if pd.isna(entry_price) or entry_price <= 0 or pd.isna(atr) or atr == 0:
                            i += 1
                            continue
                            
                        # Ustalanie poziomów TP/SL
                        tp_price = entry_price + (h3_tp_mult * atr)
                        sl_price = entry_price - (h3_sl_mult * atr)
                        
                        trade_status = 'OPEN'
                        close_price = entry_price
                        close_date = entry_candle.name
                        
                        # Pętla trzymania pozycji (Hold Period)
                        days_held = 0
                        for h in range(h3_max_hold):
                            if i + 1 + h >= len(df): break
                            
                            day_candle = df.iloc[i + 1 + h]
                            day_low = day_candle[trade_low_col]
                            day_high = day_candle[trade_high_col]
                            day_close = day_candle[trade_close_col]
                            
                            days_held += 1
                            
                            # 1. Sprawdź SL (Priorytet 1: Gap Down)
                            # Jeśli otwarcie jest poniżej SL, realizujemy stratę po cenie otwarcia (gap)
                            day_open = day_candle[trade_open_col]
                            if day_open <= sl_price:
                                trade_status = 'CLOSED_SL'
                                close_price = day_open 
                                close_date = day_candle.name
                                break
                            # Jeśli low dnia zeszło poniżej SL
                            elif day_low <= sl_price:
                                trade_status = 'CLOSED_SL'
                                close_price = sl_price
                                close_date = day_candle.name
                                break
                                
                            # 2. Sprawdź TP (Priorytet 2)
                            if day_high >= tp_price:
                                trade_status = 'CLOSED_TP'
                                close_price = tp_price
                                close_date = day_candle.name
                                break
                            
                            # 3. Sprawdź Wyjście Czasowe (Time Exit)
                            if h == h3_max_hold - 1:
                                trade_status = 'CLOSED_EXPIRED'
                                close_price = day_close
                                close_date = day_candle.name
                                break
                        
                        # Oblicz wynik P/L %
                        p_l_percent = ((close_price - entry_price) / entry_price) * 100
                        
                        # Zapisz transakcję do bazy
                        trade_data = {
                            "ticker": ticker,
                            "setup_type": f"BACKTEST_{year}_{setup_name_suffix}",
                            "entry_price": float(entry_price),
                            "stop_loss": float(sl_price),
                            "take_profit": float(tp_price),
                            
                            # Metryki diagnostyczne
                            "metric_atr_14": float(atr),
                            "metric_aqm_score_h3": float(aqm_score),
                            "metric_aqm_percentile_95": float(threshold),
                            "metric_J_norm": float(row['J_norm']),
                            "metric_nabla_sq_norm": float(row['nabla_sq_norm']),
                            "metric_m_sq_norm": float(row['m_sq_norm']),
                            "metric_inst_sync": float(row['institutional_sync']),
                            "metric_retail_herding": float(row['retail_herding']),
                            
                            "status": trade_status,
                            "close_price": float(close_price),
                            "final_profit_loss_percent": float(p_l_percent),
                            "open_date": entry_candle.name,
                            "close_date": close_date
                        }
                        
                        vt = models.VirtualTrade(**trade_data)
                        session.add(vt)
                        trades_generated += 1
                        
                        # Przesuwamy indeks o czas trwania transakcji
                        # Nie otwieramy nowych pozycji, dopóki ta trwa
                        i += max(1, days_held)
                    else:
                        i += 1
                
                processed_count += 1
                
                # Raportowanie postępu i batch commit
                if processed_count % 5 == 0:
                    update_scan_progress(session, processed_count, total_tickers)
                
                if processed_count % 20 == 0:
                    session.commit()
                    msg = f"Backtest V4: Przetworzono {processed_count}/{total_tickers}... Łącznie transakcji: {trades_generated}"
                    append_scan_log(session, msg)

            except Exception as e:
                logger.error(f"Błąd backtestu V4 dla {ticker}: {e}")
                continue

        # Ostateczny commit po zakończeniu pętli
        session.commit()
        update_scan_progress(session, total_tickers, total_tickers)
        append_scan_log(session, f"BACKTEST V4: Zakończono dla roku {year}. Wygenerowano {trades_generated} transakcji.")
        logger.info(f"[Backtest V4] Koniec. Łącznie transakcji: {trades_generated}")

    except Exception as e:
        error_msg = f"Krytyczny błąd Backtestu V4: {e}"
        logger.error(error_msg, exc_info=True)
        append_scan_log(session, error_msg)
