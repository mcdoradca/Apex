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

logger = logging.getLogger(__name__)

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    """
    Uruchamia backtest historyczny dla podanego roku, implementując pełną logikę
    Pola Kwantowego H3 (AQM V3) zgodnie z dokumentacją PDF.
    """
    logger.info(f"[Backtest] Rozpoczynanie analizy historycznej dla roku {year}...")
    append_scan_log(session, f"BACKTEST: Uruchamianie symulacji dla roku {year}...")
    
    try:
        # 1. Pobierz listę tickerów do przetestowania
        tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        tickers = [r[0] for r in tickers_rows]
        
        total_tickers = len(tickers)
        processed_count = 0
        trades_generated = 0
        
        # Parametry okien czasowych z PDF
        Z_SCORE_WINDOW = 100
        MARKET_TEMP_WINDOW = 30
        
        for ticker in tickers:
            try:
                # === KROK A: Pobieranie Danych Historycznych ===
                
                # 1. Dane Dzienne (OHLCV + Price Gravity)
                daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
                
                if not daily_raw or not daily_adj_raw:
                    continue

                # Parsowanie
                daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
                
                daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
                daily_adj.index = pd.to_datetime(daily_adj.index)
                
                # Wymagamy historii 200+ dni dla stabilnego Z-Score (window=100)
                if len(daily_adj) < 201: 
                    continue

                # Łączenie danych (Price Gravity wymaga VWAP Proxy z OHLCV)
                if 'high' in daily_ohlcv.columns and 'low' in daily_ohlcv.columns and 'close' in daily_ohlcv.columns:
                    daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
                else:
                    continue

                # Główny DataFrame do obliczeń
                df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
                close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
                
                # === KROK B: Obliczanie Surowych Metryk (Raw Metrics) ===
                
                # 1. Wymiar 1: Czasoprzestrzeń & Grawitacja
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
                
                # 2. Wymiar 2: Splątanie (Insider / News)
                h2_data = load_h2_data_into_cache(ticker, api_client, session)
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                
                df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
                df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
                
                # 3. Wymiar 4: Termodynamika (Market Temp & Entropy)
                df['daily_returns'] = df['close'].pct_change()
                df['market_temperature'] = df['daily_returns'].rolling(window=MARKET_TEMP_WINDOW).std()
                
                # News Entropy
                if not news_df.empty:
                    news_counts = news_df.groupby(news_df.index.date).size()
                    news_counts.index = pd.to_datetime(news_counts.index)
                    news_counts = news_counts.reindex(df.index, fill_value=0)
                    df['information_entropy'] = news_counts.rolling(window=10).sum()
                    # Z-Score dla Newsów (część m^2)
                    news_mean_200 = df['information_entropy'].rolling(200).mean()
                    news_std_200 = df['information_entropy'].rolling(200).std()
                    df['normalized_news'] = ((df['information_entropy'] - news_mean_200) / news_std_200).replace([np.inf, -np.inf], 0).fillna(0)
                else:
                    df['information_entropy'] = 0.0
                    df['normalized_news'] = 0.0
                
                # Volume Z-Score (część m^2)
                df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
                df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
                df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
                df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                
                # === KROK C: Konstrukcja Pola H3 (Zgodność z PDF) ===
                
                # 1. Obliczenie Komponentów Surowych
                df['m_sq'] = df['normalized_volume'] + df['normalized_news']
                df['nabla_sq'] = df['price_gravity']
                
                # J = S - (Q/T) + mu
                S = df['information_entropy']
                Q = df['retail_herding']
                T = df['market_temperature']
                mu = df['institutional_sync']
                df['J'] = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
                df['J'] = df['J'].fillna(0)

                # 2. Normalizacja Z-Score 100-dniowa (WYMAGANE PRZEZ PDF)
                # To jest kluczowy krok, którego brakowało w jawnej formie
                
                # J_norm
                j_mean = df['J'].rolling(window=Z_SCORE_WINDOW).mean()
                j_std = df['J'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                df['J_norm'] = ((df['J'] - j_mean) / j_std).replace([np.inf, -np.inf], 0).fillna(0)
                
                # Nabla_norm
                nabla_mean = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).mean()
                nabla_std = df['nabla_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                df['nabla_sq_norm'] = ((df['nabla_sq'] - nabla_mean) / nabla_std).replace([np.inf, -np.inf], 0).fillna(0)
                
                # m_norm
                m_mean = df['m_sq'].rolling(window=Z_SCORE_WINDOW).mean()
                m_std = df['m_sq'].rolling(window=Z_SCORE_WINDOW).std(ddof=1)
                df['m_sq_norm'] = ((df['m_sq'] - m_mean) / m_std).replace([np.inf, -np.inf], 0).fillna(0)

                # 3. Obliczenie AQM_V3_SCORE (Finalny Wynik Pola)
                # Wzór PDF: Score = J_norm - nabla_norm - m_norm
                df['aqm_score_h3'] = df['J_norm'] - df['nabla_sq_norm'] - df['m_sq_norm']
                
                # 4. Obliczenie Progu Percentylowego (95%)
                # Dynamiczny próg z ostatnich 100 dni
                df['aqm_percentile_95'] = df['aqm_score_h3'].rolling(window=Z_SCORE_WINDOW).quantile(0.95)

                # === KROK D: Przekazanie do Symulatora ===
                # Przekazujemy DataFrame z JUŻ obliczonymi metrykami AQM.
                # Symulator (aqm_v3_h3_simulator) został sprawdzony i posiada logikę
                # egzekucji, ale teraz ma gwarancję poprawnych danych wejściowych.
                
                sim_data = {
                    "daily": df 
                }
                
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
