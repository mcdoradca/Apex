import logging
import pandas as pd
# Importujemy `math` dla logarytmu (Wymiar 4.2) i `sqrt` (Prawo 3)
import math
from math import sqrt 
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta, timezone

# Importujemy klienta AV tylko dla funkcji "na żywo",
# funkcje "_from_data" nie będą go używać.
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

logger = logging.getLogger(__name__)

# ==================================================================
# CZĘŚĆ 1: Funkcje Pomocnicze (Pojedyncze punkty / API Live)
# Te funkcje są zachowane dla kompatybilności z istniejącym kodem
# i ewentualnych szybkich sprawdzeń.
# ==================================================================

def calculate_time_dilation_from_data(daily_df_view: pd.DataFrame, spy_df_view: pd.DataFrame) -> Optional[float]:
    """
    (Wymiar 1.1) Oblicza 'time_dilation' na podstawie historycznych widoków DataFrame.
    """
    try:
        if daily_df_view.empty or spy_df_view.empty:
            return None

        ticker_returns = daily_df_view['close'].pct_change()
        spy_returns = spy_df_view['close'].pct_change()
        
        stddev_ticker_20 = ticker_returns.tail(20).std()
        stddev_spy_20 = spy_returns.tail(20).std()
        
        if stddev_spy_20 == 0 or pd.isna(stddev_spy_20) or pd.isna(stddev_ticker_20):
            return None
            
        return stddev_ticker_20 / stddev_spy_20
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_time_dilation_from_data': {e}", exc_info=True)
        return None

def calculate_price_gravity_from_data(daily_df_view: pd.DataFrame, vwap_df_view: pd.DataFrame = None) -> Optional[float]:
    """
    (Wymiar 1.2) Oblicza 'price_gravity'. Proxy (H+L+C)/3.
    """
    try:
        if daily_df_view.empty:
            return None

        latest_candle = daily_df_view.iloc[-1]
        price = latest_candle['close']
        high = latest_candle['high']
        low = latest_candle['low']
        
        if pd.isna(price) or pd.isna(high) or pd.isna(low):
            return None

        center_of_mass_proxy = (high + low + price) / 3.0
        
        if price == 0:
            return None
            
        price_gravity = (center_of_mass_proxy - price) / price
        return price_gravity
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_price_gravity_from_data': {e}", exc_info=True)
        return None

def calculate_institutional_sync_from_data(insider_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 2.1) Oblicza 'institutional_sync'.
    """
    try:
        ninety_days_ago = current_date - timedelta(days=90)
        recent_transactions = insider_df_view.loc[insider_df_view.index >= ninety_days_ago]
        
        if recent_transactions.empty:
            return 0.0 

        total_buys = recent_transactions[recent_transactions['transaction_type'] == 'A']['transaction_shares'].sum()
        total_sells = recent_transactions[recent_transactions['transaction_type'] == 'D']['transaction_shares'].sum()
        
        denominator = total_buys + total_sells
        if denominator == 0:
            return 0.0
            
        institutional_sync = (total_buys - total_sells) / denominator
        return institutional_sync
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_institutional_sync_from_data': {e}", exc_info=True)
        return None

def calculate_retail_herding_from_data(news_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 2.2) Oblicza 'retail_herding'.
    """
    try:
        seven_days_ago = current_date - timedelta(days=7)
        
        if news_df_view.index.tz is not None:
            news_df_view_naive = news_df_view.tz_convert(None)
        else:
            news_df_view_naive = news_df_view

        recent_news = news_df_view_naive.loc[news_df_view_naive.index >= seven_days_ago]
        
        if recent_news.empty:
            return 0.0 

        scores = recent_news['overall_sentiment_score']
        if scores.empty:
             return 0.0
             
        retail_herding = scores.mean()
        return retail_herding
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_retail_herding_from_data': {e}", exc_info=True)
        return None

def calculate_breakout_energy_from_data(bbands_df_view: pd.DataFrame, daily_df_view: pd.DataFrame) -> Optional[float]:
    try:
        if daily_df_view.empty or bbands_df_view.empty:
            return None

        price = daily_df_view['close'].iloc[-1]
        upper_band = bbands_df_view['Real Upper Band'].asof(daily_df_view.index[-1])
        lower_band = bbands_df_view['Real Lower Band'].asof(daily_df_view.index[-1])

        if price == 0 or pd.isna(price) or pd.isna(upper_band) or pd.isna(lower_band):
            return None
            
        band_width_normalized = (upper_band - lower_band) / price
        
        if band_width_normalized == 0:
            return None
            
        breakout_energy_required = 1 / band_width_normalized
        return breakout_energy_required
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_breakout_energy_from_data': {e}", exc_info=True)
        return None

def calculate_market_temperature_from_data(
    intraday_5min_df_view: pd.DataFrame, 
    current_date: datetime,
    daily_df_view: Optional[pd.DataFrame] = None
) -> Optional[float]:
    try:
        if daily_df_view is None or daily_df_view.empty:
            return None

        thirty_days_ago = current_date - timedelta(days=30)
        recent_daily_data = daily_df_view.loc[daily_df_view.index <= current_date].iloc[-31:]

        if recent_daily_data.empty or len(recent_daily_data) < 2:
            return None

        returns_daily = recent_daily_data['close'].pct_change().dropna()
        
        if len(returns_daily) < 20: 
             return None

        market_temperature = returns_daily.std()
        return market_temperature
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_market_temperature_from_data': {e}", exc_info=True)
        return None

def calculate_information_entropy_from_data(news_df_view: pd.DataFrame) -> Optional[float]:
    try:
        if news_df_view.index.tz is not None:
            news_df_view_naive = news_df_view.tz_convert(None)
        else:
            news_df_view_naive = news_df_view
        
        if news_df_view_naive.empty:
            return 0.0 
            
        latest_date_naive = news_df_view_naive.index[-1].to_pydatetime()
        ten_days_ago_naive = latest_date_naive - timedelta(days=10)
        
        recent_news = news_df_view_naive.loc[news_df_view_naive.index >= ten_days_ago_naive]
        S = len(recent_news)
        
        return float(S)

    except Exception as e:
        logger.error(f"Błąd w 'calculate_information_entropy_from_data': {e}", exc_info=True)
        return None

def calculate_attention_density_from_data(daily_df_view: pd.DataFrame, news_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    try:
        historical_avg_volume_10d = daily_df_view['volume'].rolling(window=10).mean()
        valid_volume_history = historical_avg_volume_10d.iloc[-200:].dropna()
        avg_volume_10d = historical_avg_volume_10d.iloc[-1]
        
        if news_df_view.empty:
            normalized_news = 0.0 
        else:
            if news_df_view.index.tz is not None:
                news_df_view_naive = news_df_view.tz_convert(None)
            else:
                news_df_view_naive = news_df_view
            
            news_counts_daily = news_df_view_naive.groupby(news_df_view_naive.index.date).size()
            news_counts_daily.index = pd.to_datetime(news_counts_daily.index)
            news_counts_daily = news_counts_daily.reindex(pd.date_range(start=news_counts_daily.index.min(), end=news_counts_daily.index.max(), freq='D'), fill_value=0)
            
            historical_news_count_10d = news_counts_daily.rolling(window=10).sum()
            valid_news_history = historical_news_count_10d.iloc[-200:].dropna()
            news_count_10d = historical_news_count_10d.asof(current_date)
            
            if valid_news_history.empty or pd.isna(news_count_10d):
                normalized_news = 0.0
            else:
                news_mean = valid_news_history.mean()
                news_std = valid_news_history.std()
                if news_std == 0:
                    normalized_news = 0.0
                else:
                    normalized_news = (news_count_10d - news_mean) / news_std
        
        if len(valid_volume_history) < 200 or pd.isna(avg_volume_10d):
             return None

        vol_mean = valid_volume_history.mean()
        vol_std = valid_volume_history.std()
        if vol_std == 0:
            normalized_volume = 0.0
        else:
            normalized_volume = (avg_volume_10d - vol_mean) / vol_std
            
        attention_density = normalized_volume + normalized_news
        return attention_density

    except Exception as e:
        logger.error(f"Błąd w 'calculate_attention_density_from_data': {e}", exc_info=True)
        return None

# ==================================================================
# CZĘŚĆ 2: SILNIK WEKTOROWY H3 (NOWOŚĆ - Zgodnie z Roadmapą)
# Oblicza pełne równanie pola na całym DataFrame.
# ==================================================================

def calculate_retail_herding_capped_v4(retail_herding_series: pd.Series) -> pd.Series:
    """Helper dla cappingu."""
    return retail_herding_series.clip(-1.0, 1.0)

def calculate_aqm_h3_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kompletna, wektorowa implementacja równania pola H3 (AQM V3).
    Oblicza kolumny: aqm_score_h3, J_norm, nabla_sq_norm, m_sq_norm.
    
    Wymagane kolumny wejściowe w df (obliczone wcześniej):
    - institutional_sync (z H2)
    - retail_herding (z H2)
    - information_entropy (liczba newsów)
    - market_temperature (zmienność)
    - price_gravity
    - volume (lub normalized_volume)
    
    ZASADA: Czysta matematyka. Żadnej adaptacji do VIX.
    """
    try:
        # Pracujemy na kopii, aby nie psuć oryginału w pętlach
        d = df.copy()
        
        # 1. Normalizacja Institutional Sync (Z-Score w oknie 100 dni)
        if 'institutional_sync' in d.columns:
            rolling_mean = d['institutional_sync'].rolling(100, min_periods=20).mean()
            rolling_std = d['institutional_sync'].rolling(100, min_periods=20).std().fillna(1)
            d['mu_normalized'] = ((d['institutional_sync'] - rolling_mean) / rolling_std).fillna(0)
        else:
            d['mu_normalized'] = 0.0

        # 2. Capping Retail Herding (Ograniczenie wpływu tłumu do -1..1)
        if 'retail_herding' in d.columns:
            d['retail_herding_capped'] = d['retail_herding'].clip(-1.0, 1.0)
        else:
            d['retail_herding_capped'] = 0.0

        # 3. Obliczenie Energii J (Siła Napędowa)
        #    Wzór: J = Entropia - (Sentyment / Temperatura) + Insiderzy
        S = d.get('information_entropy', 0.0)
        Q = d['retail_herding_capped']
        #    Zabezpieczenie przed dzieleniem przez zero w Temperaturze
        T = d.get('market_temperature', pd.Series(1.0, index=d.index)).replace(0, np.nan)
        mu = d['mu_normalized']
        
        #    Jeśli T jest NaN (brak zmienności), Q/T traktujemy jako 0
        term_QT = (Q / T).fillna(0)
        
        d['J'] = S - term_QT + (mu * 1.0)
        d['J'] = d['J'].fillna(0)

        # 4. Normalizacja Składników Pola (Z-Score, okno 100 dni)
        #    Dzięki temu sprowadzamy wszystko do wspólnego mianownika (odchyleń standardowych)
        
        # J_norm
        j_mean = d['J'].rolling(100, min_periods=20).mean()
        j_std = d['J'].rolling(100, min_periods=20).std().fillna(1)
        d['J_norm'] = ((d['J'] - j_mean) / j_std).fillna(0)
        
        # Nabla_sq (Grawitacja) -> Normalizacja
        d['nabla_sq'] = d.get('price_gravity', 0.0)
        nab_mean = d['nabla_sq'].rolling(100, min_periods=20).mean()
        nab_std = d['nabla_sq'].rolling(100, min_periods=20).std().fillna(1)
        d['nabla_sq_norm'] = ((d['nabla_sq'] - nab_mean) / nab_std).fillna(0)
        
        # Masa m^2 (Wolumen) -> Normalizacja
        # Używamy znormalizowanego wolumenu jeśli jest, lub surowego
        if 'normalized_volume' in d.columns:
            d['m_sq'] = d['normalized_volume']
        elif 'volume' in d.columns:
             # Prosta normalizacja wolumenu w locie, jeśli brak wstępnej
             v_mean = d['volume'].rolling(200, min_periods=50).mean()
             v_std = d['volume'].rolling(200, min_periods=50).std().fillna(1)
             d['m_sq'] = ((d['volume'] - v_mean) / v_std).fillna(0)
        else:
            d['m_sq'] = 0.0
        
        m_mean = d['m_sq'].rolling(100, min_periods=20).mean()
        m_std = d['m_sq'].rolling(100, min_periods=20).std().fillna(1)
        d['m_sq_norm'] = ((d['m_sq'] - m_mean) / m_std).fillna(0)
        
        # 5. FINALNY WYNIK H3 (Równanie Pola)
        #    AQM Score = Energia - Opór Grawitacyjny - Masa Tłumu
        d['aqm_score_h3'] = (d['J_norm'] * 1.0) - (d['nabla_sq_norm'] * 1.0) - (d['m_sq_norm'] * 1.0)
        
        return d
        
    except Exception as e:
        logger.error(f"Błąd w wektorowym obliczaniu AQM H3: {e}", exc_info=True)
        return df
