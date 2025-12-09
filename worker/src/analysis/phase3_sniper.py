import logging
import pandas as pd
# Importujemy `math` dla logarytmu (Wymiar 4.2) i `sqrt` (Prawo 3)
import math
from math import sqrt 
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
# ==================================================================
# === POPRAWKA BŁĘDU (TZ-NAIVE vs TZ-AWARE) ===
# Dodajemy import 'timezone', aby móc ujednolicić strefy czasowe.
# ==================================================================
from datetime import datetime, timedelta, timezone
# ==================================================================

# Importujemy klienta AV tylko dla funkcji "na żywo",
# funkcje "_from_data" nie będą go używać.
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

logger = logging.getLogger(__name__)

# ==================================================================
# === KROK 17: "Czyste" Funkcje dla Hipotezy H1 (Backtest) ===
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

# ==================================================================
# === KROK 21b: "Czyste" Funkcje dla Hipotezy H2 (Backtest) ===
# ==================================================================

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

# ==================================================================
# === KROK 22a: "Czyste" Funkcje dla Hipotezy H3 ===
# ==================================================================

def calculate_breakout_energy_from_data(bbands_df_view
