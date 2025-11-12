import logging
import pandas as pd
# Importujemy `math` dla logarytmu (Wymiar 4.2) i `sqrt` (Prawo 3)
import math
from math import sqrt 
# Importujemy `statsmodels` i `scipy` dla Wymiaru 6 i 7
from statsmodels.tsa.stattools import grangercausalitytests
from scipy.stats import zscore, shapiro
import numpy as np
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta

# Importujemy klienta AV tylko dla funkcji "na żywo",
# funkcje "_from_data" nie będą go używać.
from ..data_ingestion.alpha_v3_client import AlphaVantageClient

logger = logging.getLogger(__name__)

# ==================================================================
# === KROK 17: "Czyste" Funkcje dla Hipotezy H1 (Backtest) ===
# ==================================================================

def calculate_time_dilation_from_data(daily_df_view: pd.DataFrame, spy_df_view: pd.DataFrame) -> Optional[float]:
    """
    (Wymiar 1.1) Oblicza 'time_dilation' na podstawie historycznych widoków DataFrame.
    """
    try:
        # 1. Oblicz zwroty
        ticker_returns = daily_df_view['close'].pct_change()
        spy_returns = spy_df_view['close'].pct_change()
        
        # 2. Oblicz 20-dniowe odchylenie standardowe
        stddev_ticker_20 = ticker_returns.rolling(window=20).std().iloc[-1]
        stddev_spy_20 = spy_returns.rolling(window=20).std().iloc[-1]
        
        # 3. Oblicz metrykę
        if stddev_spy_20 == 0 or pd.isna(stddev_spy_20) or pd.isna(stddev_ticker_20):
            return None # Unikaj dzielenia przez zero
            
        return stddev_ticker_20 / stddev_spy_20
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_time_dilation_from_data': {e}", exc_info=True)
        return None

def calculate_price_gravity_from_data(daily_df_view: pd.DataFrame, vwap_df_view: pd.DataFrame) -> Optional[float]:
    """
    (Wymiar 1.2) Oblicza 'price_gravity' na podstawie historycznych widoków DataFrame.
    """
    try:
        # 1. Pobierz najnowsze wartości
        price = daily_df_view['close'].iloc[-1]
        center_of_mass = vwap_df_view['VWAP'].iloc[-1]
        
        # 2. Oblicz metrykę
        if price == 0 or pd.isna(price) or pd.isna(center_of_mass):
            return None # Unikaj dzielenia przez zero
            
        price_gravity = (center_of_mass - price) / price
        return price_gravity
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_price_gravity_from_data': {e}", exc_info=True)
        return None

# ==================================================================
# === KROK 20: "Czyste" Funkcje dla Hipotezy H2 (Backtest) ===
# ==================================================================

def calculate_institutional_sync_from_data(insider_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 2.1) Oblicza 'institutional_sync' na podstawie historycznego widoku DataFrame
    z transakcjami insiderów (za ostatnie 90 dni).
    """
    try:
        # 1. Filtruj transakcje z ostatnich 90 dni (wg specyfikacji)
        ninety_days_ago = current_date - timedelta(days=90)
        recent_transactions = insider_df_view[insider_df_view.index >= ninety_days_ago]
        
        if recent_transactions.empty:
            return 0.0 # Neutralny, jeśli brak transakcji

        # 2. Oblicz sumy (zgodnie ze "Sztywną Formułą")
        # Zakładamy, że 'transaction_shares' jest dodatnie dla obu typów
        total_buys = recent_transactions[recent_transactions['transaction_type'] == 'P-Purchase']['transaction_shares'].sum()
        total_sells = recent_transactions[recent_transactions['transaction_type'] == 'S-Sale']['transaction_shares'].sum()
        
        # 3. Oblicz metrykę
        denominator = total_buys + total_sells
        if denominator == 0:
            return 0.0 # Neutralny, jeśli brak zakupów lub sprzedaży (np. tylko opcje)
            
        institutional_sync = (total_buys - total_sells) / denominator
        return institutional_sync
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_institutional_sync_from_data': {e}", exc_info=True)
        return None

def calculate_retail_herding_from_data(news_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 2.2) Oblicza 'retail_herding' na podstawie historycznego widoku DataFrame
    z sentymentem newsów (za ostatnie 7 dni).
    """
    try:
        # 1. Filtruj artykuły z ostatnich 7 dni (wg specyfikacji)
        seven_days_ago = current_date - timedelta(days=7)
        recent_news = news_df_view[news_df_view.index >= seven_days_ago]
        
        if recent_news.empty:
            return 0.0 # Neutralny, jeśli brak newsów

        # 2. Oblicz średnią
        # Zakładamy, że DataFrame ma kolumnę 'overall_sentiment_score'
        scores = recent_news['overall_sentiment_score']
        
        if scores.empty:
             return 0.0 # Neutralny
             
        retail_herding = scores.mean()
        return retail_herding
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_retail_herding_from_data': {e}", exc_info=True)
        return None


# ==================================================================
# === Funkcje "Na Żywo" (Oryginalna Logika - jeszcze nieużywane) ===
# ==================================================================
# Poniższe funkcje są zachowane, aby pokazać, jak metryki
# byłyby obliczane w trybie "live" (np. dla skanera EOD).

def calculate_time_dilation_live(ticker: str, ticker_daily_df: pd.DataFrame, spy_daily_df: pd.DataFrame) -> Optional[float]:
    """(Wymiar 1.1) Oblicza 'time_dilation' używając przekazanych DF."""
    try:
        ticker_returns = ticker_daily_df['close'].pct_change()
        spy_returns = spy_daily_df['close'].pct_change()
        
        stddev_ticker_20 = ticker_returns.rolling(window=20).std().iloc[-1]
        stddev_spy_20 = spy_returns.rolling(window=20).std().iloc[-1]
        
        if stddev_spy_20 == 0: return None
        return stddev_ticker_20 / stddev_spy_20
    except Exception as e:
        logger.error(f"Błąd w 'calculate_time_dilation' dla {ticker}: {e}", exc_info=True)
        return None

def calculate_price_gravity_live(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient) -> Optional[float]:
    """(Wymiar 1.2) Oblicza 'price_gravity' (wymaga 1 dodatkowego wywołania API)."""
    try:
        price = ticker_daily_df['close'].iloc[-1]
        
        vwap_data = api_client.get_vwap(ticker, interval='daily')
        if not vwap_data or 'Technical Analysis: VWAP' not in vwap_data:
             logger.warning(f"Brak danych VWAP (live) dla {ticker}")
             return None
        
        # Znajdź najnowszą wartość VWAP
        latest_vwap_date = sorted(vwap_data['Technical Analysis: VWAP'].keys())[-1]
        center_of_mass = float(vwap_data['Technical Analysis: VWAP'][latest_vwap_date]['VWAP'])
        
        if price == 0: return None
        price_gravity = (center_of_mass - price) / price
        return price_gravity
    except Exception as e:
        logger.error(f"Błąd w 'calculate_price_gravity' dla {ticker}: {e}", exc_info=True)
        return None

def calculate_institutional_sync_live(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """(Wymiar 2.1) Oblicza 'institutional_sync' (wymaga 1 wywołania API)."""
    try:
        insider_data = api_client.get_insider_transactions(ticker)
        if not insider_data or 'transactions' not in insider_data or not insider_data['transactions']:
            logger.warning(f"Brak danych Insider Transactions (live) dla {ticker}")
            return 0.0 # Neutralny, jeśli brak transakcji

        transactions = insider_data['transactions']
        total_buys = 0.0
        total_sells = 0.0
        
        # Data graniczna (90 dni temu)
        ninety_days_ago = datetime.now() - timedelta(days=90)
        
        for tx in transactions:
            try:
                tx_date = datetime.strptime(tx['transactionDate'], '%Y-%m-%d')
                if tx_date < ninety_days_ago:
                    continue # Transakcja zbyt stara
                
                shares = float(tx['transactionShares'])
                
                if tx['transactionType'] == 'P-Purchase':
                    total_buys += shares
                elif tx['transactionType'] == 'S-Sale':
                    total_sells += shares
            except (ValueError, TypeError):
                continue # Pomiń błędne rekordy

        denominator = total_buys + total_sells
        if denominator == 0:
            return 0.0
            
        return (total_buys - total_sells) / denominator
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_institutional_sync' dla {ticker}: {e}", exc_info=True)
        return None

def calculate_retail_herding_live(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """(Wymiar 2.2) Oblicza 'retail_herding' (wymaga 1 wywołania API)."""
    try:
        # Format daty dla AV: YYYYMMDDTHHMM
        seven_days_ago_str = (datetime.now() - timedelta(days=7)).strftime('%Y%m%dT%H%M')
        
        news_data = api_client.get_news_sentiment(ticker, limit=100, time_from=seven_days_ago_str)
        if not news_data or 'feed' not in news_data or not news_data['feed']:
            logger.warning(f"Brak danych News Sentiment (live) dla {ticker}")
            return 0.0 # Neutralny, jeśli brak newsów

        feed = news_data['feed']
        scores = []
        for article in feed:
            try:
                scores.append(float(article['overall_sentiment_score']))
            except (ValueError, TypeError, KeyError):
                continue
        
        if not scores:
            return 0.0
            
        return sum(scores) / len(scores)

    except Exception as e:
        logger.error(f"Błąd w 'calculate_retail_herding' dla {ticker}: {e}", exc_info=True)
        return None

# ==================================================================
# === Puste implementacje dla Wymiarów 3-7 (do wdrożenia) ===
# ==================================================================

# ... (Puste funkcje dla Wymiarów 3-7, np. calculate_breakout_energy...)
