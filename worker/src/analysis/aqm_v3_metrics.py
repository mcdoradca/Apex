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
# Importujemy Counter do obliczeń Entropii
from collections import Counter

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
    
    Wersja "czysta": Przyjmuje 20-dniowe widoki DF i oblicza metrykę.
    """
    try:
        # 1. Oblicz zwroty
        ticker_returns = daily_df_view['close'].pct_change()
        spy_returns = spy_df_view['close'].pct_change()
        
        # 2. Oblicz 20-dniowe odchylenie standardowe (dla całego okna)
        stddev_ticker_20 = ticker_returns.std()
        stddev_spy_20 = spy_returns.std()
        
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
    
    Wersja "czysta": Przyjmuje widoki DF i oblicza metrykę dla *ostatniego dnia* widoku.
    """
    try:
        # 1. Pobierz najnowsze wartości
        price = daily_df_view['close'].iloc[-1]
        
        # Znajdź najbliższy VWAP (w przypadku brakujących dat)
        center_of_mass = vwap_df_view['VWAP'].asof(daily_df_view.index[-1])
        
        # 2. Oblicz metrykę
        if price == 0 or pd.isna(price) or pd.isna(center_of_mass):
            return None # Unikaj dzielenia przez zero
            
        price_gravity = (center_of_mass - price) / price
        return price_gravity
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_price_gravity_from_data': {e}", exc_info=True)
        return None

# ==================================================================
# === KROK 21b: "Czyste" Funkcje dla Hipotezy H2 (Backtest) ===
# ==================================================================

def calculate_institutional_sync_from_data(insider_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 2.1) Oblicza 'institutional_sync' na podstawie historycznego widoku DataFrame
    z transakcjami insiderów (za ostatnie 90 dni).
    """
    try:
        # 1. Filtruj transakcje z ostatnich 90 dni (wg specyfikacji)
        ninety_days_ago = current_date - timedelta(days=90)
        # Używamy .loc do filtrowania po indeksie (który jest datą)
        recent_transactions = insider_df_view.loc[insider_df_view.index >= ninety_days_ago]
        
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
        # Używamy .loc do filtrowania po indeksie (który jest datą)
        recent_news = news_df_view.loc[news_df_view.index >= seven_days_ago]
        
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
# === KROK 22a: "Czyste" Funkcje dla Hipotezy H3 (Wymiary 3, 4, 7) ===
# ==================================================================

def calculate_breakout_energy_from_data(bbands_df_view: pd.DataFrame, daily_df_view: pd.DataFrame) -> Optional[float]:
    """
    (Wymiar 3.1) Oblicza 'breakout_energy_required'.
    """
    try:
        # 1. Pobierz najnowsze wartości
        price = daily_df_view['close'].iloc[-1]
        
        # Znajdź najbliższe wstęgi (na wypadek brakujących dat)
        upper_band = bbands_df_view['Real Upper Band'].asof(daily_df_view.index[-1])
        lower_band = bbands_df_view['Real Lower Band'].asof(daily_df_view.index[-1])

        if price == 0 or pd.isna(price) or pd.isna(upper_band) or pd.isna(lower_band):
            return None
            
        band_width_normalized = (upper_band - lower_band) / price
        
        if band_width_normalized == 0:
            return None # Unikaj dzielenia przez zero
            
        breakout_energy_required = 1 / band_width_normalized
        return breakout_energy_required
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_breakout_energy_from_data': {e}", exc_info=True)
        return None

def calculate_market_temperature_from_data(intraday_5min_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 4.1) Oblicza 'market_temperature' (zmienność 5-min).
    """
    try:
        # 1. Filtruj dane 5-minutowe z ostatnich 30 dni (wg specyfikacji)
        thirty_days_ago = current_date - timedelta(days=30)
        recent_intraday_data = intraday_5min_df_view.loc[intraday_5min_df_view.index >= thirty_days_ago]

        if recent_intraday_data.empty or len(recent_intraday_data) < 2:
            return None # Za mało danych do obliczenia zwrotów

        # 2. Oblicz zwroty 5-minutowe
        returns_5min = recent_intraday_data['close'].pct_change()
        
        # 3. Oblicz odchylenie standardowe
        market_temperature = returns_5min.std()
        
        if pd.isna(market_temperature):
            return None
            
        return market_temperature
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_market_temperature_from_data': {e}", exc_info=True)
        return None

def calculate_information_entropy_from_data(news_df_view: pd.DataFrame) -> Optional[float]:
    """
    (Wymiar 4.2) Oblicza 'information_entropy' (Entropia Shannona tematów).
    """
    try:
        # 1. Pobierz listę tematów (Zakładamy 100 ostatnich newsów w 'news_df_view')
        # Zakładamy, że 'news_df_view' ma kolumnę 'topics', która jest listą
        if 'topics' not in news_df_view.columns:
            logger.warning("Brak kolumny 'topics' w danych news. Nie można obliczyć Entropii.")
            return None

        # .dropna() usuwa puste listy tematów
        topic_list = news_df_view['topics'].dropna().sum()
        
        if not topic_list:
            return 0.0 # Zero entropii, jeśli brak tematów

        # 2. Zlicz tematy
        topic_counts = Counter(topic_list)
        total_topics = len(topic_list)
        
        # 3. Oblicz prawdopodobieństwa
        probabilities = [count / total_topics for count in topic_counts.values()]
        
        # 4. Oblicz Entropię Shannona
        information_entropy = -sum([p * math.log2(p) for p in probabilities if p > 0])
        
        return information_entropy
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_information_entropy_from_data': {e}", exc_info=True)
        return None

def calculate_attention_density_from_data(daily_df_view: pd.DataFrame, news_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 7.1) Oblicza 'attention_density'.
    Wymaga 200-dniowego 'daily_df_view' i 200-dniowego 'news_df_view'.
    """
    try:
        # Upewnij się, że mamy 200 dni
        if len(daily_df_view) < 200 or len(news_df_view) < 200:
            return None 

        # 1. Oblicz metryki 10-dniowe (dla ostatniego dnia)
        avg_volume_10d = daily_df_view['volume'].iloc[-10:].mean()
        
        ten_days_ago = current_date - timedelta(days=10)
        news_count_10d = len(news_df_view.loc[news_df_view.index >= ten_days_ago])
        
        # 2. Oblicz metryki 10-dniowe dla całej historii 200 dni
        historical_avg_volume_10d = daily_df_view['volume'].rolling(window=10).mean()
        
        # To jest trudniejsze obliczeniowo: krocząca liczba newsów
        # Uproszczenie: grupujemy newsy per dzień i robimy kroczącą sumę
        news_counts_daily = news_df_view.resample('D').size().rolling(window=10).sum()
        
        # 3. Oblicz Z-Score (dla ostatniego dnia)
        # (ostatnia wartość - średnia z 200 dni) / odchylenie std z 200 dni
        
        # Z-Score dla Wolumenu
        vol_mean = historical_avg_volume_10d.iloc[-200:].mean()
        vol_std = historical_avg_volume_10d.iloc[-200:].std()
        if vol_std == 0:
            normalized_volume = 0.0
        else:
            normalized_volume = (avg_volume_10d - vol_mean) / vol_std
            
        # Z-Score dla Newsów
        news_mean = news_counts_daily.iloc[-200:].mean()
        news_std = news_counts_daily.iloc[-200:].std()
        if news_std == 0:
            normalized_news = 0.0
        else:
            normalized_news = (news_count_10d - news_mean) / news_std
            
        # 4. Oblicz metrykę
        attention_density = normalized_volume + normalized_news
        
        if pd.isna(attention_density):
            return None
            
        return attention_density

    except Exception as e:
        logger.error(f"Błąd w 'calculate_attention_density_from_data': {e}", exc_info=True)
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
