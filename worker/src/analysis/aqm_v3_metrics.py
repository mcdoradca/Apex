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
    
    ZGODNIE Z PDF (Sztywna Formuła):
    Stosunek 20-dniowej zmienności spółki do 20-dniowej zmienności SPY.
    """
    try:
        if daily_df_view.empty or spy_df_view.empty:
            return None

        # 1. Oblicz zwroty
        ticker_returns = daily_df_view['close'].pct_change()
        spy_returns = spy_df_view['close'].pct_change()
        
        # 2. Oblicz 20-dniowe odchylenie standardowe (ściśle z ostatnich 20 dostępnych próbek)
        stddev_ticker_20 = ticker_returns.tail(20).std()
        stddev_spy_20 = spy_returns.tail(20).std()
        
        # 3. Oblicz metrykę
        if stddev_spy_20 == 0 or pd.isna(stddev_spy_20) or pd.isna(stddev_ticker_20):
            return None # Unikaj dzielenia przez zero
            
        return stddev_ticker_20 / stddev_spy_20
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_time_dilation_from_data': {e}", exc_info=True)
        return None

# ==================================================================
# === KLUCZOWA ZMIANA (ZGODNIE Z MEMO SUPPORTU 1.2) ===
# ==================================================================
def calculate_price_gravity_from_data(daily_df_view: pd.DataFrame, vwap_df_view: pd.DataFrame = None) -> Optional[float]:
    """
    (Wymiar 1.2) Oblicza 'price_gravity' na podstawie historycznych widoków DataFrame.
    
    ZGODNIE Z PDF: Zastępujemy VWAP przez proxy (H+L+C)/3.
    """
    try:
        if daily_df_view.empty:
            return None

        # 1. Pobierz najnowsze wartości OHLC
        latest_candle = daily_df_view.iloc[-1]
        price = latest_candle['close']
        high = latest_candle['high']
        low = latest_candle['low']
        
        if pd.isna(price) or pd.isna(high) or pd.isna(low):
            return None

        # 2. Oblicz proxy "centrum masy" (Typical Price)
        center_of_mass_proxy = (high + low + price) / 3.0
        
        # 3. Oblicz metrykę
        if price == 0:
            return None # Unikaj dzielenia przez zero
            
        price_gravity = (center_of_mass_proxy - price) / price
        return price_gravity
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_price_gravity_from_data' (Proxy HLC/3): {e}", exc_info=True)
        return None
# ==================================================================


# ==================================================================
# === KROK 21b: "Czyste" Funkcje dla Hipotezy H2 (Backtest) ===
# ==================================================================

def calculate_institutional_sync_from_data(insider_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 2.1) Oblicza 'institutional_sync'.
    Sztywna Formuła: Stosunek netto (Kupno-Sprzedaż) / (Kupno+Sprzedaż) z 90 dni.
    """
    try:
        # 1. Filtruj transakcje z ostatnich 90 dni (wg specyfikacji)
        ninety_days_ago = current_date - timedelta(days=90)
        
        # Porównanie Naive vs Naive jest poprawne (zakładając spójność danych wejściowych)
        recent_transactions = insider_df_view.loc[insider_df_view.index >= ninety_days_ago]
        
        if recent_transactions.empty:
            # ZGODNIE Z MEMO SUPPORTU: Fallback na 0.0
            return 0.0 

        # 2. Oblicz sumy
        # 'A' (Acquisition) i 'D' (Disposal)
        total_buys = recent_transactions[recent_transactions['transaction_type'] == 'A']['transaction_shares'].sum()
        total_sells = recent_transactions[recent_transactions['transaction_type'] == 'D']['transaction_shares'].sum()
        
        # 3. Oblicz metrykę
        denominator = total_buys + total_sells
        if denominator == 0:
            return 0.0 # Neutralny
            
        institutional_sync = (total_buys - total_sells) / denominator
        return institutional_sync
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_institutional_sync_from_data': {e}", exc_info=True)
        return None

def calculate_retail_herding_from_data(news_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 2.2) Oblicza 'retail_herding'.
    Sztywna Formuła: Średni sentyment z ostatnich 7 dni.
    """
    try:
        # 1. Filtruj artykuły z ostatnich 7 dni (wg specyfikacji)
        seven_days_ago = current_date - timedelta(days=7)
        
        # Obsługa stref czasowych
        if news_df_view.index.tz is not None:
            news_df_view_naive = news_df_view.tz_convert(None)
        else:
            news_df_view_naive = news_df_view

        recent_news = news_df_view_naive.loc[news_df_view_naive.index >= seven_days_ago]
        
        if recent_news.empty:
            # ZGODNIE Z MEMO SUPPORTU: Fallback na 0.0
            return 0.0 

        # 2. Oblicz średnią
        scores = recent_news['overall_sentiment_score']
        
        if scores.empty:
             return 0.0
             
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
    Definicja: Odwrotność znormalizowanej szerokości wstęg Bollingera.
    """
    try:
        if daily_df_view.empty or bbands_df_view.empty:
            return None

        # 1. Pobierz najnowsze wartości
        price = daily_df_view['close'].iloc[-1]
        
        # Znajdź najbliższe wstęgi
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

# ==================================================================
# === KLUCZOWA ZMIANA (ZGODNIE Z MEMO SUPPORTU 4.1) ===
# ==================================================================
def calculate_market_temperature_from_data(
    intraday_5min_df_view: pd.DataFrame, # IGNOROWANE - Zgodnie z PDF
    current_date: datetime,
    daily_df_view: Optional[pd.DataFrame] = None
) -> Optional[float]:
    """
    (Wymiar 4.1) Oblicza 'market_temperature'.
    
    ZGODNIE Z PDF ("Master Plan"): 
    Zastępujemy Intraday przez 30-dniową zmienność dziennych zwrotów.
    """
    try:
        if daily_df_view is None or daily_df_view.empty:
            logger.warning("Brak 'daily_df_view' w calculate_market_temperature - nie można obliczyć wg specyfikacji V3.")
            return None

        # 1. Filtruj dane dzienne z ostatnich 30 dni
        thirty_days_ago = current_date - timedelta(days=30)
        
        # Bezpieczny wycinek: bierzemy dane do 'current_date' i z nich ostatnie 30 wpisów
        # Zgodnie ze "Sztywną Formułą" z PDF: STDEV (returns_daily, period = 30)
        recent_daily_data = daily_df_view.loc[daily_df_view.index <= current_date].iloc[-31:] # Bierzemy 31, aby mieć 30 zmian procentowych

        if recent_daily_data.empty or len(recent_daily_data) < 2:
            return None # Za mało danych do obliczenia zwrotów

        # 2. Oblicz zwroty dzienne
        returns_daily = recent_daily_data['close'].pct_change().dropna()
        
        # 3. Oblicz odchylenie standardowe (to jest nowa 'market_temperature')
        # Upewniamy się, że mamy wystarczającą ilość danych do statystyki
        if len(returns_daily) < 20: 
             return None

        market_temperature = returns_daily.std()
        
        if pd.isna(market_temperature):
            return None
            
        return market_temperature
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_market_temperature_from_data': {e}", exc_info=True)
        return None
# ==================================================================


def calculate_information_entropy_from_data(news_df_view: pd.DataFrame) -> Optional[float]:
    """
    (Wymiar 4.2) Oblicza 'information_entropy'.
    Proxy: Liczba newsów z ostatnich 10 dni (S).
    """
    try:
        # Krok A: Ujednolić do NAIVE
        if news_df_view.index.tz is not None:
            news_df_view_naive = news_df_view.tz_convert(None)
        else:
            news_df_view_naive = news_df_view
        
        if news_df_view_naive.empty:
            return 0.0 
            
        # Krok B: Obliczenia na danych naiwnych
        latest_date_naive = news_df_view_naive.index[-1].to_pydatetime()
        ten_days_ago_naive = latest_date_naive - timedelta(days=10)
        
        # Krok C: Porównanie Naive vs Naive
        recent_news = news_df_view_naive.loc[news_df_view_naive.index >= ten_days_ago_naive]
        
        # Zwróć liczbę newsów
        S = len(recent_news)
        
        return float(S)

    except Exception as e:
        logger.error(f"Błąd w 'calculate_information_entropy_from_data': {e}", exc_info=True)
        return None

def calculate_attention_density_from_data(daily_df_view: pd.DataFrame, news_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 7.1) Oblicza 'attention_density'.
    Definicja: Z_Score(Vol_10d, 200) + Z_Score(News_10d, 200).
    """
    try:
        # 1. Oblicz 10-dniową średnią kroczącą dla WOLUMENU (dla ostatnich 200 dni)
        historical_avg_volume_10d = daily_df_view['volume'].rolling(window=10).mean()
        
        # Bierzemy OSTATNIE 200 punktów, które nie są NaN
        valid_volume_history = historical_avg_volume_10d.iloc[-200:].dropna()

        # Bieżący 10-dniowy średni wolumen (dla ostatniego dnia)
        avg_volume_10d = historical_avg_volume_10d.iloc[-1]
        
        # 2. Oblicz 10-dniową kroczącą LICZBĘ NEWSÓW (dla ostatnich 200 dni)
        if news_df_view.empty:
            normalized_news = 0.0 
        else:
            # Krok A: Ujednolić do NAIVE
            if news_df_view.index.tz is not None:
                news_df_view_naive = news_df_view.tz_convert(None)
            else:
                news_df_view_naive = news_df_view
            
            # a) Zlicz newsy dziennie
            news_counts_daily = news_df_view_naive.groupby(news_df_view_naive.index.date).size()
            news_counts_daily.index = pd.to_datetime(news_counts_daily.index)
            # Uzupełnij brakujące dni (weekendy) zerami
            news_counts_daily = news_counts_daily.reindex(pd.date_range(start=news_counts_daily.index.min(), end=news_counts_daily.index.max(), freq='D'), fill_value=0)
            
            # b) Oblicz kroczącą sumę z 10-dniowego okna
            historical_news_count_10d = news_counts_daily.rolling(window=10).sum()
            
            # Bierzemy OSTATNIE 200 punktów, które nie są NaN
            valid_news_history = historical_news_count_10d.iloc[-200:].dropna()
            
            # Krok C: Pobierz wartość na 'current_date'
            news_count_10d = historical_news_count_10d.asof(current_date)
            
            if valid_news_history.empty or pd.isna(news_count_10d):
                normalized_news = 0.0
            else:
                # Z-Score dla Newsów
                news_mean = valid_news_history.mean()
                news_std = valid_news_history.std()
                if news_std == 0:
                    normalized_news = 0.0
                else:
                    normalized_news = (news_count_10d - news_mean) / news_std
        
        # Warunek: Mamy CO NAJMNIEJ 200 PUNKTÓW do obliczenia Z-Score.
        if len(valid_volume_history) < 200 or pd.isna(avg_volume_10d):
             return None

        # 3. Oblicz Z-Score dla Wolumenu
        vol_mean = valid_volume_history.mean()
        vol_std = valid_volume_history.std()
        if vol_std == 0:
            normalized_volume = 0.0
        else:
            normalized_volume = (avg_volume_10d - vol_mean) / vol_std
            
        # 4. Oblicz metrykę m² (attention_density)
        attention_density = normalized_volume + normalized_news
        
        if pd.isna(attention_density):
            return None
            
        return attention_density

    except Exception as e:
        logger.error(f"Błąd w 'calculate_attention_density_from_data': {e}", exc_info=True)
        return None


# ==================================================================
# === Funkcja Agregująca dla Hipotezy H3 ===
# ==================================================================
def calculate_h3_components_for_day(
    current_date: datetime,
    daily_view: pd.DataFrame,        # Widok Dzienny do daty J
    insider_df: pd.DataFrame,        # Pełna historia insider
    news_df: pd.DataFrame,           # Pełna historia news
    full_daily_df: pd.DataFrame,     # Pełny DF (dla BBANDS/History)
    intraday_5min_df: pd.DataFrame   # Ignorowane
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Oblicza trzy główne komponenty modelu H3 (J, Nabla^2, m^2) dla JEDNEGO DNIA.
    """
    try:
        # === 1. Oblicz J (entropy_change) ===
        # J = S - (Q / T) + (μ * ΔN)
        
        # S = information_entropy (Proxy: Liczba newsów z 10 dni)
        S = calculate_information_entropy_from_data(news_df)
        
        # Q = retail_herding (Średni sentyment z 7 dni)
        Q = calculate_retail_herding_from_data(news_df, current_date)
        
        # T = market_temperature (Zmienność dzienna z 30 dni)
        T = calculate_market_temperature_from_data(
            intraday_5min_df, 
            current_date,
            daily_view 
        )
        
        # μ = institutional_sync (Stosunek insiderów z 90 dni)
        mu = calculate_institutional_sync_from_data(insider_df, current_date)
        
        # ΔN = 1.0 (zgodnie ze specyfikacją PDF)
        delta_N = 1.0
        
        J = None
        if S is not None and Q is not None and T is not None and mu is not None:
            if T == 0 or pd.isna(T):
                J = S + (mu * delta_N) # Unikaj dzielenia przez zero
            else:
                J = S - (Q / T) + (mu * delta_N)

        # === 2. Oblicz ∇² (nabla_sq) = price_gravity ===
        nabla_sq = calculate_price_gravity_from_data(daily_view)

        # === 3. Oblicz m² (m_sq) = attention_density ===
        # Wymaga 'full_daily_df' do obliczenia 200-dniowego Z-Score wolumenu
        m_sq = calculate_attention_density_from_data(full_daily_df, news_df, current_date)
        
        return J, nabla_sq, m_sq

    except Exception as e:
        logger.error(f"Błąd w 'calculate_h3_components_for_day' dla daty {current_date}: {e}", exc_info=True)
        return None, None, None

# ==================================================================
# === Funkcje "Na Żywo" (Dla pojedynczego punktu danych) ===
# ==================================================================

def calculate_time_dilation_live(ticker: str, ticker_daily_df: pd.DataFrame, spy_daily_df: pd.DataFrame) -> Optional[float]:
    """(Wymiar 1.1) Oblicza 'time_dilation' używając przekazanych DF (wersja live)."""
    try:
        if ticker_daily_df.empty or spy_daily_df.empty:
            return None

        ticker_returns = ticker_daily_df['close'].pct_change()
        spy_returns = spy_daily_df['close'].pct_change()
        
        # Wymuszamy ostatnie 20 dni
        stddev_ticker_20 = ticker_returns.tail(20).std()
        stddev_spy_20 = spy_returns.tail(20).std()
        
        if stddev_spy_20 == 0: return None
        return stddev_ticker_20 / stddev_spy_20
    except Exception as e:
        logger.error(f"Błąd w 'calculate_time_dilation_live' dla {ticker}: {e}", exc_info=True)
        return None

def calculate_price_gravity_live(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient = None) -> Optional[float]:
    """(Wymiar 1.2) Oblicza 'price_gravity' (na żywo, proxy HLC/3)."""
    try:
        if ticker_daily_df.empty: return None
        
        latest_candle = ticker_daily_df.iloc[-1]
        price = latest_candle['close']
        high = latest_candle['high']
        low = latest_candle['low']
        
        if pd.isna(price) or pd.isna(high) or pd.isna(low):
            return None

        center_of_mass_proxy = (high + low + price) / 3.0
        
        if price == 0: return None
        price_gravity = (center_of_mass_proxy - price) / price
        return price_gravity
    except Exception as e:
        logger.error(f"Błąd w 'calculate_price_gravity_live' dla {ticker}: {e}", exc_info=True)
        return None

def calculate_institutional_sync_live(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """(Wymiar 2.1) Oblicza 'institutional_sync' (wymaga 1 wywołania API)."""
    try:
        insider_data = api_client.get_insider_transactions(ticker)
        if not insider_data or 'data' not in insider_data or not insider_data['data']:
            logger.warning(f"Brak danych Insider Transactions (live) dla {ticker}")
            return 0.0

        transactions = insider_data['data']
        total_buys = 0.0
        total_sells = 0.0
        
        ninety_days_ago = datetime.now() - timedelta(days=90)
        
        for tx in transactions:
            try:
                tx_date = datetime.strptime(tx['transaction_date'], '%Y-%m-%d')
                if tx_date < ninety_days_ago:
                    continue
                
                shares_str = tx.get('shares')
                if not shares_str: continue
                shares = float(shares_str)
                tx_type = tx.get('acquisition_or_disposal')
                
                if tx_type == 'A':
                    total_buys += shares
                elif tx_type == 'D':
                    total_sells += shares
            except (ValueError, TypeError):
                continue

        denominator = total_buys + total_sells
        if denominator == 0:
            return 0.0
            
        return (total_buys - total_sells) / denominator
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_institutional_sync_live' dla {ticker}: {e}", exc_info=True)
        return None

def calculate_retail_herding_live(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """(Wymiar 2.2) Oblicza 'retail_herding' (wymaga 1 wywołania API)."""
    try:
        seven_days_ago_str = (datetime.now() - timedelta(days=7)).strftime('%Y%m%dT%H%M')
        
        news_data = api_client.get_news_sentiment(ticker, limit=100, time_from=seven_days_ago_str)
        if not news_data or 'feed' not in news_data or not news_data['feed']:
            logger.warning(f"Brak danych News Sentiment (live) dla {ticker}")
            return 0.0

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
        logger.error(f"Błąd w 'calculate_retail_herding_live' dla {ticker}: {e}", exc_info=True)
        return None

# ==================================================================
# === NOWE FUNKCJE V4 - BEZPIECZNIE DODANE PONIŻEJ ISTNIEJĄCYCH ===
# ==================================================================

def calculate_h3_components_v4(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    BEZPIECZNA NOWA FUNKCJA: Ulepszona implementacja H3 z normalizacją V4
    Używana przez backtest_engine i phase3_sniper dla spójności
    """
    try:
        # Importujemy nową funkcję z utils
        from .utils import calculate_h3_metrics_v4
        return calculate_h3_metrics_v4(df, params)
        
    except Exception as e:
        logger.error(f"Błąd calculate_h3_components_v4: {e}")
        return df

def calculate_retail_herding_capped_v4(retail_herding_series: pd.Series) -> pd.Series:
    """
    BEZPIECZNA NOWA FUNKCJA: Capping wartości ekstremalnych Retail Herding
    """
    return retail_herding_series.clip(-1.0, 1.0)
