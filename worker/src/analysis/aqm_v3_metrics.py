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
# ==================================================================
# === POPRAWKA BŁĘDU (TZ-NAIVE vs TZ-AWARE) ===
# Dodajemy import 'timezone', aby móc ujednolicić strefy czasowe.
# ==================================================================
from datetime import datetime, timedelta, timezone
# ==================================================================
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
    
    ZGODNIE Z MEMO SUPPORTU: Używamy 'adjusted close' do zwrotów (co 'standardize_df_columns'
    mapuje na 'close' z endpointu 'TIME_SERIES_DAILY_ADJUSTED', więc jest OK).
    """
    try:
        # 1. Oblicz zwroty (daily_df_view pochodzi z TIME_SERIES_DAILY_ADJUSTED, więc 'close' to 'adjusted close')
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

# ==================================================================
# === KLUCZOWA ZMIANA (ZGODNIE Z MEMO SUPPORTU 1.2) ===
# ==================================================================
def calculate_price_gravity_from_data(daily_df_view: pd.DataFrame, vwap_df_view: pd.DataFrame = None) -> Optional[float]:
    """
    (Wymiar 1.2) Oblicza 'price_gravity' na podstawie historycznych widoków DataFrame.
    
    ZGODNIE Z MEMO SUPPORTU: Zastępujemy VWAP przez proxy (H+L+C)/3.
    """
    try:
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
    (Wymiar 2.1) Oblicza 'institutional_sync' na podstawie historycznego widoku DataFrame
    z transakcjami insiderów (za ostatnie 90 dni).
    """
    try:
        # 1. Filtruj transakcje z ostatnich 90 dni (wg specyfikacji)
        ninety_days_ago = current_date - timedelta(days=90)
        
        # Ta funkcja jest bezpieczna:
        # 'current_date' (z daily_df) jest "naiwna" (naive).
        # 'insider_df_view.index' (z _parse_insider_transactions) też jest "naiwny".
        # Porównanie Naive vs Naive jest poprawne.
        
        # Używamy .loc do filtrowania po indeksie (który jest datą)
        recent_transactions = insider_df_view.loc[insider_df_view.index >= ninety_days_ago]
        
        if recent_transactions.empty:
            # ZGODNIE Z MEMO SUPPORTU: Fallback na 0.0 (zamiast NaN/None)
            return 0.0 

        # 2. Oblicz sumy (zgodnie ze "Sztywną Formułą")
        # === POPRAWKA: Używamy 'A' (Acquisition) i 'D' (Disposal) ===
        total_buys = recent_transactions[recent_transactions['transaction_type'] == 'A']['transaction_shares'].sum()
        total_sells = recent_transactions[recent_transactions['transaction_type'] == 'D']['transaction_shares'].sum()
        
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
        
        # ==================================================================
        # === POPRAWKA BŁĘDU (TypeError: tz-naive vs tz-aware) ===
        # ==================================================================
        
        # Krok A: 'current_date' jest NAIVE (z daily_df)
        seven_days_ago = current_date - timedelta(days=7)
        
        # Krok B: 'news_df_view.index' jest AWARE (UTC) gdy ma dane,
        # lub NAIVE (datetime64[ns]) gdy jest pusty.
        # Musimy ujednolicić wszystko do NAIVE, aby pasowało do 'current_date'.
        
        if news_df_view.index.tz is not None:
            # Jeśli indeks jest świadomy (Aware), konwertujemy go na naiwny (Naive)
            news_df_view_naive = news_df_view.tz_convert(None)
        else:
            # Jeśli indeks jest już naiwny (np. pusty), używamy go bez zmian
            news_df_view_naive = news_df_view

        # Krok C: Porównujemy Naive vs Naive (bezpieczne)
        recent_news = news_df_view_naive.loc[news_df_view_naive.index >= seven_days_ago]
        
        # ==================================================================
        # === KONIEC POPRAWKI ===
        # ==================================================================
        
        if recent_news.empty:
            # ZGODNIE Z MEMO SUPPORTU: Fallback na 0.0 (zamiast NaN/None)
            return 0.0 

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
        # UWAGA: Używamy asof, aby pobrać NAJBLIŻSZĄ wartość BBANDS
        # BBANDS DataFrame może nie mieć dokładnych dat zamknięcia rynku
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
    intraday_5min_df_view: pd.DataFrame, # TEN ARGUMENT JEST IGNOROWANY
    current_date: datetime,
    daily_df_view: Optional[pd.DataFrame] = None # UŻYWAMY TEGO ARGUMENTU
) -> Optional[float]:
    """
    (Wymiar 4.1) Oblicza 'market_temperature'.
    
    ZGODNIE Z MEMO SUPPORTU: Używamy 30-dniowej zmienności dziennych zwrotów.
    """
    try:
        # 1. Sprawdź, czy mamy nowy argument
        if daily_df_view is not None and not daily_df_view.empty:
            # =============================================
            # === NOWA LOGIKA (ZGODNIE Z MEMO SUPPORTU) ===
            # =============================================
            
            # 1. Filtruj dane dzienne z ostatnich 30 dni
            thirty_days_ago = current_date - timedelta(days=30)
            
            # Bezpieczny wycinek: bierzemy dane do 'current_date' i z nich ostatnie 30 wpisów
            recent_daily_data = daily_df_view.loc[daily_df_view.index <= current_date].iloc[-30:]

            if recent_daily_data.empty or len(recent_daily_data) < 2:
                return None # Za mało danych do obliczenia zwrotów

            # 2. Oblicz zwroty dzienne
            returns_daily = recent_daily_data['close'].pct_change()
            
            # 3. Oblicz odchylenie standardowe (to jest nowa 'market_temperature')
            market_temperature = returns_daily.std()
            # =============================================
        
        else:
            # Fallback (powinien być martwy, ale dla bezpieczeństwa)
            logger.warning("Brak 'daily_df_view' - używam starej logiki (Intraday).")
            
            # STARA LOGIKA (Intraday 5min)
            # ==================================================================
            # === POPRAWKA BŁĘDU (TZ-NAIVE vs TZ-AWARE) ===
            # Ta logika jest martwa, ale na wszelki wypadek ją też naprawimy
            if current_date.tzinfo is None:
                current_date_utc = current_date.replace(tzinfo=timezone.utc)
            else:
                current_date_utc = current_date
            thirty_days_ago_utc = current_date_utc - timedelta(days=30)

            if intraday_5min_df_view.index.tz is not None:
                intraday_naive = intraday_5min_df_view.tz_convert(None)
            else:
                intraday_naive = intraday_5min_df_view
            # ==================================================================

            recent_intraday_data = intraday_naive.loc[intraday_naive.index >= (current_date - timedelta(days=30))]
            
            if recent_intraday_data.empty or len(recent_intraday_data) < 2:
                return None
            returns_5min = recent_intraday_data['close'].pct_change()
            market_temperature = returns_5min.std()
            
        if pd.isna(market_temperature):
            return None
            
        return market_temperature
        
    except Exception as e:
        logger.error(f"Błąd w 'calculate_market_temperature_from_data': {e}", exc_info=True)
        return None
# ==================================================================
# === KONIEC KLUCZOWEJ ZMIANY ===
# ==================================================================


def calculate_information_entropy_from_data(news_df_view: pd.DataFrame) -> Optional[float]:
    """
    (Wymiar 4.2) Oblicza 'information_entropy' (Entropia Informacyjna) jako proxy (Liczba newsów).
    """
    try:
        # 1. Oblicz S - Liczba newsów z ostatnich 10 dni
        
        # ==================================================================
        # === POPRAWKA BŁĘDU (TZ-NAIVE vs TZ-AWARE) ===
        # ==================================================================
        
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
        # ==================================================================
        
        # Zwróć liczbę newsów
        S = len(recent_news)
        
        return float(S)

    except Exception as e:
        logger.error(f"Błąd w 'calculate_information_entropy_from_data' (Proxy Liczba Newsów): {e}", exc_info=True)
        return None

# ==================================================================
# === NAPRAWA BŁĘDU LOGICZNEGO (Spam w logach) ===
# ==================================================================
def calculate_attention_density_from_data(daily_df_view: pd.DataFrame, news_df_view: pd.DataFrame, current_date: datetime) -> Optional[float]:
    """
    (Wymiar 7.1) Oblicza 'attention_density'.
    
    POPRAWKA AWARYJNA: Naprawiona walidacja Z-Score dla 200 dni.
    """
    try:
        # 1. Oblicz 10-dniową średnią kroczącą dla WOLUMENU (dla ostatnich 200 dni)
        
        # Obliczenia kroczącej średniej wolumenu
        historical_avg_volume_10d = daily_df_view['volume'].rolling(window=10).mean()
        
        # Bierzemy OSTATNIE 200 punktów, które nie są NaN
        valid_volume_history = historical_avg_volume_10d.iloc[-200:].dropna()

        # Bieżący 10-dniowy średni wolumen (dla ostatniego dnia)
        avg_volume_10d = historical_avg_volume_10d.iloc[-1]
        
        
        # 2. Oblicz 10-dniową kroczącą LICZBĘ NEWSÓW (dla ostatnich 200 dni)
        
        # ZGODNIE Z MEMO SUPPORTU: Obsługa braku newsów
        if news_df_view.empty:
            # Poprawka: Fallback 0.0 bez spamowania logami
            normalized_news = 0.0 
        else:
            
            # ==================================================================
            # === POPRAWKA BŁĘDU (TZ-NAIVE vs TZ-AWARE) ===
            # ==================================================================
            # Krok A: Ujednolić do NAIVE
            if news_df_view.index.tz is not None:
                news_df_view_naive = news_df_view.tz_convert(None)
            else:
                news_df_view_naive = news_df_view
            
            # a) Zlicz newsy dziennie (na danych naiwnych)
            news_counts_daily = news_df_view_naive.resample('D').size()
            # ==================================================================
            
            # b) Oblicz kroczącą sumę z 10-dniowego okna
            historical_news_count_10d = news_counts_daily.rolling(window=10).sum()
            
            # Bierzemy OSTATNIE 200 punktów, które nie są NaN
            valid_news_history = historical_news_count_10d.iloc[-200:].dropna()
            
            # Krok C: Porównanie Naive (asof) vs Naive (current_date)
            # 'current_date' jest już naiwna
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
                    # Używamy znormalizowanego Z-Score
                    normalized_news = (news_count_10d - news_mean) / news_std
        
        # ==================================================================
        # === KLUCZOWA POPRAWKA ===
        # Warunek musi być, że mamy CO NAJMNIEJ 200 PUNKTÓW do obliczenia Z-Score.
        # W przeciwnym razie nie można obliczyć stabilnego odchylenia standardowego.
        # ==================================================================
        if len(valid_volume_history) < 200 or pd.isna(avg_volume_10d):
             # logger.warning(f"Brak wystarczającej historii wolumenu ({len(valid_volume_history)}) do obliczenia Z-Score.") # Usuwamy, by uniknąć spamowania
             return None


        # 3. Oblicz Z-Score (dla ostatniego dnia)
        
        # Z-Score dla Wolumenu
        vol_mean = valid_volume_history.mean()
        vol_std = valid_volume_history.std()
        if vol_std == 0:
            normalized_volume = 0.0
        else:
            # Używamy znormalizowanego Z-Score: (wartość - średnia) / odchylenie
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


# ==================================================================
# === NOWA FUNKCJA (Brakujący Atrybut z H3/H4) ===
# Ta funkcja łączy wszystkie metryki w komponenty H3
# ==================================================================
def calculate_h3_components_for_day(
    current_date: datetime,
    daily_view: pd.DataFrame,        # Widok Dzienny do daty J
    insider_df: pd.DataFrame,          # Pełna historia insider
    news_df: pd.DataFrame,             # Pełna historia news
    full_daily_df: pd.DataFrame,       # Pełny DF (dla BBANDS/History)
    intraday_5min_df: pd.DataFrame     # (Pusty) DF
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Oblicza trzy główne komponenty modelu H3 (J, Nabla^2, m^2) dla JEDNEGO DNIA.
    Jest to funkcja, której brakowało (AttributeError).
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
            intraday_5min_df, # Ignorowane
            current_date,
            daily_view # Używamy widoku dziennego
        )
        
        # μ = institutional_sync (Stosunek insiderów z 90 dni)
        mu = calculate_institutional_sync_from_data(insider_df, current_date)
        
        # ΔN = 1.0 (zgodnie ze specyfikacją PDF)
        delta_N = 1.0
        
        J = None
        if S is not None and Q is not None and T is not None and mu is not None:
            if T == 0 or pd.isna(T):
                J = S + (mu * delta_N) # Unikaj dzielenia przez zero lub błędu NaN
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
# === KONIEC NOWEJ FUNKCJI ===
# ==================================================================


# ==================================================================
# === Funkcje "Na Żywo" (Oryginalna Logika - jeszcze nieużywane) ===
# ==================================================================

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

def calculate_price_gravity_live(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient = None) -> Optional[float]:
    """
    (Wymiar 1.2) Oblicza 'price_gravity' (na żywo).
    ZGODNIE Z MEMO SUPPORTU: Używamy proxy (H+L+C)/3.
    """
    try:
        # Pobierz najnowsze wartości OHLC
        latest_candle = ticker_daily_df.iloc[-1]
        price = latest_candle['close']
        high = latest_candle['high']
        low = latest_candle['low']
        
        if pd.isna(price) or pd.isna(high) or pd.isna(low):
            return None

        # Oblicz proxy "centrum masy"
        center_of_mass_proxy = (high + low + price) / 3.0
        
        # Oblicz metrykę
        if price == 0: return None
        price_gravity = (center_of_mass_proxy - price) / price
        return price_gravity
    except Exception as e:
        logger.error(f"Błąd w 'calculate_price_gravity' dla {ticker}: {e}", exc_info=True)
        return None

def calculate_institutional_sync_live(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """(Wymiar 2.1) Oblicza 'institutional_sync' (wymaga 1 wywołania API)."""
    try:
        insider_data = api_client.get_insider_transactions(ticker)
        # === POPRAWKA: Używamy klucza 'data' ===
        if not insider_data or 'data' not in insider_data or not insider_data['data']:
            logger.warning(f"Brak danych Insider Transactions (live) dla {ticker}")
            return 0.0 # Neutralny, jeśli brak transakcji

        # === POPRAWKA: Używamy klucza 'data' ===
        transactions = insider_data['data']
        total_buys = 0.0
        total_sells = 0.0
        
        # Data graniczna (90 dni temu)
        ninety_days_ago = datetime.now() - timedelta(days=90)
        
        for tx in transactions:
            try:
                # === POPRAWKA: Używamy poprawnych nazw pól ===
                tx_date = datetime.strptime(tx['transaction_date'], '%Y-%m-%d')
                if tx_date < ninety_days_ago:
                    continue # Transakcja zbyt stara
                
                shares_str = tx.get('shares')
                if not shares_str:
                    continue
                shares = float(shares_str)
                tx_type = tx.get('acquisition_or_disposal')
                
                if tx_type == 'A':
                    total_buys += shares
                elif tx_type == 'D':
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
