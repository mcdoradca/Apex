# ==================================================================
# === PLIK KROKU 15: Silnik Obliczeniowy Metryk AQM V3 ===
#
# Cel: Przetłumaczenie "Sztywnych Formuł Analitycznych" 
# z dokumentacji AQM V3 (7 Wymiarów) na kod Python.
# ==================================================================

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple, List
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

# Importy bibliotek statystycznych dodanych w Kroku 14
from scipy.stats import entropy as shannon_entropy
from scipy.stats import zscore
from statsmodels.tsa.stattools import grangercausalitytests

# Import klienta API (uzbrojonego w Kroku 12) i utils
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import standardize_df_columns

logger = logging.getLogger(__name__)

# Stała Plancka Rynku (wg PDF Wymiar 5, Heisenberg)
RYNEK_HBAR = 1.0 

# Stałe do normalizacji Z-Score dla Wymiaru 7 (Gęstość Uwagi)
# Ponieważ nie mamy 200-dniowej historii liczby newsów,
# używamy statycznego przybliżenia (zgodnie z analizą).
HISTORICAL_NEWS_MEAN = 20
HISTORICAL_NEWS_STD = 10


# === WYMIAR 1: TIME-SPACE CONTINUUM (Struktura Czasoprzestrzeni) ===

def calculate_time_dilation(ticker_daily_df: pd.DataFrame, spy_daily_df: pd.DataFrame, period: int = 20) -> Optional[float]:
    """
    Oblicza Metrykę 1.1: time_dilation (Dylatacja Czasu)
    Stosunek 20-dniowej zmienności (STDEV zwrotów) spółki do SPY.
    """
    try:
        if len(ticker_daily_df) < period or len(spy_daily_df) < period:
            logger.warning(f"[AQM V3] Niewystarczające dane do obliczenia Dylatacji Czasu (wymagane {period} dni).")
            return None

        # 1. Oblicz zwroty procentowe
        ticker_returns = ticker_daily_df['close'].pct_change()
        spy_returns = spy_daily_df['close'].pct_change()
        
        # 2. Oblicz 20-dniowe odchylenie standardowe zwrotów
        stddev_ticker_20 = ticker_returns.iloc[-period:].std()
        stddev_spy_20 = spy_returns.iloc[-period:].std()

        if stddev_spy_20 == 0:
            logger.warning("[AQM V3] Dylatacja Czasu: Odchylenie standardowe SPY wynosi 0. Nie można obliczyć.")
            return None
            
        # 3. time_dilation = stddev_ticker / stddev_spy
        time_dilation = stddev_ticker_20 / stddev_spy_20
        
        return float(time_dilation)

    except Exception as e:
        logger.error(f"[AQM V3] Błąd w calculate_time_dilation: {e}", exc_info=True)
        return None

def calculate_price_gravity(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient) -> Optional[float]:
    """
    Oblicza Metrykę 1.2: price_gravity (Grawitacja Cenowa)
    Odległość ceny od jej centrum masy (VWAP).
    """
    try:
        # 1. price = DAILY_CLOSE(ticker)
        price = ticker_daily_df['close'].iloc[-1]
        
        # 2. center_of_mass = VWAP(ticker, daily)
        vwap_data = api_client.get_vwap(ticker, interval='daily')
        if not vwap_data or 'Technical Analysis: VWAP' not in vwap_data:
            logger.warning(f"[AQM V3 {ticker}] Brak danych VWAP z API.")
            return None
            
        # Znajdź najnowszy dostępny wpis VWAP
        latest_vwap_key = sorted(vwap_data['Technical Analysis: VWAP'].keys(), reverse=True)[0]
        center_of_mass = float(vwap_data['Technical Analysis: VWAP'][latest_vwap_key]['VWAP'])

        if price == 0:
            logger.warning(f"[AQM V3 {ticker}] Grawitacja Cenowa: Cena wynosi 0. Nie można obliczyć.")
            return None

        # 3. price_gravity = (center_of_mass - price) / price
        price_gravity = (center_of_mass - price) / price
        
        return float(price_gravity)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_price_gravity: {e}", exc_info=True)
        return None


# === WYMIAR 2: QUANTUM ENTANGLEMENT (Splątanie Uczestników) ===

def calculate_institutional_sync(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """
    Oblicza Metrykę 2.1: institutional_sync (Synchronizacja Instytucjonalna)
    Stosunek netto transakcji kupna do sprzedaży przez insiderów (90 dni).
    """
    try:
        transactions_data = api_client.get_insider_transactions(ticker)
        if not transactions_data:
            logger.warning(f"[AQM V3 {ticker}] Brak danych INSIDER_TRANSACTIONS z API.")
            return None # Neutralny wynik, jeśli brak danych

        ninety_days_ago = datetime.now() - timedelta(days=90)
        total_buys = 0.0
        total_sells = 0.0

        for tx in transactions_data:
            try:
                # Data transakcji w formacie 'YYYY-MM-DD'
                tx_date = datetime.strptime(tx['transactionDate'], '%Y-%m-%d')
                if tx_date < ninety_days_ago:
                    continue # Transakcja zbyt stara

                shares = float(tx['transactionShares'])
                tx_type = tx['transactionType']

                if tx_type == 'P-Purchase':
                    total_buys += shares
                elif tx_type == 'S-Sale':
                    total_sells += shares
            except Exception:
                continue # Pomiń błędny wpis

        denominator = total_buys + total_sells
        if denominator == 0:
            logger.info(f"[AQM V3 {ticker}] Synchronizacja Inst.: Brak transakcji K/S w ostatnich 90 dniach.")
            return 0.0 # Neutralny wynik

        # 4. institutional_sync = (total_buys - total_sells) / (total_buys + total_sells)
        institutional_sync = (total_buys - total_sells) / denominator
        
        return float(institutional_sync)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_institutional_sync: {e}", exc_info=True)
        return None

def calculate_retail_herding(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """
    Oblicza Metrykę 2.2: retail_herding (Zachowanie Stadne Detalu)
    Średni sentyment z wiadomości z ostatnich 7 dni.
    """
    try:
        seven_days_ago_str = (datetime.now() - timedelta(days=7)).strftime('%Y%m%dT%H%M')
        
        news_data = api_client.get_news_sentiment(ticker, limit=100, time_from=seven_days_ago_str)
        if not news_data or 'feed' not in news_data or not news_data['feed']:
            logger.warning(f"[AQM V3 {ticker}] Zachowanie Stadne: Brak newsów z ostatnich 7 dni.")
            return 0.0 # Neutralny wynik

        scores = []
        for article in news_data['feed']:
            # Sprawdzamy, czy ticker jest w 'topics', aby mieć pewność
            if any(t['ticker'] == ticker for t in article.get('topics', [])):
                score = article.get('overall_sentiment_score')
                if score is not None:
                    scores.append(float(score))

        if not scores:
            logger.warning(f"[AQM V3 {ticker}] Zachowanie Stadne: Znaleziono newsy, ale bez ocen sentymentu.")
            return 0.0 # Neutralny wynik

        # 3. retail_herding = AVG(scores)
        retail_herding = sum(scores) / len(scores)
        return float(retail_herding)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_retail_herding: {e}", exc_info=True)
        return None


# === WYMIAR 3: ENERGY POTENTIAL FIELD (Pole Potencjału Energii) ===

def calculate_breakout_energy_required(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient) -> Optional[float]:
    """
    Oblicza Metrykę 3.1: breakout_energy_required (Wymagana Energia Wybicia)
    Odwrotność znormalizowanej szerokości Wstęg Bollingera.
    """
    try:
        # 1. & 2. Pobierz BBANDS
        bbands_data = api_client.get_bollinger_bands(ticker, interval='daily', time_period=20, nbdevup=2, nbdevdn=2)
        if not bbands_data or 'Technical Analysis: BBANDS' not in bbands_data:
            logger.warning(f"[AQM V3 {ticker}] Brak danych BBANDS z API.")
            return None

        # Znajdź najnowsze wpisy
        latest_bbands_key = sorted(bbands_data['Technical Analysis: BBANDS'].keys(), reverse=True)[0]
        bbands_values = bbands_data['Technical Analysis: BBANDS'][latest_bbands_key]
        
        upper_band = float(bbands_values['Real Upper Band'])
        lower_band = float(bbands_values['Real Lower Band'])

        # 3. price = DAILY_CLOSE(ticker)
        # Używamy tej samej daty, co BBANDS
        if latest_bbands_key not in ticker_daily_df.index:
             price = ticker_daily_df['close'].iloc[-1] # Awaryjnie ostatnia cena
        else:
             price = ticker_daily_df.loc[latest_bbands_key]['close']

        if price == 0:
            logger.warning(f"[AQM V3 {ticker}] Energia Wybicia: Cena wynosi 0. Nie można obliczyć.")
            return None

        # 4. band_width_normalized = (upper_band - lower_band) / price
        band_width_normalized = (upper_band - lower_band) / price

        if band_width_normalized == 0:
            logger.warning(f"[AQM V3 {ticker}] Energia Wybicia: Szerokość wstęg wynosi 0.")
            return None # Zwracamy None, bo 1/0 jest nieskończone

        # 5. breakout_energy_required = 1 / band_width_normalized
        breakout_energy_required = 1.0 / band_width_normalized
        
        return float(breakout_energy_required)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_breakout_energy_required: {e}", exc_info=True)
        return None


# === WYMIAR 4: INFORMATION THERMODYNAMICS (Termodynamika Informacji) ===

def calculate_market_temperature(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """
    Oblicza Metrykę 4.1: market_temperature (Temperatura Rynku)
    Zmienność (STDEV) zwrotów wewnątrz-dziennych (5min) z ostatnich 30 dni.
    """
    try:
        # 1. Pobierz dane 5-minutowe z ostatnich 30 dni.
        # Musimy pobrać 'full', aby mieć pewność 30 dni
        intraday_data = api_client.get_intraday(ticker, interval='5min', outputsize='full')
        if not intraday_data or 'Time Series (5min)' not in intraday_data:
            logger.warning(f"[AQM V3 {ticker}] Temperatura Rynku: Brak danych 5min Intraday z API.")
            return None
            
        df = pd.DataFrame.from_dict(intraday_data['Time Series (5min)'], orient='index')
        df = standardize_df_columns(df)
        df.index = pd.to_datetime(df.index) # Upewnij się, że indeks to datetime

        # Filtruj dane z ostatnich 30 dni
        thirty_days_ago = datetime.now() - timedelta(days=30)
        df_30_days = df[df.index >= thirty_days_ago]
        
        if len(df_30_days) < 100: # Arbitralny próg minimalnej ilości danych
            logger.warning(f"[AQM V3 {ticker}] Temperatura Rynku: Za mało danych 5min z ostatnich 30 dni ({len(df_30_days)} świec).")
            return None

        # 2. returns_5min = 5min_CLOSE.pct_change()
        returns_5min = df_30_days['close'].pct_change().dropna()

        # 3. market_temperature = STDEV(returns_5min)
        market_temperature = returns_5min.std()
        
        return float(market_temperature)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_market_temperature: {e}", exc_info=True)
        return None

def calculate_information_entropy(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """
    Oblicza Metrykę 4.2: information_entropy (Entropia Informacyjna)
    Entropia Shannona na podstawie 100 ostatnich tematów wiadomości.
    """
    try:
        # 1. Pobierz 100 ostatnich artykułów.
        news_data = api_client.get_news_sentiment(ticker, limit=100)
        if not news_data or 'feed' not in news_data or not news_data['feed']:
            logger.warning(f"[AQM V3 {ticker}] Entropia Inform.: Brak newsów (limit 100).")
            return None # Nie 0.0, bo brak newsów to nie niska entropia

        # 2. topic_list = [topic['topic'] for article in news_feed for topic in article['topics']]
        topic_list = []
        for article in news_data['feed']:
            # Bierzemy tylko tematy dotyczące *naszej* spółki
            for topic in article.get('topics', []):
                if topic.get('ticker') == ticker:
                    topic_list.append(topic.get('topic'))
        
        if not topic_list:
            logger.warning(f"[AQM V3 {ticker}] Entropia Inform.: Znaleziono newsy, ale brak tagów 'topic' dla {ticker}.")
            return None

        # 3. topic_counts = COUNT(każdego unikalnego tematu w topic_list)
        # 4. total_topics = LEN(topic_list)
        # 5. probabilities = [count / total_topics for count in topic_counts.values()]
        # Używamy pd.Series dla łatwego obliczenia prawdopodobieństw
        topic_series = pd.Series(topic_list)
        probabilities = topic_series.value_counts(normalize=True)
        
        # 6. information_entropy = -SUM([p* log2(p) for p in probabilities])
        # Używamy scipy.stats.entropy z bazą 2
        entropy = shannon_entropy(probabilities, base=2)
        
        return float(entropy)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_information_entropy: {e}", exc_info=True)
        return None


# === WYMIAR 5: MULTIVERSE PROBABILITY (Prawdopodobieństwo Wieloświata) ===

def get_wave_function_collapse_event(ticker: str, api_client: AlphaVantageClient) -> Optional[Dict[str, Any]]:
    """
    Pobiera Metrykę 5.1: wave_function_collapse (Zapaść Funkcji Falowej)
    Zwraca dane o ostatnim "pomiarze" (wynikach kwartalnych).
    """
    try:
        earnings_data = api_client.get_earnings(ticker)
        if not earnings_data or 'quarterlyEarnings' not in earnings_data or not earnings_data['quarterlyEarnings']:
            logger.warning(f"[AQM V3 {ticker}] Zapaść Funkcji Fal.: Brak danych EARNINGS.")
            return None
            
        # 1. measurement_date = EARNINGS(reportedDate)
        latest_earning = earnings_data['quarterlyEarnings'][0]
        reported_date_str = latest_earning.get('reportedDate')
        
        # 2. measurement_impact = reportedEPS - estimatedEPS
        reported_eps = latest_earning.get('reportedEPS')
        estimated_eps = latest_earning.get('estimatedEPS')
        
        if reported_eps == 'None' or estimated_eps == 'None' or reported_eps is None or estimated_eps is None:
             logger.warning(f"[AQM V3 {ticker}] Zapaść Funkcji Fal.: Brak danych EPS dla {reported_date_str}.")
             measurement_impact = 0.0
        else:
             measurement_impact = float(reported_eps) - float(estimated_eps)
             
        # Pomijamy Krok 3 (Analiza Transkryptu) - zbyt złożone na tym etapie

        return {
            "measurement_date": reported_date_str,
            "measurement_impact": measurement_impact
        }

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w get_wave_function_collapse_event: {e}", exc_info=True)
        return None


# === WYMIAR 6: CAUSALITY STRUCTURE (Struktura Przyczynowości) ===

def calculate_granger_causality_network(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient, lag: int = 5) -> Optional[Dict[str, float]]:
    """
    Oblicza Metrykę 6.1: granger_causality_network (Sieć Przyczynowości Grangera)
    Testuje, czy zmiany w 10Y Treasury Yield lub WTI "powodują" zmiany w cenie tickera.
    """
    try:
        # 1. Pobierz dane makro
        yield_data_raw = api_client.get_treasury_yield(interval='daily', maturity='10year')
        oil_data_raw = api_client.get_wti(interval='daily')
        
        if not yield_data_raw or 'data' not in yield_data_raw or not yield_data_raw['data']:
            logger.warning(f"[AQM V3 {ticker}] Granger: Brak danych 10Y TREASURY YIELD.")
            return None
        if not oil_data_raw or 'data' not in oil_data_raw or not oil_data_raw['data']:
            logger.warning(f"[AQM V3 {ticker}] Granger: Brak danych WTI.")
            return None

        # 2. Przetwórz dane makro na DataFrame
        yield_df = pd.DataFrame(yield_data_raw['data']).rename(columns={'value': 'yield'}).set_index('date').astype(float)
        oil_df = pd.DataFrame(oil_data_raw['data']).rename(columns={'value': 'oil'}).set_index('date').astype(float)
        
        yield_df.index = pd.to_datetime(yield_df.index)
        oil_df.index = pd.to_datetime(oil_df.index)
        
        # 3. Przygotuj DataFrame spółki (mamy go już z 'ticker_daily_df')
        ticker_df = ticker_daily_df[['close']].copy()
        
        # 4. Połącz i oblicz zwroty/różnice
        df = ticker_df.join(yield_df, how='inner').join(oil_df, how='inner')
        
        if len(df) < 100: # Potrzebujemy wystarczająco danych do testu
             logger.warning(f"[AQM V3 {ticker}] Granger: Za mało (<100) wspólnych danych dla ticker, yield i oil.")
             return None

        df['ticker_returns'] = df['close'].pct_change()
        df['yield_changes'] = df['yield'].diff()
        df['oil_returns'] = df['oil'].pct_change()
        
        df = df.dropna() # Usuń NaN powstałe po diff/pct_change

        if len(df) < (lag + 10): # Upewnij się, że mamy wystarczająco danych *po* usunięciu NaN
             logger.warning(f"[AQM V3 {ticker}] Granger: Za mało danych po usunięciu NaN.")
             return None

        # 5. Wykonaj testy Grangera
        
        # Test 1: Czy Rentowność "powoduje" Ticker?
        test_data_yield = df[['ticker_returns', 'yield_changes']]
        gct_yield = grangercausalitytests(test_data_yield, maxlag=[lag], verbose=False)
        # [lag] to słownik testów dla tego opóźnienia, [0] to wyniki testu, 'ssr_ftest' to test F, [1] to p-value
        p_value_yield_causes_ticker = gct_yield[lag][0]['ssr_ftest'][1]

        # Test 2: Czy Ropa "powoduje" Ticker?
        test_data_oil = df[['ticker_returns', 'oil_returns']]
        gct_oil = grangercausalitytests(test_data_oil, maxlag=[lag], verbose=False)
        p_value_oil_causes_ticker = gct_oil[lag][0]['ssr_ftest'][1]

        return {
            "p_value_yield_causes_ticker": float(p_value_yield_causes_ticker),
            "p_value_oil_causes_ticker": float(p_value_oil_causes_ticker)
        }

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_granger_causality_network: {e}", exc_info=True)
        return None


# === WYMIAR 7: CONSCIOUSNESS FIELD (Pole Świadomości) ===

def calculate_attention_density(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient) -> Optional[float]:
    """
    Oblicza Metrykę 7.1: attention_density (Gęstość Uwagi)
    Połączenie Z-Score wolumenu i (uproszczonego) Z-Score liczby newsów.
    """
    try:
        if len(ticker_daily_df) < 200:
            logger.warning(f"[AQM V3 {ticker}] Gęstość Uwagi: Za mało danych (<200 dni) do obliczenia Z-Score.")
            return None

        # 1. avg_volume_10d = AVG(DAILY_VOLUME, period=10)
        avg_volume_10d = ticker_daily_df['volume'].iloc[-10:].mean()
        
        # 3. normalized_volume = Z_SCORE(avg_volume_10d, historical_period=200)
        # Z_SCORE = (wartość - średnia) / odchylenie_standardowe
        vol_history_200 = ticker_daily_df['volume'].iloc[-200:]
        vol_mean_200 = vol_history_200.mean()
        vol_std_200 = vol_history_200.std()

        if vol_std_200 == 0:
            normalized_volume = 0.0 # Brak zmienności wolumenu
        else:
            normalized_volume = (avg_volume_10d - vol_mean_200) / vol_std_200
            
        # 2. news_count_10d = COUNT(artykułów z ostatnich 10 dni)
        ten_days_ago_str = (datetime.now() - timedelta(days=10)).strftime('%Y%m%dT%H%M')
        news_data = api_client.get_news_sentiment(ticker, limit=100, time_from=ten_days_ago_str)
        
        news_count_10d = 0
        if news_data and 'feed' in news_data and news_data['feed']:
            # Zliczamy tylko te, które faktycznie dotyczą naszego tickera
            for article in news_data['feed']:
                if any(t['ticker'] == ticker for t in article.get('topics', [])):
                    news_count_10d += 1

        # 4. normalized_news = Z_SCORE(news_count_10d, historical_period=200)
        # Używamy przybliżenia (stałych) zdefiniowanych na górze pliku
        if HISTORICAL_NEWS_STD == 0:
            normalized_news = 0.0
        else:
            normalized_news = (news_count_10d - HISTORICAL_NEWS_MEAN) / HISTORICAL_NEWS_STD

        # 5. attention_density = normalized_volume + normalized_news
        attention_density = normalized_volume + normalized_news
        
        return float(attention_density)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_attention_density: {e}", exc_info=True)
        return None
