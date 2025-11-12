# ==================================================================
# === PLIK KROKU 17: Refaktoryzacja Metryk AQM V3 ===
#
# Cel: Stworzenie "czystych" funkcji obliczeniowych (z przyrostkiem _from_data),
# które nie wywołują API, lecz przyjmują dane z cache backtestu.
# Stare funkcje (wywołujące API) są zachowane na potrzeby przyszłego 
# skanera na żywo.
# ==================================================================

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple, List
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

# Importy bibliotek statystycznych (Krok 14)
from scipy.stats import entropy as shannon_entropy
from scipy.stats import zscore
from statsmodels.tsa.stattools import grangercausalitytests

# Import klienta API (Krok 12) i utils
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import standardize_df_columns

logger = logging.getLogger(__name__)

# Stała Plancka Rynku (wg PDF Wymiar 5, Heisenberg)
RYNEK_HBAR = 1.0 
HISTORICAL_NEWS_MEAN = 20
HISTORICAL_NEWS_STD = 10


# ==================================================================
# === NOWE "CZYSTE" FUNKCJE DLA BACKTESTU (KROK 17) ===
# ==================================================================

# === WYMIAR 1 (Dla Hipotezy H1) ===

def calculate_time_dilation_from_data(ticker_daily_view: pd.DataFrame, spy_daily_view: pd.DataFrame, period: int = 20) -> Optional[float]:
    """
    [Czysta funkcja] Oblicza Metrykę 1.1: time_dilation (Dylatacja Czasu)
    na podstawie historycznych "widoków" DataFrame.
    """
    try:
        # Widok (view) ma już odpowiednią długość (np. 200 dni)
        if len(ticker_daily_view) < period or len(spy_daily_view) < period:
            return None # Za mało danych w tym oknie

        # 1. Oblicz zwroty procentowe dla całego widoku
        ticker_returns = ticker_daily_view['close'].pct_change()
        spy_returns = spy_daily_view['close'].pct_change()
        
        # 2. Oblicz 20-dniowe odchylenie standardowe zwrotów (bierzemy ostatnie 'period' zwrotów)
        stddev_ticker_20 = ticker_returns.iloc[-period:].std()
        stddev_spy_20 = spy_returns.iloc[-period:].std()

        if stddev_spy_20 == 0 or pd.isna(stddev_spy_20):
            return None
            
        # 3. time_dilation = stddev_ticker / stddev_spy
        time_dilation = stddev_ticker_20 / stddev_spy_20
        
        return float(time_dilation)

    except Exception as e:
        logger.error(f"[AQM V3] Błąd w calculate_time_dilation_from_data: {e}", exc_info=True)
        return None

def calculate_price_gravity_from_data(daily_df_view: pd.DataFrame, vwap_df_view: pd.DataFrame) -> Optional[float]:
    """
    [Czysta funkcja] Oblicza Metrykę 1.2: price_gravity (Grawitacja Cenowa)
    na podstawie historycznych "widoków" DataFrame.
    """
    try:
        # Pobierz ostatnią datę z widoku dziennego
        current_date = daily_df_view.index[-1]
        
        # 1. price = DAILY_CLOSE(ticker)
        price = daily_df_view.loc[current_date]['close']
        
        # 2. center_of_mass = VWAP(ticker, daily)
        # Użyj .get() dla bezpieczeństwa, jeśli data nie istnieje
        vwap_row = vwap_df_view.get(current_date)
        if vwap_row is None:
            # Spróbuj znaleźć ostatnią znaną wartość (forward fill)
            vwap_row = vwap_df_view[vwap_df_view.index <= current_date].iloc[-1]
            if vwap_row is None:
                 logger.warning(f"[AQM V3] Grawitacja Cenowa: Brak danych VWAP dla {current_date}.")
                 return None
        
        center_of_mass = float(vwap_row['VWAP'])

        if price == 0 or pd.isna(price):
            logger.warning(f"[AQM V3] Grawitacja Cenowa: Cena wynosi 0 dla {current_date}. Nie można obliczyć.")
            return None

        # 3. price_gravity = (center_of_mass - price) / price
        price_gravity = (center_of_mass - price) / price
        
        return float(price_gravity)

    except (KeyError, IndexError) as e:
        logger.warning(f"[AQM V3] Grawitacja Cenowa: Niezgodność danych dla {daily_df_view.index[-1]}. {e}")
        return None
    except Exception as e:
        logger.error(f"[AQM V3] Błąd w calculate_price_gravity_from_data: {e}", exc_info=True)
        return None


# ==================================================================
# === ISTNIEJĄCE FUNKCJE API (DLA SKANERA NA ŻYWO) ===
# Te funkcje zostają na razie bez zmian. Będziemy je refaktoryzować
# w miarę implementacji kolejnych Hipotez (H2, H3, H4).
# ==================================================================

# === WYMIAR 1: TIME-SPACE CONTINUUM (Struktura Czasoprzestrzeni) ===

def calculate_time_dilation(ticker_daily_df: pd.DataFrame, spy_daily_df: pd.DataFrame, period: int = 20) -> Optional[float]:
    """
    [Funkcja API-Live] Oblicza Metrykę 1.1: time_dilation (Dylatacja Czasu)
    (Ta funkcja już była "czysta" i nie wymagała API)
    """
    return calculate_time_dilation_from_data(ticker_daily_df, spy_daily_df, period)


def calculate_price_gravity(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient) -> Optional[float]:
    """
    [Funkcja API-Live] Oblicza Metrykę 1.2: price_gravity (Grawitacja Cenowa)
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
    [Funkcja API-Live] Oblicza Metrykę 2.1: institutional_sync (Synchronizacja Instytucjonalna)
    Stosunek netto transakcji kupna do sprzedaży przez insiderów (90 dni).
    """
    try:
        transactions_data = api_client.get_insider_transactions(ticker)
        if not transactions_data:
            logger.warning(f"[AQM V3 {ticker}] Brak danych INSIDER_TRANSACTIONS z API.")
            return 0.0 # Neutralny wynik, jeśli brak danych

        ninety_days_ago = datetime.now() - timedelta(days=90)
        total_buys = 0.0
        total_sells = 0.0

        for tx in transactions_data:
            try:
                tx_date = datetime.strptime(tx['transactionDate'], '%Y-%m-%d')
                if tx_date < ninety_days_ago:
                    continue 

                shares = float(tx['transactionShares'])
                tx_type = tx['transactionType']

                if tx_type == 'P-Purchase':
                    total_buys += shares
                elif tx_type == 'S-Sale':
                    total_sells += shares
            except Exception:
                continue 

        denominator = total_buys + total_sells
        if denominator == 0:
            logger.info(f"[AQM V3 {ticker}] Synchronizacja Inst.: Brak transakcji K/S w ostatnich 90 dniach.")
            return 0.0 # Neutralny wynik

        institutional_sync = (total_buys - total_sells) / denominator
        
        return float(institutional_sync)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_institutional_sync: {e}", exc_info=True)
        return None

def calculate_retail_herding(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """
    [Funkcja API-Live] Oblicza Metrykę 2.2: retail_herding (Zachowanie Stadne Detalu)
    Średni sentyment z wiadomości z ostatnich 7 dni.
    """
    try:
        seven_days_ago_str = (datetime.now() - timedelta(days=7)).strftime('%Y%m%dT%H%M')
        
        news_data = api_client.get_news_sentiment(ticker, limit=100, time_from=seven_days_ago_str)
        if not news_data or 'feed' not in news_data or not news_data['feed']:
            logger.warning(f"[AQM V3 {ticker}] Zachowanie Stadne: Brak newsów z ostatnich 7 dni.")
            return 0.0 

        scores = []
        for article in news_data['feed']:
            if any(t['ticker'] == ticker for t in article.get('topics', [])):
                score = article.get('overall_sentiment_score')
                if score is not None:
                    scores.append(float(score))

        if not scores:
            logger.warning(f"[AQM V3 {ticker}] Zachowanie Stadne: Znaleziono newsy, ale bez ocen sentymentu.")
            return 0.0 

        retail_herding = sum(scores) / len(scores)
        return float(retail_herding)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_retail_herding: {e}", exc_info=True)
        return None


# === WYMIAR 3: ENERGY POTENTIAL FIELD (Pole Potencjału Energii) ===

def calculate_breakout_energy_required(ticker: str, ticker_daily_df: pd.DataFrame, api_client: AlphaVantageClient) -> Optional[float]:
    """
    [Funkcja API-Live] Oblicza Metrykę 3.1: breakout_energy_required (Wymagana Energia Wybicia)
    Odwrotność znormalizowanej szerokości Wstęg Bollingera.
    """
    try:
        bbands_data = api_client.get_bollinger_bands(ticker, interval='daily', time_period=20, nbdevup=2, nbdevdn=2)
        if not bbands_data or 'Technical Analysis: BBANDS' not in bbands_data:
            logger.warning(f"[AQM V3 {ticker}] Brak danych BBANDS z API.")
            return None

        latest_bbands_key = sorted(bbands_data['Technical Analysis: BBANDS'].keys(), reverse=True)[0]
        bbands_values = bbands_data['Technical Analysis: BBANDS'][latest_bbands_key]
        
        upper_band = float(bbands_values['Real Upper Band'])
        lower_band = float(bbands_values['Real Lower Band'])

        if latest_bbands_key not in ticker_daily_df.index:
             price = ticker_daily_df['close'].iloc[-1]
        else:
             price = ticker_daily_df.loc[latest_bbands_key]['close']

        if price == 0:
            logger.warning(f"[AQM V3 {ticker}] Energia Wybicia: Cena wynosi 0. Nie można obliczyć.")
            return None

        band_width_normalized = (upper_band - lower_band) / price

        if band_width_normalized == 0:
            logger.warning(f"[AQM V3 {ticker}] Energia Wybicia: Szerokość wstęg wynosi 0.")
            return None 

        breakout_energy_required = 1.0 / band_width_normalized
        
        return float(breakout_energy_required)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_breakout_energy_required: {e}", exc_info=True)
        return None


# === WYMIAR 4: INFORMATION THERMODYNAMICS (Termodynamika Informacji) ===

def calculate_market_temperature(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """
    [Funkcja API-Live] Oblicza Metrykę 4.1: market_temperature (Temperatura Rynku)
    Zmienność (STDEV) zwrotów wewnątrz-dziennych (5min) z ostatnich 30 dni.
    """
    try:
        intraday_data = api_client.get_intraday(ticker, interval='5min', outputsize='full')
        if not intraday_data or 'Time Series (5min)' not in intraday_data:
            logger.warning(f"[AQM V3 {ticker}] Temperatura Rynku: Brak danych 5min Intraday z API.")
            return None
            
        df = pd.DataFrame.from_dict(intraday_data['Time Series (5min)'], orient='index')
        df = standardize_df_columns(df)
        df.index = pd.to_datetime(df.index) 

        thirty_days_ago = datetime.now() - timedelta(days=30)
        df_30_days = df[df.index >= thirty_days_ago]
        
        if len(df_30_days) < 100: 
            logger.warning(f"[AQM V3 {ticker}] Temperatura Rynku: Za mało danych 5min z ostatnich 30 dni ({len(df_30_days)} świec).")
            return None

        returns_5min = df_30_days['close'].pct_change().dropna()
        market_temperature = returns_5min.std()
        
        return float(market_temperature)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_market_temperature: {e}", exc_info=True)
        return None

def calculate_information_entropy(ticker: str, api_client: AlphaVantageClient) -> Optional[float]:
    """
    [Funkcja API-Live] Oblicza Metrykę 4.2: information_entropy (Entropia Informacyjna)
    Entropia Shannona na podstawie 100 ostatnich tematów wiadomości.
    """
    try:
        news_data = api_client.get_news_sentiment(ticker, limit=100)
        if not news_data or 'feed' not in news_data or not news_data['feed']:
            logger.warning(f"[AQM V3 {ticker}] Entropia Inform.: Brak newsów (limit 100).")
            return None 

        topic_list = []
        for article in news_data['feed']:
            for topic in article.get('topics', []):
                if topic.get('ticker') == ticker:
                    topic_list.append(topic.get('topic'))
        
        if not topic_list:
            logger.warning(f"[AQM V3 {ticker}] Entropia Inform.: Znaleziono newsy, ale brak tagów 'topic' dla {ticker}.")
            return None

        topic_series = pd.Series(topic_list)
        probabilities = topic_series.value_counts(normalize=True)
        
        entropy = shannon_entropy(probabilities, base=2)
        
        return float(entropy)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_information_entropy: {e}", exc_info=True)
        return None


# === WYMIAR 5: MULTIVERSE PROBABILITY (Prawdopodobieństwo Wieloświata) ===

def get_wave_function_collapse_event(ticker: str, api_client: AlphaVantageClient) -> Optional[Dict[str, Any]]:
    """
    [Funkcja API-Live] Pobiera Metrykę 5.1: wave_function_collapse (Zapaść Funkcji Falowej)
    Zwraca dane o ostatnim "pomiarze" (wynikach kwartalnych).
    """
    try:
        earnings_data = api_client.get_earnings(ticker)
        if not earnings_data or 'quarterlyEarnings' not in earnings_data or not earnings_data['quarterlyEarnings']:
            logger.warning(f"[AQM V3 {ticker}] Zapaść Funkcji Fal.: Brak danych EARNINGS.")
            return None
            
        latest_earning = earnings_data['quarterlyEarnings'][0]
        reported_date_str = latest_earning.get('reportedDate')
        reported_eps = latest_earning.get('reportedEPS')
        estimated_eps = latest_earning.get('estimatedEPS')
        
        if reported_eps == 'None' or estimated_eps == 'None' or reported_eps is None or estimated_eps is None:
             logger.warning(f"[AQM V3 {ticker}] Zapaść Funkcji Fal.: Brak danych EPS dla {reported_date_str}.")
             measurement_impact = 0.0
        else:
             measurement_impact = float(reported_eps) - float(estimated_eps)
             
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
    [Funkcja API-Live] Oblicza Metrykę 6.1: granger_causality_network (Sieć Przyczynowości Grangera)
    Testuje, czy zmiany w 10Y Treasury Yield lub WTI "powodują" zmiany w cenie tickera.
    """
    try:
        yield_data_raw = api_client.get_treasury_yield(interval='daily', maturity='10year')
        oil_data_raw = api_client.get_wti(interval='daily')
        
        if not yield_data_raw or 'data' not in yield_data_raw or not yield_data_raw['data']:
            logger.warning(f"[AQM V3 {ticker}] Granger: Brak danych 10Y TREASURY YIELD.")
            return None
        if not oil_data_raw or 'data' not in oil_data_raw or not oil_data_raw['data']:
            logger.warning(f"[AQM V3 {ticker}] Granger: Brak danych WTI.")
            return None

        yield_df = pd.DataFrame(yield_data_raw['data']).rename(columns={'value': 'yield'}).set_index('date').astype(float)
        oil_df = pd.DataFrame(oil_data_raw['data']).rename(columns={'value': 'oil'}).set_index('date').astype(float)
        
        yield_df.index = pd.to_datetime(yield_df.index)
        oil_df.index = pd.to_datetime(oil_df.index)
        
        ticker_df = ticker_daily_df[['close']].copy()
        
        df = ticker_df.join(yield_df, how='inner').join(oil_df, how='inner')
        
        if len(df) < 100: 
             logger.warning(f"[AQM V3 {ticker}] Granger: Za mało (<100) wspólnych danych dla ticker, yield i oil.")
             return None

        df['ticker_returns'] = df['close'].pct_change()
        df['yield_changes'] = df['yield'].diff()
        df['oil_returns'] = df['oil'].pct_change()
        
        df = df.dropna() 

        if len(df) < (lag + 10): 
             logger.warning(f"[AQM V3 {ticker}] Granger: Za mało danych po usunięciu NaN.")
             return None
        
        test_data_yield = df[['ticker_returns', 'yield_changes']]
        gct_yield = grangercausalitytests(test_data_yield, maxlag=[lag], verbose=False)
        p_value_yield_causes_ticker = gct_yield[lag][0]['ssr_ftest'][1]

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
    [Funkcja API-Live] Oblicza Metrykę 7.1: attention_density (Gęstość Uwagi)
    Połączenie Z-Score wolumenu i (uproszczonego) Z-Score liczby newsów.
    """
    try:
        if len(ticker_daily_df) < 200:
            logger.warning(f"[AQM V3 {ticker}] Gęstość Uwagi: Za mało danych (<200 dni) do obliczenia Z-Score.")
            return None

        avg_volume_10d = ticker_daily_df['volume'].iloc[-10:].mean()
        
        vol_history_200 = ticker_daily_df['volume'].iloc[-200:]
        vol_mean_200 = vol_history_200.mean()
        vol_std_200 = vol_history_200.std()

        if vol_std_200 == 0:
            normalized_volume = 0.0
        else:
            normalized_volume = (avg_volume_10d - vol_mean_200) / vol_std_200
            
        ten_days_ago_str = (datetime.now() - timedelta(days=10)).strftime('%Y%m%dT%H%M')
        news_data = api_client.get_news_sentiment(ticker, limit=100, time_from=ten_days_ago_str)
        
        news_count_10d = 0
        if news_data and 'feed' in news_data and news_data['feed']:
            for article in news_data['feed']:
                if any(t['ticker'] == ticker for t in article.get('topics', [])):
                    news_count_10d += 1

        if HISTORICAL_NEWS_STD == 0:
            normalized_news = 0.0
        else:
            normalized_news = (news_count_10d - HISTORICAL_NEWS_MEAN) / HISTORICAL_NEWS_STD

        attention_density = normalized_volume + normalized_news
        
        return float(attention_density)

    except Exception as e:
        logger.error(f"[AQM V3 {ticker}] Błąd w calculate_attention_density: {e}", exc_info=True)
        return None
