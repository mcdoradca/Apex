import time
import requests
import logging
import json
from collections import deque
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    logger.warning("ALPHAVANTAGE_API_KEY not found in environment for API's client.")

class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str = API_KEY, requests_per_minute: int = 150, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            logger.warning("API key is missing for AlphaVantageClient instance in API.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()
        
        # Session Keep-Alive
        self.session = requests.Session()

    def _rate_limiter(self):
        """Ulepszony, bardziej rygorystyczny ogranicznik zapytań."""
        if not self.api_key:
             return

        while self.request_timestamps and (time.monotonic() - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()

        if len(self.request_timestamps) >= self.requests_per_minute:
            time_to_wait = 60 - (time.monotonic() - self.request_timestamps[0])
            if time_to_wait > 0:
                logger.warning(f"Rate limit reached. Sleeping for {time_to_wait:.2f} seconds.")
                time.sleep(time_to_wait)

        if self.request_timestamps:
            time_since_last = time.monotonic() - self.request_timestamps[-1]
            if time_since_last < self.request_interval:
                time.sleep(self.request_interval - time_since_last)

        self.request_timestamps.append(time.monotonic())


    def _make_request(self, params: dict):
        if not self.api_key:
            logger.error("Cannot make Alpha Vantage request: API key is missing.")
            return None

        self._rate_limiter()
        params['apikey'] = self.api_key

        for attempt in range(self.retries):
            try:
                response = self.session.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    return None
                    
                # Obsługa błędów API
                if "Error Message" in data:
                    logger.warning(f"API Error for {params.get('symbol')}: {data['Error Message']}")
                    return None
                if "Information" in data:
                    logger.warning(f"API Info/Limit for {params.get('symbol')}: {data['Information']}")
                    # Jeśli limit, czekamy dłużej
                    time.sleep(2)
                    continue
                    
                return data

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    # === METODY DANYCH RYNKOWYCH (ZGODNE Z TWOIMI PLIKAMI TXT) ===

    def get_market_status(self):
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def get_company_overview(self, symbol: str):
        """Pobiera dane fundamentalne o firmie (Sektor, Branża, Opis)."""
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_global_quote(self, symbol: str):
        """
        Pobiera najnowsze dane cenowe używając sprawdzonego endpointu GLOBAL_QUOTE.
        Naprawia błąd braku ceny w modalu.
        """
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol}
        data = self._make_request(params)
        return data.get('Global Quote') if data else None

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)
        
    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact'):
        params = {
            "function": "TIME_SERIES_INTRADAY", 
            "symbol": symbol, 
            "interval": interval, 
            "outputsize": outputsize
        }
        return self._make_request(params)

    # === WSKAŹNIKI TECHNICZNE ===

    def get_atr(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
        params = {"function": "ATR", "symbol": symbol, "interval": interval, "time_period": str(time_period)}
        return self._make_request(params)

    def get_rsi(self, symbol: str, time_period: int = 9, interval: str = 'daily', series_type: str = 'close'):
        params = {"function": "RSI", "symbol": symbol, "interval": interval, "time_period": str(time_period), "series_type": series_type}
        return self._make_request(params)
        
    def get_stoch(self, symbol: str, interval: str = 'daily'):
        params = {"function": "STOCH", "symbol": symbol, "interval": interval}
        return self._make_request(params)

    def get_adx(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
        params = {"function": "ADX", "symbol": symbol, "interval": interval, "time_period": str(time_period)}
        return self._make_request(params)

    def get_macd(self, symbol: str, interval: str = 'daily', series_type: str = 'close'):
        params = {"function": "MACD", "symbol": symbol, "interval": interval, "series_type": series_type}
        return self._make_request(params)
        
    def get_bollinger_bands(self, symbol: str, time_period: int = 20, interval: str = 'daily', series_type: str = 'close'):
        params = {
            "function": "BBANDS", 
            "symbol": symbol, 
            "interval": interval, 
            "time_period": str(time_period), 
            "series_type": series_type
        }
        return self._make_request(params)
    
    def get_news_sentiment(self, ticker: str, limit: int = 50):
        params = {"function": "NEWS_SENTIMENT", "tickers": ticker, "limit": str(limit)}
        return self._make_request(params)

    def get_bulk_quotes(self, symbols: list[str]):
        """
        Zachowana funkcja pomocnicza dla list (np. portfel), 
        ale dla pojedynczego sygnału używamy get_global_quote.
        """
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
            "datatype": "csv"
        }
        # Tutaj musimy użyć raw requests lub obsłużyć CSV w _make_request, 
        # ale dla bezpieczeństwa używamy dedykowanej obsługi CSV tutaj.
        self._rate_limiter()
        params['apikey'] = self.api_key
        try:
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Bulk quotes failed: {e}")
            return None

    # === ENDPOINTY MAKRO (FAZA 0) ===
    
    def get_inflation_rate(self, interval: str = 'monthly'):
        params = {"function": "INFLATION", "interval": interval, "datatype": "json"}
        return self._make_request(params)

    def get_fed_funds_rate(self, interval: str = 'monthly'):
        params = {"function": "FEDERAL_FUNDS_RATE", "interval": interval, "datatype": "json"}
        return self._make_request(params)

    def get_treasury_yield(self, interval: str = 'monthly', maturity: str = '10year'):
        params = {"function": "TREASURY_YIELD", "interval": interval, "maturity": maturity, "datatype": "json"}
        return self._make_request(params)

    def get_unemployment(self):
        params = {"function": "UNEMPLOYMENT", "datatype": "json"}
        return self._make_request(params)
