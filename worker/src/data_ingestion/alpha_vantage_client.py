import time
import requests
import logging
import json
from collections import deque

logger = logging.getLogger(__name__)

class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str, requests_per_minute: int = 150, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()

    def _rate_limiter(self):
        """Ulepszony, bardziej rygorystyczny ogranicznik zapytań."""
        # Usuń znaczniki czasu starsze niż minuta
        while self.request_timestamps and (time.monotonic() - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()
        
        # Jeśli kolejka jest pełna, poczekaj, aż zwolni się miejsce
        if len(self.request_timestamps) >= self.requests_per_minute:
            time_to_wait = 60 - (time.monotonic() - self.request_timestamps[0])
            if time_to_wait > 0:
                logger.warning(f"Rate limit reached. Sleeping for {time_to_wait:.2f} seconds.")
                time.sleep(time_to_wait)
        
        # Zawsze zachowaj minimalny odstęp między zapytaniami, aby uniknąć burstów
        if self.request_timestamps:
            time_since_last = time.monotonic() - self.request_timestamps[-1]
            if time_since_last < self.request_interval:
                time.sleep(self.request_interval - time_since_last)
                
        self.request_timestamps.append(time.monotonic())


    def _make_request(self, params: dict):
        self._rate_limiter()
        params['apikey'] = self.api_key
        
        for attempt in range(self.retries):
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                if not data or "Error Message" in data or "Information" in data:
                    logger.warning(f"API returned an error or empty data for {params.get('symbol')}: {data}")
                    if "premium" in str(data): # Jeśli błąd wspomina o premium, to znaczy, że limit jest problemem
                         logger.error("API call failed due to premium limit. Waiting longer.")
                         time.sleep(20) # Dłuższa przerwa w przypadku błędu limitu
                    return None
                if "Note" in data:
                    logger.warning(f"API Note for {params.get('symbol')}: {data['Note']}.")
                return data
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    def get_bulk_quotes(self, symbols: list[str]):
        self._rate_limiter()
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbols": ",".join(symbols), # Poprawiona nazwa parametru z 'symbol' na 'symbols'
            "datatype": "csv",
            "apikey": self.api_key
        }
        for attempt in range(self.retries):
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                text_response = response.text
                if "Error Message" in text_response or "Invalid API call" in text_response:
                    logger.error(f"Bulk quotes API returned an error: {text_response[:200]}")
                    return None
                return text_response
            except requests.exceptions.RequestException as e:
                logger.error(f"Bulk quotes request failed (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None
    
    def get_company_overview(self, symbol: str):
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)
        
    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact'):
        params = {"function": "TIME_SERIES_INTRADAY", "symbol": symbol, "interval": interval, "outputsize": outputsize}
        return self._make_request(params)

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
        params = {"function": "BBANDS", "symbol": symbol, "interval": interval, "time_period": str(time_period), "series_type": series_type}
        return self._make_request(params)
        
    def get_news_sentiment(self, ticker: str, limit: int = 50):
        params = {"function": "NEWS_SENTIMENT", "tickers": ticker, "limit": str(limit)}
        return self._make_request(params)

