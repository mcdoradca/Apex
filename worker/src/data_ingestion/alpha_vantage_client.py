import time
import requests
import logging
import json
from collections import deque

logger = logging.getLogger(__name__)

class AlphaVantageClient:
    """
    Dedykowany klient do komunikacji z API Alpha Vantage.
    Zawiera logikę rate limiting, ponowień oraz obsługę zapytań blokowych.
    """
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str, requests_per_minute: int = 75, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        
        self.requests_per_minute = requests_per_minute
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()

    def _rate_limiter(self):
        """Zapewnia nieprzekraczanie limitu zapytań na minutę."""
        while self.request_timestamps and (time.monotonic() - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()

        if len(self.request_timestamps) >= (self.requests_per_minute - 1):
            sleep_time = self.request_interval - (time.monotonic() - self.request_timestamps[-1])
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.request_timestamps.append(time.monotonic())

    def _make_raw_request(self, params: dict):
        """Wykonuje surowe zapytanie, które nie oczekuje odpowiedzi JSON."""
        self._rate_limiter()
        params['apikey'] = self.api_key
        
        for attempt in range(self.retries):
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=60)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.error(f"Raw request failed (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    def _make_request(self, params: dict):
        """
        Wykonywanie zapytań z logiką ponowień i bardziej odporną obsługą błędów.
        --- OSTATECZNA POPRAWKA ---
        Automatycznie dodaje `entitlement=delayed` do wszystkich zapytań, które tego wymagają,
        zgodnie z wytycznymi supportu Alpha Vantage dla planu premium.
        """
        # Lista funkcji, które dotyczą akcji z USA i wymagają tagu 'entitlement'
        us_equity_functions = [
            "OVERVIEW", "TIME_SERIES_DAILY_ADJUSTED", "NEWS_SENTIMENT", "BBANDS",
            "TIME_SERIES_INTRADAY", "RSI", "SMA", "ADX", "MACD", "STOCH", "GLOBAL_QUOTE"
        ]

        if params.get("function") in us_equity_functions:
            params['entitlement'] = 'delayed'
        # --- KONIEC POPRAWKI ---

        response = self._make_raw_request(params)
        if not response:
            return None
        
        try:
            data = response.json()
            if not data:
                logger.warning(f"API returned empty data for {params.get('symbol') or params.get('symbols')}.")
                return None
            
            if "Note" in data:
                logger.warning(f"API Note for {params.get('symbol')}: {data['Note']}.")
                # Nie zwracamy None, bo dane mogą być mimo wszystko obecne
            
            if "Error Message" in data:
                logger.error(f"API Error for {params.get('symbol')}: {data['Error Message']}")
                return None
            
            # Dodatkowe sprawdzenie pod kątem wiadomości o wymaganym planie premium
            if "To access the actual data, please subscribe" in str(data):
                logger.error(f"API returned 'sample data' for {params.get('symbol')}. Check entitlement.")
                return None

            return data
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from response for {params.get('symbol') or params.get('symbols')}")
            return None

    def get_company_overview(self, symbol: str):
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)

    def get_news_sentiment(self, symbol: str):
        params = {"function": "NEWS_SENTIMENT", "tickers": symbol, "limit": 50}
        return self._make_request(params)

    def get_bollinger_bands(self, symbol: str, time_period: int = 20, series_type: str = 'close'):
        params = {
            "function": "BBANDS", "symbol": symbol, "interval": "daily",
            "time_period": str(time_period), "series_type": series_type,
            "nbdevup": "2", "nbdevdn": "2"
        }
        return self._make_request(params)

    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact'):
        params = {
            "function": "TIME_SERIES_INTRADAY", "symbol": symbol, "interval": interval,
            "outputsize": outputsize, "extended_hours": "false"
        }
        return self._make_request(params)

    def get_rsi(self, symbol: str, time_period: int = 14, series_type: str = 'close'):
        params = {
            "function": "RSI", "symbol": symbol, "interval": "daily",
            "time_period": str(time_period), "series_type": series_type
        }
        return self._make_request(params)
        
    def get_sma(self, symbol: str, time_period: int = 50, series_type: str = 'close'):
        params = {
            "function": "SMA", "symbol": symbol, "interval": "daily",
            "time_period": str(time_period), "series_type": series_type
        }
        return self._make_request(params)

    def get_adx(self, symbol: str, time_period: int = 14):
        params = {
            "function": "ADX", "symbol": symbol, "interval": "daily",
            "time_period": str(time_period)
        }
        return self._make_request(params)

    def get_macd(self, symbol: str, series_type: str = 'close'):
        params = {
            "function": "MACD", "symbol": symbol, "interval": "daily", 
            "series_type": series_type
        }
        return self._make_request(params)

    def get_stoch(self, symbol: str):
        params = {
            "function": "STOCH", "symbol": symbol, "interval": "daily"
        }
        return self._make_request(params)
