import time
import requests
import logging
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
        self.requests_per_minute = requests_per_minute # POPRAWKA: Dodanie tej linii
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()

    def _rate_limiter(self):
        """Zapewnia nieprzekraczanie limitu zapytań na minutę."""
        while self.request_timestamps and (time.monotonic() - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()

        if len(self.request_timestamps) >= (self.requests_per_minute - 1): # POPRAWKA: Użycie poprawnej zmiennej
            sleep_time = self.request_interval - (time.monotonic() - self.request_timestamps[-1])
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        self.request_timestamps.append(time.monotonic())


    def _make_request(self, params: dict):
        """Wykonywanie zapytań z logiką ponowień i obsługą błędów."""
        self._rate_limiter()
        params['apikey'] = self.api_key
        
        for attempt in range(self.retries):
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data or "Note" in data or "Error Message" in data:
                    logger.warning(f"API Info/Error for {params.get('symbol') or params.get('symbols')}: {data}")
                    if "Note" in data:
                        return None
                    raise requests.exceptions.HTTPError(f"API returned an info/error message: {data}")
                return data

            except (requests.exceptions.RequestException, ValueError, requests.exceptions.HTTPError) as e:
                logger.error(f"Request failed (attempt {attempt + 1}/{self.retries}) for {params.get('symbol') or params.get('symbols')}: {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
                else:
                    logger.critical(f"All retries failed for {params.get('symbol') or params.get('symbols')}.")
                    return None
        return None

    def get_batch_quotes(self, symbols: list[str]):
        """Pobiera notowania dla wielu symboli w jednym zapytaniu."""
        params = {"function": "BATCH_STOCK_QUOTES", "symbols": ",".join(symbols)}
        return self._make_request(params)

    def get_company_overview(self, symbol: str):
        """Pobiera dane fundamentalne dla spółki."""
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
        """Pobiera historyczne dane dzienne (skorygowane). Domyślnie 'full' dla analiz."""
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)

    def get_news_sentiment(self, symbol: str):
        """Pobiera sentyment newsów dla spółki."""
        params = {"function": "NEWS_SENTIMENT", "tickers": symbol, "limit": 50}
        return self._make_request(params)

    def get_bollinger_bands(self, symbol: str, time_period: int = 20, series_type: str = 'close'):
        """Pobiera dane dla wskaźnika Bollinger Bands."""
        params = {
            "function": "BBANDS", "symbol": symbol, "interval": "daily",
            "time_period": str(time_period), "series_type": series_type,
            "nbdevup": "2", "nbdevdn": "2"
        }
        return self._make_request(params)

    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact'):
        """Pobiera dane intraday."""
        params = {
            "function": "TIME_SERIES_INTRADAY", "symbol": symbol, "interval": interval,
            "outputsize": outputsize, "extended_hours": "false"
        }
        return self._make_request(params)

    def get_rsi(self, symbol: str, time_period: int = 14, series_type: str = 'close'):
        """Pobiera dane dla wskaźnika RSI."""
        params = {
            "function": "RSI", "symbol": symbol, "interval": "daily",
            "time_period": str(time_period), "series_type": series_type
        }
        return self._make_request(params)
        
    def get_sma(self, symbol: str, time_period: int = 50, series_type: str = 'close'):
        """Pobiera dane dla wskaźnika SMA."""
        params = {
            "function": "SMA", "symbol": symbol, "interval": "daily",
            "time_period": str(time_period), "series_type": series_type
        }
        return self._make_request(params)

    def get_stoch(self, symbol: str):
        """Pobiera dane dla wskaźnika Stochastic Oscillator."""
        params = {"function": "STOCH", "symbol": symbol, "interval": "daily"}
        return self._make_request(params)

    def get_adx(self, symbol: str, time_period: int = 14):
        """Pobiera dane dla wskaźnika ADX."""
        params = {"function": "ADX", "symbol": symbol, "interval": "daily", "time_period": str(time_period)}
        return self._make_request(params)

    def get_macd(self, symbol: str, series_type: str = 'close'):
        """Pobiera dane dla wskaźnika MACD."""
        params = {"function": "MACD", "symbol": symbol, "interval": "daily", "series_type": series_type}
        return self._make_request(params)

