import time
import requests
import logging
import json
from collections import deque
import os
from dotenv import load_dotenv
from io import StringIO
import csv

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

    def _rate_limiter(self):
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
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
                if not data or "Error Message" in data or "Information" in data:
                    logger.warning(f"API returned an error or empty data for {request_identifier}: {data}")
                    if "premium" in str(data).lower():
                         logger.error(f"API call for {request_identifier} failed due to premium limit. Waiting longer.")
                         time.sleep(20)
                    return None
                return data
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
                logger.error(f"Request failed for {request_identifier} (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    def get_market_status(self):
        """Pobiera aktualny status rynku z dedykowanego endpointu."""
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def _parse_bulk_quotes_csv(self, csv_text: str) -> dict:
        """Przetwarza odpowiedź CSV z BULK_QUOTES na słownik danych."""
        if not csv_text or "symbol" not in csv_text:
            logger.warning("[DIAGNOSTYKA] Otrzymane dane CSV są puste lub nie zawierają nagłówka 'symbol'.")
            return {}
        
        csv_file = StringIO(csv_text)
        reader = csv.DictReader(csv_file)
        
        data_dict = {}
        for row in reader:
            ticker = row.get('symbol')
            if not ticker:
                continue
            
            data_dict[ticker] = {
                'price': row.get('price'),
                'close': row.get('close'), # To pole jest mylące, będziemy używać 'previous close'
                'volume': row.get('volume'),
                'change_percent': row.get('change_percent'),
                'change': row.get('change'),
                'previous close': row.get('previous close'), # Prawidłowe pole dla zamknięcia z poprzedniego dnia
                'extended_hours_price': row.get('extended_hours_price'),
                'extended_hours_change': row.get('extended_hours_change'),
                'extended_hours_change_percent': row.get('extended_hours_change_percent')
            }
        return data_dict

    def get_bulk_quotes(self, symbols: list[str]):
        self._rate_limiter()
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
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
                return self._parse_bulk_quotes_csv(text_response)
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

    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact', extended_hours: bool = True):
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "outputsize": outputsize,
            "extended_hours": "true" if extended_hours else "false"
        }
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

    @staticmethod
    def _safe_float(value) -> float | None:
        if value is None: return None
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def get_live_quote_details(self, symbol: str) -> dict:
        """
        Pobiera pełne dane "live" (REALTIME_BULK_QUOTES) oraz status rynku,
        zwracając ustandaryzowany słownik w stylu Yahoo Finance.
        """
        us_market_status = "closed"
        try:
            status_data = self.get_market_status()
            if status_data and status_data.get('markets'):
                us_market = next((m for m in status_data['markets'] if m.get('region') == 'United States'), None)
                if us_market:
                    us_market_status = us_market.get('current_status', 'closed').lower()
        except Exception as e:
            logger.warning(f"Nie można pobrać statusu rynku dla {symbol}: {e}. Przyjęto 'closed'.")

        raw_data = self.get_bulk_quotes([symbol])
        
        if not raw_data or symbol not in raw_data:
            logger.error(f"Brak danych live (REALTIME_BULK_QUOTES) dla {symbol}")
            return {
                "symbol": symbol, "market_status": us_market_status,
                "regular_session": {}, "extended_session": {}, "live_price": None
            }
            
        ticker_data = raw_data[symbol]

        # ==================================================================
        #  KOREKTA BŁĘDU
        #  Poprzednio używałem 'ticker_data.get('close')', który jest nieprawidłowy lub pusty.
        #  Poprawne pole dla ceny zamknięcia poprzedniego dnia to 'previous close'.
        # ==================================================================
        regular_close_price = self._safe_float(ticker_data.get('previous close'))
        
        response = {
            "symbol": symbol,
            "market_status": us_market_status,
            "regular_session": {
                "price": regular_close_price,
                "change": self._safe_float(ticker_data.get('change')),
                "change_percent": self._safe_float(ticker_data.get('change_percent'))
            },
            "extended_session": {
                "price": self._safe_float(ticker_data.get('extended_hours_price')),
                "change": self._safe_float(ticker_data.get('extended_hours_change')),
                "change_percent": self._safe_float(ticker_data.get('extended_hours_change_percent'))
            },
            "live_price": self._safe_float(ticker_data.get('price')) # To jest 'latest trade'
        }
        
        if us_market_status in ["pre-market", "post-market"] and response["extended_session"]["price"] is not None:
             response["live_price"] = response["extended_session"]["price"]
        elif us_market_status == "regular":
             response["live_price"] = self._safe_float(ticker_data.get('price'))
        elif us_market_status == "closed":
             # KOREKTA BŁĘDU: Poprzednio response["regular_session"]["price"] było None
             response["live_price"] = response["extended_session"]["price"] or response["regular_session"]["price"]
        
        return response

