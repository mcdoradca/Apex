import time
import requests
import logging
import json
import csv
from io import StringIO
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

    # === OPTYMALIZACJA: Limit 150 zapytań/minuta (Premium) ===
    def __init__(self, api_key: str = API_KEY, requests_per_minute: int = 150, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            logger.warning("API key is missing for AlphaVantageClient instance in API.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        
        # Pacing: Równomierne rozłożenie zapytań
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()
        
        # === OPTYMALIZACJA: Session Keep-Alive ===
        self.session = requests.Session()

    def _rate_limiter(self):
        """
        Zaawansowany Rate Limiter typu 'Rolling Window'.
        """
        if not self.api_key:
             return

        now = time.monotonic()
        
        # 1. Usuń wpisy starsze niż 60 sekund (przesuń okno)
        while self.request_timestamps and (now - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()

        # 2. Sprawdź "Twardy Limit" ilościowy w bieżącym oknie
        if len(self.request_timestamps) >= self.requests_per_minute:
            time_to_wait = 60 - (now - self.request_timestamps[0]) + 0.05
            if time_to_wait > 0:
                # logger.warning(f"API Rate limit reached. Sleeping for {time_to_wait:.2f} seconds.")
                time.sleep(time_to_wait)
                now = time.monotonic()

        # 3. Sprawdź "Pacing"
        if self.request_timestamps:
            time_since_last = now - self.request_timestamps[-1]
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
                # Użycie self.session
                response = self.session.get(self.BASE_URL, params=params, timeout=30)
                
                # Obsługa specyficznych typów odpowiedzi (np. CSV vs JSON)
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    response.raise_for_status() 
                    if params.get('datatype') == 'csv':
                        return response.text
                    raise requests.exceptions.RequestException("Response was not valid JSON.")
                    
                # Wykrywanie limitów w JSON
                is_rate_limit_json = False
                if "Information" in data:
                    info_text = data["Information"].lower()
                    if "frequency" in info_text or "api call volume" in info_text or "please contact premium" in info_text:
                        is_rate_limit_json = True
                
                is_error_msg = "Error Message" in data

                if is_rate_limit_json:
                    wait_time = 2 * (attempt + 1)
                    logger.warning(f"API Rate Limit Hint. Sleeping {wait_time}s...")
                    time.sleep(wait_time)
                    continue 

                if not data or is_error_msg:
                    return None
                
                response.raise_for_status()
                return data

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                if attempt < self.retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                else:
                    pass # Max retries
        return None

    # === NARZĘDZIA POMOCNICZE (PARSOWANIE CSV) ===

    @staticmethod
    def _safe_float(value) -> float | None:
        if value is None: return None
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_bulk_quotes_csv(self, csv_text: str, ticker: str) -> dict | None:
        if not csv_text or "symbol" not in csv_text:
            return None
        
        try:
            csv_file = StringIO(csv_text)
            reader = csv.DictReader(csv_file)
            
            for row in reader:
                if row.get('symbol') == ticker:
                    return row
            return None
        except Exception:
            return None

    # === METODY DANYCH RYNKOWYCH ===

    def get_market_status(self):
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def get_company_overview(self, symbol: str):
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_global_quote(self, symbol: str):
        """
        Pobiera najnowsze dane cenowe (Premium: REALTIME_BULK_QUOTES).
        """
        # Optymalizacja: Używamy Bulk Quotes jako głównego źródła
        bulk_csv = self.get_bulk_quotes([symbol])
        if not bulk_csv: return None
        quote_data = self._parse_bulk_quotes_csv(bulk_csv, symbol)
        if not quote_data: return None

        try:
            formatted_quote = {
                "01. symbol": quote_data.get("symbol"),
                "02. open": quote_data.get("open"),
                "03. high": quote_data.get("high"),
                "04. low": quote_data.get("low"),
                "05. price": quote_data.get("close"), 
                "06. volume": quote_data.get("volume"),
                "07. latest trading day": None, 
                "08. previous close": quote_data.get("previous_close"),
                "09. change": quote_data.get("change"),
                "10. change percent": f'{quote_data.get("change_percent")}%',
                "_price_source": "close" # Domyślne
            }
            
            # Obsługa Extended Hours
            ext_price_str = quote_data.get("extended_hours_quote")
            ext_price = self._safe_float(ext_price_str)

            if ext_price and ext_price > 0:
                formatted_quote["05. price"] = ext_price_str
                formatted_quote["09. change"] = quote_data.get("extended_hours_change")
                formatted_quote["10. change percent"] = f'{quote_data.get("extended_hours_change_percent")}%'
                formatted_quote["_price_source"] = "extended_hours"

            return formatted_quote
            
        except Exception as e:
            logger.error(f"Błąd mapowania danych Bulk w API dla {symbol}: {e}")
            return None

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
        Pobiera surowy tekst CSV dla endpointu REALTIME_BULK_QUOTES.
        """
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
            "datatype": "csv"
        }
        # Tutaj nie musimy ustawiać API Key ręcznie, zrobi to _make_request
        text_response = self._make_request(params)
        if isinstance(text_response, str) and "symbol" in text_response:
             return text_response
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
