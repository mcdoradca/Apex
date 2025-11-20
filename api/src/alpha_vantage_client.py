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
        """Przetwarza odpowiedź CSV z BULK_QUOTES i zwraca dane dla JEDNEGO tickera."""
        if not csv_text or "symbol" not in csv_text:
            logger.warning("[DIAGNOSTYKA] Otrzymane dane CSV są puste lub nie zawierają nagłówka 'symbol'.")
            return None
        
        try:
            csv_file = StringIO(csv_text)
            reader = csv.DictReader(csv_file)
            
            for row in reader:
                if row.get('symbol') == ticker:
                    return row
            
            logger.warning(f"Nie znaleziono tickera {ticker} w odpowiedzi bulk quote.")
            return None
        except Exception as e:
            logger.error(f"Błąd parsowania CSV Bulk Quotes: {e}")
            return None

    # === METODY DANYCH RYNKOWYCH ===

    def get_market_status(self):
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def get_company_overview(self, symbol: str):
        """Pobiera dane fundamentalne o firmie (Sektor, Branża, Opis)."""
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_global_quote(self, symbol: str):
        """
        Pobiera najnowsze dane cenowe.
        ZMIANA (v3): Używa endpointu REALTIME_BULK_QUOTES (Premium) zamiast GLOBAL_QUOTE.
        Pozwala to na pobranie danych 'Extended Hours' (Pre-Market/After-Market).
        """
        # 1. Pobieramy dane z Bulk Quotes (CSV), który obsługuje realtime i extended hours
        bulk_csv = self.get_bulk_quotes([symbol])
        if not bulk_csv:
            logger.error(f"Nie udało się pobrać danych REALTIME dla {symbol}.")
            return None
            
        # 2. Parsujemy CSV
        quote_data = self._parse_bulk_quotes_csv(bulk_csv, symbol)
        if not quote_data:
            return None

        # 3. Mapujemy na format GLOBAL_QUOTE (aby zachować kompatybilność z resztą systemu)
        try:
            # Standardowa cena (CLOSE lub LATEST TRADE podczas sesji regularnej)
            formatted_quote = {
                "01. symbol": quote_data.get("symbol"),
                "02. open": quote_data.get("open"),
                "03. high": quote_data.get("high"),
                "04. low": quote_data.get("low"),
                "05. price": quote_data.get("close"), # W Bulk 'close' to cena bieżąca
                "06. volume": quote_data.get("volume"),
                "07. latest trading day": None,
                "08. previous close": quote_data.get("previous_close"),
                "09. change": quote_data.get("change"),
                "10. change percent": f'{quote_data.get("change_percent")}%'
            }
            
            # 4. Obsługa EXTENDED HOURS (Pre/Post Market)
            # Zgodnie z zaleceniem Supportu: Jeśli mamy dane extended, używamy ich jako "Ceny Aktualnej"
            ext_price_str = quote_data.get("extended_hours_quote")
            ext_price = self._safe_float(ext_price_str)
            
            if ext_price and ext_price > 0:
                logger.info(f"Wykryto cenę Extended Hours dla {symbol}: {ext_price}. Nadpisywanie...")
                formatted_quote["05. price"] = ext_price_str
                # Aktualizujemy też zmianę procentową, jeśli jest dostępna dla extended hours
                if quote_data.get("extended_hours_change"):
                    formatted_quote["09. change"] = quote_data.get("extended_hours_change")
                if quote_data.get("extended_hours_change_percent"):
                    formatted_quote["10. change percent"] = f'{quote_data.get("extended_hours_change_percent")}%'

            return formatted_quote
            
        except Exception as e:
            logger.error(f"Błąd mapowania danych Bulk dla {symbol}: {e}", exc_info=True)
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
        To jest endpoint PREMIUM, który zwraca ceny extended hours.
        """
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
            "datatype": "csv"
        }
        self._rate_limiter()
        params['apikey'] = self.api_key
        try:
            # Używamy sesji dla wydajności
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
