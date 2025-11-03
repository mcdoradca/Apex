import time
import requests
import logging
import json
# NOWE IMPORTY do parsowania CSV
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
# ... (reszta __init__ bez zmian) ...
        self.request_timestamps = deque()

    def _rate_limiter(self):
# ... (bez zmian) ...
        self.request_timestamps.append(time.monotonic())

    def _make_request(self, params: dict):
# ... (bez zmian) ...
        return None

    def get_market_status(self):
# ... (bez zmian) ...
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    # === NOWA FUNKCJA (Przeniesiona z workera) ===
    def get_bulk_quotes(self, symbols: list[str]):
        """Pobiera surowy tekst CSV dla endpointu REALTIME_BULK_QUOTES."""
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
                return text_response
            except requests.exceptions.RequestException as e:
                logger.error(f"Bulk quotes request failed (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    def get_company_overview(self, symbol: str):
# ... (bez zmian) ...
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
# ... (bez zmian) ...
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)

    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact', extended_hours: bool = True):
# ... (bez zmian) ...
        }
        return self._make_request(params)

    def get_atr(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
# ... (bez zmian) ...
        params = {"function": "ATR", "symbol": symbol, "interval": "daily", "time_period": str(time_period)}
        return self._make_request(params)

    def get_rsi(self, symbol: str, time_period: int = 9, interval: str = 'daily', series_type: str = 'close'):
# ... (bez zmian) ...
        params = {"function": "RSI", "symbol": symbol, "interval": "daily", "time_period": str(time_period), "series_type": series_type}
        return self._make_request(params)

    def get_stoch(self, symbol: str, interval: str = 'daily'):
# ... (bez zmian) ...
        params = {"function": "STOCH", "symbol": symbol, "interval": "daily"}
        return self._make_request(params)

    def get_adx(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
# ... (bez zmian) ...
        params = {"function": "ADX", "symbol": symbol, "interval": "daily", "time_period": str(time_period)}
        return self._make_request(params)

    def get_macd(self, symbol: str, interval: str = 'daily', series_type: str = 'close'):
# ... (bez zmian) ...
        params = {"function": "MACD", "symbol": symbol, "interval": "daily", "series_type": series_type}
        return self._make_request(params)
        
    def get_bollinger_bands(self, symbol: str, time_period: int = 20, interval: str = 'daily', series_type: str = 'close'):
# ... (bez zmian) ...
        params = {"function": "BBANDS", "symbol": symbol, "interval": "daily", "time_period": str(time_period), "series_type": series_type}
        return self._make_request(params)
        
    def get_news_sentiment(self, ticker: str, limit: int = 50):
# ... (bez zmian) ...
        params = {"function": "NEWS_SENTIMENT", "tickers": ticker, "limit": str(limit)}
        return self._make_request(params)

    @staticmethod
    def _safe_float(value) -> float | None:
# ... (bez zmian) ...
        except (ValueError, TypeError):
            return None
    
    # === NOWA FUNKCJA (Przeniesiona z workera) ===
    def _parse_bulk_quotes_csv(self, csv_text: str, ticker: str) -> dict | None:
        """Przetwarza odpowiedź CSV z BULK_QUOTES i zwraca dane dla JEDNEGO tickera."""
        if not csv_text or "symbol" not in csv_text:
            logger.warning("[DIAGNOSTYKA] Otrzymane dane CSV są puste lub nie zawierają nagłówka 'symbol'.")
            return None
        
        csv_file = StringIO(csv_text)
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            if row.get('symbol') == ticker:
                # Zwracamy dane dla naszego tickera
                return row
        
        logger.warning(f"Nie znaleziono tickera {ticker} w odpowiedzi bulk quote.")
        return None

    # === PRZEPISANA FUNKCJA ===
    def get_global_quote(self, symbol: str):
        """
        Pobiera dane 'quote' używając niezawodnego endpointu REALTIME_BULK_QUOTES
        i konwertuje je do formatu starego GLOBAL_QUOTE dla kompatybilności z frontendem.
        """
        logger.info(f"Pobieranie ceny dla {symbol} przy użyciu REALTIME_BULK_QUOTES...")
        
        # 1. Użyj nowego, niezawodnego endpointu
        bulk_csv = self.get_bulk_quotes([symbol])
        if not bulk_csv:
            logger.error(f"Nie otrzymano danych z REALTIME_BULK_QUOTES dla {symbol}.")
            return None
        
        # 2. Sparsuj odpowiedź
        quote_data = self._parse_bulk_quotes_csv(bulk_csv, symbol)
        if not quote_data:
            logger.error(f"Nie udało się sparsować odpowiedzi z REALTIME_BULK_QUOTES dla {symbol}.")
            return None

        # 3. Skonwertuj format CSV (np. 'close') na format JSON GLOBAL_QUOTE (np. '05. price')
        #    To zapewnia, że frontend (index.html) nie potrzebuje żadnych zmian.
        try:
            formatted_quote = {
                "01. symbol": quote_data.get("symbol"),
                "02. open": quote_data.get("open"),
                "03. high": quote_data.get("high"),
                "04. low": quote_data.get("low"),
                "05. price": quote_data.get("close"), # Najważniejsza zmiana
                "06. volume": quote_data.get("volume"),
                "07. latest trading day": None, # Te dane nie są w BULK_QUOTES
                "08. previous close": quote_data.get("previous_close"),
                "09. change": quote_data.get("change"),
                "10. change percent": f'{quote_data.get("change_percent")}%' # Dodajemy % dla spójności
            }
            
            # 4. Sprawdź i nadpisz cenę danymi z after-market, jeśli istnieją
            #    (Logika, którą nam pokazałeś w swoich danych JSON)
            ext_price_str = quote_data.get("extended_hours_quote")
            ext_change_str = quote_data.get("extended_hours_change")
            ext_change_pct_str = quote_data.get("extended_hours_change_percent")

            ext_price = self._safe_float(ext_price_str)

            if ext_price is not None and ext_price > 0:
                logger.info(f"Wykryto cenę extended-hours dla {symbol}: {ext_price}. Nadpisywanie...")
                formatted_quote["05. price"] = ext_price_str
                formatted_quote["09. change"] = ext_change_str
                formatted_quote["10. change percent"] = f'{ext_change_pct_str}%'

            return formatted_quote
            
        except Exception as e:
            logger.error(f"Błąd podczas konwersji formatu Bulk->GlobalQuote dla {symbol}: {e}", exc_info=True)
            return None

