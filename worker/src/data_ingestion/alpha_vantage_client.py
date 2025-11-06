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
    # Zmieniono z warning na error dla workera, bo jest dla niego krytyczny
    logger.error("ALPHAVANTAGE_API_KEY not found in environment for WORKER's client.")

class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    # ZMIANA: Zmniejszamy domyślny limit ze 150 na 120, aby dodać bufor bezpieczeństwa
    def __init__(self, api_key: str = API_KEY, requests_per_minute: int = 120, retries: int = 3, backoff_factor: float = 0.5):
# ... (istniejący kod bez zmian) ...
        if not api_key:
            # Zmieniono z warning na error
            logger.error("API key is missing for AlphaVantageClient instance in WORKER.")
        self.api_key = api_key
# ... (istniejący kod bez zmian) ...
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()

    def _rate_limiter(self):
# ... (istniejący kod bez zmian) ...
        if not self.api_key:
             return
        while self.request_timestamps and (time.monotonic() - self.request_timestamps[0] > 60):
# ... (istniejący kod bez zmian) ...
        self.request_timestamps.append(time.monotonic())

    def _make_request(self, params: dict):
# ... (istniejący kod bez zmian) ...
        if not self.api_key:
            logger.error("Cannot make Alpha Vantage request: API key is missing.")
# ... (istniejący kod bez zmian) ...
        request_params = params.copy()
        request_params['apikey'] = self.api_key
        # ==========================================================
# ... (istniejący kod bez zmian) ...
        request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
        
        for attempt in range(self.retries):
# ... (istniejący kod bez zmian) ...
            try:
                # Używamy nowego słownika 'request_params' zamiast 'params'
                response = requests.get(self.BASE_URL, params=request_params, timeout=30)
# ... (istniejący kod bez zmian) ...
                try:
                    data = response.json()
                except json.JSONDecodeError:
# ... (istniejący kod bez zmian) ...
                    if params.get('datatype') == 'csv':
                        return response.text
                    # Jeśli to nie CSV, a JSON się nie udał, to błąd
# ... (istniejący kod bez zmian) ...
                is_rate_limit_json = False
                if "Information" in data:
# ... (istniejący kod bez zmian) ...
                    if "frequency" in info_text or "api call volume" in info_text or "please contact premium" in info_text:
                        is_rate_limit_json = True
                
                is_error_msg = "Error Message" in data
# ... (istniejący kod bez zmian) ...
                if is_rate_limit_json:
                    # To jest kluczowa zmiana: Traktuj błąd JSON jako błąd HTTP 429, aby wymusić ponowienie
# ... (istniejący kod bez zmian) ...
                    raise requests.exceptions.HTTPError(f"Rate Limit JSON: {data['Information']}", response=response)

                if not data or is_error_msg:
# ... (istniejący kod bez zmian) ...
                    return None
                
                # Jeśli wszystko jest OK (ani błąd HTTP, ani błąd JSON)
# ... (istniejący kod bez zmian) ...
                return data

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
# ... (istniejący kod bez zmian) ...
                logger.error(f"Request failed for {request_identifier} (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
# ... (istniejący kod bez zmian) ...
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Max retries reached for {request_identifier}.")

        return None # Zwróć None po wszystkich nieudanych próbach

    def get_market_status(self):
# ... (istniejący kod bez zmian) ...
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    # === Funkcja używana przez Fazę 1 i Faza 3 Monitor ===
    def get_bulk_quotes(self, symbols: list[str]):
# ... (istniejący kod bez zmian) ...
        params = {
            "function": "REALTIME_BULK_QUOTES",
# ... (istniejący kod bez zmian) ...
            "datatype": "csv",
        }
        text_response = self._make_request(params)
        
        # Sprawdzenie, czy odpowiedź tekstowa nie zawiera błędu
# ... (istniejący kod bez zmian) ...
            if "Error Message" in text_response or "Invalid API call" in text_response:
                logger.error(f"Bulk quotes API returned an error: {text_response[:200]}")
# ... (istniejący kod bez zmian) ...
            return text_response
        
        # Jeśli _make_request zwrócił None (po błędach)
        return None


    def get_company_overview(self, symbol: str):
# ... (istniejący kod bez zmian) ...
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
# ... (istniejący kod bez zmian) ...
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)
        
    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact', extended_hours: bool = True):
# ... (istniejący kod bez zmian) ...
        params = {
            "function": "TIME_SERIES_INTRADAY", 
# ... (istniejący kod bez zmian) ...
            "extended_hours": "true" if extended_hours else "false"
        }
        return self._make_request(params)

    def get_atr(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
# ... (istniejący kod bez zmian) ...
        params = {"function": "ATR", "symbol": symbol, "interval": "daily", "time_period": str(time_period)}
        return self._make_request(params)

    def get_rsi(self, symbol: str, time_period: int = 9, interval: str = 'daily', series_type: str = 'close'):
# ... (istniejący kod bez zmian) ...
        params = {"function": "RSI", "symbol": symbol, "interval": "daily", "time_period": str(time_period), "series_type": series_type}
        return self._make_request(params)
        
    def get_stoch(self, symbol: str, interval: str = 'daily'):
# ... (istniejący kod bez zmian) ...
        params = {"function": "STOCH", "symbol": symbol, "interval": "daily"}
        return self._make_request(params)

    def get_adx(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
# ... (istniejący kod bez zmian) ...
        params = {"function": "ADX", "symbol": symbol, "interval": "daily", "time_period": str(time_period)}
        return self._make_request(params)

    def get_macd(self, symbol: str, interval: str = 'daily', series_type: str = 'close'):
# ... (istniejący kod bez zmian) ...
        params = {"function": "MACD", "symbol": symbol, "interval": "daily", "series_type": series_type}
        return self._make_request(params)
        
    def get_bollinger_bands(self, symbol: str, time_period: int = 20, interval: str = 'daily', series_type: str = 'close'):
# ... (istniejący kod bez zmian) ...
        params = {"function": "BBANDS", "symbol": symbol, "interval": "daily", "time_period": str(time_period), "series_type": series_type}
        return self._make_request(params)
        
    def get_news_sentiment(self, ticker: str, limit: int = 50):
# ... (istniejący kod bez zmian) ...
        params = {"function": "NEWS_SENTIMENT", "tickers": ticker, "limit": str(limit)}
        return self._make_request(params)

    @staticmethod
# ... (istniejący kod bez zmian) ...
    def _safe_float(value) -> float | None:
        if value is None: return None
# ... (istniejący kod bez zmian) ...
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
# ... (istniejący kod bez zmian) ...
        except (ValueError, TypeError):
            return None
    
    # === NOWA FUNKCJA ===
    def _parse_bulk_quotes_csv(self, csv_text: str, ticker: str) -> dict | None:
# ... (istniejący kod bez zmian) ...
        """Przetwarza odpowiedź CSV z BULK_QUOTES i zwraca dane dla JEDNEGO tickera."""
        if not csv_text or "symbol" not in csv_text:
# ... (istniejący kod bez zmian) ...
            return None
        
        csv_file = StringIO(csv_text)
# ... (istniejący kod bez zmian) ...
        reader = csv.DictReader(csv_file)
        
        for row in reader:
# ... (istniejący kod bez zmian) ...
            if row.get('symbol') == ticker:
                # Zwracamy dane dla naszego tickera
                return row
        
        logger.warning(f"Nie znaleziono tickera {ticker} w odpowiedzi bulk quote.")
# ... (istniejący kod bez zmian) ...
        return None

    # === PRZEPISANA FUNKCJA ===
    def get_global_quote(self, symbol: str):
# ... (istniejący kod bez zmian) ...
        """
        Pobiera dane 'quote' używając niezawodnego endpointu REALTIME_BULK_QUOTES
# ... (istniejący kod bez zmian) ...
        """
        logger.info(f"Pobieranie ceny dla {symbol} przy użyciu REALTIME_BULK_QUOTES...")
        
        # 1. Użyj nowego, niezawodnego endpointu
# ... (istniejący kod bez zmian) ...
        bulk_csv = self.get_bulk_quotes([symbol])
        if not bulk_csv:
# ... (istniejący kod bez zmian) ...
            return None
        
        # 2. Sparsuj odpowiedź
# ... (istniejący kod bez zmian) ...
        quote_data = self._parse_bulk_quotes_csv(bulk_csv, symbol)
        if not quote_data:
# ... (istniejący kod bez zmian) ...
            return None

        # 3. Skonwertuj format CSV (np. 'close') na format JSON GLOBAL_QUOTE (np. '05. price')
# ... (istniejący kod bez zmian) ...
        try:
            formatted_quote = {
# ... (istniejący kod bez zmian) ...
                "05. price": quote_data.get("close"), # Najważniejsza zmiana
                "06. volume": quote_data.get("volume"),
# ... (istniejący kod bez zmian) ...
                "10. change percent": f'{quote_data.get("change_percent")}%' # Dodajemy % dla spójności
            }
            
            # 4. Sprawdź i nadpisz cenę danymi z after-market, jeśli istnieją
# ... (istniejący kod bez zmian) ...
            ext_price_str = quote_data.get("extended_hours_quote")
            ext_change_str = quote_data.get("extended_hours_change")
# ... (istniejący kod bez zmian) ...
            ext_change_pct_str = quote_data.get("extended_hours_change_percent")

            ext_price = self._safe_float(ext_price_str)

            if ext_price is not None and ext_price > 0:
# ... (istniejący kod bez zmian) ...
                formatted_quote["05. price"] = ext_price_str
                formatted_quote["09. change"] = ext_change_str
# ... (istniejący kod bez zmian) ...
                formatted_quote["10. change percent"] = f'{ext_change_pct_str}%'

            return formatted_quote
            
        except Exception as e:
# ... (istniejący kod bez zmian) ...
            logger.error(f"Błąd podczas konwersji formatu Bulk->GlobalQuote dla {symbol}: {e}", exc_info=True)
            # ==================================================================
            return None

    # ==================================================================
    # KROK B (FAZA 0): Dodanie 4 nowych funkcji dla danych makroekonomicznych
    # ==================================================================

    def get_cpi(self, interval: str = 'monthly'):
        """Pobiera dane o inflacji (CPI) (Premium)."""
        logger.info("Agent Makro: Pobieranie danych CPI...")
        params = {
            "function": "CPI",
            "interval": interval,
            "datatype": "json"
        }
        return self._make_request(params)

    def get_fed_funds_rate(self, interval: str = 'monthly'):
        """Pobiera dane o stopach procentowych FED (Premium)."""
        logger.info("Agent Makro: Pobieranie danych FED FUNDS RATE...")
        params = {
            "function": "FED_FUNDS_RATE",
            "interval": interval,
            "datatype": "json"
        }
        return self._make_request(params)

    def get_treasury_yield(self, interval: str = 'monthly', maturity: str = '10year'):
        """Pobiera dane o rentowności obligacji skarbowych (Premium)."""
        logger.info(f"Agent Makro: Pobieranie danych TREASURY YIELD ({maturity})...")
        params = {
            "function": "TREASURY_YIELD",
            "interval": interval,
            "maturity": maturity,
            "datatype": "json"
        }
        return self._make_request(params)

    def get_unemployment(self):
        """Pobiera dane o stopie bezrobocia (Premium)."""
        logger.info("Agent Makro: Pobieranie danych UNEMPLOYMENT...")
        params = {
            "function": "UNEMPLOYMENT",
            "datatype": "json"
        }
        return self._make_request(params)
    # ==================================================================
    # Koniec Krok B (FAZA 0)
    # ==================================================================
