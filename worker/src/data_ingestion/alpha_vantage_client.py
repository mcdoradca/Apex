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
    logger.error("ALPHAVANTAGE_API_KEY not found in environment for WORKER's client.")

class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    # === OPTYMALIZACJA: Limit 145 zapytań/minuta (Margines bezpieczeństwa: 5) ===
    def __init__(self, api_key: str = API_KEY, requests_per_minute: int = 145, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            logger.error("API key is missing for AlphaVantageClient instance in WORKER.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        
        # Pacing: Równomierne rozłożenie zapytań w czasie (ok. 0.41s odstępu)
        # Zapobiega wysyceniu limitu w pierwszych kilku sekundach minuty
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()
        
        # === OPTYMALIZACJA: Session Keep-Alive ===
        # Utrzymuje połączenie TCP/SSL, eliminując narzut na handshake przy każdym zapytaniu.
        # Drastycznie przyspiesza serie zapytań w pętlach (np. Faza 1 i 3).
        self.session = requests.Session()

    def _rate_limiter(self):
        """
        Zaawansowany Rate Limiter typu 'Rolling Window'.
        Zapobiega przekroczeniu limitu w DOWOLNYM oknie 60-sekundowym,
        a nie tylko w sztywnych minutach zegarowych.
        """
        if not self.api_key:
             return
             
        now = time.monotonic()
        
        # 1. Usuń wpisy starsze niż 60 sekund (przesuń okno)
        while self.request_timestamps and (now - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()
            
        # 2. Sprawdź "Twardy Limit" ilościowy w bieżącym oknie
        if len(self.request_timestamps) >= self.requests_per_minute:
            time_to_wait = 60 - (now - self.request_timestamps[0])
            if time_to_wait > 0:
                logger.warning(f"Rate limit reached ({len(self.request_timestamps)}/min). Sleeping for {time_to_wait:.2f}s.")
                time.sleep(time_to_wait)
                # Aktualizacja czasu po wybudzeniu
                now = time.monotonic()

        # 3. Sprawdź "Pacing" (Mikro-odstępy dla płynności)
        if self.request_timestamps:
            time_since_last = now - self.request_timestamps[-1]
            if time_since_last < self.request_interval:
                time.sleep(self.request_interval - time_since_last)
        
        self.request_timestamps.append(time.monotonic())

    def _make_request(self, params: dict):
        if not self.api_key:
            logger.error("Cannot make Alpha Vantage request: API key is missing.")
            return None
            
        request_params = params.copy()
        request_params['apikey'] = self.api_key
            
        request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
        
        for attempt in range(self.retries):
            self._rate_limiter() # Limiter zawsze przed wykonaniem zapytania
            
            try:
                # === OPTYMALIZACJA: Użycie self.session zamiast requests.get ===
                response = self.session.get(self.BASE_URL, params=request_params, timeout=30)
                
                # Obsługa specyficznych typów odpowiedzi (np. CSV vs JSON)
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    response.raise_for_status() 
                    # Jeśli to nie JSON i status 200 (np. CSV), zwróć tekst
                    if params.get('datatype') == 'csv':
                        return response.text
                    raise requests.exceptions.RequestException("Response was not valid JSON.")

                # Wykrywanie "miękkich" limitów w treści JSON (Alpha Vantage specyfika)
                is_rate_limit_json = False
                if "Information" in data:
                    info_text = data["Information"].lower()
                    if "frequency" in info_text or "api call volume" in info_text or "please contact premium" in info_text:
                        is_rate_limit_json = True
                
                is_error_msg = "Error Message" in data

                if is_rate_limit_json:
                    logger.warning(f"Rate limit JSON detected for {request_identifier} (Attempt {attempt + 1}/{self.retries}). Retrying...")
                    # Rzucamy wyjątek, aby wpaść w blok except i obsłużyć retry/backoff
                    raise requests.exceptions.HTTPError(f"Rate Limit JSON: {data['Information']}", response=response)

                if not data or is_error_msg:
                    logger.warning(f"API returned an error or empty data for {request_identifier}: {data}")
                    # To jest faktyczny błąd (np. zły ticker), nie ponawiamy
                    return None
                
                response.raise_for_status()
                return data

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.error(f"Request failed for {request_identifier} (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    sleep_time = self.backoff_factor * (2 ** (attempt + 1)) # Wykładniczy backoff
                    logger.info(f"Retrying in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Max retries reached for {request_identifier}.")

        return None

    # === METODY DANYCH RYNKOWYCH ===

    def get_market_status(self):
        """Pobiera aktualny status rynku z dedykowanego endpointu."""
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def get_bulk_quotes(self, symbols: list[str]):
        """Pobiera surowy tekst CSV dla endpointu REALTIME_BULK_QUOTES."""
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
            "datatype": "csv",
        }
        text_response = self._make_request(params)
        # Sprawdzenie, czy odpowiedź tekstowa nie zawiera błędu
        if isinstance(text_response, str):
            if "Error Message" in text_response or "Invalid API call" in text_response:
                logger.error(f"Bulk quotes API returned an error: {text_response[:200]}")
                return None
            return text_response
        return None

    def get_company_overview(self, symbol: str):
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)
        
    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact', extended_hours: bool = True, month: str = None):
        params = {
            "function": "TIME_SERIES_INTRADAY", 
            "symbol": symbol, 
            "interval": interval, 
            "outputsize": outputsize,
            "extended_hours": "true" if extended_hours else "false"
        }
        if month:
            params['month'] = month
        return self._make_request(params)

    # === WSKAŹNIKI TECHNICZNE ===

    def get_atr(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
        params = {"function": "ATR", "symbol": symbol, "interval": "daily", "time_period": str(time_period)}
        return self._make_request(params)

    def get_rsi(self, symbol: str, time_period: int = 9, interval: str = 'daily', series_type: str = 'close'):
        params = {"function": "RSI", "symbol": symbol, "interval": "daily", "time_period": str(time_period), "series_type": series_type}
        return self._make_request(params)
        
    def get_stoch(self, symbol: str, interval: str = 'daily'):
        params = {"function": "STOCH", "symbol": symbol, "interval": "daily"}
        return self._make_request(params)

    def get_adx(self, symbol: str, time_period: int = 14, interval: str = 'daily'):
        params = {"function": "ADX", "symbol": symbol, "interval": "daily", "time_period": str(time_period)}
        return self._make_request(params)

    def get_macd(self, symbol: str, interval: str = 'daily', series_type: str = 'close'):
        params = {"function": "MACD", "symbol": symbol, "interval": "daily", "series_type": series_type}
        return self._make_request(params)
        
    def get_bollinger_bands(self, symbol: str, time_period: int = 20, interval: str = 'daily', series_type: str = 'close', nbdevup: int = 2, nbdevdn: int = 2):
        params = {
            "function": "BBANDS", 
            "symbol": symbol, 
            "interval": interval, 
            "time_period": str(time_period), 
            "series_type": series_type,
            "nbdevup": str(nbdevup),
            "nbdevdn": str(nbdevdn)
        }
        return self._make_request(params)
    
    # === SENTYMENT I NEWSY ===

    def get_news_sentiment(self, ticker: str, limit: int = 50, time_from: str = None):
        params = {
            "function": "NEWS_SENTIMENT", 
            "tickers": ticker, 
            "limit": str(limit)
        }
        if time_from:
            params["time_from"] = time_from
        return self._make_request(params)

    # === NARZĘDZIA POMOCNICZE ===

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
        
        csv_file = StringIO(csv_text)
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            if row.get('symbol') == ticker:
                # Zwracamy dane dla naszego tickera
                return row
        
        logger.warning(f"Nie znaleziono tickera {ticker} w odpowiedzi bulk quote.")
        return None

    def get_global_quote(self, symbol: str):
        """
        Pobiera dane 'quote' używając niezawodnego endpointu REALTIME_BULK_QUOTES
        i konwertuje je do formatu starego GLOBAL_QUOTE dla kompatybilności.
        """
        logger.info(f"Pobieranie ceny dla {symbol} przy użyciu REALTIME_BULK_QUOTES...")
        
        bulk_csv = self.get_bulk_quotes([symbol])
        if not bulk_csv:
            logger.error(f"Nie otrzymano danych z REALTIME_BULK_QUOTES dla {symbol}.")
            return None
        
        quote_data = self._parse_bulk_quotes_csv(bulk_csv, symbol)
        if not quote_data:
            logger.error(f"Nie udało się sparsować odpowiedzi z REALTIME_BULK_QUOTES dla {symbol}.")
            return None

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
            
            # Sprawdź i nadpisz cenę danymi z after-market, jeśli istnieją
            ext_price_str = quote_data.get("extended_hours_quote")
            # ext_change_str = quote_data.get("extended_hours_change") # Nieużywane bezpośrednio w formatowaniu poniżej
            # ext_change_pct_str = quote_data.get("extended_hours_change_percent")

            ext_price = self._safe_float(ext_price_str)

            if ext_price is not None and ext_price > 0:
                logger.info(f"Wykryto cenę extended-hours dla {symbol}: {ext_price}. Nadpisywanie...")
                formatted_quote["05. price"] = ext_price_str
                formatted_quote["09. change"] = quote_data.get("extended_hours_change")
                formatted_quote["10. change percent"] = f'{quote_data.get("extended_hours_change_percent")}%'

            return formatted_quote
            
        except Exception as e:
            logger.error(f"Błąd podczas konwersji formatu Bulk->GlobalQuote dla {symbol}: {e}", exc_info=True)
            return None

    # === DANE MAKROEKONOMICZNE (FAZA 0) ===
    
    def get_inflation_rate(self, interval: str = 'monthly'):
        logger.info("Agent Makro: Pobieranie danych INFLATION (roczna stopa procentowa)...")
        params = {"function": "INFLATION", "interval": interval, "datatype": "json"}
        return self._make_request(params)

    def get_fed_funds_rate(self, interval: str = 'monthly'):
        logger.info("Agent Makro: Pobieranie danych FEDERAL_FUNDS_RATE...")
        params = {"function": "FEDERAL_FUNDS_RATE", "interval": interval, "datatype": "json"}
        return self._make_request(params)

    def get_treasury_yield(self, interval: str = 'monthly', maturity: str = '10year'):
        logger.info(f"Agent Makro: Pobieranie danych TREASURY YIELD ({maturity})...")
        params = {"function": "TREASURY_YIELD", "interval": interval, "maturity": maturity, "datatype": "json"}
        return self._make_request(params)

    def get_unemployment(self):
        logger.info("Agent Makro: Pobieranie danych UNEMPLOYMENT...")
        params = {"function": "UNEMPLOYMENT", "datatype": "json"}
        return self._make_request(params)
    
    # === DANE ANALITYCZNE (Używane w Backteście/H3) ===
    
    def get_time_series_weekly(self, symbol: str, outputsize: str = 'full'):
        params = {"function": "TIME_SERIES_WEEKLY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)

    def get_obv(self, symbol: str, interval: str = 'daily'):
        params = {"function": "OBV", "symbol": symbol, "interval": interval}
        return self._make_request(params)
        
    def get_sector_performance(self):
        params = {"function": "SECTOR"}
        return self._make_request(params)
    
    def get_vwap(self, symbol: str, interval: str, month: str = None):
        logger.info(f"AQM V3: Pobieranie VWAP dla {symbol} (interval: {interval}, month: {month or 'latest'})...")
        params = {"function": "VWAP", "symbol": symbol, "interval": interval}
        if month:
            params['month'] = month
        return self._make_request(params)

    def get_insider_transactions(self, symbol: str):
        logger.info(f"AQM V3: Pobieranie INSIDER_TRANSACTIONS dla {symbol}...")
        params = {"function": "INSIDER_TRANSACTIONS", "symbol": symbol}
        return self._make_request(params)

    def get_time_series_daily(self, symbol: str, outputsize: str = 'full'):
        logger.info(f"AQM V3: Pobieranie TIME_SERIES_DAILY (z VWAP) dla {symbol}...")
        params = {"function": "TIME_SERIES_DAILY", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)

    def get_earnings_calendar(self, horizon: str = '3month'):
        logger.info(f"AQM V3: Pobieranie EARNINGS_CALENDAR (horyzont: {horizon})...")
        params = {"function": "EARNINGS_CALENDAR", "horizon": horizon}
        return self._make_request(params)

    def get_earnings(self, symbol: str):
        logger.info(f"AQM V3: Pobieranie EARNINGS dla {symbol}...")
        params = {"function": "EARNINGS", "symbol": symbol}
        return self._make_request(params)

    def get_earnings_call_transcript(self, symbol: str, quarter: str):
        logger.info(f"AQM V3: Pobieranie EARNINGS_CALL_TRANSCRIPTS dla {symbol} (Q: {quarter})...")
        params = {"function": "EARNINGS_CALL_TRANSCRIPTS", "symbol": symbol, "quarter": quarter}
        return self._make_request(params)

    def get_wti(self, interval: str = 'daily'):
        logger.info(f"AQM V3: Pobieranie WTI (interval: {interval})...")
        params = {"function": "WTI", "interval": interval}
        return self._make_request(params)
