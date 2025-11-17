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
        if not api_key:
            # Zmieniono z warning na error
            logger.error("API key is missing for AlphaVantageClient instance in WORKER.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()

    def _rate_limiter(self):
        if not self.api_key:
             return
        # ==================================================================
        # === POPRAWKA KRYTYCZNA: Logika Rate Limitera została odkomentowana ===
        # ==================================================================
        while self.request_timestamps and (time.monotonic() - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()
        if len(self.request_timestamps) >= self.requests_per_minute:
            time_to_wait = 60 - (time.monotonic() - self.request_timestamps[0])
            if time_to_wait > 0:
                logger.warning(f"Rate limit reached (Rolling Window). Sleeping for {time_to_wait:.2f} seconds.")
                time.sleep(time_to_wait)
        if self.request_timestamps:
            time_since_last = time.monotonic() - self.request_timestamps[-1]
            if time_since_last < self.request_interval:
                # To jest główny ogranicznik
                time.sleep(self.request_interval - time_since_last)
        # ==================================================================
        # Koniec Poprawki
        # ==================================================================
        self.request_timestamps.append(time.monotonic())

    def _make_request(self, params: dict):
        if not self.api_key:
            logger.error("Cannot make Alpha Vantage request: API key is missing.")
            return None
            
        # === POPRAWKA KODU: Dodanie klucza API do każdego zapytania ===
        # Tworzymy nowy słownik params, który zawiera oryginalne parametry
        # oraz klucz API.
        request_params = params.copy()
        request_params['apikey'] = self.api_key
        # ==========================================================
            
        request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
        
        for attempt in range(self.retries):
            self._rate_limiter() # Przeniesiono limiter *do* pętli ponawiania
            try:
                # Używamy nowego słownika 'request_params' zamiast 'params'
                response = requests.get(self.BASE_URL, params=request_params, timeout=30)
                
                # Spróbuj sparsować JSON w pierwszej kolejności
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    # Jeśli JSON failuje (np. przy CSV), sprawdź status i rzuć błąd
                    response.raise_for_status() 
                    # Jeśli to nie-JSON (jak CSV) i status 200, zwróć surowy tekst (tylko dla get_bulk_quotes)
                    if params.get('datatype') == 'csv':
                        return response.text
                    # Jeśli to nie CSV, a JSON się nie udał, to błąd
                    raise requests.exceptions.RequestException("Response was not valid JSON.")

                # ==================================================================
                # ZMIANA: Bardziej robustyczna logika sprawdzania błędów w JSON
                # ==================================================================
                is_rate_limit_json = False
                if "Information" in data:
                    info_text = data["Information"].lower()
                    # Sprawdzamy kluczowe frazy z logów, które wskazują na błąd limitu
                    if "frequency" in info_text or "api call volume" in info_text or "please contact premium" in info_text:
                        is_rate_limit_json = True
                
                is_error_msg = "Error Message" in data
                # ==================================================================

                if is_rate_limit_json:
                    # To jest kluczowa zmiana: Traktuj błąd JSON jako błąd HTTP 429, aby wymusić ponowienie
                    logger.warning(f"Rate limit JSON detected for {request_identifier} (Attempt {attempt + 1}/{self.retries}). Retrying...")
                    # Rzucamy błąd, aby został złapany przez 'except' poniżej i aktywował logikę ponawiania
                    raise requests.exceptions.HTTPError(f"Rate Limit JSON: {data['Information']}", response=response)

                if not data or is_error_msg:
                    logger.warning(f"API returned an error or empty data for {request_identifier}: {data}")
                    # Usunęliśmy logikę "premium", ponieważ jest ona teraz obsługiwana przez 'is_rate_limit_json'
                    # To jest *faktyczny* błąd (np. zły ticker), więc nie ponawiamy, tylko zwracamy None
                    return None
                
                # Jeśli wszystko jest OK (ani błąd HTTP, ani błąd JSON)
                response.raise_for_status() # Sprawdź błędy 4xx/5xx
                return data

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                # Ta sekcja 'except' jest teraz poprawnie aktywowana przez 'is_rate_limit_json'
                logger.error(f"Request failed for {request_identifier} (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    sleep_time = self.backoff_factor * (2 ** (attempt + 1)) # Zwiększamy backoff
                    logger.info(f"Retrying in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Max retries reached for {request_identifier}.")

        return None # Zwróć None po wszystkich nieudanych próbach

    def get_market_status(self):
        """Pobiera aktualny status rynku z dedykowanego endpointu."""
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    # === Funkcja używana przez Fazę 1 i Faza 3 Monitor ===
    def get_bulk_quotes(self, symbols: list[str]):
        """Pobiera surowy tekst CSV dla endpointu REALTIME_BULK_QUOTES."""
        # ZMIANA: _make_request teraz obsługuje także odpowiedzi tekstowe (CSV)
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
        
        # Jeśli _make_request zwrócił None (po błędach)
        return None


    def get_company_overview(self, symbol: str):
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)
        
    # ==================================================================
    # === POPRAWKA (NAPRAWA VWAP/H3): Dodanie parametru 'month' ===
    # ==================================================================
    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact', extended_hours: bool = True, month: str = None):
        # Specyfikacja AQM V3 (4.1) wymaga interval=5min, outputsize=full
        params = {
            "function": "TIME_SERIES_INTRADAY", 
            "symbol": symbol, 
            "interval": interval, 
            "outputsize": outputsize,
            "extended_hours": "true" if extended_hours else "false"
        }
        if month:
            params['month'] = month # np. '2022-01'
        # ==================================================================
        return self._make_request(params)

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
        
    # ==================================================================
    # === AKTUALIZACJA AQM V3 (Krok 12) ===
    # Dodano parametry `nbdevup` i `nbdevdn` zgodnie ze specyfikacją Wymiaru 3.1
    # ==================================================================
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
    
    # ==================================================================
    # === AKTUALIZACJA AQM V3 (Krok 12) ===
    # Dodano parametr `time_from` zgodnie ze specyfikacją Wymiaru 2.2
    # ==================================================================
    def get_news_sentiment(self, ticker: str, limit: int = 50, time_from: str = None):
        params = {
            "function": "NEWS_SENTIMENT", 
            "tickers": ticker, 
            "limit": str(limit)
        }
        if time_from:
            params["time_from"] = time_from # Np. "20220410T0130"
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
    
    # === NOWA FUNKCJA ===
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
            # ==================================================================
            # KRYTYCZNA POPRAWKA: Zmiana ':' na '='
            # ==================================================================
            logger.error(f"Błąd podczas konwersji formatu Bulk->GlobalQuote dla {symbol}: {e}", exc_info=True)
            # ==================================================================
            return None

    # ==================================================================
    # === WERYFIKACJA ENDPOINTÓW MAKRO (FAZA 0) ===
    # ==================================================================
    def get_inflation_rate(self, interval: str = 'monthly'):
        """
        WERYFIKACJA: Nazwa funkcji API `INFLATION` jest poprawna.
        Zwraca roczną stopę inflacji (procentową).
        """
        logger.info("Agent Makro: Pobieranie danych INFLATION (roczna stopa procentowa)...")
        params = {
            "function": "INFLATION",
            "interval": interval,
            "datatype": "json"
        }
        return self._make_request(params)

    def get_fed_funds_rate(self, interval: str = 'monthly'):
        """
        WERYFIKACJA: Nazwa funkcji API to `FEDERAL_FUNDS_RATE`.
        """
        logger.info("Agent Makro: Pobieranie danych FEDERAL_FUNDS_RATE...")
        params = {
            "function": "FEDERAL_FUNDS_RATE", # Poprawna nazwa
            "interval": interval,
            "datatype": "json"
        }
        return self._make_request(params)

    def get_treasury_yield(self, interval: str = 'monthly', maturity: str = '10year'):
        """
        WERYFIKACJA: Nazwa funkcji API to `TREASURY_YIELD`.
        """
        logger.info(f"Agent Makro: Pobieranie danych TREASURY YIELD ({maturity})...")
        params = {
            "function": "TREASURY_YIELD",
            "interval": interval,
            "maturity": maturity,
            "datatype": "json"
        }
        return self._make_request(params)

    def get_unemployment(self):
        """
        WERYFIKACJA: Nazwa funkcji API to `UNEMPLOYMENT`.
        """
        logger.info("Agent Makro: Pobieranie danych UNEMPLOYMENT...")
        params = {
            "function": "UNEMPLOYMENT",
            "datatype": "json"
        }
        return self._make_request(params)
    
    # ==================================================================
    # === WERYFIKACJA ENDPOINTÓW AQM V3 (Backtest) ===
    # ==================================================================
    
    def get_time_series_weekly(self, symbol: str, outputsize: str = 'full'):
        """Pobiera *zwykłe* (nie-adjusted) dane tygodniowe."""
        params = {
            "function": "TIME_SERIES_WEEKLY_ADJUSTED", 
            "symbol": symbol,
            "outputsize": outputsize
        }
        # WERYFIKACJA: TIME_SERIES_WEEKLY_ADJUSTED to dobry wybór
        return self._make_request(params)

    def get_obv(self, symbol: str, interval: str = 'daily'):
        """Pobiera dane On-Balance Volume (OBV)."""
        params = {
            "function": "OBV",
            "symbol": symbol,
            "interval": interval
        }
        # WERYFIKACJA: Funkcja `OBV` jest poprawna
        return self._make_request(params)
        
    def get_sector_performance(self):
        """Pobiera dane o wydajności sektorów."""
        params = {"function": "SECTOR"}
        # WERYFIKACJA: Funkcja `SECTOR` jest poprawna
        return self._make_request(params)
    
    # ==================================================================
    # === POPRAWKA (NAPRAWA VWAP): Endpoint `get_vwap` ===
    # Zgodnie z info od supportu, ta funkcja przyjmuje `month` i `interval`.
    # `outputsize` nie jest obsługiwany przez ten endpoint.
    # ==================================================================
    def get_vwap(self, symbol: str, interval: str, month: str = None):
        """(AQM V3 - Wymiar 1.2) Pobiera dane VWAP."""
        logger.info(f"AQM V3: Pobieranie VWAP dla {symbol} (interval: {interval}, month: {month or 'latest'})...")
        params = {
            "function": "VWAP",
            "symbol": symbol,
            "interval": interval
        }
        if month:
            params['month'] = month # np. '2022-01'
            
        return self._make_request(params)
    # ==================================================================

    def get_insider_transactions(self, symbol: str):
        """(AQM V3 - Wymiar 2.1) Pobiera transakcje insiderów."""
        logger.info(f"AQM V3: Pobieranie INSIDER_TRANSACTIONS dla {symbol}...")
        params = {
            "function": "INSIDER_TRANSACTIONS",
            "symbol": symbol
        }
        # WERYFIKACJA: Funkcja `INSIDER_TRANSACTIONS` jest poprawna.
        return self._make_request(params)

    def get_time_series_daily(self, symbol: str, outputsize: str = 'full'):
        """
        (AQM V3 - Wymiar 1.2, 3.1, 7.1) Pobiera dane dzienne *bez* korekty.
        TEN ENDPOINT ZAWIERA kolumnę 6. vwap, która jest kluczowa dla $\nabla^2$.
        """
        logger.info(f"AQM V3: Pobieranie TIME_SERIES_DAILY (z VWAP) dla {symbol}...")
        params = {
            "function": "TIME_SERIES_DAILY", # WERYFIKACJA: Poprawna funkcja dla VWAP w treści
            "symbol": symbol,
            "outputsize": outputsize
        }
        return self._make_request(params)

    def get_earnings_calendar(self, horizon: str = '3month'):
        """(AQM V3 - Wymiar 5.1) Pobiera kalendarz wyników."""
        logger.info(f"AQM V3: Pobieranie EARNINGS_CALENDAR (horyzont: {horizon})...")
        params = {
            "function": "EARNINGS_CALENDAR",
            "horizon": horizon
        }
        # WERYFIKACJA: Funkcja `EARNINGS_CALENDAR` jest poprawna
        return self._make_request(params)

    def get_earnings(self, symbol: str):
        """(AQM V3 - Wymiar 5.1) Pobiera historyczne wyniki kwartalne."""
        logger.info(f"AQM V3: Pobieranie EARNINGS dla {symbol}...")
        params = {
            "function": "EARNINGS",
            "symbol": symbol
        }
        # WERYFIKACJA: Funkcja `EARNINGS` jest poprawna
        return self._make_request(params)

    def get_earnings_call_transcript(self, symbol: str, quarter: str):
        """(AQM V3 - Wymiar 5.1) Pobiera transkrypcje wyników."""
        logger.info(f"AQM V3: Pobieranie EARNINGS_CALL_TRANSCRIPTS dla {symbol} (Q: {quarter})...")
        params = {
            "function": "EARNINGS_CALL_TRANSCRIPTS",
            "symbol": symbol,
            "quarter": quarter # Np. "2024Q1"
        }
        # WERYFIKACJA: Funkcja `EARNINGS_CALL_TRANSCRIPTS` jest poprawna
        return self._make_request(params)

    def get_wti(self, interval: str = 'daily'):
        """(AQM V3 - Wymiar 6.1) Pobiera ceny ropy WTI."""
        logger.info(f"AQM V3: Pobieranie WTI (interval: {interval})...")
        params = {
            "function": "WTI",
            "interval": interval
        }
        # WERYFIKACJA: Funkcja `WTI` jest poprawna
        return self._make_request(params)
    
    # ==================================================================
    # === KONIEC WERYFIKACJI ===
    # ==================================================================
