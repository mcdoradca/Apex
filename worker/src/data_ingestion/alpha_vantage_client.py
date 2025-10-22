import time
import requests
import logging
import json
from collections import deque
import os # Dodano import os
from dotenv import load_dotenv # Dodano import dotenv
from io import StringIO
import csv
# Zmieniono ścieżkę importu, aby pasowała do struktury workera
from ..models import Base

load_dotenv() # Dodano wywołanie load_dotenv

logger = logging.getLogger(__name__)

# Dodano pobieranie API_KEY specyficznie dla workera
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    # Zmieniono na warning, bo worker może mieć klucz w innym miejscu? Lepiej jednak, żeby był.
    logger.warning("ALPHAVANTAGE_API_KEY not found in environment for Worker's client.")


class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    # Używamy API_KEY zdefiniowanego wyżej jako domyślny
    def __init__(self, api_key: str = API_KEY, requests_per_minute: int = 150, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            # Zmieniono na warning, bo może być dostarczony później? Ale lepiej ostrzec.
            logger.warning("API key is missing or empty for AlphaVantageClient instance in Worker.")
            # raise ValueError("API key cannot be empty.") # Usunięto rzucanie błędem, aby worker mógł potencjalnie działać dalej lub pobrać klucz inaczej
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()

    def _rate_limiter(self):
        # Sprawdzenie api_key na początku
        if not self.api_key:
             logger.warning("Rate limiting skipped: API key is missing.")
             return # Nie rób nic jeśli nie ma klucza
        # Reszta logiki bez zmian
        while self.request_timestamps and (time.monotonic() - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()
        if len(self.request_timestamps) >= self.requests_per_minute:
            time_to_wait = 60 - (time.monotonic() - self.request_timestamps[0])
            if time_to_wait > 0:
                logger.warning(f"Rate limit reached. Sleeping for {time_to_wait:.2f} seconds.")
                time.sleep(time_to_wait)
        # Poprawka: Czekaj tylko jeśli są jakieś timestampy i czas od ostatniego jest za krótki
        if self.request_timestamps:
            time_since_last = time.monotonic() - self.request_timestamps[-1]
            if time_since_last < self.request_interval:
                 # Dodano mały margines, żeby uniknąć zbyt dokładnego trafienia w limit
                 sleep_duration = self.request_interval - time_since_last + 0.01
                 logger.debug(f"Throttling request. Sleeping for {sleep_duration:.3f} seconds.")
                 time.sleep(sleep_duration)
        self.request_timestamps.append(time.monotonic())


    def _make_request(self, params: dict, is_fallback: bool = False):
        if not self.api_key:
            # Dla fallbacku tylko ostrzegamy, dla normalnego błąd
            log_func = logger.warning if is_fallback else logger.error
            log_func("Cannot make Alpha Vantage request: API key is missing.")
            return None

        # Używamy _rate_limiter() tylko dla głównych zapytań
        if not is_fallback:
            self._rate_limiter()

        params['apikey'] = self.api_key
        request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
        max_retries = 1 if is_fallback else self.retries # Mniej prób dla fallbacku

        for attempt in range(max_retries):
            try:
                # Logowanie poziomu DEBUG dla zwykłych zapytań, INFO dla fallbacku (żeby go widzieć)
                log_level = logging.INFO if is_fallback else logging.DEBUG
                logger.log(log_level, f"Making AV request for {request_identifier} (Attempt {attempt+1}/{max_retries}). Function: {params.get('function')}")

                response = requests.get(self.BASE_URL, params=params, timeout=15 if is_fallback else 30) # Krótszy timeout dla fallbacku
                response.raise_for_status() # Sprawdza błędy HTTP (4xx, 5xx)

                # --- Obsługa CSV ---
                if params.get('datatype') == 'csv':
                    text_response = response.text
                    # Sprawdzenie czy odpowiedź nie jest pusta lub nie jest HTMLem błędu
                    if not text_response or text_response.strip().startswith('<'):
                         logger.error(f"Alpha Vantage API returned empty or non-CSV response for {request_identifier}. Response: {text_response[:200]}")
                         return None
                    # Sprawdzenie typowych błędów w treści CSV
                    if "Error Message" in text_response or "Invalid API call" in text_response:
                        logger.error(f"Alpha Vantage API returned an error (CSV): {text_response[:200]}")
                        # Dodatkowe opóźnienie przy błędach API, nawet w CSV
                        if "premium" in text_response.lower() and not is_fallback:
                            logger.error(f"CSV API call for {request_identifier} failed due to premium limit. Waiting longer.")
                            time.sleep(20)
                        return None
                    return text_response # Zwracamy tekst CSV

                # --- Obsługa JSON ---
                data = response.json()
                # Sprawdzenie czy odpowiedź nie jest pusta lub nie zawiera typowych błędów AV
                if not data or "Error Message" in data or "Information" in data:
                    log_func = logger.warning if is_fallback else logger.error
                    log_func(f"API returned an error or empty data for {request_identifier}: {data}")
                    if "premium" in str(data).lower() and not is_fallback:
                         logger.error(f"API call for {request_identifier} failed due to premium limit. Waiting longer.")
                         time.sleep(20)
                    return None
                return data # Zwracamy sparsowany JSON

            except requests.exceptions.HTTPError as http_err:
                 # Błąd HTTP (np. 404, 500, 429)
                 log_func = logger.warning if is_fallback else logger.error
                 log_func(f"HTTP error occurred for {request_identifier} (Attempt {attempt + 1}/{max_retries}): {http_err} - Status: {http_err.response.status_code}")
                 # Można dodać specyficzną obsługę 429 (Too Many Requests) jeśli potrzebne

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                # Inne błędy sieciowe, timeouty, błędy parsowania JSON
                 log_func = logger.warning if is_fallback else logger.error
                 log_func(f"Request failed for {request_identifier} (attempt {attempt + 1}/{max_retries}): {e}")

            # Czekanie przed ponowieniem (tylko jeśli to nie ostatnia próba)
            if attempt < max_retries - 1:
                sleep_time = self.backoff_factor * (2 ** attempt)
                logger.info(f"Retrying request for {request_identifier} in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

        # Jeśli pętla się zakończyła bez sukcesu
        logger.error(f"Request failed for {request_identifier} after {max_retries} attempts.")
        return None

    def get_market_status(self):
        """Pobiera aktualny status rynku z dedykowanego endpointu."""
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def _parse_bulk_quotes_csv(self, csv_text: str) -> dict:
        """Przetwarza odpowiedź CSV z BULK_QUOTES na słownik danych."""
        if not csv_text or "symbol" not in csv_text.lower(): # Sprawdzamy case-insensitive
            logger.warning("[CSV PARSER] Otrzymane dane CSV są puste lub nie zawierają nagłówka 'symbol'. Treść: %s", csv_text[:200])
            return {}

        csv_file = StringIO(csv_text)
        try:
            reader = csv.DictReader(csv_file)
            # Sprawdzenie czy nagłówki istnieją
            if not reader.fieldnames:
                 logger.warning("[CSV PARSER] CSV nie zawiera nagłówków.")
                 return {}

            # Normalizacja nagłówków (małe litery, zamiana spacji na podkreślniki)
            normalized_fieldnames = [name.lower().strip().replace(' ', '_') for name in reader.fieldnames]
            logger.debug(f"[CSV PARSER] Normalized headers: {normalized_fieldnames}")
            # Przypisanie znormalizowanych nagłówków do czytnika
            reader.fieldnames = normalized_fieldnames

            data_dict = {}
            row_count = 0
            for row in reader:
                row_count += 1
                ticker = row.get('symbol') # Klucz 'symbol' jest już znormalizowany
                if not ticker:
                    logger.warning(f"[CSV PARSER] Row {row_count} has no 'symbol'. Skipping row: {row}")
                    continue

                # Klucze, których *oczekujemy* po normalizacji i poprawkach
                expected_keys_after_fix = ['symbol', 'price', 'previous_close', 'extended_hours_quote', 'close'] # Dodano 'close' do sprawdzanych
                # Sprawdzamy czy klucze istnieją i czy wartości nie są puste
                missing_keys_in_row = [key for key in expected_keys_after_fix if row.get(key) is None or str(row.get(key)).strip() == ""]

                # Ostrzeżenie tylko jeśli brakuje *cen* (price, previous_close, extended_hours_quote, close)
                missing_prices = [k for k in ['price', 'previous_close', 'extended_hours_quote', 'close'] if k in missing_keys_in_row]
                if missing_prices:
                    # Zmieniono poziom logowania na DEBUG, żeby nie zaśmiecać przy normalnych pustych polach (np. extended w regular)
                    logger.debug(f"[CSV PARSER] Ticker {ticker} - Row {row_count} has missing/empty PRICE values for keys: {missing_prices}. Row content: {row}")

                # Zapisujemy cały wiersz (z znormalizowanymi kluczami) do słownika
                data_dict[ticker] = row

            if not data_dict:
                 logger.warning("[CSV PARSER] Parsowanie CSV zakończone, ale nie znaleziono żadnych danych tickerów.")
            return data_dict
        except csv.Error as csv_err:
             # Błędy specyficzne dla parsowania CSV
             logger.error(f"[CSV PARSER] Błąd podczas parsowania CSV: {csv_err}. Treść CSV (początek): {csv_text[:500]}")
             return {}
        except Exception as e:
             # Inne, nieoczekiwane błędy
             logger.error(f"[CSV PARSER] Nieoczekiwany błąd podczas parsowania CSV: {e}. Treść CSV (początek): {csv_text[:500]}", exc_info=True)
             return {}

    def get_bulk_quotes(self, symbols: list[str]):
        """Pobiera dane BULK_QUOTES w formacie CSV i parsuje je."""
        # Dodatkowe sprawdzenie klucza przed wywołaniem
        if not self.api_key:
            logger.error("Cannot get bulk quotes: API key is missing.")
            return None
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
            "datatype": "csv"
            # api_key zostanie dodany przez _make_request
        }
        csv_text = self._make_request(params) # _make_request obsłuży logowanie błędów API
        if csv_text is None:
             # Logowanie błędu pobrania danych już w _make_request
             # logger.error(f"Nie udało się pobrać danych bulk quotes (CSV) dla: {','.join(symbols)}")
             return None # Zwracamy None jeśli pobranie się nie powiodło

        # Parsujemy otrzymany tekst CSV
        parsed_data = self._parse_bulk_quotes_csv(csv_text)
        # _parse_bulk_quotes_csv obsłuży logowanie błędów parsowania
        return parsed_data # Zwracamy sparsowany słownik (może być pusty)


    def get_global_quote_json(self, symbol: str):
        """Pobiera dane GLOBAL_QUOTE w formacie JSON (używane jako fallback)."""
        # Dodatkowe sprawdzenie klucza
        if not self.api_key:
            logger.warning("Cannot get global quote (fallback): API key is missing.")
            return None
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol}
        # Używamy flagi is_fallback=True
        return self._make_request(params, is_fallback=True)

    # --- Pozostałe metody pobierania danych (bez zmian logiki, tylko _make_request) ---

    def get_company_overview(self, symbol: str):
        params = {"function": "OVERVIEW", "symbol": symbol}
        return self._make_request(params)

    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'):
        params = {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize}
        return self._make_request(params)

    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact', extended_hours: bool = True):
        params = {
            "function": "TIME_SERIES_INTRADAY", "symbol": symbol, "interval": interval,
            "outputsize": outputsize, "extended_hours": "true" if extended_hours else "false"
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
        """Bezpiecznie konwertuje wartość na float."""
        if value is None: return None
        # Obsługa int/float bezpośrednio
        if isinstance(value, (int, float)): return float(value)
        # Obsługa stringów
        if isinstance(value, str):
            # Czyszczenie stringa
            cleaned_value = value.strip().replace(',', '').replace('%', '')
            # Sprawdzenie czy pusty lub znane wartości nieliczbowe
            if not cleaned_value or cleaned_value.lower() in ['n/a', 'none', '-']: return None
            # Próba konwersji
            try: return float(cleaned_value)
            except (ValueError, TypeError):
                logger.debug(f"[_safe_float] Could not convert cleaned string '{cleaned_value}' to float.")
                return None
        # Próba konwersji innych typów (mało prawdopodobne, ale dla bezpieczeństwa)
        try: return float(value)
        except (ValueError, TypeError):
             logger.debug(f"[_safe_float] Could not convert value '{value}' (type: {type(value)}) to float.")
             return None


    def get_live_quote_details(self, symbol: str) -> dict:
        """
        Pobiera dane live z POPRAWIONĄ LOGIKĄ FALLBACKU dla otwartego rynku.
        Używa pola 'close' z CSV jako źródła latest_trade price.
        """
        logger.info(f"[DIAG] Rozpoczynanie get_live_quote_details dla {symbol}")
        us_market_status = "unknown"
        # --- Pobieranie Statusu Rynku ---
        try:
            status_data = self.get_market_status() # Używa _make_request
            if status_data and status_data.get('markets'):
                us_market = next((m for m in status_data['markets'] if m.get('region') == 'United States'), None)
                if us_market: us_market_status = us_market.get('current_status', 'unknown').lower()
                logger.info(f"[DIAG] Ustalony status rynku USA dla {symbol}: {us_market_status}")
            else:
                logger.warning(f"[DIAG] Brak 'markets' w odpowiedzi statusu dla {symbol}. Odpowiedź: {status_data}")
        except Exception as e:
            # Błędy w get_market_status są już logowane w _make_request
            logger.error(f"[DIAG] Nie udało się ustalić statusu rynku dla {symbol}: {e}", exc_info=False)
            us_market_status = "unknown" # Ustawiamy na unknown w razie błędu

        # --- Etap 1: Próba pobrania danych z BULK_QUOTES (CSV) ---
        raw_data_csv = self.get_bulk_quotes([symbol]) # Używa _make_request i _parse_bulk_quotes_csv
        determined_live_price = None # Zmienna na ostateczną cenę
        trigger_fallback = False # Flaga do uruchomienia fallbacku
        response = { # Szkielet odpowiedzi
            "symbol": symbol, "market_status": us_market_status,
            "regular_session": {}, "extended_session": {}, "live_price": None
        }

        # --- Zmienne na ceny z CSV (inicjowane jako None) ---
        regular_close_price_csv = None
        extended_price_csv = None
        latest_trade_price_csv = None # Cena z pola 'close' w CSV

        # --- Przetwarzanie danych z CSV (jeśli dostępne) ---
        if raw_data_csv and symbol in raw_data_csv:
            ticker_data = raw_data_csv[symbol] # Słownik z danymi dla tickera
            logger.debug(f"[DIAG] Surowe dane dla {symbol} z CSV po normalizacji: {ticker_data}")

            # Odczytujemy ceny z poprawnymi kluczami, używając _safe_float
            regular_close_price_csv = self._safe_float(ticker_data.get('previous_close'))
            extended_price_csv = self._safe_float(ticker_data.get('extended_hours_quote'))
            latest_trade_price_csv = self._safe_float(ticker_data.get('close')) # Używamy 'close'

            # Odczytujemy zmiany (niezależnie od cen)
            regular_change = self._safe_float(ticker_data.get('change'))
            regular_change_percent = self._safe_float(ticker_data.get('change_percent'))
            extended_change = self._safe_float(ticker_data.get('extended_hours_change'))
            extended_change_percent = self._safe_float(ticker_data.get('extended_hours_change_percent'))

            logger.info(f"[DIAG-CSV] {symbol} - Ceny po konwersji: regular_close={regular_close_price_csv}, extended_price={extended_price_csv}, latest_trade={latest_trade_price_csv}")

            # Wypełnienie sesji w odpowiedzi danymi z CSV (nawet jeśli ceny są None)
            response["regular_session"] = {"price": regular_close_price_csv, "change": regular_change, "change_percent": regular_change_percent}
            response["extended_session"] = {"price": extended_price_csv, "change": extended_change, "change_percent": extended_change_percent}

            # --- Logika Wyboru Ceny na Podstawie Danych CSV i Statusu Rynku ---
            # (Używa cen * _csv odczytanych wyżej)
            if us_market_status in ["pre-market", "post-market"]:
                if extended_price_csv is not None:
                    determined_live_price = extended_price_csv
                    logger.info(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Użyto ceny extended z CSV: {determined_live_price}")
                else:
                    logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Brak extended_price z CSV. Uruchamiam fallback.")
                    trigger_fallback = True

            elif us_market_status == "regular" or us_market_status == "open":
                if latest_trade_price_csv is not None:
                    determined_live_price = latest_trade_price_csv
                    logger.info(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Użyto ceny latest trade ('close') z CSV: {determined_live_price}")
                else:
                    logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Brak latest_trade_price ('close') z CSV. Uruchamiam fallback.")
                    trigger_fallback = True

            elif us_market_status == "closed":
                 determined_live_price = extended_price_csv if extended_price_csv is not None else regular_close_price_csv
                 log_source = "extended (CSV)" if extended_price_csv is not None else "regular_close (CSV)"
                 logger.info(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Użyto ceny closed ({log_source}): {determined_live_price}")
                 if determined_live_price is None:
                      logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Brak ceny extended i regular_close z CSV. Uruchamiam fallback.")
                      trigger_fallback = True

            else: # unknown status
                 logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Nieznany status rynku. Próba ustalenia ceny z CSV, potem fallback.")
                 determined_live_price = latest_trade_price_csv if latest_trade_price_csv is not None else \
                                          extended_price_csv if extended_price_csv is not None else \
                                          regular_close_price_csv
                 if determined_live_price is None:
                      logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Brak jakiejkolwiek ceny z CSV. Uruchamiam fallback.")
                      trigger_fallback = True
                 else:
                      logger.info(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Użyto ceny fallback z CSV: {determined_live_price}")

            # Jeśli cena została ustalona z CSV, przypisz ją do odpowiedzi
            if determined_live_price is not None:
                response["live_price"] = determined_live_price

        else: # Jeśli w ogóle nie było danych CSV dla tickera
            logger.error(f"[DIAG-CSV] Brak danych bulk quotes (CSV) dla {symbol} po parsowaniu.")
            trigger_fallback = True # Musimy spróbować fallbacku

        # --- Etap 2: Fallback do GLOBAL_QUOTE (JSON), jeśli trigger_fallback jest True ---
        if trigger_fallback:
            logger.warning(f"[DIAG-FALLBACK] Uruchamianie fallbacku do GLOBAL_QUOTE dla {symbol}...")
            global_quote_data = self.get_global_quote_json(symbol) # Używa _make_request(is_fallback=True)

            if global_quote_data:
                 logger.info(f"[DIAG-FALLBACK] Odpowiedź z GLOBAL_QUOTE dla {symbol}: {json.dumps(global_quote_data)}")
            # else: Błąd pobrania już zalogowany w _make_request

            if global_quote_data and "Global Quote" in global_quote_data:
                quote = global_quote_data["Global Quote"]
                fallback_price = self._safe_float(quote.get('05. price'))

                if fallback_price is not None:
                    # Jeśli cena z CSV była None LUB jesteśmy w sesji regularnej/open a CSV nie dało ceny live
                    # (drugi warunek jest ważny, żeby nie nadpisać ceny extended z CSV ceną z GLOBAL_QUOTE)
                    if response["live_price"] is None or (us_market_status in ["regular", "open"] and latest_trade_price_csv is None):
                         response["live_price"] = fallback_price
                         logger.info(f"[DIAG-FALLBACK] {symbol} - Użyto ceny fallback z GLOBAL_QUOTE: {fallback_price}")
                    else:
                         logger.warning(f"[DIAG-FALLBACK] {symbol} - Cena została już ustalona z CSV ({response['live_price']}), ignoruję fallback_price ({fallback_price}).")

                    # Uzupełnianie danych sesji z GLOBAL_QUOTE, jeśli brakowało ich w CSV
                    if response["regular_session"].get("price") is None: response["regular_session"]["price"] = self._safe_float(quote.get('08. previous close'))
                    if response["regular_session"].get("change") is None: response["regular_session"]["change"] = self._safe_float(quote.get('09. change'))
                    if response["regular_session"].get("change_percent") is None:
                         change_percent_str = quote.get('10. change percent', '').replace('%', '')
                         response["regular_session"]["change_percent"] = self._safe_float(change_percent_str)
                else:
                    logger.error(f"[DIAG-FALLBACK] {symbol} - GLOBAL_QUOTE nie zwrócił poprawnej ceny ('05. price' był: {quote.get('05. price')}).")
            # else: Błąd formatu odpowiedzi już zalogowany w _make_request

        # --- Ostateczność: Jeśli nadal nie ma ceny live, użyj previous close z CSV (jeśli jest) ---
        if response["live_price"] is None and regular_close_price_csv is not None:
             response["live_price"] = regular_close_price_csv
             logger.error(f"[DIAG-FINAL-FALLBACK] {symbol} - Brak ceny live z CSV i GLOBAL_QUOTE. Użyto ceny previous_close z CSV: {regular_close_price_csv} jako ostateczność.")


        # --- Ostatnie logowanie przed zwróceniem ---
        if response["live_price"] is None:
             logger.error(f"[DIAG-FINAL] {symbol} - Końcowa wartość live_price to NADAL None po CSV, fallbacku i ostateczności! Zwracany obiekt: {response}")
        else:
            # Używamy formatowania f-string dla ceny
            logger.info(f"[DIAG-FINAL] {symbol} - Zakończono get_live_quote_details. Finalna live_price: {response['live_price']:.4f}. Status: {us_market_status}") # Dodano formatowanie

        return response
