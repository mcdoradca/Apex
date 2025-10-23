import time
import requests
import logging
import json
from collections import deque
import os
from dotenv import load_dotenv
from io import StringIO
import csv
# POPRAWKA BŁĘDU #3: Dodanie importów do obsługi czasu
from datetime import datetime, time as dt_time # Dodano import time
import pytz
from typing import Dict, Any, List, Tuple 

load_dotenv()

logger = logging.getLogger(__name__)

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    logger.warning("ALPHAVANTAGE_API_KEY not found in environment for Worker's client.")


class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str = API_KEY, requests_per_minute: int = 150, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            logger.warning("API key is missing or empty for AlphaVantageClient instance in Worker.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        # Dostosowanie limitu, aby zostawić margines (np. 140 zamiast 150)
        self.requests_per_minute = requests_per_minute - 10
        self.request_interval = 60.0 / self.requests_per_minute if self.requests_per_minute > 0 else 0.5 # Minimalny odstęp
        self.request_timestamps = deque()

    def _rate_limiter(self):
        """Implementuje mechanizm ograniczania zapytań API."""
        if not self.api_key:
             logger.warning("Rate limiting skipped: API key is missing.")
             return
        now = time.monotonic()
        # Usuń stare znaczniki czasu (starsze niż 60 sekund)
        while self.request_timestamps and (now - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()
        # Sprawdź, czy osiągnięto limit
        if len(self.request_timestamps) >= self.requests_per_minute:
            time_since_oldest = now - self.request_timestamps[0]
            time_to_wait = 60.1 - time_since_oldest # Dodano mały margines
            if time_to_wait > 0:
                logger.warning(f"Rate limit approx. reached ({len(self.request_timestamps)} reqs). Sleeping for {time_to_wait:.2f} seconds.")
                time.sleep(time_to_wait)
                # Po odczekaniu, zaktualizuj 'now' i ponownie usuń stare znaczniki
                now = time.monotonic()
                while self.request_timestamps and (now - self.request_timestamps[0] > 60):
                     self.request_timestamps.popleft()
        # Sprawdź minimalny odstęp między zapytaniami
        if self.request_timestamps:
            time_since_last = now - self.request_timestamps[-1]
            if time_since_last < self.request_interval:
                 sleep_duration = self.request_interval - time_since_last
                 logger.debug(f"Throttling request. Sleeping for {sleep_duration:.3f} seconds.")
                 time.sleep(sleep_duration)
        # Dodaj nowy znacznik czasu
        self.request_timestamps.append(time.monotonic())


    def _make_request(self, params: dict, is_fallback: bool = False):
        """Wykonuje zapytanie do API Alpha Vantage z obsługą błędów i ponowień."""
        if not self.api_key:
            log_func = logger.warning if is_fallback else logger.error
            log_func("Cannot make Alpha Vantage request: API key is missing.")
            return None

        if not is_fallback:
            self._rate_limiter()

        params['apikey'] = self.api_key
        request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
        max_retries = 1 if is_fallback else self.retries

        for attempt in range(max_retries):
            try:
                log_level = logging.INFO if is_fallback else logging.DEBUG
                logger.log(log_level, f"Making AV request for {request_identifier} (Attempt {attempt+1}/{max_retries}). Function: {params.get('function')}")

                response = requests.get(self.BASE_URL, params=params, timeout=15 if is_fallback else 30)
                response.raise_for_status()

                # --- Obsługa CSV ---
                if params.get('datatype') == 'csv':
                    text_response = response.text
                    if not text_response or text_response.strip().startswith('<'):
                         logger.error(f"Alpha Vantage API returned empty or non-CSV response for {request_identifier}. Response: {text_response[:200]}")
                         return None
                    if "Error Message" in text_response or "Invalid API call" in text_response:
                        logger.error(f"Alpha Vantage API returned an error (CSV): {text_response[:200]}")
                        if "premium" in text_response.lower() and not is_fallback:
                            logger.error(f"CSV API call for {request_identifier} failed due to premium limit. Waiting longer.")
                            time.sleep(20)
                        return None
                    return text_response

                # --- Obsługa JSON ---
                data = response.json()
                if not data or "Error Message" in data or "Information" in data:
                    # Sprawdzenie komunikatu o limicie zapytań
                    info_msg = str(data.get("Information", "")).lower()
                    if "premium" in info_msg or "call frequency" in info_msg:
                         logger.error(f"API call limit reached for {request_identifier}: {data.get('Information')}. Waiting significantly longer.")
                         time.sleep(60) # Czekamy minutę przy problemach z limitem
                         # Kontynuujemy pętlę ponowień po odczekaniu
                         continue
                    else:
                        # Inny błąd lub puste dane
                        log_func = logger.warning if is_fallback else logger.error
                        log_func(f"API returned an error or empty data for {request_identifier}: {data}")
                        return None # Zwracamy None przy innych błędach

                return data

            except requests.exceptions.HTTPError as http_err:
                 log_func = logger.warning if is_fallback else logger.error
                 log_func(f"HTTP error occurred for {request_identifier} (Attempt {attempt + 1}/{max_retries}): {http_err} - Status: {http_err.response.status_code}")
                 # Dodatkowa obsługa 429 (Too Many Requests) - chociaż AV rzadko go używa
                 if http_err.response.status_code == 429:
                     logger.warning(f"Received HTTP 429 (Too Many Requests) for {request_identifier}. Waiting...")
                     time.sleep(15) # Dodatkowe czekanie

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                 log_func = logger.warning if is_fallback else logger.error
                 log_func(f"Request failed for {request_identifier} (attempt {attempt + 1}/{max_retries}): {e}")

            if attempt < max_retries - 1:
                sleep_time = self.backoff_factor * (2 ** attempt)
                logger.info(f"Retrying request for {request_identifier} in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

        logger.error(f"Request failed for {request_identifier} after {max_retries} attempts.")
        return None

    def _parse_bulk_quotes_csv(self, csv_text: str) -> dict:
        """Przetwarza odpowiedź CSV z BULK_QUOTES na słownik danych."""
        if not csv_text or "symbol" not in csv_text.lower():
            logger.warning("[CSV PARSER] Otrzymane dane CSV są puste lub nie zawierają nagłówka 'symbol'. Treść: %s", csv_text[:200])
            return {}

        csv_file = StringIO(csv_text)
        try:
            reader = csv.DictReader(csv_file)
            if not reader.fieldnames:
                 logger.warning("[CSV PARSER] CSV nie zawiera nagłówków.")
                 return {}

            normalized_fieldnames = [name.lower().strip().replace(' ', '_') for name in reader.fieldnames]
            logger.debug(f"[CSV PARSER] Normalized headers: {normalized_fieldnames}")
            reader.fieldnames = normalized_fieldnames

            data_dict = {}
            row_count = 0
            for row in reader:
                row_count += 1
                ticker = row.get('symbol')
                if not ticker:
                    logger.warning(f"[CSV PARSER] Row {row_count} has no 'symbol'. Skipping row: {row}")
                    continue

                # Klucze, których *oczekujemy* po normalizacji
                expected_keys = ['symbol', 'price', 'previous_close', 'extended_hours_quote', 'close', 'change', 'change_percent', 'extended_hours_change', 'extended_hours_change_percent', 'latest_trading_day']
                missing_keys_in_row = [key for key in expected_keys if row.get(key) is None or str(row.get(key)).strip() == ""]

                missing_prices = [k for k in ['price', 'previous_close', 'extended_hours_quote', 'close'] if k in missing_keys_in_row]
                if missing_prices:
                    logger.debug(f"[CSV PARSER] Ticker {ticker} - Row {row_count} has missing/empty PRICE values for keys: {missing_prices}. Row content: {row}")

                data_dict[ticker] = row

            if not data_dict:
                 logger.warning("[CSV PARSER] Parsowanie CSV zakończone, ale nie znaleziono żadnych danych tickerów.")
            return data_dict
        except csv.Error as csv_err:
             logger.error(f"[CSV PARSER] Błąd podczas parsowania CSV: {csv_err}. Treść CSV (początek): {csv_text[:500]}")
             return {}
        except Exception as e:
             logger.error(f"[CSV PARSER] Nieoczekiwany błąd podczas parsowania CSV: {e}. Treść CSV (początek): {csv_text[:500]}", exc_info=True)
             return {}

    def get_bulk_quotes(self, symbols: list[str]):
        """Pobiera dane BULK_QUOTES w formacie CSV i parsuje je."""
        if not self.api_key:
            logger.error("Cannot get bulk quotes: API key is missing.")
            return None
        # Ogranicz liczbę symboli na zapytanie (np. do 100)
        chunk_size = 100
        all_parsed_data = {}
        for i in range(0, len(symbols), chunk_size):
             chunk = symbols[i:i + chunk_size]
             logger.debug(f"Fetching bulk quotes for chunk: {','.join(chunk)}")
             params = {
                 "function": "REALTIME_BULK_QUOTES",
                 "symbol": ",".join(chunk),
                 "datatype": "csv"
             }
             csv_text = self._make_request(params)
             if csv_text:
                 parsed_chunk = self._parse_bulk_quotes_csv(csv_text)
                 all_parsed_data.update(parsed_chunk)
             else:
                 logger.warning(f"Failed to fetch bulk quotes for chunk starting with {chunk[0]}.")
                 # Można dodać logikę ponowienia dla chunka, ale na razie pomijamy
        return all_parsed_data


    def get_global_quote_json(self, symbol: str):
        """Pobiera dane GLOBAL_QUOTE w formacie JSON (używane jako fallback)."""
        if not self.api_key:
            logger.warning("Cannot get global quote (fallback): API key is missing.")
            return None
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol}
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
        # Dodano walidację interwału dla RSI
        valid_rsi_intervals = ['1min', '5min', '15min', '30min', '60min', 'daily', 'weekly', 'monthly']
        if interval not in valid_rsi_intervals:
            logger.error(f"Invalid interval '{interval}' for RSI. Using 'daily'.")
            interval = 'daily'
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
        if isinstance(value, (int, float)): return float(value)
        if isinstance(value, str):
            cleaned_value = value.strip().replace(',', '').replace('%', '')
            if not cleaned_value or cleaned_value.lower() in ['n/a', 'none', '-']: return None
            try: return float(cleaned_value)
            except (ValueError, TypeError):
                logger.debug(f"[_safe_float] Could not convert cleaned string '{cleaned_value}' to float.")
                return None
        try: return float(value)
        except (ValueError, TypeError):
             logger.debug(f"[_safe_float] Could not convert value '{value}' (type: {type(value)}) to float.")
             return None


    # === POPRAWKA BŁĘDU #3: Przepisanie funkcji get_live_quote_details ===
    # Rezygnujemy z endpointu MARKET_STATUS na rzecz ręcznego sprawdzania czasu.
    def get_live_quote_details(self, symbol: str) -> dict:
        """
        Pobiera dane live i SAMODZIELNIE określa status rynku na podstawie
        aktualnego czasu w strefie US/Eastern.
        Zwraca ujednolicony słownik z danymi.
        """
        logger.info(f"[DIAG] Rozpoczynanie get_live_quote_details dla {symbol}")

        # --- Ustalanie Statusu Rynku na podstawie czasu ---
        try:
            tz = pytz.timezone('US/Eastern')
            now_ny = datetime.now(tz)
            time_ny_str = now_ny.strftime('%H:%M:%S ET')
            date_ny_str = now_ny.strftime('%Y-%m-%d')
            # Używamy obiektu time dla precyzji
            current_time = now_ny.time()

            # Definicja zakresów czasowych (w ET)
            # 4:00 AM ET - 9:30 AM ET (Pre-Market)
            PRE_MARKET_START = dt_time(4, 0)
            REGULAR_START = dt_time(9, 30)
            REGULAR_END = dt_time(16, 0)
            POST_MARKET_END = dt_time(20, 0)

            # Logika statusu
            if current_time >= PRE_MARKET_START and current_time < REGULAR_START:
                us_market_status = "PRE_MARKET"
            elif current_time >= REGULAR_START and current_time < REGULAR_END:
                us_market_status = "REGULAR"
            elif current_time >= REGULAR_END and current_time < POST_MARKET_END:
                 us_market_status = "POST_MARKET"
            else:
                us_market_status = "CLOSED"

            logger.info(f"[DIAG] Ustalony status rynku (wg czasu) dla {symbol}: {us_market_status} (Czas NY: {time_ny_str})")

        except Exception as e:
            logger.error(f"[DIAG] Nie udało się ustalić czasu NY: {e}", exc_info=False)
            us_market_status = "UNKNOWN"
            time_ny_str = "N/A"
            date_ny_str = "N/A"

        # --- Inicjalizacja odpowiedzi ---
        response = {
            "symbol": symbol,
            "market_status_internal": us_market_status, # Używamy _internal dla jasności
            "time_ny": time_ny_str,
            "date_ny": date_ny_str,
            "regular_session": {"price": None, "change": None, "change_percent": None},
            "extended_session": {"price": None, "change": None, "change_percent": None},
            "live_price": None,
            "actual_close_price": None # Użyjemy tego pola, aby przechować `close` z CSV
        }
        trigger_fallback = False # Czy potrzebujemy fallbacku do JSON?

        # --- Etap 1: Próba pobrania danych z BULK_QUOTES (CSV) ---
        raw_data_csv = self.get_bulk_quotes([symbol])

        if raw_data_csv and symbol in raw_data_csv:
            ticker_data = raw_data_csv[symbol]
            logger.debug(f"[DIAG] Surowe dane dla {symbol} z CSV po normalizacji: {ticker_data}")

            # Odczytujemy wszystkie potrzebne pola z CSV
            regular_close_price_csv = self._safe_float(ticker_data.get('previous_close')) # Poprzednie zamknięcie (1.71)
            extended_price_csv = self._safe_float(ticker_data.get('extended_hours_quote')) # Cena extended (1.79)
            latest_trade_price_csv = self._safe_float(ticker_data.get('close')) # Faktyczna cena zamknięcia (1.76)
            
            response["actual_close_price"] = latest_trade_price_csv # Zapisujemy faktyczną cenę zamknięcia
            
            regular_change_csv = self._safe_float(ticker_data.get('change'))
            regular_change_percent_csv = self._safe_float(ticker_data.get('change_percent'))
            extended_change_csv = self._safe_float(ticker_data.get('extended_hours_change'))
            extended_change_percent_csv = self._safe_float(ticker_data.get('extended_hours_change_percent'))
            latest_trading_day_csv = ticker_data.get('latest_trading_day') # Zachowujemy jako string

            logger.info(f"[DIAG-CSV] {symbol} - Ceny po konwersji: previous_close={regular_close_price_csv}, extended={extended_price_csv}, actual_close={latest_trade_price_csv}")

            # === POCZĄTEK POPRAWKI BŁĘDU (Cena "At Close") ===
            # Wypełniamy pola odpowiedzi danymi z CSV
            # Używamy 'latest_trade_price_csv' (1.76) jako 'price' dla sesji regularnej,
            # ponieważ to jest cena "At Close", którą chcemy widzieć.
            response["regular_session"] = {
                "price": latest_trade_price_csv, # POPRAWKA: Używamy 'close' (1.76)
                "change": regular_change_csv, 
                "change_percent": regular_change_percent_csv
            }
            # 'previous_close' (1.71) nie jest już bezpośrednio używane w 'regular_session.price'
            # === KONIEC POPRAWKI BŁĘDU ===
            
            response["extended_session"] = {"price": extended_price_csv, "change": extended_change_csv, "change_percent": extended_change_percent_csv}

            # --- Logika Wyboru Ceny Live na podstawie NASZEGO Statusu Rynku ---
            if us_market_status in ["PRE_MARKET", "POST_MARKET"]:
                if extended_price_csv is not None:
                    response["live_price"] = extended_price_csv
                    logger.info(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Użyto ceny extended z CSV: {response['live_price']}")
                else:
                    logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Brak extended_price z CSV. Uruchamiam fallback.")
                    trigger_fallback = True

            elif us_market_status == "REGULAR":
                # Używamy `latest_trade_price_csv` (pole 'close' z CSV), ponieważ w trakcie sesji
                # jest ono aktualizowane na żywo i jest dokładniejsze niż `price` z GLOBAL_QUOTE.
                if latest_trade_price_csv is not None:
                    response["live_price"] = latest_trade_price_csv
                    logger.info(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Użyto ceny latest trade ('close') z CSV: {response['live_price']}")
                else:
                    logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Brak latest_trade_price ('close') z CSV. Uruchamiam fallback.")
                    trigger_fallback = True

            elif us_market_status == "CLOSED":
                 # Dla CLOSED priorytet ma extended, potem faktyczne zamknięcie
                 final_price = extended_price_csv if extended_price_csv is not None else latest_trade_price_csv
                 response["live_price"] = final_price
                 log_source = "extended (CSV)" if extended_price_csv is not None else "actual_close (CSV)"
                 logger.info(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Użyto ceny closed ({log_source}): {response['live_price']}")
                 if response["live_price"] is None:
                      logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Brak ceny extended i actual_close z CSV. Uruchamiam fallback.")
                      trigger_fallback = True
            else: # UNKNOWN status
                 logger.warning(f"[DIAG-CSV] {symbol} (Status: {us_market_status}) - Nieznany status rynku (błąd czasu?). Uruchamiam fallback.")
                 trigger_fallback = True

        else: # Jeśli w ogóle nie było danych CSV dla tickera
            logger.error(f"[DIAG-CSV] Brak danych bulk quotes (CSV) dla {symbol} po parsowaniu lub zapytanie API nie powiodło się.")
            trigger_fallback = True # Musimy spróbować fallbacku

        # --- Etap 2: Fallback do GLOBAL_QUOTE (JSON), jeśli potrzebny ---
        if trigger_fallback:
            logger.warning(f"[DIAG-FALLBACK] Uruchamianie fallbacku do GLOBAL_QUOTE dla {symbol}...")
            global_quote_data = self.get_global_quote_json(symbol)

            if global_quote_data and "Global Quote" in global_quote_data:
                 quote = global_quote_data["Global Quote"]
                 logger.info(f"[DIAG-FALLBACK] Odpowiedź z GLOBAL_QUOTE dla {symbol}: {json.dumps(quote)}")

                 fallback_price = self._safe_float(quote.get('05. price'))
                 fallback_prev_close = self._safe_float(quote.get('08. previous close'))
                 fallback_change = self._safe_float(quote.get('09. change'))
                 fallback_change_percent_str = quote.get('10. change percent', '').replace('%', '')
                 fallback_change_percent = self._safe_float(fallback_change_percent_str)
                 fallback_latest_day = quote.get('07. latest trading day') # String YYYY-MM-DD

                 # Używamy ceny z fallbacku tylko jeśli cena live jest nadal None
                 if response["live_price"] is None and fallback_price is not None:
                      if us_market_status == "REGULAR" and fallback_latest_day != date_ny_str:
                          logger.warning(f"[DIAG-FALLBACK] {symbol} - Cena z GLOBAL_QUOTE ({fallback_price}) pochodzi z {fallback_latest_day}, a nie z dzisiaj ({date_ny_str}). Może być nieaktualna.")
                      response["live_price"] = fallback_price
                      logger.info(f"[DIAG-FALLBACK] {symbol} - Użyto ceny fallback z GLOBAL_QUOTE: {fallback_price}")

                 # === POCZĄTEK POPRAWKI BŁĘDU (Fallback "At Close") ===
                 # Uzupełniamy dane sesji regularnej, jeśli brakowało ich w CSV lub CSV zawiodło
                 if response["regular_session"]["price"] is None: 
                     response["regular_session"]["price"] = fallback_price # POPRAWKA: Używamy '05. price'
                 # === KONIEC POPRAWKI BŁĘDU ===
                 
                 if response["regular_session"]["change"] is None: response["regular_session"]["change"] = fallback_change
                 if response["regular_session"]["change_percent"] is None: response["regular_session"]["change_percent"] = fallback_change_percent
                 
                 if response["actual_close_price"] is None: response["actual_close_price"] = fallback_price

            else:
                 logger.error(f"[DIAG-FALLBACK] {symbol} - Nie otrzymano poprawnych danych z GLOBAL_QUOTE.")

        # --- Ostateczność: Jeśli nadal nie ma ceny live, użyj previous close (z CSV lub fallbacku) ---
        if response["live_price"] is None:
            # === POPRAWKA BŁĘDU: Używamy ceny z 'regular_session' (która jest teraz poprawna) ===
            final_fallback_price = response["regular_session"].get("price") # price to teraz actual close
            if final_fallback_price is not None:
                 response["live_price"] = final_fallback_price
                 logger.error(f"[DIAG-FINAL-FALLBACK] {symbol} - Brak ceny live z CSV/JSON. Użyto ceny 'actual_close': {final_fallback_price} jako ostateczność.")
            else:
                 logger.error(f"[DIAG-FINAL-FALLBACK] {symbol} - Brak jakiejkolwiek ceny (live, actual_close) do użycia!")

        # --- Końcowe logowanie ---
        if response["live_price"] is None:
             logger.error(f"[DIAG-FINAL] {symbol} - Końcowa wartość live_price to NADAL None! Zwracany obiekt: {response}")
        else:
            logger.info(f"[DIAG-FINAL] {symbol} - Zakończono get_live_quote_details. Finalna live_price: {response['live_price']:.4f}. Status: {us_market_status}")

        return response
