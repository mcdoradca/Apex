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
        request_identifier = params.get('symbol') or params.get('tickers') or params.get('function') # Definiujemy wcześniej
        for attempt in range(self.retries):
            try:
                logger.info(f"Making AV request for {request_identifier} (Attempt {attempt+1}/{self.retries}). Function: {params.get('function')}")
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()

                # Obsługa CSV - musimy sprawdzić `.text`, a nie `.json()`
                if params.get('datatype') == 'csv':
                    text_response = response.text
                    if "Error Message" in text_response or "Invalid API call" in text_response:
                        logger.error(f"Alpha Vantage API returned an error (CSV): {text_response[:200]}")
                        return None
                    return text_response # Zwracamy tekst CSV do dalszego parsowania

                # Obsługa JSON
                data = response.json()
                if not data or "Error Message" in data or "Information" in data:
                    logger.warning(f"API returned an error or empty data for {request_identifier}: {data}")
                    if "premium" in str(data).lower():
                         logger.error(f"API call for {request_identifier} failed due to premium limit. Waiting longer.")
                         time.sleep(20)
                    return None
                return data
            except requests.exceptions.HTTPError as http_err:
                 logger.error(f"HTTP error occurred for {request_identifier} (Attempt {attempt + 1}/{self.retries}): {http_err} - Status: {http_err.response.status_code}")
                 # Specjalna obsługa 429 lub 5xx? Na razie tylko logujemy.
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.error(f"Request failed for {request_identifier} (attempt {attempt + 1}/{self.retries}): {e}")

            if attempt < self.retries - 1:
                sleep_time = self.backoff_factor * (2 ** attempt)
                logger.info(f"Retrying request for {request_identifier} in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        logger.error(f"Request failed for {request_identifier} after {self.retries} attempts.")
        return None


    def get_market_status(self):
        """Pobiera aktualny status rynku z dedykowanego endpointu."""
        params = {"function": "MARKET_STATUS"}
        # Używamy _make_request, który teraz zwraca JSON
        return self._make_request(params)

    def _parse_bulk_quotes_csv(self, csv_text: str) -> dict:
        """Przetwarza odpowiedź CSV z BULK_QUOTES na słownik danych."""
        if not csv_text or "symbol" not in csv_text.lower(): # Sprawdzamy nagłówek bez względu na wielkość liter
            logger.warning("[CSV PARSER] Otrzymane dane CSV są puste lub nie zawierają nagłówka 'symbol'. Treść: %s", csv_text[:200])
            return {}

        csv_file = StringIO(csv_text)
        # --- DODANO OBSŁUGĘ POTENCJALNYCH BŁĘDÓW CSV ---
        try:
            # Używamy `DictReader` dla łatwiejszego dostępu po nazwie kolumny
            reader = csv.DictReader(csv_file)
            # Sprawdzamy, czy nagłówki istnieją
            if not reader.fieldnames:
                 logger.warning("[CSV PARSER] CSV nie zawiera nagłówków.")
                 return {}
            # Normalizujemy nazwy nagłówków (małe litery, usuwamy spacje) dla spójności
            reader.fieldnames = [name.lower().strip() for name in reader.fieldnames]

            data_dict = {}
            for row in reader:
                ticker = row.get('symbol')
                if not ticker:
                    continue

                # Logujemy surowy wiersz dla diagnostyki, jeśli potrzebne
                # logger.debug(f"[CSV PARSER] Processing row for {ticker}: {row}")

                # Mapujemy znormalizowane nazwy nagłówków CSV na klucze słownika
                # Używamy .get() z domyślną wartością None, aby uniknąć KeyError
                data_dict[ticker] = {
                    'price': row.get('price'), # 'price' to 'latest trade' w CSV
                    'close': row.get('close'), # To pole jest mylące w CSV
                    'volume': row.get('volume'),
                    'change_percent': row.get('change_percent'),
                    'change': row.get('change'),
                    'previous close': row.get('previous close'), # Prawidłowe pole dla zamknięcia z poprzedniego dnia
                    'extended_hours_price': row.get('extended_hours_price'), # Sprawdzamy znormalizowaną nazwę
                    'extended_hours_change': row.get('extended_hours_change'),
                    'extended_hours_change_percent': row.get('extended_hours_change_percent')
                }
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
        """Pobiera dane bulk quotes jako tekst CSV."""
        if not self.api_key: return None
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
            "datatype": "csv" # Zawsze prosimy o CSV
        }
        # Używamy _make_request, który teraz zwróci tekst CSV lub None
        csv_text = self._make_request(params)
        if csv_text is None:
             logger.error(f"Nie udało się pobrać danych bulk quotes (CSV) dla: {','.join(symbols)}")
             return None
        # Parsujemy CSV dopiero tutaj
        return self._parse_bulk_quotes_csv(csv_text)


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
            # Obsługa pustych stringów
            if isinstance(value, str) and value.strip() == "":
                return None
            # Standardowe czyszczenie
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
            return float(value)
        except (ValueError, TypeError):
            logger.debug(f"[_safe_float] Could not convert '{value}' (type: {type(value)}) to float.")
            return None

    def get_live_quote_details(self, symbol: str) -> dict:
        """
        Pobiera pełne dane "live" (REALTIME_BULK_QUOTES) oraz status rynku,
        zwracając ustandaryzowany słownik w stylu Yahoo Finance.
        Dodano szczegółowe logowanie diagnostyczne.
        """
        logger.info(f"[DIAG] Rozpoczynanie get_live_quote_details dla {symbol}")
        us_market_status = "unknown" # Zmieniono domyślny na unknown dla lepszej diagnostyki
        try:
            status_data = self.get_market_status()
            logger.debug(f"[DIAG] Otrzymano status rynku dla {symbol}: {status_data}")
            if status_data and status_data.get('markets'):
                us_market = next((m for m in status_data['markets'] if m.get('region') == 'United States'), None)
                if us_market:
                    us_market_status = us_market.get('current_status', 'unknown').lower()
                    logger.info(f"[DIAG] Ustalony status rynku USA dla {symbol}: {us_market_status}")
                else:
                     logger.warning(f"[DIAG] Nie znaleziono rynku 'United States' w odpowiedzi statusu dla {symbol}.")
            else:
                 logger.warning(f"[DIAG] Brak klucza 'markets' lub pusta odpowiedź statusu dla {symbol}.")
        except Exception as e:
            logger.error(f"[DIAG] Błąd podczas pobierania statusu rynku dla {symbol}: {e}", exc_info=True)
            # Pozostawiamy us_market_status jako "unknown"

        # --- Pobieranie i parsowanie danych Bulk Quotes ---
        raw_data = self.get_bulk_quotes([symbol]) # Zwraca sparsowany słownik lub None
        logger.debug(f"[DIAG] Otrzymano sparsowane dane bulk quotes dla {symbol}: {raw_data}")

        if not raw_data or symbol not in raw_data:
            logger.error(f"[DIAG] Brak danych bulk quotes dla {symbol} po parsowaniu.")
            # Zwracamy obiekt z informacją o błędzie, ale zgodny ze schematem
            return {
                "symbol": symbol, "market_status": us_market_status, # Przekazujemy status jaki udało się ustalić
                "regular_session": {}, "extended_session": {}, "live_price": None
            }

        ticker_data = raw_data[symbol]
        logger.debug(f"[DIAG] Dane dla tickera {symbol}: {ticker_data}")

        # --- Konwersja danych na floaty z użyciem _safe_float ---
        regular_close_price = self._safe_float(ticker_data.get('previous close'))
        regular_change = self._safe_float(ticker_data.get('change'))
        regular_change_percent = self._safe_float(ticker_data.get('change_percent'))
        extended_price = self._safe_float(ticker_data.get('extended_hours_price'))
        extended_change = self._safe_float(ticker_data.get('extended_hours_change'))
        extended_change_percent = self._safe_float(ticker_data.get('extended_hours_change_percent'))
        latest_trade_price = self._safe_float(ticker_data.get('price')) # Cena 'latest trade' z CSV

        logger.debug(f"[DIAG] {symbol} - Ceny po konwersji: regular_close={regular_close_price}, regular_change={regular_change}, regular_change_percent={regular_change_percent}, extended_price={extended_price}, extended_change={extended_change}, extended_change_percent={extended_change_percent}, latest_trade={latest_trade_price}")

        # --- Budowanie obiektu odpowiedzi ---
        response = {
            "symbol": symbol,
            "market_status": us_market_status,
            "regular_session": {
                "price": regular_close_price,
                "change": regular_change,
                "change_percent": regular_change_percent
            },
            "extended_session": {
                "price": extended_price,
                "change": extended_change,
                "change_percent": extended_change_percent
            },
            "live_price": None # Inicjalizujemy jako None
        }

        # --- Logika wyboru 'live_price' ---
        determined_live_price = None
        if us_market_status in ["pre-market", "post-market"] and extended_price is not None:
             determined_live_price = extended_price
             logger.info(f"[DIAG] {symbol} (Status: {us_market_status}) - Użyto ceny extended: {determined_live_price}")
        elif us_market_status == "regular" and latest_trade_price is not None:
             determined_live_price = latest_trade_price
             logger.info(f"[DIAG] {symbol} (Status: {us_market_status}) - Użyto ceny latest trade: {determined_live_price}")
        elif us_market_status == "closed":
             # Po zamknięciu preferujemy cenę extended, jeśli jest, inaczej cenę zamknięcia regularnej sesji
             determined_live_price = extended_price if extended_price is not None else regular_close_price
             logger.info(f"[DIAG] {symbol} (Status: {us_market_status}) - Użyto ceny closed (extended?: {extended_price is not None}): {determined_live_price}")
        else:
             # Fallback dla statusu 'unknown' lub innych nieprzewidzianych
             # Próbujemy użyć w kolejności: latest_trade, extended, regular_close
             determined_live_price = latest_trade_price if latest_trade_price is not None else \
                                      extended_price if extended_price is not None else \
                                      regular_close_price
             logger.warning(f"[DIAG] {symbol} (Status: {us_market_status}) - Użyto ceny fallback: {determined_live_price}")

        response["live_price"] = determined_live_price

        # --- Ostatnie logowanie przed zwróceniem ---
        if response["live_price"] is None:
             logger.error(f"[DIAG] {symbol} - Końcowa wartość live_price to nadal None! Zwracany obiekt: {response}")
        else:
            logger.info(f"[DIAG] {symbol} - Zakończono get_live_quote_details. Finalna live_price: {response['live_price']}. Status: {us_market_status}")

        return response
