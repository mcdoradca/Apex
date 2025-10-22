import time
import requests
import logging
# ... existing code ...
import os
from dotenv import load_dotenv
# NOWE IMPORTY
from io import StringIO
import csv

load_dotenv()

# ... existing code ...
        self.request_timestamps = deque()

    def _rate_limiter(self):
# ... existing code ...
                time.sleep(self.request_interval - time_since_last)
        self.request_timestamps.append(time.monotonic())

    def _make_request(self, params: dict):
# ... existing code ...
                    return None
                return data
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
# ... existing code ...
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    def get_market_status(self):
# ... existing code ...
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    # NOWA: Prywatna funkcja do parsowania CSV (taka sama jak w workerze)
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
                'close': row.get('close'),
                'volume': row.get('volume'),
                'change_percent': row.get('change_percent'),
                'change': row.get('change'),
                'previous close': row.get('previous close'),
                'extended_hours_price': row.get('extended_hours_price'),
                'extended_hours_change': row.get('extended_hours_change'),
                'extended_hours_change_percent': row.get('extended_hours_change_percent')
            }
        return data_dict

    def get_bulk_quotes(self, symbols: list[str]):
        # Ta funkcja jest teraz potrzebna do zasilania get_live_quote_details
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
                # ZMIANA: Zwracamy przetworzony słownik
                return self._parse_bulk_quotes_csv(text_response)
            except requests.exceptions.RequestException as e:
                logger.error(f"Bulk quotes request failed (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    def get_company_overview(self, symbol: str):
# ... existing code ...
# ... existing code ...
    @staticmethod
    def _safe_float(value) -> float | None:
        if value is None: return None
# ... existing code ...
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
            return float(value)
        except (ValueError, TypeError):
            return None
    
    # USUNIĘTA: Funkcja _get_latest_intraday_price

    # ZASTĄPIONA: Funkcja get_global_quote została zastąpiona przez get_live_quote_details
    def get_live_quote_details(self, symbol: str) -> dict:
        """
        Pobiera pełne dane "live" (REALTIME_BULK_QUOTES) oraz status rynku,
        zwracając ustandaryzowany słownik w stylu Yahoo Finance.
        """
        # 1. Pobierz Status Rynku
        us_market_status = "closed" # Bezpieczny domyślny
        try:
            status_data = self.get_market_status()
            if status_data and status_data.get('markets'):
                us_market = next((m for m in status_data['markets'] if m.get('region') == 'United States'), None)
                if us_market:
                    us_market_status = us_market.get('current_status', 'closed').lower()
        except Exception as e:
            logger.warning(f"Nie można pobrać statusu rynku dla {symbol}: {e}. Przyjęto 'closed'.")

        # 2. Pobierz Dane Czasu Rzeczywistego
        raw_data = self.get_bulk_quotes([symbol])
        
        if not raw_data or symbol not in raw_data:
            logger.error(f"Brak danych live (REALTIME_BULK_QUOTES) dla {symbol}")
            return {
                "symbol": symbol, "market_status": us_market_status,
                "regular_session": {}, "extended_session": {}, "live_price": None
            }
            
        ticker_data = raw_data[symbol]

        # 3. Zbuduj nowy, bogaty obiekt odpowiedzi
        regular_close_price = self._safe_float(ticker_data.get('close'))
        
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
        
        # 4. Ustalenie "live_price"
        if us_market_status in ["pre-market", "post-market"] and response["extended_session"]["price"] is not None:
             response["live_price"] = response["extended_session"]["price"]
        elif us_market_status == "regular":
             response["live_price"] = self._safe_float(ticker_data.get('price'))
        elif us_market_status == "closed":
             response["live_price"] = response["extended_session"]["price"] or response["regular_session"]["price"]
        
        return response
