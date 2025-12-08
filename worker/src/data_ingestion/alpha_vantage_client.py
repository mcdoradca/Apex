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

    # Limit 150 zapytań/minuta dla Premium
    def __init__(self, api_key: str = API_KEY, requests_per_minute: int = 150, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            logger.error("API key is missing for AlphaVantageClient instance in WORKER.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()
        self.session = requests.Session()

    def _rate_limiter(self):
        if not self.api_key: return
        now = time.monotonic()
        while self.request_timestamps and (now - self.request_timestamps[0] > 60):
            self.request_timestamps.popleft()
        if len(self.request_timestamps) >= self.requests_per_minute:
            time_to_wait = 60 - (now - self.request_timestamps[0]) + 0.1
            if time_to_wait > 0:
                time.sleep(time_to_wait)
                now = time.monotonic()
        if self.request_timestamps:
            time_since_last = now - self.request_timestamps[-1]
            if time_since_last < self.request_interval:
                time.sleep(self.request_interval - time_since_last)
        self.request_timestamps.append(time.monotonic())

    def _make_request(self, params: dict):
        if not self.api_key:
            return None
        request_params = params.copy()
        request_params['apikey'] = self.api_key
        request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
        
        for attempt in range(self.retries):
            self._rate_limiter()
            try:
                response = self.session.get(self.BASE_URL, params=request_params, timeout=30)
                
                # === KLUCZOWA NAPRAWA ===
                # Jeśli żądamy CSV, nie próbujemy parsować JSON, chyba że odpowiedź wygląda jak błąd JSON.
                if params.get('datatype') == 'csv':
                    content = response.text.strip()
                    # Jeśli zaczyna się od '{', to prawdopodobnie JSON z błędem
                    if content.startswith('{'):
                        try:
                            err_data = response.json()
                            info_text = err_data.get("Information", "").lower()
                            if "frequency" in info_text or "api call volume" in info_text:
                                wait_time = 5 * (attempt + 1)
                                logger.warning(f"API Rate Limit (CSV) for {request_identifier}. Wait {wait_time}s.")
                                time.sleep(wait_time)
                                continue
                            if "Error Message" in err_data:
                                logger.warning(f"API Error (CSV mode): {err_data}")
                                return None
                        except: pass
                    
                    response.raise_for_status()
                    return content

                # Standardowa ścieżka JSON
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    raise requests.exceptions.RequestException("Response was not valid JSON.")

                is_rate_limit = False
                if "Information" in data:
                    info = data["Information"].lower()
                    if "frequency" in info or "api call volume" in info:
                        is_rate_limit = True
                
                if is_rate_limit:
                    wait_time = 5 * (attempt + 1)
                    logger.warning(f"API Rate Limit (JSON) for {request_identifier}. Wait {wait_time}s.")
                    time.sleep(wait_time)
                    continue

                if "Error Message" in data:
                    logger.warning(f"API Error for {request_identifier}: {data}")
                    return None
                
                response.raise_for_status()
                return data

            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                if attempt < self.retries - 1:
                    time.sleep(1)
                else:
                    logger.error(f"Failed request for {request_identifier}: {e}")
        return None

    @staticmethod
    def _safe_float(value) -> float | None:
        if value is None: return None
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '').strip()
                if value == '-' or value == '': return None
            return float(value)
        except (ValueError, TypeError):
            return None
            
    def _parse_bulk_quotes_csv(self, csv_text: str, ticker: str) -> dict | None:
        if not csv_text or "symbol" not in csv_text.lower(): return None
        csv_file = StringIO(csv_text)
        reader = csv.DictReader(csv_file)
        if reader.fieldnames:
            reader.fieldnames = [name.strip().lower() for name in reader.fieldnames]
        ticker_lower = ticker.lower()
        for row in reader:
            if row.get('symbol', '').lower() == ticker_lower: return row
        return None

    def get_market_status(self):
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def get_bulk_quotes(self, symbols: list[str]):
        params = {
            "function": "REALTIME_BULK_QUOTES",
            "symbol": ",".join(symbols),
            "datatype": "csv",
        }
        text_response = self._make_request(params)
        if isinstance(text_response, str) and "symbol" in text_response.lower():
             return text_response
        return None

    def get_bulk_quotes_parsed(self, symbols: list[str]) -> list[dict]:
        csv_text = self.get_bulk_quotes(symbols)
        if not csv_text: return []
        results = []
        try:
            f = StringIO(csv_text)
            reader = csv.DictReader(f)
            if reader.fieldnames:
                reader.fieldnames = [name.strip().lower() for name in reader.fieldnames]

            for row in reader:
                price = (self._safe_float(row.get('close')) or self._safe_float(row.get('4. close')) or self._safe_float(row.get('price')) or self._safe_float(row.get('5. price')))
                volume = (self._safe_float(row.get('volume')) or self._safe_float(row.get('5. volume')) or self._safe_float(row.get('6. volume')))
                bid = self._safe_float(row.get('bid')) or self._safe_float(row.get('8. bid price'))
                ask = self._safe_float(row.get('ask')) or self._safe_float(row.get('9. ask price'))
                bid_size = self._safe_float(row.get('bid_size')) or self._safe_float(row.get('bid size'))
                ask_size = self._safe_float(row.get('ask_size')) or self._safe_float(row.get('ask size'))
                symbol = row.get('symbol') or row.get('code')

                if symbol:
                    results.append({'symbol': symbol, 'price': price, 'volume': volume, 'bid': bid, 'ask': ask, 'bid_size': bid_size, 'ask_size': ask_size})
        except Exception as e:
            logger.error(f"Error parsing Bulk CSV: {e}")
        return results

    def get_global_quote(self, symbol: str):
        bulk_csv = self.get_bulk_quotes([symbol])
        if not bulk_csv: return None
        quote_data = self._parse_bulk_quotes_csv(bulk_csv, symbol)
        if not quote_data: return None
        try:
            price = (self._safe_float(quote_data.get('close')) or self._safe_float(quote_data.get('4. close')) or self._safe_float(quote_data.get('price')))
            volume = (self._safe_float(quote_data.get('volume')) or self._safe_float(quote_data.get('5. volume')))
            prev_close = (self._safe_float(quote_data.get('previous_close')) or self._safe_float(quote_data.get('previous close')))
            change = self._safe_float(quote_data.get('change'))
            change_pct = quote_data.get('change_percent') or quote_data.get('change percent')
            formatted_quote = {
                "01. symbol": quote_data.get("symbol"), "02. open": quote_data.get("open"), "03. high": quote_data.get("high"), "04. low": quote_data.get("low"),
                "05. price": price, "06. volume": volume, "07. latest trading day": None, "08. previous close": prev_close,
                "09. change": change, "10. change percent": f'{change_pct}' if change_pct else "0%"
            }
            ext_price = self._safe_float(quote_data.get("extended_hours_quote"))
            if ext_price and ext_price > 0:
                formatted_quote["05. price"] = ext_price
                formatted_quote["09. change"] = quote_data.get("extended_hours_change")
                formatted_quote["10. change percent"] = f'{quote_data.get("extended_hours_change_percent")}%'
                formatted_quote["_price_source"] = "extended_hours"
            return formatted_quote
        except Exception: return None

    def get_company_overview(self, symbol: str): return self._make_request({"function": "OVERVIEW", "symbol": symbol})
    def get_daily_adjusted(self, symbol: str, outputsize: str = 'full'): return self._make_request({"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": symbol, "outputsize": outputsize})
    def get_time_series_daily(self, symbol: str, outputsize: str = 'full'): return self._make_request({"function": "TIME_SERIES_DAILY", "symbol": symbol, "outputsize": outputsize})
    def get_weekly_adjusted(self, symbol: str): return self._make_request({"function": "TIME_SERIES_WEEKLY_ADJUSTED", "symbol": symbol})
    def get_intraday(self, symbol: str, interval: str = '60min', outputsize: str = 'compact', extended_hours: bool = True, month: str = None):
        params = {"function": "TIME_SERIES_INTRADAY", "symbol": symbol, "interval": interval, "outputsize": outputsize, "extended_hours": "true" if extended_hours else "false"}
        if month: params['month'] = month
        return self._make_request(params)
    def get_atr(self, symbol: str, time_period: int = 14, interval: str = 'daily'): return self._make_request({"function": "ATR", "symbol": symbol, "interval": "daily", "time_period": str(time_period)})
    def get_rsi(self, symbol: str, time_period: int = 14, interval: str = 'daily', series_type: str = 'close'): return self._make_request({"function": "RSI", "symbol": symbol, "interval": "daily", "time_period": str(time_period), "series_type": series_type})
    def get_stoch(self, symbol: str, interval: str = 'daily'): return self._make_request({"function": "STOCH", "symbol": symbol, "interval": "daily"})
    def get_adx(self, symbol: str, time_period: int = 14, interval: str = 'daily'): return self._make_request({"function": "ADX", "symbol": symbol, "interval": "daily", "time_period": str(time_period)})
    def get_macd(self, symbol: str, interval: str = 'daily', series_type: str = 'close'): return self._make_request({"function": "MACD", "symbol": symbol, "interval": "daily", "series_type": series_type})
    def get_bollinger_bands(self, symbol: str, time_period: int = 20, interval: str = 'daily', series_type: str = 'close', nbdevup: int = 2, nbdevdn: int = 2):
        return self._make_request({"function": "BBANDS", "symbol": symbol, "interval": interval, "time_period": str(time_period), "series_type": series_type, "nbdevup": str(nbdevup), "nbdevdn": str(nbdevdn)})
    def get_obv(self, symbol: str, interval: str = 'daily'): return self._make_request({"function": "OBV", "symbol": symbol, "interval": interval})
    def get_news_sentiment(self, ticker: str, limit: int = 50, time_from: str = None, time_to: str = None):
        params = {"function": "NEWS_SENTIMENT", "tickers": ticker, "limit": str(limit)}
        if time_from: params["time_from"] = time_from
        if time_to: params["time_to"] = time_to
        return self._make_request(params)
    def get_insider_transactions(self, symbol: str): return self._make_request({"function": "INSIDER_TRANSACTIONS", "symbol": symbol})
    def get_earnings(self, symbol: str): return self._make_request({"function": "EARNINGS", "symbol": symbol})
    def get_inflation_rate(self, interval: str = 'monthly'): return self._make_request({"function": "INFLATION", "interval": interval, "datatype": "json"})
    def get_fed_funds_rate(self, interval: str = 'monthly'): return self._make_request({"function": "FEDERAL_FUNDS_RATE", "interval": interval, "datatype": "json"})
    def get_treasury_yield(self, interval: str = 'monthly', maturity: str = '10year'): return self._make_request({"function": "TREASURY_YIELD", "interval": interval, "maturity": maturity, "datatype": "json"})
    def get_unemployment(self): return self._make_request({"function": "UNEMPLOYMENT", "datatype": "json"})
