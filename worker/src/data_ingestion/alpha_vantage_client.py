import time
import requests
import logging
import json
from collections import deque

logger = logging.getLogger(__name__)

class AlphaVantageClient:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self, api_key: str, requests_per_minute: int = 150, retries: int = 3, backoff_factor: float = 0.5):
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.requests_per_minute = requests_per_minute
        self.request_interval = 60.0 / requests_per_minute
        self.request_timestamps = deque()

    def _rate_limiter(self):
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
        self._rate_limiter()
        params['apikey'] = self.api_key
        
        for attempt in range(self.retries):
            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                # Zmiana logowania na bardziej szczegółowe
                request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
                if not data or "Error Message" in data or "Information" in data:
                    logger.warning(f"API returned an error or empty data for {request_identifier}: {data}")
                    if "premium" in str(data).lower():
                         logger.error(f"API call for {request_identifier} failed due to premium limit. Waiting longer.")
                         time.sleep(20)
                    return None
                return data
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                request_identifier = params.get('symbol') or params.get('tickers') or params.get('function')
                logger.error(f"Request failed for {request_identifier} (attempt {attempt + 1}/{self.retries}): {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.backoff_factor * (2 ** attempt))
        return None

    def get_market_status(self):
        """Pobiera aktualny status rynku z dedykowanego endpointu."""
        params = {"function": "MARKET_STATUS"}
        return self._make_request(params)

    def get_bulk_quotes(self, symbols: list[str]):
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
        if value is None:
            return None
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _get_latest_intraday_price(self, symbol: str) -> dict | None:
        try:
            data = self.get_intraday(symbol, interval="1min", outputsize="compact", extended_hours=True)
            
            if data and "Time Series (1min)" in data:
                time_series = data["Time Series (1min)"]
                if not time_series: return None
                
                latest_timestamp_str = sorted(time_series.keys())[-1]
                latest_price = self._safe_float(time_series[latest_timestamp_str]['4. close'])
                if latest_price:
                    return {"price": latest_price, "timestamp": latest_timestamp_str}

        except Exception as e:
            logger.warning(f"Could not get extended intraday price for {symbol}, will use GLOBAL_QUOTE only. Error: {e}")
        return None

    def get_global_quote(self, symbol: str):
        # 1. Zawsze pobieraj bazowy cytat
        params = {"function": "GLOBAL_QUOTE", "symbol": symbol}
        data = self._make_request(params)
        base_quote = data.get('Global Quote') if data else {}

        if not base_quote:
            logger.warning(f"Could not retrieve base GLOBAL_QUOTE for {symbol}. Function will return None.")
            return None

        # 2. Spróbuj pobrać status rynku w sposób odporny na błędy
        us_market_status = "closed" # Bezpieczny domyślny status
        try:
            market_status_data = self.get_market_status()
            if market_status_data and market_status_data.get('markets'):
                 us_market = next((m for m in market_status_data['markets'] if m.get('region') == 'United States'), None)
                 if us_market:
                     us_market_status = us_market.get('current_status', 'closed').lower()
            else:
                logger.warning("Market status response was empty or invalid. Defaulting to 'closed'.")
        except Exception as e:
            logger.error(f"An exception occurred while fetching market status: {e}. Defaulting to 'closed'.")
        
        # 3. Jeśli rynek jest w handlu pozasesyjnym, spróbuj nadpisać cenę
        if us_market_status in ["pre-market", "post-market"]:
            latest_intraday = self._get_latest_intraday_price(symbol)
            if latest_intraday:
                try:
                    intraday_price = latest_intraday['price']
                    logger.info(f"Price Override for {symbol} ({us_market_status.upper()}): Replacing old price with real-time intraday price ({intraday_price}).")
                    
                    # Nadpisujemy cenę i przeliczamy zmianę
                    base_quote['05. price'] = str(intraday_price)
                    previous_close = self._safe_float(base_quote.get('08. previous close'))
                    if previous_close and previous_close != 0:
                        new_change = intraday_price - previous_close
                        new_change_percent = (new_change / previous_close) * 100
                        base_quote['09. change'] = f"{new_change:.4f}"
                        base_quote['10. change percent'] = f"{new_change_percent:.4f}%"
                except Exception as e:
                    logger.error(f"Error during price override logic for {symbol}: {e}")

        return base_quote

