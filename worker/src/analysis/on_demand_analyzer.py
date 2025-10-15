import logging
from .utils import safe_float

logger = logging.getLogger(__name__)

def _get_latest_value(data: dict, series_key: str, value_key: str) -> float | None:
    """Pobiera najnowszą wartość z serii danych technicznych."""import logging
import pandas as pd
from .utils import safe_float

logger = logging.getLogger(__name__)

def _get_latest_value(data: dict, series_key: str, value_key: str) -> float | None:
    if not data or series_key not in data: return None
    try:
        latest_date = sorted(data[series_key].keys())[-1]
        return safe_float(data[series_key][latest_date][value_key])
    except (IndexError, KeyError):
        return None

def _analyze_macd_trend(data: dict) -> str:
    if not data or 'Technical Analysis: MACD' not in data: return "Neutralny"
    try:
        series = data['Technical Analysis: MACD']
        dates = sorted(series.keys())
        if len(dates) < 2: return "Neutralny"
        
        latest_macd = safe_float(series[dates[-1]]['MACD'])
        prev_macd = safe_float(series[dates[-2]]['MACD'])
        
        if latest_macd is not None and prev_macd is not None:
            if latest_macd > prev_macd: return "Byczy"
            elif latest_macd < prev_macd: return "Niedźwiedzi"
    except (IndexError, KeyError):
        pass
    return "Neutralny"

def perform_full_analysis(ticker: str, api_client) -> dict:
    logger.info(f"Performing on-demand analysis for {ticker}")

    overview_data = api_client.get_company_overview(ticker)
    daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
    rsi_data = api_client.get_rsi(ticker)
    stoch_data = api_client.get_stoch(ticker)
    adx_data = api_client.get_adx(ticker)
    macd_data = api_client.get_macd(ticker)

    if not overview_data or not daily_data_raw:
        raise Exception("Could not retrieve fundamental or price data for analysis.")

    technicals = {
        'RSI (14)': _get_latest_value(rsi_data, 'Technical Analysis: RSI', 'RSI'),
        'Stochastic %K': _get_latest_value(stoch_data, 'Technical Analysis: STOCH', 'SlowK'),
        'ADX (14)': _get_latest_value(adx_data, 'Technical Analysis: ADX', 'ADX'),
        'Sygnał MACD': _analyze_macd_trend(macd_data)
    }

    fundamentals = {
        'MarketCap': overview_data.get('MarketCapitalization'),
        'PERatio': overview_data.get('PERatio'),
        'EPS': overview_data.get('EPS'),
        'ProfitMargin': overview_data.get('ProfitMargin'),
        'DividendYield': overview_data.get('DividendYield')
    }

    score = 0
    reasons = []
    
    rsi = technicals.get('RSI (14)')
    stoch = technicals.get('Stochastic %K')
    adx = technicals.get('ADX (14)')
    macd_signal = technicals.get('Sygnał MACD')
    
    try:
        time_series_data = daily_data_raw['Time Series (Daily)']
        cleaned_data = {pd.to_datetime(date): {key.split(' ')[1]: float(val) for key, val in values.items()} for date, values in time_series_data.items()}
        daily_df = pd.DataFrame.from_dict(cleaned_data, orient='index').sort_index()

        latest_price = daily_df['close'].iloc[-1]
        prev_price = daily_df['close'].iloc[-2]
        change_percent = ((latest_price - prev_price) / prev_price) * 100 if prev_price else 0
        if change_percent > 3:
            score += 1
            reasons.append("Silny sentyment pozytywny (zmiana > 3%)")
    except (IndexError, KeyError, TypeError):
        pass
        
    if rsi is not None and rsi < 35:
        score += 1; reasons.append("RSI wskazuje na wyprzedanie")
    if stoch is not None and stoch < 25:
        score += 1; reasons.append("Stochastic wskazuje na wyprzedanie")
    if macd_signal == "Byczy":
        score += 1; reasons.append("Sygnał MACD byczy")
    if adx is not None and adx > 25:
        score += 1; reasons.append("Silny trend (ADX > 25)")
        
    pe_ratio = safe_float(fundamentals.get('PERatio'))
    profit_margin_str = fundamentals.get('ProfitMargin')
    profit_margin = safe_float(profit_margin_str) * 100 if isinstance(profit_margin_str, str) else safe_float(profit_margin_str)

    if pe_ratio is not None and 0 < pe_ratio < 20:
        score += 2; reasons.append("Atrakcyjna wycena (niski P/E)")
    if profit_margin is not None and profit_margin > 15:
        score += 2; reasons.append("Wysoka rentowność (>15%)")
        
    if score >= 6: recommendation = "Silny Sygnał Kupna"
    elif score >= 3: recommendation = "Sygnał Kupna"
    else: recommendation = "Neutralny / Obserwuj"

    return {
        "ticker": ticker,
        "fundamentals": {k: v for k, v in fundamentals.items() if v is not None and v != 'None'},
        "technicals": {k: f"{v:.2f}" if isinstance(v, float) else v for k, v in technicals.items() if v is not None},
        "ai_score": score,
        "recommendation": recommendation,
        "reasons": reasons if reasons else ["Brak wyraźnych sygnałów."]
    }

    if not data or series_key not in data:
        return None
    try:
        latest_date = sorted(data[series_key].keys())[-1]
        return safe_float(data[series_key][latest_date][value_key])
    except (IndexError, KeyError):
        return None

def _analyze_macd_trend(data: dict) -> str:
    """Analizuje trend na podstawie ostatnich dwóch wartości MACD."""
    if not data or 'Technical Analysis: MACD' not in data:
        return "Neutralny"
    try:
        series = data['Technical Analysis: MACD']
        dates = sorted(series.keys())
        if len(dates) < 2:
            return "Neutralny"
        
        latest_macd = safe_float(series[dates[-1]]['MACD'])
        prev_macd = safe_float(series[dates[-2]]['MACD'])
        
        if latest_macd is not None and prev_macd is not None:
            if latest_macd > prev_macd:
                return "Byczy"
            elif latest_macd < prev_macd:
                return "Niedźwiedzi"
    except (IndexError, KeyError):
        pass
    return "Neutralny"

def perform_full_analysis(ticker: str, api_client) -> dict:
    """Wykonuje pełną analizę na żądanie dla pojedynczej spółki."""
    logger.info(f"Performing on-demand analysis for {ticker}")

    # 1. Pobieranie danych
    overview_data = api_client.get_company_overview(ticker)
    daily_data = api_client.get_daily_adjusted(ticker, 'compact')
    rsi_data = api_client.get_rsi(ticker)
    stoch_data = api_client.get_stoch(ticker)
    adx_data = api_client.get_adx(ticker)
    macd_data = api_client.get_macd(ticker)

    if not overview_data or not daily_data:
        raise Exception("Could not retrieve fundamental or price data for analysis.")

    # 2. Składanie pakietów analitycznych
    technicals = {
        'RSI': _get_latest_value(rsi_data, 'Technical Analysis: RSI', 'RSI'),
        'Stochastic': _get_latest_value(stoch_data, 'Technical Analysis: STOCH', 'SlowK'),
        'ADX': _get_latest_value(adx_data, 'Technical Analysis: ADX', 'ADX'),
        'MACD_Signal': _analyze_macd_trend(macd_data)
    }

    fundamentals = {
        'MarketCap': overview_data.get('MarketCapitalization'),
        'PERatio': overview_data.get('PERatio'),
        'EPS': overview_data.get('EPS'),
        'ProfitMargin': overview_data.get('ProfitMargin'),
        'DividendYield': overview_data.get('DividendYield')
    }

    # 3. System Scoringu AI
    score = 0
    reasons = []
    
    # Warunki techniczne
    rsi = technicals.get('RSI')
    stoch = technicals.get('Stochastic')
    adx = technicals.get('ADX')
    macd_signal = technicals.get('MACD_Signal')
    
    try:
        dates = sorted(daily_data['Time Series (Daily)'].keys())
        latest_price = safe_float(daily_data['Time Series (Daily)'][dates[-1]]['4. close'])
        prev_price = safe_float(daily_data['Time Series (Daily)'][dates[-2]]['4. close'])
        change_percent = ((latest_price - prev_price) / prev_price) * 100 if prev_price else 0
        if change_percent > 3:
            score += 1
            reasons.append("Silny sentyment pozytywny")
    except (IndexError, KeyError, TypeError):
        pass # Ignoruj błąd, jeśli nie ma wystarczająco danych
        
    if rsi is not None and rsi < 35:
        score += 1
        reasons.append("RSI wskazuje na wyprzedanie")
    if stoch is not None and stoch < 25:
        score += 1
        reasons.append("Stochastic wskazuje na wyprzedanie")
    if macd_signal == "Byczy":
        score += 1
        reasons.append("Sygnał MACD byczy")
    if adx is not None and adx > 25:
        score += 1
        reasons.append("Silny trend (ADX > 25)")
        
    # Warunki fundamentalne
    pe_ratio = safe_float(fundamentals.get('PERatio'))
    profit_margin_str = fundamentals.get('ProfitMargin')
    profit_margin = safe_float(profit_margin_str) * 100 if isinstance(profit_margin_str, str) else safe_float(profit_margin_str)

    if pe_ratio is not None and 0 < pe_ratio < 20:
        score += 2
        reasons.append("Atrakcyjna wycena (niski P/E)")
    if profit_margin is not None and profit_margin > 15:
        score += 2
        reasons.append("Wysoka rentowność")
        
    # 4. Rekomendacje
    if score >= 6: recommendation = "Silny Sygnał Kupna"
    elif score >= 3: recommendation = "Sygnał Kupna"
    elif score >= 0: recommendation = "Neutralny / Obserwuj"
    else: recommendation = "Unikaj / Sprzedawaj"

    # 5. Formatowanie finalnego wyniku
    return {
        "ticker": ticker,
        "fundamentals": {k: v for k, v in fundamentals.items() if v is not None},
        "technicals": {k: f"{v:.2f}" if isinstance(v, float) else v for k, v in technicals.items() if v is not None},
        "ai_score": score,
        "recommendation": recommendation,
        "reasons": reasons if reasons else ["Brak wyraźnych sygnałów."]
    }

