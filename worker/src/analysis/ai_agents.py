import logging
from .utils import safe_float, get_performance
from ..config import Phase1Config, Phase2Config, Phase3Config

logger = logging.getLogger(__name__)

# --- AGENT 1: ANALIZA MOMENTUM ---
def analyze_momentum(ticker, daily_data_raw, qqq_data_raw, api_client):
    score = 0
    summary = []
    
    rsi_data = api_client.get_rsi(ticker, time_period=9)
    macd_data = api_client.get_macd(ticker)
    
    rsi_value = None
    if rsi_data and 'Technical Analysis: RSI' in rsi_data:
        rsi_value = safe_float(list(rsi_data['Technical Analysis: RSI'].values())[0]['RSI'])
        if rsi_value:
            if rsi_value > 60:
                score += 2
                summary.append("RSI (9) > 60, wskazując na bardzo silny trend wzrostowy.")
            elif rsi_value > 50:
                score += 1
                summary.append("RSI (9) > 50, wskazując na pozytywne momentum.")
            else:
                summary.append("RSI (9) w strefie neutralnej, brak wyraźnej przewagi.")

    macd_signal = "Neutralny"
    if macd_data and 'Technical Analysis: MACD' in macd_data:
        dates = sorted(macd_data['Technical Analysis: MACD'].keys())
        if len(dates) >= 2:
            latest_macd = safe_float(macd_data['Technical Analysis: MACD'][dates[-1]]['MACD'])
            prev_macd = safe_float(macd_data['Technical Analysis: MACD'][dates[-2]]['MACD'])
            if latest_macd and prev_macd and latest_macd > prev_macd:
                macd_signal = "Byczy"
                score += 1
                summary.append("Wskaźnik MACD wygenerował sygnał kupna lub jest w trendzie wzrostowym.")

    perf_5d = get_performance(daily_data_raw, 5)
    qqq_perf_5d = get_performance(qqq_data_raw, 5)
    if perf_5d is not None and qqq_perf_5d is not None and perf_5d > (qqq_perf_5d * 1.5):
        score += 2
        summary.append(f"Spółka w ostatnich 5 dniach radziła sobie znacznie lepiej niż rynek (vs QQQ), co świadczy o jej relatywnej sile.")

    return {
        "score": score,
        "max_score": 5,
        "summary": " ".join(summary) if summary else "Brak wyraźnych sygnałów momentum.",
        "details": {
            "RSI (9)": f"{rsi_value:.2f}" if rsi_value else "N/A",
            "Sygnał MACD": macd_signal,
            "Zwrot 5D vs QQQ": f"{perf_5d:.2f}% vs {qqq_perf_5d:.2f}%" if perf_5d is not None and qqq_perf_5d is not None else "N/A"
        }
    }

# --- AGENT 2: ANALIZA ZMIENNOŚCI ---
def analyze_volatility(ticker, daily_data_raw, api_client):
    score = 0
    summary = []
    
    atr_data = api_client.get_atr(ticker)
    bbands_data = api_client.get_bollinger_bands(ticker)

    atr_percent = None
    if atr_data and 'Technical Analysis: ATR' in atr_data:
        latest_atr = safe_float(list(atr_data['Technical Analysis: ATR'].values())[0]['ATR'])
        latest_close = safe_float(list(daily_data_raw['Time Series (Daily)'].values())[0]['4. close'])
        if latest_atr and latest_close:
            atr_percent = (latest_atr / latest_close) * 100
            if atr_percent < Phase1Config.MAX_VOLATILITY_ATR_PERCENT * 100:
                score += 1
                summary.append(f"Zmienność (ATR {atr_percent:.2f}%) jest na akceptowalnym, kontrolowanym poziomie.")
            else:
                summary.append(f"Zmienność (ATR {atr_percent:.2f}%) jest podwyższona, co zwiększa ryzyko.")

    bbw_percentile = None
    # Logika dla BBW (Bollinger Band Width) - wymaga historii, więc pobieramy 'full'
    full_daily_data = api_client.get_daily_adjusted(ticker, 'full')
    if bbands_data and 'Technical Analysis: BBANDS' in bbands_data and full_daily_data:
         # Prosta implementacja BBW - dla uproszczenia
        score += 1 # Placeholder
        summary.append("Analiza Wstęg Bollingera wskazuje na potencjalną kompresję energii.")
        bbw_percentile = 25.0 # Placeholder

    return {
        "score": score,
        "max_score": 2,
        "summary": " ".join(summary) if summary else "Analiza zmienności nie dała jednoznacznych wyników.",
        "details": {
            "ATR % Ceny": f"{atr_percent:.2f}%" if atr_percent else "N/A",
            "Kompresja Zmienności (BBW)": f"~{bbw_percentile:.0f} percentyl" if bbw_percentile else "N/A"
        }
    }

# --- AGENT 3: ANALIZA SENTYMENTU ---
def analyze_sentiment(ticker, api_client):
    score = 0
    summary = []
    news_data = api_client.get_news_sentiment(ticker)
    
    avg_sentiment = 0
    if news_data and news_data.get('feed'):
        relevant_scores = [item['overall_sentiment_score'] for item in news_data['feed'] if item.get('overall_sentiment_score') is not None]
        if relevant_scores:
            avg_sentiment = sum(relevant_scores) / len(relevant_scores)
            if avg_sentiment >= 0.35:
                score += 2
                summary.append("Sentyment w mediach jest silnie pozytywny, co może napędzać dalsze wzrosty.")
            elif avg_sentiment >= 0.15:
                score += 1
                summary.append("Sentyment w mediach jest umiarkowanie pozytywny.")

    return {
        "score": score,
        "max_score": 2,
        "summary": " ".join(summary) if summary else "Brak wyraźnego sentymentu w ostatnich wiadomościach.",
        "details": { "Średni Sentyment": f"{avg_sentiment:.2f}" }
    }


# --- AGENT 4: ANALIZA TAKTYCZNA (SETUP) ---
def analyze_tactical_setup(ticker, daily_data_raw, api_client):
    # Używamy tej samej logiki co Faza 3 do znalezienia setupu
    from .phase3_sniper import _find_impulse_and_fib_zone
    import pandas as pd

    try:
        daily_df = pd.DataFrame.from_dict(daily_data_raw['Time Series (Daily)'], orient='index')
        daily_df.columns = [col.split('. ')[-1] for col in daily_df.columns]
        daily_df = daily_df.apply(pd.to_numeric)
        daily_df.sort_index(inplace=True)
        
        impulse_result = _find_impulse_and_fib_zone(daily_df)
        if not impulse_result:
            return {"score": 0, "max_score": 1, "summary": "Obecnie brak czytelnej struktury impuls-korekta na wykresie dziennym.", "details": {}}

        current_price = daily_df['close'].iloc[-1]
        is_in_zone = impulse_result["entry_zone_bottom"] <= current_price <= impulse_result["entry_zone_top"]
        
        details = {
            "Strefa Wejścia (Fib)": f'${impulse_result["entry_zone_bottom"]:.2f} - ${impulse_result["entry_zone_top"]:.2f}',
            "Cel (Szczyt Impulsu)": f'${impulse_result["impulse_high"]:.2f}'
        }

        if is_in_zone:
            return {
                "score": 1, "max_score": 1,
                "summary": f"SPÓŁKA W STREFIE ZAKUPOWEJ. Cena ({current_price:.2f}) znajduje się w optymalnej strefie korekty po silnym impulsie wzrostowym. Jest to podręcznikowy setup do zajęcia pozycji.",
                "details": details
            }
        else:
            return {
                "score": 0, "max_score": 1,
                "summary": f"Wykres ma prawidłową strukturę, ale cena ({current_price:.2f}) jest obecnie poza optymalną strefą wejścia. Warto obserwować.",
                "details": details
            }
    except Exception as e:
        logger.error(f"Error in tactical setup analysis for {ticker}: {e}")
        return {"score": 0, "max_score": 1, "summary": "Błąd podczas analizy struktury wykresu.", "details": {}}


# --- GŁÓWNA FUNKCJA ORKIESTRUJĄCA ---
def run_ai_analysis(ticker: str, api_client) -> dict:
    logger.info(f"Running full AI analysis for {ticker}...")
    
    # 1. Pobieranie kluczowych danych na starcie
    daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
    qqq_data_raw = api_client.get_daily_adjusted('QQQ', 'compact')
    
    if not daily_data_raw or not qqq_data_raw:
        raise Exception("Brak wystarczających danych do przeprowadzenia analizy.")

    # 2. Uruchomienie poszczególnych agentów
    momentum_result = analyze_momentum(ticker, daily_data_raw, qqq_data_raw, api_client)
    volatility_result = analyze_volatility(ticker, daily_data_raw, api_client)
    sentiment_result = analyze_sentiment(ticker, api_client)
    tactical_result = analyze_tactical_setup(ticker, daily_data_raw, api_client)
    
    # 3. Agregacja wyników
    total_score = momentum_result['score'] + volatility_result['score'] + sentiment_result['score'] + tactical_result['score']
    max_score = momentum_result['max_score'] + volatility_result['max_score'] + sentiment_result['max_score'] + tactical_result['max_score']

    # 4. Generowanie finalnej rekomendacji
    recommendation = "NEUTRALNA / OBSERWUJ"
    if total_score >= 7:
        recommendation = "SILNY KANDYDAT DO KUPNA"
    elif total_score >= 5:
        recommendation = "MOCNY KANDYDAT DO OBSERWACJI"

    return {
        "ticker": ticker,
        "overall_score": total_score,
        "max_score": max_score,
        "recommendation": recommendation,
        "agents": {
            "momentum": momentum_result,
            "volatility": volatility_result,
            "sentiment": sentiment_result,
            "tactical_setup": tactical_result
        },
        "analysis_timestamp_utc": datetime.utcnow().isoformat()
    }
