import logging
import pandas as pd
from sqlalchemy.orm import Session
# ==========================================================
# KRYTYCZNA POPRAWKA: Dodanie brakującego importu 'datetime'
# To jest ostateczne rozwiązanie błędu 'NameError'.
# ==========================================================
from datetime import datetime

from ..config import Phase2Config
from .utils import safe_float, get_performance

logger = logging.getLogger(__name__)

# --- AGENT 1: ANALIZA MOMENTUM I SIŁY WZGLĘDNEJ ---
def _run_momentum_agent(ticker: str, api_client: object) -> dict:
    """Analizuje momentum RSI i siłę względną w stosunku do rynku (QQQ)."""
    score = 0
    max_score = 4
    report_items = []
    
    try:
        # 1. Pobieranie danych RSI i cen
        rsi_data = api_client.get_rsi(ticker, time_period=9)
        ticker_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
        qqq_data_raw = api_client.get_daily_adjusted('QQQ', 'compact')

        # 2. Analiza RSI
        if rsi_data and 'Technical Analysis: RSI' in rsi_data:
            latest_rsi = safe_float(list(rsi_data['Technical Analysis: RSI'].values())[0]['RSI'])
            if latest_rsi:
                report_items.append({"label": "9-okresowy RSI", "value": f"{latest_rsi:.2f}"})
                if latest_rsi > 60:
                    score += 2
                    report_items.append({"label": "Wniosek RSI", "value": "Bardzo silne momentum (RSI > 60)"})
                elif latest_rsi > 50:
                    score += 1
                    report_items.append({"label": "Wniosek RSI", "value": "Pozytywne momentum (RSI > 50)"})
                else:
                    report_items.append({"label": "Wniosek RSI", "value": "Neutralne lub słabe momentum"})
            else:
                 report_items.append({"label": "RSI", "value": "Brak danych"})
        else:
            report_items.append({"label": "RSI", "value": "Brak danych"})

        # 3. Analiza siły względnej
        ticker_perf = get_performance(ticker_data_raw, 5)
        qqq_perf = get_performance(qqq_data_raw, 5)

        if ticker_perf is not None and qqq_perf is not None:
            report_items.append({"label": "Zwrot (5 dni)", "value": f"{ticker_perf:.2f}%"})
            report_items.append({"label": "Zwrot QQQ (5 dni)", "value": f"{qqq_perf:.2f}%"})
            if ticker_perf > (qqq_perf * 1.5):
                score += 2
                report_items.append({"label": "Siła Względna", "value": "Spółka jest liderem rynku"})
            else:
                report_items.append({"label": "Siła Względna", "value": "Zgodnie z rynkiem lub słabiej"})
        else:
            report_items.append({"label": "Siła Względna", "value": "Brak danych do porównania"})

    except Exception as e:
        logger.error(f"Błąd w Agencie Momentum dla {ticker}: {e}")
        return {"name": "Agent Momentum", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "report_items": []}
    
    summary = "Spółka wykazuje bardzo silne momentum i jest liderem rynku." if score >= 3 else \
              "Spółka ma pozytywne momentum, ale nie jest wyraźnym liderem." if score >= 1 else \
              "Brak wyraźnych sygnałów siły."
              
    return {"name": "Agent Momentum", "score": score, "max_score": max_score, "summary": summary, "report_items": report_items}


# --- AGENT 2: ANALIZA KOMPRESJI ENERGII ---
def _run_volatility_agent(ticker: str, api_client: object) -> dict:
    """Analizuje kompresję zmienności za pomocą wskaźnika Bollinger Bandwidth (BBW)."""
    score = 0
    max_score = 3
    report_items = []
    
    try:
        bbands_data = api_client.get_bollinger_bands(ticker, time_period=20)
        tech_analysis = bbands_data.get('Technical Analysis: BBANDS')
        if not tech_analysis or len(tech_analysis) < 100:
             return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Niewystarczająca historia danych do analizy.", "report_items": []}

        bbw_values = []
        for date_str, values in tech_analysis.items():
            upper = safe_float(values.get('Real Upper Band'))
            lower = safe_float(values.get('Real Lower Band'))
            middle = safe_float(values.get('Real Middle Band'))
            if upper and lower and middle and middle > 0:
                bbw = (upper - lower) / middle
                bbw_values.append(bbw)
        
        if not bbw_values:
            return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Błąd obliczeń BBW.", "report_items": []}

        bbw_series = pd.Series(bbw_values)
        current_bbw = bbw_series.iloc[0]
        percentile_rank = bbw_series.rank(pct=True).iloc[0] * 100

        report_items.append({"label": "Ranga % BBW (100 dni)", "value": f"{percentile_rank:.1f}%"})

        if percentile_rank < 10:
            score = 3
            summary = "Ekstremalna kompresja zmienności. Wysoki potencjał na gwałtowny ruch ceny."
        elif percentile_rank < 25:
            score = 2
            summary = "Zmienność jest niska. Potencjał na ruch ceny rośnie."
        elif percentile_rank < 40:
            score = 1
            summary = "Zmienność poniżej średniej. Spółka w fazie konsolidacji."
        else:
            score = 0
            summary = "Standardowa lub wysoka zmienność. Brak oznak kompresji energii."
        
        report_items.append({"label": "Wniosek", "value": summary})
            
    except Exception as e:
        logger.error(f"Błąd w Agencie Zmienności dla {ticker}: {e}")
        return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "report_items": []}
    
    return {"name": "Agent Zmienności", "score": score, "max_score": max_score, "summary": summary, "report_items": report_items}


# --- AGENT 3: ANALIZA SENTYMENTU ---
def _run_sentiment_agent(ticker: str, api_client: object) -> dict:
    """Analizuje sentyment na podstawie najnowszych wiadomości."""
    score = 0
    max_score = 3
    report_items = []
    
    try:
        news_data = api_client.get_news_sentiment(ticker)
        if not news_data or not news_data.get('feed'):
            return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Brak dostępnych wiadomości do analizy.", "report_items": []}
        
        relevant_scores = [item['overall_sentiment_score'] for item in news_data['feed'] if item.get('overall_sentiment_score') is not None]
        if not relevant_scores:
            return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Brak wiarygodnych ocen sentymentu.", "report_items": []}
        
        avg_sentiment = sum(relevant_scores) / len(relevant_scores)
        report_items.append({"label": "Średni Sentyment", "value": f"{avg_sentiment:.3f}"})
        
        if avg_sentiment >= 0.35:
            score = 3
            summary = "Bardzo silny, jednoznacznie pozytywny sentyment w mediach."
        elif avg_sentiment >= 0.15:
            score = 2
            summary = "Wyraźnie pozytywny sentyment w mediach."
        elif avg_sentiment > 0:
            score = 1
            summary = "Lekko pozytywny sentyment, przewaga byków."
        else:
            score = 0
            summary = "Neutralny lub negatywny sentyment."
            
        report_items.append({"label": "Wniosek", "value": summary})
            
    except Exception as e:
        logger.error(f"Błąd w Agencie Sentymentu dla {ticker}: {e}")
        return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "report_items": []}
        
    return {"name": "Agent Sentymentu", "score": score, "max_score": max_score, "summary": summary, "report_items": report_items}


# --- AGENT 4: WYSZUKIWANIE SETUPU TAKTYCZNEGO (FAZA 3) ---
def _run_tactical_agent(ticker: str, api_client: object) -> dict:
    """Sprawdza, czy spółka spełnia kryteria taktyczne Fazy 3 (setup EOD)."""
    # Używamy logiki z phase3_sniper, aby zachować spójność
    from . import phase3_sniper
    
    try:
        trade_setup = phase3_sniper.find_end_of_day_setup(ticker, api_client)
        
        if trade_setup.get("signal"):
            score = 5
            max_score = 5
            summary = "Znaleziono prawidłowy setup taktyczny! Spółka jest w idealnej strukturze do potencjalnego wejścia."
            report_items = [
                {"label": "Status", "value": "Setup Potwierdzony"},
                {"label": "Strefa Wejścia", "value": f"${trade_setup['entry_zone_bottom']:.2f} - ${trade_setup['entry_zone_top']:.2f}"},
                {"label": "Potencjalny Cel (TP)", "value": f"${trade_setup['take_profit']:.2f}"},
            ]
        else:
            score = 0
            max_score = 5
            summary = "Brak setupu taktycznego. Spółka nie jest obecnie w optymalnej strukturze do wejścia."
            report_items = [
                {"label": "Status", "value": "Brak Setupu"},
                {"label": "Powód", "value": trade_setup.get('reason', 'Nieznany.')},
            ]
            
    except Exception as e:
        logger.error(f"Błąd w Agencie Taktycznym dla {ticker}: {e}")
        return {"name": "Agent Taktyczny", "score": 0, "max_score": 5, "summary": "Błąd analizy.", "report_items": []}

    return {"name": "Agent Taktyczny", "score": score, "max_score": max_score, "summary": summary, "report_items": report_items}


# --- GŁÓWNA FUNKCJA ORKIESTRUJĄCA ---
def run_ai_analysis(ticker: str, api_client: object) -> dict:
    """Uruchamia wszystkich agentów AI i agreguje ich wyniki."""
    logger.info(f"Running full AI analysis for {ticker}...")
    
    # Uruchomienie agentów równolegle (w przyszłości można użyć asyncio)
    momentum_results = _run_momentum_agent(ticker, api_client)
    volatility_results = _run_volatility_agent(ticker, api_client)
    sentiment_results = _run_sentiment_agent(ticker, api_client)
    tactical_results = _run_tactical_agent(ticker, api_client)
    
    agents_results = [momentum_results, volatility_results, sentiment_results, tactical_results]
    
    total_score = sum(agent['score'] for agent in agents_results)
    total_max_score = sum(agent['max_score'] for agent in agents_results)
    
    final_score_percent = (total_score / total_max_score) * 100 if total_max_score > 0 else 0
    
    # Określenie rekomendacji
    if final_score_percent >= 75 and tactical_results['score'] > 0:
        recommendation = "BARDZO SILNY KANDYDAT DO KUPNA"
        recommendation_details = "Spółka wykazuje wyjątkową siłę na wielu płaszczyznach i posiada prawidłowy setup taktyczny."
    elif final_score_percent >= 60:
        recommendation = "SILNY KANDYDAT DO OBSERWACJI"
        recommendation_details = "Spółka ma wiele pozytywnych cech. Warto dodać do obserwowanych i czekać na setup taktyczny."
    elif final_score_percent >= 40:
        recommendation = "INTERESUJĄCY KANDYDAT"
        recommendation_details = "Spółka wykazuje pewne pozytywne sygnały, ale wymaga dalszej obserwacji."
    else:
        recommendation = "NEUTRALNY / ZALECA SIĘ OSTROŻNOŚĆ"
        recommendation_details = "Obecnie spółka nie wykazuje wystarczająco silnych sygnałów do podjęcia działań."

    return {
        "status": "DONE",
        "ticker": ticker,
        "overall_score": f"{total_score}/{total_max_score}",
        "final_score_percent": round(final_score_percent),
        "recommendation": recommendation,
        "recommendation_details": recommendation_details,
        "agents": agents_results,
        "analysis_timestamp_utc": datetime.utcnow().isoformat()
    }

