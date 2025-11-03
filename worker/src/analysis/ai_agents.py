import logging
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
# POPRAWKA: Dodajemy importy dla lokalnych obliczeń
from .utils import (
    safe_float, get_market_status_and_time, standardize_df_columns, 
    calculate_rsi, calculate_bbands
)
# POPRAWKA: Importujemy phase3_sniper, aby uzyskać dostęp do funkcji setupu
from . import phase3_sniper

logger = logging.getLogger(__name__)

# --- AGENT 1: ANALIZA MOMENTUM I SIŁY WZGLĘDNEJ ---
# POPRAWKA: Agent przyjmuje teraz przetworzone dane (daily_df i qqq_perf)
def _run_momentum_agent(ticker: str, daily_df: pd.DataFrame, qqq_perf: float) -> dict:
    score = 0
    max_score = 4
    details = {}
    
    try:
        # 1. Oblicz RSI lokalnie (zamiast wywołania API)
        rsi_series = calculate_rsi(daily_df['close'], period=9)
        if not rsi_series.empty:
            latest_rsi = rsi_series.iloc[-1]
            details["9-okresowy RSI"] = f"{latest_rsi:.2f}"
            if latest_rsi > 60:
                score += 2
                details["Wniosek RSI"] = "Bardzo silne momentum (RSI > 60)"
            elif latest_rsi > 50:
                score += 1
                details["Wniosek RSI"] = "Pozytywne momentum (RSI > 50)"
            else:
                details["Wniosek RSI"] = "Neutralne lub słabe momentum"
        else:
            details["RSI"] = "Brak danych"

        # 2. Oblicz Performance vs QQQ lokalnie
        if len(daily_df) > 5:
            ticker_perf = (daily_df['close'].iloc[-1] - daily_df['close'].iloc[-6]) / daily_df['close'].iloc[-6] * 100
            details["Zwrot (5 dni)"] = f"{ticker_perf:.2f}%"
            details["Zwrot QQQ (5 dni)"] = f"{qqq_perf:.2f}%"
            
            if ticker_perf > (qqq_perf * 1.5):
                score += 2
                details["Siła Względna"] = "Spółka jest liderem rynku"
            else:
                details["Siła Względna"] = "Zgodnie z rynkiem lub słabiej"
        else:
            details["Siła Względna"] = "Brak danych do porównania"

    except Exception as e:
        logger.error(f"Błąd w Agencie Momentum dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Momentum", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "details": {}}
    
    summary = "Spółka wykazuje bardzo silne momentum i jest liderem rynku." if score >= 3 else \
              "Spółka ma pozytywne momentum, ale nie jest wyraźnym liderem." if score >= 1 else \
              "Brak wyraźnych sygnałów siły."
              
    return {"name": "Agent Momentum", "score": score, "max_score": max_score, "summary": summary, "details": details}


# --- AGENT 2: ANALIZA KOMPRESJI ENERGII ---
# POPRAWKA: Agent przyjmuje teraz przetworzone dane (daily_df)
def _run_volatility_agent(ticker: str, daily_df: pd.DataFrame) -> dict:
    score = 0
    max_score = 3
    details = {}
    
    try:
        # 1. Oblicz BBands i BBW lokalnie (zamiast wywołania API)
        if len(daily_df) < 100:
             return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Niewystarczająca historia danych do analizy.", "details": {}}

        middle_band, upper_band, lower_band, bbw_series = calculate_bbands(daily_df['close'], period=20)
        bbw_series = bbw_series.dropna()
        
        if bbw_series.empty:
            return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Błąd obliczeń BBW.", "details": {}}

        # 2. Oblicz rangę procentową
        percentile_rank = bbw_series.rank(pct=True).iloc[-1] * 100
        details["Ranga % BBW (100 dni)"] = f"{percentile_rank:.1f}%"

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
        
        details["Wniosek"] = summary
            
    except Exception as e:
        logger.error(f"Błąd w Agencie Zmienności dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "details": {}}
    
    return {"name": "Agent Zmienności", "score": score, "max_score": max_score, "summary": summary, "details": details}


# --- AGENT 3: ANALIZA SENTYMENTU ---
# POPRAWKA: Bez zmian, ten agent i tak był niezależny
def _run_sentiment_agent(ticker: str, api_client: object) -> dict:
    score = 0
    max_score = 3
    details = {}
    
    try:
        news_data = api_client.get_news_sentiment(ticker)
        if not news_data or not news_data.get('feed'):
            return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Brak dostępnych wiadomości do analizy.", "details": {}}
        
        relevant_scores = [item['overall_sentiment_score'] for item in news_data['feed'] if item.get('overall_sentiment_score') is not None]
        if not relevant_scores:
            return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Brak wiarygodnych ocen sentymentu.", "details": {}}
        
        avg_sentiment = sum(relevant_scores) / len(relevant_scores)
        details["Średni Sentyment"] = f"{avg_sentiment:.3f}"
        
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
            
        details["Wniosek"] = summary
            
    except Exception as e:
        logger.error(f"Błąd w Agencie Sentymentu dla {ticker}: {e}")
        return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "details": {}}
        
    return {"name": "Agent Sentymentu", "score": score, "max_score": max_score, "summary": summary, "details": details}


# --- AGENT 4: WYSZUKIWANIE SETUPU TAKTYCZNEGO (FAZA 3) ---
# POPRAWKA: Agent przyjmuje teraz przetworzone dane (daily_df)
def _run_tactical_agent(ticker: str, daily_df: pd.DataFrame) -> dict:
    details = {}
    try:
        # POPRAWKA: Wywołujemy funkcję z 'daily_df' (naprawiając błąd)
        trade_setup = phase3_sniper.find_end_of_day_setup(ticker, daily_df)
        
        if trade_setup.get("signal"):
            score = 5
            max_score = 5
            summary = "Znaleziono prawidłowy setup taktyczny! Spółka jest w idealnej strukturze do potencjalnego wejścia."
            details["Status"] = "Setup Potwierdzony"
            
            if trade_setup.get('entry_price'):
                details["Cena Wejścia"] = f"${trade_setup['entry_price']:.2f}"
            elif trade_setup.get('entry_zone_bottom'):
                details["Strefa Wejścia"] = f"${trade_setup['entry_zone_bottom']:.2f} - ${trade_setup['entry_zone_top']:.2f}"
            
            if trade_setup.get('take_profit'):
                details["Potencjalny Cel (TP)"] = f"${trade_setup['take_profit']:.2f}"
        else:
            score = 0
            max_score = 5
            summary = "Brak setupu taktycznego. Spółka nie jest obecnie w optymalnej strukturze do wejścia."
            details["Status"] = "Brak Setupu"
            details["Powód"] = trade_setup.get('reason', 'Nieznany.')
            
    except Exception as e:
        logger.error(f"Błąd w Agencie Taktycznym dla {ticker}: {e}", exc_info=True)
        # Zwracamy bardziej szczegółowy błąd, jeśli to możliwe
        return {"name": "Agent Taktyczny", "score": 0, "max_score": 5, "summary": "Błąd analizy.", "details": {"Błąd": str(e)}}

    return {"name": "Agent Taktyczny", "score": score, "max_score": max_score, "summary": summary, "details": details}


# --- GŁÓWNA FUNKCJA ORKIESTRUJĄCA ---
# POPRAWKA: Ta funkcja jest teraz zoptymalizowana
def run_ai_analysis(ticker: str, api_client: object) -> dict:
    """Uruchamia wszystkich agentów AI i agreguje ich wyniki."""
    logger.info(f"Running full AI analysis for {ticker}...")
    
    # --- ETAP 1: Zbieranie Danych ---
    try:
        quote_data = api_client.get_global_quote(ticker)
        market_info = get_market_status_and_time(api_client)
        
        # Pobieramy dane historyczne DLA TICKERA (1 wywołanie)
        ticker_data_raw = api_client.get_daily_adjusted(ticker, 'full') # 'full' dla historii BBands
        if not ticker_data_raw or 'Time Series (Daily)' not in ticker_data_raw:
            raise Exception(f"Brak danych historycznych (daily) dla {ticker}")
        daily_df = pd.DataFrame.from_dict(ticker_data_raw['Time Series (Daily)'], orient='index')
        daily_df = standardize_df_columns(daily_df)
        
        # Pobieramy dane historyczne DLA QQQ (1 wywołanie)
        qqq_data_raw = api_client.get_daily_adjusted('QQQ', 'compact')
        if not qqq_data_raw or 'Time Series (Daily)' not in qqq_data_raw:
            raise Exception("Brak danych historycznych dla QQQ")
        qqq_df = pd.DataFrame.from_dict(qqq_data_raw['Time Series (Daily)'], orient='index')
        qqq_df = standardize_df_columns(qqq_df)
        
        if len(qqq_df) < 6: raise Exception("Za mało danych QQQ dla 5-dniowej wydajności")
        qqq_perf = (qqq_df['close'].iloc[-1] - qqq_df['close'].iloc[-6]) / qqq_df['close'].iloc[-6] * 100

    except Exception as e:
        logger.error(f"Krytyczny błąd podczas pobierania danych w AI Analysis dla {ticker}: {e}", exc_info=True)
        return {"status": "ERROR", "message": f"Błąd pobierania danych bazowych: {e}"}

    # --- ETAP 2: Uruchamianie Agentów ---
    # Przekazujemy pobrane dane - oszczędzamy wywołania API
    momentum_results = _run_momentum_agent(ticker, daily_df, qqq_perf)
    volatility_results = _run_volatility_agent(ticker, daily_df)
    sentiment_results = _run_sentiment_agent(ticker, api_client) # Ten agent jest niezależny
    tactical_results = _run_tactical_agent(ticker, daily_df)
    
    agents_list = [momentum_results, volatility_results, sentiment_results, tactical_results]
    
    total_score = sum(agent['score'] for agent in agents_list)
    total_max_score = sum(agent['max_score'] for agent in agents_list)
    
    final_score_percent = (total_score / total_max_score) * 100 if total_max_score > 0 else 0
    
    # --- ETAP 3: Agregacja Wyników ---
    if final_score_percent >= 75 and tactical_results['score'] > 0:
        recommendation = "BARDZO SILNY KANDDAT DO KUPNA"
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
        "quote_data": quote_data,
        "market_info": market_info,
        "overall_score": total_score,
        "max_score": total_max_score,
        "final_score_percent": round(final_score_percent),
        "recommendation": recommendation,
        "recommendation_details": recommendation_details,
        "agents": {
            "momentum": momentum_results,
            "volatility": volatility_results,
            "sentiment": sentiment_results,
            "tactical_setup": tactical_results
        },
        "analysis_timestamp_utc": datetime.utcnow().isoformat()
    }
