import logging
import pandas as pd
# ZMIANA: Dodajemy import Session
from sqlalchemy.orm import Session
from datetime import datetime
# ZMIANA: Importujemy 'utils', aby użyć nowej funkcji
from . import utils
from .utils import (
    safe_float, get_market_status_and_time, standardize_df_columns, 
    calculate_rsi, calculate_bbands
)
# ZMIANA: Nie potrzebujemy już importować całego phase3_sniper
# from . import phase3_sniper 

logger = logging.getLogger(__name__)

# --- AGENT 1: ANALIZA MOMENTUM I SIŁY WZGLĘDNEJ ---
# (Bez zmian)
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
# (Bez zmian)
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
# (Bez zmian)
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


# --- AGENT 4: AGENT STRAŻNIKA WEJŚĆ (NOWA LOGIKA) ---
# ZMIANA: Agent odczytuje teraz stan z bazy danych, zamiast go obliczać
def _run_tactical_agent(session: Session, ticker: str) -> dict:
    """
    Agent, który odczytuje AKTUALNY stan setupu (ACTIVE, PENDING, INVALIDATED)
    z bazy danych i przekazuje go do frontendu.
    """
    score = 0
    max_score = 5 
    details = {}

    try:
        # 1. Użyj nowej funkcji pomocniczej, aby pobrać ostatni istotny sygnał
        relevant_signal = utils.get_relevant_signal_from_db(session, ticker)
        
        # 2. Jeśli nie ma sygnału (nawet unieważnionego), to znaczy, że nic nie znaleziono
        if not relevant_signal:
            return {
                "name": "Agent Strażnik Wejść",
                "score": 0, "max_score": max_score,
                "summary": "Brak aktywnego setupu taktycznego w bazie danych.",
                "details": {"Status": "Brak Setupu"}
            }

        # 3. Mamy sygnał. Sprawdźmy jego status.
        signal_status = relevant_signal.status
        signal_notes = relevant_signal.notes
        
        # 4. Jeśli setup został już UNIEWAŻNIONY przez Strażnika (backend)...
        if signal_status == 'INVALIDATED':
            score = 1 # Dajemy niski wynik, ale nie zero
            summary = "Setup ZANEGOWANY. Został unieważniony przez Strażnika backendu (cena spadła poniżej SL)."
            details["Status"] = "ZANEGOWANY"
            details["Powód"] = signal_notes
            if relevant_signal.stop_loss:
                details["Stop Loss (EOD)"] = f"${relevant_signal.stop_loss:.2f}"
            
            return {"name": "Agent Strażnik Wejść", "score": score, "max_score": max_score, "summary": summary, "details": details}

        # 5. Jeśli setup jest AKTYWNY lub OCZEKUJĄCY...
        score = 5
        summary = "Wykryto aktywny setup EOD. Strażnik w przeglądarce zweryfikuje cenę LIVE."
        details["Status EOD"] = f"Setup {signal_status}: {signal_notes}"
        
        # Przekaż statyczne parametry do frontendu
        entry_price = relevant_signal.entry_price or relevant_signal.entry_zone_top
        stop_loss = relevant_signal.stop_loss
        take_profit = relevant_signal.take_profit

        if entry_price:
             details["Cena Wejścia (EOD)"] = f"${entry_price:.2f}"
        if stop_loss:
            details["Stop Loss (EOD)"] = f"${stop_loss:.2f}"
        if take_profit:
            details["Take Profit (EOD)"] = f"${take_profit:.2f}"
        
        return {"name": "Agent Strażnik Wejść", "score": score, "max_score": max_score, "summary": summary, "details": details}

    except Exception as e:
        logger.error(f"Błąd w Agencie Taktycznym dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Strażnik Wejść", "score": 0, "max_score": 5, "summary": "Błąd krytyczny agenta taktycznego.", "details": {"Błąd": str(e)}}


# --- GŁÓWNA FUNKCJA ORKIESTRUJĄCA ---
# ZMIANA: Dodano 'session: Session' jako pierwszy argument
def run_ai_analysis(session: Session, ticker: str, api_client: object) -> dict:
    """Uruchamia wszystkich agentów AI i agreguje ich wyniki."""
    logger.info(f"Running full AI analysis for {ticker}...")
    
    # --- ETAP 1: Zbieranie Danych ---
    try:
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
        
        market_info = get_market_status_and_time(api_client)

    except Exception as e:
        logger.error(f"Krytyczny błąd podczas pobierania danych w AI Analysis dla {ticker}: {e}", exc_info=True)
        return {"status": "ERROR", "message": f"Błąd pobierania danych bazowych: {e}"}

    # --- ETAP 2: Uruchamianie Agentów ---
    momentum_results = _run_momentum_agent(ticker, daily_df, qqq_perf)
    volatility_results = _run_volatility_agent(ticker, daily_df)
    sentiment_results = _run_sentiment_agent(ticker, api_client) 
    
    # ZMIANA: Wywołujemy agenta taktycznego, przekazując mu 'session'
    tactical_results = _run_tactical_agent(session, ticker)
    
    agents_list = [momentum_results, volatility_results, sentiment_results, tactical_results]
    
    total_score = sum(agent['score'] for agent in agents_list)
    total_max_score = sum(agent['max_score'] for agent in agents_list)
    
    final_score_percent = (total_score / total_max_score) * 100 if total_max_score > 0 else 0
    
    # --- ETAP 3: Agregacja Wyników ---
    # ZMIANA: Logika rekomendacji opiera się teraz na wyniku taktycznym
    if final_score_percent >= 75 and tactical_results['score'] == 5:
        recommendation = "BARDZO SILNY KANDDAT DO KUPNA"
        recommendation_details = "Spółka wykazuje wyjątkową siłę na wielu płaszczyznach. Strażnik (LIVE) zweryfikuje, czy setup jest nadal aktywny."
    elif final_score_percent >= 60 and tactical_results['score'] > 0:
        recommendation = "SILNY KANDYDAT DO OBSERWACJI"
        recommendation_details = "Spółka ma wiele pozytywnych cech. Warto dodać do obserwowanych."
    elif final_score_percent >= 40:
        recommendation = "INTERESUJĄCY KANDYDAT"
        recommendation_details = "Spółka wykazuje pewne pozytywne sygnały, ale wymaga dalszej obserwacji."
    else:
        recommendation = "NEUTRALNY / ZALECA SIĘ OSTROŻNOŚĆ"
        recommendation_details = "Obecnie spółka nie wykazuje wystarczająco silnych sygnałów do podjęcia działań."

    # ZMIANA: Usuwamy 'quote_data' z odpowiedzi
    return {
        "status": "DONE",
        "ticker": ticker,
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
            "tactical_and_guard": tactical_results # Frontend oczekuje tego klucza
        },
        "analysis_timestamp_utc": datetime.utcnow().isoformat()
    }

