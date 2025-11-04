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


# --- AGENT 4: NOWY AGENT STRAŻNIKA WEJŚĆ (ZASTĘPUJE STAREGO AGENTA TAKTYCZNEGO) ---
def _run_tactical_and_guard_agent(ticker: str, daily_df: pd.DataFrame, api_client: object) -> dict:
    """
    Agent łączący dwie funkcje:
    1. Weryfikuje, czy setup EOD (dzienny) nadal istnieje.
    2. Uruchamia "Strażnika", który analizuje dane Intraday (H1) w celu walidacji wejścia.
    """
    score = 0
    max_score = 5
    details = {}

    try:
        # 1. Sprawdź, czy setup EOD (na danych dziennych) w ogóle istnieje
        trade_setup = phase3_sniper.find_end_of_day_setup(ticker, daily_df)
        
        if not trade_setup.get("signal"):
            # Jeśli nie ma setupu EOD, nie ma czego pilnować
            return {
                "name": "Agent Strażnik Wejść",
                "score": 0, "max_score": max_score,
                "summary": "Brak setupu taktycznego EOD. Spółka nie jest obecnie w strukturze do wejścia.",
                "details": {"Status": "Brak Setupu", "Powód": trade_setup.get('reason', 'Nieznany.')}
            }

        # 2. Setup EOD istnieje. Uruchamiamy Strażnika Wejść.
        details["Status EOD"] = "Wykryto Setup: " + trade_setup.get('notes', 'Brak notatek.')
        entry_price = trade_setup.get('entry_price') or trade_setup.get('entry_zone_top')
        stop_loss = trade_setup.get('stop_loss')
        take_profit = trade_setup.get('take_profit')

        # Setup musi mieć cenę wejścia i stop loss do walidacji
        if not entry_price or not stop_loss:
             return {"name": "Agent Strażnik Wejść", "score": 0, "max_score": max_score, "summary": "Setup EOD istnieje, ale brakuje mu ceny wejścia lub stop lossa do weryfikacji.", "details": details}

        # 3. Pobierz dane intraday (H1) do weryfikacji
        intraday_raw = api_client.get_intraday(ticker, interval='60min', outputsize='compact', extended_hours=False) # extended_hours=False dla czystszego obrazu
        if not intraday_raw or 'Time Series (60min)' not in intraday_raw:
            return {"name": "Agent Strażnik Wejść", "score": 1, "max_score": max_score, "summary": "Setup EOD istnieje, ale nie można pobrać danych intraday (H1) do jego weryfikacji. Zachowaj ostrożność.", "details": details}

        intraday_df = pd.DataFrame.from_dict(intraday_raw['Time Series (60min)'], orient='index')
        intraday_df = standardize_df_columns(intraday_df) # Używamy utils
        
        # Bierzemy ostatnie 16 świec H1 (ok. 2 dni handlowe) do analizy
        recent_candles = intraday_df.iloc[-16:] 

        current_price = recent_candles.iloc[-1]['close']
        
        # --- Logika Strażnika ---
        
        # RULE A: "Za Późno" (Spike) - czy cel został prawie osiągnięty?
        if take_profit:
            # Obliczmy 50% dystansu do TP
            halfway_to_target = entry_price + (take_profit - entry_price) * 0.5
            if recent_candles['high'].max() > halfway_to_target:
                score = 0
                summary = "Setup SPALONY (Za Późno). Cena już wykonała znaczną część ruchu (ponad 50% do TP) i cofa. Wejście teraz jest bardzo ryzykowne."
                details["Weryfikacja Strażnika"] = "Ruch zrealizowany. Najwyższa cena H1 przekroczyła 50% celu."
                details["Najwyższa cena H1"] = f"${recent_candles['high'].max():.2f}"
                return {"name": "Agent Strażnik Wejść", "score": score, "max_score": max_score, "summary": summary, "details": details}

        # RULE B: "Stop-out" (Negacja)
        if recent_candles['low'].min() < stop_loss:
            score = 0
            summary = "Setup ZANEGOWANY. Cena spadła już poniżej wyznaczonego poziomu Stop Loss (na wykresie H1). Setup jest nieważny."
            details["Weryfikacja Strażnika"] = "Setup nieważny (Stop Loss trafiony)."
            details["Najniższa cena H1"] = f"${recent_candles['low'].min():.2f}"
            return {"name": "Agent Strażnik Wejść", "score": score, "max_score": max_score, "summary": summary, "details": details}

        # RULE D: "Cena Uciekła" (Niekorzystny R/R)
        # Sprawdź, czy cena jest > 2% powyżej wejścia
        if current_price > (entry_price * 1.02):
            score = 1
            summary = "Cena UCIEKŁA. Setup jest nadal technicznie aktywny (SL nie trafiony), ale obecna cena jest znacznie powyżej idealnego wejścia. Stosunek R/R jest niekorzystny."
            details["Weryfikacja Strażnika"] = "Wysokie ryzyko (Cena uciekła)."
            details["Obecna Cena"] = f"${current_price:.2f}"
            return {"name": "Agent Strażnik Wejść", "score": score, "max_score": max_score, "summary": summary, "details": details}

        # RULE C: "Wszystko OK"
        score = 5
        summary = "Setup POTWIERDZONY i WAŻNY. Cena jest nadal w strefie wejścia lub blisko niej. Setup EOD jest aktualny i bezpieczny do rozważenia."
        details["Weryfikacja Strażnika"] = "Wszystko OK (Cena w strefie wejścia)."
        details["Obecna Cena"] = f"${current_price:.2f}"
        return {"name": "Agent Strażnik Wejść", "score": score, "max_score": max_score, "summary": summary, "details": details}

    except Exception as e:
        logger.error(f"Błąd w Agencie Taktycznym (Strażnik) dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Strażnik Wejść", "score": 0, "max_score": 5, "summary": "Błąd krytyczny agenta Strażnika.", "details": {"Błąd": str(e)}}



# --- GŁÓWNA FUNKCJA ORKIESTRUJĄCA ---
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
    
    # ZMIANA: Wywołujemy nowego agenta
    tactical_and_guard_results = _run_tactical_and_guard_agent(ticker, daily_df, api_client)
    
    agents_list = [momentum_results, volatility_results, sentiment_results, tactical_and_guard_results]
    
    total_score = sum(agent['score'] for agent in agents_list)
    total_max_score = sum(agent['max_score'] for agent in agents_list)
    
    final_score_percent = (total_score / total_max_score) * 100 if total_max_score > 0 else 0
    
    # --- ETAP 3: Agregacja Wyników ---
    # ZMIANA: Używamy nowego agenta w logice rekomendacji
    if final_score_percent >= 75 and tactical_and_guard_results['score'] > 1: # Wymagamy, aby Strażnik dał OK (wynik 5) lub ostrzegał (wynik 1)
        recommendation = "BARDZO SILNY KANDDAT DO KUPNA"
        recommendation_details = "Spółka wykazuje wyjątkową siłę na wielu płaszczyznach. Strażnik potwierdza, że setup jest nadal aktywny."
    elif final_score_percent >= 60:
        recommendation = "SILNY KANDYDAT DO OBSERWACJI"
        recommendation_details = "Spółka ma wiele pozytywnych cech. Warto dodać do obserwowanych i czekać na setup taktyczny."
    elif final_score_percent >= 40:
        recommendation = "INTERESUJĄCY KANDYDAT"
        recommendation_details = "Spółka wykazuje pewne pozytywne sygnały, ale wymaga dalszej obserwacji."
    else:
        recommendation = "NEUTRALNY / ZALECA SIĘ OSTROŻNOŚĆ"
        recommendation_details = "Obecnie spółka nie wykazuje wystarczająco silnych sygnałów do podjęcia działań."

    # ZMIANA: Zmieniamy klucz dla agenta taktycznego
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
            "tactical_and_guard": tactical_and_guard_results
        },
        "analysis_timestamp_utc": datetime.utcnow().isoformat()
    }

