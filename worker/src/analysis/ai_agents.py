import logging
import pandas as pd
# ==================================================================
# KROK 1 (KAT. 2): Dodatkowe importy dla Agenta Newsowego
# ==================================================================
import requests
import json
import os
import time
import random
# ==================================================================
from sqlalchemy.orm import Session
from datetime import datetime
# ZMIANA: Importujemy 'utils', aby użyć nowej funkcji
from . import utils
from .utils import (
    safe_float, get_market_status_and_time, standardize_df_columns, 
    calculate_rsi, calculate_bbands,
    # Poprawka: Import funkcji get_relevant_signal_from_db był w 'utils', ale nie tutaj
    get_relevant_signal_from_db 
)

logger = logging.getLogger(__name__)

# ==================================================================
# KROK 1 (KAT. 2): Konfiguracja API Gemini dla Agenta Newsowego
# ==================================================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.critical("GEMINI_API_KEY nie został znaleziony! Agent Newsowy nie będzie działać.")
    GEMINI_API_KEY = "" 

GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
API_HEADERS = {'Content-Type': 'application/json'}
# ==================================================================


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
# (Logika zgodna z wdrożonymi poprawkami F-3)
def _run_tactical_agent(session: Session, ticker: str) -> dict:
    """
    Agent, który odczytuje AKTUALNY stan setupu (ACTIVE, PENDING, INVALIDATED)
    z bazy danych i przekazuje go do frontendu, używając dynamicznych
    i precyzyjnych komunikatów, a nie statycznego tekstu.
    """
    score = 0
    max_score = 5 
    details = {}
    summary = "" # Zostanie ustawiony dynamicznie

    try:
        # 1. Użyj nowej funkcji pomocniczej, aby pobrać ostatni istotny sygnał
        #    (Funkcja ta pobiera teraz także COMPLETED)
        relevant_signal = get_relevant_signal_from_db(session, ticker)
        
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
        
        # 4. Dynamiczne generowanie podsumowania i wyniku na podstawie statusu
        
        if signal_status == 'INVALIDATED':
            score = 1 # Niski wynik, ale nie zero (informacja jest cenna)
            summary = "Setup ZANEGOWANY (NIEAKTUALNY). Został unieważniony przez Strażnika Backendu."
            details["Status"] = "ZANEGOWANY"
            details["Powód unieważnienia"] = signal_notes
            
        elif signal_status == 'COMPLETED':
            score = 1 # Niski wynik, informacja historyczna
            summary = "Setup ZAKOŃCZONY. Cel (Take Profit) został osiągnięty."
            details["Status"] = "ZAKOŃCZONY"
            details["Notatki"] = signal_notes

        elif signal_status == 'PENDING':
            score = 5 # Najwyższy wynik - gotowy do obserwacji
            summary = "Wykryto setup EOD (Oczekujący). System monitoruje cenę wejścia."
            details["Status EOD"] = f"Setup {signal_status}"
            details["Notatki"] = signal_notes

        elif signal_status == 'ACTIVE':
            score = 5 # Najwyższy wynik - aktywny
            summary = "Setup AKTYWNY. System monitoruje poziomy Stop Loss i Take Profit."
            details["Status EOD"] = f"Setup {signal_status}"
            details["Notatki"] = signal_notes
            
        else: # Obsługa innych statusów, np. CANCELLED
            score = 0
            summary = f"Wykryto sygnał ze statusem: {signal_status}."
            details["Status"] = signal_status
            details["Notatki"] = signal_notes
        
        # 5. Przekaż statyczne parametry do frontendu (bez zmian)
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
    if final_score_percent >= 75 and tactical_results['score'] == 5:
        recommendation = "BARDZO SILNY KANDDAT DO KUPNA"
        recommendation_details = "Spółka wykazuje wyjątkową siłę na wielu płaszczyznach. Strażnik Backendu monitoruje ceny wejścia, SL i TP."
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


# ==================================================================
# KROK 1 (KAT. 2): Dodanie "Mózgu" Agenta Newsowego
# Ta funkcja będzie wywoływana przez nowego Agenta (news_agent.py)
# ==================================================================

def _run_news_analysis_agent(ticker: str, headline: str, summary: str, url: str) -> dict:
    """
    Wywołuje Gemini API, aby przeanalizować pojedynczą wiadomość (z Alpha Vantage)
    i zwrócić krytyczną klasyfikację (CRITICAL_NEGATIVE, CRITICAL_POSITIVE, NEUTRAL).
    """
    if not GEMINI_API_KEY:
        logger.error("Agent Newsowy: Brak klucza GEMINI_API_KEY. Analiza niemożliwa.")
        return {"sentiment": "NEUTRAL", "reason": "Brak klucza API Gemini"}

    # Krótka pauza, aby utrzymać się w limitach Gemini (ok. 60 zapytań/min)
    time.sleep(1.1 + random.uniform(0, 0.5)) 
    
    # Precyzyjny prompt trenujący, o którym rozmawialiśmy
    prompt = f"""
    Jesteś analitykiem ryzyka daytradingowego. Twoim zadaniem jest ochrona kapitału tradera.
    Przeanalizuj poniższy nagłówek i streszczenie wiadomości dla spółki {ticker}.
    Ignoruj standardowy szum rynkowy i analizy cenowe. Skup się wyłącznie na
    informacjach, które mogą GWAŁTOWNIE i NATYCHMIASTOWO zmienić cenę akcji.

    Wiadomość:
    Nagłówek: "{headline}"
    Streszczenie: "{summary}"
    Źródło: {url}

    Sklasyfikuj tę wiadomość jako JEDNĄ z trzech opcji:
    1.  `CRITICAL_NEGATIVE`: Wiadomość, która może spowodować natychmiastową panikę lub spadek (np. obniżenie prognoz, złe wyniki finansowe, śledztwo, fatalne dane FDA, rezygnacja CEO, pozew zbiorowy).
    2.  `CRITICAL_POSITIVE`: Wiadomość, która może spowodować natychmiastową euforię lub wzrost (np. zatwierdzenie FDA, przejęcie, partnerstwo strategiczne, wyniki znacznie lepsze od oczekiwań).
    3.  `NEUTRAL`: Standardowy szum rynkowy (np. "Analitycy uważają, że...", "Cena akcji wzrosła o X%", "Spółka prezentuje się na konferencji", ogólne analizy sektorowe).
    """

    # Definicja schematu JSON dla odpowiedzi Gemini
    sentiment_schema = {
        "type": "OBJECT",
        "properties": {
            "sentiment": {
                "type": "STRING",
                "enum": ["CRITICAL_POSITIVE", "CRITICAL_NEGATIVE", "NEUTRAL"],
            },
            "reason": {
                "type": "STRING",
                "description": "Krótkie (1 zdanie) wyjaśnienie, dlaczego ta wiadomość jest lub nie jest krytyczna."
            }
        },
        "required": ["sentiment", "reason"]
    }
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": sentiment_schema
        }
    }

    max_retries = 3
    initial_backoff = 3

    for attempt in range(max_retries):
        try:
            response = requests.post(GEMINI_API_URL, headers=API_HEADERS, data=json.dumps(payload), timeout=20)
            response.raise_for_status()
            data = response.json()
            
            text_content = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')
            analysis_json = json.loads(text_content)
            
            sentiment = analysis_json.get('sentiment', 'NEUTRAL')
            reason = analysis_json.get('reason', 'Brak analizy')
            
            logger.info(f"Agent Newsowy ({ticker}): Sentyment={sentiment}. Powód: {reason}")
            return {"sentiment": sentiment, "reason": reason}

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = (initial_backoff * (2 ** attempt)) + random.uniform(0, 1)
                logger.warning(f"Agent Newsowy: Rate limit (429) dla analizy {ticker} (Próba {attempt + 1}/{max_retries}). Ponawiam za {wait:.2f}s...")
                time.sleep(wait)
                continue
            else:
                logger.error(f"Agent Newsowy: Błąd HTTP (inny niż 429) podczas wywołania Gemini dla {ticker}: {e}", exc_info=True)
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Agent Newsowy: Błąd sieciowy podczas wywołania Gemini dla {ticker}: {e}", exc_info=True)
            break
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.error(f"Agent Newsowy: Błąd przetwarzania odpowiedzi JSON z Gemini dla {ticker}: {e}", exc_info=True)
            break
    
    logger.error(f"Agent Newsowy: Nie udało się przeanalizować newsa dla {ticker} po {max_retries} próbach.")
    return {"sentiment": "NEUTRAL", "reason": "Błąd po stronie serwera podczas analizy"}
# ==================================================================
# Koniec Krok 1 (KAT. 2)
# ==================================================================


# ==================================================================
# KROK C (FAZA 0): Dodanie "Mózgu" Agenta Makroekonomicznego
# === GŁÓWNA NAPRAWA BŁĘDU FAZY 0 ===
# ==================================================================
def _run_macro_analysis_agent(inflation: dict, fed_rate: dict, yield_10y: dict, unemployment: dict) -> dict:
    """
    Wywołuje Gemini API, aby przeanalizować kluczowe wskaźniki makro
    i zwrócić sentyment rynkowy ('RISK_ON' lub 'RISK_OFF').
    
    POPRAWIONA WERSJA: Akceptuje `inflation` (np. 3.0) zamiast `cpi` (np. 324.8).
    """
    if not GEMINI_API_KEY:
        logger.error("Agent Makro: Brak klucza GEMINI_API_KEY. Analiza niemożliwa. Ustawiam domyślny 'RISK_ON'.")
        return {"sentiment": "RISK_ON", "reason": "Brak klucza API Gemini, Faza 0 domyślnie przepuszcza skanowanie."}

    # Krótka pauza, aby utrzymać się w limitach Gemini
    time.sleep(1.1 + random.uniform(0, 0.5)) 
    
    # Przygotowanie danych wejściowych dla promptu
    try:
        # Przetwarzamy dane, aby uzyskać najnowsze wartości
        # ZMIANA: `cpi` -> `inflation`
        latest_inflation_value = inflation.get('data', [{}])[0].get('value', 'N/A')
        latest_inflation_date = inflation.get('data', [{}])[0].get('date', 'N/A')
        
        latest_fed_rate_value = fed_rate.get('data', [{}])[0].get('value', 'N/A')
        latest_fed_rate_date = fed_rate.get('data', [{}])[0].get('date', 'N/A')
        
        latest_yield_value = yield_10y.get('data', [{}])[0].get('value', 'N/A')
        latest_yield_date = yield_10y.get('data', [{}])[0].get('date', 'N/A')

        latest_unemployment_value = unemployment.get('data', [{}])[0].get('value', 'N/A')
        latest_unemployment_date = unemployment.get('data', [{}])[0].get('date', 'N/A')

    except Exception as e:
        logger.error(f"Agent Makro: Błąd parsowania danych wejściowych: {e}. Ustawiam domyślny 'RISK_ON'.")
        return {"sentiment": "RISK_ON", "reason": f"Błąd parsowania danych wejściowych: {e}"}

    # Precyzyjny prompt trenujący
    # ZMIANA: Używamy `latest_inflation_value` i poprawiamy etykietę w prompcie.
    prompt = f"""
    Jesteś głównym analitykiem makroekonomicznym w funduszu hedgingowym typu 'long-only' skupionym na akcjach wzrostowych (Nasdaq).
    Twoim zadaniem jest ochrona kapitału funduszu. Masz określić, czy otoczenie rynkowe sprzyja podejmowaniu ryzyka.

    Przeanalizuj poniższe 4 kluczowe wskaźniki makroekonomiczne dla USA:

    1.  Inflacja (Roczna stopa): {latest_inflation_value}% (z dnia: {latest_inflation_date})
    2.  Stopy Procentowe (FED Funds Rate): {latest_fed_rate_value}% (z dnia: {latest_fed_rate_date})
    3.  Rentowność Obligacji (10-letnie): {latest_yield_value}% (z dnia: {latest_yield_date})
    4.  Stopa Bezrobocia: {latest_unemployment_value}% (z dnia: {latest_unemployment_date})

    Cel FED dla inflacji to 2.0%. Cel dla bezrobocia to ~4.0%.

    Sklasyfikuj obecne środowisko jako JEDNĄ z dwóch opcji:
    1.  `RISK_ON`: Środowisko sprzyjające. Inflacja jest pod kontrolą (blisko 2-3%), stopy FED są stabilne lub spadają, rentowność obligacji jest niska/stabilna, bezrobocie jest niskie. Można agresywnie kupować akcje wzrostowe.
    2.  `RISK_OFF`: Środowisko niebezpieczne. Inflacja jest wysoka (> 3.5%) lub szybko rośnie, stopy FED rosną, rentowność obligacji gwałtownie rośnie (np. > 4.5%), lub bezrobocie rośnie. Należy wstrzymać nowe zakupy i chronić kapitał.
    """

    # Definicja schematu JSON dla odpowiedzi Gemini
    macro_schema = {
        "type": "OBJECT",
        "properties": {
            "sentiment": {
                "type": "STRING",
                "enum": ["RISK_ON", "RISK_OFF"],
            },
            "reason": {
                "type": "STRING",
                "description": "Krótkie (1 zdanie) wyjaśnienie, dlaczego podjąłeś taką decyzję."
            }
        },
        "required": ["sentiment", "reason"]
    }
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": macro_schema
        }
    }

    max_retries = 3
    initial_backoff = 3

    for attempt in range(max_retries):
        try:
            response = requests.post(GEMINI_API_URL, headers=API_HEADERS, data=json.dumps(payload), timeout=20)
            response.raise_for_status()
            data = response.json()
            
            text_content = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')
            analysis_json = json.loads(text_content)
            
            sentiment = analysis_json.get('sentiment', 'RISK_ON') # Bezpieczny domyślny
            reason = analysis_json.get('reason', 'Brak analizy')
            
            logger.info(f"Agent Makro (Faza 0): Sentyment={sentiment}. Powód: {reason}")
            return {"sentiment": sentiment, "reason": reason}

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = (initial_backoff * (2 ** attempt)) + random.uniform(0, 1)
                logger.warning(f"Agent Makro: Rate limit (429) (Próba {attempt + 1}/{max_retries}). Ponawiam za {wait:.2f}s...")
                time.sleep(wait)
                continue
            else:
                logger.error(f"Agent Makro: Błąd HTTP (inny niż 429) podczas wywołania Gemini: {e}", exc_info=True)
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Agent Makro: Błąd sieciowy podczas wywołania Gemini: {e}", exc_info=True)
            break
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.error(f"Agent Makro: Błąd przetwarzania odpowiedzi JSON z Gemini: {e}", exc_info=True)
            break
    
    logger.error(f"Agent Makro: Nie udało się przeanalizować danych makro po {max_retries} próbach. Ustawiam domyślny 'RISK_ON'.")
    return {"sentiment": "RISK_ON", "reason": "Błąd po stronie serwera podczas analizy makro."}
# ==================================================================
# Koniec Krok C (FAZA 0) i Koniec GŁÓWNEJ NAPRAWY
# ==================================================================
