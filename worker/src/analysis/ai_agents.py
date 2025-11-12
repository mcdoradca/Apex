import logging
import pandas as pd
# ==================================================================
# KROK 1 (KAT. 2): Dodatkowe importy dla Agenta Newsowego
# (Te importy są nadal potrzebne)
# ==================================================================
import requests
import json
import os
import time
import random
# ==================================================================
from sqlalchemy.orm import Session
from datetime import datetime
# ==================================================================
# === DEKONSTRUKCJA (KROK 5) ===
# Usunięto importy, które były używane tylko przez starych,
# wygaszonych agentów AI na żądanie (Momentum, Volatility, Tactical).
# ==================================================================
# from . import utils # Nieużywane
# from .utils import (
#     safe_float, get_market_status_and_time, standardize_df_columns, 
#     calculate_rsi, calculate_bbands,
#     get_relevant_signal_from_db 
# )
# ==================================================================

logger = logging.getLogger(__name__)

# ==================================================================
# KROK 1 (KAT. 2): Konfiguracja API Gemini dla Agenta Newsowego
# (Nadal potrzebne dla obu agentów)
# ==================================================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.critical("GEMINI_API_KEY nie został znaleziony! Agent Newsowy nie będzie działać.")
    GEMINI_API_KEY = "" 

GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
API_HEADERS = {'Content-Type': 'application/json'}
# ==================================================================


# ==================================================================
# === DEKONSTRUKCJA (KROK 5) ===
#
# Poniższe funkcje (Agent Momentum, Zmienności, Sentymentu, Taktyczny)
# oraz ich główny orkiestrator (run_ai_analysis) zostały usunięte.
#
# Były one powiązane ze starą logiką Fazy 2/3 i były wywoływane
# tylko przez analizę na żądanie (wyszukiwarkę), która
# również jest usuwana.
#
# POZOSTAJĄ TYLKO agenci AI (News i Macro), którzy są
# wywoływani przez inne części systemu (Faza 0 i News Agent).
#
# ==================================================================

# --- AGENT 1: ANALIZA MOMENTUM I SIŁY WZGLĘDNEJ ---
# def _run_momentum_agent(...):
#     ... (USUNIĘTE) ...

# --- AGENT 2: ANALIZA KOMPRESJI ENERGII ---
# def _run_volatility_agent(...):
#     ... (USUNIĘTE) ...

# --- AGENT 3: ANALIZA SENTYMENTU ---
# def _run_sentiment_agent(...):
#     ... (USUNIĘTE) ...

# --- AGENT 4: AGENT STRAŻNIKA WEJŚĆ (NOWA LOGIKA) ---
# def _run_tactical_agent(...):
#     ... (USUNIĘTE) ...

# --- GŁÓWNA FUNKCJA ORKIESTRUJĄCA ---
# def run_ai_analysis(...):
#     ... (USUNIĘTE) ...


# ==================================================================
# KROK 1 (KAT. 2): Dodanie "Mózgu" Agenta Newsowego
# (Ta funkcja POZOSTAJE, jest używana przez `news_agent.py`)
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
# (Ta funkcja POZOSTAJE, jest używana przez `phase0_macro_agent.py`)
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
