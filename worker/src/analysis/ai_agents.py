import logging
import pandas as pd
import requests
import json
import os
import time
import random
from sqlalchemy.orm import Session
from datetime import datetime

logger = logging.getLogger(__name__)

# ==================================================================
# Konfiguracja API Gemini
# ==================================================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.critical("GEMINI_API_KEY nie został znaleziony! Agenty AI nie będą działać.")
    GEMINI_API_KEY = "" 

GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
API_HEADERS = {'Content-Type': 'application/json'}

# ==================================================================
# AGENT NEWSOWY (Analiza sentymentu wiadomości)
# ==================================================================

def _run_news_analysis_agent(ticker: str, headline: str, summary: str, url: str) -> dict:
    """
    Wywołuje Gemini API, aby przeanalizować pojedynczą wiadomość.
    Wersja V3: Tryb "Reporter" dla Biotech - powiadamia o wszystkim, co istotne.
    """
    if not GEMINI_API_KEY:
        logger.error("Agent Newsowy: Brak klucza GEMINI_API_KEY. Analiza niemożliwa.")
        return {"sentiment": "NEUTRAL", "reason": "Brak klucza API Gemini"}

    # Krótka pauza, aby utrzymać się w limitach Gemini (ok. 60 zapytań/min)
    time.sleep(1.1 + random.uniform(0, 0.5)) 
    
    # 1. Wykrywanie kontekstu BioX (Biotech)
    # Sprawdzamy słowa kluczowe charakterystyczne dla branży
    biotech_keywords = [
        "fda", "phase 1", "phase 2", "phase 3", "clinical", "trial", "study", "patient", 
        "drug", "therapy", "treatment", "approval", "orphan", "designation", "patent", 
        "biotech", "pharmaceu", "oncology", "cancer", "gene", "cell", "pipeline", "ind submission",
        "topline", "data", "results", "endpoint", "meeting", "presentation"
    ]
    
    content_lower = (headline + " " + summary).lower()
    is_biotech_context = any(kw in content_lower for kw in biotech_keywords)
    
    # 2. Wybór Promptu
    if is_biotech_context:
        # --- PROMPT SPECJALISTYCZNY (BIOX - TRYB "NEWS FEED") ---
        # Zmiana strategii: Użytkownik chce wiedzieć o WSZYSTKIM.
        # Traktujemy CRITICAL_POSITIVE jako flagę "WYŚLIJ ALERT" dla każdej merytorycznej wiadomości.
        prompt = f"""
        Jesteś inteligentnym filtrem newsów dla aktywnego inwestora w sektorze BIOTECH.
        Twój cel: Powiadomić tradera o KAŻDYM nowym fakcie korporacyjnym dotyczącym spółki {ticker}.
        NIE OCENIAJ WAGI ani WPŁYWU CENOWEGO. Twoim zadaniem jest jedynie odsiać spam i artykuły generowane automatycznie.

        Wiadomość:
        Nagłówek: "{headline}"
        Streszczenie: "{summary}"
        Źródło: {url}

        Zasady klasyfikacji:
        1. `CRITICAL_POSITIVE`: Użyj tej etykiety dla KAŻDEJ konkretnej informacji korporacyjnej, niezależnie czy jest dobra czy zła.
           Przykłady (co raportować):
           - Wyniki badań (dobre LUB złe)
           - Decyzje, wnioski, spotkania z FDA
           - Patenty, publikacje naukowe
           - Wyniki finansowe (Earnings), Guidance
           - Wystąpienia na konferencjach, prezentacje
           - Zmiany w zarządzie, insider trading
           - Emisje akcji (Offering), zmiany w strukturze kapitału
           - Partnerstwa, licencje, fuzje i przejęcia
           (W systemie ta etykieta oznacza: "WAŻNY NEWS -> WYŚLIJ ALERT").
           
        2. `NEUTRAL`: Użyj tej etykiety TYLKO dla bezwartościowego szumu.
           Przykłady (co ignorować):
           - "Akcje {ticker} rosną/spadają o X%" (bez podania przyczyny)
           - "Analiza techniczna", "Sygnał kupna wg wskaźnika RSI"
           - "Raport o nastrojach w sektorze", "Najlepsze akcje na dziś"
           - "Dlaczego akcje się ruszają?" (jeśli artykuł tylko spekuluje)

        WAŻNE: NIE używaj etykiety `CRITICAL_NEGATIVE` w tym trybie. Jeśli wiadomość jest negatywna (np. porażka badania), oznacz ją jako `CRITICAL_POSITIVE`, aby system wysłał powiadomienie, a trader sam podejmie decyzję o reakcji.
        """
    else:
        # --- PROMPT OGÓLNY (STANDARD) ---
        # Dla reszty rynku zachowujemy standardową ochronę kapitału
        prompt = f"""
        Jesteś analitykiem ryzyka daytradingowego.
        Przeanalizuj news dla spółki {ticker}. Ignoruj typowy szum rynkowy.
        Szukaj TYLKO informacji, które mogą wywołać NATYCHMIASTOWY ruch ceny.

        Wiadomość:
        Nagłówek: "{headline}"
        Streszczenie: "{summary}"
        Źródło: {url}

        Sklasyfikuj tę wiadomość:
        1. `CRITICAL_POSITIVE`: Silny katalizator wzrostu (Przejęcie, Earnings Beat, Kontrakt).
        2. `CRITICAL_NEGATIVE`: Silny katalizator spadku (Bankructwo, Śledztwo, Dilution).
        3. `NEUTRAL`: Standardowy szum (Zmiana ceny, opinia analityka, ogólny komentarz).
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
                "description": "Krótkie (1 zdanie) wyjaśnienie decyzji."
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
            
            # Logowanie specyficzne dla kontekstu
            if is_biotech_context:
                logger.info(f"Agent Newsowy [BIOX] ({ticker}): {sentiment} | {reason}")
            else:
                logger.info(f"Agent Newsowy [STD] ({ticker}): {sentiment} | {reason}")
                
            return {"sentiment": sentiment, "reason": reason}

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            if status_code == 429 or status_code >= 500:
                wait = (initial_backoff * (2 ** attempt)) + random.uniform(0, 1)
                logger.warning(f"Agent Newsowy: Błąd HTTP {status_code} dla {ticker} (Próba {attempt + 1}/{max_retries}). Ponawiam za {wait:.2f}s...")
                time.sleep(wait)
                continue
            else:
                logger.error(f"Agent Newsowy: Błąd HTTP (inny niż 429/5xx) podczas wywołania Gemini dla {ticker}: {e}", exc_info=True)
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
# AGENT MAKRO (Analiza wskaźników ekonomicznych - Faza 0)
# ==================================================================

def _run_macro_analysis_agent(inflation: dict, fed_rate: dict, yield_10y: dict, unemployment: dict) -> dict:
    """
    Wywołuje Gemini API, aby przeanalizować kluczowe wskaźniki makro
    i zwrócić sentyment rynkowy ('RISK_ON' lub 'RISK_OFF').
    """
    if not GEMINI_API_KEY:
        logger.error("Agent Makro: Brak klucza GEMINI_API_KEY. Analiza niemożliwa. Ustawiam domyślny 'RISK_ON'.")
        return {"sentiment": "RISK_ON", "reason": "Brak klucza API Gemini, Faza 0 domyślnie przepuszcza skanowanie."}

    time.sleep(1.1 + random.uniform(0, 0.5)) 
    
    try:
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
            
            sentiment = analysis_json.get('sentiment', 'RISK_ON') 
            reason = analysis_json.get('reason', 'Brak analizy')
            
            logger.info(f"Agent Makro (Faza 0): Sentyment={sentiment}. Powód: {reason}")
            return {"sentiment": sentiment, "reason": reason}

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            if status_code == 429 or status_code >= 500:
                wait = (initial_backoff * (2 ** attempt)) + random.uniform(0, 1)
                logger.warning(f"Agent Makro: Błąd HTTP {status_code} (Próba {attempt + 1}/{max_retries}). Ponawiam za {wait:.2f}s...")
                time.sleep(wait)
                continue
            else:
                logger.error(f"Agent Makro: Błąd HTTP (inny niż 429/5xx) podczas wywołania Gemini: {e}", exc_info=True)
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Agent Makro: Błąd sieciowy podczas wywołania Gemini: {e}", exc_info=True)
            break
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.error(f"Agent Makro: Błąd przetwarzania odpowiedzi JSON z Gemini: {e}", exc_info=True)
            break
    
    logger.error(f"Agent Makro: Nie udało się przeanalizować danych makro po {max_retries} próbach. Ustawiam domyślny 'RISK_ON'.")
    return {"sentiment": "RISK_ON", "reason": "Błąd po stronie serwera podczas analizy makro."}
