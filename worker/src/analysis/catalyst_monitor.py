import logging
import requests
import json
import hashlib
import os # <-- NOWY IMPORT
from dotenv import load_dotenv # <-- NOWY IMPORT
from sqlalchemy.orm import Session
from sqlalchemy import text, select, func
from datetime import datetime, timedelta

# Importy z wnętrza projektu
from ..database import get_db_session
from ..models import TradingSignal, ProcessedNews
from ..analysis.utils import update_system_control

logger = logging.getLogger(__name__)

# --- Konfiguracja API Gemini ---
load_dotenv() # <-- NOWE WYWOŁANIE

# === POPRAWKA: Pobieramy klucz ze środowiska ===
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    logger.critical("GEMINI_API_KEY nie został znaleziony w zmiennych środowiskowych! Catalyst Monitor nie będzie działać.")
    API_KEY = "" # Zapewnia, że f-string poniżej nie rzuci błędu

GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={API_KEY}"
API_HEADERS = {'Content-Type': 'application/json'}

# --- Funkcje pomocnicze ---

def _create_news_hash(headline: str, uri: str) -> str:
    """Tworzy unikalny hash SHA-256 dla wiadomości, aby uniknąć duplikatów."""
    s = f"{headline.strip()}{uri.strip()}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _get_tickers_to_monitor(session: Session) -> list[str]:
    """Pobiera listę tickerów ze statusami 'ACTIVE' lub 'PENDING'."""
    try:
        tickers = session.scalars(
            select(TradingSignal.ticker)
            .where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))
            .distinct()
        ).all()
        return tickers
    except Exception as e:
        logger.error(f"CatalystMonitor: Błąd podczas pobierania tickerów do monitorowania: {e}", exc_info=True)
        return []

def _check_if_news_processed(session: Session, ticker: str, news_hash: str) -> bool:
    """Sprawdza, czy dany news (hash) był już przetwarzany dla danego tickera."""
    try:
        # Sprawdzamy newsy z ostatnich 7 dni, aby baza danych nie rosła w nieskończoność
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        exists = session.scalar(
            select(func.count(ProcessedNews.id))
            .where(ProcessedNews.ticker == ticker)
            .where(ProcessedNews.news_hash == news_hash)
            .where(ProcessedNews.processed_at >= seven_days_ago)
        )
        return exists > 0
    except Exception as e:
        logger.error(f"CatalystMonitor: Błąd podczas sprawdzania hasha newsa dla {ticker}: {e}", exc_info=True)
        return False # Na wszelki wypadek lepiej nie wysłać alertu niż spamować

def _save_processed_news(session: Session, ticker: str, news_hash: str, sentiment: str, headline: str, url: str):
    """Zapisuje przetworzony news do bazy danych."""
    try:
        new_entry = ProcessedNews(
            ticker=ticker,
            news_hash=news_hash,
            sentiment=sentiment,
            headline=headline,
            source_url=url
        )
        session.add(new_entry)
        session.commit()
    except Exception as e:
        logger.error(f"CatalystMonitor: Błąd podczas zapisywania newsa dla {ticker}: {e}", exc_info=True)
        session.rollback()

# --- Logika Wywołań API Gemini ---

def _call_gemini_search(ticker: str) -> list[dict]:
    """
    Wywołuje Gemini API z Google Search (Grounding), aby znaleźć najnowsze wiadomości.
    Zwraca listę przetworzonych obiektów newsów (headline, uri).
    """
    prompt = f"Find breaking, high-impact financial news, press releases, or FDA announcements for the company {ticker} from the last 3 hours. Focus on catalysts that could move the stock price significantly."
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}]
    }

    try:
        response = requests.post(GEMINI_API_URL, headers=API_HEADERS, data=json.dumps(payload), timeout=20)
        response.raise_for_status() # To jest linia 90, która rzuca błąd 403
        data = response.json()

        candidate = data.get('candidates', [{}])[0]
        metadata = candidate.get('groundingMetadata', {})
        attributions = metadata.get('groundingAttributions', [])

        if not attributions:
            logger.info(f"CatalystMonitor: Gemini Search nie znalazł żadnych wiadomości (grounding) dla {ticker}.")
            return []

        processed_news = []
        for attr in attributions:
            web = attr.get('web')
            if web and web.get('uri') and web.get('title'):
                processed_news.append({
                    "headline": web['title'],
                    "uri": web['uri']
                })
        return processed_news
    
    except requests.exceptions.RequestException as e:
        logger.error(f"CatalystMonitor: Błąd sieciowy podczas wywołania Gemini Search dla {ticker}: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"CatalystMonitor: Błąd podczas przetwarzania odpowiedzi Gemini Search dla {ticker}: {e}", exc_info=True)
        return []

def _call_gemini_analysis(ticker: str, headline: str, uri: str) -> str:
    """
    Wywołuje Gemini API, aby przeanalizować pojedynczą wiadomość i zwrócić sentyment.
    Używa JSON Schema do wymuszenia odpowiedzi.
    """
    prompt = f"""
    Analyze the following news headline and URL for stock ticker {ticker} from a day-trader's perspective.
    Is this news a significant positive catalyst, a significant negative catalyst, or just neutral noise?
    
    News Headline: "{headline}"
    Source URL: {uri}
    
    Respond with your analysis based on the potential for immediate, high-volume price movement.
    """

    sentiment_schema = {
        "type": "OBJECT",
        "properties": {
            "sentiment": {
                "type": "STRING",
                "enum": ["POSITIVE", "NEGATIVE", "NEUTRAL"],
            },
            "reason": {
                "type": "STRING",
                "description": "A brief 1-sentence explanation for your decision."
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

    try:
        response = requests.post(GEMINI_API_URL, headers=API_HEADERS, data=json.dumps(payload), timeout=15)
        response.raise_for_status()
        data = response.json()
        
        text_content = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')
        analysis_json = json.loads(text_content)
        
        sentiment = analysis_json.get('sentiment', 'NEUTRAL')
        logger.info(f"CatalystMonitor: Analiza dla {ticker}: Sentyment={sentiment}. Powód: {analysis_json.get('reason')}")
        return sentiment

    except requests.exceptions.RequestException as e:
        logger.error(f"CatalystMonitor: Błąd sieciowy podczas wywołania Gemini Analysis dla {ticker}: {e}", exc_info=True)
        return "NEUTRAL"
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.error(f"CatalystMonitor: Błąd przetwarzania odpowiedzi JSON z Gemini Analysis dla {ticker}: {e}", exc_info=True)
        return "NEUTRAL"

# --- Główna funkcja orkiestrująca ---

def run_catalyst_check(session: Session):
    """
    Główna funkcja "Agencji Prasowej" uruchamiana przez harmonogram.
    """
    # === POPRAWKA: Sprawdź klucz API na początku ===
    if not API_KEY:
        logger.warning("CatalystMonitor: Brak klucza GEMINI_API_KEY. Pomijanie cyklu sprawdzania wiadomości.")
        return

    logger.info("CatalystMonitor: Uruchamianie cyklu sprawdzania wiadomości...")
    
    tickers = _get_tickers_to_monitor(session)
    if not tickers:
        logger.info("CatalystMonitor: Brak tickerów Fazy 3 do monitorowania wiadomości.")
        return

    logger.info(f"CatalystMonitor: Monitorowanie wiadomości dla {len(tickers)} tickerów: {', '.join(tickers)}")

    for ticker in tickers:
        try:
            # 1. Znajdź najnowsze wiadomości
            news_items = _call_gemini_search(ticker)
            
            if not news_items:
                continue

            for item in news_items:
                headline = item['headline']
                uri = item['uri']
                
                # 2. Stwórz hash i sprawdź, czy już to widzieliśmy
                news_hash = _create_news_hash(headline, uri)
                if _check_if_news_processed(session, ticker, news_hash):
                    logger.info(f"CatalystMonitor: Wiadomość dla {ticker} (hash: ...{news_hash[-6:]}) została już przetworzona. Pomijam.")
                    continue
                
                logger.info(f"CatalystMonitor: Znaleziono NOWĄ wiadomość dla {ticker}: {headline}. Rozpoczynanie analizy sentymentu...")
                
                # 3. Jeśli news jest nowy, przeanalizuj go
                sentiment = _call_gemini_analysis(ticker, headline, uri)
                
                # 4. Zapisz w bazie (nawet jeśli neutralny), aby uniknąć ponownej analizy
                _save_processed_news(session, ticker, news_hash, sentiment, headline, uri)

                # 5. Jeśli sentyment jest istotny, wyślij alert!
                if sentiment in ["POSITIVE", "NEGATIVE"]:
                    alert_message = f"PILNY ALERT: {ticker} | {sentiment} | {headline}"
                    logger.warning(f"CatalystMonitor: WYSYŁANIE ALERTU! {alert_message}")
                    update_system_control(session, 'system_alert', alert_message)
        
        except Exception as e:
            logger.error(f"CatalystMonitor: Nieoczekiwany błąd w pętli dla tickera {ticker}: {e}", exc_info=True)
            session.rollback() # Upewnij się, że sesja jest czysta na następny ticker

    logger.info("CatalystMonitor: Cykl sprawdzania wiadomości zakończony.")
