
import logging
import time
import json
import hashlib
import os
import requests
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text

# === POPRAWIONE IMPORTY ===
# models są w katalogu wyżej (worker/src)
from .. import models
# utils są w tym samym katalogu (worker/src/analysis)
from . import utils

logger = logging.getLogger(__name__)

# === KONFIGURACJA ZGODNA Z SUPORTEM ALPHA VANTAGE (WARIANT B) ===
# Celujemy w 120 zapytań/minutę na Newsy, zostawiając 30 RPM bufora na inne moduły.
TARGET_RPM = 120  
REQUEST_INTERVAL = 60.0 / TARGET_RPM  # ~0.5s przerwy między zapytaniami
LOOKBACK_WINDOW_MINUTES = 2  # Margines bezpieczeństwa ("time_from") dla ciągłości danych

# Progi decyzyjne dla Agenta
MIN_RELEVANCE_SCORE = 0.60
DEFAULT_SENTIMENT_THRESHOLD = 0.30
LIFE_SCIENCES_SENTIMENT_THRESHOLD = 0.25 # Niższy próg dla branży Biotech (Pump Hunter)
URGENT_SENTIMENT_THRESHOLD = 0.45

# Konfiguracja Telegrama (Pobierana z ENV Workera)
# Upewnij się, że w Render Dashboard zmienne nazywają się dokładnie tak:
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

class NewsScout:
    def __init__(self, session: Session, api_client):
        self.session = session
        self.api_client = api_client
        self.stats = {
            "processed_tickers": 0,
            "articles_found": 0,
            "alerts_sent": 0,
            "errors": 0
        }

    def run_cycle(self, specific_tickers=None):
        \"\"\"
        Główna pętla agenta newsowego.
        Obsługuje listę tickerów z Fazy X (lub inną przekazaną), zachowując limity API.
        \"\"\"
        start_time = time.time()
        logger.info(">>> NEWS AGENT: Rozpoczynam cykl skanowania (Wariant B: 120 RPM)...")

        # 1. Pobierz listę tickerów do monitorowania
        if specific_tickers:
            tickers = specific_tickers
        else:
            # Domyślnie: Pobierz kandydatów z Fazy X (Pump Hunter) + Fazy 1 (EOD)
            # Support AV sugerował listę ~650 tickerów. Łączymy kluczowe tabele.
            try:
                # Pobieramy tickery jako listę krotek i spłaszczamy do listy stringów
                q_phasex = self.session.query(models.PhaseXCandidate.ticker).all()
                q_phase1 = self.session.query(models.Phase1Candidate.ticker).all()
                
                # Tworzymy unikalną listę (set -> list)
                tickers = list(set([t[0] for t in q_phasex] + [t[0] for t in q_phase1]))
                
                # Zabezpieczenie: Jeśli lista pusta (start systemu), weź top 200 z bazy firm
                if not tickers:
                    q_companies = self.session.query(models.Company.ticker).limit(200).all()
                    tickers = [t[0] for t in q_companies]
            except Exception as e:
                logger.error(f"NEWS AGENT: Błąd pobierania listy tickerów: {e}")
                tickers = []

        logger.info(f"NEWS AGENT: Lista do skanowania: {len(tickers)} tickerów.")

        # 2. Ustalenie okna czasowego (time_from)
        # Cofamy się o margines (2 min), aby pokryć ewentualne luki w transmisji
        time_from_dt = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_WINDOW_MINUTES)
        time_from_str = time_from_dt.strftime('%Y%m%dT%H%M')

        # 3. Iteracja po tickerach z Pacingiem (0.5s)
        for ticker in tickers:
            step_start = time.time()
            
            try:
                self._process_ticker(ticker, time_from_str)
            except Exception as e:
                # Nie przerywamy pętli przez błąd jednego tickera
                logger.error(f"Błąd przetwarzania newsów dla {ticker}: {e}")
                self.stats["errors"] += 1

            self.stats["processed_tickers"] += 1
            
            # PACING: Czekaj, aby utrzymać tempo 120 RPM i nie zablokować klucza
            elapsed = time.time() - step_start
            sleep_time = max(0, REQUEST_INTERVAL - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        duration = time.time() - start_time
        logger.info(f"<<< NEWS AGENT: Cykl zakończony w {duration:.1f}s. Statystyki: {self.stats}")

    def _process_ticker(self, ticker: str, time_from: str):
        \"\"\"Pobiera i analizuje newsy dla pojedynczego tickera.\"\"\"
        
        # Wywołanie API (Sortowanie LATEST zaszyte w kliencie AV)
        data = self.api_client.get_news_sentiment(
            ticker=ticker,
            limit=50,
            time_from=time_from
        )

        if not data or "feed" not in data:
            return

        feed = data.get("feed", [])
        
        for article in feed:
            self._analyze_article(ticker, article)

    def _analyze_article(self, ticker: str, article: dict):
        \"\"\"Analizuje pojedynczy artykuł pod kątem relewancji i sentymentu.\"\"\"
        
        # 1. Wyciągnij kluczowe dane
        url = article.get("url")
        title = article.get("title")
        source = article.get("source")
        # Format daty z API: 20240101T123000
        time_published = article.get("time_published") 
        overall_sentiment_label = article.get("overall_sentiment_label", "Neutral")
        topics = article.get("topics", [])
        
        # Znajdź sentyment specyficzny dla TEGO tickera w liście ticker_sentiment
        # Artykuł może dotyczyć wielu spółek, szukamy "naszej"
        ticker_sentiment_list = article.get("ticker_sentiment", [])
        specific_sentiment = next((item for item in ticker_sentiment_list if item.get("ticker") == ticker), None)
        
        if not specific_sentiment:
            return # Artykuł nie dotyczy bezpośrednio tego tickera

        relevance_score = float(specific_sentiment.get("relevance_score", 0))
        ticker_score = float(specific_sentiment.get("ticker_sentiment_score", 0))
        ticker_label = specific_sentiment.get("ticker_sentiment_label", overall_sentiment_label)

        # 2. FILTR RELEWANCJI (Musi być > 0.6)
        if relevance_score < MIN_RELEVANCE_SCORE:
            return 

        # 3. FILTR TOPICS (Life Sciences & Biotech)
        # Dla branży Life Sciences obniżamy próg (ważne dla Bio-X / Pump Hunter)
        is_life_sciences = any(
            t.get("topic") == "Life Sciences" or "Mergers & Acquisitions" in t.get("topic") 
            for t in topics
        )
        
        threshold = LIFE_SCIENCES_SENTIMENT_THRESHOLD if is_life_sciences else DEFAULT_SENTIMENT_THRESHOLD
        
        # 4. FILTR SENTYMENTU
        # Interesuje nas tylko mocny sentyment (zarówno pozytywny jak i negatywny - zmienność)
        if abs(ticker_score) < threshold:
            return

        # 5. DEDUPLIKACJA (Sprawdź bazę)
        # Tworzymy unikalny hash newsa
        news_hash = self._generate_news_hash(url, title, source)
        
        # Szybkie sprawdzenie w bazie, czy już przetwarzaliśmy ten news dla tego tickera
        exists = self.session.query(models.ProcessedNews).filter_by(
            ticker=ticker, 
            news_hash=news_hash
        ).first()
        
        if exists:
            return # Już to widzieliśmy

        # 6. AKCJA: ZAPIS I ALERT
        self._save_news(ticker, news_hash, ticker_label, title, url)
        self.stats["articles_found"] += 1
        
        is_urgent = abs(ticker_score) >= URGENT_SENTIMENT_THRESHOLD
        priority_label = "🔥 PILNE" if is_urgent else "INFO"
        
        # Formatowanie wiadomości
        alert_msg = (
            f"[{priority_label}] {ticker}: {ticker_label} (Score: {ticker_score:.2f}, Rel: {relevance_score})\\n"
            f"Tytuł: {title}\\n"
            f"Link: {url}"
        )
        
        # A. Wyświetl w Aplikacji (System Alert - widoczny w UI)
        try:
            utils.update_system_control(self.session, "system_alert", alert_msg)
        except AttributeError:
            # Fallback dla starszych wersji utils
            try:
                utils.set_system_control_value(self.session, "system_alert", alert_msg)
            except:
                pass
        
        # B. Wyślij na Telegram (Bezpośrednio lub przez utils)
        # Sprawdzamy, czy utils ma gotową funkcję (aby nie dublować kodu), jeśli nie - używamy własnej
        if hasattr(utils, 'send_telegram_alert'):
            try:
                utils.send_telegram_alert(alert_msg)
                self.stats["alerts_sent"] += 1
            except Exception as e:
                logger.error(f"Błąd utils.send_telegram_alert: {e}")
        else:
            self._send_telegram(alert_msg)
        
        # C. Loguj w bazie (Dopisanie do notatki sygnału, jeśli istnieje aktywny setup)
        try:
            signal = self.session.query(models.TradingSignal).filter(
                models.TradingSignal.ticker == ticker,
                models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
            ).first()
            
            if signal:
                timestamp = datetime.now().strftime("%H:%M")
                safe_title = title.replace("'", "").replace('"', "")[:50]
                new_note = f"\\n[{timestamp}] NEWS: {ticker_label} - {safe_title}..."
                signal.notes = (signal.notes or "") + new_note
                self.session.commit()
        except Exception as e:
            logger.error(f"Błąd aktualizacji notatki sygnału: {e}")

        logger.info(f"NEWS ALERT ({ticker}): {title}")

    def _generate_news_hash(self, url, title, source):
        \"\"\"Tworzy unikalny hash dla newsa (MD5).\"\"\"
        raw_str = f"{url}|{title}|{source}"
        return hashlib.md5(raw_str.encode('utf-8')).hexdigest()

    def _save_news(self, ticker, news_hash, sentiment, headline, url):
        \"\"\"Zapisuje przetworzony news w bazie danych.\"\"\"
        try:
            news_entry = models.ProcessedNews(
                ticker=ticker,
                news_hash=news_hash,
                sentiment=sentiment,
                headline=headline,
                source_url=url,
                processed_at=datetime.now(timezone.utc)
            )
            self.session.add(news_entry)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Błąd zapisu newsa do DB: {e}")

    def _send_telegram(self, message):
        \"\"\"Wysyła powiadomienie na Telegram (Implementacja zapasowa).\"\"\"
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            # Brak konfiguracji = brak wysyłki (tylko logi)
            return

        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            # Krótki timeout (5s), aby nie blokować pętli Workera
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code == 200:
                self.stats["alerts_sent"] += 1
            else:
                logger.error(f"Telegram API Error: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"Błąd wysyłania Telegrama: {e}")

def run_news_agent_cycle(session, api_client):
    \"\"\"Funkcja wrapper uruchamiana przez Workera (schedule).\"\"\"
    scout = NewsScout(session, api_client)
    scout.run_cycle()
"""
