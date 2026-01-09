import logging
import time
import json
import hashlib
import os
import requests
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text

# === IMPORTY ===
from .. import models
from . import utils

logger = logging.getLogger(__name__)

TARGET_RPM = 100  
REQUEST_INTERVAL = 60.0 / TARGET_RPM  

# === TRYB TESTOWY: CYCN DEEP SCAN ===
# Ustawiamy 5000 minut (ok 3.5 dnia), żeby złapać newsa sprzed 3 dni
LOOKBACK_WINDOW_MINUTES = 5000  

MIN_RELEVANCE_SCORE = 0.60
DEFAULT_SENTIMENT_THRESHOLD = 0.30
LIFE_SCIENCES_SENTIMENT_THRESHOLD = 0.25 
URGENT_SENTIMENT_THRESHOLD = 0.45

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
        start_time = time.time()
        
        # === WYMUSZENIE TESTU DLA CYCN ===
        tickers = ['CYCN'] # <--- TYLKO CYCN
        
        msg_start = f"🧪 TEST NEWS: Skanowanie CYCN (Window: {LOOKBACK_WINDOW_MINUTES}m)..."
        logger.info(msg_start)
        utils.append_scan_log(self.session, msg_start)

        time_from_dt = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_WINDOW_MINUTES)
        time_from_str = time_from_dt.strftime('%Y%m%dT%H%M')

        for i, ticker in enumerate(tickers):
            try:
                self._process_ticker(ticker, time_from_str)
            except Exception as e:
                logger.error(f"Błąd przetwarzania newsów dla {ticker}: {e}")
                self.stats["errors"] += 1

            self.stats["processed_tickers"] += 1
            time.sleep(1) # Krótka przerwa

        duration = time.time() - start_time
        msg_end = f"🧪 TEST KONIEC: Znaleziono: {self.stats['articles_found']}, Alerty: {self.stats['alerts_sent']}."
        logger.info(msg_end)
        utils.append_scan_log(self.session, msg_end)

    def _process_ticker(self, ticker: str, time_from: str):
        data = self.api_client.get_news_sentiment(
            ticker=ticker,
            limit=50,
            time_from=time_from
        )

        if not data or "feed" not in data:
            logger.warning(f"Brak danych z API dla {ticker}")
            return

        feed = data.get("feed", [])
        logger.info(f"API zwróciło {len(feed)} artykułów dla {ticker}")
        
        for article in feed:
            self._analyze_article(ticker, article)

    def _analyze_article(self, ticker: str, article: dict):
        url = article.get("url")
        title = article.get("title", "")
        source = article.get("source", "")
        time_published = article.get("time_published") 
        overall_sentiment_label = article.get("overall_sentiment_label", "Neutral")
        
        # === TESTOWY BACKDOOR DLA CYCN ===
        # Jeśli to CYCN, wysyłamy alert BEZWARUNKOWO, żeby sprawdzić rury.
        if ticker == 'CYCN':
            # Deduplikacja nadal ważna, żeby nie spamować tym samym
            news_hash = self._generate_news_hash(url, title, source)
            exists = self.session.query(models.ProcessedNews).filter_by(ticker=ticker, news_hash=news_hash).first()
            
            if exists:
                logger.info(f"Pominięto duplikat dla CYCN: {title}")
                return 

            # Zapisz i Wyślij
            self._save_news(ticker, news_hash, overall_sentiment_label, title, url)
            self.stats["articles_found"] += 1
            self.stats["alerts_sent"] += 1
            
            alert_msg = (
                f"🧪 TEST SUKCES: Znaleziono news dla {ticker}!\n"
                f"Tytuł: {title}\n"
                f"Data: {time_published}\n"
                f"🔗 {url}"
            )
            
            utils.update_system_control(self.session, "system_alert", f"TEST {ticker}: {title[:40]}...")
            utils.send_telegram_alert(alert_msg)
            logger.info(f"WYSŁANO ALERT TELEGRAM DLA {ticker}")
            return
        # =================================

    def _generate_news_hash(self, url, title, source):
        raw_str = f"{url}|{title}|{source}"
        return hashlib.md5(raw_str.encode('utf-8')).hexdigest()

    def _save_news(self, ticker, news_hash, sentiment, headline, url):
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

def run_news_agent_cycle(session, api_client):
    scout = NewsScout(session, api_client)
    scout.run_cycle()
