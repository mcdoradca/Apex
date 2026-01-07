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
# Importujemy utils z tego samego katalogu
from . import utils

logger = logging.getLogger(__name__)

# === KONFIGURACJA ZGODNA Z SUPORTEM ALPHA VANTAGE (WARIANT B - ZMODYFIKOWANY) ===
# Zmniejszamy z 120 na 100 RPM, aby uniknąć ciągłego "Sleeping 15s" widocznego w logach.
# Stabilne 100 jest szybsze niż rwane 120.
TARGET_RPM = 100  
REQUEST_INTERVAL = 60.0 / TARGET_RPM  
LOOKBACK_WINDOW_MINUTES = 2  

# Progi decyzyjne dla Agenta
MIN_RELEVANCE_SCORE = 0.60
DEFAULT_SENTIMENT_THRESHOLD = 0.30
LIFE_SCIENCES_SENTIMENT_THRESHOLD = 0.25 
URGENT_SENTIMENT_THRESHOLD = 0.45

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
        """
        Główna pętla agenta newsowego.
        """
        start_time = time.time()
        
        # 1. Pobierz listę tickerów
        if specific_tickers:
            tickers = specific_tickers
        else:
            try:
                q_phasex = self.session.query(models.PhaseXCandidate.ticker).all()
                q_phase1 = self.session.query(models.Phase1Candidate.ticker).all()
                tickers = list(set([t[0] for t in q_phasex] + [t[0] for t in q_phase1]))
                
                if not tickers:
                    q_companies = self.session.query(models.Company.ticker).limit(200).all()
                    tickers = [t[0] for t in q_companies]
            except Exception as e:
                logger.error(f"NEWS AGENT: Błąd pobierania listy tickerów: {e}")
                tickers = []

        # LOGOWANIE DO UI (Dziennik Operacyjny) - START
        msg_start = f"NEWS: Start skanowania {len(tickers)} tickerów (Cel: {TARGET_RPM} RPM)..."
        logger.info(msg_start)
        utils.append_scan_log(self.session, msg_start)

        # 2. Ustalenie okna czasowego
        time_from_dt = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_WINDOW_MINUTES)
        time_from_str = time_from_dt.strftime('%Y%m%dT%H%M')

        # 3. Iteracja po tickerach
        for i, ticker in enumerate(tickers):
            step_start = time.time()
            
            try:
                self._process_ticker(ticker, time_from_str)
            except Exception as e:
                logger.error(f"Błąd przetwarzania newsów dla {ticker}: {e}")
                self.stats["errors"] += 1

            self.stats["processed_tickers"] += 1
            
            # Raportowanie postępu co 50 tickerów do UI, żebyś widział że żyje
            if (i + 1) % 50 == 0:
                progress_msg = f"NEWS: Przeanalizowano {i + 1}/{len(tickers)}..."
                utils.append_scan_log(self.session, progress_msg)
            
            # PACING
            elapsed = time.time() - step_start
            sleep_time = max(0, REQUEST_INTERVAL - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        duration = time.time() - start_time
        
        # LOGOWANIE DO UI - KONIEC
        msg_end = f"NEWS: Koniec cyklu ({duration:.1f}s). Znaleziono: {self.stats['articles_found']}, Alerty: {self.stats['alerts_sent']}."
        logger.info(msg_end)
        utils.append_scan_log(self.session, msg_end)

    def _process_ticker(self, ticker: str, time_from: str):
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
        url = article.get("url")
        title = article.get("title")
        source = article.get("source")
        time_published = article.get("time_published") 
        overall_sentiment_label = article.get("overall_sentiment_label", "Neutral")
        topics = article.get("topics", [])
        
        ticker_sentiment_list = article.get("ticker_sentiment", [])
        specific_sentiment = next((item for item in ticker_sentiment_list if item.get("ticker") == ticker), None)
        
        if not specific_sentiment:
            return 

        relevance_score = float(specific_sentiment.get("relevance_score", 0))
        ticker_score = float(specific_sentiment.get("ticker_sentiment_score", 0))
        ticker_label = specific_sentiment.get("ticker_sentiment_label", overall_sentiment_label)

        if relevance_score < MIN_RELEVANCE_SCORE:
            return 

        is_life_sciences = any(
            t.get("topic") == "Life Sciences" or "Mergers & Acquisitions" in t.get("topic") 
            for t in topics
        )
        
        threshold = LIFE_SCIENCES_SENTIMENT_THRESHOLD if is_life_sciences else DEFAULT_SENTIMENT_THRESHOLD
        
        if abs(ticker_score) < threshold:
            return

        news_hash = self._generate_news_hash(url, title, source)
        
        exists = self.session.query(models.ProcessedNews).filter_by(
            ticker=ticker, 
            news_hash=news_hash
        ).first()
        
        if exists:
            return 

        # Znaleziono newsa!
        self._save_news(ticker, news_hash, ticker_label, title, url)
        self.stats["articles_found"] += 1
        
        is_urgent = abs(ticker_score) >= URGENT_SENTIMENT_THRESHOLD
        priority_label = "🔥 PILNE" if is_urgent else "INFO"
        
        alert_msg = (
            f"[{priority_label}] {ticker}: {ticker_label} (Score: {ticker_score:.2f}, Rel: {relevance_score})\n"
            f"Tytuł: {title}\n"
            f"Link: {url}"
        )
        
        # A. Wyświetl w Aplikacji (System Alert - Czerwona belka)
        try:
            utils.update_system_control(self.session, "system_alert", alert_msg)
            # Dodatkowo wpis do logu operacyjnego
            utils.append_scan_log(self.session, f"NEWS ALERT: {ticker} - {title[:30]}...")
        except AttributeError:
            try:
                utils.set_system_control_value(self.session, "system_alert", alert_msg)
            except:
                pass
        
        # B. Wyślij na Telegram
        if hasattr(utils, 'send_telegram_alert'):
            try:
                utils.send_telegram_alert(alert_msg)
                self.stats["alerts_sent"] += 1
            except Exception as e:
                logger.error(f"Błąd utils.send_telegram_alert: {e}")
        else:
            self._send_telegram(alert_msg)
        
        # C. Notatka do sygnału
        try:
            signal = self.session.query(models.TradingSignal).filter(
                models.TradingSignal.ticker == ticker,
                models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
            ).first()
            
            if signal:
                timestamp = datetime.now().strftime("%H:%M")
                safe_title = title.replace("'", "").replace('"', "")[:50]
                new_note = f"\n[{timestamp}] NEWS: {ticker_label} - {safe_title}..."
                signal.notes = (signal.notes or "") + new_note
                self.session.commit()
        except Exception as e:
            logger.error(f"Błąd aktualizacji notatki sygnału: {e}")

        logger.info(f"NEWS ALERT ({ticker}): {title}")

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

    def _send_telegram(self, message):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return

        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            requests.post(url, json=payload, timeout=5)
            self.stats["alerts_sent"] += 1
        except Exception as e:
            logger.error(f"Błąd wysyłania Telegrama: {e}")

def run_news_agent_cycle(session, api_client):
    scout = NewsScout(session, api_client)
    scout.run_cycle()
