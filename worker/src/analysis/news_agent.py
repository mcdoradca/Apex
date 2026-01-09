
import logging
import time
import json
import hashlib
import os
import requests
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text

# === TEST MODE START ===
if ticker == "CYNC":  # Wybierz ticker, który na pewno ma newsy
    utils.send_telegram_alert(f"🧪 TEST ALARMU: Znaleziono news dla {ticker}\nTytuł: {article.get('title')}\nLink: {article.get('url')}")
    logger.info(f"🧪 TEST ALARM WYSŁANY DLA {ticker}")
# === TEST MODE END ===

# === IMPORTY ===
# models są w katalogu wyżej (worker/src)
from .. import models
# utils są w tym samym katalogu (worker/src/analysis)
from . import utils

logger = logging.getLogger(__name__)

# === KONFIGURACJA ZGODNA Z SUPORTEM ALPHA VANTAGE (WARIANT B - STRICT PHASE X) ===
TARGET_RPM = 100  
REQUEST_INTERVAL = 60.0 / TARGET_RPM  

# === FIX 1: LIKWIDACJA ŚLEPEJ PLAMKI ===
# Zamiast 2 minut, patrzymy 60 minut wstecz.
# Deduplikacja (news_hash) w bazie danych zapobiegnie powtórnym alertom,
# a my mamy pewność, że żaden news nie ucieknie między cyklami.
LOOKBACK_WINDOW_MINUTES = 60  

# Progi decyzyjne dla Agenta
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
        """
        Główna pętla agenta newsowego.
        SKANUJE WYŁĄCZNIE FAZĘ X (Biotech/Pharma).
        """
        start_time = time.time()
        
        # 1. Pobierz listę tickerów - TYLKO FAZA X
        if specific_tickers:
            tickers = specific_tickers
        else:
            try:
                # ŚCISŁA REGUŁA: Tylko PhaseXCandidate (Biotech/Pharma)
                q_phasex = self.session.query(models.PhaseXCandidate.ticker).all()
                tickers = [t[0] for t in q_phasex]
                
                # Usuwamy ewentualne duplikaty
                tickers = list(set(tickers))

            except Exception as e:
                logger.error(f"NEWS AGENT: Błąd pobierania listy Fazy X: {e}")
                tickers = []

        # LOGOWANIE DO UI (Dziennik Operacyjny) - START
        msg_start = f"NEWS: Start skanowania Fazy X ({len(tickers)} tickerów, {TARGET_RPM} RPM, Window: {LOOKBACK_WINDOW_MINUTES}m)..."
        logger.info(msg_start)
        utils.append_scan_log(self.session, msg_start)

        if not tickers:
            utils.append_scan_log(self.session, "NEWS: Brak tickerów w Fazie X. Kończę pracę.")
            return

        # 2. Ustalenie okna czasowego (time_from)
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
            
            # Raportowanie postępu co 20 tickerów
            if (i + 1) % 20 == 0:
                progress_msg = f"NEWS: Przeanalizowano {i + 1}/{len(tickers)}..."
                utils.append_scan_log(self.session, progress_msg)
            
            # PACING
            elapsed = time.time() - step_start
            sleep_time = max(0, REQUEST_INTERVAL - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        duration = time.time() - start_time
        
        # LOGOWANIE DO UI - KONIEC
        if self.stats['articles_found'] > 0:
            msg_end = f"NEWS: Koniec cyklu ({duration:.1f}s). ✅ Znaleziono: {self.stats['articles_found']} newsów."
        else:
            msg_end = f"NEWS: Koniec cyklu ({duration:.1f}s). Brak nowych wiadomości."
            
        logger.info(msg_end)
        utils.append_scan_log(self.session, msg_end)

    def _process_ticker(self, ticker: str, time_from: str):
        """Pobiera i analizuje newsy dla pojedynczego tickera."""
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
        """Analizuje pojedynczy artykuł pod kątem relewancji i sentymentu."""
        url = article.get("url")
        title = article.get("title", "")
        source = article.get("source", "")
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

        # Filtr 1: Relewancja (Musi dotyczyć tej spółki, a nie tylko o niej wspominać)
        if relevance_score < MIN_RELEVANCE_SCORE:
            return 

        # === FIX 2: KEYWORD BOOST (BIOTECH) ===
        # Wykrywanie słów kluczowych dla branży Life Sciences
        urgent_keywords = ["FDA", "CLINICAL", "TRIAL", "PHASE", "APPROVAL", "MERGER", "ACQUISITION", "PATENT", "BREAKTHROUGH"]
        title_upper = title.upper()
        
        is_biotech_hot = any(kw in title_upper for kw in urgent_keywords)
        
        # Sprawdzanie tematów API
        is_life_sciences = any(
            t.get("topic") == "Life Sciences" or "Mergers & Acquisitions" in t.get("topic") 
            for t in topics
        )
        
        # Dynamiczny próg sentymentu
        threshold = LIFE_SCIENCES_SENTIMENT_THRESHOLD if is_life_sciences else DEFAULT_SENTIMENT_THRESHOLD
        
        # Jeśli news zawiera słowo kluczowe (np. FDA), obniżamy próg sentymentu prawie do zera,
        # bo każdy news o FDA jest ważny (nawet neutralny/mixed).
        if is_biotech_hot:
            threshold = 0.1

        # Filtr 3: Sentyment
        if abs(ticker_score) < threshold:
            return

        # Filtr 4: Deduplikacja
        news_hash = self._generate_news_hash(url, title, source)
        
        exists = self.session.query(models.ProcessedNews).filter_by(
            ticker=ticker, 
            news_hash=news_hash
        ).first()
        
        if exists:
            return 

        # Znaleziono newsa! Zapisz i alarmuj.
        self._save_news(ticker, news_hash, ticker_label, title, url)
        self.stats["articles_found"] += 1
        
        # === FIX 3: FORMATOWANIE ALERTU ===
        # Priorytetyzacja etykiet
        if is_biotech_hot or abs(ticker_score) >= URGENT_SENTIMENT_THRESHOLD:
            priority_label = "🔥 BIOTECH HOT" if is_life_sciences else "🚀 PILNE"
        else:
            priority_label = "ℹ️ INFO"
        
        # Ikona sentymentu
        sent_icon = "🟢" if ticker_score > 0 else "🔴"
        
        alert_msg = (
            f"{priority_label}: {ticker} {sent_icon}\n"
            f"Sentyment: {ticker_label} ({ticker_score:.2f})\n"
            f"{title}\n\n"
            f"🔗 {url}"
        )
        
        # A. Wyświetl w Aplikacji (System Alert - krótki)
        utils.update_system_control(self.session, "system_alert", f"{priority_label} {ticker}: {title[:40]}...")
        utils.append_scan_log(self.session, f"!!! NEWS ALERT: {ticker} - {title[:50]}...")
        
        # B. Wyślij na Telegram (Pełny)
        utils.send_telegram_alert(alert_msg)
        self.stats["alerts_sent"] += 1
        
        # C. Notatka do sygnału (Context injection)
        try:
            signal = self.session.query(models.TradingSignal).filter(
                models.TradingSignal.ticker == ticker,
                models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
            ).first()
            
            if signal:
                timestamp = datetime.now().strftime("%H:%M")
                safe_title = title.replace("'", "").replace('"', "")[:40]
                new_note = f"\n[{timestamp}] NEWS {sent_icon}: {safe_title}..."
                signal.notes = (signal.notes or "") + new_note
                self.session.commit()
        except Exception as e:
            logger.error(f"Błąd aktualizacji notatki sygnału: {e}")

        logger.info(f"NEWS ALERT ({ticker}): {title}")

    def _generate_news_hash(self, url, title, source):
        """Tworzy unikalny hash dla newsa (MD5)."""
        raw_str = f"{url}|{title}|{source}"
        return hashlib.md5(raw_str.encode('utf-8')).hexdigest()

    def _save_news(self, ticker, news_hash, sentiment, headline, url):
        """Zapisuje przetworzony news w bazie danych."""
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
    """Funkcja wrapper uruchamiana przez Workera (schedule)."""
    scout = NewsScout(session, api_client)
    scout.run_cycle()
