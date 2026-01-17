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

# === KONFIGURACJA ===
TARGET_RPM = 100  
REQUEST_INTERVAL = 60.0 / TARGET_RPM  

# Szerokie okno czasowe (60 min) eliminuje "ślepe plamki" przy restarcie workera
LOOKBACK_WINDOW_MINUTES = 60  

# Progi decyzyjne
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
            "errors": 0,
            "rejected_dupe": 0,
            "rejected_score": 0,
            "updates_sent": 0
        }

    def run_cycle(self, specific_tickers=None):
        """
        Główna pętla agenta newsowego.
        """
        start_time = time.time()
        
        # 1. Pobierz listę tickerów (Biotech/Pharma)
        if specific_tickers:
            tickers = specific_tickers
        else:
            try:
                q_phasex = self.session.query(models.PhaseXCandidate.ticker).all()
                tickers = [t[0] for t in q_phasex]
                tickers = list(set(tickers))
            except Exception as e:
                logger.error(f"NEWS AGENT: Błąd pobierania listy Fazy X: {e}")
                tickers = []

        msg_start = f"NEWS: Start skanowania Fazy X ({len(tickers)} tickerów, {TARGET_RPM} RPM, Window: {LOOKBACK_WINDOW_MINUTES}m)..."
        logger.info(msg_start)
        utils.append_scan_log(self.session, msg_start)

        if not tickers:
            utils.append_scan_log(self.session, "NEWS: Brak tickerów w Fazie X. Kończę pracę.")
            return

        # 2. Okno czasowe
        time_from_dt = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_WINDOW_MINUTES)
        time_from_str = time_from_dt.strftime('%Y%m%dT%H%M')

        # 3. Iteracja
        for i, ticker in enumerate(tickers):
            step_start = time.time()
            try:
                self._process_ticker(ticker, time_from_str)
            except Exception as e:
                logger.error(f"Błąd przetwarzania newsów dla {ticker}: {e}")
                self.stats["errors"] += 1

            self.stats["processed_tickers"] += 1
            
            # Raportowanie postępu
            if (i + 1) % 20 == 0:
                progress_msg = f"NEWS: Przeanalizowano {i + 1}/{len(tickers)}..."
                utils.append_scan_log(self.session, progress_msg)
            
            # Pacing
            elapsed = time.time() - step_start
            sleep_time = max(0, REQUEST_INTERVAL - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        duration = time.time() - start_time
        
        # Podsumowanie
        found = self.stats['articles_found']
        alerts = self.stats['alerts_sent']
        updates = self.stats['updates_sent']
        dupes = self.stats['rejected_dupe']
        low_score = self.stats['rejected_score']
        
        if found > 0 or updates > 0:
            msg_end = f"NEWS: Koniec cyklu ({duration:.1f}s). ✅ Nowe: {found}, Aktualizacje: {updates}, Alerty: {alerts}."
        else:
            msg_end = f"NEWS: Koniec cyklu ({duration:.1f}s). Brak istotnych zmian (Duplikaty: {dupes}, Słaby Sentyment: {low_score})."
            
        logger.info(msg_end)
        utils.append_scan_log(self.session, msg_end)

    def _process_ticker(self, ticker: str, time_from: str):
        """Pobiera i analizuje newsy."""
        # FIX 1: Sortowanie LATEST (kluczowe dla wyłapania świeżych newsów)
        data = self.api_client.get_news_sentiment(
            ticker=ticker,
            limit=50,
            time_from=time_from
        ) # Parametr sort='LATEST' jest już dodany w metodzie get_news_sentiment w alpha_vantage_client.py

        if not data or "feed" not in data:
            return

        feed = data.get("feed", [])
        for article in feed:
            self._analyze_article(ticker, article)

    def _analyze_article(self, ticker: str, article: dict):
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

        # Filtr 1: Relewancja
        if relevance_score < MIN_RELEVANCE_SCORE:
            return 

        # Detekcja słów kluczowych (HOT)
        urgent_keywords = ["FDA", "CLINICAL", "TRIAL", "PHASE", "APPROVAL", "MERGER", "ACQUISITION", "PATENT", "BREAKTHROUGH"]
        title_upper = title.upper()
        is_biotech_hot = any(kw in title_upper for kw in urgent_keywords)
        
        is_life_sciences = any(
            t.get("topic") == "Life Sciences" or "Mergers & Acquisitions" in t.get("topic") 
            for t in topics
        )
        
        # Dynamiczny próg
        current_threshold = LIFE_SCIENCES_SENTIMENT_THRESHOLD if is_life_sciences else DEFAULT_SENTIMENT_THRESHOLD
        if is_life_sciences and is_biotech_hot:
            current_threshold = 0.15

        # Filtr 2: Sentyment
        if abs(ticker_score) < current_threshold:
            # FIX 3: Logowanie przyczyny odrzucenia (Debug)
            # logger.debug(f"Odrzucono {ticker}: Słaby sentyment {ticker_score:.2f} < {current_threshold}")
            self.stats["rejected_score"] += 1
            return

        # Filtr 3: Deduplikacja (Smart)
        news_hash = self._generate_news_hash(url, title, source)
        
        existing_news = self.session.query(models.ProcessedNews).filter_by(
            ticker=ticker, 
            news_hash=news_hash
        ).first()
        
        if existing_news:
            # FIX 2: Smart Dedup - Sprawdź czy warto zaktualizować
            # Jeśli news jest HOT (FDA) lub ma znacząco inny/lepszy sentyment niż poprzednio zapisany
            # (Tutaj uproszczamy: jeśli jest HOT lub bardzo silny sentyment, a minęło trochę czasu, przypominamy)
            
            should_update = False
            prev_label = existing_news.sentiment
            
            # Priorytety sentymentu
            sent_strength = {"Bearish": 3, "Somewhat-Bearish": 2, "Neutral": 1, "Somewhat-Bullish": 2, "Bullish": 3}
            
            new_strength = sent_strength.get(ticker_label, 1)
            old_strength = sent_strength.get(prev_label, 1)
            
            # Jeśli nowy sentyment jest silniejszy (np. z Neutral na Bullish)
            if new_strength > old_strength:
                should_update = True
                
            # Jeśli to "Biotech Hot" (np. FDA), zawsze warto o tym wiedzieć, nawet jak już był
            # Ale żeby nie spamować, robimy to tylko raz na jakiś czas dla danego hasha? 
            # W tym modelu (dedup po hash) hash jest stały dla treści.
            # Zatem aktualizacja ma sens tylko przy zmianie metadanych (sentymentu) przez dostawcę.
            
            if should_update:
                logger.info(f"Aktualizacja newsa dla {ticker}: {prev_label} -> {ticker_label}")
                existing_news.sentiment = ticker_label
                existing_news.processed_at = datetime.now(timezone.utc)
                self.session.commit()
                
                self._send_alert(ticker, ticker_label, ticker_score, relevance_score, title, url, is_biotech_hot, is_life_sciences, is_update=True)
                self.stats["updates_sent"] += 1
                return
            else:
                self.stats["rejected_dupe"] += 1
                return 

        # Nowy news - Zapisz i wyślij
        self._save_news(ticker, news_hash, ticker_label, title, url)
        self.stats["articles_found"] += 1
        self._send_alert(ticker, ticker_label, ticker_score, relevance_score, title, url, is_biotech_hot, is_life_sciences)

    def _send_alert(self, ticker, label, score, relevance, title, url, is_hot, is_life_science, is_update=False):
        """Wysyła powiadomienie na Telegram i do systemu."""
        
        # Priorytetyzacja etykiet
        if (is_life_science and is_hot) or abs(score) >= URGENT_SENTIMENT_THRESHOLD:
            priority_label = "🔥 BIOTECH HOT" if is_life_science else "🚀 PILNE"
        else:
            priority_label = "ℹ️ INFO"
            
        if is_update:
            priority_label += " (UPDATE)"
        
        # Ikona sentymentu
        sent_icon = "🟢" if score > 0 else "🔴"
        
        alert_msg = (
            f"[{priority_label}] {ticker}: {label} {sent_icon}\n"
            f"Score: {score:.2f} (Rel: {relevance:.2f})\n"
            f"{title}\n\n"
            f"🔗 {url}"
        )
        
        # A. Wyświetl w Aplikacji
        utils.update_system_control(self.session, "system_alert", f"{priority_label} {ticker}: {title[:40]}...")
        utils.append_scan_log(self.session, f"!!! NEWS ALERT: {ticker} - {title[:50]}...")
        
        # B. Wyślij na Telegram
        utils.send_telegram_alert(alert_msg)
        
        if is_update: return # Nie dodajemy notatki dla update'u, żeby nie śmiecić
        self.stats["alerts_sent"] += 1
        
        # C. Notatka do sygnału
        try:
            signal = self.session.query(models.TradingSignal).filter(
                models.TradingSignal.ticker == ticker,
                models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
            ).first()
            
            if signal:
                timestamp = datetime.now().strftime("%H:%M")
                safe_title = title.replace("'", "").replace('"', "")[:50]
                new_note = f"\n[{timestamp}] NEWS: {label} - {safe_title}..."
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
