
import logging
import hashlib
import time
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, select, func

# Importy modeli
from ..models import TradingSignal, ProcessedNews, PortfolioHolding, PhaseXCandidate

# Importy narzędziowe
from ..analysis.utils import update_system_control, get_system_control_value, send_telegram_alert, append_scan_log
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

logger = logging.getLogger(__name__)

# ==================================================================
# KONFIGURACJA AGENTA
# ==================================================================

BATCH_SIZE = 50                 # Limit Alpha Vantage na jeden request
# ZMNIEJSZONO PROGI, ABY WYŁAPYWAĆ WIĘCEJ SYGNAŁÓW
MIN_RELEVANCE_SCORE = 0.40      # Obniżono z 0.60
MIN_SENTIMENT_SCORE = 0.15      # Obniżono z 0.20 (żeby łapać "Somewhat-Bullish")

# Klucz w system_control do przechowywania czasu ostatniego skanu
LAST_SCAN_KEY = 'news_agent_last_scan_time'

class NewsAgent:
    def __init__(self, session: Session, api_client: AlphaVantageClient):
        self.session = session
        self.client = api_client
        self.is_active = True

    def _create_news_hash(self, headline: str, uri: str) -> str:
        """Tworzy unikalny hash SHA-256 dla wiadomości."""
        s = f"{headline.strip()}{uri.strip()}"
        return hashlib.sha256(s.encode('utf-8')).hexdigest()

    def _check_if_news_processed(self, ticker: str, news_hash: str) -> bool:
        """Sprawdza, czy dany news był już przetwarzany w ciągu ostatnich 7 dni."""
        try:
            seven_days_ago = datetime.utcnow() - timedelta(days=7)
            exists = self.session.scalar(
                select(func.count(ProcessedNews.id))
                .where(ProcessedNews.ticker == ticker)
                .where(ProcessedNews.news_hash == news_hash)
                .where(ProcessedNews.processed_at >= seven_days_ago)
            )
            return exists > 0
        except Exception as e:
            logger.error(f"Agent Newsowy: Błąd sprawdzenia duplikatu dla {ticker}: {e}")
            return False 

    def _save_processed_news(self, ticker: str, news_hash: str, sentiment: str, headline: str, url: str):
        """Zapisuje przetworzony news do bazy danych."""
        try:
            entry = ProcessedNews(
                ticker=ticker,
                news_hash=news_hash,
                sentiment=sentiment,
                headline=headline[:1000] if headline else "",
                source_url=url[:1000] if url else ""
            )
            self.session.add(entry)
            self.session.commit()
        except Exception as e:
            logger.error(f"Agent Newsowy: Błąd zapisu newsa dla {ticker}: {e}")
            self.session.rollback()

    def _get_time_from_param(self) -> str:
        """Pobiera timestamp ostatniego skanu (YYYYMMDDTHHMM)."""
        last_val = get_system_control_value(self.session, LAST_SCAN_KEY)
        if last_val:
            return last_val
        else:
            # Domyślnie 48h wstecz przy pierwszym uruchomieniu
            dt = datetime.utcnow() - timedelta(hours=48)
            return dt.strftime('%Y%m%dT%H%M')

    def _update_last_scan_time_to_now(self, current_dt: datetime):
        """Aktualizuje znacznik czasu w bazie."""
        fmt = current_dt.strftime('%Y%m%dT%H%M')
        update_system_control(self.session, LAST_SCAN_KEY, fmt)

    def run_news_monitor(self):
        """
        Główna pętla monitorująca (kompatybilna z main.py).
        Skanuje BioX (Faza X) + Aktywne Sygnały + Portfel.
        """
        logger.info("[NewsAgent] Uruchamianie cyklu monitorowania...")
        
        try:
            # 1. Pobierz listy monitorowanych tickerów
            # Używamy try-except, bo tabele mogą być puste na starcie
            try:
                phasex_tickers = set(self.session.scalars(select(PhaseXCandidate.ticker)).all())
                active_signals = self.session.scalars(select(TradingSignal.ticker).where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))).all()
                portfolio_tickers = self.session.scalars(select(PortfolioHolding.ticker)).all()
            except Exception as db_err:
                logger.error(f"[NewsAgent] Błąd pobierania tickerów z bazy: {db_err}")
                return

            standard_tickers = set(active_signals + portfolio_tickers)
            all_tickers = list(phasex_tickers.union(standard_tickers))
            all_tickers.sort()

            if not all_tickers:
                msg = "⚠️ Agent Newsowy: Brak spółek do monitorowania! (Tabele puste). Radar wyłączony."
                logger.warning(msg)
                append_scan_log(self.session, msg)
                return

            # 2. Logika Czasowa
            scan_start_time = datetime.utcnow()
            time_from_str = self._get_time_from_param()

            start_msg = (
                f"Agent Newsowy: Skanowanie {len(all_tickers)} spółek od {time_from_str}. "
                f"(Filtry: Rev>{MIN_RELEVANCE_SCORE}, Sent>{MIN_SENTIMENT_SCORE})"
            )
            logger.info(start_msg)
            # append_scan_log(self.session, start_msg) # Opcjonalnie włącz dla verbose UI

            processed_count = 0
            alerts_sent = 0
            
            # 3. Batching Loop
            batches = [all_tickers[i:i + BATCH_SIZE] for i in range(0, len(all_tickers), BATCH_SIZE)]

            for i, batch in enumerate(batches):
                ticker_string = ",".join(batch)
                
                try:
                    # limit=1000, time_from załatwia przyrostowość
                    news_data = self.client.get_news_sentiment(
                        ticker=ticker_string, 
                        limit=1000, 
                        time_from=time_from_str
                    )
                except Exception as e:
                    logger.error(f"Agent Newsowy: Błąd API dla paczki {i+1}: {e}")
                    continue

                if len(batches) > 1:
                    time.sleep(0.25) # Lekkie opóźnienie dla API

                if not news_data or 'feed' not in news_data:
                    continue

                # 4. Przetwarzanie Feed-u
                for item in news_data.get('feed', []):
                    headline = item.get('title', 'No Title')
                    url = item.get('url', '#')
                    ticker_sentiment_list = item.get('ticker_sentiment', [])
                    
                    if not ticker_sentiment_list: continue

                    topics = item.get('topics', [])
                    topic_tags = []
                    is_hot_topic = False
                    for t in topics:
                        t_name = t.get('topic', '')
                        if t_name in ['Earnings', 'Mergers & Acquisitions', 'Life Sciences']:
                            topic_tags.append(t_name)
                            is_hot_topic = True

                    # Iteracja po tickerach
                    for ts_data in ticker_sentiment_list:
                        ticker = ts_data.get('ticker')
                        
                        if ticker not in all_tickers:
                            continue

                        news_hash = self._create_news_hash(headline + ticker, url)
                        if self._check_if_news_processed(ticker, news_hash):
                            continue

                        try:
                            relevance_score = float(ts_data.get('relevance_score', 0))
                            sentiment_score = float(ts_data.get('ticker_sentiment_score', 0))
                            sentiment_label = ts_data.get('ticker_sentiment_label', 'Neutral')
                        except (ValueError, TypeError):
                            continue

                        # === LOGIKA DECYZYJNA ===

                        # Warunek 1: Relevance
                        if relevance_score < MIN_RELEVANCE_SCORE:
                            if relevance_score > 0.2: # Loguj tylko sensowne
                                logger.debug(f"SKIP {ticker}: Low Relevance {relevance_score:.2f} < {MIN_RELEVANCE_SCORE}")
                            continue
                        
                        # Warunek 2: Sentiment
                        # Hot Topic przepuszcza słabsze newsy (np. 0.05)
                        threshold = MIN_SENTIMENT_SCORE
                        if is_hot_topic:
                            threshold = 0.05

                        if sentiment_score <= threshold:
                            if sentiment_score > -0.1: # Loguj bliskie zera
                                 logger.info(f"SKIP {ticker}: Low Sentiment {sentiment_score:.2f} <= {threshold} | {headline[:40]}...")
                            continue

                        # === ALERT ===
                        alerts_sent += 1
                        
                        alert_emoji = "🚀" if sentiment_score >= 0.4 else "📈"
                        if is_hot_topic: alert_emoji = "🔥"

                        alert_type = "POSITIVE_NEWS"
                        topic_str = f" | {', '.join(topic_tags)}" if topic_tags else ""

                        self._save_processed_news(ticker, news_hash, alert_type, headline, url)
                        
                        clean_msg = (
                            f"{alert_emoji} <b>NEWS ALERT: {ticker}</b>\n"
                            f"Sentyment: {sentiment_label} (Score: {sentiment_score})\n"
                            f"Relevance: {relevance_score}{topic_str}\n\n"
                            f"<b>{headline}</b>\n"
                            f"{url}"
                        )
                        
                        log_msg = f"NEWS: {ticker} (Sc:{sentiment_score}) | {headline[:50]}..."
                        append_scan_log(self.session, log_msg)
                        logger.info(f"✅ ALERT SENT: {log_msg}")
                        
                        # Wysłanie Alertu (Telegram)
                        send_telegram_alert(clean_msg)
                        
                        # System Alert w UI
                        if sentiment_score >= 0.35 or is_hot_topic:
                            update_system_control(self.session, 'system_alert', f"{ticker}: {headline[:60]}...")

                        processed_count += 1

            # 5. Aktualizacja czasu
            self._update_last_scan_time_to_now(scan_start_time)

            if processed_count > 0:
                logger.info(f"[NewsAgent] Znaleziono {processed_count} newsów.")
                self.session.commit()
            else:
                logger.debug("[NewsAgent] Brak nowych newsów.")

        except Exception as e:
            logger.error(f"[NewsAgent] Błąd krytyczny: {e}", exc_info=True)
            self.session.rollback()
