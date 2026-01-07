
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

# BATCH_SIZE 15 jest bezpieczny dla News Sentiment (ciężki endpoint)
BATCH_SIZE = 15                 
LOOKBACK_HOURS = 72             

# Filtry (Zgodne z Twoimi wytycznymi)
MIN_RELEVANCE_SCORE = 0.50      
MIN_SENTIMENT_SCORE = 0.25      

LAST_SCAN_KEY = 'news_agent_last_scan_time'

# ==================================================================
# FUNKCJE POMOCNICZE
# ==================================================================

def _create_smart_hash(ticker: str, headline: str, source: str) -> str:
    """Hashuje Ticker + Tytuł + Źródło (omija URL i czas)."""
    s = f"{ticker.strip().upper()}|{headline.strip().lower()}|{source.strip().lower()}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _check_if_news_processed(session: Session, ticker: str, news_hash: str) -> bool:
    try:
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        exists = session.scalar(
            select(func.count(ProcessedNews.id))
            .where(ProcessedNews.ticker == ticker)
            .where(ProcessedNews.news_hash == news_hash)
            .where(ProcessedNews.processed_at >= seven_days_ago)
        )
        return exists > 0
    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd DB (Check Dup): {e}")
        return False 

def _save_processed_news(session: Session, ticker: str, news_hash: str, sentiment: str, headline: str, url: str):
    try:
        entry = ProcessedNews(
            ticker=ticker,
            news_hash=news_hash,
            sentiment=sentiment,
            headline=headline[:1000] if headline else "",
            source_url=url[:1000] if url else ""
        )
        session.add(entry)
        session.commit() # Ważne: Commitujemy od razu
    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd DB (Insert): {e}")
        session.rollback()

def _get_time_from_param(session: Session) -> str:
    # ZAWSZE bierzemy okno przesuwne (ostatnie 72h), żeby wyłapać opóźnione newsy
    dt = datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)
    return dt.strftime('%Y%m%dT%H%M')

def _log_to_ui(session: Session, msg: str):
    """Pomocnicza funkcja do natychmiastowego zapisu logu w UI."""
    try:
        logger.info(msg)
        append_scan_log(session, msg)
        session.commit() # KLUCZOWE: Wymuszenie zapisu w bazie, żebyś widział to w UI
    except Exception as e:
        logger.error(f"Błąd logowania do UI: {e}")

# ==================================================================
# GŁÓWNA FUNKCJA WORKERA
# ==================================================================

def run_news_agent_cycle(session: Session, api_client: AlphaVantageClient):
    """
    Uruchamiana przez harmonogram (Schedule) w main.py.
    """
    # 1. LOG STARTOWY (Dla pewności w UI)
    _log_to_ui(session, "NEWS AGENT: 🟢 Start cyklu skanowania...")

    # 2. TELEGRAM PING (Dla pewności powiadomień)
    # Wyślij to raz, żeby sprawdzić rurę.
    try:
        # send_telegram_alert("📡 <b>NewsAgent:</b> Rozpoczynam nasłuch rynku...")
        pass # Odkomentuj jeśli chcesz spam przy każdym cyklu, na razie cisza
    except:
        pass

    try:
        # 3. POBIERANIE TICKERÓW
        try:
            phasex_tickers = set(session.scalars(select(PhaseXCandidate.ticker)).all())
            active_signals = session.scalars(select(TradingSignal.ticker).where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))).all()
            portfolio_tickers = set(session.scalars(select(PortfolioHolding.ticker)).all())
        except Exception as db_err:
            _log_to_ui(session, f"NEWS AGENT: 🔴 Błąd pobierania tickerów: {db_err}")
            return

        all_tickers = list(phasex_tickers.union(set(active_signals)).union(portfolio_tickers))
        all_tickers.sort()

        if not all_tickers:
            _log_to_ui(session, "NEWS AGENT: ⚠️ Lista tickerów jest PUSTA. Sprawdź skaner Fazy 1/X.")
            return

        time_from_str = _get_time_from_param(session)
        _log_to_ui(session, f"NEWS AGENT: Skanuję {len(all_tickers)} spółek (Okno: {LOOKBACK_HOURS}h).")

        processed_count = 0
        batches = [all_tickers[i:i + BATCH_SIZE] for i in range(0, len(all_tickers), BATCH_SIZE)]
        total_batches = len(batches)

        # 4. PĘTLA PO PACZKACH
        for i, batch in enumerate(batches):
            ticker_string = ",".join(batch)
            
            # Log co 5 paczek w UI, żebyś widział że żyje
            if i % 5 == 0 and i > 0:
                _log_to_ui(session, f"NEWS AGENT: Przetwarzanie... ({i}/{total_batches} paczek)")

            try:
                # API CALL
                news_data = api_client.get_news_sentiment(
                    ticker=ticker_string, 
                    limit=50, 
                    time_from=time_from_str
                )
            except Exception as e:
                logger.error(f"[NewsAgent] API Error Batch {i+1}: {e}")
                continue

            # Sleep 1.5s (Rate Limit Guard) - kluczowe dla uniknięcia błędów 429
            if len(batches) > 1:
                time.sleep(1.5)

            if not news_data or 'feed' not in news_data:
                continue

            # 5. ANALIZA
            for item in news_data.get('feed', []):
                headline = item.get('title', 'No Title')
                url = item.get('url', '#')
                source = item.get('source', 'Unknown')
                ticker_sentiment_list = item.get('ticker_sentiment', [])
                
                if not ticker_sentiment_list: continue

                # Hot Topics
                topics = item.get('topics', [])
                topic_tags = []
                is_hot_topic = False
                for t in topics:
                    t_name = t.get('topic', '')
                    if t_name in ['Earnings', 'Mergers & Acquisitions', 'Life Sciences']:
                        topic_tags.append(t_name)
                        is_hot_topic = True

                for ts_data in ticker_sentiment_list:
                    ticker = ts_data.get('ticker')
                    if ticker not in all_tickers: continue

                    # Deduplikacja (Ticker+Title+Source)
                    news_hash = _create_smart_hash(ticker, headline, source)
                    if _check_if_news_processed(session, ticker, news_hash):
                        continue

                    try:
                        relevance_score = float(ts_data.get('relevance_score', 0))
                        sentiment_score = float(ts_data.get('ticker_sentiment_score', 0))
                        sentiment_label = ts_data.get('ticker_sentiment_label', 'Neutral')
                    except: continue

                    # === FILTRY ===
                    
                    # 1. Relevance
                    req_relevance = MIN_RELEVANCE_SCORE
                    if is_hot_topic: req_relevance = 0.3 # Earningsy przepuszczamy łatwiej
                    
                    if relevance_score < req_relevance:
                        continue

                    # 2. Sentiment
                    req_sentiment = MIN_SENTIMENT_SCORE
                    if is_hot_topic: req_sentiment = 0.1 

                    if abs(sentiment_score) < req_sentiment:
                        continue

                    # === AKCJA: ALERT ===
                    alert_type = "NEWS"
                    if sentiment_score > 0.3: alert_type = "STRONG_BUY_NEWS"
                    elif sentiment_score < -0.3: alert_type = "STRONG_SELL_NEWS"

                    # Zapis do bazy
                    _save_processed_news(session, ticker, news_hash, alert_type, headline, url)

                    # Formatowanie
                    icon = "📰"
                    if sentiment_score > 0.4: icon = "🚀"
                    elif sentiment_score < -0.4: icon = "🔻"
                    elif is_hot_topic: icon = "🔥"

                    msg_body = (
                        f"{icon} <b>NEWS ALERT: {ticker}</b>\n"
                        f"Tytuł: {headline}\n"
                        f"Sentyment: {sentiment_label} ({sentiment_score:.2f})\n"
                        f"Relevance: {relevance_score:.2f} | Źródło: {source}\n"
                        f"{url}"
                    )

                    # Logowanie SUKCESU w UI
                    log_entry = f"NEWS FOUND: {ticker} (Sc:{sentiment_score:.2f}) | {headline[:40]}..."
                    _log_to_ui(session, f"✅ {log_entry}")

                    # Wysłanie Telegrama
                    try:
                        send_telegram_alert(msg_body)
                    except Exception as tele_err:
                        logger.error(f"Telegram FAIL: {tele_err}")

                    # Trigger System Alert
                    if sentiment_score >= 0.30 or is_hot_topic:
                        update_system_control(session, 'system_alert', f"{ticker}: {headline[:50]}...")

                    processed_count += 1

        # RAPORT KOŃCOWY W UI
        if processed_count > 0:
            _log_to_ui(session, f"NEWS AGENT: 🏁 Zakończono. Wysłano {processed_count} alertów.")
        else:
            _log_to_ui(session, "NEWS AGENT: 🏁 Zakończono. Brak nowych ważnych newsów.")

    except Exception as e:
        _log_to_ui(session, f"NEWS AGENT: 🔴 BŁĄD KRYTYCZNY: {e}")
        session.rollback()
