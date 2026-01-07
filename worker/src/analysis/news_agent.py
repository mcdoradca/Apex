
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

BATCH_SIZE = 50                 
# SKORYGOWANE FILTRY (Zgodnie z Twoim poleceniem)
MIN_RELEVANCE_SCORE = 0.40      # Obniżono z 0.60
MIN_SENTIMENT_SCORE = 0.15      # Obniżono z 0.20

LAST_SCAN_KEY = 'news_agent_last_scan_time'

# ==================================================================
# FUNKCJE POMOCNICZE
# ==================================================================

def _create_news_hash(headline: str, uri: str) -> str:
    s = f"{headline.strip()}{uri.strip()}"
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
        logger.error(f"Agent Newsowy: Błąd duplikatu {ticker}: {e}")
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
        session.commit()
    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd zapisu DB {ticker}: {e}")
        session.rollback()

def _get_time_from_param(session: Session) -> str:
    last_val = get_system_control_value(session, LAST_SCAN_KEY)
    if last_val:
        return last_val
    else:
        # 48h wstecz na start
        dt = datetime.utcnow() - timedelta(hours=48)
        return dt.strftime('%Y%m%dT%H%M')

def _update_last_scan_time_to_now(session: Session, current_dt: datetime):
    fmt = current_dt.strftime('%Y%m%dT%H%M')
    update_system_control(session, LAST_SCAN_KEY, fmt)

# ==================================================================
# GŁÓWNA FUNKCJA WORKERA (Run Cycle)
# ==================================================================

def run_news_agent_cycle(session: Session, api_client: AlphaVantageClient):
    """
    Funkcja wykonywana przez harmonogram (Schedule) w main.py.
    """
    try:
        # 1. Pobieranie Tickerów
        try:
            phasex_tickers = set(session.scalars(select(PhaseXCandidate.ticker)).all())
            active_signals = session.scalars(select(TradingSignal.ticker).where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))).all()
            portfolio_tickers = session.scalars(select(PortfolioHolding.ticker)).all()
        except Exception as db_err:
            logger.error(f"[NewsAgent] Błąd bazy danych: {db_err}")
            return

        standard_tickers = set(active_signals + portfolio_tickers)
        all_tickers = list(phasex_tickers.union(standard_tickers))
        all_tickers.sort()

        # === DIAGNOSTYKA: LOGUJEMY JEŚLI LISTA PUSTA ===
        if not all_tickers:
            msg = "⚠️ Agent Newsowy: Lista monitorowanych spółek jest PUSTA. Sprawdź Fazy Skanowania."
            logger.warning(msg)
            # append_scan_log(session, msg) # Odkomentuj jeśli chcesz to widzieć w UI
            return

        # 2. Logika Czasu
        scan_start_time = datetime.utcnow()
        time_from_str = _get_time_from_param(session)

        # Log startowy w konsoli (potwierdzenie że żyje)
        logger.info(f"[NewsAgent] Start skanu: {len(all_tickers)} spółek od {time_from_str}")
        
        processed_count = 0
        
        # 3. Pętla po paczkach (Batching)
        batches = [all_tickers[i:i + BATCH_SIZE] for i in range(0, len(all_tickers), BATCH_SIZE)]

        for i, batch in enumerate(batches):
            ticker_string = ",".join(batch)
            
            try:
                # Zapytanie do API
                news_data = api_client.get_news_sentiment(
                    ticker=ticker_string, 
                    limit=1000, 
                    time_from=time_from_str
                )
            except Exception as e:
                logger.error(f"Agent Newsowy: Błąd API (Batch {i+1}): {e}")
                continue

            if len(batches) > 1:
                time.sleep(0.2) 

            if not news_data or 'feed' not in news_data:
                continue

            # 4. Analiza Newsów
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

                for ts_data in ticker_sentiment_list:
                    ticker = ts_data.get('ticker')
                    
                    if ticker not in all_tickers:
                        continue

                    news_hash = _create_news_hash(headline + ticker, url)
                    if _check_if_news_processed(session, ticker, news_hash):
                        continue

                    try:
                        relevance_score = float(ts_data.get('relevance_score', 0))
                        sentiment_score = float(ts_data.get('ticker_sentiment_score', 0))
                        sentiment_label = ts_data.get('ticker_sentiment_label', 'Neutral')
                    except: continue

                    # === FILTRY (TERAZ ŁAGODNIEJSZE) ===
                    
                    # 1. Relevance
                    if relevance_score < MIN_RELEVANCE_SCORE:
                        # Loguj tylko te warte uwagi (np. > 0.2), żeby nie śmiecić
                        if relevance_score > 0.2:
                            logger.debug(f"SKIP {ticker}: Rel {relevance_score:.2f} < {MIN_RELEVANCE_SCORE}")
                        continue
                    
                    # 2. Sentiment
                    # Dla Hot Topics obniżamy próg do prawie zera (0.05)
                    threshold = 0.05 if is_hot_topic else MIN_SENTIMENT_SCORE

                    if sentiment_score <= threshold:
                        # Loguj bliskie odrzucenia
                        if sentiment_score > -0.15:
                             logger.debug(f"SKIP {ticker}: Sent {sentiment_score:.2f} <= {threshold} | {headline[:30]}...")
                        continue

                    # === ALERT ===
                    alert_emoji = "🚀" if sentiment_score >= 0.35 else "📈"
                    if is_hot_topic: alert_emoji = "🔥"

                    alert_type = "POSITIVE_NEWS"
                    topic_str = f" | {', '.join(topic_tags)}" if topic_tags else ""

                    # Zapis
                    _save_processed_news(session, ticker, news_hash, alert_type, headline, url)
                    
                    # Wiadomość
                    clean_msg = (
                        f"{alert_emoji} <b>NEWS: {ticker}</b>\n"
                        f"Sent: {sentiment_label} ({sentiment_score})\n"
                        f"Rel: {relevance_score}{topic_str}\n"
                        f"<a href='{url}'>{headline}</a>"
                    )
                    
                    # Logi
                    log_msg = f"NEWS: {ticker} (Sc:{sentiment_score}) | {headline[:40]}..."
                    logger.info(f"✅ ALERT: {log_msg}")
                    append_scan_log(session, log_msg)
                    
                    # Telegram
                    send_telegram_alert(clean_msg)
                    
                    # UI System Alert (Tylko mocne)
                    if sentiment_score >= 0.30 or is_hot_topic:
                        update_system_control(session, 'system_alert', f"{ticker}: {headline[:50]}...")

                    processed_count += 1

        # 5. Aktualizacja czasu
        _update_last_scan_time_to_now(session, scan_start_time)

        # Raport końcowy (Dla pewności "Znaku Życia")
        if processed_count > 0:
            logger.info(f"[NewsAgent] Cykl zakończony. Wysłano {processed_count} powiadomień.")
            session.commit()
        else:
            # Ważne: Logujemy też brak wyników, żebyś wiedział że skan przeszedł!
            logger.info("[NewsAgent] Cykl zakończony. Brak nowych newsów spełniających kryteria.")

    except Exception as e:
        logger.error(f"[NewsAgent] CRITICAL ERROR: {e}", exc_info=True)
        session.rollback()
