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
# KONFIGURACJA AGENTA (ZGODNA Z SUGESTIAMI SUPPORTU AV)
# ==================================================================

BATCH_SIZE = 15                 # Małe paczki, żeby uniknąć Rate Limit
LOOKBACK_HOURS = 72             # Okno przesuwne (zamiast sztywnego last_scan)
LIMIT_PER_REQ = 50              # Więcej newsów na request

# Nowe Progi (Wyższe, ale pewniejsze)
MIN_RELEVANCE_SCORE = 0.50      
MIN_SENTIMENT_SCORE = 0.25      

# ==================================================================
# FUNKCJE POMOCNICZE
# ==================================================================

def _create_smart_hash(ticker: str, headline: str, source: str) -> str:
    """
    Deduplikacja Hybrydowa: Ticker + Tytuł + Źródło.
    Ignoruje URL (który może się zmieniać) i czas (który może być przesunięty).
    """
    # Normalizacja stringów (małe litery, bez spacji na końcach)
    s = f"{ticker.strip().upper()}|{headline.strip().lower()}|{source.strip().lower()}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _check_if_news_processed(session: Session, ticker: str, news_hash: str) -> bool:
    """Sprawdza w bazie, czy ten konkretny news (hash treści) już był."""
    try:
        # Sprawdzamy historię z 7 dni (żeby nie trzymać wiecznie hashy)
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
    """Rejestruje news w bazie, aby nie wysłać go drugi raz."""
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
        logger.error(f"Agent Newsowy: Błąd DB (Insert): {e}")
        session.rollback()

# ==================================================================
# GŁÓWNA FUNKCJA WORKERA
# ==================================================================

def run_news_agent_cycle(session: Session, api_client: AlphaVantageClient):
    """
    Wykonywane przez Schedule w main.py.
    """
    # Log startowy
    logger.info(f"[NewsAgent] >>> START CYKLU (Window: {LOOKBACK_HOURS}h, Rel>{MIN_RELEVANCE_SCORE})")

    try:
        # 1. Pobieranie Tickerów
        try:
            phasex_tickers = set(session.scalars(select(PhaseXCandidate.ticker)).all())
            active_signals = session.scalars(select(TradingSignal.ticker).where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))).all()
            portfolio_tickers = set(session.scalars(select(PortfolioHolding.ticker)).all())
        except Exception as db_err:
            logger.error(f"[NewsAgent] Błąd bazy danych przy pobieraniu tickerów: {db_err}")
            return

        # Łączenie list
        all_tickers = list(phasex_tickers.union(set(active_signals)).union(portfolio_tickers))
        all_tickers.sort()

        if not all_tickers:
            logger.warning("[NewsAgent] Lista tickerów jest PUSTA. Nic do roboty.")
            return

        # 2. Testowy Ping Telegrama (Tylko raz na uruchomienie workera, można sterować flagą w bazie, ale tu zrobimy zawsze przy starcie cyklu DEBUGOWO)
        # Aby nie spamować, sprawdzamy czy wysłaliśmy już "ping" w ciągu ostatniej godziny - można to pominąć jeśli chcesz widzieć test za każdym razem.
        # logger.info("[NewsAgent] Wysyłanie testowego pinga na Telegram...")
        # send_telegram_alert("📡 <b>NewsAgent Heartbeat</b>: System nasłuchuje.")

        # 3. Konfiguracja Czasu (Sliding Window)
        # ZAWSZE bierzemy ostatnie 72h. Deduplikacja w bazie (ProcessedNews) zadba o to, żeby nie dublować alertów.
        time_from_str = (datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)).strftime('%Y%m%dT%H%M')

        processed_count = 0
        
        # 4. Batching
        batches = [all_tickers[i:i + BATCH_SIZE] for i in range(0, len(all_tickers), BATCH_SIZE)]
        total_batches = len(batches)

        for i, batch in enumerate(batches):
            ticker_string = ",".join(batch)
            
            # Log postępu
            if i % 5 == 0:
                logger.info(f"[NewsAgent] Batch {i+1}/{total_batches} ({len(batch)} tickerów)...")

            try:
                # API CALL
                news_data = api_client.get_news_sentiment(
                    ticker=ticker_string, 
                    limit=LIMIT_PER_REQ, 
                    time_from=time_from_str
                )
            except Exception as e:
                logger.error(f"[NewsAgent] API Error Batch {i+1}: {e}")
                continue

            # Sleep 1.5s (Rate Limit Guard)
            if len(batches) > 1:
                time.sleep(1.5)

            if not news_data or 'feed' not in news_data:
                # To nie błąd, po prostu brak newsów w oknie 72h dla tych spółek
                continue

            # 5. Analiza Feeda
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
                    
                    if ticker not in all_tickers:
                        continue

                    # === NOWA DEDUPLIKACJA ===
                    # Hashujemy: Ticker + Tytuł + Źródło (omijamy URL i czas)
                    news_hash = _create_smart_hash(ticker, headline, source)
                    
                    if _check_if_news_processed(session, ticker, news_hash):
                        # Już to widzieliśmy -> SKIP
                        continue

                    try:
                        relevance_score = float(ts_data.get('relevance_score', 0))
                        sentiment_score = float(ts_data.get('ticker_sentiment_score', 0))
                        sentiment_label = ts_data.get('ticker_sentiment_label', 'Neutral')
                    except: continue

                    # === FILTRY (STRICTER) ===
                    
                    # 1. Relevance > 0.5 (chyba że Hot Topic)
                    req_relevance = MIN_RELEVANCE_SCORE
                    if is_hot_topic: req_relevance = 0.3 # Dla earningsów bierzemy szersze spektrum
                    
                    if relevance_score < req_relevance:
                        # logger.debug(f"SKIP {ticker}: Rel {relevance_score:.2f} < {req_relevance}")
                        continue

                    # 2. Sentiment > 0.25 (lub Hot Topic)
                    req_sentiment = MIN_SENTIMENT_SCORE
                    if is_hot_topic: req_sentiment = 0.1 # Earningsy są ważne nawet przy neutralnym sentymencie

                    if abs(sentiment_score) < req_sentiment:
                        # logger.debug(f"SKIP {ticker}: Sent {sentiment_score:.2f} too weak")
                        continue

                    # === ACTION: NOTIFICATION ===
                    
                    # 1. Zapis do DB (żeby nie wysłać ponownie)
                    # Używamy etykiety alertu, np. "BULLISH_NEWS"
                    alert_type = "NEWS"
                    if sentiment_score > 0.3: alert_type = "STRONG_BUY_NEWS"
                    elif sentiment_score < -0.3: alert_type = "STRONG_SELL_NEWS"

                    _save_processed_news(session, ticker, news_hash, alert_type, headline, url)

                    # 2. Formatowanie Wiadomości
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

                    # 3. Log Systemowy
                    log_entry = f"NEWS DETECTED: {ticker} (Sc:{sentiment_score:.2f}) | {headline[:50]}"
                    logger.info(f"✅ {log_entry}")
                    append_scan_log(session, log_entry)

                    # 4. TELEGRAM (CRITICAL PATH)
                    try:
                        send_telegram_alert(msg_body)
                        # Log success
                        logger.info(f"Telegram sent for {ticker}")
                    except Exception as tele_err:
                        logger.error(f"Telegram FAILED for {ticker}: {tele_err}")

                    processed_count += 1

        if processed_count > 0:
            logger.info(f"[NewsAgent] Zakończono. Wysłano {processed_count} alertów.")
        else:
            logger.info("[NewsAgent] Zakończono. Brak nowych alertów (wszystko przefiltrowane lub duplikaty).")

    except Exception as e:
        logger.error(f"[NewsAgent] Błąd krytyczny cyklu: {e}", exc_info=True)
        session.rollback()
