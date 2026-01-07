
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

logger = logging.getLogger(__name__)

# ==================================================================
# KONFIGURACJA AGENTA (AV NATIVE) - SKORYGOWANA
# ==================================================================

BATCH_SIZE = 50                 # Limit Alpha Vantage na jeden request
# ZMNIEJSZONO PROGI, ABY WYŁAPYWAĆ WIĘCEJ SYGNAŁÓW
MIN_RELEVANCE_SCORE = 0.40      # Obniżono z 0.60 (często ważne newsy mają np. 0.45)
MIN_SENTIMENT_SCORE = 0.15      # Obniżono z 0.20 (żeby łapać "Somewhat-Bullish" od dolnej granicy)

# Klucz w system_control do przechowywania czasu ostatniego skanu
LAST_SCAN_KEY = 'news_agent_last_scan_time'

# ==================================================================
# FUNKCJE POMOCNICZE
# ==================================================================

def _create_news_hash(headline: str, uri: str) -> str:
    """Tworzy unikalny hash SHA-256 dla wiadomości, aby uniknąć duplikatów."""
    s = f"{headline.strip()}{uri.strip()}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _check_if_news_processed(session: Session, ticker: str, news_hash: str) -> bool:
    """Sprawdza, czy dany news (hash) był już przetwarzany dla danego tickera."""
    try:
        # Sprawdzamy newsy z ostatnich 7 dni
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        exists = session.scalar(
            select(func.count(ProcessedNews.id))
            .where(ProcessedNews.ticker == ticker)
            .where(ProcessedNews.news_hash == news_hash)
            .where(ProcessedNews.processed_at >= seven_days_ago)
        )
        return exists > 0
    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd podczas sprawdzania hasha newsa dla {ticker}: {e}")
        return False 

def _save_processed_news(session: Session, ticker: str, news_hash: str, sentiment: str, headline: str, url: str):
    """Zapisuje przetworzony news do bazy danych."""
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
        logger.error(f"Agent Newsowy: Błąd zapisu newsa dla {ticker}: {e}")
        session.rollback()

def _get_time_from_param(session: Session) -> str:
    """
    Pobiera timestamp ostatniego skanu.
    Format: YYYYMMDDTHHMM
    """
    last_val = get_system_control_value(session, LAST_SCAN_KEY)
    
    if last_val:
        return last_val
    else:
        # Domyślnie: 48h wstecz (bezpieczniejszy margines niż 24h)
        dt = datetime.utcnow() - timedelta(hours=48)
        return dt.strftime('%Y%m%dT%H%M')

def _update_last_scan_time_to_now(session: Session, current_dt: datetime):
    """Aktualizuje znacznik czasu w bazie."""
    fmt = current_dt.strftime('%Y%m%dT%H%M')
    update_system_control(session, LAST_SCAN_KEY, fmt)

# ==================================================================
# GŁÓWNY CYKL AGENTA (WERSJA V3 - POPRAWIONA DIAGNOSTYKA)
# ==================================================================

def run_news_agent_cycle(session: Session, api_client: object):
    """
    Agent Newsowy V3.
    - Skanuje BioX (Faza X) + Aktywne Sygnały + Portfel.
    - Zwiększona "gadatliwość" (Logging) odrzuconych newsów.
    - Poluzowane filtry.
    """
    
    try:
        # 1. Pobierz listy monitorowanych tickerów
        phasex_tickers = set(session.scalars(select(PhaseXCandidate.ticker)).all())
        active_signals = session.scalars(select(TradingSignal.ticker).where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))).all()
        portfolio_tickers = session.scalars(select(PortfolioHolding.ticker)).all()
        
        standard_tickers = set(active_signals + portfolio_tickers)
        all_tickers = list(phasex_tickers.union(standard_tickers))
        all_tickers.sort()

        # === FIX: Logowanie ostrzeżenia jeśli lista pusta ===
        if not all_tickers:
            msg = "⚠️ Agent Newsowy: Brak spółek do monitorowania! (Faza X, Portfel i Sygnały są puste). Radar wyłączony."
            logger.warning(msg)
            append_scan_log(session, msg)
            return

        # 2. Logika Czasowa
        scan_start_time = datetime.utcnow()
        time_from_str = _get_time_from_param(session)

        start_msg = (
            f"Agent Newsowy: Skanowanie {len(all_tickers)} spółek. "
            f"Od: {time_from_str}. (Filtry: Rev>{MIN_RELEVANCE_SCORE}, Sent>{MIN_SENTIMENT_SCORE})"
        )
        logger.info(start_msg)
        # Logujemy w UI tylko co jakiś czas lub przy zmianie, żeby nie spamować
        # append_scan_log(session, start_msg) 

        processed_count = 0
        alerts_sent = 0
        skipped_info_log = [] # Bufor na logi o pominiętych (żeby nie zabić IO)
        
        # 3. Batching Loop
        batches = [all_tickers[i:i + BATCH_SIZE] for i in range(0, len(all_tickers), BATCH_SIZE)]

        for i, batch in enumerate(batches):
            ticker_string = ",".join(batch)
            
            try:
                # limit=1000, sort='LATEST' (domyślne)
                news_data = api_client.get_news_sentiment(
                    ticker=ticker_string, 
                    limit=1000, 
                    time_from=time_from_str
                )
            except Exception as e:
                logger.error(f"Agent Newsowy: Błąd API dla paczki {i+1}: {e}")
                continue

            if len(batches) > 1:
                time.sleep(0.25) # Lekkie opóźnienie

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

                # Iteracja po tickerach w newsie
                for ts_data in ticker_sentiment_list:
                    ticker = ts_data.get('ticker')
                    
                    # Czy to nasza spółka?
                    if ticker not in all_tickers:
                        continue

                    # Sprawdź duplikaty
                    news_hash = _create_news_hash(headline + ticker, url)
                    if _check_if_news_processed(session, ticker, news_hash):
                        continue

                    try:
                        relevance_score = float(ts_data.get('relevance_score', 0))
                        sentiment_score = float(ts_data.get('ticker_sentiment_score', 0))
                        sentiment_label = ts_data.get('ticker_sentiment_label', 'Neutral')
                    except (ValueError, TypeError):
                        continue

                    # === LOGIKA DECYZYJNA (POPRAWIONA) ===

                    # Warunek 1: Relevance
                    if relevance_score < MIN_RELEVANCE_SCORE:
                        # LOGUJEMY DLACZEGO ODRZUCONO (Dla celów debugowania)
                        if relevance_score > 0.1: # Nie loguj śmieci
                            logger.debug(f"SKIP {ticker}: Low Relevance {relevance_score} < {MIN_RELEVANCE_SCORE}")
                        continue
                    
                    # Warunek 2: Sentiment
                    # Jeśli to Hot Topic (Earnings/Mergers), akceptujemy niższy sentyment (nawet lekko negatywny/neutralny)
                    threshold = MIN_SENTIMENT_SCORE
                    if is_hot_topic:
                        threshold = 0.05 # Prawie każdy news o wynikach nas interesuje

                    if sentiment_score <= threshold:
                        # LOGUJEMY ODRZUCENIE SENTYMENTU
                        # To pozwoli Ci sprawdzić, czy "GLUE" był widziany, ale miał np. 0.14
                        if sentiment_score > -0.1: # Loguj tylko te "blisko" zera lub pozytywne
                             logger.info(f"SKIP {ticker}: Low Sentiment {sentiment_score} <= {threshold} ({headline[:30]}...)")
                        continue

                    # === ALERT ===
                    alerts_sent += 1
                    
                    alert_emoji = "🚀" if sentiment_score >= 0.4 else "📈"
                    if is_hot_topic: alert_emoji = "🔥"

                    alert_type = "POSITIVE_NEWS"
                    topic_str = f" | {', '.join(topic_tags)}" if topic_tags else ""

                    _save_processed_news(session, ticker, news_hash, alert_type, headline, url)
                    
                    clean_msg = (
                        f"{alert_emoji} <b>NEWS ALERT: {ticker}</b>\n"
                        f"Sentyment: {sentiment_label} (Score: {sentiment_score})\n"
                        f"Relevance: {relevance_score}{topic_str}\n\n"
                        f"<b>{headline}</b>\n"
                        f"{url}"
                    )
                    
                    log_msg = f"NEWS: {ticker} (Sc:{sentiment_score}) | {headline[:50]}..."
                    append_scan_log(session, log_msg)
                    logger.info(f"✅ ALERT SENT: {log_msg}")
                    
                    # Wysłanie Alertu (Telegram)
                    send_telegram_alert(clean_msg)
                    
                    if sentiment_score >= 0.35 or is_hot_topic:
                        update_system_control(session, 'system_alert', f"{ticker}: {headline[:60]}...")

                    processed_count += 1

        # 5. Aktualizacja znacznika czasu
        _update_last_scan_time_to_now(session, scan_start_time)

        if processed_count > 0:
            logger.info(f"Agent Newsowy: Wysłano {processed_count} alertów.")
            session.commit()
        else:
            # Info w logach, że skan przeszedł pusto (dla pewności, że działa)
            logger.debug("Agent Newsowy: Brak nowych newsów spełniających kryteria.")

    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd krytyczny cyklu: {e}", exc_info=True)
        session.rollback()
