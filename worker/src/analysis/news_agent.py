
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
# KONFIGURACJA AGENTA (AV NATIVE)
# ==================================================================

BATCH_SIZE = 50                 # Limit Alpha Vantage na jeden request
MIN_RELEVANCE_SCORE = 0.60      # Próg relewancji (musi dotyczyć spółki)
MIN_SENTIMENT_SCORE = 0.20      # Próg sentymentu (Tylko pozytywne: Bullish/Somewhat-Bullish)

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
        # Sprawdzamy newsy z ostatnich 7 dni (aby nie spamować powtórkami)
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
    Pobiera timestamp ostatniego skanu i formatuje go dla Alpha Vantage.
    Format: YYYYMMDDTHHMM
    Jeśli brak wpisu w bazie, zwraca czas sprzed 24 godzin (startowy).
    """
    last_val = get_system_control_value(session, LAST_SCAN_KEY)
    
    if last_val:
        return last_val
    else:
        # Domyślnie: 24h wstecz, jeśli uruchamiamy pierwszy raz
        dt = datetime.utcnow() - timedelta(hours=24)
        return dt.strftime('%Y%m%dT%H%M')

def _update_last_scan_time_to_now(session: Session, current_dt: datetime):
    """
    Aktualizuje znacznik czasu w bazie na podany czas (zazwyczaj start cyklu).
    Używany jako 'time_from' w następnym cyklu.
    Format: YYYYMMDDTHHMM
    """
    fmt = current_dt.strftime('%Y%m%dT%H%M')
    update_system_control(session, LAST_SCAN_KEY, fmt)

# ==================================================================
# GŁÓWNY CYKL AGENTA (WERSJA V2 - ROLLING WINDOW)
# ==================================================================

def run_news_agent_cycle(session: Session, api_client: object):
    """
    Agent Newsowy V2.
    - Skanuje BioX (Faza X) + Aktywne Sygnały + Portfel.
    - Używa okna czasowego (time_from) dla optymalizacji.
    - Filtruje: Relevance >= 0.6 AND Sentiment > 0.2 (Positive Only).
    """
    
    try:
        # 1. Pobierz listy monitorowanych tickerów (LIVE)
        # Dzięki temu nowe spółki z Fazy X są widoczne natychmiast w kolejnym cyklu 5-minutowym.
        
        # A. Kandydaci BioX / Faza X (Priorytet)
        phasex_tickers = set(session.scalars(select(PhaseXCandidate.ticker)).all())
        
        # B. Aktywne Sygnały i Portfel (Bezpieczeństwo pozycji)
        active_signals = session.scalars(select(TradingSignal.ticker).where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))).all()
        portfolio_tickers = session.scalars(select(PortfolioHolding.ticker)).all()
        
        # Suma zbiorów
        standard_tickers = set(active_signals + portfolio_tickers)
        all_tickers = list(phasex_tickers.union(standard_tickers))
        all_tickers.sort() # Sortowanie alfabetyczne dla porządku

        if not all_tickers:
            return

        # 2. Logika Czasowa (Rolling Window)
        scan_start_time = datetime.utcnow() # Zapisujemy czas startu obecnego cyklu
        time_from_str = _get_time_from_param(session)

        # LOGOWANIE STARTU DO UI (Potwierdzenie dla Użytkownika)
        start_msg = (
            f"Agent Newsowy: Start. Monitoruję {len(all_tickers)} spółek "
            f"(w tym {len(phasex_tickers)} z Fazy X). "
            f"Pobieram newsy od: {time_from_str}"
        )
        # Logujemy w konsoli i w UI (rzadziej, żeby nie spamować, ale tu jest ważne info o liczbie spółek)
        logger.info(start_msg)
        append_scan_log(session, start_msg)

        processed_count = 0
        alerts_sent = 0
        
        # 3. Batching Loop (Paczki po 50)
        # Przy 600 spółkach => 12 zapytań API. Limit to 150/min, więc bezpiecznie.
        batches = [all_tickers[i:i + BATCH_SIZE] for i in range(0, len(all_tickers), BATCH_SIZE)]

        for i, batch in enumerate(batches):
            ticker_string = ",".join(batch)
            
            # Zapytanie do Alpha Vantage
            # limit=1000 aby pobrać wszystko co wpadło w oknie czasowym
            # sort='LATEST' jest domyślne w AV, ale time_from załatwia sprawę przyrostowości
            try:
                news_data = api_client.get_news_sentiment(
                    ticker=ticker_string, 
                    limit=1000, 
                    time_from=time_from_str
                )
            except Exception as e:
                logger.error(f"Agent Newsowy: Błąd API dla paczki {i+1}: {e}")
                continue

            # Krótki sleep dla kultury pracy API (mimo rate limitera w kliencie)
            if len(batches) > 1:
                time.sleep(0.2)

            if not news_data or 'feed' not in news_data:
                continue

            # 4. Przetwarzanie Feed-u
            for item in news_data.get('feed', []):
                headline = item.get('title', 'No Title')
                url = item.get('url', '#')
                ticker_sentiment_list = item.get('ticker_sentiment', [])
                
                if not ticker_sentiment_list: continue

                # Sprawdź tematy specjalne (Topics)
                topics = item.get('topics', [])
                topic_tags = []
                for t in topics:
                    t_name = t.get('topic', '')
                    if t_name in ['Earnings', 'Mergers & Acquisitions', 'Life Sciences']:
                        topic_tags.append(t_name)

                # Dla każdego tickera wymienionego w newsie
                for ts_data in ticker_sentiment_list:
                    ticker = ts_data.get('ticker')
                    
                    # Czy nas ten ticker obchodzi?
                    if ticker not in all_tickers:
                        continue

                    # Sprawdź duplikaty (Hash)
                    news_hash = _create_news_hash(headline + ticker, url)
                    if _check_if_news_processed(session, ticker, news_hash):
                        continue

                    # Wyciągamy metryki AV
                    try:
                        relevance_score = float(ts_data.get('relevance_score', 0))
                        sentiment_score = float(ts_data.get('ticker_sentiment_score', 0))
                        sentiment_label = ts_data.get('ticker_sentiment_label', 'Neutral')
                    except (ValueError, TypeError):
                        continue

                    # === LOGIKA DECYZYJNA (FILTRY) ===

                    # 1. Relevance: Musi dotyczyć spółki
                    if relevance_score < MIN_RELEVANCE_SCORE:
                        continue
                    
                    # 2. Sentiment: Musi być POZYTYWNY (> 0.20)
                    # Odrzucamy Neutral, Bearish i słabe Bullish (<=0.2)
                    if sentiment_score <= MIN_SENTIMENT_SCORE:
                        continue

                    # === ALERT ===
                    alerts_sent += 1
                    
                    # Ikona i Typ
                    alert_emoji = "🚀" if sentiment_score >= 0.4 else "📈"
                    if topic_tags: 
                        alert_emoji = "🔥" # Hot Topic Override

                    alert_type = "POSITIVE_NEWS"
                    topic_str = f" | {', '.join(topic_tags)}" if topic_tags else ""

                    # Log w bazie
                    _save_processed_news(session, ticker, news_hash, alert_type, headline, url)
                    
                    # Formatowanie wiadomości Telegram
                    clean_msg = (
                        f"{alert_emoji} <b>NEWS ALERT: {ticker}</b>\n"
                        f"Sentyment: {sentiment_label} (Score: {sentiment_score})\n"
                        f"Relevance: {relevance_score}{topic_str}\n\n"
                        f"<b>{headline}</b>\n"
                        f"{url}"
                    )
                    
                    # Log w UI Dashboard
                    append_scan_log(session, f"NEWS: {ticker} | {sentiment_label} | {headline[:50]}...")
                    
                    # Wysłanie Alertu
                    send_telegram_alert(clean_msg)
                    
                    # Alert Systemowy w UI (Dla bardzo silnych newsów)
                    if sentiment_score >= 0.4 or "Earnings" in topic_tags:
                        update_system_control(session, 'system_alert', f"{ticker}: {headline[:60]}...")

                    processed_count += 1

        # 5. Aktualizacja znacznika czasu NA KONIEC
        # Ustawiamy czas startu obecnego cyklu jako punkt odniesienia dla następnego.
        _update_last_scan_time_to_now(session, scan_start_time)

        if processed_count > 0:
            logger.info(f"Agent Newsowy: Znaleziono {processed_count} pozytywnych newsów. Wysłano powiadomienia.")
            session.commit()

    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd krytyczny cyklu: {e}", exc_info=True)
        session.rollback()
