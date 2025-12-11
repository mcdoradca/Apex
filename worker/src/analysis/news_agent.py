import logging
import hashlib
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, select, func

# Importy modeli
from ..models import TradingSignal, ProcessedNews, PortfolioHolding, PhaseXCandidate

# Importy narzędziowe
from ..analysis.utils import update_system_control, get_market_status_and_time, send_telegram_alert, append_scan_log

logger = logging.getLogger(__name__)

# ==================================================================
# KONFIGURACJA PROGÓW (HARD LOGIC)
# ==================================================================

# Próg relewancji dla standardowych spółek (aby uniknąć wzmianek "przy okazji")
STANDARD_RELEVANCE_THRESHOLD = 0.60 

# Progi sentymentu dla standardowych spółek (0.15 łapie też "Somewhat Bullish/Bearish")
STANDARD_BULLISH_THRESHOLD = 0.15
STANDARD_BEARISH_THRESHOLD = -0.15

# Konfiguracja BioX (Faza X)
BIOX_RELEVANCE_THRESHOLD = 0.90 # Musi dotyczyć stricte tej spółki
# Dla BioX nie ma progu sentymentu - każdy news o wysokiej relewancji jest ważny.

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
            headline=headline[:1000] if headline else "", # Przycinamy na wszelki wypadek
            source_url=url[:1000] if url else ""
        )
        session.add(entry)
        session.commit()
    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd zapisu newsa dla {ticker}: {e}")
        session.rollback()

# ==================================================================
# GŁÓWNY CYKL AGENTA (WERSJA AV NATIVE)
# ==================================================================

def run_news_agent_cycle(session: Session, api_client: object):
    """
    Główna funkcja Agenta Newsowego opartego na metadanych Alpha Vantage.
    Nie używa LLM. Analizuje pola `ticker_sentiment` z JSON-a.
    """
    # logger.info("Uruchamianie cyklu Agenta Newsowego (AV Native)...")

    # 1. Sprawdź status rynku (opcjonalne, ale oszczędza zasoby w nocy)
    # Można to zakomentować, jeśli chcesz newsy 24/7
    market_info = get_market_status_and_time(api_client)
    # market_status = market_info.get("status")
    # if market_status == "CLOSED": return

    try:
        # 2. Pobierz listy monitorowanych tickerów
        
        # A. Aktywne Sygnały i Portfel (Standard)
        active_signals = session.scalars(select(TradingSignal.ticker).where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))).all()
        portfolio_tickers = session.scalars(select(PortfolioHolding.ticker)).all()
        standard_tickers = set(active_signals + portfolio_tickers)

        # B. Kandydaci BioX (Specjalne traktowanie)
        phasex_tickers = set(session.scalars(select(PhaseXCandidate.ticker)).all())
        
        # Wszystkie unikalne tickery do zapytania API
        all_tickers = list(standard_tickers.union(phasex_tickers))

        if not all_tickers:
            return

        # 3. Zapytanie do Alpha Vantage (Batch)
        # Limit 50 newsów, sortowanie LATEST (domyślne w AV)
        ticker_string = ",".join(all_tickers[:50]) # AV limituje długość URL, więc bezpiecznie bierzemy 50 tickerów max na raz w workerze
        
        # Używamy klienta API. Jeśli funkcja nie istnieje w mocku, to wywali błąd, ale w produkcji jest OK.
        # Parametr topics='life_sciences' można dodać opcjonalnie, ale tu chcemy wszystko.
        news_data = api_client.get_news_sentiment(ticker=ticker_string, limit=50)

        if not news_data or 'feed' not in news_data:
            return

        processed_count = 0
        alerts_count = 0

        # 4. Przetwarzanie Feed-u
        for item in news_data.get('feed', []):
            headline = item.get('title', 'No Title')
            summary = item.get('summary', 'No Summary')
            url = item.get('url', '#')
            
            # Najważniejsze: Lista sentymentów per ticker
            ticker_sentiment_list = item.get('ticker_sentiment', [])
            
            if not ticker_sentiment_list: continue

            # Dla każdego tickera wymienionego w newsie
            for ts_data in ticker_sentiment_list:
                ticker = ts_data.get('ticker')
                
                # Czy nas ten ticker obchodzi?
                if ticker not in all_tickers:
                    continue

                # Sprawdź duplikaty (per ticker, bo ten sam news może dotyczyć wielu)
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

                is_biox = ticker in phasex_tickers
                should_alert = False
                alert_type = "NEUTRAL"
                alert_emoji = "ℹ️"

                # === LOGIKA DECYZYJNA ===

                # SCENARIUSZ 1: BioX (Biotech) - Opcja B
                # Każdy news o wysokiej relewancji to katalizator
                if is_biox:
                    if relevance_score >= BIOX_RELEVANCE_THRESHOLD:
                        should_alert = True
                        alert_type = "BIOX_CATALYST"
                        alert_emoji = "🧬"
                        # Aktualizujemy datę analizy w BioX, aby pokazać aktywność
                        session.execute(text("UPDATE phasex_candidates SET analysis_date = NOW() WHERE ticker = :t"), {'t': ticker})
                
                # SCENARIUSZ 2: Standard (Sygnały/Portfel)
                # Filtrujemy szum, szukamy sentymentu
                else:
                    if relevance_score >= STANDARD_RELEVANCE_THRESHOLD:
                        if sentiment_score >= STANDARD_BULLISH_THRESHOLD:
                            should_alert = True
                            alert_type = "POSITIVE"
                            alert_emoji = "🚀" if sentiment_score >= 0.35 else "📈" # Rakieta dla Bullish, Wykres dla Somewhat
                        
                        elif sentiment_score <= STANDARD_BEARISH_THRESHOLD:
                            should_alert = True
                            alert_type = "NEGATIVE"
                            alert_emoji = "💥" if sentiment_score <= -0.35 else "📉" # Wybuch dla Bearish, Wykres dla Somewhat

                # === AKCJA ===
                if should_alert:
                    alerts_count += 1
                    
                    # Log w bazie
                    _save_processed_news(session, ticker, news_hash, alert_type, headline, url)
                    
                    # Formatowanie wiadomości
                    msg = (
                        f"{alert_emoji} <b>NEWS ALERT: {ticker}</b>\n"
                        f"Label: {sentiment_label} (Score: {sentiment_score})\n"
                        f"Relevance: {relevance_score}\n\n"
                        f"📰 {headline}\n"
                        f"🔗 <a href='{url}'>Link do źródła</a>"
                    )
                    
                    # Log w UI Dashboard
                    append_scan_log(session, f"NEWS: {ticker} | {sentiment_label} | {headline[:50]}...")
                    
                    # Alert Telegram (HTML parse mode jest obsługiwany przez bibliotekę utils jeśli jest wdrożony, tutaj plain text bezpieczniej)
                    # W utils.py mamy quote_plus, więc HTML tagi mogą nie przejść idealnie, wysyłamy czysty tekst
                    clean_msg = (
                        f"{alert_emoji} NEWS: {ticker} [{alert_type}]\n"
                        f"Sentyment: {sentiment_label} ({sentiment_score})\n"
                        f"{headline}\n"
                        f"{url}"
                    )
                    send_telegram_alert(clean_msg)
                    
                    # Alert Systemowy w UI (Dla krytycznych wartości)
                    if abs(sentiment_score) >= 0.35 or alert_type == "BIOX_CATALYST":
                        update_system_control(session, 'system_alert', f"{ticker}: {headline[:60]}...")

                processed_count += 1

        if processed_count > 0:
            logger.info(f"Agent Newsowy: Przetworzono {processed_count} wzmianek. Wysłano {alerts_count} alertów.")
            session.commit()

    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd krytyczny cyklu: {e}", exc_info=True)
        session.rollback()
