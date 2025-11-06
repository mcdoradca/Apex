import logging
import hashlib
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, select, func

# Importy z wnętrza projektu
from ..models import TradingSignal, ProcessedNews, PortfolioHolding
# ==================================================================
# KROK 3 (KAT. 1): Import funkcji alertów Telegram
# ==================================================================
from ..analysis.utils import update_system_control, get_market_status_and_time, send_telegram_alert
# ==================================================================
# Import "mózgu" agenta, który stworzyliśmy w poprzednim kroku
from ..analysis.ai_agents import _run_news_analysis_agent

logger = logging.getLogger(__name__)

# --- Funkcje pomocnicze skopiowane ze starego 'catalyst_monitor' ---
# (Są niezbędne do działania nowego agenta)

def _create_news_hash(headline: str, uri: str) -> str:
# ... (bez zmian) ...
    """Tworzy unikalny hash SHA-256 dla wiadomości, aby uniknąć duplikatów."""
    s = f"{headline.strip()}{uri.strip()}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _check_if_news_processed(session: Session, ticker: str, news_hash: str) -> bool:
# ... (bez zmian) ...
    """Sprawdza, czy dany news (hash) był już przetwarzany dla danego tickera."""
    try:
        # Sprawdzamy newsy z ostatnich 3 dni (krótsze okno niż poprzednio)
        three_days_ago = datetime.utcnow() - timedelta(days=3)
        exists = session.scalar(
            select(func.count(ProcessedNews.id))
            .where(ProcessedNews.ticker == ticker)
            .where(ProcessedNews.news_hash == news_hash)
            .where(ProcessedNews.processed_at >= three_days_ago)
        )
        return exists > 0
    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd podczas sprawdzania hasha newsa dla {ticker}: {e}", exc_info=True)
        return False # Na wszelki wypadek lepiej przetworzyć ponownie

def _save_processed_news(session: Session, ticker: str, news_hash: str, sentiment: str, headline: str, url: str):
# ... (bez zmian) ...
    """Zapisuje przetworzony news do bazy danych."""
    try:
        new_entry = ProcessedNews(
            ticker=ticker,
            news_hash=news_hash,
            sentiment=sentiment, # Zapisujemy CRITICAL_NEGATIVE / NEUTRAL itp.
            headline=headline,
            source_url=url
        )
        session.add(new_entry)
        session.commit()
    except Exception as e:
        logger.error(f"Agent Newsowy: Błąd podczas zapisywania newsa dla {ticker}: {e}", exc_info=True)
        session.rollback()

# --- Główna funkcja "Ultra Agenta Newsowego" ---

def run_news_agent_cycle(session: Session, api_client: object):
# ... (bez zmian) ...
    """
    Główna funkcja "Ultra Agenta Newsowego".
    Pobiera dane Premium (NEWS_SENTIMENT) dla wszystkich monitorowanych spółek,
    analizuje je za pomocą Gemini i unieważnia sygnały w przypadku krytycznych
    negatywnych wiadomości.
    """
    logger.info("Uruchamianie cyklu 'Ultra Agenta Newsowego' (Kategoria 2)...")

    # 1. Sprawdź, czy rynek jest aktywny
    market_info = get_market_status_and_time(api_client)
# ... (bez zmian) ...
    market_status = market_info.get("status")
    
    if market_status not in ["MARKET_OPEN", "PRE_MARKET", "AFTER_MARKET"]:
# ... (bez zmian) ...
        logger.info(f"Agent Newsowy: Rynek jest {market_status}. Pomijanie cyklu.")
        return

    try:
        # 2. Pobierz listę wszystkich tickerów, które nas interesują
# ... (bez zmian) ...
        active_signals = session.scalars(
            select(TradingSignal.ticker)
            .where(TradingSignal.status.in_(['ACTIVE', 'PENDING']))
            .distinct()
        ).all()
        
        portfolio_tickers = session.scalars(
            select(PortfolioHolding.ticker)
            .distinct()
        ).all()
        
        # Połącz listy i usuń duplikaty
        tickers_to_monitor = list(set(active_signals + portfolio_tickers))

        if not tickers_to_monitor:
# ... (bez zmian) ...
            logger.info("Agent Newsowy: Brak tickerów do monitorowania.")
            return

        logger.info(f"Agent Newsowy: Monitorowanie {len(tickers_to_monitor)} tickerów: {', '.join(tickers_to_monitor)}")

        # 3. Wykonaj jedno zapytanie batchowe (Premium) o wiadomości
# ... (bez zmian) ...
        ticker_string = ",".join(tickers_to_monitor)
        # Używamy limitu 50, aby dostać najnowsze newsy z ostatnich godzin
        news_data = api_client.get_news_sentiment(ticker=ticker_string, limit=50)

        if not news_data or not news_data.get('feed'):
# ... (bez zmian) ...
            logger.info("Agent Newsowy: Endpoint NEWS_SENTIMENT nie zwrócił żadnych wiadomości dla monitorowanych tickerów.")
            return

        # 4. Przetwórz każdą otrzymaną wiadomość
        processed_items = 0
# ... (bez zmian) ...
        critical_alerts = 0
        
        for item in news_data.get('feed', []):
            headline = item.get('title')
# ... (bez zmian) ...
            summary = item.get('summary')
            url = item.get('url')
            
            if not all([headline, summary, url]):
# ... (bez zmian) ...
                continue # Pomiń niekompletne dane

            # 5. Sprawdź, dla których z naszych tickerów jest ta wiadomość
# ... (bez zmian) ...
            tickers_in_news = [t['ticker'] for t in item.get('topics', [])]
            
            for ticker in tickers_in_news:
                # Jeśli ten news dotyczy spółki, której nie monitorujemy, zignoruj
# ... (bez zmian) ...
                if ticker not in tickers_to_monitor:
                    continue

                # 6. Sprawdź, czy już przetwarzaliśmy ten news dla tego tickera
                news_hash = _create_news_hash(headline, url)
                if _check_if_news_processed(session, ticker, news_hash):
# ... (bez zmian) ...
                    continue # Już to widzieliśmy, pomiń
                
                logger.info(f"Agent Newsowy: Wykryto nowy news dla {ticker}: '{headline}'. Rozpoczynanie analizy AI...")
                processed_items += 1

                # 7. Mamy nowy news. Wyślij go do "mózgu" AI (Gemini)
                analysis = _run_news_analysis_agent(ticker, headline, summary, url)
# ... (bez zmian) ...
                sentiment = analysis.get('sentiment', 'NEUTRAL')

                # 8. Zapisz wynik analizy w bazie (aby nie analizować ponownie)
                _save_processed_news(session, ticker, news_hash, sentiment, headline, url)

                # 9. REAKCJA NA KRYTYCZNY NEWS
                if sentiment == 'CRITICAL_NEGATIVE':
                    critical_alerts += 1
# ... (bez zmian) ...
                    logger.warning(f"Agent Newsowy: KRYTYCZNY NEGATYWNY NEWS DLA {ticker}! Unieważnianie sygnałów.")
                    
                    # Unieważnij wszystkie aktywne/oczekujące sygnały dla tego tickera
                    update_stmt = text("""
# ... (bez zmian) ...
                        UPDATE trading_signals 
                        SET status = 'INVALIDATED', 
                            notes = :notes, 
                            updated_at = NOW()
                        WHERE ticker = :ticker 
                        AND status IN ('ACTIVE', 'PENDING')
                    """)
                    session.execute(update_stmt, {
# ... (bez zmian) ...
                        'ticker': ticker,
                        'notes': f"Sygnał unieważniony przez Agenta Newsowego (CRITICAL_NEGATIVE). News: {headline}"
                    })
                    session.commit()
                    
                    # Wyślij pilny alert do UI (i w przyszłości na Telegram)
                    alert_msg = f"PILNY ALERT NEWSOWY: {ticker} | {sentiment} | {headline}"
                    update_system_control(session, 'system_alert', alert_msg)
                    # ==================================================================
                    # KROK 3 (KAT. 1): Wysyłanie alertu na Telegram
                    # ==================================================================
                    send_telegram_alert(f"💥 PILNY ALERT NEGATYWNY 💥\n{alert_msg}")
                    # ==================================================================
                
                # ==================================================================
                # NOWA POPRAWKA (zgodnie z Pana sugestią): Reakcja na Pozytywny News
# ... (bez zmian) ...
                # ==================================================================
                elif sentiment == 'CRITICAL_POSITIVE':
                    critical_alerts += 1 # Traktujemy to również jako alert
# ... (bez zmian) ...
                    logger.warning(f"Agent Newsowy: KRYTYCZNY POZYTYWNY NEWS DLA {ticker}! Wysyłanie alertu.")
                    
                    # Dla pozytywnego newsa NIE unieważniamy sygnału,
                    # ale wysyłamy alert, aby trader mógł podjąć decyzję.
                    alert_msg = f"PILNY ALERT NEWSOWY: {ticker} | {sentiment} | {headline}"
                    update_system_control(session, 'system_alert', alert_msg)
                    # ==================================================================
                    # KROK 3 (KAT. 1): Wysyłanie alertu na Telegram
                    # ==================================================================
                    send_telegram_alert(f"🚀 PILNY ALERT POZYTYWNY 🚀\n{alert_msg}")
                    # ==================================================================
        
        logger.info(f"Agent Newsowy: Cykl zakończony. Przetworzono {processed_items} nowych wiadomości. Wygenerowano {critical_alerts} alertów krytycznych.")

    except Exception as e:
# ... (bez zmian) ...
        logger.error(f"Agent Newsowy: Nieoczekiwany błąd w głównym cyklu: {e}", exc_info=True)
        session.rollback()
