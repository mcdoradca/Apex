import logging
import time
import hashlib
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, select, func

# Modele bazy danych
from ..models import ProcessedNews, PhaseXCandidate

# Importy narzędziowe
from .utils import (
    append_scan_log, 
    send_telegram_alert, 
    get_raw_data_with_cache,
    update_system_control
)
# Mózg Agenta Newsowego
from .ai_agents import _run_news_analysis_agent

logger = logging.getLogger(__name__)

# ==================================================================
# NARZĘDZIA POMOCNICZE
# ==================================================================

def _create_news_hash(headline: str, uri: str) -> str:
    s = f"{headline.strip()}{uri.strip()}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _is_news_processed(session: Session, ticker: str, news_hash: str) -> bool:
    try:
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        exists = session.scalar(
            select(func.count(ProcessedNews.id))
            .where(ProcessedNews.ticker == ticker)
            .where(ProcessedNews.news_hash == news_hash)
            .where(ProcessedNews.processed_at >= seven_days_ago)
        )
        return exists > 0
    except Exception:
        return False

def _register_processed_news(session: Session, ticker: str, news_hash: str, sentiment: str, headline: str, url: str):
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
        session.rollback()

# ==================================================================
# CZĘŚĆ 1: LIVE MONITOR (Strażnik BioX - 5 min check)
# ==================================================================

def run_biox_live_monitor(session: Session, api_client):
    """
    Strażnik BioX. Monitoruje listę kandydatów Fazy X.
    Wersja VERBOSE - raportuje aktywność w UI.
    """
    # 1. Pobierz listę tickerów Fazy X
    try:
        tickers_rows = session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()
        tickers = [r[0] for r in tickers_rows]
    except Exception as e:
        logger.error(f"BioX Live: Błąd bazy: {e}")
        return

    if not tickers:
        # Jeśli lista pusta, milczymy lub dajemy znać raz na jakiś czas
        return

    # LOG STARTOWY (Dla widoczności w UI)
    start_msg = f"🕵️ BioX Agent: Start cyklu. Monitoruję {len(tickers)} spółek Biotech..."
    logger.info(start_msg)
    append_scan_log(session, start_msg)

    chunk_size = 50
    processed_news_count = 0
    alerts_sent = 0
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        tickers_str = ",".join(chunk)
        
        try:
            # 2. Pobierz NEWSY (Premium Endpoint)
            news_response = api_client.get_news_sentiment(ticker=tickers_str, limit=50)
            
            if not news_response or 'feed' not in news_response:
                time.sleep(1)
                continue
                
            for item in news_response.get('feed', []):
                headline = item.get('title', '')
                summary = item.get('summary', '')
                url = item.get('url', '')
                
                if not headline: continue

                relevant_ticker = None
                for topic in item.get('topics', []):
                    if topic['ticker'] in chunk:
                        relevant_ticker = topic['ticker']
                        break
                
                if not relevant_ticker: continue

                # Sprawdź duplikaty
                news_hash = _create_news_hash(headline, url)
                if _is_news_processed(session, relevant_ticker, news_hash):
                    continue

                # === ANALIZA AI ===
                ai_verdict = _run_news_analysis_agent(relevant_ticker, headline, summary, url)
                sentiment = ai_verdict.get('sentiment', 'NEUTRAL')
                reason = ai_verdict.get('reason', 'Brak analizy')
                
                _register_processed_news(session, relevant_ticker, news_hash, sentiment, headline, url)
                processed_news_count += 1

                # Logika Powiadomień
                if sentiment == 'CRITICAL_POSITIVE':
                    alerts_sent += 1
                    alert_msg = (
                        f"🧬 BioX ALERT: {relevant_ticker} 🧬\n"
                        f"MOŻLIWY WYBUCH!\n"
                        f"📰 {headline}\n"
                        f"🤖 AI: {reason}"
                    )
                    append_scan_log(session, f"🚀 {alert_msg}")
                    send_telegram_alert(alert_msg)
                    
                    # Oflagowanie (podbicie daty analizy)
                    session.execute(text("UPDATE phasex_candidates SET analysis_date = NOW() WHERE ticker = :t"), {'t': relevant_ticker})
                    session.commit()
                
                # Logujemy też "ciekawe" ale nie krytyczne, żebyś widział pracę AI
                elif sentiment != 'NEUTRAL':
                    append_scan_log(session, f"ℹ️ BioX Info: {relevant_ticker} - {sentiment} ({reason})")

        except Exception as e:
            logger.error(f"BioX Live: Błąd API: {e}")
            continue
        
        time.sleep(1.5) 

    # LOG KOŃCOWY (Podsumowanie cyklu)
    if processed_news_count > 0:
        end_msg = f"🏁 BioX Agent: Przeanalizowano {processed_news_count} nowych newsów. Alertów: {alerts_sent}."
        append_scan_log(session, end_msg)
    else:
        # Dajemy znać, że żyjemy, ale nic nie znaleziono (cisza w eterze)
        pass # Można odkomentować poniższą linię, jeśli chcesz widzieć log co 5 min nawet przy braku newsów
        # append_scan_log(session, "BioX Agent: Brak nowych wiadomości w tym cyklu.")

# ==================================================================
# CZĘŚĆ 2: HISTORICAL AUDIT (Dla Backtestu)
# ==================================================================

def run_historical_catalyst_scan(session: Session, api_client):
    """
    Analiza Wsteczna dla Backtestu.
    """
    logger.info("BioX History: Start analizy wstecznej...")
    append_scan_log(session, "🧬 BioX History: Analiza katalizatorów dla historycznych pomp...")

    # Pobieramy kandydatów, którzy mieli pompę (tutaj pole last_pump_percent będzie 0,
    # bo usunęliśmy logikę ze skanera, więc w nowym podejściu ten moduł będzie czekał
    # na dane z Backtest Engine, który uzupełni historię transakcji).
    
    # W tym momencie (po czystym skanie) ta funkcja może nie mieć co robić,
    # dopóki nie puścisz Backtestu, który wygeneruje 'virtual_trades' z pompami.
    
    append_scan_log(session, "BioX History: Oczekiwanie na wyniki Backtestu (Symulacji Pomp).")
