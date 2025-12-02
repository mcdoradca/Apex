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
# NARZĘDZIA POMOCNICZE (Unikanie duplikatów newsów)
# ==================================================================

def _create_news_hash(headline: str, uri: str) -> str:
    """Tworzy unikalny hash dla newsa, aby nie analizować go wielokrotnie."""
    s = f"{headline.strip()}{uri.strip()}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def _is_news_processed(session: Session, ticker: str, news_hash: str) -> bool:
    """Sprawdza w bazie 'processed_news', czy ten news był już analizowany."""
    try:
        # Sprawdzamy historię z ostatnich 7 dni (dla BioX dynamika jest duża)
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
    """Zapisuje przeanalizowany news w bazie."""
    try:
        entry = ProcessedNews(
            ticker=ticker,
            news_hash=news_hash,
            sentiment=sentiment,
            headline=headline[:1000] if headline else "", # Zabezpieczenie długości
            source_url=url[:1000] if url else ""
        )
        session.add(entry)
        session.commit()
    except Exception as e:
        logger.error(f"BioX: Błąd zapisu newsa dla {ticker}: {e}")
        session.rollback()

# ==================================================================
# CZĘŚĆ 1: LIVE MONITOR (Strażnik BioX - 5 min check)
# ==================================================================

def run_biox_live_monitor(session: Session, api_client):
    """
    Strażnik BioX. Monitoruje listę kandydatów Fazy X (Biotech Penny Stocks).
    Jeśli pojawi się news oznaczony przez AI jako 'CRITICAL_POSITIVE',
    wysyła natychmiastowy alert na Telegram i flaguje spółkę.
    """
    logger.info("BioX Live: Uruchamianie monitora czasu rzeczywistego...")
    
    # 1. Pobierz listę tickerów Fazy X z bazy (te wyselekcjonowane w Kroku 1)
    try:
        tickers_rows = session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()
        tickers = [r[0] for r in tickers_rows]
    except Exception as e:
        logger.error(f"BioX Live: Błąd pobierania kandydatów: {e}")
        return

    if not tickers:
        logger.info("BioX Live: Brak kandydatów Fazy X do monitorowania.")
        return

    # Dzielimy na paczki (Batching), aby szanować limity API
    chunk_size = 50
    processed_count = 0
    alerts_sent = 0
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        tickers_str = ",".join(chunk)
        
        try:
            # 2. Pobierz NAJNOWSZE newsy dla całej paczki
            # Używamy NEWS_SENTIMENT (Premium) - to jest kluczowe źródło
            news_response = api_client.get_news_sentiment(ticker=tickers_str, limit=50)
            
            if not news_response or 'feed' not in news_response:
                time.sleep(1) # Krótki oddech przed kolejną paczką
                continue
                
            for item in news_response.get('feed', []):
                headline = item.get('title', '')
                summary = item.get('summary', '')
                url = item.get('url', '')
                
                # Walidacja danych
                if not headline: continue

                # Znajdź ticker, którego dotyczy news (musi być w naszym koszyku BioX)
                relevant_ticker = None
                for topic in item.get('topics', []):
                    if topic['ticker'] in chunk:
                        relevant_ticker = topic['ticker']
                        break
                
                if not relevant_ticker: 
                    continue

                # 3. Sprawdź duplikaty (żeby nie spamować tym samym newsem co 5 min)
                news_hash = _create_news_hash(headline, url)
                if _is_news_processed(session, relevant_ticker, news_hash):
                    continue

                # === ANALIZA AI (Serce Systemu) ===
                # Pytamy Gemini, czy news to "paliwo rakietowe" (CRITICAL_POSITIVE)
                ai_verdict = _run_news_analysis_agent(relevant_ticker, headline, summary, url)
                sentiment = ai_verdict.get('sentiment', 'NEUTRAL')
                reason = ai_verdict.get('reason', 'Brak analizy')
                
                # Zapisujemy, że widzieliśmy ten news
                _register_processed_news(session, relevant_ticker, news_hash, sentiment, headline, url)
                processed_count += 1

                if sentiment == 'CRITICAL_POSITIVE':
                    # 4. REAKCJA NA SYGNAŁ
                    alerts_sent += 1
                    
                    # A. Formatowanie Alertu
                    alert_msg = (
                        f"🧬 BioX ALERT: {relevant_ticker} 🧬\n"
                        f"MOŻLIWY WYBUCH CENY!\n"
                        f"📰 News: {headline}\n"
                        f"🤖 AI: {reason}\n"
                        f"🔗 {url}"
                    )
                    
                    # B. Log systemowy i Telegram (Natychmiast!)
                    append_scan_log(session, f"🚀 {alert_msg}")
                    send_telegram_alert(alert_msg)
                    
                    # C. "Oflagowanie" spółki - wyciągamy na górę listy
                    # Aktualizujemy analysis_date na TERAZ, co pozwoli posortować listę w UI po świeżości
                    try:
                        session.execute(text("""
                            UPDATE phasex_candidates 
                            SET analysis_date = NOW() 
                            WHERE ticker = :t
                        """), {'t': relevant_ticker})
                        session.commit()
                        logger.info(f"BioX Live: {relevant_ticker} oflagowany jako HOT.")
                    except Exception as ex:
                        logger.error(f"BioX Live: Błąd oflagowania {relevant_ticker}: {ex}")

        except Exception as e:
            logger.error(f"BioX Live: Błąd w pętli API dla paczki {chunk}: {e}")
            continue
        
        time.sleep(1.5) # Throttle między paczkami (Rate Limit Guard)

    if alerts_sent > 0:
        logger.info(f"BioX Live: Cykl zakończony. Przeanalizowano {processed_count} nowych newsów. Wysłano {alerts_sent} alertów.")

# ==================================================================
# CZĘŚĆ 2: HISTORICAL AUDIT (Weryfikacja Pomp > 20%)
# ==================================================================

def run_historical_catalyst_scan(session: Session, api_client):
    """
    Analiza Wsteczna dla Backtestu.
    Przegląda kandydatów BioX, którzy mieli pompę >20% (zidentyfikowaną przez Skaner).
    Sprawdza historyczne newsy z dnia pompy, aby potwierdzić korelację.
    """
    logger.info("BioX History: Uruchamianie analizy przyczyn historycznych pomp...")
    append_scan_log(session, "🧬 BioX History: Start weryfikacji pomp >20% pod kątem newsów.")

    # 1. Pobierz kandydatów z pompą > 20%
    try:
        # PUMP_THRESHOLD_PERCENT w skanerze było 0.20 (20%), więc bierzemy > 20
        query = text("""
            SELECT ticker, last_pump_date, last_pump_percent 
            FROM phasex_candidates 
            WHERE last_pump_date IS NOT NULL 
              AND last_pump_percent >= 20.0
            ORDER BY last_pump_percent DESC 
            LIMIT 20
        """)
        candidates = session.execute(query).fetchall()
    except Exception as e:
        logger.error(f"BioX History: Błąd pobierania kandydatów: {e}")
        return

    if not candidates:
        append_scan_log(session, "BioX History: Brak historycznych pomp >20% do analizy.")
        return

    confirmed_connections = 0

    # 2. Dla każdego kandydata sprawdź newsy z przeszłości
    for row in candidates:
        ticker = row[0]
        pump_date = row[1] # object date (np. 2023-10-15)
        pump_pct = row[2]
        
        if not pump_date: continue

        # Formatowanie dat dla API Alpha Vantage (YYYYMMDDTHHMM)
        # Szukamy newsów z dnia pompy ORAZ dnia poprzedniego (często news po sesji odpala gap-up)
        # Np. Pompa 15.10 -> Szukamy od 14.10 godz 16:00 do 15.10 godz 23:59
        time_end = pump_date.strftime("%Y%m%dT2359")
        time_start = (pump_date - timedelta(days=1)).strftime("%Y%m%dT1200")
        
        try:
            news_data = api_client.get_news_sentiment(
                ticker=ticker, 
                time_from=time_start, 
                time_to=time_end,
                limit=5 # Najważniejsze nagłówki z okna czasowego
            )
            
            if not news_data or 'feed' not in news_data or not news_data['feed']:
                logger.info(f"BioX History: {ticker} (Pompa {pump_pct}%) - Brak newsów w API dla daty {pump_date}.")
                continue

            # Analiza AI pierwszego (najtrafniejszego) newsa
            top_story = news_data['feed'][0]
            headline = top_story.get('title', '')
            summary = top_story.get('summary', '')
            url = top_story.get('url', '')
            
            # Pytamy AI o ocenę historyczną
            ai_result = _run_news_analysis_agent(ticker, headline, summary, url)
            sentiment = ai_result.get('sentiment', 'NEUTRAL')
            reason = ai_result.get('reason', 'Brak analizy')

            # Raportowanie
            log_entry = (
                f"🕵️ BioX Audit: {ticker} (+{pump_pct:.0f}%) w dniu {pump_date}\n"
                f"   News: {headline}\n"
                f"   AI Werdykt: {sentiment} -> {reason}"
            )
            append_scan_log(session, log_entry)
            
            if sentiment == 'CRITICAL_POSITIVE':
                confirmed_connections += 1
                logger.info(f"BioX History: POTWIERDZONO KORELACJĘ dla {ticker}!")

            time.sleep(1.5) # Szanuj limity API (Backtest nie musi być błyskawiczny)

        except Exception as e:
            logger.error(f"BioX History: Błąd analizy dla {ticker}: {e}")
            continue

    append_scan_log(session, f"BioX History: Analiza zakończona. Potwierdzono związek z newsami dla {confirmed_connections} spółek.")
