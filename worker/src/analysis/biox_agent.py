import logging
import time
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text

# Importy narzędziowe
from .utils import (
    append_scan_log, 
    send_telegram_alert, 
    get_raw_data_with_cache
)
from .ai_agents import _run_news_analysis_agent

logger = logging.getLogger(__name__)

# ==================================================================
# CZĘŚĆ 1: ANALIZA HISTORYCZNA (Detektyw Wsteczny)
# ==================================================================

def run_historical_catalyst_scan(session: Session, api_client):
    """
    Przegląda kandydatów Fazy X, którzy mieli 'pompę' >50%.
    Pobiera newsy z daty pompy i pyta AI, co było przyczyną.
    Generuje raport w logach.
    """
    logger.info("BioX History: Uruchamianie analizy przyczyn historycznych pomp...")
    append_scan_log(session, "🧬 BioX History: Start analizy katalizatorów dla zidentyfikowanych pomp.")

    # 1. Pobierz kandydatów z pompą w historii
    try:
        query = text("""
            SELECT ticker, last_pump_date, last_pump_percent 
            FROM phasex_candidates 
            WHERE last_pump_date IS NOT NULL 
            ORDER BY last_pump_percent DESC 
            LIMIT 10
        """)
        candidates = session.execute(query).fetchall()
    except Exception as e:
        logger.error(f"BioX History: Błąd pobierania kandydatów: {e}")
        return

    if not candidates:
        append_scan_log(session, "BioX History: Brak kandydatów z historią pomp do analizy.")
        return

    # 2. Dla każdego kandydata sprawdź newsy z dnia pompy
    for row in candidates:
        ticker = row[0]
        pump_date = row[1] # object date
        pump_pct = row[2]
        
        # Formatowanie dat dla API (Alpha Vantage wymaga YYYYMMDDTHHMM)
        # Szukamy newsów z dnia pompy oraz dnia poprzedniego (często news jest po sesji)
        time_to = pump_date.strftime("%Y%m%dT2359")
        time_from = (pump_date - timedelta(days=1)).strftime("%Y%m%dT0000")
        
        try:
            # Używamy get_news_sentiment z filtrem czasowym
            news_data = api_client.get_news_sentiment(
                ticker=ticker, 
                time_from=time_from, 
                time_to=time_to,
                limit=5 # Wystarczy kilka nagłówków
            )
            
            if not news_data or 'feed' not in news_data or not news_data['feed']:
                logger.info(f"BioX: Brak newsów dla {ticker} w dniu pompy ({pump_date}).")
                continue

            # Analiza AI pierwszego (najważniejszego) newsa
            top_story = news_data['feed'][0]
            headline = top_story.get('title', '')
            summary = top_story.get('summary', '')
            url = top_story.get('url', '')
            
            # Pytamy AI: Czy to jest CRITICAL_POSITIVE?
            ai_result = _run_news_analysis_agent(ticker, headline, summary, url)
            sentiment = ai_result.get('sentiment', 'NEUTRAL')
            reason = ai_result.get('reason', 'Brak analizy')

            # Raportowanie
            log_entry = (
                f"🕵️ BioX Audit: {ticker} (+{pump_pct}%) w dniu {pump_date}\n"
                f"   News: {headline}\n"
                f"   AI Ocena: {sentiment} ({reason})"
            )
            append_scan_log(session, log_entry)
            
            # Jeśli AI potwierdzi, że news był krytyczny, to mamy potwierdzony schemat!
            if sentiment == 'CRITICAL_POSITIVE':
                logger.info(f"BioX: Potwierdzono katalizator dla {ticker}!")

            time.sleep(1.5) # Szanuj limity API

        except Exception as e:
            logger.error(f"BioX: Błąd analizy dla {ticker}: {e}")
            continue

    append_scan_log(session, "BioX History: Analiza wsteczna zakończona.")


# ==================================================================
# CZĘŚĆ 2: LIVE MONITOR (System BioX)
# ==================================================================

def run_biox_live_monitor(session: Session, api_client):
    """
    Strażnik BioX. Monitoruje CAŁY koszyk Fazy X w poszukiwaniu
    świeżych newsów typu 'CRITICAL_POSITIVE'.
    Wysyła natychmiastowy alert na Telegram.
    """
    logger.info("BioX Live: Uruchamianie monitora czasu rzeczywistego...")
    
    # 1. Pobierz listę tickerów Fazy X (Biotech Penny Stocks)
    try:
        tickers_rows = session.execute(text("SELECT ticker FROM phasex_candidates")).fetchall()
        tickers = [r[0] for r in tickers_rows]
    except Exception as e:
        logger.error(f"BioX Live: Błąd bazy: {e}")
        return

    if not tickers:
        logger.info("BioX Live: Pusty koszyk Fazy X. Pomiń.")
        return

    # Dzielimy na paczki po 50 (limit API dla newsów)
    chunk_size = 50
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        tickers_str = ",".join(chunk)
        
        try:
            # Pobieramy NAJNOWSZE newsy dla całej paczki
            # Bez time_from pobiera najświeższe
            news_response = api_client.get_news_sentiment(ticker=tickers_str, limit=50)
            
            if not news_response or 'feed' not in news_response:
                continue
                
            for item in news_response.get('feed', []):
                # Sprawdź datę newsa - interesują nas tylko z ostatniej godziny/doby
                # (Tu upraszczamy, API Alpha Vantage sortuje od najnowszych)
                
                headline = item.get('title', '')
                summary = item.get('summary', '')
                url = item.get('url', '')
                
                # Znajdź ticker, którego dotyczy news (i który jest w naszym koszyku BioX)
                relevant_ticker = None
                for topic in item.get('topics', []):
                    if topic['ticker'] in chunk:
                        relevant_ticker = topic['ticker']
                        break
                
                if not relevant_ticker: 
                    continue

                # === ANALIZA AI ===
                # To jest serce systemu. Pytamy Gemini, czy to jest "ten" news.
                ai_verdict = _run_news_analysis_agent(relevant_ticker, headline, summary, url)
                
                if ai_verdict.get('sentiment') == 'CRITICAL_POSITIVE':
                    # ZNALEZIONO ZŁOTY GRAAL!
                    alert_msg = (
                        f"🧬 BioX ALERT: {relevant_ticker} 🧬\n"
                        f"Możliwy start rakiety!\n"
                        f"News: {headline}\n"
                        f"AI: {ai_verdict.get('reason')}\n"
                        f"Link: {url}"
                    )
                    
                    # 1. Log do systemu
                    append_scan_log(session, f"🚀 {alert_msg}")
                    
                    # 2. Powiadomienie Telegram (Priorytet)
                    send_telegram_alert(alert_msg)
                    
                    logger.info(f"BioX Live: WYSŁANO ALERT DLA {relevant_ticker}")

        except Exception as e:
            logger.error(f"BioX Live: Błąd w pętli API: {e}")
            continue
        
        time.sleep(1) # Throttle między paczkami
