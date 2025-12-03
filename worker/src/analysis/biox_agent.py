import logging
import time
import hashlib
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text, select, func
import pandas as pd # Potrzebne do analizy danych historycznych

# Modele bazy danych
from ..models import ProcessedNews, PhaseXCandidate

# Importy narzędziowe
from .utils import (
    append_scan_log, 
    send_telegram_alert, 
    get_raw_data_with_cache,
    update_system_control,
    standardize_df_columns
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
        pass 

# ==================================================================
# CZĘŚĆ 2: HISTORICAL AUDIT (Poprawa: Explicit Candidate Passing)
# ==================================================================

def run_historical_catalyst_scan(session: Session, api_client, candidates: list = None):
    """
    Analiza Wsteczna dla Fazy X (BioX Audit).
    Przeszukuje historię cen kandydatów, aby znaleźć "Pompy" (>20%) w ostatnim roku.
    
    Arg:
        candidates: Opcjonalna lista tickerów. Jeśli podana, wymusza sprawdzenie tych konkretnych spółek
                    (np. bezpośrednio po Skanerze Fazy X).
    """
    logger.info("BioX Audit: Uruchamianie analizy historycznej pomp...")
    append_scan_log(session, "🧬 BioX Audit: Analiza historii cen w poszukiwaniu pomp >20%...")

    # 1. Pobierz kandydatów do sprawdzenia
    tickers_to_check = []
    
    if candidates and len(candidates) > 0:
        logger.info(f"BioX Audit: Otrzymano {len(candidates)} kandydatów bezpośrednio ze Skanera.")
        tickers_to_check = candidates
    else:
        # Fallback do bazy (stara logika dla trybu standalone)
        try:
            stmt = text("""
                SELECT ticker FROM phasex_candidates 
                WHERE last_pump_date IS NULL 
                OR analysis_date < (NOW() - INTERVAL '24 hours')
                ORDER BY ticker
            """)
            tickers_to_check = [r[0] for r in session.execute(stmt).fetchall()]
        except Exception as e:
            logger.error(f"BioX Audit: Błąd pobierania z bazy: {e}")
            return

    if not tickers_to_check:
        append_scan_log(session, "BioX Audit: Brak kandydatów do sprawdzenia.")
        return

    logger.info(f"BioX Audit: {len(tickers_to_check)} tickerów w kolejce.")
    
    processed = 0
    updated_count = 0     # Ile spółek zaktualizowaliśmy w bazie (przeanalizowano)
    pumps_found_count = 0 # Ile faktycznie miało pompy
    
    start_time = time.time()
    
    for ticker in tickers_to_check:
        try:
            # 2. Pobierz dane dzienne (FULL) z cache lub API
            raw_data = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=24, outputsize='full'
            )
            
            if not raw_data or 'Time Series (Daily)' not in raw_data:
                continue
            
            df = standardize_df_columns(pd.DataFrame.from_dict(raw_data['Time Series (Daily)'], orient='index'))
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            
            # Filtrujemy ostatni rok (ok. 252 dni handlowe)
            one_year_ago = datetime.now() - timedelta(days=365)
            df_1y = df[df.index >= one_year_ago].copy()
            
            if df_1y.empty: continue

            # 3. Szukamy pomp (>20%)
            # Definicja pompy:
            # A. Intraday spike: (High - Open) / Open >= 0.20
            # B. Gap/Session run: (Close - PrevClose) / PrevClose >= 0.20
            
            df_1y['prev_close'] = df_1y['close'].shift(1)
            df_1y['pump_intraday'] = (df_1y['high'] - df_1y['open']) / df_1y['open']
            df_1y['pump_session'] = (df_1y['close'] - df_1y['prev_close']) / df_1y['prev_close']
            
            # Znajdź dni spełniające warunek (20% = 0.20)
            pump_threshold = 0.20
            pumps = df_1y[
                (df_1y['pump_intraday'] >= pump_threshold) | 
                (df_1y['pump_session'] >= pump_threshold)
            ]
            
            pump_count = len(pumps)
            last_pump_date = None
            last_pump_percent = 0.0
            
            if pump_count > 0:
                pumps_found_count += 1
                # Bierzemy ostatnią pompę
                last_pump_row = pumps.iloc[-1]
                last_pump_date = last_pump_row.name.date() # timestamp to date
                
                # Wybieramy większą wartość (intraday vs session) jako "Moc"
                max_pump = max(last_pump_row['pump_intraday'], last_pump_row['pump_session'])
                last_pump_percent = round(max_pump * 100, 2) # Zapisujemy jako % (np. 45.20)

            # 4. Aktualizacja w bazie
            update_stmt = text("""
                UPDATE phasex_candidates 
                SET pump_count_1y = :count, 
                    last_pump_date = :date, 
                    last_pump_percent = :percent,
                    analysis_date = NOW()
                WHERE ticker = :ticker
            """)
            
            session.execute(update_stmt, {
                'count': pump_count,
                'date': last_pump_date,
                'percent': last_pump_percent,
                'ticker': ticker
            })
            session.commit()
            updated_count += 1
            
        except Exception as e:
            # logger.error(f"Err {ticker}: {e}")
            session.rollback()
            continue
        
        processed += 1
        # Co 20 sztuk mały log (rzadziej, żeby nie śmiecić)
        if processed % 20 == 0:
            logger.info(f"BioX Audit: Przetworzono {processed}/{len(tickers_to_check)}.")
            # Mały sleep tylko co jakiś czas, aby przyspieszyć, ale nie zabić bazy
            time.sleep(0.1) 

    summary = f"🏁 BioX Audit: Zakończono. Przeanalizowano: {updated_count} spółek. Znaleziono pomp: {pumps_found_count} (zaktualizowano w bazie)."
    logger.info(summary)
    append_scan_log(session, summary)
