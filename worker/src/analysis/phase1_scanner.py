import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
# Importy narzędziowe
from .utils import (
    append_scan_log, update_scan_progress, safe_float, 
    standardize_df_columns, calculate_atr,
    get_raw_data_with_cache 
)
from ..config import SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

def _check_sector_health(session: Session, api_client, sector_name: str) -> tuple[bool, float, str]:
    """
    Sprawdza kondycję sektora (ETF).
    Zwraca: (czy_zdrowy, wynik_trendu, symbol_etf)
    """
    etf_ticker = SECTOR_TO_ETF_MAP.get(sector_name, DEFAULT_MARKET_ETF)
    
    try:
        # Używamy cache z długim czasem wygaśnięcia, aby nie męczyć API
        raw_data = get_raw_data_with_cache(
            session, api_client, etf_ticker, 
            'DAILY_ADJUSTED', 'get_daily_adjusted', 
            expiry_hours=24, outputsize='compact' 
        )
        
        if not raw_data:
            return True, 0.0, etf_ticker 

        df = standardize_df_columns(pd.DataFrame.from_dict(raw_data.get('Time Series (Daily)', {}), orient='index'))
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        
        if len(df) < 50:
            return True, 0.0, etf_ticker

        current_price = df['close'].iloc[-1]
        sma_50 = df['close'].rolling(window=50).mean().iloc[-1]
        
        is_healthy = current_price > sma_50
        trend_score = 1.0 if is_healthy else -1.0
        
        return is_healthy, trend_score, etf_ticker

    except Exception as e:
        logger.warning(f"Błąd sprawdzania sektora {sector_name} ({etf_ticker}): {e}")
        return True, 0.0, etf_ticker

def _flush_candidates_batch(session: Session, candidates_buffer: list):
    """
    Pomocnicza funkcja do bezpiecznego zapisu paczki kandydatów.
    """
    if not candidates_buffer:
        return

    try:
        # 1. Zapisz kandydatów (Bulk Insert)
        # SQLAlchemy automatycznie obsłuży listę słowników jako executemany
        insert_stmt = text("""
            INSERT INTO phase1_candidates (ticker, price, volume, change_percent, score, sector_ticker, sector_trend_score, analysis_date)
            VALUES (:ticker, :price, :volume, 0.0, 1, :sector_ticker, :sector_trend, NOW())
        """)
        
        session.execute(insert_stmt, candidates_buffer)
        
        # 2. Zbiorczy log do bazy
        tickers_str = ", ".join([c['ticker'] for c in candidates_buffer])
        log_msg = f"✅ ZAPISANO PACZKĘ F1 ({len(candidates_buffer)}): {tickers_str}"
        
        # Logujemy tylko do workera
        append_scan_log(session, log_msg)
        
        # 3. Jeden commit na całą paczkę
        session.commit()
        logger.info(log_msg)
        
        # 4. SAFETY THROTTLE: Odczekaj chwilę po zapisie, aby baza zwolniła połączenia
        time.sleep(0.5) 
        
    except Exception as e:
        logger.error(f"CRITICAL: Błąd zapisu paczki kandydatów: {e}", exc_info=True)
        session.rollback()
        # W razie awarii spróbuj odczekać dłużej (Cool Down)
        time.sleep(5)

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    Skaner Fazy 1 (V6.2 - ULTRA SAFE BATCH MODE).
    Zoptymalizowany pod kątem stabilności połączenia z bazą danych.
    Zwiększony BATCH_SIZE i dodany Throttling.
    """
    logger.info("Running Phase 1: EOD Scan (V6.2 Ultra Safe Mode)...")
    append_scan_log(session, "Faza 1 (V6.2): Start. Tryb oszczędzania połączeń (Batch 100 + Throttle) aktywny.")

    # Czyszczenie tabeli przed startem
    try:
        session.execute(text("DELETE FROM phase1_candidates"))
        session.commit()
    except Exception as e:
        logger.error(f"Failed to clear Phase 1 table: {e}")
        session.rollback()

    try:
        all_tickers_rows = session.execute(text("SELECT ticker, sector FROM companies ORDER BY ticker")).fetchall()
        total_tickers = len(all_tickers_rows)
        logger.info(f"Found {total_tickers} tickers to process.")
    except Exception as e:
        logger.error(f"Could not fetch companies: {e}", exc_info=True)
        return []

    final_candidate_tickers = []
    reject_stats = {'price': 0, 'volume': 0, 'atr': 0, 'intraday': 0, 'sector': 0, 'data': 0, 'trend': 0}
    
    # Bufor na kandydatów do zapisu wsadowego
    candidates_buffer = []
    # ZWIĘKSZONY BATCH SIZE DLA OCHRONY BAZY
    BATCH_SIZE = 100 
    
    start_time = time.time()

    for processed_count, row in enumerate(all_tickers_rows):
        ticker = row[0]
        sector = row[1]
        
        # Pauza manualna
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        # Logowanie postępu rzadziej (co 100)
        if processed_count % 100 == 0: 
            update_scan_progress(session, processed_count, total_tickers)
            # Dodatkowe mini-opóźnienie co 100 tickerów, aby nie zajechać CPU/Bazy
            time.sleep(0.1)

        if processed_count > 0 and processed_count % 200 == 0:
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            logger.info(f"F1 Heartbeat: {processed_count}/{total_tickers} ({rate:.1f} t/s)")

        try:
            # Używamy cache z agresywnym czasem wygaśnięcia (12 godzin)
            price_data_raw = get_raw_data_with_cache(
                session, api_client, ticker, 
                'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=12, outputsize='full'
            )
            
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                reject_stats['data'] += 1
                continue
            
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.sort_index(inplace=True)

            if len(daily_df) < 200: 
                reject_stats['data'] += 1
                continue

            latest_candle = daily_df.iloc[-1]
            current_price = latest_candle['close']
            
            if pd.isna(current_price): continue
                
            # === 1. Cena (0.5$ - 50.0$) ===
            if not (0.5 <= current_price <= 50.0): 
                reject_stats['price'] += 1
                continue
            
            # === 2. Płynność (Vol > 300k, średnia z ostatnich 20 dni) ===
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            if pd.isna(avg_volume) or avg_volume < 300000: 
                reject_stats['volume'] += 1
                continue
            
            # === 3. Zmienność (ATR > 2% ceny) ===
            atr_series = calculate_atr(daily_df, period=14)
            if atr_series.empty: continue
            current_atr = atr_series.iloc[-1]
            atr_percent = (current_atr / current_price)
            if atr_percent < 0.02: 
                reject_stats['atr'] += 1
                continue 

            # === 4. TREND GUARD (SMA 200) ===
            sma_200 = daily_df['close'].rolling(window=200).mean().iloc[-1]
            if pd.isna(sma_200) or current_price < sma_200:
                reject_stats['trend'] += 1
                continue

            # 5. Strażnik Sektora
            is_sector_healthy, sector_trend, etf_symbol = _check_sector_health(session, api_client, sector)
            
            # === SUKCES - Dodaj do bufora ===
            candidates_buffer.append({
                'ticker': ticker, 
                'price': float(current_price),
                'volume': int(latest_candle['volume']),
                'sector_ticker': etf_symbol,
                'sector_trend': float(sector_trend)
            })
            
            final_candidate_tickers.append(ticker)
            
            # Jeśli bufor pełny (100), zrzucamy do bazy
            if len(candidates_buffer) >= BATCH_SIZE:
                _flush_candidates_batch(session, candidates_buffer)
                candidates_buffer = [] 

        except Exception as e:
            logger.error(f"Error F1 logic for {ticker}: {e}")
            continue
    
    # Na koniec pętli zapisz resztki
    if candidates_buffer:
        _flush_candidates_batch(session, candidates_buffer)

    update_scan_progress(session, total_tickers, total_tickers)
    
    summary_msg = (f"🏁 Faza 1 (Ultra Safe Mode) zakończona. Kandydatów: {len(final_candidate_tickers)}. "
                   f"Odrzuty: Trend(SMA200)={reject_stats['trend']}, Cena={reject_stats['price']}, Vol={reject_stats['volume']}")
    
    logger.info(summary_msg)
    append_scan_log(session, summary_msg)
    
    return final_candidate_tickers
