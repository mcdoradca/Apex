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

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    Skaner Fazy 1 (V5.3 - Ultra Verbose Logging).
    
    ZMIANY:
    - Logowanie KAŻDEGO sprawdzanego tickera z wynikiem weryfikacji.
    - Pełna transparentność odrzuceń (Cena, Vol, ATR).
    """
    logger.info("Running Phase 1: EOD Scan (V5.3 Ultra Verbose)...")
    append_scan_log(session, "Faza 1 (V5.3): Start skanowania. Tryb pełnego raportowania (każdy ticker).")

    try:
        session.execute(text("DELETE FROM phase1_candidates"))
        session.commit()
    except Exception as e:
        logger.error(f"Failed to clear Phase 1 table: {e}", exc_info=True)
        session.rollback()
        return [] 

    try:
        all_tickers_rows = session.execute(text("SELECT ticker, sector FROM companies ORDER BY ticker")).fetchall()
        total_tickers = len(all_tickers_rows)
        logger.info(f"Found {total_tickers} tickers to process.")
    except Exception as e:
        logger.error(f"Could not fetch companies: {e}", exc_info=True)
        return []

    final_candidate_tickers = []
    reject_stats = {'price': 0, 'volume': 0, 'atr': 0, 'intraday': 0, 'sector': 0, 'data': 0}
    
    start_time = time.time()

    for processed_count, row in enumerate(all_tickers_rows):
        ticker = row[0]
        sector = row[1]
        
        # Obsługa pauzy
        if get_current_state() == 'PAUSED':
            append_scan_log(session, "Skaner wstrzymany (PAUZA)...")
            while get_current_state() == 'PAUSED': time.sleep(1)
            append_scan_log(session, "Skaner wznowiony.")

        # Pasek postępu (dla UI)
        if processed_count % 10 == 0: 
            update_scan_progress(session, processed_count, total_tickers)

        # Logowanie "Heartbeat" do konsoli (żeby nie spamować, ale wiedzieć że działa)
        if processed_count > 0 and processed_count % 100 == 0:
            elapsed = time.time() - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0
            logger.info(f"F1 Heartbeat: {processed_count}/{total_tickers} ({rate:.1f} t/s)")

        try:
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                reject_stats['data'] += 1
                append_scan_log(session, f"❌ {ticker}: Brak danych dziennych (API).")
                continue
            
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)

            if len(daily_df) < 50: 
                reject_stats['data'] += 1
                append_scan_log(session, f"❌ {ticker}: Za krótka historia ({len(daily_df)} dni).")
                continue

            latest_candle = daily_df.iloc[-1]
            current_price = latest_candle['close']
            
            if pd.isna(current_price): 
                append_scan_log(session, f"❌ {ticker}: Błąd ceny (NaN).")
                continue
                
            # === FILTRY V5 (Z PEŁNYM LOGOWANIEM) ===

            # 1. Cena (1-20$)
            if not (1.0 <= current_price <= 20.0): 
                reject_stats['price'] += 1
                append_scan_log(session, f"❌ {ticker}: Cena {current_price:.2f}$ (Wymagane 1-20$)")
                continue
            
            # 2. Płynność (Vol > 600k)
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            if pd.isna(avg_volume) or avg_volume < 600000: 
                reject_stats['volume'] += 1
                vol_display = f"{int(avg_volume/1000)}k" if not pd.isna(avg_volume) else "NaN"
                append_scan_log(session, f"❌ {ticker}: Wolumen {vol_display} (Wymagane >300k)")
                continue
            
            # 3. Zmienność (ATR > 3%)
            atr_series = calculate_atr(daily_df, period=14)
            if atr_series.empty: 
                append_scan_log(session, f"❌ {ticker}: Błąd obliczania ATR.")
                continue
            
            current_atr = atr_series.iloc[-1]
            atr_percent = (current_atr / current_price)
            if atr_percent < 0.03: 
                reject_stats['atr'] += 1
                append_scan_log(session, f"❌ {ticker}: Niska zmienność ATR {atr_percent:.2%} (Wymagane >3%)")
                continue 
            
            # 4. Dane Intraday (Wstępna weryfikacja dostępności)
            try:
                intraday_test = api_client.get_intraday(ticker, interval='60min', outputsize='compact')
                if intraday_test and 'Time Series (60min)' not in intraday_test and 'Information' not in intraday_test:
                     reject_stats['intraday'] += 1
                     append_scan_log(session, f"❌ {ticker}: Brak danych Intraday (wymagane w F3).")
                     continue
            except: pass

            # 5. Strażnik Sektora (Logujący)
            is_sector_healthy, sector_trend, etf_symbol = _check_sector_health(session, api_client, sector)
            sector_msg = f"Sektor {etf_symbol} OK" if is_sector_healthy else f"⚠️ Sektor {etf_symbol} słaby"
            if not is_sector_healthy:
                reject_stats['sector'] += 1
                # Uwaga: Faza 1 przepuszcza słaby sektor, ale logujemy to jako ostrzeżenie w sukcesie
            
            # === KWALIFIKACJA SUKCES ===
            log_msg = (f"✅ KANDYDAT: {ticker} | Cena: {current_price:.2f}$ | "
                       f"Vol: {int(avg_volume/1000)}k | ATR: {atr_percent:.1%} | {sector_msg}")
            logger.info(log_msg)
            append_scan_log(session, log_msg)
            
            insert_stmt = text("""
                INSERT INTO phase1_candidates (ticker, price, volume, change_percent, score, sector_ticker, sector_trend_score, analysis_date)
                VALUES (:ticker, :price, :volume, 0.0, 1, :sector_ticker, :sector_trend, NOW())
            """)
            
            session.execute(insert_stmt, {
                'ticker': ticker, 
                'price': float(current_price),
                'volume': int(latest_candle['volume']),
                'sector_ticker': etf_symbol,
                'sector_trend': float(sector_trend)
            })
            session.commit()
            
            final_candidate_tickers.append(ticker)

        except Exception as e:
            logger.error(f"Error F1 for {ticker}: {e}")
            append_scan_log(session, f"⛔ BŁĄD SYSTEMU dla {ticker}: {e}")
            session.rollback()
    
    update_scan_progress(session, total_tickers, total_tickers)
    
    summary_msg = (f"🏁 Faza 1 zakończona. Kandydatów: {len(final_candidate_tickers)}. "
                   f"Statystyki odrzuceń: Cena={reject_stats['price']}, Vol={reject_stats['volume']}, "
                   f"ATR={reject_stats['atr']}, Dane={reject_stats['data']}")
    
    logger.info(summary_msg)
    append_scan_log(session, summary_msg)
    
    return final_candidate_tickers
