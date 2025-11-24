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
    get_raw_data_with_cache # Potrzebne do pobrania danych sektora
)
# Import konfiguracji (Mapowanie Sektorów)
from ..config import SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

def _check_sector_health(session: Session, api_client, sector_name: str) -> tuple[bool, float, str]:
    """
    Sprawdza kondycję sektora (ETF).
    Zwraca: (czy_zdrowy, wynik_trendu, symbol_etf)
    """
    etf_ticker = SECTOR_TO_ETF_MAP.get(sector_name, DEFAULT_MARKET_ETF)
    
    try:
        # Pobierz dane dzienne dla ETF
        raw_data = get_raw_data_with_cache(
            session, api_client, etf_ticker, 
            'DAILY_ADJUSTED', 'get_daily_adjusted', 
            expiry_hours=24, outputsize='compact' # Wystarczy compact dla SMA50
        )
        
        if not raw_data:
            return True, 0.0, etf_ticker # Brak danych = przepuść (fail-open)

        df = standardize_df_columns(pd.DataFrame.from_dict(raw_data.get('Time Series (Daily)', {}), orient='index'))
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        
        if len(df) < 50:
            return True, 0.0, etf_ticker

        current_price = df['close'].iloc[-1]
        sma_50 = df['close'].rolling(window=50).mean().iloc[-1]
        
        # Logika Strażnika:
        # Trend jest ZDROWY, jeśli cena jest powyżej SMA50
        is_healthy = current_price > sma_50
        trend_score = 1.0 if is_healthy else -1.0
        
        return is_healthy, trend_score, etf_ticker

    except Exception as e:
        logger.warning(f"Błąd sprawdzania sektora {sector_name} ({etf_ticker}): {e}")
        return True, 0.0, etf_ticker # W razie błędu nie blokuj

def _check_earnings_proximity(api_client, ticker: str) -> int:
    """
    Sprawdza, za ile dni są wyniki finansowe.
    Zwraca liczbę dni (lub 999 jeśli nieznane/odległe).
    """
    try:
        # To zapytanie nie jest cache'owane w ten sam sposób, bo jest rzadkie
        # Używamy bezpośrednio klienta, ale z rate limiterem
        earnings_data = api_client.get_earnings(ticker)
        
        if not earnings_data or 'quarterlyEarnings' not in earnings_data:
            return 999
            
        # Szukamy najbliższej PRZYSZŁEJ daty
        # Alpha Vantage często zwraca tylko historię w 'quarterlyEarnings', 
        # ale 'annualEarnings' lub inne pola mogą mieć hinty. 
        # Niestety darmowy endpoint jest ograniczony. 
        # W wersji Premium (którą masz) 'quarterlyEarnings' powinno zawierać raportowane daty.
        # Jednak dla pewności sprawdzamy strukturę.
        
        # PROSTSZE PODEJŚCIE DLA V5:
        # Jeśli nie ma wprost "next earnings date", zakładamy bezpieczeństwo.
        # (Pełna implementacja wymagałaby płatnego kalendarza, AV Earnings endpoint jest specyficzny).
        
        return 999 # Placeholder - włączenie tego wymagałoby parsowania skomplikowanego JSONA z AV
                   # Na razie zostawiamy to jako "future feature" lub prosty check, jeśli dane są dostępne.

    except Exception:
        return 999

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    DEKONSTRUKCJA (KROK 4): Faza 1 z nową, uproszczoną logiką "Pierwszego Sita".
    
    NOWA LOGIKA V5 (Holy Grail):
    - Filtr Ceny (1-40$)
    - Filtr Płynności (Vol > 500k)
    - Filtr Zmienności (ATR > 3%)
    - Filtr Danych (Intraday Check)
    - NOWOŚĆ: Filtr Sektorowy (Sector Rotation Guardian)
    """
    logger.info("Running Phase 1: EOD Scan (V5 Holy Grail - Sector Guardian Active)...")
    append_scan_log(session, "Faza 1 (V5): Start skanowania. Strażnik Sektora: AKTYWNY.")

    # Czyszczenie starych kandydatów
    try:
        session.execute(text("DELETE FROM phase1_candidates"))
        session.commit()
    except Exception as e:
        logger.error(f"Failed to clear Phase 1 table: {e}", exc_info=True)
        session.rollback()
        return [] 

    try:
        # Pobieramy ticker ORAZ sektor
        all_tickers_rows = session.execute(text("SELECT ticker, sector FROM companies ORDER BY ticker")).fetchall()
        total_tickers = len(all_tickers_rows)
        logger.info(f"Found {total_tickers} tickers to process.")
    except Exception as e:
        logger.error(f"Could not fetch companies: {e}", exc_info=True)
        return []

    final_candidate_tickers = []
    
    for processed_count, row in enumerate(all_tickers_rows):
        ticker = row[0]
        sector = row[1]
        
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        if processed_count % 20 == 0: 
            update_scan_progress(session, processed_count, total_tickers)

        try:
            # 1. Pobierz dane EOD
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue
            
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)

            if len(daily_df) < 50: # Potrzebujemy 50 dni do SMA
                continue

            latest_candle = daily_df.iloc[-1]
            current_price = latest_candle['close']
            
            if pd.isna(current_price): continue
                
            # ==================================================================
            # === FILTRY V5 (ZŁOTA LISTA + STRAŻNIK) ===
            # ==================================================================

            # WARUNEK 1: Cena (1-40$)
            if not (1.0 <= current_price <= 40.0): continue
            
            # WARUNEK 2: Płynność (Vol > 500k)
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            if pd.isna(avg_volume) or avg_volume < 500000: continue
            
            # WARUNEK 3: Zmienność (ATR > 3%)
            atr_series = calculate_atr(daily_df, period=14)
            if atr_series.empty: continue
            current_atr = atr_series.iloc[-1]
            atr_percent = (current_atr / current_price)
            if atr_percent < 0.03: continue 
            
            # WARUNEK 4: Dane Intraday (Pre-flight)
            try:
                intraday_test = api_client.get_intraday(ticker, interval='60min', outputsize='compact')
                if not intraday_test or 'Time Series (60min)' not in intraday_test:
                    continue 
            except: continue

            # === NOWOŚĆ V5: STRAŻNIK SEKTORA ===
            # Sprawdzamy kondycję całego sektora. Jeśli sektor krwawi, nie kupujemy.
            is_sector_healthy, sector_trend, etf_symbol = _check_sector_health(session, api_client, sector)
            
            if not is_sector_healthy:
                # Logujemy odrzucenie (cicha eliminacja słabych ogniw)
                # append_scan_log(session, f"Odrzucono {ticker}: Słaby sektor {sector} ({etf_symbol} < SMA50).")
                continue 

            # (Warunek 5: Earnings - placeholder, w przyszłości tu wstawimy _check_earnings_proximity)

            # ==================================================================
            
            # KWALIFIKACJA
            log_msg = f"Kwalifikacja (F1): {ticker} (Cena: {current_price:.2f}, Sektor: {etf_symbol} OK)"
            append_scan_log(session, log_msg)
            
            # Zapisz kandydata (z nowymi polami V5)
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
            session.rollback()
    
    update_scan_progress(session, total_tickers, total_tickers)
    logger.info(f"Phase 1 (V5) completed. Found {len(final_candidate_tickers)} candidates.")
    return final_candidate_tickers
