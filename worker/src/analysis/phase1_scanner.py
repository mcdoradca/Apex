import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
# Usunięto import Phase1Config, ponieważ filtry są teraz wbudowane
# from ..config import Phase1Config 
from .utils import (
    append_scan_log, update_scan_progress, safe_float, 
    standardize_df_columns, 
    # Przywracamy pełne wykorzystanie calculate_atr
    calculate_atr 
)

logger = logging.getLogger(__name__)

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    DEKONSTRUKCJA (KROK 4): Faza 1 z nową, uproszczoną logiką "Pierwszego Sita".
    
    NOWA LOGIKA (KROK 5 - Filtr Wstępny):
    Dodano "próbne wywołanie" API (zgodnie z sugestią supportu AV),
    aby filtrować spółki, które nie wspierają danych intraday.
    
    AKTUALIZACJA: Przywrócono filtr Płynności (Wolumen) i dodano
    nowy filtr Zmienności (ATR), aby stworzyć "Złotą Listę".
    """
    logger.info("Running Phase 1: EOD Scan (New 'Golden List' Logic - Price, Volume, ATR, Data)...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania (Filtry: Cena, Płynność, Zmienność, Dane)...")

    # Czyszczenie starych kandydatów Fazy 1, aby uniknąć konfliktów
    try:
        session.execute(text("DELETE FROM phase1_candidates"))
        session.commit()
        logger.info("Cleared ALL old Phase 1 candidates.")
    except Exception as e:
        logger.error(f"Failed to clear Phase 1 table: {e}", exc_info=True)
        session.rollback()
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można wyczyścić tabeli Fazy 1: {e}")
        return [] 

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        total_tickers = len(all_tickers)
        logger.info(f"Found {total_tickers} tickers to process one-by-one using EOD data.")
        append_scan_log(session, f"Znaleziono {total_tickers} spółek w bazie do skanowania EOD.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}", exc_info=True)
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można pobrać listy spółek z bazy: {e}")
        return []

    if not all_tickers:
        logger.warning("Ticker list from database is empty. Phase 1 cannot proceed.")
        append_scan_log(session, "BŁĄD: Lista spółek do skanowania jest pusta.")
        return []

    # Lista tickerów, która zostanie przekazana do Fazy 3
    final_candidate_tickers = []
    
    for processed_count, ticker in enumerate(all_tickers):
        
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        if processed_count % 20 == 0: 
            update_scan_progress(session, processed_count, total_tickers)

        try:
            # 1. Pobierz dane EOD (compact wystarczy na 100 dni)
            # Potrzebujemy ich do obliczenia 20-dniowego średniego wolumenu
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                continue
            
            # 2. Standaryzuj dane
            daily_df_raw = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)

            # 3. Sprawdź, czy mamy wystarczająco danych (20 dni na średnią + 1 bieżący)
            if len(daily_df) < 21:
                continue

            # 4. Wyodrębnij dane
            latest_candle = daily_df.iloc[-1]
            current_price = latest_candle['close']
            
            if pd.isna(current_price):
                continue
                
            # ==================================================================
            # === NOWA LOGIKA FILTROWANIA (ZŁOTA LISTA) ===
            # ==================================================================

            # 5. Zastosuj FILTRY BEZWZGLĘDNE
            
            # WARUNEK 1: Cena (zgodnie z Pana prośbą)
            if not (1.0 <= current_price <= 40.0):
                continue
            
            # WARUNEK 2: Płynność (PRZYWRÓCONY)
            # Obliczamy z 20 ostatnich *zamkniętych* świec (przed 'latest')
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            if pd.isna(avg_volume) or avg_volume < 500000:
                continue
            
            # WARUNEK 3: Zmienność (NOWY)
            atr_series = calculate_atr(daily_df, period=14)
            if atr_series.empty or pd.isna(atr_series.iloc[-1]):
                continue # Nie można obliczyć ATR
                
            current_atr = atr_series.iloc[-1]
            if current_price == 0:
                continue # Unikaj dzielenia przez zero
                
            atr_percent = (current_atr / current_price)
            if atr_percent < 0.03: # Wymagamy minimum 3% zmienności
                continue 
            
            # ==================================================================
            # === NOWY FILTR WSTĘPNY (PRE-FLIGHT CHECK) ===
            # (Zgodnie z Pana pomysłem i sugestią Supportu AV)
            # ==================================================================
            
            # WARUNEK 4: Sprawdzenie wsparcia dla danych H3/H4 (Intraday)
            try:
                # Wykonujemy lekkie, próbne wywołanie, aby sprawdzić, czy spółka wspiera dane intraday.
                intraday_test_data = api_client.get_intraday(
                    ticker, 
                    interval='60min', 
                    outputsize='compact',
                    extended_hours=False
                )
                
                # Jeśli API zwróci błąd lub puste dane, odrzucamy spółkę
                if not intraday_test_data or 'Time Series (60min)' not in intraday_test_data or not intraday_test_data['Time Series (60min)']:
                    append_scan_log(session, f"INFO (F1): {ticker} - Odrzucony (brak danych Intraday dla H3/H4).")
                    continue # Pomiń tę spółkę
                    
            except Exception as e:
                logger.warning(f"Błąd filtru Intraday dla {ticker}: {e}. Odrzucanie.")
                continue
            
            # (Na razie pomijamy test INSIDER_TRANSACTIONS, aby nie potroić czasu skanowania)
            
            # ==================================================================
            
            # 6. KWALIFIKACJA
            # ZMIANA: Dodano Śr. Wol i ATR do logu
            log_msg = f"Kwalifikacja (F1): {ticker} (Cena: {current_price:.2f}, Śr. Wol: {avg_volume:.0f}, ATR: {atr_percent:.1%}, Dane Intraday: OK)"
            append_scan_log(session, log_msg)
            
            # 7. Zapisz kandydata w bazie
            candidate_data = {
                'ticker': ticker, 
                'price': float(current_price),
                'volume': int(latest_candle['volume']), # Zapisujemy bieżący wolumen
                'change_percent': 0.0, # Pole nieużywane, zapisujemy 0.0
                'score': 1 # Wartość zastępcza
            }
            
            insert_stmt = text("""
                INSERT INTO phase1_candidates (ticker, price, volume, change_percent, score, analysis_date)
                VALUES (:ticker, :price, :volume, :change_percent, :score, NOW())
            """)
            session.execute(insert_stmt, [candidate_data])
            session.commit() # Zapisujemy tego JEDNEGO kandydata
            
            # Dodajemy ticker do listy, którą przekażemy do Fazy 3
            final_candidate_tickers.append(ticker)

        except Exception as e:
            logger.error(f"Error in Phase 1 EOD processing for {ticker}: {e}", exc_info=True)
            session.rollback() # Wycofaj błąd tylko dla tego jednego tickera
    
    # Zakończ postęp
    update_scan_progress(session, total_tickers, total_tickers)

    logger.info(f"Phase 1 (New Sieve) completed. Found {len(final_candidate_tickers)} final candidates.")
    append_scan_log(session, f"Faza 1 (Nowe Sito z Filtrem Wstępnym) zakończona. Znaleziono {len(final_candidate_tickers)} kandydatów.")
    
    # Zwracamy listę tickerów
    return final_candidate_tickers
