import logging
import csv
from io import StringIO
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
# Poprawiony import, aby pasował do struktury workera
from ..config import Phase1Config
from .utils import append_scan_log, update_scan_progress, safe_float

logger = logging.getLogger(__name__)

def run_scan(session: Session, get_current_state, api_client) -> list[str]:
    """
    Przeprowadza skanowanie Fazy 1 z poprawioną obsługą wartości None.
    """
    logger.info("Running Phase 1: Advanced Momentum Scan...")
    append_scan_log(session, "Faza 1: Rozpoczynanie skanowania Advanced Momentum...")

    try:
        all_tickers_rows = session.execute(text("SELECT ticker FROM companies ORDER BY ticker")).fetchall()
        all_tickers = [row[0] for row in all_tickers_rows]
        total_tickers = len(all_tickers)
        logger.info(f"Found {total_tickers} tickers to process.")
        append_scan_log(session, f"Znaleziono {total_tickers} spółek w bazie do skanowania.")
    except Exception as e:
        logger.error(f"Could not fetch companies from database: {e}", exc_info=True)
        append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie można pobrać listy spółek z bazy: {e}")
        return []

    if not all_tickers:
        logger.warning("Ticker list from database is empty. Phase 1 cannot proceed.")
        append_scan_log(session, "BŁĄD: Lista spółek do skanowania jest pusta.")
        return []

    # --- ETAP 1: Szybkie skanowanie blokowe ---
    append_scan_log(session, "Etap 1: Szybkie skanowanie blokowe...")
    pre_candidates = []
    chunk_size = 100 # Rozmiar bloku tickerów do przetworzenia naraz

    detailed_logs_count = 0
    max_detailed_logs = 50 # Ograniczenie liczby szczegółowych logów odrzuceń

    for i in range(0, total_tickers, chunk_size):
        chunk = all_tickers[i:i + chunk_size]
        try:
            # Używamy klienta API workera (już zsynchronizowanego)
            parsed_data = api_client.get_bulk_quotes(chunk) # Zwraca słownik

            if not parsed_data:
                logger.warning(f"[DIAGNOSTYKA] Nie otrzymano sparsowanych danych dla chunka zaczynającego się od {chunk[0]}.")
                continue

            for ticker in chunk:
                data = parsed_data.get(ticker)
                if not data:
                    # Logujemy brak danych dla tickera, jeśli to nieoczekiwane
                    # logger.debug(f"No data found for {ticker} in parsed bulk quotes.")
                    continue

                # Używamy 'close' jako bieżącej ceny, zgodnie z ustaleniami
                price = data.get('close')
                volume = data.get('volume')
                change_percent = data.get('change_percent')

                # --- POCZĄTEK POPRAWIONEJ WALIDACJI ---
                price_float = safe_float(price)
                volume_float = safe_float(volume)
                change_percent_float = safe_float(change_percent)

                is_ok = True
                reasons = []

                # Sprawdzenie Ceny
                if price_float is None:
                    is_ok = False
                    reasons.append("Invalid Price (None or unparseable)")
                elif not (Phase1Config.MIN_PRICE <= price_float <= Phase1Config.MAX_PRICE):
                    is_ok = False
                    # Używamy sformatowanej wartości float
                    reasons.append(f"Price {price_float:.2f} out of range ({Phase1Config.MIN_PRICE}-{Phase1Config.MAX_PRICE})")

                # Sprawdzenie Wolumenu
                if volume_float is None:
                    is_ok = False
                    reasons.append("Invalid Volume (None or unparseable)")
                elif volume_float < Phase1Config.MIN_VOLUME:
                    is_ok = False
                    # Używamy sformatowanej wartości float
                    reasons.append(f"Volume {volume_float:.0f} < {Phase1Config.MIN_VOLUME}")

                # Sprawdzenie Zmiany Procentowej
                if change_percent_float is None:
                    # Zgodnie z poprzednią logiką, brak danych o zmianie odrzuca
                    is_ok = False
                    reasons.append("Invalid Change Percent (None or unparseable)")
                elif change_percent_float < Phase1Config.MIN_DAY_CHANGE_PERCENT:
                    is_ok = False
                    # Używamy sformatowanej wartości float
                    reasons.append(f"Change {change_percent_float:.2f}% < {Phase1Config.MIN_DAY_CHANGE_PERCENT}%")
                # --- KONIEC POPRAWIONEJ WALIDACJI ---

                if is_ok:
                    pre_candidates.append({
                        'ticker': ticker,
                        'price': price_float, # Zapisujemy przekonwertowaną wartość
                        'volume': int(volume_float), # Zapisujemy przekonwertowaną wartość jako int
                        'change_percent': change_percent_float # Zapisujemy przekonwertowaną wartość
                    })
                # Logowanie odrzuceń (tylko ograniczona liczba dla przejrzystości)
                elif detailed_logs_count < max_detailed_logs:
                    logger.info(f"[DIAGNOSTYKA] Odrzucono {ticker}: {'; '.join(reasons)}")
                    detailed_logs_count += 1

        except Exception as e:
            # Logujemy błąd przetwarzania chunka, ale kontynuujemy z następnym
            logger.error(f"Error processing bulk chunk starting with {chunk[0]}: {e}", exc_info=True)
            # Nie przerywamy całego skanowania z powodu błędu w jednym chunku
            # Dodajemy wpis do logów systemowych
            append_scan_log(session, f"BŁĄD: Nie udało się przetworzyć bloku danych zaczynającego się od {chunk[0]}: {e}")


        # Aktualizacja postępu po przetworzeniu (lub próbie przetworzenia) chunka
        update_scan_progress(session, min(i + chunk_size, total_tickers), total_tickers)

    logger.info(f"Stage 1 found {len(pre_candidates)} pre-candidates.")
    append_scan_log(session, f"Etap 1 zakończony. Znaleziono {len(pre_candidates)} wstępnych kandydatów.")

    if not pre_candidates:
        append_scan_log(session, "Brak kandydatów po etapie 1. Zakończono Fazę 1.")
        # Zmieniono: Zamiast rzucać wyjątek, zwracamy pustą listę,
        # co pozwoli głównej pętli na normalne zakończenie cyklu.
        logger.warning("Phase 1 finished with no candidates. Analysis cycle will end after this phase.")
        return [] # Zwróć pustą listę zamiast rzucać wyjątek

    # --- ETAP 2: Głęboka analiza zaawansowana ---
    # Ta część jest wykonywana tylko jeśli są kandydaci z Etapu 1
    append_scan_log(session, "Etap 2: Głęboka analiza zaawansowana (Wolumen Względny i ATR)...")

    final_candidates = []

    # Licznik dla postępu Etapu 2
    total_pre_candidates = len(pre_candidates)
    processed_stage2_count = 0

    for candidate in pre_candidates:
        ticker = candidate['ticker']
        try:
            # Pobieranie danych dziennych
            price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='compact')
            if not price_data_raw or 'Time Series (Daily)' not in price_data_raw:
                logger.warning(f"Stage 2: No daily data for {ticker}. Skipping.")
                continue

            # Konwersja do DataFrame i standaryzacja (używamy tej samej logiki co w Phase 3)
            daily_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')

            # Standaryzacja nazw kolumn - użyjmy funkcji pomocniczej jeśli istnieje
            # lub prostej logiki jak w phase3_sniper
            try:
                 daily_df.columns = [col.split('. ')[-1] for col in daily_df.columns]
            except Exception as e_col:
                 logger.debug(f"Error standardizing columns for {ticker} (might be standard): {e_col}")
            # Konwersja na numeryczne
            for col in ['open', 'high', 'low', 'close', 'volume']:
                 if col in daily_df.columns:
                      daily_df[col] = pd.to_numeric(daily_df[col], errors='coerce')

            daily_df.sort_index(inplace=True)

            # Weryfikacja Wolumenu Względnego
            if len(daily_df) < 22: # Potrzebujemy co najmniej 21 dni historii + bieżący
                logger.warning(f"Stage 2: Not enough daily history for {ticker} ({len(daily_df)} days). Skipping.")
                continue

            # Średni wolumen z ostatnich 20 dni (bez bieżącego dnia - [-21:-1])
            avg_volume = daily_df['volume'].iloc[-21:-1].mean()
            current_volume = candidate['volume'] # Wolumen z Etapu 1 (bulk quotes)

            if pd.isna(avg_volume) or avg_volume == 0:
                logger.warning(f"Stage 2: Invalid average volume for {ticker}. Skipping.")
                continue

            volume_ratio = current_volume / avg_volume
            volume_ratio_ok = volume_ratio >= Phase1Config.MIN_VOLUME_RATIO

            if not volume_ratio_ok:
                logger.info(f"Stage 2: {ticker} rejected. Volume Ratio {volume_ratio:.2f} < {Phase1Config.MIN_VOLUME_RATIO}")
                continue

            # Weryfikacja ATR%
            atr_data_raw = api_client.get_atr(ticker)
            if not atr_data_raw or 'Technical Analysis: ATR' not in atr_data_raw:
                logger.warning(f"Stage 2: No ATR data for {ticker}. Skipping.")
                continue

            # Znajdź najnowszy wpis ATR
            atr_series = atr_data_raw['Technical Analysis: ATR']
            if not atr_series:
                 logger.warning(f"Stage 2: Empty ATR series for {ticker}. Skipping.")
                 continue
            latest_atr_date = sorted(atr_series.keys())[-1]
            latest_atr = safe_float(atr_series[latest_atr_date]['ATR'])
            current_price = candidate['price'] # Cena z Etapu 1 (bulk quotes)

            if not current_price or not latest_atr or current_price == 0:
                logger.warning(f"Stage 2: Invalid price or ATR for ATR% calculation for {ticker} (Price: {current_price}, ATR: {latest_atr}). Skipping.")
                continue

            # Oblicz ATR jako procent ceny
            atr_percent = (latest_atr / current_price) * 100 # Mnożymy przez 100, bo MAX_VOLATILITY_ATR_PERCENT jest w %
            atr_ok = atr_percent <= Phase1Config.MAX_VOLATILITY_ATR_PERCENT

            if not atr_ok:
                logger.info(f"Stage 2: {ticker} rejected. ATR Percent {atr_percent:.2f}% > {Phase1Config.MAX_VOLATILITY_ATR_PERCENT}%")
                continue

            # Jeśli doszliśmy tutaj, ticker przeszedł wszystkie testy Etapu 2
            final_candidates.append(candidate)
            log_msg = f"Kwalifikacja (F1): {ticker} (VolRatio: {volume_ratio:.2f}, ATR%: {atr_percent:.2f}%)"
            append_scan_log(session, log_msg)
            logger.info(log_msg)

        except Exception as e:
            logger.error(f"Error in Stage 2 processing for {ticker}: {e}", exc_info=True)
            append_scan_log(session, f"BŁĄD: Nie udało się przetworzyć {ticker} w Etapie 2: {e}")
        finally:
             processed_stage2_count += 1
             # Aktualizacja postępu wewnątrz Etapu 2 (opcjonalne, może spowalniać)
             # update_scan_progress(session, processed_stage2_count, total_pre_candidates)


    logger.info(f"Phase 1 (Stage 2) completed. Found {len(final_candidates)} final candidates.")
    append_scan_log(session, f"Faza 1 (Etap 2) zakończona. Znaleziono {len(final_candidates)} ostatecznych kandydatów.")

    if final_candidates:
        try:
            # Przygotuj dane do wstawienia
            candidates_to_insert = [
                {
                    'ticker': c['ticker'],
                    'price': c['price'],
                    'volume': c['volume'],
                    'change_percent': c['change_percent'],
                    'score': 1 # Można by tu dodać bardziej złożony scoring, np. na podstawie siły VolRatio i ATR%
                } for c in final_candidates
            ]

            # Używamy INSERT ... ON CONFLICT DO UPDATE, aby obsłużyć istniejące wpisy
            insert_stmt = text("""
                INSERT INTO phase1_candidates (ticker, price, volume, change_percent, score, analysis_date)
                VALUES (:ticker, :price, :volume, :change_percent, :score, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    change_percent = EXCLUDED.change_percent,
                    score = EXCLUDED.score,
                    analysis_date = EXCLUDED.analysis_date;
            """)
            session.execute(insert_stmt, candidates_to_insert)
            session.commit()
            append_scan_log(session, f"Zapisano {len(final_candidates)} kandydatów Fazy 1 w bazie danych.")
        except Exception as e:
            logger.error(f"Failed to save Phase 1 candidates to database: {e}", exc_info=True)
            append_scan_log(session, f"BŁĄD KRYTYCZNY: Nie udało się zapisać kandydatów Fazy 1 do bazy: {e}")
            session.rollback()
            # W tym przypadku nie zwracamy tickerów, bo zapis się nie udał
            return []

    # Zwracamy listę tickerów, które przeszły obie części Fazy 1
    return [c['ticker'] for c in final_candidates]
