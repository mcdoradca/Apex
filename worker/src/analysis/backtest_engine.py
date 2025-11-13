import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

# Krok 18: Importujemy kalkulatory metryk
from . import aqm_v3_metrics
# Krok 19b: Importujemy nowy silnik symulacji H1
from . import aqm_v3_h1_simulator 
# Krok 20b (Część 2): Importujemy nowy silnik ładowania H2
from . import aqm_v3_h2_loader
# Krok 21c: Importujemy nowy silnik symulacji H2
from . import aqm_v3_h2_simulator
# === INTEGRACJA H3 (KROK 1): Import nowego symulatora H3 ===
from . import aqm_v3_h3_simulator
# === INTEGRACJA H4 (KROK 1): Import nowego symulatora H4 ===
from . import aqm_v3_h4_simulator
# Importujemy funkcje parsowania z H3 loader (potrzebne do przetwarzania surowych danych)
from .aqm_v3_h3_loader import _parse_bbands, _parse_intraday_5min 

from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    get_current_NY_datetime,
    append_scan_log,
    update_scan_progress,
    calculate_atr,
    # === NOWY IMPORT: Funkcja cache z utils ===
    get_raw_data_with_cache
)
from .. import models
from ..config import SECTOR_TO_ETF_MAP
# === NOWY IMPORT: Garbage Collector ===
import gc

logger = logging.getLogger(__name__)

# ==================================================================
# === PAMIĘĆ PODRĘCZNA (CACHE) LITE (Bez zmian) ===
# ==================================================================
_backtest_cache = {
    "vix_data": None, 
    "spy_data": None, 
    "sector_etf_data": {}, 
    "tickers_by_sector": {}, 
    "sector_map": {} 
}

def _get_tickers_in_sector(sector: str) -> List[str]:
    """Pobiera listę tickerów dla danego sektora z cache."""
    return _backtest_cache["tickers_by_sector"].get(sector, [])

def _get_sector_for_ticker(session: Session, ticker: str) -> str:
    """Pobiera sektor dla tickera z bazy danych (z cache)."""
    if ticker not in _backtest_cache["sector_map"]:
        try:
            sector = session.execute(
                text("SELECT sector FROM companies WHERE ticker = :ticker"),
                {'ticker': ticker}
            ).scalar()
            _backtest_cache["sector_map"][ticker] = sector or "N/A"
        except Exception as e:
            logger.error(f"Nie udało się pobrać sektora dla {ticker}: {e}")
            _backtest_cache["sector_map"][ticker] = "N/A"
            
    return _backtest_cache["sector_map"][ticker]
# ==================================================================


# ==================================================================
# === DETEKTOR REŻIMU RYNKOWEGO (Bez zmian) ===
# ==================================================================
def _detect_market_regime(current_date_str: str) -> str:
    """
    Wykrywa reżim rynkowy na podstawie danych VXX i SPY z cache.
    """
    try:
        vix_df = _backtest_cache["vix_data"]
        spy_df = _backtest_cache["spy_data"]
        
        vix_row = vix_df[vix_df.index <= current_date_str].iloc[-1]
        spy_row = spy_df[spy_df.index <= current_date_str].iloc[-1]
        
        vix_price = vix_row['close']
        spy_price = spy_row['close']
        spy_sma_50 = spy_row['ema_50']
        spy_sma_200 = spy_row['ema_200']

        if pd.isna(vix_price) or pd.isna(spy_price) or pd.isna(spy_sma_50) or pd.isna(spy_sma_200):
            return 'volatile' 

        if vix_price < 18 and spy_price > spy_sma_200:
            return 'bull'
        elif vix_price > 25 or spy_price < spy_sma_50: 
            return 'bear'
        else:
            return 'volatile'
            
    except IndexError:
        return 'volatile'
    except Exception as e:
        logger.error(f"Błąd podczas wykrywania reżimu dla {current_date_str}: {e}")
        return 'volatile'

# ==================================================================
# === NOWA FUNKCJA POMOCNICZA: Parser VWAP (Intraday -> Daily) ===
# ==================================================================
def _parse_vwap_intraday_to_daily(raw_data_list: List[Dict[str, Any]]) -> Optional[pd.DataFrame]:
    """
    Przetwarza LISTĘ surowych odpowiedzi JSON z VWAP (Intraday, 12 miesięcy) 
    na DataFrame i oblicza średni DZIENNY VWAP.
    """
    try:
        all_vwap_df = pd.DataFrame()
        
        for raw_data in raw_data_list:
            # Klucz to 'Technical Analysis: VWAP' lub 'Technical Analysis: VWAP (60min)'
            data_keys = [k for k in raw_data.keys() if k.startswith('Technical Analysis: VWAP')]
            if not data_keys:
                continue # Pomiń ten miesiąc, jeśli brak danych
                
            data = raw_data[data_keys[0]] # Pobierz dane z dynamicznego klucza
            if not data:
                continue

            df = pd.DataFrame.from_dict(data, orient='index')
            all_vwap_df = pd.concat([all_vwap_df, df])

        if all_vwap_df.empty:
            return pd.DataFrame(columns=['vwap']).set_index(pd.to_datetime([]))

        all_vwap_df.index = pd.to_datetime(all_vwap_df.index)
        
        # Konwertuj na liczby
        all_vwap_df['VWAP'] = pd.to_numeric(all_vwap_df['VWAP'], errors='coerce')
        
        # === KLUCZOWY KROK: Agregacja danych Intraday do Dziennych ===
        # Obliczamy średni VWAP dla każdego dnia
        daily_vwap_df = all_vwap_df['VWAP'].resample('D').mean()
        
        # Zmień nazwę kolumny na tę, której oczekuje reszta systemu
        daily_vwap_df = daily_vwap_df.to_frame(name='vwap')
            
        daily_vwap_df.sort_index(inplace=True)
        return daily_vwap_df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania i agregowania danych VWAP: {e}", exc_info=True)
        return None
# ==================================================================


# ==================================================================
# === ZMIANA KROK 3b: ŁADOWANIE DANYCH Z CACHE DLA JEDNEGO TICKERA ===
# ==================================================================
def _load_all_data_for_ticker(ticker: str, api_client: AlphaVantageClient, session: Session, year_to_test: str) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Pobiera i wstępnie przetwarza wszystkie dane (H1, H2, H3, H4) dla pojedynczego tickera,
    korzystając z mechanizmu cache w bazie danych.
    """
    try:
        # --- KROK 1: ŁADOWANIE H1 (Daily Time Series) ---
        price_data_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='DAILY_WITH_VWAP',
            api_func='get_time_series_daily', outputsize='full'
        )
        # TIME_SERIES_WEEKLY_ADJUSTED
        weekly_data_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='WEEKLY',
            api_func='get_time_series_weekly', outputsize='full'
        )
        
        # ==================================================================
        # === NOWE WYWOŁANIE (NAPRAWA VWAP): Pętla 12 miesięcy dla VWAP ===
        # ==================================================================
        
        # Potrzebujemy danych z roku `year_to_test` ORAZ roku poprzedniego (dla historii wskaźników)
        years_needed = [str(int(year_to_test) - 1), year_to_test]
        months_needed = []
        for year in years_needed:
            for month in range(1, 13):
                months_needed.append(f"{year}-{month:02d}") # Format YYYY-MM
        
        vwap_raw_list = []
        
        for month_str in months_needed:
            # Używamy 60min, aby dostać się jak najdalej wstecz
            # Używamy unikalnego data_type dla cache per miesiąc
            vwap_raw_month = get_raw_data_with_cache(
                session=session, api_client=api_client, ticker=ticker, 
                data_type=f'VWAP_INTRADAY_60MIN_{month_str}',
                api_func='get_vwap', 
                interval='60min', 
                month=month_str
            )
            if vwap_raw_month:
                vwap_raw_list.append(vwap_raw_month)
        
        # ==================================================================
        
        # --- KROK 2: ŁADOWANIE H3 (BBands i Intraday 5min) ---
        bbands_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='BBANDS',
            api_func='get_bollinger_bands', interval='daily', time_period=20, nbdevup=2, nbdevdn=2
        )
        
        # Dla H3/H4 potrzebujemy danych 5-minutowych. Musimy je pobrać dla każdego miesiąca.
        intraday_raw_list = []
        for month_str in months_needed:
            intraday_raw_month = get_raw_data_with_cache(
                session=session, api_client=api_client, ticker=ticker,
                data_type=f'INTRADAY_5MIN_{month_str}',
                api_func='get_intraday',
                interval='5min',
                month=month_str,
                outputsize='full' # Musimy użyć 'full', aby dostać pełne dane miesięczne
            )
            if intraday_raw_month:
                intraday_raw_list.append(intraday_raw_month)

        
        # --- KROK 3: ŁADOWANIE H2 (Insider i News) ---
        # NOTE: load_h2_data_into_cache ma już wbudowany mechanizm cache
        h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
        
        # 4. Walidacja danych
        if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
           not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw:
            
            logger.warning(f"[Backtest V3] Brak podstawowych danych (Daily/Weekly) z cache/API dla {ticker}, pomijanie.")
            return None
            
        # --- KROK 5: Przetwarzanie i Wzbogacanie Daily DF ---
        
        # Przetwórz Daily (H1)
        daily_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
        daily_df.index = pd.to_datetime(daily_df.index) # <-- POPRAWKA 1 (Z POPRZEDNIEJ RUNDY): Rozwiązuje błąd 'to_pydatetime'
        # UWAGA: `standardize_df_columns` (w `utils.py`) teraz poprawnie mapuje '5. vwap' -> 'vwap'
        daily_df = standardize_df_columns(daily_df)
        
        # Przetwórz Weekly (H1)
        weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
        weekly_df.index = pd.to_datetime(weekly_df.index)
        weekly_df = standardize_df_columns(weekly_df)
        
        # Wzbogacanie DataFrame (z użyciem danych SPY z Cache LITE)
        spy_aligned = _backtest_cache["spy_data"]['close'].reindex(daily_df.index, method='ffill').rename('spy_close')
        enriched_df = daily_df.join(spy_aligned) 
        enriched_df['atr_14'] = calculate_atr(enriched_df, period=14)
        
        # ==================================================================
        # === POPRAWKA (NAPRAWA VWAP): Dołączenie prawdziwych, zagregowanych danych VWAP ===
        # ==================================================================
        
        # Parsujemy dane VWAP (60min) i agregujemy je do Dziennych
        vwap_daily_df = _parse_vwap_intraday_to_daily(vwap_raw_list)
        
        if vwap_daily_df is not None and not vwap_daily_df.empty:
            # Dołączamy prawdziwy, obliczony Dzienny VWAP do naszego głównego DataFrame
            # `vwap` będzie teraz kolumną w `enriched_df`
            enriched_df = enriched_df.join(vwap_daily_df)
            logger.info(f"Pomyślnie obliczono i dołączono PRAWDZIWE dane VWAP (Daily) dla {ticker}.")
        else:
            logger.warning(f"Brak danych VWAP (Intraday) dla {ticker}. Proxy SMA(20) jest wyłączone. H1 nie wygeneruje sygnałów dla tej spółki.")
            # Tworzymy pustą kolumnę, aby uniknąć błędów, ale będzie pełna NaN
            enriched_df['vwap'] = np.nan
        # ==================================================================
        
        # Obliczenia H1 (time_dilation, price_gravity)
        ticker_returns_rolling = enriched_df['close'].pct_change().rolling(window=20)
        spy_returns_rolling = enriched_df['spy_close'].pct_change().rolling(window=20)
        std_ticker = ticker_returns_rolling.std()
        std_spy = spy_returns_rolling.std()
        enriched_df['time_dilation'] = std_ticker / std_spy
        
        # Obliczamy price_gravity. Jeśli 'vwap' to NaN, 'price_gravity' też będzie NaN.
        enriched_df['price_gravity'] = (enriched_df['vwap'] - enriched_df['close']) / enriched_df['close']
        
        enriched_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # POPRAWKA 3 (FutureWarning): Użyj bezpośredniego przypisania zamiast inplace
        enriched_df['time_dilation'] = enriched_df['time_dilation'].fillna(0)
        enriched_df['price_gravity'] = enriched_df['price_gravity'].fillna(0)

        # Przetwarzanie BBANDS (H3) i Intraday 5min (H3)
        # Te funkcje muszą teraz poprawnie obsłużyć puste dane
        bbands_df = _parse_bbands(bbands_raw)
        
        # === POPRAWKA (H3/H4): Scal 12 miesięcy danych 5-min ===
        all_intraday_df = pd.DataFrame()
        for raw_data in intraday_raw_list:
            df = _parse_intraday_5min(raw_data)
            if df is not None:
                all_intraday_df = pd.concat([all_intraday_df, df])
        
        intraday_5min_df = all_intraday_df.sort_index()
        # Usuń duplikaty indeksu, jeśli wystąpiły
        intraday_5min_df = intraday_5min_df[~intraday_5min_df.index.duplicated(keep='first')]
        # =======================================================
        
        # Zdobądź i zapisz sektor (dla spójności)
        sector = _get_sector_for_ticker(session, ticker)
        
        # Zwracamy wszystkie przetworzone dane
        return {
            "daily": enriched_df, 
            "weekly": weekly_df,
            "vwap": vwap_daily_df, # Przekazujemy również osobno, choć jest już w 'daily'
            "insider_df": h2_data["insider_df"], 
            "news_df": h2_data["news_df"],       
            "bbands_df": bbands_df,             
            "intraday_5min_df": intraday_5min_df, 
            "sector": sector
        }
    
    except Exception as e:
        logger.error(f"[Backtest V3] Błąd ładowania danych dla {ticker}: {e}", exc_info=True)
        return None
# ==================================================================


# ==================================================================
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (Bez zmian w logice pętli) ===
# ==================================================================
def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny.
    Optymalizacja pamięci: Ładowanie danych odbywa się per-ticker w pętli.
    """
    
    try:
        if not (year.isdigit() and len(year) == 4):
            raise ValueError(f"Otrzymano nieprawidłowy rok: {year}")
        
        current_year = datetime.now(timezone.utc).year
        if int(year) > current_year:
             raise ValueError(f"Nie można testować przyszłości: {year}")
        
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
    except Exception as e:
        logger.error(f"[Backtest] Błąd walidacji roku: {e}", exc_info=True)
        append_scan_log(session, f"[Backtest] BŁĄD: Nieprawidłowy format roku: {year}")
        return
        
    log_msg = f"BACKTEST HISTORYCZNY (Platforma AQM V3): Rozpoczynanie testu dla roku '{year}' ({start_date} do {end_date})"
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # === KROK 1: Czyszczenie Bazy Danych (Bez zmian) ===
    try:
        like_pattern = f"BACKTEST_{year}_AQM_V3_%"
        logger.info(f"Czyszczenie starych wyników AQM V3 dla wzorca: {like_pattern}...")
        
        delete_stmt = text("DELETE FROM virtual_trades WHERE setup_type LIKE :pattern")
        result = session.execute(delete_stmt, {'pattern': like_pattern})
        
        session.commit()
        logger.info(f"Pomyślnie usunięto {result.rowcount} starych wpisów backtestu AQM V3 dla roku {year}.")
        
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()

    # === KROK 2: Pobieranie Listy Spółek (Bez zmian) ===
    try:
        log_msg_tickers = "[Backtest V3] Pobieranie listy spółek z Fazy 1 ('Pierwsze Sito')..."
        logger.info(log_msg_tickers)
        append_scan_log(session, log_msg_tickers)
        
        tickers_p2_rows = session.execute(text(
            "SELECT DISTINCT ticker FROM phase1_candidates"
        )).fetchall()
        
        initial_tickers_to_test = sorted([row[0] for row in tickers_p2_rows])

        if not initial_tickers_to_test:
            log_msg = f"[Backtest] BŁĄD: Tabela 'phase1_candidates' jest pusta. Uruchom najpierw główny skan (przycisk 'Start'), aby zapełnić tę listę."
            logger.error(log_msg)
            append_scan_log(session, log_msg)
            return

        log_msg = f"[Backtest V3] Znaleziono {len(initial_tickers_to_test)} spółek z Fazy 1 do przetestowania."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów: {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # ==================================================================
    # === USUNIĘTY KROK (PRE-FLIGHT CHECK) ===
    # Zgodnie z Pana sugestią, usunęliśmy ten zbędny blok,
    # aby oszczędzić czas iteracji. Ufamy teraz liście z Fazy 1.
    # ==================================================================
    
    # === KROK 3: Budowanie Cache (TYLKO DANE GLOBALNE) ===
    
    try:
        logger.info("[Backtest V3] Rozpoczynanie budowania pamięci podręcznej (Cache LITE)...")
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}
        _backtest_cache["sector_map"] = {}

        # 1. Pobierz dane Makro (VXX i SPY)
        logger.info("[Backtest V3] Cache: Ładowanie VXX i SPY...")
        # Wymagamy pełnych danych do obliczenia reżimu (EMA 50/200)
        vix_raw = api_client.get_daily_adjusted('VXX', outputsize='full')
        vix_df = pd.DataFrame.from_dict(vix_raw['Time Series (Daily)'], orient='index')
        vix_df = standardize_df_columns(vix_df)
        vix_df.index = pd.to_datetime(vix_df.index)
        _backtest_cache["vix_data"] = vix_df
        
        spy_raw = api_client.get_daily_adjusted('SPY', outputsize='full')
        spy_df = pd.DataFrame.from_dict(spy_raw['Time Series (Daily)'], orient='index')
        spy_df = standardize_df_columns(spy_df)
        spy_df.index = pd.to_datetime(spy_df.index)
        spy_df['ema_50'] = calculate_ema(spy_df['close'], period=50)
        spy_df['ema_200'] = calculate_ema(spy_df['close'], period=200)
        _backtest_cache["spy_data"] = spy_df
        
        # 2. Pobierz dane Sektorowe (ETF-y)
        logger.info("[Backtest V3] Cache: Ładowanie 11 sektorów ETF...")
        for sector_name, etf_ticker in SECTOR_TO_ETF_MAP.items():
            try:
                sector_raw = api_client.get_daily_adjusted(etf_ticker, outputsize='full')
                sector_df = pd.DataFrame.from_dict(sector_raw['Time Series (Daily)'], orient='index')
                sector_df = standardize_df_columns(sector_df)
                sector_df.index = pd.to_datetime(sector_df.index)
                _backtest_cache["sector_etf_data"][etf_ticker] = sector_df
            except Exception as e:
                logger.error(f"  > BŁĄD ładowania danych dla sektora {etf_ticker}: {e}")
        
        # 3. Wstępne ładowanie mapy sektorów (dla spójności)
        # Używamy teraz listy z Fazy 1
        for ticker in initial_tickers_to_test:
            sector = _get_sector_for_ticker(session, ticker)
            if sector not in _backtest_cache["tickers_by_sector"]:
                _backtest_cache["tickers_by_sector"][sector] = []
            _backtest_cache["tickers_by_sector"][sector].append(ticker)


        logger.info("[Backtest V3] Budowanie pamięci podręcznej (Cache LITE) zakończone.")
        append_scan_log(session, "[Backtest V3] Budowanie pamięci podręcznej (Cache LITE) zakończone.")
        # Resetujemy postęp dla głównej pętli
        update_scan_progress(session, 0, len(initial_tickers_to_test)) 

    except Exception as e:
        log_msg = f"[Backtest V3] BŁĄD KRYTYCZNY podczas budowania cache LITE: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 4: Uruchomienie Symulacji (Ładowanie danych per-ticker) ===
    
    log_msg_aqm = "[Backtest V3] Uruchamianie Pętli Symulacyjnych H1, H2, H3 i H4 (Ładowanie Danych PER-TICKER)..."
    logger.info(log_msg_aqm)
    append_scan_log(session, log_msg_aqm)
    
    trades_found_h1 = 0
    trades_found_h2 = 0
    trades_found_h3 = 0
    trades_found_h4 = 0
    
    # UŻYWAMY LISTY Z FAZY 1
    total_tickers = len(initial_tickers_to_test)

    for i, ticker in enumerate(initial_tickers_to_test):
        if i % 10 == 0:
            log_msg = f"[Backtest V3][H1/H2/H3/H4] Ładowanie i przetwarzanie {ticker} ({i}/{total_tickers})..."
            append_scan_log(session, log_msg)
            update_scan_progress(session, i, total_tickers)
        
        ticker_data = None
        try:
            # KLUCZOWA ZMIANA: Dane ładowane są dla JEDNEGO tickera
            ticker_data = _load_all_data_for_ticker(ticker, api_client, session, year) # <-- Przekazujemy ROK
            
            if not ticker_data or 'daily' not in ticker_data:
                logger.warning(f"[Backtest V3] Brak danych po ładowaniu per-ticker dla {ticker}. Pomijanie.")
                continue
            
            full_historical_data = ticker_data['daily']
            
            # Weryfikacja zakresu dat (bez zmian)
            try:
                indexer = full_historical_data.index.get_indexer([start_date], method='bfill')
                if indexer[0] == -1: raise KeyError("Data startu nie znaleziona")
                start_index = indexer[0]
            except KeyError:
                logger.warning(f"[Backtest V3] Brak danych dla {ticker} w roku {year} lub przed nim. Pomijanie.")
                continue

            # ==================================================================
            # === POPRAWKA: Złagodzenie "widełek" (Problem 2) ===
            # Obniżamy próg z 301 do 101, aby pozwolić H1 i H2 działać
            # ==================================================================
            
            # Używamy 101 (zamiast 301) jako kompromis dla H1/H2
            if start_index < 101: # <-- ZMIANA: Obniżono próg z 301 na 101
                logger.warning(f"Za mało danych historycznych dla {ticker} przed {year} (znaleziono {start_index} świec, wymagane 101). Pomijanie.")
                continue

            # Wycinek danych na potrzeby backtestu (100 dni wstecz + testowany rok)
            historical_data_slice = full_historical_data.iloc[start_index - 101:].loc[:end_date] # <-- ZMIANA: z 301 na 101
            
            if historical_data_slice.empty or len(historical_data_slice) < 102: # <-- ZMIANA: z 302 na 102
                logger.warning(f"Pusty wycinek danych dla {ticker} w roku {year}. Pomijanie.")
                continue
            
            # ==================================================================
            # === KONIEC POPRAWKI WIDEŁEK ===
            # ==================================================================

            # === Uruchomienie Symulatorów H1-H4 (Logika bez zmian) ===
            
            trades_found_h1 += aqm_v3_h1_simulator._simulate_trades_h1(
                session, 
                ticker, 
                historical_data_slice, 
                year
            )
            
            h2_data_slice = {
                "daily": historical_data_slice,
                "insider_df": ticker_data.get("insider_df"),
                "news_df": ticker_data.get("news_df")
            }

            trades_found_h2 += aqm_v3_h2_simulator._simulate_trades_h2(
                session,
                ticker,
                h2_data_slice, 
                year
            )
            
            # Przygotuj dane dla H3/H4
            h3_data_slice = {
                "daily": historical_data_slice,
                "insider_df": ticker_data.get("insider_df"),
                "news_df": ticker_data.get("news_df"),
                "intraday_5min_df": ticker_data.get("intraday_5min_df")
            }
            
            # Symulatory H3/H4 mają wewnętrzne zabezpieczenia i pominą,
            # jeśli intraday_5min_df lub insider_df są puste (co jest teraz oczekiwane)
            
            trades_found_h3 += aqm_v3_h3_simulator._simulate_trades_h3(
                session,
                ticker,
                h3_data_slice, 
                year
            )

            trades_found_h4 += aqm_v3_h4_simulator._simulate_trades_h4(
                session,
                ticker,
                h3_data_slice, 
                year
            )

        except Exception as e:
            logger.error(f"[Backtest V3][H1/H2/H3/H4] Błąd krytyczny dla {ticker}: {e}", exc_info=True)
            session.rollback()
        finally:
            # Wymuszenie czyszczenia pamięci po każdym tickerze (optymalizacja RAM)
            if 'ticker_data' in locals():
                del ticker_data
            if 'full_historical_data' in locals():
                del full_historical_data
            if 'historical_data_slice' in locals():
                del historical_data_slice
            
            gc.collect() # Wymuszenie GC, aby agresywnie zwalniać pamięć
            
            
    trades_found_total = trades_found_h1 + trades_found_h2 + trades_found_h3 + trades_found_h4
    update_scan_progress(session, total_tickers, total_tickers) 
    
    log_msg_final = f"BACKTEST HISTORYCZNY (AQM V3/H1/H2/H3/H4): Zakończono test dla roku '{year}'. Znaleziono łącznie {trades_found_total} transakcji (H1: {trades_found_h1}, H2: {trades_found_h2}, H3: {trades_found_h3}, H4: {trades_found_h4})."
    logger.info(log_msg_final)
    append_scan_log(session, log_msg_final)
