import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

# Krok 18: Importujemy kalkulatory metryk z Krok 17 i 15
from . import aqm_v3_metrics
# Krok 19b: Importujemy nowy silnik symulacji H1
from . import aqm_v3_h1_simulator 
# Krok 20b (Część 2): Importujemy nowy silnik ładowania H2
from . import aqm_v3_h2_loader
# Krok 21c: Importujemy nowy silnik symulacji H2
from . import aqm_v3_h2_simulator
# Krok 22b (Część 2): Importujemy nowy silnik ładowania H3
from . import aqm_v3_h3_loader
# ==================================================================
# === INTEGRACJA H3 (KROK 1): Import nowego symulatora H3 ===
# ==================================================================
from . import aqm_v3_h3_simulator
# ==================================================================
# === INTEGRACJA H4 (KROK 1): Import nowego symulatora H4 ===
# ==================================================================
from . import aqm_v3_h4_simulator
# ==================================================================
from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    get_current_NY_datetime,
    append_scan_log,
    update_scan_progress,
    # Importujemy calculate_atr, aby móc go użyć w pętli cache
    calculate_atr
)
from .. import models
from ..config import SECTOR_TO_ETF_MAP

logger = logging.getLogger(__name__)

# ==================================================================
# === SŁOWNIK CACHE ===
# ==================================================================

_backtest_cache = {
    "vix_data": None, 
    "spy_data": None, 
    "sector_etf_data": {}, 
    "company_data": {}, 
    "tickers_by_sector": {}, 
    "sector_map": {} 
}

def _get_ticker_data_from_cache(ticker: str) -> Optional[Dict[str, Any]]:
    """Pobiera dane spółki (dzienne, tygodniowe, vwap, sektor) z cache."""
    return _backtest_cache["company_data"].get(ticker)

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
# === DETEKTOR REŻIMU RYNKOWEGO ===
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
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (ZAKTUALIZOWANA) ===
# ==================================================================

def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny dla
    zdefiniowanego okresu i listy tickerów.
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

    # === KROK 1: Czyszczenie Bazy Danych ===
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

    # === KROK 2: Pobieranie Listy Spółek ===
    try:
        log_msg_tickers = "[Backtest V3] Pobieranie listy spółek z Fazy 1 ('Pierwsze Sito')..."
        logger.info(log_msg_tickers)
        append_scan_log(session, log_msg_tickers)
        
        tickers_p2_rows = session.execute(text(
            "SELECT DISTINCT ticker FROM phase1_candidates"
        )).fetchall()
        
        tickers_to_test = sorted([row[0] for row in tickers_p2_rows])

        if not tickers_to_test:
            log_msg = f"[Backtest] BŁĄD: Tabela 'phase1_candidates' jest pusta. Uruchom najpierw główny skan (przycisk 'Start'), aby zapełnić tę listę."
            logger.error(log_msg)
            append_scan_log(session, log_msg)
            return

        log_msg = f"[Backtest V3] Znaleziono {len(tickers_to_test)} spółek z Fazy 1 do przetestowania."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów: {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 3: Budowanie Pamięci Podręcznej (Cache) ===
    
    try:
        logger.info("[Backtest V3] Rozpoczynanie budowania pamięci podręcznej (Cache)...")
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["company_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}
        _backtest_cache["sector_map"] = {}

        # 2. Pobierz dane Makro (VXX i SPY)
        logger.info("[Backtest V3] Cache: Ładowanie VXX i SPY...")
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
        
        # 3. Pobierz dane Sektorowe (ETF-y)
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
        
        # 4. Pobierz dane dla wszystkich spółek
        logger.info(f"[Backtest V3] Cache: Ładowanie danych i wstępne obliczanie metryk V3...")
        total_cache_build = len(tickers_to_test)
        for i, ticker in enumerate(tickers_to_test):
            if i % 10 == 0:
                log_msg = f"[Backtest V3] Budowanie cache... ({i}/{total_cache_build})"
                append_scan_log(session, log_msg)
                update_scan_progress(session, i, total_cache_build)
            
            try:
                # ==================================================================
                # === KROK 18, 20b, 22b: Ładowanie danych H1, H2 i H3 ===
                # ==================================================================
                
                # --- ŁADOWANIE H1 ---
                price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
                weekly_data_raw = api_client.get_time_series_weekly(ticker)
                vwap_data_raw = api_client.get_vwap(ticker, interval='daily')
                
                # --- ŁADOWANIE H3 (CZĘŚĆ 1) ---
                bbands_raw = api_client.get_bollinger_bands(ticker, interval='daily', time_period=20, nbdevup=2, nbdevdn=2)
                
                # --- ŁADOWANIE H3 (CZĘŚĆ 2) ---
                intraday_raw = api_client.get_intraday(ticker, interval='5min', outputsize='full')
                
                # Walidacja danych H1 i H3
                if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
                   not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw or \
                   not vwap_data_raw or 'Technical Analysis: VWAP' not in vwap_data_raw or \
                   not bbands_raw or 'Technical Analysis: BBANDS' not in bbands_raw or \
                   not intraday_raw or 'Time Series (5min)' not in intraday_raw:
                    logger.warning(f"Brak pełnych danych (H1/H3) dla {ticker}, pomijanie.")
                    continue

                # Przetwórz Daily (H1)
                daily_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
                daily_df = standardize_df_columns(daily_df)
                daily_df.index = pd.to_datetime(daily_df.index)
                
                # Przetwórz Weekly (H1)
                weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
                weekly_df = standardize_df_columns(weekly_df)
                weekly_df.index = pd.to_datetime(weekly_df.index)

                # Przetwórz VWAP (H1)
                vwap_df = pd.DataFrame.from_dict(vwap_data_raw['Technical Analysis: VWAP'], orient='index')
                vwap_df.index = pd.to_datetime(vwap_df.index)
                vwap_df['VWAP'] = pd.to_numeric(vwap_df['VWAP'], errors='coerce')
                vwap_df.sort_index(inplace=True)
                
                # Przetwórz BBANDS (H3)
                bbands_df = aqm_v3_h3_loader._parse_bbands(bbands_raw)
                if bbands_df is None: bbands_df = pd.DataFrame() # Utwórz pusty, jeśli błąd parsowania
                
                # Przetwórz Intraday 5min (H3)
                intraday_5min_df = aqm_v3_h3_loader._parse_intraday_5min(intraday_raw)
                if intraday_5min_df is None: intraday_5min_df = pd.DataFrame() # Utwórz pusty, jeśli błąd parsowania
                
                # --- Wzbogacanie DataFrame (Krok 18 - H1) ---
                spy_aligned = _backtest_cache["spy_data"]['close'].reindex(daily_df.index, method='ffill').rename('spy_close')
                vwap_aligned = vwap_df['VWAP'].reindex(daily_df.index, method='ffill').rename('vwap')
                
                enriched_df = daily_df.join(spy_aligned).join(vwap_aligned)
                enriched_df['atr_14'] = calculate_atr(enriched_df, period=14)
                
                ticker_returns_rolling = enriched_df['close'].pct_change().rolling(window=20)
                spy_returns_rolling = enriched_df['spy_close'].pct_change().rolling(window=20)
                std_ticker = ticker_returns_rolling.std()
                std_spy = spy_returns_rolling.std()
                enriched_df['time_dilation'] = std_ticker / std_spy
                enriched_df['price_gravity'] = (enriched_df['vwap'] - enriched_df['close']) / enriched_df['close']
                
                enriched_df.replace([np.inf, -np.inf], np.nan, inplace=True)
                enriched_df['time_dilation'].fillna(0, inplace=True)
                enriched_df['price_gravity'].fillna(0, inplace=True)
                
                # --- ŁADOWANIE H2 ---
                h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client)
                
                # Zdobądź i zapisz sektor
                sector = _get_sector_for_ticker(session, ticker)
                
                # Zapisz wszystko w cache
                _backtest_cache["company_data"][ticker] = {
                    "daily": enriched_df, # Wzbogacony o H1
                    "weekly": weekly_df,
                    "vwap": vwap_df, 
                    "insider_df": h2_data["insider_df"], # Dane H2
                    "news_df": h2_data["news_df"],       # Dane H2
                    "bbands_df": bbands_df,             # <-- NOWE DANE H3
                    "intraday_5min_df": intraday_5min_df, # <-- NOWE DANE H3
                    "sector": sector
                }
                
                if sector not in _backtest_cache["tickers_by_sector"]:
                    _backtest_cache["tickers_by_sector"][sector] = []
                _backtest_cache["tickers_by_sector"][sector].append(ticker)

            except Exception as e:
                logger.error(f"[Backtest V3] Błąd budowania cache dla {ticker}: {e}", exc_info=True)
                
        logger.info("[Backtest V3] Budowanie pamięci podręcznej (Cache) zakończone.")
        append_scan_log(session, "[Backtest V3] Budowanie pamięci podręcznej (Cache) zakończone.")
        update_scan_progress(session, total_cache_build, total_cache_build)

    except Exception as e:
        log_msg = f"[Backtest V3] BŁĄD KRYTYCZNY podczas budowania cache: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # ==================================================================
    # === KROK 4: Uruchomienie Symulacji (Hipoteza H1, H2, H3 i H4) ===
    # ==================================================================
    
    # INTEGRACJA H4 (KROK 2): Aktualizacja logów
    log_msg_aqm = "[Backtest V3] Uruchamianie Pętli Symulacyjnych H1, H2, H3 i H4..."
    logger.info(log_msg_aqm)
    append_scan_log(session, log_msg_aqm)
    
    trades_found_h1 = 0
    trades_found_h2 = 0
    trades_found_h3 = 0
    # INTEGRACJA H4 (KROK 3): Dodanie licznika dla H4
    trades_found_h4 = 0
    total_tickers = len(tickers_to_test)

    for i, ticker in enumerate(tickers_to_test):
        if i % 10 == 0:
            # INTEGRACJA H4 (KROK 4): Aktualizacja logów
            log_msg = f"[Backtest V3][H1/H2/H3/H4] Przetwarzanie {ticker} ({i}/{total_tickers})..."
            append_scan_log(session, log_msg)
            update_scan_progress(session, i, total_tickers)
        
        try:
            # 1. Pobierz wstępnie obliczone dane z cache
            ticker_data = _get_ticker_data_from_cache(ticker)
            if not ticker_data or 'daily' not in ticker_data:
                logger.warning(f"[Backtest V3] Brak danych w cache dla {ticker}. Pomijanie.")
                continue
            
            # 2. Wytnij plaster danych na dany rok (z buforem 100+1 dni dla H1)
            full_historical_data = ticker_data['daily']
            
            try:
                # Znajdź indeks pierwszej daty, która jest >= start_date
                indexer = full_historical_data.index.get_indexer([start_date], method='bfill')
                if indexer[0] == -1: raise KeyError("Data startu nie znaleziona")
                start_index = indexer[0]
            except KeyError:
                logger.warning(f"[Backtest V3] Brak danych dla {ticker} w roku {year} lub przed nim. Pomijanie.")
                continue

            # Wymagamy 100 dni historii (dla percentyla H1) PRZED startem roku
            # ZMIANA: Wymagamy 200 dni (dla H3) + 100 dni (dla H4) = 300 dni
            # (Bazując na logice H3 i H4, które wymagają `history_window + percentile_window`)
            # Użyjemy 301 (200+100+1) jako bezpiecznego minimum
            if start_index < 301:
                logger.warning(f"Za mało danych historycznych dla {ticker} przed {year} (znaleziono {start_index} świec, wymagane 301). Pomijanie.")
                continue

            # Kroimy dane: od (start_date - 301 świec) do end_date
            # Bufor jest potrzebny dla okien kroczących H3 i H4
            historical_data_slice = full_historical_data.iloc[start_index - 301:].loc[:end_date]
            
            if historical_data_slice.empty or len(historical_data_slice) < 302:
                logger.warning(f"Pusty wycinek danych dla {ticker} w roku {year}. Pomijanie.")
                continue

            # 3. Wywołaj symulator H1
            trades_found_h1 += aqm_v3_h1_simulator._simulate_trades_h1(
                session, 
                ticker, 
                historical_data_slice, # H1 potrzebuje tylko 'daily' (wzbogaconego)
                year
            )
            
            # ==================================================================
            # === KROK 21c: Aktywacja Pętli Symulacyjnej H2 ===
            # ==================================================================
            
            h2_data_slice = {
                "daily": historical_data_slice,
                "insider_df": ticker_data.get("insider_df"),
                "news_df": ticker_data.get("news_df")
            }

            trades_found_h2 += aqm_v3_h2_simulator._simulate_trades_h2(
                session,
                ticker,
                h2_data_slice, # Przekazujemy słownik z pociętymi danymi
                year
            )
            # ==================================================================
            
            # ==================================================================
            # === INTEGRACJA H3 (KROK 3): Aktywacja Pętli Symulacyjnej H3 ===
            # ==================================================================
            h3_data_slice = {
                "daily": historical_data_slice,
                "insider_df": ticker_data.get("insider_df"),
                "news_df": ticker_data.get("news_df"),
                "intraday_5min_df": ticker_data.get("intraday_5min_df")
            }
            
            trades_found_h3 += aqm_v3_h3_simulator._simulate_trades_h3(
                session,
                ticker,
                h3_data_slice, # Przekazujemy słownik z danymi H3
                year
            )
            # ==================================================================

            # ==================================================================
            # === INTEGRACJA H4 (KROK 5): Aktywacja Pętli Symulacyjnej H4 ===
            # ==================================================================
            # H4 używa tego samego zestawu danych co H3 (daily, insider, news, intraday)
            trades_found_h4 += aqm_v3_h4_simulator._simulate_trades_h4(
                session,
                ticker,
                h3_data_slice, # Ponownie używamy h3_data_slice
                year
            )
            # ==================================================================
            
        except Exception as e:
            # INTEGRACJA H4 (KROK 6): Aktualizacja logu błędu
            logger.error(f"[Backtest V3][H1/H2/H3/H4] Błąd krytyczny dla {ticker}: {e}", exc_info=True)
            session.rollback()
            
    # === POPRAWKA BŁĘDU (SyntaxError) ===
    # Usunięto błędny, nadmiarowy nawias klamrowy '}' z tego miejsca (była linia 427)
    # === KONIEC POPRAWKI ===
            
    # === KONIEC SYMULACJI ===
    # INTEGRACJA H4 (KROK 7): Aktualizacja sumy końcowej
    trades_found_total = trades_found_h1 + trades_found_h2 + trades_found_h3 + trades_found_h4
    update_scan_progress(session, total_tickers, total_tickers) # Ustaw na 100%
    
    # INTEGRACJA H4 (KROK 8): Aktualizacja logu końcowego
    log_msg_final = f"BACKTEST HISTORYCZNY (AQM V3/H1/H2/H3/H4): Zakończono test dla roku '{year}'. Znaleziono łącznie {trades_found_total} transakcji (H1: {trades_found_h1}, H2: {trades_found_h2}, H3: {trades_found_h3}, H4: {trades_found_h4})."
    logger.info(log_msg_final)
    append_scan_log(session, log_msg_final)

}
