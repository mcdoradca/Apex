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
# === REFAKTORYZACJA (WYDAJNOŚĆ): Ta funkcja jest teraz wywoływana na MAŁYM WYCINKU ===
# ==================================================================
def _pre_calculate_metrics(
    daily_df: pd.DataFrame, # To jest już "wycinek" (np. 450 dni)
    insider_df: pd.DataFrame, 
    news_df: pd.DataFrame, 
    bbands_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Wstępnie oblicza *wszystkie* złożone metryki (H2, H3, H4)
    i dodaje je jako kolumny do DataFrame'u.
    """
    ticker = daily_df['ticker'].iloc[0] if 'ticker' in daily_df.columns and not daily_df.empty else 'UNKNOWN'
    logger.info(f"[{ticker}] Rozpoczynanie wstępnego obliczania metryk (H2, H3, H4) dla {len(daily_df)} dni...")
    
    # Kopiujemy, aby uniknąć SettingWithCopyWarning
    df = daily_df.copy()
    
    # === Krok 1: Przygotowanie danych zewnętrznych (News, Insider) ===
    
    # Konwertujemy indeksy na "naiwne" (naive), aby dopasować je do 'df.index'
    if insider_df.index.tz is not None:
        insider_df = insider_df.tz_convert(None)
    if news_df.index.tz is not None:
        news_df = news_df.tz_convert(None)

    # --- Obliczenia H2 (Metryki zależne od daty) ---
    # Używamy .apply() - jest znacznie szybsze niż pętla w Pythonie
    
    # 1. Metryka 2.1: institutional_sync (ostatnie 90 dni)
    logger.info(f"[{ticker}] Obliczanie 'institutional_sync' (90d)...")
    # Używamy try-except, ponieważ .apply() może zawieść
    try:
        df['institutional_sync'] = df.apply(
            lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(
                insider_df, row.name # row.name to data (indeks)
            ), 
            axis=1
        )
    except Exception as e:
        logger.error(f"[{ticker}] BŁĄD wektorowy 'institutional_sync': {e}. Ustawiam 0.0")
        df['institutional_sync'] = 0.0


    # 2. Metryka 2.2: retail_herding (ostatnie 7 dni)
    logger.info(f"[{ticker}] Obliczanie 'retail_herding' (7d)...")
    try:
        df['retail_herding'] = df.apply(
            lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(
                news_df, row.name
            ), 
            axis=1
        )
    except Exception as e:
        logger.error(f"[{ticker}] BŁĄD wektorowy 'retail_herding': {e}. Ustawiam 0.0")
        df['retail_herding'] = 0.0

    # === Krok 2: Obliczenia H3/H4 (Metryki kroczące) ===
    
    # T = market_temperature (Zmienność dzienna z 30 dni)
    logger.info(f"[{ticker}] Obliczanie 'market_temperature' (T)...")
    df['daily_returns'] = df['close'].pct_change()
    df['market_temperature'] = df['daily_returns'].rolling(window=30).std()

    # ∇² (nabla_sq) = price_gravity (już obliczone i dodane w _load_all_data_for_ticker)
    df['nabla_sq'] = df['price_gravity']

    # --- Przygotowanie danych do S i m_sq ---
    
    # m_sq (attention_density - Wolumen)
    logger.info(f"[{ticker}] Obliczanie 'attention_density' (m_sq) - Wolumen...")
    df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
    df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
    df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
    
    # Z-Score dla Wolumenu
    df['normalized_volume'] = (df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']
    
    # ==================================================================
    # === NAPRAWA (FutureWarning: SettingWithCopyWarning) ===
    # Zastąpiono inplace=True bezpiecznym przypisaniem zwrotnym.
    # df['normalized_volume'].replace([np.inf, -np.inf], 0, inplace=True) # Obsługa dzielenia przez 0
    # df['normalized_volume'].fillna(0, inplace=True)
    df['normalized_volume'] = df['normalized_volume'].replace([np.inf, -np.inf], 0).fillna(0)
    # ==================================================================

    # m_sq (attention_density - Newsy) i S (information_entropy)
    logger.info(f"[{ticker}] Obliczanie 'attention_density' (m_sq) i 'info_entropy' (S) - Newsy...")
    if not news_df.empty:
        # a) Zlicz newsy dziennie (na danych naiwnych)
        news_counts_daily = news_df.groupby(news_df.index.date).size()
        news_counts_daily.index = pd.to_datetime(news_counts_daily.index)
        
        # b) Uzupełnij brakujące dni (weekendy) zerami
        # Używamy indeksu 'df' jako szablonu
        news_counts_daily = news_counts_daily.reindex(df.index, fill_value=0)

        # S = information_entropy (Proxy: Liczba newsów z ostatnich 10 dni)
        df['information_entropy'] = news_counts_daily.rolling(window=10).sum()
        
        # m_sq (część newsowa)
        df['news_mean_200d'] = df['information_entropy'].rolling(window=200).mean()
        df['news_std_200d'] = df['information_entropy'].rolling(window=200).std()

        df['normalized_news'] = (df['information_entropy'] - df['news_mean_200d']) / df['news_std_200d']
        
        # ==================================================================
        # === NAPRAWA (FutureWarning: SettingWithCopyWarning) ===
        # Zastąpiono inplace=True bezpiecznym przypisaniem zwrotnym.
        # df['normalized_news'].replace([np.inf, -np.inf], 0, inplace=True) # Obsługa dzielenia przez 0
        # df['normalized_news'].fillna(0, inplace=True)
        df['normalized_news'] = df['normalized_news'].replace([np.inf, -np.inf], 0).fillna(0)
        # ==================================================================
    else:
        df['information_entropy'] = 0.0
        df['normalized_news'] = 0.0
        
    # Finalizacja m_sq (attention_density)
    df['m_sq'] = df['normalized_volume'] + df['normalized_news']

    # === Krok 3: Obliczenie J (entropy_change) ===
    # J = S - (Q / T) + (μ * ΔN)
    logger.info(f"[{ticker}] Obliczanie 'entropy_change' (J)...")
    
    # Używamy już obliczonych kolumn
    S = df['information_entropy']
    Q = df['retail_herding']
    T = df['market_temperature']
    mu = df['institutional_sync']
    delta_N = 1.0 # Stała

    # Oblicz J (wektorowo)
    # Zabezpieczenie: T.replace(0, np.nan) aby uniknąć dzielenia przez zero
    J = S - (Q / T.replace(0, np.nan)) + (mu * delta_N)
    
    # Wypełnij NaN (które powstały z dzielenia przez zero)
    # Jeśli T było 0, Q/T = NaN, J = NaN. Wypełniamy S + (mu*dN)
    
    # ==================================================================
    # === NAPRAWA (FutureWarning: SettingWithCopyWarning) ===
    # Zastąpiono inplace=True bezpiecznym przypisaniem zwrotnym.
    # J.fillna(S + (mu * delta_N), inplace=True)
    J = J.fillna(S + (mu * delta_N))
    # ==================================================================
    
    df['J'] = J

    logger.info(f"[{ticker}] Wstępne obliczanie metryk zakończone.")
    
    # Czyszczenie kolumn pomocniczych
    cols_to_drop = [
        'daily_returns', 'avg_volume_10d', 'vol_mean_200d', 'vol_std_200d',
        'normalized_volume', 'information_entropy', 'news_mean_200d', 
        'news_std_200d', 'normalized_news'
    ]
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    
    return df
# ==================================================================


# ==================================================================
# === REFAKTORYZACJA (WYDAJNOŚĆ): Ta funkcja już NIE OBLICZA metryk ===
# ==================================================================
def _load_all_data_for_ticker(ticker: str, api_client: AlphaVantageClient, session: Session, year_to_test: str) -> Optional[Dict[str, Any]]:
    """
    Pobiera i wstępnie przetwarza wszystkie dane (H1, H2, H3, H4) dla pojedynczego tickera,
    korzystając z mechanizmu cache w bazie danych.
    
    TA FUNKCJA JUŻ NIE WYWOŁUJE `_pre_calculate_metrics`.
    """
    try:
        # --- KROK 1: ŁADOWANIE H1 (Daily Time Series) ---
        
        # TIME_SERIES_DAILY (OHLCV - do HLC/3 Proxy)
        price_data_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='DAILY_OHLCV',
            api_func='get_time_series_daily', outputsize='full'
        )
        # TIME_SERIES_DAILY_ADJUSTED (Dla time_dilation)
        daily_adjusted_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='DAILY_ADJUSTED',
            api_func='get_daily_adjusted', outputsize='full'
        )
        # TIME_SERIES_WEEKLY_ADJUSTED
        weekly_data_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='WEEKLY',
            api_func='get_time_series_weekly', outputsize='full'
        )
        
        # --- KROK 2: ŁADOWANIE BBANDS (H3) ---
        bbands_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='BBANDS',
            api_func='get_bollinger_bands', interval='daily', time_period=20, nbdevup=2, nbdevdn=2
        )
        
        # --- KROK 3: DANE INTRADAY (USUWANIE) ---
        intraday_5min_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume']).set_index(pd.to_datetime([]))
        
        # --- KROK 4: ŁADOWANIE H2 (Insider i News) ---
        h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
        
        # --- KROK 5: Walidacja i Przetwarzanie ---
        
        if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
           not daily_adjusted_raw or 'Time Series (Daily)' not in daily_adjusted_raw or \
           not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw:
            
            logger.warning(f"[Backtest V3] Brak podstawowych danych (Daily/Weekly) z cache/API dla {ticker}, pomijanie.")
            return None
            
        # Przetwórz Daily OHLCV (dla HLC/3 proxy)
        daily_ohlcv_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
        daily_ohlcv_df.index = pd.to_datetime(daily_ohlcv_df.index) 
        daily_ohlcv_df = standardize_df_columns(daily_ohlcv_df)
        
        # Przetwórz Daily Adjusted (dla time_dilation i ATR)
        daily_adjusted_df = pd.DataFrame.from_dict(daily_adjusted_raw['Time Series (Daily)'], orient='index')
        daily_adjusted_df.index = pd.to_datetime(daily_adjusted_df.index)
        daily_adjusted_df = standardize_df_columns(daily_adjusted_df)

        # Przetwórz Weekly (H1)
        weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
        weekly_df.index = pd.to_datetime(weekly_df.index)
        weekly_df = standardize_df_columns(weekly_df)
        
        # === KLUCZOWA POPRAWKA (VWAP PROXY HLC/3) ===
        daily_ohlcv_df['vwap_proxy'] = (daily_ohlcv_df['high'] + daily_ohlcv_df['low'] + daily_ohlcv_df['close']) / 3.0
        
        # Używamy DF Adjusted jako bazy i dołączamy do niego OHLCV i VWAP Proxy
        enriched_df = daily_adjusted_df.join(daily_ohlcv_df[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
        
        enriched_df['vwap'] = enriched_df['vwap_proxy']
        
        if enriched_df['vwap'].isnull().all():
             logger.warning(f"Brak danych HLC/3 VWAP Proxy dla {ticker}.")
        
        # === KRYTYCZNA POPRAWKA: TWORZENIE KOLUMNY 'price_gravity' ===
        close_col = 'close_ohlcv' if 'close_ohlcv' in enriched_df.columns else 'close'
        enriched_df['price_gravity'] = (enriched_df['vwap'] - enriched_df[close_col]) / enriched_df[close_col]
        # ================================================================

        # Wzbogacanie DataFrame (z użyciem danych SPY z Cache LITE)
        spy_aligned = _backtest_cache["spy_data"]['close'].reindex(enriched_df.index, method='ffill').rename('spy_close')
        enriched_df = enriched_df.join(spy_aligned) 
        
        # Obliczamy ATR (na nie-adjustowanych danych HLC, które są w enriched_df)
        enriched_df['atr_14'] = calculate_atr(enriched_df, period=14)
        
        # Obliczenia H1 (time_dilation)
        ticker_returns_rolling = enriched_df['close'].pct_change().rolling(window=20)
        spy_returns_rolling = enriched_df['spy_close'].pct_change().rolling(window=20)
        std_ticker = ticker_returns_rolling.std()
        std_spy = spy_returns_rolling.std()
        enriched_df['time_dilation'] = std_ticker / std_spy
        
        # === Dodanie tickera do DF (dla logowania w _pre_calculate_metrics) ===
        enriched_df['ticker'] = ticker
        
        enriched_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # Wypełnij NaN wartościami 0 (lub metodą ffill), aby symulatory nie napotkały NaN
        enriched_df['time_dilation'] = enriched_df['time_dilation'].fillna(0)
        enriched_df['price_gravity'] = enriched_df['price_gravity'].fillna(0) # Zapewnienie, że jest kolumna
        
        # ==================================================================
        # === NAPRAWA (FutureWarning: .fillna(method='ffill')) ===
        # Zastąpiono przestarzałe .fillna(method='ffill') nowym .ffill()
        # enriched_df['atr_14'] = enriched_df['atr_14'].fillna(method='ffill').fillna(0)
        # enriched_df['vwap'] = enriched_df['vwap'].fillna(method='ffill').fillna(0)
        enriched_df['atr_14'] = enriched_df['atr_14'].ffill().fillna(0)
        enriched_df['vwap'] = enriched_df['vwap'].ffill().fillna(0)
        # ==================================================================

        
        # Przetwarzanie BBANDS (H3)
        bbands_df = _parse_bbands(bbands_raw)
        
        # DANE INTRADAY (USUWANIE)
        intraday_5min_df = pd.DataFrame() 
        
        # Zdobądź i zapisz sektor (dla spójności)
        sector = _get_sector_for_ticker(session, ticker)
        
        # ==================================================================
        # === REFAKTORYZACJA (WYDAJNOŚĆ): Usunięto wywołanie _pre_calculate_metrics ===
        # Ta funkcja NIE OBLICZA już metryk H2/H3/H4. Robi to pętla główna.
        # ==================================================================
        
        # Zwracamy wszystkie *surowe* przetworzone dane
        return {
            "daily_raw": enriched_df, # Zwracamy surowy, pełny DF (5066+ dni)
            "weekly": weekly_df,
            "insider_df": h2_data["insider_df"], 
            "news_df": h2_data["news_df"],       
            "bbands_df": bbands_df,             
            "sector": sector
        }
    
    except Exception as e:
        logger.error(f"[Backtest V3] Błąd ładowania danych dla {ticker}: {e}", exc_info=True)
        return None
# ==================================================================


# ==================================================================
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (NOWA LOGIKA "SLICE-FIRST") ===
# ==================================================================
def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny.
    REFAKTORYZACJA: Logika "Slice First" zaimplementowana w pętli głównej.
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

    # === KROK 3: Budowanie Cache (TYLKO DANE GLOBALNE) (Bez zmian) ===
    
    try:
        logger.info("[Backtest V3] Rozpoczynanie budowania pamięci podręcznej (Cache LITE)...")
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}
        _backtest_cache["sector_map"] = {}

        # 1. Pobierz dane Makro (VXX i SPY)
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
        for ticker in initial_tickers_to_test:
            sector = _get_sector_for_ticker(session, ticker)
            if sector not in _backtest_cache["tickers_by_sector"]:
                _backtest_cache["tickers_by_sector"][sector] = []
            _backtest_cache["tickers_by_sector"][sector].append(ticker)


        logger.info("[Backtest V3] Budowanie pamięci podręcznej (Cache LITE) zakończone.")
        append_scan_log(session, "[Backtest V3] Budowanie pamięci podręcznej (Cache LITE) zakończone.")
        update_scan_progress(session, 0, len(initial_tickers_to_test)) 

    except Exception as e:
        log_msg = f"[Backtest V3] BŁĄD KRYTYCZNY podczas budowania cache LITE: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 4: Uruchomienie Symulacji (NOWA LOGIKA "SLICE-FIRST") ===
    
    log_msg_aqm = "[Backtest V3] Uruchamianie Pętli Symulacyjnych H1, H2, H3 i H4 (Logika: Slice-First)..."
    logger.info(log_msg_aqm)
    append_scan_log(session, log_msg_aqm)
    
    trades_found_h1 = 0
    trades_found_h2 = 0
    trades_found_h3 = 0
    trades_found_h4 = 0
    
    total_tickers = len(initial_tickers_to_test)

    for i, ticker in enumerate(initial_tickers_to_test):
        if i % 1 == 0: # Loguj każdy ticker, aby widzieć postęp
            log_msg = f"[Backtest V3] Przetwarzanie {ticker} ({i+1}/{total_tickers})..."
            logger.info(log_msg)
            # Loguj do bazy co 20 tickerów
            if i % 20 == 0: 
                 append_scan_log(session, log_msg)
            update_scan_progress(session, i, total_tickers)
        
        ticker_data_raw_dict = None # Zmieniona nazwa, aby uniknąć pomyłek
        try:
            # ==================================================================
            # === NOWA LOGIKA (KROK 4.1): Ładowanie surowych danych ===
            # Ta funkcja już NIE oblicza metryk. Zwraca surowe DFy.
            # ==================================================================
            ticker_data_raw_dict = _load_all_data_for_ticker(ticker, api_client, session, year)
            
            if not ticker_data_raw_dict or 'daily_raw' not in ticker_data_raw_dict:
                logger.warning(f"[Backtest V3] Brak surowych danych dla {ticker}. Pomijanie.")
                continue
            
            # Pobieramy pełny, surowy DF (5066+ dni)
            full_historical_data_raw = ticker_data_raw_dict['daily_raw']
            
            # ==================================================================
            # === NOWA LOGIKA (KROK 4.2): "SLICE FIRST" (NAPRAWA BŁĘDU "5066 DNI") ===
            # Znajdź indeks startowy i wycinamy *zanim* cokolwiek obliczymy
            # ==================================================================
            try:
                # Znajdź pierwszy dzień handlowy ROKU
                indexer = full_historical_data_raw.index.get_indexer([start_date], method='bfill')
                if indexer[0] == -1: raise KeyError("Data startu nie znaleziona")
                start_index = indexer[0]
            except KeyError:
                logger.warning(f"[Backtest V3] Brak danych dla {ticker} w roku {year} lub przed nim. Pomijanie.")
                continue

            # Wymagamy 201 dni historii PRZED startem roku (dla Z-Score i EMA)
            history_buffer = 201 
            if start_index < history_buffer: 
                logger.warning(f"Za mało danych historycznych dla {ticker} przed {year} (znaleziono {start_index} świec, wymagane {history_buffer}). Pomijanie.")
                continue

            # Wycinamy DOKŁADNY zakres: (Rok Testowy + Bufor Historii)
            # To jest teraz mały DataFrame (np. ~450 wierszy), a nie 5066+
            data_slice_for_processing = full_historical_data_raw.iloc[start_index - history_buffer:].loc[:end_date] 
            
            if data_slice_for_processing.empty or len(data_slice_for_processing) < history_buffer + 1:
                logger.warning(f"Pusty wycinek danych dla {ticker} w roku {year}. Pomijanie.")
                continue
            
            # ==================================================================
            # === NOWA LOGIKA (KROK 4.3): PRE-CALCULATE (na małym wycinku) ===
            # ==================================================================
            logger.info(f"[{ticker}] Wycinek ({len(data_slice_for_processing)} dni) gotowy. Rozpoczynanie obliczeń metryk...")
            
            # Ta funkcja jest teraz BARDZO SZYBKA, bo działa na ~450 wierszach
            enriched_slice = _pre_calculate_metrics(
                daily_df=data_slice_for_processing,
                insider_df=ticker_data_raw_dict["insider_df"],
                news_df=ticker_data_raw_dict["news_df"],
                bbands_df=ticker_data_raw_dict["bbands_df"]
            )
            
            # ==================================================================
            # === NOWA LOGIKA (KROK 4.4): Uruchomienie Symulatorów ===
            # ==================================================================
            
            # Symulator H1 (potrzebuje tylko 'daily' DF)
            trades_found_h1 += aqm_v3_h1_simulator._simulate_trades_h1(
                session, 
                ticker, 
                enriched_slice, # Przekazujemy wstępnie obliczony WYCINEK
                year
            )
            
            # Słownik dla symulatorów H2, H3, H4
            h_data_slice_dict = {
                "daily": enriched_slice, # Wzbogacony wycinek
                "insider_df": ticker_data_raw_dict.get("insider_df"),
                "news_df": ticker_data_raw_dict.get("news_df"),
                "bbands_df": ticker_data_raw_dict.get("bbands_df")
            }

            # Symulator H2
            trades_found_h2 += aqm_v3_h2_simulator._simulate_trades_h2(
                session,
                ticker,
                h_data_slice_dict, 
                year
            )
            
            # Symulator H3
            trades_found_h3 += aqm_v3_h3_simulator._simulate_trades_h3(
                session,
                ticker,
                h_data_slice_dict, 
                year
            )

            # Symulator H4
            trades_found_h4 += aqm_v3_h4_simulator._simulate_trades_h4(
                session,
                ticker,
                h_data_slice_dict, 
                year
            )

        except Exception as e:
            logger.error(f"[Backtest V3][GŁÓWNA PĘTLA] Błąd krytyczny dla {ticker}: {e}", exc_info=True)
            session.rollback()
        finally:
            # Wymuszenie czyszczenia pamięci po każdym tickerze (optymalizacja RAM)
            if 'ticker_data_raw_dict' in locals():
                del ticker_data_raw_dict
            if 'full_historical_data_raw' in locals():
                del full_historical_data_raw
            if 'data_slice_for_processing' in locals():
                del data_slice_for_processing
            if 'enriched_slice' in locals():
                del enriched_slice
            if 'h_data_slice_dict' in locals():
                del h_data_slice_dict
            
            gc.collect() # Wymuszenie GC, aby agresywnie zwalniać pamięć
            
            
    trades_found_total = trades_found_h1 + trades_found_h2 + trades_found_h3 + trades_found_h4
    update_scan_progress(session, total_tickers, total_tickers) 
    
    log_msg_final = f"BACKTEST HISTORYCZNY (AQM V3/H1/H2/H3/H4): Zakończono test dla roku '{year}'. Znaleziono łącznie {trades_found_total} transakcji (H1: {trades_found_h1}, H2: {trades_found_h2}, H3: {trades_found_h3}, H4: {trades_found_h4})."
    logger.info(log_msg_final)
    append_scan_log(session, log_msg_final)
