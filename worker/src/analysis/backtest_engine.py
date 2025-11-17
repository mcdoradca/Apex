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
# === NOWE IMPORTY (AQM V2) ===
from . import aqm_v2_simulator
# Importujemy parsery z nowego pliku V2
from .aqm_v2_simulator import _parse_indicator_data, _parse_macd_data
# ============================
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
# === PAMIĘĆ PODRĘCZNA (CACHE) LITE ===
# ==================================================================
_backtest_cache = {
    # Dane V3
    "vix_data": None, 
    "spy_data": None, 
    "sector_etf_data": {}, 
    "tickers_by_sector": {}, 
    "sector_map": {},
    # Dane V2
    "ras_v2_data": None
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
# === DETEKTOR REŻIMU RYNKOWEGO (DLA AQM V3) ===
# ==================================================================
def _detect_market_regime(current_date_str: str) -> str:
    """
    Wykrywa reżim rynkowy na podstawie danych VXX i SPY z cache.
    (Używane tylko przez logikę AQM V3)
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
# === PARSER DANYCH EARNINGS (DLA AQM V3 i V2) ===
# ==================================================================
def _parse_earnings_data(raw_data: Dict[str, Any]) -> pd.DataFrame:
    """Przetwarza surową odpowiedź JSON z EARNINGS na DataFrame z datami raportów."""
    try:
        quarterly_earnings = raw_data.get('quarterlyEarnings', [])
        if not quarterly_earnings:
            return pd.DataFrame(columns=['reportDate']).set_index(pd.to_datetime([]))

        dates = []
        for report in quarterly_earnings:
            date_str = report.get('reportedDate')
            if date_str:
                try:
                    dates.append(pd.to_datetime(date_str))
                except (ValueError, TypeError):
                    continue
        
        if not dates:
            return pd.DataFrame(columns=['reportDate']).set_index(pd.to_datetime([]))

        df = pd.DataFrame(dates, columns=['reportDate'])
        df.set_index('reportDate', inplace=True)
        df.sort_index(inplace=True)
        # Usuwamy zduplikowane daty raportów, jeśli istnieją
        df = df[~df.index.duplicated(keep='first')]
        return df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych EARNINGS: {e}", exc_info=True)
        return pd.DataFrame(columns=['reportDate']).set_index(pd.to_datetime([]))
# ==================================================================


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
    (Logika AQM V3)
    Wstępnie oblicza *wszystkie* złożone metryki (H2, H3, H4)
    i dodaje je jako kolumny do DataFrame'u.
    """
    ticker = daily_df['ticker'].iloc[0] if 'ticker' in daily_df.columns and not daily_df.empty else 'UNKNOWN'
    logger.info(f"[{ticker}] (V3) Rozpoczynanie wstępnego obliczania metryk (H2, H3, H4) dla {len(daily_df)} dni...")
    
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
    logger.info(f"[{ticker}] (V3) Obliczanie 'institutional_sync' (90d)...")
    # Używamy try-except, ponieważ .apply() może zawieść
    try:
        df['institutional_sync'] = df.apply(
            lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(
                insider_df, row.name # row.name to data (indeks)
            ), 
            axis=1
        )
    except Exception as e:
        logger.error(f"[{ticker}] (V3) BŁĄD wektorowy 'institutional_sync': {e}. Ustawiam 0.0")
        df['institutional_sync'] = 0.0


    # 2. Metryka 2.2: retail_herding (ostatnie 7 dni)
    logger.info(f"[{ticker}] (V3) Obliczanie 'retail_herding' (7d)...")
    try:
        df['retail_herding'] = df.apply(
            lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(
                news_df, row.name
            ), 
            axis=1
        )
    except Exception as e:
        logger.error(f"[{ticker}] (V3) BŁĄD wektorowy 'retail_herding': {e}. Ustawiam 0.0")
        df['retail_herding'] = 0.0

    # === Krok 2: Obliczenia H3/H4 (Metryki kroczące) ===
    
    # T = market_temperature (Zmienność dzienna z 30 dni)
    logger.info(f"[{ticker}] (V3) Obliczanie 'market_temperature' (T)...")
    df['daily_returns'] = df['close'].pct_change()
    df['market_temperature'] = df['daily_returns'].rolling(window=30).std()

    # ∇² (nabla_sq) = price_gravity (już obliczone i dodane w _load_all_data_for_ticker)
    df['nabla_sq'] = df['price_gravity']

    # --- Przygotowanie danych do S i m_sq ---
    
    # m_sq (attention_density - Wolumen)
    logger.info(f"[{ticker}] (V3) Obliczanie 'attention_density' (m_sq) - Wolumen...")
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
    logger.info(f"[{ticker}] (V3) Obliczanie 'attention_density' (m_sq) i 'info_entropy' (S) - Newsy...")
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
    logger.info(f"[{ticker}] (V3) Obliczanie 'entropy_change' (J)...")
    
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

    logger.info(f"[{ticker}] (V3) Wstępne obliczanie metryk zakończone.")
    
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
# === REFAKTORYZACJA (WYDAJNOŚĆ): Ta funkcja ładuje teraz dane dla V3 i V2 ===
# ==================================================================
def _load_all_data_for_ticker(ticker: str, api_client: AlphaVantageClient, session: Session, year_to_test: str) -> Optional[Dict[str, Any]]:
    """
    Pobiera i wstępnie przetwarza wszystkie dane (dla V3 i V2) dla pojedynczego tickera,
    korzystając z mechanizmu cache w bazie danych.
    """
    try:
        # --- KROK 1: ŁADOWANIE DANYCH CENOWYCH (Wspólne dla V3 i V2) ---
        
        # TIME_SERIES_DAILY (OHLCV - do HLC/3 Proxy)
        price_data_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='DAILY_OHLCV',
            api_func='get_time_series_daily', outputsize='full'
        )
        # TIME_SERIES_DAILY_ADJUSTED (Główna seria dla V3 i V2)
        daily_adjusted_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='DAILY_ADJUSTED',
            api_func='get_daily_adjusted', outputsize='full'
        )
        # TIME_SERIES_WEEKLY_ADJUSTED (Główna seria dla V2, V3 jej nie używa)
        weekly_data_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='WEEKLY',
            api_func='get_time_series_weekly', outputsize='full'
        )
        
        # --- KROK 2: ŁADOWANIE DANYCH V3 (H2, H3) ---
        bbands_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='BBANDS',
            api_func='get_bollinger_bands', interval='daily', time_period=20, nbdevup=2, nbdevdn=2
        )
        h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
        
        # --- KROK 3: ŁADOWANIE DANYCH V2 (QPS, VMS, TCS) ---
        rsi_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='RSI',
            api_func='get_rsi', interval='daily', time_period=14, series_type='close' # Spec V2: RSI(14)
        )
        macd_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='MACD',
            api_func='get_macd', interval='daily', series_type='close'
        )
        obv_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='OBV',
            api_func='get_obv', interval='daily'
        )
        ad_raw = get_raw_data_with_cache(
            session=session, api_client=api_client, ticker=ticker, data_type='AD',
            api_func='get_ad', interval='daily'
        )
        atr_v2_raw = get_raw_data_with_cache( # Oddzielne ATR(14) dla SL/TP w V2
            session=session, api_client=api_client, ticker=ticker, data_type='ATR',
            api_func='get_atr', interval='daily', time_period=14
        )
        earnings_raw = get_raw_data_with_cache( # Wspólne dla V3 i V2
            session=session, api_client=api_client, ticker=ticker, data_type='EARNINGS',
            api_func='get_earnings'
        )
        
        # --- KROK 4: DANE INTRADAY V3 (USUWANIE) ---
        intraday_5min_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume']).set_index(pd.to_datetime([]))
        
        # --- KROK 5: Walidacja i Przetwarzanie (V3) ---
        
        if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
           not daily_adjusted_raw or 'Time Series (Daily)' not in daily_adjusted_raw or \
           not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw:
            
            logger.warning(f"[Backtest V3/V2] Brak podstawowych danych (Daily/Weekly) z cache/API dla {ticker}, pomijanie.")
            return None
            
        # Przetwórz Daily OHLCV (dla HLC/3 proxy V3)
        daily_ohlcv_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
        daily_ohlcv_df.index = pd.to_datetime(daily_ohlcv_df.index) 
        daily_ohlcv_df = standardize_df_columns(daily_ohlcv_df)
        
        # Przetwórz Daily Adjusted (Główny DF dla V3 i V2)
        daily_adjusted_df = pd.DataFrame.from_dict(daily_adjusted_raw['Time Series (Daily)'], orient='index')
        daily_adjusted_df.index = pd.to_datetime(daily_adjusted_df.index)
        daily_adjusted_df = standardize_df_columns(daily_adjusted_df)

        # Przetwórz Weekly (Główny DF dla V2, V3 go nie używa)
        weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
        weekly_df.index = pd.to_datetime(weekly_df.index)
        weekly_df = standardize_df_columns(weekly_df)
        
        # === KLUCZOWA POPRAWKA (VWAP PROXY HLC/3) - Logika V3 ===
        daily_ohlcv_df['vwap_proxy'] = (daily_ohlcv_df['high'] + daily_ohlcv_df['low'] + daily_ohlcv_df['close']) / 3.0
        
        # Używamy DF Adjusted jako bazy i dołączamy do niego OHLCV i VWAP Proxy (dla V3)
        enriched_df = daily_adjusted_df.join(daily_ohlcv_df[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
        
        enriched_df['vwap'] = enriched_df['vwap_proxy']
        
        if enriched_df['vwap'].isnull().all():
             logger.warning(f"(V3) Brak danych HLC/3 VWAP Proxy dla {ticker}.")
        
        # === KRYTYCZNA POPRAWKA: TWORZENIE KOLUMNY 'price_gravity' (dla V3) ===
        close_col = 'close_ohlcv' if 'close_ohlcv' in enriched_df.columns else 'close'
        enriched_df['price_gravity'] = (enriched_df['vwap'] - enriched_df[close_col]) / enriched_df[close_col]
        # ================================================================

        # Wzbogacanie DataFrame (V3) (z użyciem danych SPY z Cache LITE)
        spy_aligned = _backtest_cache["spy_data"]['close'].reindex(enriched_df.index, method='ffill').rename('spy_close')
        enriched_df = enriched_df.join(spy_aligned) 
        
        # Obliczamy ATR (V3) (na nie-adjustowanych danych HLC, które są w enriched_df)
        enriched_df['atr_14'] = calculate_atr(enriched_df, period=14)
        
        # Obliczenia H1 (time_dilation) (V3)
        ticker_returns_rolling = enriched_df['close'].pct_change().rolling(window=20)
        spy_returns_rolling = enriched_df['spy_close'].pct_change().rolling(window=20)
        std_ticker = ticker_returns_rolling.std()
        std_spy = spy_returns_rolling.std()
        enriched_df['time_dilation'] = std_ticker / std_spy
        
        # === Dodanie tickera do DF (dla logowania w _pre_calculate_metrics V3) ===
        enriched_df['ticker'] = ticker
        
        enriched_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # Wypełnij NaN wartościami 0 (lub metodą ffill), aby symulatory V3 nie napotkały NaN
        enriched_df['time_dilation'] = enriched_df['time_dilation'].fillna(0)
        enriched_df['price_gravity'] = enriched_df['price_gravity'].fillna(0) # Zapewnienie, że jest kolumna
        
        enriched_df['atr_14'] = enriched_df['atr_14'].ffill().fillna(0)
        enriched_df['vwap'] = enriched_df['vwap'].ffill().fillna(0)

        
        # Przetwarzanie BBANDS (H3 V3)
        bbands_df = _parse_bbands(bbands_raw)
        
        # Przetwarzanie EARNINGS (Wspólne dla V3 i V2)
        earnings_df = _parse_earnings_data(earnings_raw)
        if earnings_df.index.tz is not None:
             # Konwertuj na naive, aby pasowało do indeksu daily_df
            earnings_df.index = earnings_df.index.tz_convert(None)
        
        # DANE INTRADAY V3 (USUWANIE)
        intraday_5min_df = pd.DataFrame() 
        
        # Zdobądź i zapisz sektor (Wspólne)
        sector = _get_sector_for_ticker(session, ticker)
        
        # ==================================================================
        # === NOWY KROK: Przetwarzanie i Tworzenie DF dla V2 ===
        # ==================================================================
        logger.info(f"[{ticker}] (V2) Przetwarzanie wskaźników dla AQM V2...")
        
        # Parsujemy nowe wskaźniki V2
        rsi_df = _parse_indicator_data(rsi_raw, 'Technical Analysis: RSI', 'RSI')
        macd_df = _parse_macd_data(macd_raw)
        obv_df = _parse_indicator_data(obv_raw, 'Technical Analysis: OBV', 'OBV')
        ad_df = _parse_indicator_data(ad_raw, 'Technical Analysis: AD', 'AD')
        atr_v2_df = _parse_indicator_data(atr_v2_raw, 'Technical Analysis: ATR', 'ATR')

        # Walidacja V2: Sprawdzamy, czy mamy kluczowe wskaźniki
        aqm_v2_ready = not (rsi_df.empty or macd_df.empty or obv_df.empty or ad_df.empty or atr_v2_df.empty)
        if not aqm_v2_ready:
             logger.warning(f"[Backtest V2] Brak kluczowych danych (RSI/MACD/OBV/AD/ATR) dla {ticker}. AQM V2 zostanie pominięte.")
        
        # AQM V2 (QPS) wymaga specyficznych EMA (50/200 D, 20/50 W)
        
        # V2 Daily DF (baza dla QPS i VMS)
        daily_v2_df = daily_adjusted_df.copy() # Używamy tej samej bazy cenowej co V3
        daily_v2_df['ema_50'] = calculate_ema(daily_v2_df['close'], period=50)
        daily_v2_df['ema_200'] = calculate_ema(daily_v2_df['close'], period=200)
        # Wolumen jest już w 'daily_adjusted_df' z 'standardize_df_columns'
        
        # V2 Weekly DF (baza dla QPS)
        weekly_v2_df = weekly_df.copy()
        weekly_v2_df['ema_20'] = calculate_ema(weekly_v2_df['close'], period=20)
        weekly_v2_df['ema_50'] = calculate_ema(weekly_v2_df['close'], period=50)
        # ==================================================================
        
        
        # Zwracamy wszystkie *surowe* przetworzone dane
        return {
            # Dane V3 (istniejące)
            "daily_raw": enriched_df, # Używane przez V3
            "weekly": weekly_df, # Używane przez V3 (chociaż V2 też go używa)
            "insider_df": h2_data["insider_df"], 
            "news_df": h2_data["news_df"],       
            "bbands_df": bbands_df,
            "earnings_df": earnings_df, # Używane przez V3 (Temporal) i V2 (TCS)
            "sector": sector,
            
            # === NOWE DANE (dla AQM V2) ===
            "aqm_v2_ready": aqm_v2_ready, # Flaga, czy mamy wszystkie dane
            "daily_v2": daily_v2_df,
            "weekly_v2": weekly_v2_df,
            "ras_df": _backtest_cache["ras_v2_data"], # Załadowane globalnie
            "rsi_v2": rsi_df,
            "macd_v2": macd_df,
            "obv_v2": obv_df,
            "ad_v2": ad_df,
            "atr_v2": atr_v2_df
        }
    
    except Exception as e:
        logger.error(f"[Backtest V3/V2] Błąd ładowania danych dla {ticker}: {e}", exc_info=True)
        return None
# ==================================================================


# ==================================================================
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (NOWA LOGIKA "SLICE-FIRST" + V2) ===
# ==================================================================
def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny dla V3 i V2.
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
        
    log_msg = f"BACKTEST HISTORYCZNY (Platforma AQM V3 + V2): Rozpoczynanie testu dla roku '{year}' ({start_date} do {end_date})"
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # === KROK 1: Czyszczenie Bazy Danych (V3 i V2) ===
    try:
        # Czyszczenie V3
        like_pattern_v3 = f"BACKTEST_{year}_AQM_V3_%"
        logger.info(f"Czyszczenie starych wyników AQM V3 dla wzorca: {like_pattern_v3}...")
        
        delete_stmt_v3 = text("DELETE FROM virtual_trades WHERE setup_type LIKE :pattern")
        result_v3 = session.execute(delete_stmt_v3, {'pattern': like_pattern_v3})
        
        logger.info(f"Pomyślnie usunięto {result_v3.rowcount} starych wpisów backtestu AQM V3 dla roku {year}.")

        # === NOWE CZYSZCZENIE (AQM V2) ===
        like_pattern_v2 = f"BACKTEST_{year}_AQM_V2" # Tylko jeden setup_type
        logger.info(f"Czyszczenie starych wyników AQM V2 dla wzorca: {like_pattern_v2}...")
        delete_stmt_v2 = text("DELETE FROM virtual_trades WHERE setup_type = :pattern")
        result_v2 = session.execute(delete_stmt_v2, {'pattern': like_pattern_v2})
        logger.info(f"Pomyślnie usunięto {result_v2.rowcount} starych wpisów backtestu AQM V2 dla roku {year}.")
        # =================================
        
        session.commit()
        
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()

    # === KROK 2: Pobieranie Listy Spółek (Bez zmian) ===
    try:
        log_msg_tickers = "[Backtest V3/V2] Pobieranie listy spółek z Fazy 1 ('Pierwsze Sito')..."
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

        log_msg = f"[Backtest V3/V2] Znaleziono {len(initial_tickers_to_test)} spółek z Fazy 1 do przetestowania."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów: {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 3: Budowanie Cache (DANE GLOBALNE V3 i V2) ===
    
    try:
        logger.info("[Backtest V3/V2] Rozpoczynanie budowania pamięci podręcznej (Cache LITE)...")
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}
        _backtest_cache["sector_map"] = {}
        _backtest_cache["ras_v2_data"] = None # Nowy cache dla V2

        # 1. Pobierz dane Makro V3 (VXX i SPY)
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
        
        # 2. Pobierz dane Sektorowe V3 (ETF-y)
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
        
        # ==================================================================
        # === NOWY KROK: Budowanie Cache dla AQM V2 (RAS) ===
        # ==================================================================
        logger.info("[Backtest V2] Cache: Ładowanie danych makro dla RAS (V2)...")
        
        # 1. Pobierz dane makro
        inflation_raw = api_client.get_inflation_rate(interval='monthly')
        fed_rate_raw = api_client.get_fed_funds_rate(interval='monthly')
        yield_raw = api_client.get_treasury_yield(interval='monthly', maturity='10year')
        
        # 2. Sparsuj je (używamy generycznego parsera V2)
        inflation_df = _parse_indicator_data(inflation_raw, 'data', 'value').rename(columns={'value': 'inflation'})
        fed_rate_df = _parse_indicator_data(fed_rate_raw, 'data', 'value').rename(columns={'value': 'fed_rate'})
        yield_df = _parse_indicator_data(yield_raw, 'data', 'value').rename(columns={'value': 'yield_10y'})

        # 3. Połącz w jeden DataFrame
        ras_df = pd.merge_asof(
            _backtest_cache["spy_data"][['close', 'ema_200']], # Użyj SPY jako głównej osi czasu
            inflation_df, left_index=True, right_index=True, direction='backward'
        )
        ras_df = pd.merge_asof(ras_df, fed_rate_df, left_index=True, right_index=True, direction='backward')
        ras_df = pd.merge_asof(ras_df, yield_df, left_index=True, right_index=True, direction='backward')
        
        ras_df = ras_df.ffill() # Wypełnij brakujące dane makro

        # 4. Oblicz logikę reżimu (zgodnie ze specyfikacją V2)
        # Używamy diff(1), aby sprawdzić, czy ostatnia *publikacja* była wyższa (dane miesięczne)
        ras_df['fed_rate_rising'] = ras_df['fed_rate'].diff(1) > 0 
        
        is_risk_off = (
            (ras_df['inflation'] > 4.0) |
            (ras_df['fed_rate_rising'] == True) |
            (ras_df['yield_10y'] > 4.5) |
            (ras_df['close'] < ras_df['ema_200'])
        )
        
        ras_df['RAS'] = np.where(is_risk_off, 0.1, 1.0) # 0.1 (kara) lub 1.0 (brak kary)
        
        _backtest_cache["ras_v2_data"] = ras_df[['RAS']] # Zapisz tylko wynikową kolumnę
        logger.info("[Backtest V2] Cache: Obliczanie Regime Adaptation Score (RAS) zakończone.")
        # ==================================================================

        # 3. Wstępne ładowanie mapy sektorów V3 (dla spójności)
        for ticker in initial_tickers_to_test:
            sector = _get_sector_for_ticker(session, ticker)
            if sector not in _backtest_cache["tickers_by_sector"]:
                _backtest_cache["tickers_by_sector"][sector] = []
            _backtest_cache["tickers_by_sector"][sector].append(ticker)


        logger.info("[Backtest V3/V2] Budowanie pamięci podręcznej (Cache LITE) zakończone.")
        append_scan_log(session, "[Backtest V3/V2] Budowanie pamięci podręcznej (Cache LITE) zakończone.")
        update_scan_progress(session, 0, len(initial_tickers_to_test)) 

    except Exception as e:
        log_msg = f"[Backtest V3/V2] BŁĄD KRYTYCZNY podczas budowania cache LITE: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 4: Uruchomienie Symulacji (NOWA LOGIKA "SLICE-FIRST" + V2) ===
    
    log_msg_aqm = "[Backtest V3/V2] Uruchamianie Pętli Symulacyjnych H1-H4 (V3) oraz AQM_V2..."
    logger.info(log_msg_aqm)
    append_scan_log(session, log_msg_aqm)
    
    trades_found_h1 = 0
    trades_found_h2 = 0
    trades_found_h3 = 0
    trades_found_h4 = 0
    trades_found_v2 = 0 # Nowy licznik
    
    total_tickers = len(initial_tickers_to_test)

    for i, ticker in enumerate(initial_tickers_to_test):
        if i % 1 == 0: # Loguj każdy ticker, aby widzieć postęp
            log_msg = f"[Backtest V3/V2] Przetwarzanie {ticker} ({i+1}/{total_tickers})..."
            logger.info(log_msg)
            # Loguj do bazy co 20 tickerów
            if i % 20 == 0: 
                 append_scan_log(session, log_msg)
            update_scan_progress(session, i, total_tickers)
        
        ticker_data_raw_dict = None # Zmieniona nazwa, aby uniknąć pomyłek
        try:
            # ==================================================================
            # === NOWA LOGIKA (KROK 4.1): Ładowanie surowych danych (V3 i V2) ===
            # ==================================================================
            ticker_data_raw_dict = _load_all_data_for_ticker(ticker, api_client, session, year)
            
            if not ticker_data_raw_dict or 'daily_raw' not in ticker_data_raw_dict:
                logger.warning(f"[Backtest V3/V2] Brak surowych danych dla {ticker}. Pomijanie.")
                continue
            
            # Pobieramy pełny, surowy DF (5066+ dni)
            full_historical_data_raw = ticker_data_raw_dict['daily_raw']
            
            # ==================================================================
            # === NOWA LOGIKA (KROK 4.2): "SLICE FIRST" (NAPRAWA BŁĘDU "5066 DNI") ===
            # ==================================================================
            try:
                # Znajdź pierwszy dzień handlowy ROKU
                indexer = full_historical_data_raw.index.get_indexer([start_date], method='bfill')
                if indexer[0] == -1: raise KeyError("Data startu nie znaleziona")
                start_index = indexer[0]
            except KeyError:
                logger.warning(f"[Backtest V3/V2] Brak danych dla {ticker} w roku {year} lub przed nim. Pomijanie.")
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
            # === ISTNIEJĄCA LOGIKA (V3) (KROK 4.3): PRE-CALCULATE (na małym wycinku) ===
            # ==================================================================
            logger.info(f"[{ticker}] (V3) Wycinek ({len(data_slice_for_processing)} dni) gotowy. Rozpoczynanie obliczeń metryk V3...")
            
            # Ta funkcja jest teraz BARDZO SZYBKA, bo działa na ~450 wierszach
            enriched_slice = _pre_calculate_metrics(
                daily_df=data_slice_for_processing,
                insider_df=ticker_data_raw_dict["insider_df"],
                news_df=ticker_data_raw_dict["news_df"],
                bbands_df=ticker_data_raw_dict["bbands_df"]
            )
            
            # ==================================================================
            # === ISTNIEJĄCA LOGIKA (V3) (KROK 4.4): Uruchomienie Symulatorów V3 ===
            # ==================================================================
            
            # Słownik dla symulatorów H1, H2, H3, H4
            h_data_slice_dict = {
                "daily": enriched_slice, # Wzbogacony wycinek
                "insider_df": ticker_data_raw_dict.get("insider_df"),
                "news_df": ticker_data_raw_dict.get("news_df"),
                "bbands_df": ticker_data_raw_dict.get("bbands_df"),
                "earnings_df": ticker_data_raw_dict.get("earnings_df") # Dodajemy dane o wynikach
            }

            # Symulator H1 (potrzebuje 'daily' DF)
            trades_found_h1 += aqm_v3_h1_simulator._simulate_trades_h1(
                session, 
                ticker, 
                h_data_slice_dict, 
                year
            )
            
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

            # ==================================================================
            # === NOWA LOGIKA: Uruchomienie Symulatora AQM V2 ===
            # ==================================================================
            logger.info(f"[{ticker}] (V2) Uruchamianie symulatora AQM V2...")
            
            # Sprawdzamy, czy dane dla V2 zostały poprawnie załadowane
            if ticker_data_raw_dict.get("aqm_v2_ready", False):
                # Przekazujemy *tylko* te dane, których V2 potrzebuje (dla czystości)
                v2_data_dict = {
                    "daily_v2": ticker_data_raw_dict.get("daily_v2"),
                    "weekly_v2": ticker_data_raw_dict.get("weekly_v2"),
                    "ras_df": ticker_data_raw_dict.get("ras_df"),
                    "earnings_v2": ticker_data_raw_dict.get("earnings_df"), # Używamy tego samego co V3
                    "atr_v2": ticker_data_raw_dict.get("atr_v2"),
                    "rsi_v2": ticker_data_raw_dict.get("rsi_v2"),
                    "macd_v2": ticker_data_raw_dict.get("macd_v2"),
                    "obv_v2": ticker_data_raw_dict.get("obv_v2"),
                    "ad_v2": ticker_data_raw_dict.get("ad_v2"),
                }
                
                # Używamy tego samego `year`
                trades_found_v2 += aqm_v2_simulator._simulate_trades_aqm_v2(
                    session,
                    ticker,
                    v2_data_dict, 
                    year
                )
            else:
                logger.warning(f"[{ticker}] (V2) Pomijanie symulacji AQM V2 z powodu braku kompletnych danych (flaga 'aqm_v2_ready' była False).")
            # ==================================================================

        except Exception as e:
            logger.error(f"[Backtest V3/V2][GŁÓWNA PĘTLA] Błąd krytyczny dla {ticker}: {e}", exc_info=True)
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
            if 'v2_data_dict' in locals():
                del v2_data_dict
            
            gc.collect() # Wymuszenie GC, aby agresywnie zwalniać pamięć
            
            
    trades_found_total = trades_found_h1 + trades_found_h2 + trades_found_h3 + trades_found_h4 + trades_found_v2 # Dodano V2
    update_scan_progress(session, total_tickers, total_tickers) 
    
    log_msg_final = f"BACKTEST HISTORYCZNY (V3+V2) ZAKOŃCZONY (Rok '{year}').\n" \
                    f"  > Model V3 (H1-H4): {trades_found_h1 + trades_found_h2 + trades_found_h3 + trades_found_h4} transakcji (H1:{trades_found_h1}, H2:{trades_found_h2}, H3:{trades_found_h3}, H4:{trades_found_h4})\n" \
                    f"  > Model V2 (AQM): {trades_found_v2} transakcji\n" \
                    f"  > ŁĄCZNIE: {trades_found_total} transakcji."
    logger.info(log_msg_final)
    append_scan_log(session, log_msg_final)
