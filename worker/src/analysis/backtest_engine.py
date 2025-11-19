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

# IMPORT DANYCH (Musi zostać dla H3)
# Ten moduł ładuje dane Insiderów i Newsów, które są składową "J" w H3.
from . import aqm_v3_h2_loader 
from .aqm_v3_h3_loader import _parse_bbands

# SYMULATORY (Tylko H3)
from . import aqm_v3_h3_simulator

from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    append_scan_log,
    update_scan_progress,
    calculate_atr,
    get_raw_data_with_cache
)
from .. import models
from ..config import SECTOR_TO_ETF_MAP
import gc

logger = logging.getLogger(__name__)

# ==================================================================
# Cache LITE (Bez zmian)
# ==================================================================
_backtest_cache = { "vix_data": None, "spy_data": None, "sector_etf_data": {}, "tickers_by_sector": {}, "sector_map": {} }

def _get_sector_for_ticker(session: Session, ticker: str) -> str:
    if ticker not in _backtest_cache["sector_map"]:
        try:
            sector = session.execute(text("SELECT sector FROM companies WHERE ticker = :ticker"), {'ticker': ticker}).scalar()
            _backtest_cache["sector_map"][ticker] = sector or "N/A"
        except Exception: _backtest_cache["sector_map"][ticker] = "N/A"
    return _backtest_cache["sector_map"][ticker]

# ==================================================================
# PRZYGOTOWANIE METRYK (Critical for H3)
# Ta funkcja musi zostać, ponieważ oblicza J, institucional_sync, etc.
# ==================================================================
def _pre_calculate_metrics(daily_df: pd.DataFrame, insider_df: pd.DataFrame, news_df: pd.DataFrame, bbands_df: pd.DataFrame) -> pd.DataFrame:
    ticker = daily_df['ticker'].iloc[0] if 'ticker' in daily_df.columns and not daily_df.empty else 'UNKNOWN'
    df = daily_df.copy()
    
    if insider_df.index.tz is not None: insider_df = insider_df.tz_convert(None)
    if news_df.index.tz is not None: news_df = news_df.tz_convert(None)

    # Metryki H2 (Używane przez H3 jako składniki J)
    try:
        df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
    except: df['institutional_sync'] = 0.0

    try:
        df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
    except: df['retail_herding'] = 0.0

    # Metryki H3/H4
    df['daily_returns'] = df['close'].pct_change()
    df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
    df['nabla_sq'] = df['price_gravity'] # Zdefiniowane w load_all_data

    # Metryki Wolumenu i Newsów (dla m_sq i S)
    df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
    df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
    df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
    df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)

    if not news_df.empty:
        news_counts_daily = news_df.groupby(news_df.index.date).size()
        news_counts_daily.index = pd.to_datetime(news_counts_daily.index)
        news_counts_daily = news_counts_daily.reindex(df.index, fill_value=0)
        df['information_entropy'] = news_counts_daily.rolling(window=10).sum()
        
        df['news_mean_200d'] = df['information_entropy'].rolling(window=200).mean()
        df['news_std_200d'] = df['information_entropy'].rolling(window=200).std()
        df['normalized_news'] = ((df['information_entropy'] - df['news_mean_200d']) / df['news_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
    else:
        df['information_entropy'] = 0.0
        df['normalized_news'] = 0.0
        
    df['m_sq'] = df['normalized_volume'] + df['normalized_news']

    # Obliczenie J (Używane przez H3)
    S = df['information_entropy']
    Q = df['retail_herding']
    T = df['market_temperature']
    mu = df['institutional_sync']
    
    J = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
    df['J'] = J.fillna(S + (mu * 1.0))

    cols_to_drop = ['daily_returns', 'avg_volume_10d', 'vol_mean_200d', 'vol_std_200d', 'normalized_volume', 'information_entropy', 'news_mean_200d', 'news_std_200d', 'normalized_news']
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    
    return df

def _load_all_data_for_ticker(ticker: str, api_client: AlphaVantageClient, session: Session, year_to_test: str) -> Optional[Dict[str, Any]]:
    try:
        # Pobieranie danych cenowych (H1 Base)
        price_data_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
        daily_adjusted_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        
        # Dane BBANDS (H3)
        bbands_raw = get_raw_data_with_cache(session, api_client, ticker, 'BBANDS', 'get_bollinger_bands', interval='daily', time_period=20, nbdevup=2, nbdevdn=2)
        
        # Dane News/Insider (Używane w H3 jako J)
        h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
        
        if not price_data_raw or not daily_adjusted_raw: return None
            
        daily_ohlcv_df = standardize_df_columns(pd.DataFrame.from_dict(price_data_raw.get('Time Series (Daily)', {}), orient='index'))
        daily_ohlcv_df.index = pd.to_datetime(daily_ohlcv_df.index)
        
        daily_adjusted_df = standardize_df_columns(pd.DataFrame.from_dict(daily_adjusted_raw.get('Time Series (Daily)', {}), orient='index'))
        daily_adjusted_df.index = pd.to_datetime(daily_adjusted_df.index)

        # Obliczenia podstawowe (VWAP Proxy, Price Gravity, Time Dilation)
        daily_ohlcv_df['vwap_proxy'] = (daily_ohlcv_df['high'] + daily_ohlcv_df['low'] + daily_ohlcv_df['close']) / 3.0
        enriched_df = daily_adjusted_df.join(daily_ohlcv_df[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
        enriched_df['vwap'] = enriched_df['vwap_proxy']
        
        close_col = 'close_ohlcv' if 'close_ohlcv' in enriched_df.columns else 'close'
        enriched_df['price_gravity'] = (enriched_df['vwap'] - enriched_df[close_col]) / enriched_df[close_col]

        spy_aligned = _backtest_cache["spy_data"]['close'].reindex(enriched_df.index, method='ffill').rename('spy_close')
        enriched_df = enriched_df.join(spy_aligned)
        
        enriched_df['atr_14'] = calculate_atr(enriched_df, period=14)
        
        std_ticker = enriched_df['close'].pct_change().rolling(window=20).std()
        std_spy = enriched_df['spy_close'].pct_change().rolling(window=20).std()
        enriched_df['time_dilation'] = std_ticker / std_spy
        enriched_df['ticker'] = ticker
        
        enriched_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        enriched_df['time_dilation'] = enriched_df['time_dilation'].fillna(0)
        enriched_df['price_gravity'] = enriched_df['price_gravity'].fillna(0)
        enriched_df['atr_14'] = enriched_df['atr_14'].ffill().fillna(0)
        enriched_df['vwap'] = enriched_df['vwap'].ffill().fillna(0)

        bbands_df = _parse_bbands(bbands_raw)
        sector = _get_sector_for_ticker(session, ticker)
        
        return {
            "daily_raw": enriched_df,
            "insider_df": h2_data["insider_df"], 
            "news_df": h2_data["news_df"],       
            "bbands_df": bbands_df,             
            "sector": sector
        }
    except Exception as e:
        logger.error(f"[Backtest V3] Błąd ładowania danych dla {ticker}: {e}", exc_info=True)
        return None

# ==================================================================
# GŁÓWNA PĘTLA (H3 ONLY)
# ==================================================================
def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str, parameters: Dict[str, Any] = None):
    try:
        if not (year.isdigit() and len(year) == 4): raise ValueError(f"Zły rok: {year}")
        if int(year) > datetime.now(timezone.utc).year: raise ValueError(f"Przyszłość: {year}")
        start_date, end_date = f"{year}-01-01", f"{year}-12-31"
    except Exception as e:
        logger.error(f"[Backtest] Błąd walidacji: {e}")
        return

    # === KLUCZOWA ZMIANA: IDENTYFIKACJA DOKŁADNEJ NAZWY SETUPU ===
    # Musimy wiedzieć, jaki dokładnie setup (suffix) zostanie użyty,
    # aby usunąć TYLKO jego stare wyniki, a nie wszystkie wyniki "AQM_V3" z tego roku.
    
    setup_name_suffix = 'AQM_V3_H3_DYNAMIC' # Domyślna nazwa (musi pasować do tej w aqm_v3_h3_simulator.py)
    if parameters and parameters.get('setup_name'):
        s_name = str(parameters.get('setup_name')).strip()
        if s_name:
             setup_name_suffix = s_name
    
    target_setup_type = f"BACKTEST_{year}_{setup_name_suffix}"
    
    log_msg = f"BACKTEST HISTORYCZNY (H3 ONLY): Rok {year} [Setup: {target_setup_type}]"
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # === CHIRURGICZNE CZYSZCZENIE ===
    try:
        # Usuwamy dokładnie ten jeden setup_type, pozwalając innym (np. TEST_V1 vs TEST_V2) współistnieć.
        delete_stmt = text("DELETE FROM virtual_trades WHERE setup_type = :target_type")
        result = session.execute(delete_stmt, {"target_type": target_setup_type})
        session.commit()
        if result.rowcount > 0:
            logger.info(f"Usunięto {result.rowcount} starych wpisów dla '{target_setup_type}'.")
    except Exception as e:
        logger.error(f"Błąd czyszczenia starych wyników: {e}")
        session.rollback()

    try:
        initial_tickers_to_test = sorted([r[0] for r in session.execute(text("SELECT DISTINCT ticker FROM phase1_candidates")).fetchall()])
        if not initial_tickers_to_test:
            append_scan_log(session, "BŁĄD: Brak kandydatów Fazy 1.")
            return
    except Exception: return

    # Cache LITE (Makro)
    try:
        vix_raw = api_client.get_daily_adjusted('VXX', outputsize='full')
        spy_raw = api_client.get_daily_adjusted('SPY', outputsize='full')
        
        vix_df = standardize_df_columns(pd.DataFrame.from_dict(vix_raw.get('Time Series (Daily)', {}), orient='index'))
        vix_df.index = pd.to_datetime(vix_df.index)
        _backtest_cache["vix_data"] = vix_df
        
        spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
        spy_df.index = pd.to_datetime(spy_df.index)
        _backtest_cache["spy_data"] = spy_df
        
        update_scan_progress(session, 0, len(initial_tickers_to_test)) 
    except Exception as e:
        logger.error(f"Błąd Cache LITE: {e}")
        return

    trades_found_h3 = 0
    total_tickers = len(initial_tickers_to_test)

    for i, ticker in enumerate(initial_tickers_to_test):
        if i % 5 == 0:
            logger.info(f"Przetwarzanie {ticker} ({i+1}/{total_tickers})...")
            update_scan_progress(session, i, total_tickers)
        
        ticker_data_raw_dict = None
        try:
            ticker_data_raw_dict = _load_all_data_for_ticker(ticker, api_client, session, year)
            if not ticker_data_raw_dict: continue
            
            full_historical_data_raw = ticker_data_raw_dict['daily_raw']
            
            try:
                start_index = full_historical_data_raw.index.get_indexer([start_date], method='bfill')[0]
                if start_index == -1: raise KeyError
            except KeyError: continue

            history_buffer = 201 
            if start_index < history_buffer: continue

            data_slice_for_processing = full_historical_data_raw.iloc[start_index - history_buffer:].loc[:end_date] 
            if len(data_slice_for_processing) < history_buffer + 1: continue
            
            enriched_slice = _pre_calculate_metrics(
                daily_df=data_slice_for_processing,
                insider_df=ticker_data_raw_dict["insider_df"],
                news_df=ticker_data_raw_dict["news_df"],
                bbands_df=ticker_data_raw_dict["bbands_df"]
            )
            
            h_data_slice_dict = { "daily": enriched_slice }

            # === TYLKO H3 ===
            trades_found_h3 += aqm_v3_h3_simulator._simulate_trades_h3(
                session, ticker, h_data_slice_dict, year, parameters=parameters
            )

        except Exception as e:
            logger.error(f"Błąd dla {ticker}: {e}", exc_info=True)
            session.rollback()
        finally:
            if 'enriched_slice' in locals(): del enriched_slice
            gc.collect()
            
    update_scan_progress(session, total_tickers, total_tickers) 
    final_msg = f"BACKTEST ZAKOŃCZONY. Znaleziono {trades_found_h3} transakcji H3."
    logger.info(final_msg)
    append_scan_log(session, final_msg)
