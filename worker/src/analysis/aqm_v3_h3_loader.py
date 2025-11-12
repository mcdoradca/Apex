import logging
import pandas as pd
from typing import Dict, Any, Optional
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import standardize_df_columns # Potrzebne do parsowania

logger = logging.getLogger(__name__)

def _parse_bbands(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Przetwarza surową odpowiedź JSON z BBANDS na DataFrame
    gotowy do backtestu.
    """
    try:
        data = raw_data.get('Technical Analysis: BBANDS', {})
        if not data:
            return pd.DataFrame(columns=['Real Middle Band', 'Real Upper Band', 'Real Lower Band']).set_index(pd.to_datetime([]))

        df = pd.DataFrame.from_dict(data, orient='index')
        df.index = pd.to_datetime(df.index)
        
        # Konwertuj na liczby
        for col in ['Real Middle Band', 'Real Upper Band', 'Real Lower Band']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        df.sort_index(inplace=True)
        return df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych BBANDS: {e}", exc_info=True)
        return None

def _parse_intraday_5min(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Przetwarza surową odpowiedź JSON z TIME_SERIES_INTRADAY (5min)
    na DataFrame gotowy do backtestu.
    """
    try:
        data = raw_data.get('Time Series (5min)', {})
        if not data:
            return pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume']).set_index(pd.to_datetime([]))

        df = pd.DataFrame.from_dict(data, orient='index')
        # Używamy standardize_df_columns do konwersji '1. open' -> 'open' i typów
        df = standardize_df_columns(df)
        df.index = pd.to_datetime(df.index)
            
        df.sort_index(inplace=True)
        return df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych Intraday 5min: {e}", exc_info=True)
        return None

def load_h3_data_into_cache(ticker: str, api_client: AlphaVantageClient) -> Dict[str, pd.DataFrame]:
    """
    Główna funkcja tego modułu. Pobiera i przetwarza dane Wymiaru 3 i 4
    dla pojedynczego tickera.
    """
    logger.info(f"[Backtest V3][H3 Loader] Ładowanie danych Wymiaru 3 i 4 dla {ticker}...")
    
    # 1. Pobierz dane BBANDS (Wymiar 3.1)
    # Używamy parametrów ze specyfikacji (period=20, nbdev=2)
    bbands_raw = api_client.get_bollinger_bands(
        ticker, 
        interval='daily', 
        time_period=20, 
        nbdevup=2, 
        nbdevdn=2
    )
    bbands_df = _parse_bbands(bbands_raw)
    
    if bbands_df is None:
        logger.warning(f"[Backtest V3][H3 Loader] Nie udało się przetworzyć danych BBANDS dla {ticker}. Tworzenie pustego DataFrame.")
        bbands_df = pd.DataFrame(columns=['Real Middle Band', 'Real Upper Band', 'Real Lower Band']).set_index(pd.to_datetime([]))
    
    # 2. Pobierz dane Intraday 5min (Wymiar 4.1)
    # Specyfikacja wymaga 'outputsize=full' (dla 30 dni danych)
    intraday_raw = api_client.get_intraday(
        ticker, 
        interval='5min', 
        outputsize='full'
    )
    intraday_5min_df = _parse_intraday_5min(intraday_raw)
    
    if intraday_5min_df is None:
        logger.warning(f"[Backtest V3][H3 Loader] Nie udało się przetworzyć danych Intraday 5min dla {ticker}. Tworzenie pustego DataFrame.")
        intraday_5min_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume']).set_index(pd.to_datetime([]))

    logger.info(f"[Backtest V3][H3 Loader] Załadowano {len(bbands_df)} punktów BBANDS i {len(intraday_5min_df)} świec 5-min dla {ticker}.")

    return {
        "bbands_df": bbands_df,
        "intraday_5min_df": intraday_5min_df
    }
