import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from typing import Dict, Any

# Importujemy modele i funkcje pomocnicze
from .. import models
from .utils import calculate_atr, _resolve_trade

logger = logging.getLogger(__name__)

def _simulate_trades_h3(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], 
    year: str,
    parameters: Dict[str, Any] = None
) -> int:
    """
    Symulator Hipotezy H3 (Simplified Field Model).
    W pełni niezależny od innych symulatorów.
    """
    trades_found = 0
    daily_df = historical_data.get("daily")

    if daily_df is None or daily_df.empty:
        return 0

    # === KONFIGURACJA PARAMETRÓW ===
    params = parameters or {}
    DEFAULT_PERCENTILE = 0.95
    DEFAULT_M_SQ_THRESHOLD = -0.5
    DEFAULT_TP_MULT = 5.0
    DEFAULT_SL_MULT = 2.0
    DEFAULT_MAX_HOLD = 5
    DEFAULT_SETUP_NAME = 'AQM_V3_H3_DYNAMIC'
    DEFAULT_MIN_SCORE = 0.0 

    try:
        param_percentile = float(params.get('h3_percentile')) if params.get('h3_percentile') is not None else DEFAULT_PERCENTILE
        param_m_sq_threshold = float(params.get('h3_m_sq_threshold')) if params.get('h3_m_sq_threshold') is not None else DEFAULT_M_SQ_THRESHOLD
        param_tp_mult = float(params.get('h3_tp_multiplier')) if params.get('h3_tp_multiplier') is not None else DEFAULT_TP_MULT
        param_sl_mult = float(params.get('h3_sl_multiplier')) if params.get('h3_sl_multiplier') is not None else DEFAULT_SL_MULT
        param_max_hold = int(params.get('h3_max_hold')) if params.get('h3_max_hold') is not None else DEFAULT_MAX_HOLD
        param_name = str(params.get('setup_name')) if params.get('setup_name') and str(params.get('setup_name')).strip() else DEFAULT_SETUP_NAME
        setup_name_suffix = param_name
        
        param_min_score = float(params.get('h3_min_score')) if params.get('h3_min_score') is not None else DEFAULT_MIN_SCORE

    except (ValueError, TypeError) as e:
        logger.error(f"Błąd parsowania parametrów H3 dla {ticker}: {e}. Używam domyślnych.")
        param_percentile = DEFAULT_PERCENTILE
        param_m_sq_threshold = DEFAULT_M_SQ_THRESHOLD
        param_tp_mult = DEFAULT_TP_MULT
        param_sl_mult = DEFAULT_SL_MULT
        param_max_hold = DEFAULT_MAX_HOLD
        param_min_score = DEFAULT_MIN_SCORE
        setup_name_suffix = 'AQM_V3_H3_PARSING_ERROR'

    history_buffer = 201 
    percentile_window = 100 
    
    if len(daily_df) < history_buffer + 1:
        return 0

    # === OBLICZENIA METRYK H3 ===
    
    j_mean = daily_df['J'].rolling(window=percentile_window).mean()
    j_norm = (daily_df['J'] - j_mean) / daily_df['J'].rolling(window=percentile_window).std(ddof=1)
    
    nabla_mean = daily_df['nabla_sq'].rolling(window=percentile_window).mean()
    nabla_norm = (daily_df['nabla_sq'] - nabla_mean) / daily_df['nabla_sq'].rolling(window=percentile_window).std(ddof=1)
    
    m_mean = daily_df['m_sq'].rolling(window=percentile_window).mean()
    m_norm = (daily_df['m_sq'] - m_mean) / daily_df['m_sq'].rolling(window=percentile_window).std(ddof=1)

    j_norm = j_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    nabla_norm = nabla_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    m_norm = m_norm.replace([np.inf, -np.inf], np.nan).fillna(0)

    # Główna Formuła Pola (AQM V3 Score)
    aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
    
    # Dynamiczny próg (percentyl)
    percentile_threshold_series = aqm_score_series.rolling(window=percentile_window).quantile(param_percentile)

    # === PĘTLA SYMULACYJNA ===
    for i in range(history_buffer, len(daily_df) - 1): 
        candle_D = daily_df.iloc[i] 

        current_aqm_score = aqm_score_series.iloc[i]
        current_threshold = percentile_threshold_series.iloc[i]
        current_m_norm = m_norm.iloc[i]

        if pd.isna(current_aqm_score) or pd.isna(current_threshold):
            continue
        
        if (current_aqm_score > current_threshold) and \
           (current_m_norm < param_m_sq_threshold) and \
           (current_aqm_score > param_min_score):  
            
            try:
                candle_D_plus_1 = daily_df.iloc[i + 1]
                entry_price = candle_D_plus_1['open']
                atr_value = candle_D['atr_14']
                
                if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                    continue
                
                take_profit = entry_price + (param_tp_mult * atr_value)
                stop_loss = entry_price - (param_sl_mult * atr_value)
                
                # === ZABEZPIECZENIE PRZED KEYERROR: Time Dilation ===
                time_dilation = 0.0
                if 'time_dilation' in candle_D:
                    time_dilation = float(candle_D['time_dilation'])
                elif 'time_dilation' in daily_df.columns:
                     # Próba odczytu z dataframe, jeśli w serii jest błąd
                     time_dilation = float(daily_df.iloc[i]['time_dilation'])
                # ===================================================
                
                setup_h3 = {
                    "ticker": ticker,
                    "setup_type": setup_name_suffix, 
                    "entry_price": float(entry_price),
                    "stop_loss": float(stop_loss),
                    "take_profit": float(take_profit),
                    
                    # Logowanie Metryk H3
                    "metric_atr_14": float(atr_value),
                    "metric_aqm_score_h3": float(current_aqm_score),
                    "metric_aqm_percentile_95": float(current_threshold), 
                    "metric_J_norm": float(j_norm.iloc[i]),
                    "metric_nabla_sq_norm": float(nabla_norm.iloc[i]),
                    "metric_m_sq_norm": float(current_m_norm),
                    
                    # Logowanie Komponentów Składowych
                    "metric_J": float(candle_D['J']),
                    "metric_inst_sync": float(candle_D['institutional_sync']),
                    "metric_retail_herding": float(candle_D['retail_herding']),
                    "metric_time_dilation": time_dilation, # Używamy bezpiecznej zmiennej
                    "metric_price_gravity": float(candle_D['price_gravity']),
                }

                trade = _resolve_trade(
                    daily_df, 
                    i + 1, 
                    setup_h3, 
                    param_max_hold, 
                    year, 
                    direction='LONG'
                )
                if trade:
                    session.add(trade)
                    trades_found += 1
                    
            except IndexError:
                continue
            except Exception as e:
                logger.error(f"[Backtest H3] Error (Day {daily_df.index[i].date()}): {e}", exc_info=True)
                session.rollback()

    if trades_found > 0:
        try:
            session.commit()
            logger.debug(f"[Backtest H3] Saved {trades_found} trades for {ticker}.")
        except Exception as e:
            logger.error(f"Error committing H3 trades: {e}")
            session.rollback()
        
    return trades_found
