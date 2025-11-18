import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional, Tuple

# Importujemy modele i funkcje pomocnicze
from .. import models
# Importujemy funkcję egzekucji transakcji z symulatora H1
from .aqm_v3_h1_simulator import _resolve_trade
# Importujemy ATR z utils
from .utils import calculate_atr

logger = logging.getLogger(__name__)

def _simulate_trades_h3(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], 
    year: str,
    parameters: Dict[str, Any] = None # <-- NOWOŚĆ: Argument dla dynamicznych parametrów
) -> int:
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEJ SPÓŁKI
    i szuka setupów zgodnych z Hipotezą H3 (Uproszczony Model Pola).
    
    Teraz obsługuje dynamiczne parametry przekazywane z Dashboardu.
    """
    trades_found = 0
    daily_df = historical_data.get("daily")

    if daily_df is None or daily_df.empty:
        return 0

    # === 1. KONFIGURACJA PARAMETRÓW (Z Domyślnymi Wartościami) ===
    # Jeśli parameters jest None, używamy pustego słownika
    params = parameters or {}

    # Pobieramy wartości lub ustawiamy domyślne (zgodne ze strategią bazową/Test 2)
    try:
        param_percentile = float(params.get('h3_percentile', 0.95))
        param_m_sq_threshold = float(params.get('h3_m_sq_threshold', -0.5)) # Domyślnie Test 2
        param_tp_mult = float(params.get('h3_tp_multiplier', 5.0))
        param_sl_mult = float(params.get('h3_sl_multiplier', 2.0))
        param_max_hold = int(params.get('h3_max_hold', 5))
        # Nazwa setupu, np. "CUSTOM_H3" lub domyślna
        setup_name_suffix = str(params.get('setup_name', 'AQM_V3_H3_DYNAMIC'))
    except (ValueError, TypeError) as e:
        logger.error(f"Błąd parsowania parametrów H3 dla {ticker}: {e}. Używam domyślnych.")
        param_percentile = 0.95
        param_m_sq_threshold = -0.5
        param_tp_mult = 5.0
        param_sl_mult = 2.0
        param_max_hold = 5
        setup_name_suffix = 'AQM_V3_H3_ERROR_FALLBACK'

    # ==========================================================

    history_buffer = 201 
    percentile_window = 100 
    
    if len(daily_df) < history_buffer + 1:
        return 0

    # === OBLICZENIA METRYK ===
    # (Te obliczenia pozostają bez zmian, są rdzeniem fizyki modelu)
    
    j_mean = daily_df['J'].rolling(window=percentile_window).mean()
    j_norm = (daily_df['J'] - j_mean) / daily_df['J'].rolling(window=percentile_window).std(ddof=1)
    
    nabla_mean = daily_df['nabla_sq'].rolling(window=percentile_window).mean()
    nabla_norm = (daily_df['nabla_sq'] - nabla_mean) / daily_df['nabla_sq'].rolling(window=percentile_window).std(ddof=1)
    
    m_mean = daily_df['m_sq'].rolling(window=percentile_window).mean()
    m_norm = (daily_df['m_sq'] - m_mean) / daily_df['m_sq'].rolling(window=percentile_window).std(ddof=1)

    j_norm = j_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    nabla_norm = nabla_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    m_norm = m_norm.replace([np.inf, -np.inf], np.nan).fillna(0)

    # Oblicz AQM_V3_SCORE
    aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
    
    # === DYNAMICZNY PERCENTYL ===
    # Używamy zmiennej param_percentile zamiast sztywnego 0.95
    percentile_threshold_series = aqm_score_series.rolling(window=percentile_window).quantile(param_percentile)

    # Główna pętla symulacyjna
    for i in range(history_buffer, len(daily_df) - 1): 
        
        # --- Dzień D ---
        # === POPRAWKA BŁĘDU NameError ===
        # Musimy zdefiniować candle_D na początku pętli, aby mieć dostęp do ATR
        candle_D = daily_df.iloc[i] 
        # ================================

        current_aqm_score = aqm_score_series.iloc[i]
        current_threshold = percentile_threshold_series.iloc[i]
        current_m_norm = m_norm.iloc[i]

        if pd.isna(current_aqm_score) or pd.isna(current_threshold):
            continue
        
        # === DYNAMICZNE WARUNKI WEJŚCIA ===
        # 1. Score > Próg (zdefiniowany przez param_percentile)
        # 2. Masa < Próg (zdefiniowany przez param_m_sq_threshold)
        
        if (current_aqm_score > current_threshold) and (current_m_norm < param_m_sq_threshold):
            
            # --- ZNALEZIONO SYGNAŁ ---
            try:
                candle_D_plus_1 = daily_df.iloc[i + 1]
                entry_price = candle_D_plus_1['open']
                
                # Teraz candle_D jest zdefiniowane, więc to zadziała
                atr_value = candle_D['atr_14']
                
                if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                    continue
                
                # === DYNAMICZNE PARAMETRY EGZEKUCJI ===
                take_profit = entry_price + (param_tp_mult * atr_value)
                stop_loss = entry_price - (param_sl_mult * atr_value)
                max_hold_days = param_max_hold
                
                setup_h3 = {
                    "ticker": ticker,
                    "setup_type": setup_name_suffix, # Używamy dynamicznej nazwy
                    "entry_price": float(entry_price),
                    "stop_loss": float(stop_loss),
                    "take_profit": float(take_profit),
                    
                    # Metryki do logowania
                    "metric_atr_14": float(atr_value),
                    "metric_aqm_score_h3": float(current_aqm_score),
                    "metric_aqm_percentile_95": float(current_threshold), # Logujemy obliczony próg
                    "metric_J_norm": float(j_norm.iloc[i]),
                    "metric_nabla_sq_norm": float(nabla_norm.iloc[i]),
                    "metric_m_sq_norm": float(current_m_norm),
                }

                trade = _resolve_trade(
                    daily_df, 
                    i + 1, 
                    setup_h3, 
                    max_hold_days, 
                    year, 
                    direction='LONG'
                )
                if trade:
                    session.add(trade)
                    trades_found += 1
                    
            except IndexError:
                continue
            except Exception as e:
                logger.error(f"[Backtest H3] Błąd (Dzień {daily_df.index[i].date()}): {e}", exc_info=True)
                session.rollback()

    if trades_found > 0:
        try:
            session.commit()
            # Logujemy tylko raz na ticker, żeby nie śmiecić, ale z info o parametrach
            logger.debug(f"[Backtest H3] Zapisano {trades_found} transakcji dla {ticker}. Params: m<{param_m_sq_threshold}, p>{param_percentile}")
        except Exception as e:
            logger.error(f"Błąd commitowania H3 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
