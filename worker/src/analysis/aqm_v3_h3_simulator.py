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
    Wersja ZNORMALIZOWANA (Fix: Data Trap).
    Musi być matematycznie spójna z 'backtest_engine.py' i 'apex_optimizer.py'.
    """
    trades_found = 0
    daily_df = historical_data.get("daily")

    if daily_df is None or daily_df.empty:
        return 0

    # === KONFIGURACJA PARAMETRÓW ===
    params = parameters or {}
    # Wartości domyślne (zgodne z APEX V4)
    DEFAULT_PERCENTILE = 0.95
    DEFAULT_M_SQ_THRESHOLD = -0.5
    DEFAULT_TP_MULT = 5.0
    DEFAULT_SL_MULT = 2.0
    DEFAULT_MAX_HOLD = 5
    DEFAULT_SETUP_NAME = 'AQM_V3_H3_NORMALIZED'
    DEFAULT_MIN_SCORE = 1.0 # Hard Floor (V4)

    try:
        param_percentile = float(params.get('h3_percentile')) if params.get('h3_percentile') is not None else DEFAULT_PERCENTILE
        param_m_sq_threshold = float(params.get('h3_m_sq_threshold')) if params.get('h3_m_sq_threshold') is not None else DEFAULT_M_SQ_THRESHOLD
        param_tp_mult = float(params.get('h3_tp_multiplier')) if params.get('h3_tp_multiplier') is not None else DEFAULT_TP_MULT
        param_sl_mult = float(params.get('h3_sl_multiplier')) if params.get('h3_sl_multiplier') is not None else DEFAULT_SL_MULT
        param_max_hold = int(params.get('h3_max_hold')) if params.get('h3_max_hold') is not None else DEFAULT_MAX_HOLD
        param_name = str(params.get('setup_name')) if params.get('setup_name') and str(params.get('setup_name')).strip() else DEFAULT_SETUP_NAME
        
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
    else:
        setup_name_suffix = param_name

    history_buffer = 201 
    # Okno do normalizacji Z-Score (musi być wystarczająco długie, identyczne jak w backtest_engine)
    norm_window = 100 
    
    if len(daily_df) < history_buffer + 1:
        return 0

    # === PRE-PROCESSING: NORMALIZACJA SKŁADNIKÓW (Fix Data Trap) ===
    # Cel: Sprowadzenie wszystkich metryk do wspólnego mianownika (odchylenia standardowe)
    # Aby surowa liczba newsów nie dominowała nad sentymentem.
    
    # Sprawdzenie czy kolumny istnieją, jeśli nie - inicjalizacja zerami (zabezpieczenie)
    for col in ['institutional_sync', 'retail_herding', 'market_temperature', 'information_entropy']:
        if col not in daily_df.columns:
            daily_df[col] = 0.0

    # 1. Institutional Sync (mu) -> mu_norm (Z-Score)
    # Ograniczamy (clip) ekstremalne wartości do +/- 3 sigma
    mu_mean = daily_df['institutional_sync'].rolling(window=norm_window).mean()
    mu_std = daily_df['institutional_sync'].rolling(window=norm_window).std(ddof=1)
    daily_df['mu_norm'] = ((daily_df['institutional_sync'] - mu_mean) / mu_std).replace([np.inf, -np.inf], 0).fillna(0).clip(-3.0, 3.0)

    # 2. Retail Herding (Q) -> Q_norm (Z-Score)
    q_mean = daily_df['retail_herding'].rolling(window=norm_window).mean()
    q_std = daily_df['retail_herding'].rolling(window=norm_window).std(ddof=1)
    daily_df['Q_norm'] = ((daily_df['retail_herding'] - q_mean) / q_std).replace([np.inf, -np.inf], 0).fillna(0).clip(-3.0, 3.0)

    # 3. Market Temp (T) -> T_norm (Z-Score)
    t_mean = daily_df['market_temperature'].rolling(window=norm_window).mean()
    t_std = daily_df['market_temperature'].rolling(window=norm_window).std(ddof=1)
    daily_df['T_norm'] = ((daily_df['market_temperature'] - t_mean) / t_std).replace([np.inf, -np.inf], 0).fillna(0).clip(-3.0, 3.0)

    # 4. Information Entropy (S) -> S_norm (Z-Score)
    s_mean = daily_df['information_entropy'].rolling(window=norm_window).mean()
    s_std = daily_df['information_entropy'].rolling(window=norm_window).std(ddof=1)
    daily_df['S_norm'] = ((daily_df['information_entropy'] - s_mean) / s_std).replace([np.inf, -np.inf], 0).fillna(0).clip(-3.0, 3.0)

    # === OBLICZENIE J (POTENCJAŁU) NA ZNORMALIZOWANYCH DANYCH ===
    # J = S_norm - (Q_norm / T_norm) + (mu_norm * 1.0)
    
    # Unikamy dzielenia przez zero w Q/T
    denominator = daily_df['T_norm'].replace(0, 0.001)
    term_qt = daily_df['Q_norm'] / denominator
    
    daily_df['J_new'] = daily_df['S_norm'] - term_qt + (daily_df['mu_norm'] * 1.0)
    
    # === OBLICZENIA METRYK KOŃCOWYCH (AQM SCORE) ===
    
    # Upewnij się, że m_sq i nabla_sq są obecne
    if 'm_sq' not in daily_df.columns: daily_df['m_sq'] = 0.0
    if 'nabla_sq' not in daily_df.columns: daily_df['nabla_sq'] = 0.0

    # Teraz normalizujemy same finalne składniki pola (J, Nabla, M)
    j_mean = daily_df['J_new'].rolling(window=norm_window).mean()
    j_std = daily_df['J_new'].rolling(window=norm_window).std(ddof=1)
    j_norm_final = ((daily_df['J_new'] - j_mean) / j_std).replace([np.inf, -np.inf], 0).fillna(0)
    
    nabla_mean = daily_df['nabla_sq'].rolling(window=norm_window).mean()
    nabla_std = daily_df['nabla_sq'].rolling(window=norm_window).std(ddof=1)
    nabla_norm_final = ((daily_df['nabla_sq'] - nabla_mean) / nabla_std).replace([np.inf, -np.inf], 0).fillna(0)
    
    m_mean = daily_df['m_sq'].rolling(window=norm_window).mean()
    m_std = daily_df['m_sq'].rolling(window=norm_window).std(ddof=1)
    m_norm_final = ((daily_df['m_sq'] - m_mean) / m_std).replace([np.inf, -np.inf], 0).fillna(0)

    # Główna Formuła Pola (AQM V3 Score)
    # Score = Potencjał (J) - Opór (Nabla) - Masa (M)
    aqm_score_series = (1.0 * j_norm_final) - (1.0 * nabla_norm_final) - (1.0 * m_norm_final)
    
    # Dynamiczny próg (percentyl)
    percentile_threshold_series = aqm_score_series.rolling(window=norm_window).quantile(param_percentile)

    # === PĘTLA SYMULACYJNA (STRICT MODE) ===
    for i in range(history_buffer, len(daily_df) - 1): 
        candle_D = daily_df.iloc[i] 
        date_str = daily_df.index[i].strftime('%Y-%m-%d')

        current_aqm_score = aqm_score_series.iloc[i]
        current_threshold = percentile_threshold_series.iloc[i]
        current_m_norm = m_norm_final.iloc[i]

        if pd.isna(current_aqm_score) or pd.isna(current_threshold):
            continue
        
        # WARUNKI WEJŚCIA (STRICT)
        # 1. Przebicie percentyla (Sygnał relatywny)
        is_h3_signal = current_aqm_score > current_threshold
        
        # 2. Masa poniżej progu (Low inertia)
        is_mass_ok = current_m_norm < param_m_sq_threshold
        
        # 3. Hard Floor (Sygnał absolutny - Fix V4)
        is_score_high_enough = current_aqm_score > param_min_score

        if is_h3_signal:
            # LOGOWANIE DIAGNOSTYCZNE
            # Logujemy szczegóły dla wybranych tickerów, abyś widział składowe AQM
            if ticker in ['SSP', 'BCG', 'AAPL', 'TSLA'] or trades_found < 3:
                logger.info(f"AQM Debug - {ticker} @ {date_str}: "
                            f"J_raw={candle_D['J_new']:.2f}, "
                            f"J_norm={j_norm_final.iloc[i]:.2f}, "
                            f"M_norm={current_m_norm:.2f}, "
                            f"AQM={current_aqm_score:.2f} > {current_threshold:.2f}")

            if is_mass_ok and is_score_high_enough:
                logger.info(f"✅ H3 SIGNAL ACCEPTED: {ticker} on {date_str}. Score: {current_aqm_score:.2f}")
                
                try:
                    candle_D_plus_1 = daily_df.iloc[i + 1]
                    entry_price = candle_D_plus_1['open']
                    atr_value = candle_D['atr_14']
                    
                    if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                        continue
                    
                    take_profit = entry_price + (param_tp_mult * atr_value)
                    stop_loss = entry_price - (param_sl_mult * atr_value)
                    
                    time_dilation = 0.0
                    if 'time_dilation' in candle_D:
                        time_dilation = float(candle_D['time_dilation'])
                    elif 'time_dilation' in daily_df.columns:
                         time_dilation = float(daily_df.iloc[i]['time_dilation'])
                    
                    setup_h3 = {
                        "ticker": ticker,
                        "setup_type": setup_name_suffix, 
                        "entry_price": float(entry_price),
                        "stop_loss": float(stop_loss),
                        "take_profit": float(take_profit),
                        
                        # Metryki (Zapisujemy znormalizowane wersje dla analizy)
                        "metric_atr_14": float(atr_value),
                        "metric_aqm_score_h3": float(current_aqm_score),
                        "metric_aqm_percentile_95": float(current_threshold), 
                        "metric_J_norm": float(j_norm_final.iloc[i]),
                        "metric_nabla_sq_norm": float(nabla_norm_final.iloc[i]),
                        "metric_m_sq_norm": float(current_m_norm),
                        
                        "metric_J": float(candle_D['J_new']), # Nowe J
                        "metric_inst_sync": float(candle_D['institutional_sync']),
                        "metric_retail_herding": float(candle_D['retail_herding']),
                        "metric_time_dilation": time_dilation,
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
                        
                except Exception as e:
                    logger.error(f"[Backtest H3] Error processing signal (Day {date_str}): {e}", exc_info=True)
                    session.rollback()
            else:
                # Logowanie odrzucenia (diagnostyka)
                if ticker in ['SSP', 'BCG']:
                    logger.info(f"❌ H3 REJECTED: {ticker} (Mass: {current_m_norm:.2f} < {param_m_sq_threshold}? {is_mass_ok} | Score: {current_aqm_score:.2f} > {param_min_score}? {is_score_high_enough})")
        
        else:
            pass

    if trades_found > 0:
        try:
            session.commit()
            logger.info(f"[Backtest H3] SUCCESS: Saved {trades_found} normalized trades for {ticker}.")
        except Exception as e:
            logger.error(f"Error committing H3 trades: {e}")
            session.rollback()
        
    return trades_found
