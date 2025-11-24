import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

from .utils import (
    get_raw_data_with_cache,
    standardize_df_columns,
    calculate_atr,
    append_scan_log,
    update_scan_progress,
    send_telegram_alert,
    safe_float,
    get_system_control_value,
    calculate_h3_metrics_v4 # Używamy zunifikowanej funkcji V4
)
# Korzystamy z centralnych metryk
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands

# Import Adaptive Executor
from .apex_optimizer import AdaptiveExecutor

logger = logging.getLogger(__name__)

# ============================================================================
# === STAŁE DOMYŚLNE (FALLBACK) ===
# ============================================================================
DEFAULT_PARAMS = {
    'h3_percentile': 0.95,
    'h3_m_sq_threshold': -0.5,
    'h3_min_score': 0.0,
    'h3_tp_multiplier': 5.0,
    'h3_sl_multiplier': 2.0,
    'h3_max_hold': 5
}

H3_CALC_WINDOW = 100 
REQUIRED_HISTORY_SIZE = 201 

def _get_market_conditions(session: Session, api_client: AlphaVantageClient) -> Dict[str, Any]:
    """
    Oblicza metryki rynkowe (VIX Proxy, Trend) dla AdaptiveExecutor.
    """
    conditions = {'vix': 20.0, 'trend': 'NEUTRAL'} 
    
    try:
        macro_sentiment = get_system_control_value(session, 'macro_sentiment') or "UNKNOWN"
        
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=24, outputsize='full')
        
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)
            spy_df.sort_index(inplace=True)
            
            if len(spy_df) > 200:
                recent_returns = spy_df['close'].pct_change().tail(30)
                vix_proxy = recent_returns.std() * (252 ** 0.5) * 100
                
                current_price = spy_df['close'].iloc[-1]
                sma_200 = spy_df['close'].rolling(window=200).mean().iloc[-1]
                trend = 'BULL' if current_price > sma_200 else 'BEAR'
                
                conditions['vix'] = float(vix_proxy) if not pd.isna(vix_proxy) else 20.0
                conditions['trend'] = trend
        
        if "RISK_OFF" in macro_sentiment:
            logger.warning("Faza 0 zgłasza RISK_OFF. Wymuszam tryb HIGH_VOLATILITY dla parametrów.")
            conditions['vix'] = max(conditions['vix'], 30.0) 
            
        logger.info(f"Warunki Rynkowe wykryte: VIX={conditions['vix']:.2f}, Trend={conditions['trend']}, Makro={macro_sentiment}")
        return conditions

    except Exception as e:
        logger.warning(f"Błąd podczas badania warunków rynkowych: {e}. Używam domyślnych.")
        return conditions

def _is_setup_still_valid(entry: float, sl: float, tp: float, current_price: float) -> tuple[bool, str]:
    """Strażnik Ważności Setupu."""
    if current_price is None or entry == 0:
        return False, "Brak aktualnej ceny lub błędna cena wejścia"
    if current_price <= sl:
        return False, f"SPALONY: Cena ({current_price:.2f}) przebiła już Stop Loss ({sl:.2f})."
    if current_price >= tp:
        return False, f"ZA PÓŹNO: Cena ({current_price:.2f}) osiągnęła już Take Profit ({tp:.2f})."
    
    potential_profit = tp - current_price
    potential_loss = current_price - sl
    if potential_loss <= 0: return False, "Błąd matematyczny (Cena poniżej SL)."
    current_rr = potential_profit / potential_loss
    if current_rr < 1.5:
        return False, f"NIEOPŁACALNY: Cena uciekła ({current_price:.2f}). Aktualny R:R to tylko {current_rr:.2f}."
    return True, "OK"

def run_h3_live_scan(session: Session, candidates: List[str], api_client: AlphaVantageClient, parameters: Dict[str, Any] = None):
    """
    Główna pętla Fazy 3 (H3 LIVE SNIPER) - V5 UPGRADE.
    
    V5 CHANGES:
    - Wdrożono analizę RVOL (Relative Volume)
    - Wykrywanie "Cichej Akumulacji"
    """
    logger.info("Uruchamianie Fazy 3: H3 LIVE SNIPER (Adaptive + V5 Volume Hunter)...")
    
    base_params = DEFAULT_PARAMS.copy()
    if parameters:
        for k, v in parameters.items():
            if v is not None: base_params[k] = float(v)

    append_scan_log(session, "Faza 3 (V5): Analiza warunków i adaptacja parametrów...")
    
    market_conditions = _get_market_conditions(session, api_client)
    executor = AdaptiveExecutor(base_params)
    adapted_params = executor.get_adapted_params(market_conditions)
    
    h3_percentile = float(adapted_params['h3_percentile'])
    h3_m_sq_threshold = float(adapted_params['h3_m_sq_threshold'])
    h3_min_score = float(adapted_params['h3_min_score'])
    h3_tp_mult = float(adapted_params['h3_tp_multiplier'])
    h3_sl_mult = float(adapted_params['h3_sl_multiplier'])
    
    signals_generated = 0
    total_candidates = len(candidates)

    for i, ticker in enumerate(candidates):
        if i % 5 == 0: update_scan_progress(session, i, total_candidates)
        
        try:
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12, outputsize='full')
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12, outputsize='full')
            
            if not daily_raw or not daily_adj_raw: continue
            
            h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')

            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj.index = pd.to_datetime(daily_adj.index)

            if len(daily_adj) < REQUIRED_HISTORY_SIZE: continue

            daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
            df['close'] = df[close_col]

            # Metryki Podstawowe
            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            df['nabla_sq'] = df['price_gravity']

            # === V5 VOLUME HUNTER (ANOMALIE) ===
            # Obliczamy średnią kroczącą wolumenu (20 dni)
            df['vol_mean_20'] = df['volume'].rolling(window=20).mean()
            # RVOL: Relative Volume Ratio
            df['rvol'] = df['volume'] / df['vol_mean_20']
            
            # Detekcja Cichej Akumulacji: Mała zmienność ceny + Duży Wolumen
            # Cena zmienia się < 1.5%, ale wolumen > 120% średniej
            df['is_silent_accumulation'] = (df['daily_returns'].abs() < 0.015) & (df['rvol'] > 1.2)

            # Normalizacja i H3 (Unified V4 Logic)
            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(df.index, fill_value=0)
                df['information_entropy'] = news_counts.rolling(window=10).sum()
            else:
                df['information_entropy'] = 0.0
            
            # Wywołanie zunifikowanej funkcji obliczeniowej
            df = calculate_h3_metrics_v4(df, adapted_params)
            
            # Pobranie serii
            aqm_score_series = df['aqm_score_h3']
            threshold_series = df['aqm_percentile_95']
            m_norm = df['m_sq_norm']

            # Analiza Ostatniej Świecy
            last_candle = df.iloc[-1]
            current_aqm = aqm_score_series.iloc[-1]
            current_thresh = threshold_series.iloc[-1]
            current_m = m_norm.iloc[-1]
            
            # === LOGIKA DECYZYJNA V5 ===
            is_accumulation = bool(last_candle.get('is_silent_accumulation', False))
            rvol_val = last_candle.get('rvol', 1.0)
            
            # Warunek podstawowy
            condition_met = (current_aqm > current_thresh) and \
                            (current_m < h3_m_sq_threshold) and \
                            (current_aqm > h3_min_score)
            
            # V5: BONUS ZA AKUMULACJĘ
            # Jeśli wykryto cichą akumulację, obniżamy lekko próg wejścia (bo "Smart Money" już tam są)
            if is_accumulation:
                condition_met = (current_aqm > (current_thresh * 0.9)) and \
                                (current_m < h3_m_sq_threshold) and \
                                (current_aqm > h3_min_score)
                append_scan_log(session, f"V5: Wykryto akumulację dla {ticker} (RVOL: {rvol_val:.2f}). Obniżono próg wejścia.")

            if condition_met:
                atr = last_candle['atr_14']
                ref_price = last_candle['close']
                
                take_profit = ref_price + (h3_tp_mult * atr)
                stop_loss = ref_price - (h3_sl_mult * atr)
                entry_price = ref_price

                # E. WALIDACJA LIVE (Realtime Price Check)
                current_live_quote = api_client.get_global_quote(ticker)
                current_live_price = safe_float(current_live_quote.get('05. price')) if current_live_quote else None
                
                validation_status = True
                validation_reason = "Setup świeży (Post/Pre-Market)"
                
                if current_live_price:
                    validation_status, validation_reason = _is_setup_still_valid(entry_price, stop_loss, take_profit, current_live_price)

                if not validation_status:
                    append_scan_log(session, f"Odrzucono {ticker}: {validation_reason} (AQM: {current_aqm:.2f})")
                    continue
                
                # F. Zapis Sygnału
                existing = session.query(models.TradingSignal).filter(
                    models.TradingSignal.ticker == ticker,
                    models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
                ).first()
                
                if not existing:
                    # Notatka V5 z informacją o wolumenie
                    vol_note = f" [RVOL: {rvol_val:.2f}]" if rvol_val > 1.1 else ""
                    acc_note = " [ACCUMULATION]" if is_accumulation else ""
                    
                    new_signal = models.TradingSignal(
                        ticker=ticker,
                        status='PENDING',
                        generation_date=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                        signal_candle_timestamp=last_candle.name,
                        entry_price=float(entry_price),
                        stop_loss=float(stop_loss),
                        take_profit=float(take_profit),
                        entry_zone_top=float(ref_price + (0.5 * atr)),
                        entry_zone_bottom=float(ref_price - (0.5 * atr)),
                        risk_reward_ratio=float(h3_tp_mult/h3_sl_mult),
                        # Włączamy Trailing Stop domyślnie dla V5
                        is_trailing_active=True, 
                        highest_price_since_entry=float(ref_price),
                        notes=f"AQM H3 V5. Score:{current_aqm:.2f}{vol_note}{acc_note}"
                    )
                    session.add(new_signal)
                    session.commit()
                    signals_generated += 1
                    
                    msg = (f"⚛️ H3 QUANTUM V5: {ticker}\n"
                           f"Cena: {ref_price:.2f}{vol_note}{acc_note}\n"
                           f"Trailing Stop: AKTYWNY")
                    send_telegram_alert(msg)

        except Exception as e:
            logger.error(f"Błąd H3 Live dla {ticker}: {e}", exc_info=True)
            session.rollback()
            continue

    update_scan_progress(session, total_candidates, total_candidates)
    append_scan_log(session, f"Faza 3 (H3 Live V5) zakończona. Wygenerowano {signals_generated} sygnałów.")
    logger.info(f"Faza 3 zakończona. Sygnałów: {signals_generated}")
