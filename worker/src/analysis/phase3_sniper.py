import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import List, Dict, Any

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
    calculate_h3_metrics_v4
)
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands

logger = logging.getLogger(__name__)

# Domyślne parametry (używane tylko gdy UI wyśle puste)
DEFAULT_PARAMS = {
    'h3_percentile': 0.95,
    'h3_m_sq_threshold': -0.5,
    'h3_min_score': 0.0,
    'h3_tp_multiplier': 5.0,
    'h3_sl_multiplier': 2.0,
    'h3_max_hold': 5
}

REQUIRED_HISTORY_SIZE = 201 

def run_h3_live_scan(session: Session, candidates: List[str], api_client: AlphaVantageClient, parameters: Dict[str, Any] = None):
    """
    Faza 3 (H3 Live - RAW).
    Wersja oczyszczona ze wszystkich "bezpieczników" V5.
    Używa WYŁĄCZNIE parametrów podanych przez użytkownika.
    """
    logger.info("Uruchamianie Fazy 3 (RAW - Strict User Parameters)...")
    
    # 1. Wczytanie parametrów Użytkownika (BEZ ADAPTACJI)
    final_params = DEFAULT_PARAMS.copy()
    if parameters:
        for k, v in parameters.items():
            if v is not None and str(v).strip() != "": 
                try:
                    final_params[k] = float(v)
                except:
                    pass # Jeśli nie da się rzutować, zostaw domyślny

    # Logowanie parametrów - pełna transparentność
    params_log = f"Faza 3 START. Parametry Użytkownika: {final_params}"
    logger.info(params_log)
    append_scan_log(session, params_log)
    
    # Rozpakowanie parametrów
    h3_percentile = float(final_params['h3_percentile'])
    h3_m_sq_threshold = float(final_params['h3_m_sq_threshold'])
    h3_min_score = float(final_params['h3_min_score'])
    h3_tp_mult = float(final_params['h3_tp_multiplier'])
    h3_sl_mult = float(final_params['h3_sl_multiplier'])
    
    signals_generated = 0
    total_candidates = len(candidates)
    
    # Statystyki odrzuceń (dla Twojej wiedzy)
    rejects = {'data': 0, 'history': 0, 'score': 0, 'mass': 0, 'min_floor': 0}

    for i, ticker in enumerate(candidates):
        if i % 5 == 0: update_scan_progress(session, i, total_candidates)
        
        # Logowanie postępu co 50 (żebyś widział, że działa)
        if i > 0 and i % 50 == 0:
             append_scan_log(session, f"Faza 3: {i}/{total_candidates}. Sygnałów: {signals_generated}. Odrzucono (Score/Mass): {rejects['score']}/{rejects['mass']}")

        try:
            # A. Pobieranie Danych
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12, outputsize='full')
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12, outputsize='full')
            
            if not daily_raw or not daily_adj_raw: 
                rejects['data'] += 1; continue
            
            h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')

            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj.index = pd.to_datetime(daily_adj.index)

            if len(daily_adj) < REQUIRED_HISTORY_SIZE: 
                rejects['history'] += 1; continue

            daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
            df['close'] = df[close_col]

            # Metryki podstawowe
            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            df['nabla_sq'] = df['price_gravity']

            # Dane o Newsach
            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(df.index, fill_value=0)
                df['information_entropy'] = news_counts.rolling(window=10).sum()
            else:
                df['information_entropy'] = 0.0
            
            # B. Obliczenia H3 (Zunifikowane V4, ale na Twoich parametrach)
            df = calculate_h3_metrics_v4(df, final_params)
            
            last_candle = df.iloc[-1]
            
            # Pobranie wartości (zabezpieczone, ale surowe)
            current_aqm = last_candle.get('aqm_score_h3', 0.0)
            current_thresh = last_candle.get('aqm_percentile_95', 0.0)
            current_m = last_candle.get('m_sq_norm', 0.0)
            
            # C. LOGIKA DECYZYJNA (Czysta)
            # Żadnych dodatkowych warunków. Tylko matematyka H3.
            
            score_pass = current_aqm > current_thresh
            mass_pass = current_m < h3_m_sq_threshold
            floor_pass = current_aqm > h3_min_score
            
            # Diagnostyka
            if not score_pass: rejects['score'] += 1
            elif not mass_pass: rejects['mass'] += 1
            elif not floor_pass: rejects['min_floor'] += 1

            if score_pass and mass_pass and floor_pass:
                atr = last_candle['atr_14']
                ref_price = last_candle['close']
                
                take_profit = ref_price + (h3_tp_mult * atr)
                stop_loss = ref_price - (h3_sl_mult * atr)
                entry_price = ref_price

                # D. Zapis Sygnału (Bez sprawdzania Live API, żeby nic nie blokowało)
                existing = session.query(models.TradingSignal).filter(
                    models.TradingSignal.ticker == ticker,
                    models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
                ).first()
                
                if not existing:
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
                        is_trailing_active=True, 
                        highest_price_since_entry=float(ref_price),
                        notes=f"AQM H3 RAW. Score:{current_aqm:.2f} > Thresh:{current_thresh:.2f}. Mass:{current_m:.2f}"
                    )
                    session.add(new_signal)
                    session.commit()
                    signals_generated += 1
                    
                    msg = (f"⚛️ H3 SIGNAL (RAW): {ticker}\n"
                           f"Cena: {ref_price:.2f}\n"
                           f"Score: {current_aqm:.2f}")
                    send_telegram_alert(msg)
                    append_scan_log(session, f"✅ SYGNAŁ: {ticker} (Score: {current_aqm:.2f}).")

        except Exception as e:
            logger.error(f"Błąd H3 Live dla {ticker}: {e}", exc_info=True)
            session.rollback()
            continue

    update_scan_progress(session, total_candidates, total_candidates)
    
    summary = (f"Faza 3 zakończona. Sygnałów: {signals_generated}. "
               f"Odrzuty: Score={rejects['score']}, Mass={rejects['mass']}, "
               f"Floor={rejects['min_floor']}, Data={rejects['data']}")
    
    append_scan_log(session, summary)
    logger.info(summary)
