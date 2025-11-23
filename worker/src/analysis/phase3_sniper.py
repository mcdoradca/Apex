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
    get_system_control_value # Dodano do odczytu sentymentu Fazy 0
)
# Korzystamy z centralnych metryk
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands

# === NOWOŚĆ: Import Adaptive Executor ===
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
    'h3_sl_multiplier': 2.0
}

H3_CALC_WINDOW = 100 
REQUIRED_HISTORY_SIZE = 201 

def _get_market_conditions(session: Session, api_client: AlphaVantageClient) -> Dict[str, Any]:
    """
    Oblicza metryki rynkowe (VIX Proxy, Trend) dla AdaptiveExecutor.
    """
    conditions = {'vix': 20.0, 'trend': 'NEUTRAL'} # Wartości bezpieczne/domyślne
    
    try:
        # 1. Sprawdź sentyment Makro z Fazy 0 (baza danych)
        macro_sentiment = get_system_control_value(session, 'macro_sentiment') or "UNKNOWN"
        
        # 2. Pobierz dane SPY (Benchmark) do obliczenia VIX Proxy i Trendu
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=24, outputsize='full')
        
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)
            spy_df.sort_index(inplace=True)
            
            if len(spy_df) > 200:
                # A. VIX Proxy: Roczna zmienność z ostatnich 30 dni
                # Wzór: StdDev(Returns_30d) * sqrt(252) * 100
                recent_returns = spy_df['close'].pct_change().tail(30)
                vix_proxy = recent_returns.std() * (252 ** 0.5) * 100
                
                # B. Trend: Cena vs SMA 200
                current_price = spy_df['close'].iloc[-1]
                sma_200 = spy_df['close'].rolling(window=200).mean().iloc[-1]
                trend = 'BULL' if current_price > sma_200 else 'BEAR'
                
                conditions['vix'] = float(vix_proxy) if not pd.isna(vix_proxy) else 20.0
                conditions['trend'] = trend
        
        # Logika hybrydowa: Jeśli Faza 0 krzyczy RISK_OFF, wymuś tryb wysokiej zmienności
        if "RISK_OFF" in macro_sentiment:
            logger.warning("Faza 0 zgłasza RISK_OFF. Wymuszam tryb HIGH_VOLATILITY dla parametrów.")
            conditions['vix'] = max(conditions['vix'], 30.0) # Wymuś > 25
            
        logger.info(f"Warunki Rynkowe wykryte: VIX={conditions['vix']:.2f}, Trend={conditions['trend']}, Makro={macro_sentiment}")
        return conditions

    except Exception as e:
        logger.warning(f"Błąd podczas badania warunków rynkowych: {e}. Używam domyślnych.")
        return conditions

def _is_setup_still_valid(entry: float, sl: float, tp: float, current_price: float) -> tuple[bool, str]:
    """
    Strażnik Ważności Setupu.
    """
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
    Główna pętla Fazy 3 (H3 LIVE SNIPER).
    Analizuje rynek w czasie rzeczywistym, ADAPTUJE parametry i szuka sygnałów.
    """
    logger.info("Uruchamianie Fazy 3: H3 LIVE SNIPER (Adaptive)...")
    
    # 1. Pobierz i scal parametry (Użytkownik > Domyślne)
    base_params = DEFAULT_PARAMS.copy()
    if parameters:
        for k, v in parameters.items():
            if v is not None: base_params[k] = float(v)

    # 2. === ADAPTACJA (Apex V4 Brain) ===
    append_scan_log(session, "Faza 3: Analiza warunków rynkowych i adaptacja parametrów...")
    
    market_conditions = _get_market_conditions(session, api_client)
    executor = AdaptiveExecutor(base_params)
    adapted_params = executor.get_adapted_params(market_conditions)
    
    # Logowanie zmian adaptacyjnych
    changes_log = []
    for k, v in adapted_params.items():
        orig = base_params.get(k)
        if orig != v:
            changes_log.append(f"{k}: {orig} -> {v:.3f}")
    
    if changes_log:
        log_msg = f"ADAPTACJA AKTYWNA (VIX: {market_conditions['vix']:.1f}): " + ", ".join(changes_log)
        logger.info(log_msg)
        append_scan_log(session, log_msg)
    else:
        append_scan_log(session, "Faza 3: Warunki neutralne. Parametry bez zmian.")

    # Rozpakowanie finalnych parametrów do zmiennych dla pętli
    h3_percentile = float(adapted_params['h3_percentile'])
    h3_m_sq_threshold = float(adapted_params['h3_m_sq_threshold'])
    h3_min_score = float(adapted_params['h3_min_score'])
    h3_tp_mult = float(adapted_params['h3_tp_multiplier'])
    h3_sl_mult = float(adapted_params['h3_sl_multiplier'])
    
    signals_generated = 0
    total_candidates = len(candidates)

    # 3. Główna Pętla Skanowania
    for i, ticker in enumerate(candidates):
        if i % 5 == 0: update_scan_progress(session, i, total_candidates)
        
        try:
            # A. Pobieranie Danych (Cache + API)
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12, outputsize='full')
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12, outputsize='full')
            
            if not daily_raw or not daily_adj_raw: continue
            
            bbands_raw = get_raw_data_with_cache(session, api_client, ticker, 'BBANDS', 'get_bollinger_bands', expiry_hours=24, interval='daily', time_period=20)
            bbands_df = _parse_bbands(bbands_raw)

            h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')

            # B. Przetwarzanie
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

            # C. Metryki (Vectorized)
            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            df['nabla_sq'] = df['price_gravity']

            df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
            df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
            df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
            df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)

            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(df.index, fill_value=0)
                df['information_entropy'] = news_counts.rolling(window=10).sum()
                df['news_mean_200d'] = df['information_entropy'].rolling(window=200).mean()
                df['news_std_200d'] = df['information_entropy'].rolling(window=200).std()
                df['normalized_news'] = ((df['information_entropy'] - df['news_mean_200d']) / df['news_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
            else:
                df['information_entropy'] = 0.0
                df['normalized_news'] = 0.0
            
            df['m_sq'] = df['normalized_volume'] + df['normalized_news']

            S = df['information_entropy']
            Q = df['retail_herding']
            T = df['market_temperature']
            mu = df['institutional_sync']
            J = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
            df['J'] = J.fillna(S + (mu * 1.0))

            j_mean = df['J'].rolling(window=H3_CALC_WINDOW).mean()
            j_std = df['J'].rolling(window=H3_CALC_WINDOW).std(ddof=1)
            j_norm = ((df['J'] - j_mean) / j_std).fillna(0)
            
            nabla_mean = df['nabla_sq'].rolling(window=H3_CALC_WINDOW).mean()
            nabla_std = df['nabla_sq'].rolling(window=H3_CALC_WINDOW).std(ddof=1)
            nabla_norm = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
            
            m_mean = df['m_sq'].rolling(window=H3_CALC_WINDOW).mean()
            m_std = df['m_sq'].rolling(window=H3_CALC_WINDOW).std(ddof=1)
            m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)
            
            aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
            threshold_series = aqm_score_series.rolling(window=H3_CALC_WINDOW).quantile(h3_percentile)

            # D. Analiza Ostatniej Świecy
            last_candle = df.iloc[-1]
            current_aqm = aqm_score_series.iloc[-1]
            current_thresh = threshold_series.iloc[-1]
            current_m = m_norm.iloc[-1]

            if (current_aqm > current_thresh) and \
               (current_m < h3_m_sq_threshold) and \
               (current_aqm > h3_min_score):
               
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
                        notes=f"AQM H3 Live (Adapted). Score:{current_aqm:.2f}. J:{last_candle['J']:.2f}. VIX:{market_conditions['vix']:.1f}"
                    )
                    session.add(new_signal)
                    session.commit()
                    signals_generated += 1
                    
                    msg = (f"⚛️ H3 QUANTUM SIGNAL (ADAPTIVE): {ticker}\n"
                           f"Cena: {ref_price:.2f} | Live: {current_live_price if current_live_price else '---'}\n"
                           f"TP: {take_profit:.2f} | SL: {stop_loss:.2f}\n"
                           f"VIX Ref: {market_conditions['vix']:.1f}")
                    send_telegram_alert(msg)

        except Exception as e:
            logger.error(f"Błąd H3 Live dla {ticker}: {e}", exc_info=True)
            session.rollback()
            continue

    update_scan_progress(session, total_candidates, total_candidates)
    append_scan_log(session, f"Faza 3 (H3 Live Adaptive) zakończona. Wygenerowano {signals_generated} sygnałów.")
    logger.info(f"Faza 3 zakończona. Sygnałów: {signals_generated}")
