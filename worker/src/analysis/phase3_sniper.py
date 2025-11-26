import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple

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
    get_system_control_value
)
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands
from .apex_optimizer import AdaptiveExecutor
from ..config import SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

# ============================================================================
# === STAŁE DOMYŚLNE ===
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

# ============================================================================
# === NARZĘDZIE 1: LIVE GUARD (STRAŻNIK) - PRZYWRÓCONY ===
# ============================================================================

def _is_setup_still_valid(entry_price: float, stop_loss: float, take_profit: float, current_price: float) -> Tuple[bool, str]:
    """
    Weryfikuje czy setup jest nadal ważny w kontekście aktualnej ceny (Live).
    Zabezpiecza przed wejściem w "uciekający pociąg" lub po spalonym SL.
    """
    try:
        # 1. Sprawdź czy cena nie przebiła SL
        if current_price <= stop_loss:
            return False, f"Cena {current_price:.2f} poniżej SL {stop_loss:.2f} (Spalony)"
        
        # 2. Sprawdź czy cena nie osiągnęła już TP
        if current_price >= take_profit:
            return False, f"Cena {current_price:.2f} powyżej TP {take_profit:.2f} (Zrealizowany)"
            
        # 3. Sprawdź R:R (Risk:Reward) przy obecnej cenie
        if current_price > entry_price:
            potential_profit = take_profit - current_price
            potential_risk = current_price - stop_loss
            
            if potential_risk <= 0: return False, "Błąd wyliczenia ryzyka (Cena <= SL)"
            
            current_rr = potential_profit / potential_risk
            
            # Minimalny akceptowalny R:R dla spóźnionego wejścia
            MIN_LIVE_RR = 1.3 
            
            if current_rr < MIN_LIVE_RR:
                return False, f"R:R spadł do {current_rr:.2f} (Cena uciekła do {current_price:.2f})"
                
        return True, "OK"
        
    except Exception as e:
        return False, f"Błąd walidacji live: {e}"

# ============================================================================
# === NARZĘDZIE 2: SILNIK RANKINGOWY (EV + SCORE) ===
# ============================================================================

def _get_historical_ev_stats(session: Session) -> Dict[str, Dict[str, float]]:
    """Analizuje historię VirtualTrades dla EV."""
    try:
        trades = session.query(
            models.VirtualTrade.final_profit_loss_percent,
            models.VirtualTrade.metric_aqm_score_h3,
            models.VirtualTrade.metric_aqm_percentile_95
        ).filter(
            models.VirtualTrade.status.in_(['CLOSED_TP', 'CLOSED_SL', 'CLOSED_EXPIRED']),
            models.VirtualTrade.final_profit_loss_percent.isnot(None),
            models.VirtualTrade.metric_aqm_score_h3.isnot(None)
        ).all()

        if len(trades) < 10: return {}

        data = []
        for t in trades:
            power_surplus = float(t.metric_aqm_score_h3) - float(t.metric_aqm_percentile_95)
            data.append({'pl': float(t.final_profit_loss_percent), 'power': power_surplus})
        
        df = pd.DataFrame(data)
        
        buckets = {
            'LOW': df[(df['power'] >= 0) & (df['power'] < 0.2)],
            'MID': df[(df['power'] >= 0.2) & (df['power'] < 0.5)],
            'HIGH': df[df['power'] >= 0.5]
        }
        
        stats = {}
        for bucket_name, subset in buckets.items():
            if len(subset) < 5: subset = df 
            wins = subset[subset['pl'] > 0]
            losses = subset[subset['pl'] <= 0]
            win_rate = len(wins) / len(subset) if len(subset) > 0 else 0
            avg_win = wins['pl'].mean() if not wins.empty else 0
            avg_loss = abs(losses['pl'].mean()) if not losses.empty else 0
            ev = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
            stats[bucket_name] = {'ev': ev, 'win_rate': win_rate}
            
        return stats
    except Exception as e:
        logger.warning(f"Błąd EV: {e}")
        return {}

def _calculate_setup_score(aqm_score, aqm_threshold, mass_sq, ticker_df, spy_df, sector_trend_score):
    """Oblicza punktowy wynik jakości setupu (0-100)."""
    score = 0
    details = {}

    surplus = aqm_score - aqm_threshold
    tech_score = 0 if surplus <= 0 else 10 + min(30, (surplus / 0.5) * 30)
    score += tech_score
    details['tech_score'] = int(tech_score)

    market_score = 0
    if not spy_df.empty:
        spy_price = spy_df['close'].iloc[-1]
        spy_sma200 = spy_df['close'].rolling(200).mean().iloc[-1]
        if not pd.isna(spy_sma200) and spy_price > spy_sma200: market_score += 15
    if sector_trend_score > 0: market_score += 15
    score += market_score
    details['market_score'] = market_score

    rs_score = 0
    if not spy_df.empty and len(ticker_df) > 5 and len(spy_df) > 5:
        ticker_5d = ticker_df['close'].pct_change(5).iloc[-1]
        spy_5d = spy_df['close'].pct_change(5).iloc[-1]
        if not pd.isna(ticker_5d) and not pd.isna(spy_5d):
            if ticker_5d > spy_5d: rs_score += 10
            if ticker_5d > 0 and spy_5d < 0: rs_score += 10
            elif ticker_5d > (spy_5d * 1.5): rs_score += 5
    score += rs_score
    details['rs_score'] = rs_score

    setup_context_score = 0
    if mass_sq < -1.0: setup_context_score = 10
    elif mass_sq < -0.5: setup_context_score = 5
    score += setup_context_score
    details['context_score'] = setup_context_score

    return int(score), details

# ============================================================================
# === FUNKCJE POMOCNICZE FAZY 3 ===
# ============================================================================

def _get_market_data_package(session: Session, api_client: AlphaVantageClient) -> Dict[str, Any]:
    package = {'vix': 20.0, 'trend': 'NEUTRAL', 'spy_df': pd.DataFrame(), 'macro': 'UNKNOWN'}
    try:
        package['macro'] = get_system_control_value(session, 'macro_sentiment') or "UNKNOWN"
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=24, outputsize='full')
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)
            spy_df.sort_index(inplace=True)
            package['spy_df'] = spy_df
            if len(spy_df) > 200:
                recent_returns = spy_df['close'].pct_change().tail(30)
                vix_proxy = recent_returns.std() * (252 ** 0.5) * 100
                package['vix'] = float(vix_proxy) if not pd.isna(vix_proxy) else 20.0
                current_price = spy_df['close'].iloc[-1]
                sma_200 = spy_df['close'].rolling(window=200).mean().iloc[-1]
                package['trend'] = 'BULL' if current_price > sma_200 else 'BEAR'
        if "RISK_OFF" in package['macro']: package['vix'] = max(package['vix'], 30.0)
        return package
    except Exception: return package

def _get_sector_trend_from_cache(session: Session, ticker: str) -> float:
    try:
        result = session.execute(text("SELECT sector_trend_score FROM phase1_candidates WHERE ticker = :ticker"), {'ticker': ticker}).fetchone()
        return float(result[0]) if result and result[0] is not None else 0.0
    except: return 0.0

# ============================================================================
# === GŁÓWNA FUNKCJA SKANOWANIA ===
# ============================================================================

def run_h3_live_scan(session: Session, candidates: List[str], api_client: AlphaVantageClient, parameters: Dict[str, Any] = None):
    logger.info("Uruchamianie Fazy 3: H3 LIVE SNIPER (Adaptive + Ranking EV/Score)...")
    
    base_params = DEFAULT_PARAMS.copy()
    if parameters:
        for k, v in parameters.items():
            if v is not None: base_params[k] = float(v)

    market_pkg = _get_market_data_package(session, api_client)
    ev_model = _get_historical_ev_stats(session)
    
    executor = AdaptiveExecutor(base_params)
    market_conditions = {'vix': market_pkg['vix'], 'trend': market_pkg['trend']}
    adapted_params = executor.get_adapted_params(market_conditions)
    
    changes_log = [f"{k}: {base_params.get(k)}->{v:.3f}" for k, v in adapted_params.items() if base_params.get(k) != v]
    if changes_log: append_scan_log(session, f"ADAPTACJA (VIX {market_pkg['vix']:.1f}): " + ", ".join(changes_log))
    
    h3_percentile = float(adapted_params['h3_percentile'])
    h3_m_sq_threshold = float(adapted_params['h3_m_sq_threshold'])
    h3_min_score = float(adapted_params['h3_min_score'])
    h3_tp_mult = float(adapted_params['h3_tp_multiplier'])
    h3_sl_mult = float(adapted_params['h3_sl_multiplier'])
    h3_max_hold = int(adapted_params.get('h3_max_hold', 10)) # Upewniamy się, że jest max_hold
    
    signals_generated = 0
    total_candidates = len(candidates)
    reject_stats = {'aqm_low': 0, 'm_sq_high': 0, 'history': 0, 'data_error': 0, 'validation': 0}

    for i, ticker in enumerate(candidates):
        if i % 5 == 0: update_scan_progress(session, i, total_candidates)
        
        try:
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12, outputsize='full')
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12, outputsize='full')
            
            if not daily_raw or not daily_adj_raw: 
                reject_stats['data_error'] += 1; append_scan_log(session, f"❌ {ticker}: Brak danych."); continue
            
            h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')

            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj.index = pd.to_datetime(daily_adj.index)

            if len(daily_adj) < REQUIRED_HISTORY_SIZE: 
                reject_stats['history'] += 1; append_scan_log(session, f"❌ {ticker}: Krótka historia."); continue

            daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
            df['close'] = df[close_col]

            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            df['nabla_sq'] = df['price_gravity']
            
            df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
            df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
            df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
            df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).fillna(0)
            
            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(df.index, fill_value=0)
                df['information_entropy'] = news_counts.rolling(window=10).sum()
                # === FIX: PRZYWRÓCENIE LOGIKI "222 SETUPY" ===
                # W oryginalnym (udanym) backteście V4/V3, normalized_news było zerowane dla obliczeń MASY (m^2).
                # Newsy wchodziły tylko do 'information_entropy' (J).
                # Wprowadzenie normalized_news do m_sq w V5 podbiło masę i zabiło sygnały.
                df['normalized_news'] = 0.0
            else:
                df['information_entropy'] = 0.0; df['normalized_news'] = 0.0
            
            df['m_sq'] = df['normalized_volume'] + df['normalized_news']

            df['mu_normalized'] = (df['institutional_sync'] - df['institutional_sync'].rolling(H3_CALC_WINDOW).mean()) / df['institutional_sync'].rolling(H3_CALC_WINDOW).std()
            df['mu_normalized'] = df['mu_normalized'].fillna(0)
            df['retail_herding_capped'] = df['retail_herding'].clip(-1.0, 1.0)
            
            S = df['information_entropy']
            Q = df['retail_herding_capped']
            T = df['market_temperature']
            mu_norm = df['mu_normalized']
            
            df['J'] = (S - (Q / T.replace(0, np.nan)) + (mu_norm * 1.0)).fillna(0)

            j_mean = df['J'].rolling(window=H3_CALC_WINDOW).mean()
            j_std = df['J'].rolling(window=H3_CALC_WINDOW).std()
            j_norm = ((df['J'] - j_mean) / j_std).fillna(0)
            
            nabla_mean = df['nabla_sq'].rolling(window=H3_CALC_WINDOW).mean()
            nabla_std = df['nabla_sq'].rolling(window=H3_CALC_WINDOW).std()
            nabla_norm = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
            
            m_mean = df['m_sq'].rolling(window=H3_CALC_WINDOW).mean()
            m_std = df['m_sq'].rolling(window=H3_CALC_WINDOW).std()
            m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)
            
            aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
            threshold_series = aqm_score_series.rolling(window=H3_CALC_WINDOW).quantile(h3_percentile)

            last_candle = df.iloc[-1]
            current_aqm = aqm_score_series.iloc[-1]
            current_thresh = threshold_series.iloc[-1]
            current_m = m_norm.iloc[-1]

            is_score_good = (current_aqm > current_thresh) and (current_aqm > h3_min_score)
            is_mass_good = (current_m < h3_m_sq_threshold)
            
            if not is_score_good:
                reject_stats['aqm_low'] += 1; append_scan_log(session, f"❌ {ticker}: AQM {current_aqm:.2f} < {current_thresh:.2f}"); continue
            if not is_mass_good:
                reject_stats['m_sq_high'] += 1; append_scan_log(session, f"❌ {ticker}: Masa {current_m:.2f} > {h3_m_sq_threshold:.2f}"); continue

            sector_trend = _get_sector_trend_from_cache(session, ticker)
            setup_score, score_details = _calculate_setup_score(current_aqm, current_thresh, current_m, df, market_pkg['spy_df'], sector_trend)
            
            surplus = current_aqm - current_thresh
            ev_bucket = 'LOW' if surplus < 0.2 else ('MID' if surplus < 0.5 else 'HIGH')
            ev_stats = ev_model.get(ev_bucket, {'ev': 0.0})
            calculated_ev = ev_stats['ev'] if ev_model else (surplus * 2.0)

            atr = last_candle['atr_14']
            ref_price = last_candle['close']
            take_profit = ref_price + (h3_tp_mult * atr)
            stop_loss = ref_price - (h3_sl_mult * atr)
            entry_price = ref_price

            current_live_quote = api_client.get_global_quote(ticker)
            current_live_price = safe_float(current_live_quote.get('05. price')) if current_live_quote else None
            
            valid_status = True
            valid_reason = ""
            if current_live_price:
                # Teraz funkcja jest zdefiniowana i dostępna!
                valid_status, valid_reason = _is_setup_still_valid(entry_price, stop_loss, take_profit, current_live_price)

            if not valid_status:
                reject_stats['validation'] += 1; append_scan_log(session, f"❌ {ticker}: Live Error: {valid_reason}"); continue

            recommendation = "HOLD"
            if setup_score >= 80: recommendation = "TOP_PICK 💎"
            elif setup_score >= 60: recommendation = "STRONG_BUY ✅"
            elif setup_score >= 40: recommendation = "MODERATE ⚠️"
            else: recommendation = "WEAK ❌"

            ranking_note = (
                f"RANKING:\n"
                f"EV: {calculated_ev:+.2f}% | SCORE: {setup_score}/100 | REKOMENDACJA: {recommendation}\n"
                f"DETALE: Tech:{score_details['tech_score']} Mkt:{score_details['market_score']} "
                f"RS:{score_details['rs_score']} Ctx:{score_details['context_score']}\n"
                f"AQM:{current_aqm:.2f} (vs {current_thresh:.2f})"
            )

            existing = session.query(models.TradingSignal).filter(models.TradingSignal.ticker == ticker, models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])).first()
            if not existing:
                new_signal = models.TradingSignal(
                    ticker=ticker, status='PENDING', generation_date=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
                    signal_candle_timestamp=last_candle.name, entry_price=float(entry_price), stop_loss=float(stop_loss), take_profit=float(take_profit),
                    entry_zone_top=float(ref_price + (0.5 * atr)), entry_zone_bottom=float(ref_price - (0.5 * atr)), risk_reward_ratio=float(h3_tp_mult/h3_sl_mult),
                    notes=ranking_note
                )
                session.add(new_signal)
                session.commit()
                signals_generated += 1
                success_msg = (f"💎 SYGNAŁ: {ticker} | EV: {calculated_ev:+.1f}% | SCORE: {setup_score} | {recommendation}")
                logger.info(success_msg)
                append_scan_log(session, success_msg)
                send_telegram_alert(f"⚛️ H3 SIGNAL: {ticker}\nCena: {ref_price:.2f}\nSCORE: {setup_score} ({recommendation})")
            else:
                append_scan_log(session, f"ℹ️ {ticker}: Sygnał już aktywny.")

        except Exception as e:
            logger.error(f"Błąd H3 Live dla {ticker}: {e}", exc_info=True)
            append_scan_log(session, f"⛔ BŁĄD dla {ticker}: {e}")
            session.rollback()
            continue

    update_scan_progress(session, total_candidates, total_candidates)
    summary_msg = (f"🏁 Faza 3 zakończona. Sygnałów: {signals_generated}. "
                   f"Odrzuty: AQM={reject_stats['aqm_low']}, Masa={reject_stats['m_sq_high']}, Live={reject_stats['validation']}")
    append_scan_log(session, summary_msg)
    logger.info(f"Faza 3 zakończona. Sygnałów: {signals_generated}")
