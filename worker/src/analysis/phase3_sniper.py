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
    get_system_control_value,
    calculate_h3_metrics_v4, 
    calculate_retail_herding_capped_v4
)
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands
from .apex_optimizer import AdaptiveExecutor
from ..config import SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

DEFAULT_PARAMS = {
    'h3_percentile': 0.95,
    'h3_m_sq_threshold': -0.5,
    'h3_min_score': 0.0,
    'h3_tp_multiplier': 5.0,
    'h3_sl_multiplier': 2.0,
    'h3_max_hold': 5 # Domyślny czas życia (dni robocze)
}

H3_CALC_WINDOW = 100 
REQUIRED_HISTORY_SIZE = 201 

def _is_setup_still_valid(entry_price: float, stop_loss: float, take_profit: float, current_price: float) -> Tuple[bool, str]:
    try:
        if current_price <= stop_loss:
            return False, f"Cena {current_price:.2f} poniżej SL {stop_loss:.2f} (Spalony)"
        if current_price >= take_profit:
            return False, f"Cena {current_price:.2f} powyżej TP {take_profit:.2f} (Zrealizowany)"
        if current_price > entry_price:
            potential_profit = take_profit - current_price
            potential_risk = current_price - stop_loss
            if potential_risk <= 0: return False, "Błąd ryzyka"
            current_rr = potential_profit / potential_risk
            if current_rr < 1.3:
                return False, f"R:R spadł do {current_rr:.2f}"
        return True, "OK"
    except Exception as e:
        return False, f"Błąd walidacji: {e}"

def _get_historical_ev_stats(session: Session) -> Dict[str, Dict[str, float]]:
    try:
        trades = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.status.in_(['CLOSED_TP', 'CLOSED_SL', 'CLOSED_EXPIRED']),
            models.VirtualTrade.metric_aqm_score_h3.isnot(None)
        ).limit(500).all()

        if len(trades) < 10: return {}

        data = []
        for t in trades:
            if t.metric_aqm_score_h3 is None or t.metric_aqm_percentile_95 is None or t.final_profit_loss_percent is None: continue
            power = float(t.metric_aqm_score_h3) - float(t.metric_aqm_percentile_95)
            data.append({'pl': float(t.final_profit_loss_percent), 'power': power})
        
        if not data: return {}
        df = pd.DataFrame(data)
        
        buckets = {
            'LOW': df[(df['power'] >= 0) & (df['power'] < 0.2)],
            'MID': df[(df['power'] >= 0.2) & (df['power'] < 0.5)],
            'HIGH': df[df['power'] >= 0.5]
        }
        
        stats = {}
        for name, subset in buckets.items():
            if len(subset) < 5: subset = df 
            wins = subset[subset['pl'] > 0]
            losses = subset[subset['pl'] <= 0]
            win_rate = len(wins) / len(subset) if len(subset) > 0 else 0
            avg_win = wins['pl'].mean() if not wins.empty else 0
            avg_loss = abs(losses['pl'].mean()) if not losses.empty else 0
            ev = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
            stats[name] = {'ev': ev}
        return stats
    except: return {}

def _calculate_setup_score(aqm_score, aqm_thresh, mass_sq, ticker_df, spy_df, sector_trend):
    score = 0
    details = {}
    
    surplus = aqm_score - aqm_thresh
    tech = 0 if surplus <= 0 else 10 + min(30, (surplus/0.5)*30)
    score += tech; details['tech_score'] = int(tech)
    
    mkt = 0
    if not spy_df.empty:
        spy_curr = spy_df['close'].iloc[-1]
        spy_ma = spy_df['close'].rolling(200).mean().iloc[-1]
        if not pd.isna(spy_ma) and spy_curr > spy_ma: mkt += 15
    if sector_trend > 0: mkt += 15
    score += mkt; details['market_score'] = int(mkt)
    
    rs = 0
    if not spy_df.empty and len(ticker_df) > 5:
        t_ch = ticker_df['close'].pct_change(5).iloc[-1]
        s_ch = spy_df['close'].pct_change(5).iloc[-1]
        if not pd.isna(t_ch) and not pd.isna(s_ch):
            if t_ch > s_ch: rs += 10
            elif t_ch > 0 and s_ch < 0: rs += 10
    score += rs; details['rs_score'] = int(rs)
    
    ctx = 0
    if mass_sq < -1.0: ctx = 10
    elif mass_sq < -0.5: ctx = 5
    score += ctx; details['context_score'] = int(ctx)
    
    return int(score), details

def _get_market_pkg(session, client):
    pkg = {'vix': 20.0, 'trend': 'NEUTRAL', 'spy_df': pd.DataFrame(), 'macro': 'UNKNOWN'}
    try:
        pkg['macro'] = get_system_control_value(session, 'macro_sentiment') or "UNKNOWN"
        spy = get_raw_data_with_cache(session, client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=24)
        if spy:
            df = standardize_df_columns(pd.DataFrame.from_dict(spy.get('Time Series (Daily)', {}), orient='index'))
            df.index = pd.to_datetime(df.index); df.sort_index(inplace=True)
            pkg['spy_df'] = df
            if len(df) > 200:
                pkg['vix'] = float(df['close'].pct_change().tail(30).std() * (252**0.5) * 100)
                pkg['trend'] = 'BULL' if df['close'].iloc[-1] > df['close'].rolling(200).mean().iloc[-1] else 'BEAR'
        if "RISK_OFF" in pkg['macro']: pkg['vix'] = max(pkg['vix'], 30.0)
    except: pass
    return pkg

def _get_sector_trend(session, ticker):
    try:
        res = session.execute(text("SELECT sector_trend_score FROM phase1_candidates WHERE ticker=:t"), {'t': ticker}).fetchone()
        return float(res[0]) if res and res[0] is not None else 0.0
    except: return 0.0

def run_h3_live_scan(session, candidates, client, parameters=None):
    logger.info("Start H3 Live Sniper (V6 TTL Logic)...")
    base_params = DEFAULT_PARAMS.copy()
    if parameters:
        for k,v in parameters.items(): 
            if v is not None: base_params[k] = float(v)
    
    # Pobranie Max Hold do obliczenia daty wygaśnięcia
    max_hold_days = int(base_params.get('h3_max_hold', 5))
            
    mkt = _get_market_pkg(session, client)
    ev_model = _get_historical_ev_stats(session)
    
    executor = AdaptiveExecutor(base_params)
    adapted = executor.get_adapted_params({'vix': mkt['vix'], 'trend': mkt['trend']})
    
    h3_p = float(adapted['h3_percentile'])
    h3_m = float(adapted['h3_m_sq_threshold'])
    h3_min = float(adapted['h3_min_score'])
    tp_mult = float(adapted['h3_tp_multiplier'])
    sl_mult = float(adapted['h3_sl_multiplier'])
    
    chg = [f"{k}: {base_params.get(k)}->{v:.3f}" for k,v in adapted.items() if base_params.get(k)!=v]
    if chg: append_scan_log(session, f"ADAPTACJA (VIX {mkt['vix']:.1f}): {', '.join(chg)}")
    
    signals = 0
    rejects = {'aqm':0, 'mass':0, 'data':0, 'live':0}
    
    for i, ticker in enumerate(candidates):
        if i%5==0: update_scan_progress(session, i, len(candidates))
        
        try:
            d_raw = get_raw_data_with_cache(session, client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12)
            da_raw = get_raw_data_with_cache(session, client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12)
            if not d_raw or not da_raw:
                rejects['data']+=1; append_scan_log(session, f"❌ {ticker}: Brak danych"); continue
                
            h2 = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, client, session)
            
            ohlcv = standardize_df_columns(pd.DataFrame.from_dict(d_raw.get('Time Series (Daily)', {}), orient='index'))
            adj = standardize_df_columns(pd.DataFrame.from_dict(da_raw.get('Time Series (Daily)', {}), orient='index'))
            ohlcv.index = pd.to_datetime(ohlcv.index); adj.index = pd.to_datetime(adj.index)
            
            if len(adj) < REQUIRED_HISTORY_SIZE:
                rejects['data']+=1; append_scan_log(session, f"❌ {ticker}: Krótka historia"); continue
                
            ohlcv['vwap_proxy'] = (ohlcv['high']+ohlcv['low']+ohlcv['close'])/3.0
            df = adj.join(ohlcv[['open','high','low','vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df).ffill().fillna(0)
            df['close'] = df[close_col]
            
            insider = h2.get('insider_df')
            news = h2.get('news_df')
            df['institutional_sync'] = df.apply(lambda r: aqm_v3_metrics.calculate_institutional_sync_from_data(insider, r.name), axis=1)
            df['retail_herding'] = df.apply(lambda r: aqm_v3_metrics.calculate_retail_herding_from_data(news, r.name), axis=1)
            
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(30).std()
            
            if not news.empty:
                nc = news.groupby(news.index.date).size()
                nc.index = pd.to_datetime(nc.index)
                nc = nc.reindex(df.index, fill_value=0)
                df['information_entropy'] = nc.rolling(10).sum()
            else: df['information_entropy'] = 0.0
            
            df['avg_volume_10d'] = df['volume'].rolling(10).mean()
            df['vol_mean_200d'] = df['avg_volume_10d'].rolling(200).mean()
            df['vol_std_200d'] = df['avg_volume_10d'].rolling(200).std()
            df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).fillna(0)
            
            df['normalized_news'] = 0.0 
            df['m_sq'] = df['normalized_volume'] + df['normalized_news']
            df['nabla_sq'] = df['price_gravity']
            
            df['mu_normalized'] = (df['institutional_sync'] - df['institutional_sync'].rolling(100).mean()) / df['institutional_sync'].rolling(100).std().fillna(1)
            
            df['retail_herding_capped'] = calculate_retail_herding_capped_v4(df['retail_herding'])
            
            S = df['information_entropy']
            Q = df['retail_herding_capped']
            T = df['market_temperature']
            mu = df['mu_normalized'].fillna(0)
            
            df['J'] = (S - (Q/T.replace(0, np.nan)) + (mu*1.0)).fillna(0)
            
            j_norm = ((df['J'] - df['J'].rolling(100).mean()) / df['J'].rolling(100).std()).fillna(0)
            nabla_norm = ((df['nabla_sq'] - df['nabla_sq'].rolling(100).mean()) / df['nabla_sq'].rolling(100).std()).fillna(0)
            
            m_mean = df['m_sq'].rolling(100).mean()
            m_std = df['m_sq'].rolling(100).std()
            m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)
            
            aqm_score = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
            thresh = aqm_score.rolling(100).quantile(h3_p)
            
            curr_aqm = aqm_score.iloc[-1]
            curr_thr = thresh.iloc[-1]
            curr_m = m_norm.iloc[-1]
            
            if curr_aqm <= curr_thr or curr_aqm <= h3_min:
                rejects['aqm']+=1; append_scan_log(session, f"❌ {ticker}: AQM {curr_aqm:.2f} < {curr_thr:.2f}"); continue
            if curr_m >= h3_m:
                rejects['mass']+=1; append_scan_log(session, f"❌ {ticker}: Masa {curr_m:.2f} > {h3_m:.2f}"); continue
                
            st = _get_sector_trend(session, ticker)
            score, det = _calculate_setup_score(curr_aqm, curr_thr, curr_m, df, mkt['spy_df'], st)
            surplus = curr_aqm - curr_thr
            ev_b = 'LOW' if surplus < 0.2 else ('MID' if surplus < 0.5 else 'HIGH')
            ev = ev_model.get(ev_b, {'ev': surplus*2})['ev']
            
            last = df.iloc[-1]
            entry = last['close'] 
            tp = entry + (tp_mult * last['atr_14'])
            sl = entry - (sl_mult * last['atr_14'])
            
            lq = client.get_global_quote(ticker)
            lp = safe_float(lq.get('05. price')) if lq else None
            
            if lp:
                valid, msg = _is_setup_still_valid(entry, sl, tp, lp)
                if not valid:
                    rejects['live']+=1; append_scan_log(session, f"❌ {ticker}: Live: {msg}"); continue
            
            rec = "HOLD"
            if score >= 80: rec = "TOP 💎"
            elif score >= 60: rec = "BUY ✅"
            elif score >= 40: rec = "MOD ⚠️"
            else: rec = "WEAK ❌"
            
            note = f"RANKING:\nEV: {ev:.2f}% | SCORE: {score}/100 | {rec}\nDETALE: Tech:{det['tech_score']} Mkt:{det['market_score']} RS:{det['rs_score']} Ctx:{det['context_score']}\nAQM:{curr_aqm:.2f} (vs {curr_thr:.2f})"
            
            ex = session.query(models.TradingSignal).filter(models.TradingSignal.ticker==ticker, models.TradingSignal.status.in_(['ACTIVE','PENDING'])).first()
            if not ex:
                # === OBLICZANIE DATY WYGAŚNIĘCIA (TTL) ===
                expiration_dt = datetime.now(timezone.utc) + timedelta(days=max_hold_days)
                
                sig = models.TradingSignal(
                    ticker=ticker, status='PENDING', generation_date=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
                    signal_candle_timestamp=last.name, entry_price=entry, stop_loss=sl, take_profit=tp,
                    entry_zone_top=entry+(0.5*last['atr_14']), entry_zone_bottom=entry-(0.5*last['atr_14']),
                    risk_reward_ratio=tp_mult/sl_mult, notes=note,
                    expiration_date=expiration_dt # Zapisz datę wygaśnięcia
                )
                session.add(sig); session.commit()
                signals+=1
                msg = f"💎 SYGNAŁ: {ticker} | EV: {ev:.1f}% | SCORE: {score} | {rec}"
                logger.info(msg); append_scan_log(session, msg)
                send_telegram_alert(f"⚛️ H3: {ticker}\nCena: {entry:.2f}\nSCORE: {score}")
            else:
                append_scan_log(session, f"ℹ️ {ticker}: Już aktywny.")
                
        except Exception as e:
            logger.error(f"Error {ticker}: {e}")
            continue
            
    update_scan_progress(session, len(candidates), len(candidates))
    sum_msg = f"🏁 Faza 3: Sygnałów: {signals}. Odrzuty: AQM={rejects['aqm']}, Masa={rejects['mass']}, Live={rejects['live']}"
    append_scan_log(session, sum_msg)
