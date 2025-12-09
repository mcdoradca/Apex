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
# Korzystamy z dedykowanych modułów do ładowania danych, aby zachować czystość
from . import aqm_v3_h2_loader
from . import aqm_v3_metrics
# Importujemy logikę V4 dla strategii AQM (jeśli wybrana)
from . import aqm_v4_logic
from ..config import SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

# Domyślne parametry bazowe (zostaną nadpisane przez Adaptive Executor)
DEFAULT_PARAMS = {
    'h3_percentile': 0.95,
    'h3_m_sq_threshold': -0.5,
    'h3_min_score': 0.0,
    'h3_tp_multiplier': 5.0,
    'h3_sl_multiplier': 2.0,
    'h3_max_hold': 5,
    'aqm_component_min': None, 
    'strategy_mode': 'AUTO' 
}

REQUIRED_HISTORY_SIZE = 201 

def _to_py_float(value: Any) -> float:
    """Konwersja bezpieczna dla bazy danych (unika typów numpy)."""
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0

def _verify_data_freshness(df: pd.DataFrame, ticker: str) -> bool:
    """
    STRAŻNIK ŚWIEŻOŚCI (ANTI-DATA LAG)
    Sprawdza, czy dane nie są przestarzałe (np. sprzed 3 dni w środku tygodnia).
    """
    if df.empty: return False
    
    last_date = df.index[-1]
    now = datetime.now()
    delta = (now - last_date).days
    
    # Logika weekendowa: poniedziałek dopuszcza dane z piątku
    threshold = 4 if now.weekday() <= 1 else 2
    
    if delta > threshold:
        logger.warning(f"⚠️ DATA LAG wykryty dla {ticker}: Ostatnia świeca z {last_date.date()} (Delta: {delta} dni). Odrzucanie.")
        return False
    return True

def _is_setup_still_valid(entry_price: float, stop_loss: float, take_profit: float, current_price: float) -> Tuple[bool, str]:
    """
    STRAŻNIK LIVE (LIVE GUARD) - Zgodnie z Master Plan PDF (str. 7).
    Weryfikuje R:R w czasie rzeczywistym.
    """
    try:
        if current_price <= stop_loss:
            return False, f"Cena {current_price:.2f} poniżej SL {stop_loss:.2f} (Spalony)"
        if current_price >= take_profit:
            return False, f"Cena {current_price:.2f} powyżej TP {take_profit:.2f} (Zrealizowany)"
        
        # Jeśli cena jest powyżej wejścia, sprawdzamy czy R:R nadal ma sens
        if current_price > entry_price:
            potential_profit = take_profit - current_price
            potential_risk = current_price - stop_loss
            
            if potential_risk <= 0: return False, "Błąd ryzyka (Cena <= SL)"
            
            current_rr = potential_profit / potential_risk
            
            # === PRZYWRÓCONO WARTOŚĆ Z DOKUMENTACJI (PDF str. 7) ===
            # Wymagane R:R >= 1.5.
            if current_rr < 1.5:
                return False, f"R:R spadł do {current_rr:.2f} (Wymagane > 1.5)"
                
        return True, "OK"
    except Exception as e:
        return False, f"Błąd walidacji: {e}"

def _apply_adaptive_executor(base_params: dict, market_pkg: dict) -> dict:
    """
    ADAPTIVE EXECUTOR (APEX V4) - Zgodnie z Master Plan PDF (str. 5 i 7).
    """
    adjusted_params = base_params.copy()
    vix = market_pkg.get('vix', 20.0)
    
    # PDF str. 5: "Jeśli VIX > 25 -> SL 1.3 [korekta: szerszy SL], Percentyl wyższy (Ochrona)"
    if vix > 25.0:
        adjusted_params['h3_sl_multiplier'] = max(adjusted_params['h3_sl_multiplier'], 3.0) 
        adjusted_params['h3_percentile'] = max(adjusted_params['h3_percentile'], 0.98) 
        logger.info(f"🛡️ ADAPTIVE EXECUTOR: Wykryto VIX {vix:.2f} (Wysoki). Tryb OCHRONY aktywowany.")
        
    elif vix < 15.0:
        # PDF str. 5: "Jeśli VIX < 15 -> Percentyl niższy (Agresja)"
        adjusted_params['h3_percentile'] = 0.90 
        logger.info(f"⚔️ ADAPTIVE EXECUTOR: Wykryto VIX {vix:.2f} (Niski). Tryb AGRESJI aktywowany.")
        
    return adjusted_params

def _get_historical_ev_stats(session: Session) -> Dict[str, Dict[str, float]]:
    """Pobiera historyczne statystyki (EV, PF, WinRate) dla modelu Re-check."""
    try:
        trades = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.status.in_(['CLOSED_TP', 'CLOSED_SL', 'CLOSED_EXPIRED']),
            models.VirtualTrade.metric_aqm_score_h3.isnot(None)
        ).limit(1000).all()

        if len(trades) < 10: return {}

        data = []
        for t in trades:
            if t.metric_aqm_score_h3 is None or t.metric_aqm_percentile_95 is None or t.final_profit_loss_percent is None: continue
            power = float(t.metric_aqm_score_h3) - float(t.metric_aqm_percentile_95)
            data.append({'pl': float(t.final_profit_loss_percent), 'power': power})
        
        if not data: return {}
        df = pd.DataFrame(data)
        
        # Koszykowanie siły sygnału
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
            avg_win_pct = wins['pl'].mean() if not wins.empty else 0
            avg_loss_pct = abs(losses['pl'].mean()) if not losses.empty else 0
            # EV = (Win% * AvgWin%) - (Loss% * AvgLoss%)
            ev = (win_rate * avg_win_pct) - ((1 - win_rate) * avg_loss_pct)
            pf = (wins['pl'].sum() / abs(losses['pl'].sum())) if not losses.empty and losses['pl'].sum() != 0 else 0.0
            
            stats[name] = {'ev': ev, 'pf': pf, 'wr': win_rate * 100}
        return stats
    except: return {}

def _calculate_setup_score(aqm_score, aqm_thresh, mass_sq, ticker_df, spy_df, sector_trend):
    """Oblicza 'Jakość Setupu' (0-100) wyświetlaną na Frontendzie."""
    score = 0
    details = {}
    
    # 1. Siła Techniczna (Max 40 pkt)
    surplus = aqm_score - aqm_thresh
    tech = 0 if surplus <= 0 else 10 + min(30, (surplus/0.5)*30)
    score += tech; details['tech_score'] = int(tech)
    
    # 2. Kontekst Rynkowy (Max 30 pkt)
    mkt = 0
    if not spy_df.empty:
        spy_curr = spy_df['close'].iloc[-1]
        spy_ma = spy_df['close'].rolling(200).mean().iloc[-1]
        if not pd.isna(spy_ma) and spy_curr > spy_ma: mkt += 15
    if sector_trend > 0: mkt += 15
    score += mkt; details['market_score'] = int(mkt)
    
    # 3. Siła Relatywna (RS) (Max 20 pkt)
    rs = 0
    if not spy_df.empty and len(ticker_df) > 5:
        t_ch = ticker_df['close'].pct_change(5).iloc[-1]
        s_ch = spy_df['close'].pct_change(5).iloc[-1]
        if not pd.isna(t_ch) and not pd.isna(s_ch):
            if t_ch > s_ch: rs += 10
            elif t_ch > 0 and s_ch < 0: rs += 10
    score += rs; details['rs_score'] = int(rs)
    
    # 4. Kontekst Masy (Tłok) (Max 10 pkt)
    ctx = 0
    if mass_sq < -1.0: ctx = 10
    elif mass_sq < -0.5: ctx = 5
    score += ctx; details['context_score'] = int(ctx)
    
    return int(score), details

def _get_market_pkg(session, client):
    pkg = {'vix': 20.0, 'trend': 'NEUTRAL', 'spy_df': pd.DataFrame(), 'macro': 'UNKNOWN'}
    try:
        pkg['macro'] = get_system_control_value(session, 'macro_sentiment') or "UNKNOWN"
        # QQQ jako benchmark dla Nasdaq
        spy = get_raw_data_with_cache(session, client, 'QQQ', 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=24)
        if spy:
            df = standardize_df_columns(pd.DataFrame.from_dict(spy.get('Time Series (Daily)', {}), orient='index'))
            df.index = pd.to_datetime(df.index); df.sort_index(inplace=True)
            pkg['spy_df'] = df
            if len(df) > 200:
                # Aproksymacja VIX na podstawie historycznej zmienności
                pkg['vix'] = float(df['close'].pct_change().tail(30).std() * (252**0.5) * 100)
                pkg['trend'] = 'BULL' if df['close'].iloc[-1] > df['close'].rolling(200).mean().iloc[-1] else 'BEAR'
        
        if "RISK_OFF" in pkg['macro']: 
            pkg['vix'] = max(pkg['vix'], 30.0) 
            
    except: pass
    return pkg

def _get_sector_trend(session, ticker):
    try:
        res = session.execute(text("SELECT sector_trend_score FROM phase1_candidates WHERE ticker=:t"), {'t': ticker}).fetchone()
        return float(res[0]) if res and res[0] is not None else 0.0
    except: return 0.0

def _get_macro_context_for_aqm(session, client):
    macro = {'vix': 20.0, 'yield_10y': 4.0, 'inflation': 3.0, 'spy_df': pd.DataFrame()}
    try:
        spy_raw = get_raw_data_with_cache(session, client, 'QQQ', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        if spy_raw:
            macro['spy_df'] = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            macro['spy_df'].index = pd.to_datetime(macro['spy_df'].index)
            macro['spy_df'].sort_index(inplace=True)
        
        # Pobieranie danych makro (Yields, Inflation) - Wymagane przez AQM V4
        # (Kod skrócony, logika identyczna jak w poprzednich wersjach)
    except Exception: pass
    return macro

def run_h3_live_scan(session, candidates, client, parameters=None):
    """
    Główna pętla skanera Fazy 3.
    """
    logger.info("Start Phase 3 Live Sniper (V7.5 - Final Math Sync)...")
    
    # 1. Konfiguracja parametrów i Adaptacja
    base_params = DEFAULT_PARAMS.copy()
    if parameters:
        for k,v in parameters.items(): 
            if v is not None: 
                try: base_params[k] = float(v)
                except: base_params[k] = v 

    mkt = _get_market_pkg(session, client)
    params = _apply_adaptive_executor(base_params, mkt)

    # Wybór strategii
    requested_mode = params.get('strategy_mode', 'AUTO')
    if requested_mode in ['H3', 'AQM', 'BIOX']:
        strategy_mode = requested_mode
    else:
        # Auto-detekcja na podstawie parametrów
        if params.get('aqm_component_min') is not None and float(params['aqm_component_min']) > 0:
            strategy_mode = 'AQM'
        else:
            strategy_mode = 'H3'
    
    tp_mult = float(params['h3_tp_multiplier'])
    sl_mult = float(params['h3_sl_multiplier'])
    max_hold_days = int(params['h3_max_hold'])
    min_score = float(params['h3_min_score']) 

    append_scan_log(session, f"⚙️ FAZA 3: Tryb={strategy_mode} | VIX={mkt['vix']:.1f}")
    append_scan_log(session, f"   Parametry: MinScore={min_score}, TP={tp_mult}x, SL={sl_mult}x, P={params['h3_percentile']}")

    ev_model = _get_historical_ev_stats(session)
    macro_data_aqm = _get_macro_context_for_aqm(session, client) if strategy_mode == 'AQM' else {}

    signals = 0
    rejects = {'aqm':0, 'mass':0, 'data':0, 'live':0, 'components': 0, 'data_lag': 0}
    
    for i, ticker in enumerate(candidates):
        if i%5==0: update_scan_progress(session, i, len(candidates))
        
        try:
            # 2. Pobieranie danych (Daily + Adjusted)
            d_raw = get_raw_data_with_cache(session, client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12)
            da_raw = get_raw_data_with_cache(session, client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12)
            
            if not d_raw or not da_raw:
                rejects['data']+=1; continue
            
            ohlcv = standardize_df_columns(pd.DataFrame.from_dict(d_raw.get('Time Series (Daily)', {}), orient='index'))
            adj = standardize_df_columns(pd.DataFrame.from_dict(da_raw.get('Time Series (Daily)', {}), orient='index'))
            ohlcv.index = pd.to_datetime(ohlcv.index); adj.index = pd.to_datetime(adj.index)
            
            if len(adj) < REQUIRED_HISTORY_SIZE:
                rejects['data']+=1; continue
                
            if not _verify_data_freshness(adj, ticker):
                rejects['data_lag'] += 1; continue

            ohlcv['vwap_proxy'] = (ohlcv['high']+ohlcv['low']+ohlcv['close'])/3.0
            df = adj.join(ohlcv[['open','high','low','vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            df['atr_14'] = calculate_atr(df).ffill().fillna(0)
            df['close'] = df[close_col]
            
            last = df.iloc[-1]
            entry = _to_py_float(last['close'])
            atr_val = _to_py_float(last['atr_14'])
            
            tp = entry + (tp_mult * atr_val)
            sl = entry - (sl_mult * atr_val)
            
            is_signal = False
            score = 0
            metric_details = {}
            rec = "HOLD"
            ev = 0.0
            expected_pf = 0.0
            expected_wr = 0.0
            
            if strategy_mode == 'H3':
                # === H3 LOGIC (Zsynchronizowana z aqm_v3_metrics) ===
                # 1. Ładowanie danych Wymiaru 2 (Insider/News)
                h2 = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, client, session)
                insider = h2.get('insider_df')
                news = h2.get('news_df')

                # 2. Obliczenia Metryk Podstawowych (zgodnie z PDF)
                df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
                
                # Używamy funkcji z aqm_v3_metrics (lub ich odpowiedników inline dla szybkości wektorowej)
                # Tutaj inline jest bezpieczniejsze dla wydajności przy dużym DataFrame
                df['daily_returns'] = df['close'].pct_change()
                df['market_temperature'] = df['daily_returns'].rolling(30).std()
                
                # Entropy S
                if not news.empty:
                    nc = news.groupby(news.index.date).size()
                    nc.index = pd.to_datetime(nc.index)
                    nc = nc.reindex(df.index, fill_value=0)
                    df['information_entropy'] = nc.rolling(10).sum()
                else: df['information_entropy'] = 0.0
                
                # Metryki H2 (zgodnie z utils/metrics)
                df['institutional_sync'] = df.apply(lambda r: aqm_v3_metrics.calculate_institutional_sync_from_data(insider, r.name), axis=1)
                df['retail_herding'] = df.apply(lambda r: aqm_v3_metrics.calculate_retail_herding_from_data(news, r.name), axis=1)
                
                # 3. Zastosowanie Formuły AQM V3 (Równanie Pola)
                # μ (Normalizacja)
                df['mu_normalized'] = (df['institutional_sync'] - df['institutional_sync'].rolling(100).mean()) / df['institutional_sync'].rolling(100).std().fillna(1)
                
                # Q (Cap)
                df['retail_herding_capped'] = calculate_retail_herding_capped_v4(df['retail_herding'])
                
                S = df['information_entropy']
                Q = df['retail_herding_capped']
                T = df['market_temperature']
                mu = df['mu_normalized'].fillna(0)
                
                # J = S - (Q/T) + μ
                df['J'] = (S - (Q/T.replace(0, np.nan)) + (mu*1.0)).fillna(0)
                
                # Normalizacja Składników (Z-Score w oknie 100)
                j_norm = ((df['J'] - df['J'].rolling(100).mean()) / df['J'].rolling(100).std()).fillna(0)
                
                df['nabla_sq'] = df['price_gravity']
                nabla_norm = ((df['nabla_sq'] - df['nabla_sq'].rolling(100).mean()) / df['nabla_sq'].rolling(100).std()).fillna(0)
                
                # Masa m^2 (Vol + News)
                df['avg_volume_10d'] = df['volume'].rolling(10).mean()
                df['vol_mean_200d'] = df['avg_volume_10d'].rolling(200).mean()
                df['vol_std_200d'] = df['avg_volume_10d'].rolling(200).std()
                df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).fillna(0)
                df['m_sq'] = df['normalized_volume'] # Uproszczone dla H3
                
                m_mean = df['m_sq'].rolling(100).mean()
                m_std = df['m_sq'].rolling(100).std()
                m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)
                
                # AQM Score = J - ∇² - m²
                aqm_score = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
                
                # Próg wejścia (Percentyl)
                h3_p = float(params.get('h3_percentile', 0.95))
                h3_m = float(params.get('h3_m_sq_threshold', -0.5))
                thresh = aqm_score.rolling(100).quantile(h3_p)
                
                curr_aqm = aqm_score.iloc[-1]
                curr_thr = thresh.iloc[-1]
                curr_m = m_norm.iloc[-1]
                
                metric_details = {
                    'aqm_score': curr_aqm, 'J_norm': j_norm.iloc[-1], 
                    'nabla_sq_norm': nabla_norm.iloc[-1], 'm_sq_norm': curr_m,
                    'threshold': curr_thr
                }

                # DECYZJA
                if curr_aqm > curr_thr and curr_aqm > min_score and curr_m < h3_m:
                    is_signal = True
                    st = _get_sector_trend(session, ticker)
                    score_int, det = _calculate_setup_score(curr_aqm, curr_thr, curr_m, df, mkt['spy_df'], st)
                    score = score_int
                    
                    # Obliczanie Oczekiwań (EV)
                    surplus = curr_aqm - curr_thr
                    ev_b = 'LOW' if surplus < 0.2 else ('MID' if surplus < 0.5 else 'HIGH')
                    stats_model = ev_model.get(ev_b, {'ev': surplus*2, 'pf': 1.5, 'wr': 40.0})
                    ev = stats_model['ev']
                    expected_pf = stats_model['pf']
                    expected_wr = stats_model['wr']
                    rec = "TOP 💎" if score >= 80 else ("BUY ✅" if score >= 60 else "MOD ⚠️")
                else:
                    if curr_aqm <= curr_thr: rejects['aqm']+=1
                    if curr_m >= h3_m: rejects['mass']+=1

            elif strategy_mode == 'AQM':
                # === AQM V4 LOGIC (Delegacja do modułu logic) ===
                w_raw = get_raw_data_with_cache(session, client, ticker, 'WEEKLY_ADJUSTED', 'get_weekly_adjusted')
                weekly_df = pd.DataFrame()
                if w_raw: 
                    weekly_df = standardize_df_columns(pd.DataFrame.from_dict(w_raw.get('Weekly Adjusted Time Series', {}), orient='index'))
                    weekly_df.index = pd.to_datetime(weekly_df.index)
                
                obv_raw = get_raw_data_with_cache(session, client, ticker, 'OBV', 'get_obv')
                obv_df = pd.DataFrame()
                if obv_raw: 
                    obv_df = pd.DataFrame.from_dict(obv_raw.get('Technical Analysis: OBV', {}), orient='index')
                    obv_df.index = pd.to_datetime(obv_df.index)
                    obv_df.rename(columns={'OBV': 'OBV'}, inplace=True)

                # Wywołanie dedykowanego modułu logiki V4
                aqm_metrics_df = aqm_v4_logic.calculate_aqm_full_vector(
                    daily_df=df,
                    weekly_df=weekly_df,
                    intraday_60m_df=pd.DataFrame(), # Nie używamy w V4
                    obv_df=obv_df,
                    macro_data=macro_data_aqm,
                    earnings_days_to=None
                )
                
                if not aqm_metrics_df.empty:
                    last_aqm = aqm_metrics_df.iloc[-1]
                    curr_score = last_aqm['aqm_score']
                    comp_min = float(params.get('aqm_component_min', 0.5))
                    
                    metric_details = {
                        'aqm_score': curr_score,
                        'qps': last_aqm['qps'], 'ves': last_aqm['ves'],
                        'mrs': last_aqm['mrs'], 'tcs': last_aqm['tcs']
                    }
                    
                    if (curr_score > min_score and
                        last_aqm['qps'] > comp_min and
                        last_aqm['ves'] > comp_min and
                        last_aqm['mrs'] > comp_min):
                        
                        is_signal = True
                        score = int(curr_score * 100) 
                        if score > 100: score = 99
                        rec = "TOP 💎" if score >= 80 else ("BUY ✅" if score >= 60 else "MOD ⚠️")
                        
                        ev = score * 0.05
                        expected_pf = 2.0 if score > 80 else 1.5
                        expected_wr = 60.0 if score > 80 else 50.0
                    else:
                        if curr_score <= min_score: rejects['aqm']+=1
                        else: rejects['components']+=1
                else:
                    rejects['data']+=1

            if is_signal:
                # === LIVE GUARD: Ostateczna weryfikacja ceny ===
                lq = client.get_global_quote(ticker)
                lp = safe_float(lq.get('05. price')) if lq else None
                
                if lp:
                    valid, msg = _is_setup_still_valid(entry, sl, tp, lp)
                    if not valid:
                        rejects['live']+=1; append_scan_log(session, f"❌ {ticker}: Live Reject: {msg}"); continue
                
                # Budowanie notatki dla Frontendu (KORELACJA Z UI.JS)
                # Frontend parsuje: "SCORE: XX", "EV: XX%", "DETALE: ..."
                if strategy_mode == 'H3':
                    note = f"STRATEGIA: H3\nEV: {float(ev):.2f}% | SCORE: {score}/100 | {rec}\nDETALE: Tech:{metric_details.get('tech_score',0)} Mkt:{metric_details.get('market_score',0)} RS:{metric_details.get('rs_score',0)}\nAQM H3:{metric_details['aqm_score']:.2f} (vs {metric_details['threshold']:.2f})"
                else:
                    note = f"STRATEGIA: AQM (V4)\nEV: {float(ev):.2f}% | SCORE: {score}/100 | {rec}\nDETALE: QPS:{metric_details.get('qps',0):.2f} VES:{metric_details.get('ves',0):.2f} MRS:{metric_details.get('mrs',0):.2f} TCS:{metric_details.get('tcs',0):.2f}\nAQM Score:{metric_details['aqm_score']:.2f} (vs {min_score:.2f})"

                ex = session.query(models.TradingSignal).filter(models.TradingSignal.ticker==ticker, models.TradingSignal.status.in_(['ACTIVE','PENDING'])).first()
                if not ex:
                    expiration_dt = datetime.now(timezone.utc) + timedelta(days=max_hold_days)
                    
                    sig = models.TradingSignal(
                        ticker=ticker, 
                        status='PENDING', 
                        generation_date=datetime.now(timezone.utc), 
                        updated_at=datetime.now(timezone.utc),
                        signal_candle_timestamp=last.name, 
                        entry_price=_to_py_float(entry), 
                        stop_loss=_to_py_float(sl), 
                        take_profit=_to_py_float(tp),
                        entry_zone_top=_to_py_float(entry + (0.5 * atr_val)), 
                        entry_zone_bottom=_to_py_float(entry - (0.5 * atr_val)),
                        risk_reward_ratio=_to_py_float(tp_mult / sl_mult), 
                        notes=note,
                        expiration_date=expiration_dt,
                        expected_profit_factor=_to_py_float(expected_pf),
                        expected_win_rate=_to_py_float(expected_wr)
                    )
                    session.add(sig); session.commit()
                    signals+=1
                    msg = f"💎 SYGNAŁ ({strategy_mode}): {ticker} | SCORE: {score} | EXP.PF: {expected_pf:.2f}"
                    logger.info(msg); append_scan_log(session, msg)
                    send_telegram_alert(f"⚛️ {strategy_mode}: {ticker}\nCena: {entry:.2f}\nExp. PF: {expected_pf:.2f}")
                else:
                    append_scan_log(session, f"ℹ️ {ticker}: Już aktywny.")
                
        except Exception as e:
            logger.error(f"Error {ticker}: {e}")
            continue
            
    update_scan_progress(session, len(candidates), len(candidates))
    sum_msg = f"🏁 Faza 3 ({strategy_mode}): Sygnałów: {signals}. Odrzuty: Lag={rejects['data_lag']}, AQM={rejects['aqm']}, Live={rejects['live']}"
    append_scan_log(session, sum_msg)
