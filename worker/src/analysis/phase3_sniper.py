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
# === NOWOŚĆ: SILNIK RANKINGOWY (EV + SCORE) ===
# ============================================================================

def _get_historical_ev_stats(session: Session) -> Dict[str, Dict[str, float]]:
    """
    Analizuje historię VirtualTrades, aby stworzyć mapę oczekiwanej wartości (EV)
    w zależności od siły sygnału (AQM Score).
    Tworzy "koszyki" jakości: LOW, MID, HIGH.
    """
    try:
        # Pobierz zamknięte transakcje z metrykami
        trades = session.query(
            models.VirtualTrade.final_profit_loss_percent,
            models.VirtualTrade.metric_aqm_score_h3,
            models.VirtualTrade.metric_aqm_percentile_95
        ).filter(
            models.VirtualTrade.status.in_(['CLOSED_TP', 'CLOSED_SL', 'CLOSED_EXPIRED']),
            models.VirtualTrade.final_profit_loss_percent.isnot(None),
            models.VirtualTrade.metric_aqm_score_h3.isnot(None)
        ).all()

        if len(trades) < 10:
            return {} # Za mało danych do statystyki

        # Konwersja na DataFrame dla szybkości
        data = []
        for t in trades:
            # Oblicz "Nadwyżkę Mocy" (Score - Threshold). To lepszy wskaźnik niż surowy Score.
            power_surplus = float(t.metric_aqm_score_h3) - float(t.metric_aqm_percentile_95)
            data.append({
                'pl': float(t.final_profit_loss_percent),
                'power': power_surplus
            })
        
        df = pd.DataFrame(data)
        
        # Definiujemy koszyki mocy sygnału
        # LOW: Sygnał ledwo przekroczył próg (0.0 - 0.2)
        # MID: Sygnał solidny (0.2 - 0.5)
        # HIGH: Sygnał bardzo silny (> 0.5)
        
        buckets = {
            'LOW': df[(df['power'] >= 0) & (df['power'] < 0.2)],
            'MID': df[(df['power'] >= 0.2) & (df['power'] < 0.5)],
            'HIGH': df[df['power'] >= 0.5]
        }
        
        stats = {}
        for bucket_name, subset in buckets.items():
            if len(subset) < 5:
                # Fallback do średniej ogólnej jeśli koszyk pusty
                subset = df 
            
            wins = subset[subset['pl'] > 0]
            losses = subset[subset['pl'] <= 0]
            
            win_rate = len(wins) / len(subset) if len(subset) > 0 else 0
            avg_win = wins['pl'].mean() if not wins.empty else 0
            avg_loss = abs(losses['pl'].mean()) if not losses.empty else 0
            
            # EV = (WinRate * AvgWin) - (LossRate * AvgLoss)
            ev = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
            
            stats[bucket_name] = {
                'ev': ev,
                'win_rate': win_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'sample_size': len(subset)
            }
            
        return stats

    except Exception as e:
        logger.warning(f"Błąd obliczania historycznego EV: {e}")
        return {}

def _calculate_setup_score(
    aqm_score: float, 
    aqm_threshold: float, 
    mass_sq: float,
    ticker_df: pd.DataFrame, 
    spy_df: pd.DataFrame,
    sector_trend_score: float
) -> Tuple[int, Dict[str, Any]]:
    """
    Oblicza punktowy wynik jakości setupu (0-100) wg Koncepcji 1.
    """
    score = 0
    details = {}

    # 1. Siła Sygnału Technicznego (AQM) - Waga 40% (Max 40 pkt)
    # Bazujemy na tym, jak bardzo przebiliśmy próg.
    surplus = aqm_score - aqm_threshold
    if surplus <= 0:
        tech_score = 0
    else:
        # Skalowanie: surplus 0.0 -> 10 pkt, surplus 0.5 -> 40 pkt
        tech_score = 10 + min(30, (surplus / 0.5) * 30)
    
    score += tech_score
    details['tech_score'] = int(tech_score)

    # 2. Kontekst Rynkowy - Waga 30% (Max 30 pkt)
    market_score = 0
    # A. Trend SPY (15 pkt)
    if not spy_df.empty:
        spy_price = spy_df['close'].iloc[-1]
        spy_sma200 = spy_df['close'].rolling(200).mean().iloc[-1]
        if not pd.isna(spy_sma200) and spy_price > spy_sma200:
            market_score += 15
    # B. Trend Sektora (15 pkt) - przekazany z Phase 1 cache
    if sector_trend_score > 0:
        market_score += 15
    
    score += market_score
    details['market_score'] = market_score

    # 3. Siła Relatywna (RS) - Waga 20% (Max 20 pkt)
    # Porównujemy zwrot z 5 dni
    rs_score = 0
    if not spy_df.empty and len(ticker_df) > 5 and len(spy_df) > 5:
        ticker_5d = ticker_df['close'].pct_change(5).iloc[-1]
        spy_5d = spy_df['close'].pct_change(5).iloc[-1]
        
        if not pd.isna(ticker_5d) and not pd.isna(spy_5d):
            if ticker_5d > spy_5d:
                rs_score += 10 # Lepszy od rynku
            if ticker_5d > 0 and spy_5d < 0:
                rs_score += 10 # Rośnie gdy rynek spada (Mega siła)
            elif ticker_5d > (spy_5d * 1.5):
                rs_score += 5 # Znacznie lepszy
    
    score += rs_score
    details['rs_score'] = rs_score

    # 4. Sytuacja Wyjściowa (Masa/Opór) - Waga 10% (Max 10 pkt)
    # Ujemna masa m^2 oznacza "podciśnienie" / brak oporu / konsolidację
    setup_context_score = 0
    if mass_sq < -1.0:
        setup_context_score = 10 # Bardzo pusto
    elif mass_sq < -0.5:
        setup_context_score = 5  # Dość pusto
        
    score += setup_context_score
    details['context_score'] = setup_context_score

    return int(score), details

# ============================================================================
# === FUNKCJE POMOCNICZE FAZY 3 ===
# ============================================================================

def _get_market_data_package(session: Session, api_client: AlphaVantageClient) -> Dict[str, Any]:
    """
    Pobiera dane rynkowe raz na cykl: SPY DF, Sentyment Makro, Warunki VIX.
    """
    package = {'vix': 20.0, 'trend': 'NEUTRAL', 'spy_df': pd.DataFrame(), 'macro': 'UNKNOWN'}
    
    try:
        # 1. Makro z bazy
        package['macro'] = get_system_control_value(session, 'macro_sentiment') or "UNKNOWN"
        
        # 2. Dane SPY
        spy_raw = get_raw_data_with_cache(session, api_client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=24, outputsize='full')
        if spy_raw:
            spy_df = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
            spy_df.index = pd.to_datetime(spy_df.index)
            spy_df.sort_index(inplace=True)
            package['spy_df'] = spy_df
            
            if len(spy_df) > 200:
                # VIX Proxy
                recent_returns = spy_df['close'].pct_change().tail(30)
                vix_proxy = recent_returns.std() * (252 ** 0.5) * 100
                package['vix'] = float(vix_proxy) if not pd.isna(vix_proxy) else 20.0
                
                # Trend Rynku
                current_price = spy_df['close'].iloc[-1]
                sma_200 = spy_df['close'].rolling(window=200).mean().iloc[-1]
                package['trend'] = 'BULL' if current_price > sma_200 else 'BEAR'

        # Logic Risk-Off override
        if "RISK_OFF" in package['macro']:
            package['vix'] = max(package['vix'], 30.0)

        return package
    except Exception as e:
        logger.warning(f"Błąd pobierania pakietu rynkowego: {e}")
        return package

def _get_sector_trend_from_cache(session: Session, ticker: str) -> float:
    """Pobiera zapisany w Fazie 1 trend sektora dla danego tickera."""
    try:
        result = session.execute(
            text("SELECT sector_trend_score FROM phase1_candidates WHERE ticker = :ticker"),
            {'ticker': ticker}
        ).fetchone()
        return float(result[0]) if result and result[0] is not None else 0.0
    except:
        return 0.0

# ============================================================================
# === GŁÓWNA FUNKCJA SKANOWANIA ===
# ============================================================================

def run_h3_live_scan(session: Session, candidates: List[str], api_client: AlphaVantageClient, parameters: Dict[str, Any] = None):
    """
    Główna pętla Fazy 3 (H3 LIVE SNIPER).
    Analizuje rynek, adaptuje parametry i...
    NOWOŚĆ: RANKINGUJE SYGNAŁY WG EV I SCORE.
    """
    logger.info("Uruchamianie Fazy 3: H3 LIVE SNIPER (Adaptive + Ranking EV/Score)...")
    
    # 1. Parametry bazowe
    base_params = DEFAULT_PARAMS.copy()
    if parameters:
        for k, v in parameters.items():
            if v is not None: base_params[k] = float(v)

    # 2. Pobierz dane rynkowe i model EV (raz na cykl)
    market_pkg = _get_market_data_package(session, api_client)
    ev_model = _get_historical_ev_stats(session)
    
    # 3. Adaptacja parametrów
    executor = AdaptiveExecutor(base_params)
    market_conditions = {'vix': market_pkg['vix'], 'trend': market_pkg['trend']}
    adapted_params = executor.get_adapted_params(market_conditions)
    
    # Log adaptacji
    changes_log = [f"{k}: {base_params.get(k)}->{v:.3f}" for k, v in adapted_params.items() if base_params.get(k) != v]
    if changes_log:
        append_scan_log(session, f"ADAPTACJA (VIX {market_pkg['vix']:.1f}): " + ", ".join(changes_log))
    
    # Rozpakowanie zmiennych
    h3_percentile = float(adapted_params['h3_percentile'])
    h3_m_sq_threshold = float(adapted_params['h3_m_sq_threshold'])
    h3_min_score = float(adapted_params['h3_min_score'])
    h3_tp_mult = float(adapted_params['h3_tp_multiplier'])
    h3_sl_mult = float(adapted_params['h3_sl_multiplier'])
    
    signals_generated = 0
    total_candidates = len(candidates)
    reject_stats = {'aqm_low': 0, 'm_sq_high': 0, 'history': 0, 'data_error': 0, 'validation': 0}

    # 4. PĘTLA SKANOWANIA
    for i, ticker in enumerate(candidates):
        if i % 5 == 0: update_scan_progress(session, i, total_candidates)
        
        try:
            # --- POBIERANIE I OBLICZENIA DANYCH (BEZ ZMIAN) ---
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

            # Metryki AQM
            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            df['nabla_sq'] = df['price_gravity']
            
            # Wolumen i Entropia
            df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
            df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
            df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
            df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).fillna(0)
            
            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(df.index, fill_value=0)
                df['information_entropy'] = news_counts.rolling(window=10).sum()
                
                # === FIX (V5.4): SYNCHRONIZACJA Z BACKTESTEM ===
                # W Backteście V4 (Optimizer) normalized_news jest zerowane (uproszczenie).
                # Phase 3 (Live) w V5 wliczała newsy do m_sq, co zawyżało wartości i psuło progi (-1.48).
                # Przywracamy logikę Backtestu dla m_sq (ale zachowujemy Entropy dla J).
                df['normalized_news'] = 0.0
            else:
                df['information_entropy'] = 0.0; df['normalized_news'] = 0.0
            
            df['m_sq'] = df['normalized_volume'] + df['normalized_news']

            # H3 Kalkulacje
            df['mu_normalized'] = (df['institutional_sync'] - df['institutional_sync'].rolling(H3_CALC_WINDOW).mean()) / df['institutional_sync'].rolling(H3_CALC_WINDOW).std()
            df['mu_normalized'] = df['mu_normalized'].fillna(0)
            df['retail_herding_capped'] = df['retail_herding'].clip(-1.0, 1.0)
            
            S = df['information_entropy']
            Q = df['retail_herding_capped']
            T = df['market_temperature']
            mu_norm = df['mu_normalized']
            
            df['J'] = (S - (Q / T.replace(0, np.nan)) + (mu_norm * 1.0)).fillna(0)

            # Normalizacja komponentów
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

            # Analiza ostatniej świecy
            last_candle = df.iloc[-1]
            current_aqm = aqm_score_series.iloc[-1]
            current_thresh = threshold_series.iloc[-1]
            current_m = m_norm.iloc[-1]

            # Walidacja Wejścia
            is_score_good = (current_aqm > current_thresh) and (current_aqm > h3_min_score)
            is_mass_good = (current_m < h3_m_sq_threshold)
            
            if not is_score_good:
                reject_stats['aqm_low'] += 1; append_scan_log(session, f"❌ {ticker}: AQM {current_aqm:.2f} < {current_thresh:.2f}"); continue
            if not is_mass_good:
                reject_stats['m_sq_high'] += 1; append_scan_log(session, f"❌ {ticker}: Masa {current_m:.2f} > {h3_m_sq_threshold:.2f}"); continue

            # === KWALIFIKACJA TECHNICZNA SUKCES - TERAZ LICZYMY SCORE i EV ===
            
            # A. Oblicz EV (Digital Intuition)
            surplus = current_aqm - current_thresh
            ev_bucket = 'LOW' if surplus < 0.2 else ('MID' if surplus < 0.5 else 'HIGH')
            ev_stats = ev_model.get(ev_bucket, {'ev': 0.0, 'win_rate': 0.0})
            
            calculated_ev = ev_stats['ev']
            # Jeśli brak danych historycznych, daj lekki bonus za wysoki surplus (teoretyczny)
            if not ev_model: 
                calculated_ev = surplus * 2.0

            # B. Oblicz Setup Score (0-100)
            sector_trend = _get_sector_trend_from_cache(session, ticker)
            setup_score, score_details = _calculate_setup_score(
                current_aqm, current_thresh, current_m, df, market_pkg['spy_df'], sector_trend
            )
            
            # C. Walidacja Live (Cena)
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
                valid_status, valid_reason = _is_setup_still_valid(entry_price, stop_loss, take_profit, current_live_price)

            if not valid_status:
                reject_stats['validation'] += 1; append_scan_log(session, f"❌ {ticker}: Live Error: {valid_reason}"); continue

            # === REKOMENDACJA KOŃCOWA ===
            recommendation = "HOLD"
            if setup_score >= 80: recommendation = "TOP_PICK 💎"
            elif setup_score >= 60: recommendation = "STRONG_BUY ✅"
            elif setup_score >= 40: recommendation = "MODERATE ⚠️"
            else: recommendation = "WEAK ❌"

            # Konstrukcja Notatki z Rankingiem
            ranking_note = (
                f"RANKING:\n"
                f"EV: {calculated_ev:+.2f}% | SCORE: {setup_score}/100 | REKOMENDACJA: {recommendation}\n"
                f"DETALE: Tech:{score_details['tech_score']} Mkt:{score_details['market_score']} "
                f"RS:{score_details['rs_score']} Ctx:{score_details['context_score']}\n"
                f"AQM:{current_aqm:.2f} (vs {current_thresh:.2f})"
            )

            # D. Zapis Sygnału
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
                    notes=ranking_note # Zapisujemy bogatą notatkę
                )
                session.add(new_signal)
                session.commit()
                signals_generated += 1
                
                # Logowanie Sukcesu
                success_msg = (f"💎 SYGNAŁ: {ticker} | EV: {calculated_ev:+.1f}% | SCORE: {setup_score} | {recommendation}")
                logger.info(success_msg)
                append_scan_log(session, success_msg)
                
                telegram_msg = (
                    f"⚛️ H3 SIGNAL + RANKING: {ticker}\n"
                    f"Cena: {ref_price:.2f} | EV: {calculated_ev:+.2f}%\n"
                    f"SCORE: {setup_score}/100 ({recommendation})\n"
                    f"RS: {score_details['rs_score']}/20 | Sektor: {score_details['market_score']}/30"
                )
                send_telegram_alert(telegram_msg)
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
