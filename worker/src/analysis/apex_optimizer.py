import logging
import optuna
import json
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio

# Importy wewnętrzne
from .. import models
from . import backtest_engine
from .utils import (
    update_system_control, 
    append_scan_log, 
    get_optimized_periods_v4,
    standardize_df_columns, 
    calculate_atr,
    calculate_h3_metrics_v4,
    get_raw_data_with_cache
)
from . import aqm_v3_metrics 
from . import aqm_v3_h2_loader
from .apex_audit import SensitivityAnalyzer
from ..database import get_db_session 

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

class AdaptiveExecutor:
    """
    Moduł adaptacji parametrów w czasie rzeczywistym (Live).
    Dostosowuje sztywne parametry strategii do bieżącego reżimu rynkowego (VIX, Trend).
    """
    def __init__(self, base_params: dict):
        self.base_params = base_params

    def get_adapted_params(self, market_context: dict) -> dict:
        adapted = self.base_params.copy()
        vix = market_context.get('vix', 20.0)
        trend = market_context.get('trend', 'NEUTRAL')
        
        if vix > 25.0:
            adapted['h3_sl_multiplier'] = adapted.get('h3_sl_multiplier', 2.0) * 1.5
            adapted['h3_tp_multiplier'] = adapted.get('h3_tp_multiplier', 5.0) * 1.2
            adapted['h3_percentile'] = min(0.99, adapted.get('h3_percentile', 0.95) + 0.02)
        elif vix < 15.0:
            adapted['h3_tp_multiplier'] = adapted.get('h3_tp_multiplier', 5.0) * 0.8
            adapted['h3_sl_multiplier'] = max(1.5, adapted.get('h3_sl_multiplier', 2.0) * 0.8)

        if trend == 'BEAR':
            adapted['h3_min_score'] = max(0.5, adapted.get('h3_min_score', 0.0))
            adapted['h3_tp_multiplier'] = adapted.get('h3_tp_multiplier', 5.0) * 0.7
            
        return adapted

class QuantumOptimizer:
    """
    SERCE SYSTEMU APEX V7 - TURBO MODE + HIGH PF CONFIG
    - Pre-kalkulacja Rankingu Percentylowego (O(1) w pętli symulacji).
    - WYMUSZONY BRAK WPŁYWU NEWSÓW NA M_SQ (normalized_news = 0.0).
      To ustawienie historycznie generowało najwyższy PF (1.67).
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session 
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.best_score_so_far = -1.0
        self.data_cache = {}  
        self.tickers_count = 0
        
        logger.info(f"QuantumOptimizer V7 (Turbo + High PF) initialized for Job {job_id}")

    def run(self, n_trials: int = 1000):
        start_msg = f"🚀 QUANTUM OPTIMIZER V7: Start {self.job_id} (Rok: {self.target_year}, Próby: {n_trials})"
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_INIT')
        
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            self._preload_data_to_cache()
            
            if not self.data_cache:
                raise Exception("Brak danych w cache. Prerywam.")

            update_system_control(self.session, 'worker_status', 'OPTIMIZING_CALC')
            msg_calc = "✅ Dane w RAM (V7 Turbo). Uruchamianie Optuny..."
            logger.info(msg_calc)
            append_scan_log(self.session, msg_calc)

            self.study = optuna.create_study(
                direction='maximize',
                sampler=optuna.samplers.TPESampler(
                    n_startup_trials=min(20, max(5, int(n_trials/5))), 
                    multivariate=True,
                    group=True
                )
            )
            
            self.study.optimize(
                self._objective, 
                n_trials=n_trials,
                catch=(Exception,),
                show_progress_bar=False
            )
            
            if len(self.study.trials) == 0:
                raise Exception("Brak udanych prób optymalizacji.")

            best_trial = self.study.best_trial
            best_value = float(best_trial.value)
            
            end_msg = f"🏁 ZAKOŃCZONO! Najlepszy Score: {best_value:.4f}"
            logger.info(end_msg)
            append_scan_log(self.session, end_msg)
            
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            append_scan_log(self.session, f"🏆 Zwycięskie Parametry:\n{json.dumps(safe_params, indent=2)}")

            trials_data = self._collect_trials_data()
            sensitivity_report = self._run_sensitivity_analysis(trials_data)
            
            self._finalize_job(best_trial, sensitivity_report)

        except Exception as e:
            self.session.rollback()
            error_msg = f"❌ QUANTUM OPTIMIZER V7 AWARIA: {str(e)}"
            logger.error(error_msg, exc_info=True)
            append_scan_log(self.session, error_msg)
            self._mark_job_failed()
            raise

    def _preload_data_to_cache(self):
        """Ładuje dane i oblicza STATIC RANKINGI dla ultra-szybkiego backtestu."""
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_DATA_LOAD')
        msg = "🔄 V7 PRELOAD: Ładowanie danych i obliczanie rankingów statycznych..."
        logger.info(msg)
        append_scan_log(self.session, msg)
        
        tickers = self._get_all_tickers()
        tickers_to_load = tickers[:250] # Limit dla bezpieczeństwa pamięci
        
        with ThreadPoolExecutor(max_workers=4) as executor: 
            futures = []
            for ticker in tickers_to_load:
                futures.append(executor.submit(self._load_ticker_data, ticker))
            
            count = 0
            for f in as_completed(futures):
                try:
                    f.result()
                    count += 1
                    if count % 50 == 0:
                        append_scan_log(self.session, f"   ... przetworzono {count}/{len(tickers_to_load)}")
                except: pass
        
        self.tickers_count = len(self.data_cache)
        append_scan_log(self.session, f"✅ Cache gotowy: {self.tickers_count} instrumentów.")

    def _load_ticker_data(self, ticker):
        with get_db_session() as thread_session:
            try:
                api_client = backtest_engine.AlphaVantageClient()
                
                daily_data = get_raw_data_with_cache(
                    thread_session, api_client, ticker, 
                    'DAILY_OHLCV', 'get_time_series_daily', outputsize='full'
                )
                h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, thread_session)
                
                if daily_data and h2_data:
                    processed_df = self._preprocess_ticker_turbo(daily_data, h2_data)
                    if not processed_df.empty:
                        self.data_cache[ticker] = processed_df
            except: pass

    def _preprocess_ticker_turbo(self, daily_data, h2_data) -> pd.DataFrame:
        """
        V7 TURBO PREPROCESSING:
        Oblicza wszystko co statyczne (wskaźniki, AQM score, RANKINGI).
        Dzięki temu w pętli Optuny nie liczymy żadnych średnich ani kwantyli.
        """
        try:
            daily_df = standardize_df_columns(
                pd.DataFrame.from_dict(daily_data.get('Time Series (Daily)', {}), orient='index')
            )
            if len(daily_df) < 100: return pd.DataFrame()
            
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.sort_index(inplace=True)

            # 1. Podstawy
            daily_df['atr_14'] = calculate_atr(daily_df).ffill().fillna(0)
            daily_df['price_gravity'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3 / daily_df['close'] - 1
            
            # 2. H2 (News & Insider)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')
            
            daily_df['institutional_sync'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            daily_df['retail_herding'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            
            # 3. Metryki techniczne H3
            daily_df['daily_returns'] = daily_df['close'].pct_change()
            daily_df['market_temperature'] = daily_df['daily_returns'].rolling(window=30).std()
            
            if not news_df.empty:
                nc = news_df.groupby(news_df.index.date).size()
                nc.index = pd.to_datetime(nc.index)
                nc = nc.reindex(daily_df.index, fill_value=0)
                daily_df['information_entropy'] = nc.rolling(window=10).sum()
            else:
                daily_df['information_entropy'] = 0.0
            
            # m_sq calculation
            daily_df['avg_volume_10d'] = daily_df['volume'].rolling(window=10).mean()
            daily_df['vol_mean_200d'] = daily_df['avg_volume_10d'].rolling(window=200).mean()
            daily_df['vol_std_200d'] = daily_df['avg_volume_10d'].rolling(window=200).std()
            daily_df['normalized_volume'] = ((daily_df['avg_volume_10d'] - daily_df['vol_mean_200d']) / daily_df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
            
            # === ZMIANA DLA WYSOKIEGO PF: IGNOROWANIE NEWSÓW W M_SQ ===
            # To ustawienie odfiltrowuje szum medialny i daje lepsze wyniki.
            daily_df['normalized_news'] = 0.0
            
            daily_df['m_sq'] = daily_df['normalized_volume'] + daily_df['normalized_news']
            daily_df['nabla_sq'] = daily_df['price_gravity']

            # 4. Pełne obliczenie AQM Score
            daily_df = calculate_h3_metrics_v4(daily_df, {})
            
            # 5. === TURBO BOOST: PRE-KALKULACJA RANKINGU ===
            # Liczymy percentylową rangę każdego punktu w jego oknie 100-dniowym.
            daily_df['aqm_rank'] = daily_df['aqm_score_h3'].rolling(window=100).rank(pct=True).fillna(0)
            
            # Czyścimy DF zostawiając tylko to co niezbędne do symulacji (oszczędność RAM)
            cols_needed = ['open', 'high', 'low', 'close', 'atr_14', 'aqm_score_h3', 'aqm_rank', 'm_sq_norm']
            return daily_df[cols_needed].dropna()
            
        except Exception:
            return pd.DataFrame()

    def _objective(self, trial):
        params = {
            'h3_percentile': trial.suggest_float('h3_percentile', 0.80, 0.99), 
            'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -1.5, 0.0), # Zakres dla czystego wolumenu
            'h3_min_score': trial.suggest_float('h3_min_score', -0.5, 1.5),
            'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 8.0),
            'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.0, 4.0),
            'h3_max_hold': trial.suggest_int('h3_max_hold', 2, 10),
        }

        # Daty symulacji
        start_ts = pd.Timestamp(f"{self.target_year}-01-01")
        end_ts = pd.Timestamp(f"{self.target_year}-12-31")
        
        result = self._run_turbo_simulation(params, start_ts, end_ts)
        
        pf = result['profit_factor']
        trades = result['total_trades']
        
        if trial.number % 20 == 0:
            append_scan_log(self.session, f"⚡ Próba {trial.number}: PF={pf:.2f} (Trades: {trades})")

        if pf > self.best_score_so_far:
            self.best_score_so_far = pf
            self._update_best_score(pf)

        self._save_trial(trial, params, pf, trades, pf)
        
        if trades < 20: return 0.0
        return pf

    def _run_turbo_simulation(self, params, start_ts, end_ts):
        """
        Ultra-szybka pętla symulacyjna wykorzystująca pre-kalkulowane rankingi.
        """
        trades_pnl = []
        
        h3_p = params['h3_percentile']
        h3_m = params['h3_m_sq_threshold']
        h3_min = params['h3_min_score']
        tp_mult = params['h3_tp_multiplier']
        sl_mult = params['h3_sl_multiplier']
        max_hold = params['h3_max_hold']

        for ticker, df in self.data_cache.items():
            if df.empty: continue
            
            mask_date = (df.index >= start_ts) & (df.index <= end_ts)
            sim_df = df[mask_date]
            
            if len(sim_df) < 2: continue
            
            # === TURBO LOGIC: WEKTORYZACJA WARUNKÓW ===
            entry_mask = (
                (sim_df['aqm_rank'] > h3_p) & 
                (sim_df['m_sq_norm'] < h3_m) & 
                (sim_df['aqm_score_h3'] > h3_min)
            )
            
            entry_indices = np.where(entry_mask)[0]
            last_exit_idx = -1
            
            for idx in entry_indices:
                if idx <= last_exit_idx: continue
                if idx + 1 >= len(sim_df): break 
                
                entry_idx = idx + 1
                entry_row = sim_df.iloc[entry_idx]
                signal_row = sim_df.iloc[idx] 
                
                entry_price = entry_row['open']
                atr = signal_row['atr_14']
                
                if atr == 0: continue
                
                tp = entry_price + (tp_mult * atr)
                sl = entry_price - (sl_mult * atr)
                
                pnl = 0.0
                
                for hold_day in range(max_hold):
                    current_idx = entry_idx + hold_day
                    if current_idx >= len(sim_df): 
                        exit_price = sim_df.iloc[-1]['close']
                        pnl = (exit_price - entry_price) / entry_price
                        last_exit_idx = current_idx
                        break
                    
                    candle = sim_df.iloc[current_idx]
                    
                    if candle['low'] <= sl:
                        pnl = (sl - entry_price) / entry_price
                        last_exit_idx = current_idx
                        break
                    elif candle['high'] >= tp:
                        pnl = (tp - entry_price) / entry_price
                        last_exit_idx = current_idx
                        break
                    
                    if hold_day == max_hold - 1:
                        pnl = (candle['close'] - entry_price) / entry_price
                        last_exit_idx = current_idx
                
                trades_pnl.append(pnl)

        return self._calculate_stats(trades_pnl)

    def _calculate_stats(self, trades):
        if not trades: return {'profit_factor': 0.0, 'total_trades': 0}
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        total_win = sum(wins)
        total_loss = abs(sum(losses))
        pf = total_win / total_loss if total_loss > 0 else 0.0
        return {'profit_factor': pf, 'total_trades': len(trades)}

    def _get_all_tickers(self):
        try:
            query = text("(SELECT ticker FROM phase1_candidates) UNION (SELECT ticker FROM portfolio_holdings) UNION (SELECT ticker FROM companies LIMIT 300)")
            result = self.session.execute(query)
            return [r[0] for r in result]
        except: return []

    def _collect_trials_data(self):
        trials_data = []
        for t in self.study.trials:
            if t.state == optuna.trial.TrialState.COMPLETE:
                safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in t.params.items()}
                trials_data.append({'params': safe_params, 'profit_factor': float(t.value) if t.value is not None else 0.0})
        return trials_data

    def _run_sensitivity_analysis(self, trials_data):
        if len(trials_data) < 10: return {}
        try:
            analyzer = SensitivityAnalyzer()
            return analyzer.analyze_parameter_sensitivity(trials_data)
        except: return {}

    def _update_best_score(self, score):
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job: job.best_score = float(score); self.session.commit()
        except: self.session.rollback()

    def _save_trial(self, trial, params, pf, trades, score):
        try:
            trial_record = models.OptimizationTrial(
                job_id=self.job_id, trial_number=trial.number, params=params,
                profit_factor=pf, total_trades=trades, state='COMPLETE', created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            if trial.number % 20 == 0: self.session.commit()
        except: self.session.rollback()

    def _finalize_job(self, best_trial, sensitivity_report):
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'COMPLETED'
            job.best_score = float(best_trial.value)
            best_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            job.configuration = {'best_params': best_params, 'sensitivity_analysis': sensitivity_report, 'version': 'V7_TURBO_HIGH_PF'}
            self.session.commit()

    def _mark_job_failed(self):
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job: job.status = 'FAILED'; self.session.commit()
        except: self.session.rollback()
