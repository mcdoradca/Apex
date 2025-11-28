import logging
import optuna
import json
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os # Dodano do obsługi zmiennych środowiskowych

# Importy wewnętrzne - zachowane bez zmian
from .. import models
from . import backtest_engine
from .utils import (
    update_system_control, 
    append_scan_log, 
    calculate_atr,
    calculate_h3_metrics_v4,
    get_raw_data_with_cache,
    standardize_df_columns
)
from . import aqm_v3_metrics 
from . import aqm_v3_h2_loader
from . import aqm_v4_logic
from .apex_audit import SensitivityAnalyzer
from ..database import get_db_session 

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

class QuantumOptimizer:
    """
    SERCE SYSTEMU APEX V14 - PERSISTENT MEMORY MODE (SAFE)
    - Zapisywanie wyników Optuny do bazy danych PostgreSQL.
    - Osobna historia nauki dla każdego roku (study_name).
    - BRAK agresywnego Prunera.
    - BRAK zmian w zakresach parametrów (Max Hold, TP).
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session 
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.best_score_so_far = -1.0
        self.data_cache = {}  
        self.tickers_count = 0
        
        # Pobierz DATABASE_URL ze zmiennych środowiskowych
        self.storage_url = os.getenv("DATABASE_URL")
        if not self.storage_url:
            logger.warning("Brak DATABASE_URL. Optuna będzie działać w trybie ulotnym (bez zapisu).")
            self.storage_url = None
        
        self.job_config = {}
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job and job.configuration:
                self.job_config = job.configuration
        except: pass
        
        self.strategy_mode = self.job_config.get('strategy', 'H3') 
        
        logger.info(f"QuantumOptimizer V14 initialized for Job {job_id} (Mode: {self.strategy_mode})")

    def run(self, n_trials: int = 50):
        start_msg = f"🚀 OPTIMIZER V14 ({self.strategy_mode}): Start {self.job_id} (Rok: {self.target_year}, Próby: {n_trials})"
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            self.macro_data = self._load_macro_context()
            self._preload_data_to_cache()
            
            if not self.data_cache:
                raise Exception("Brak danych w cache.")

            update_system_control(self.session, 'worker_status', 'OPTIMIZING_CALC')
            
            # --- KONFIGURACJA OPTUNY Z BAZĄ DANYCH ---
            
            # Unikalna nazwa badania dla danego roku i strategii
            study_name = f"apex_opt_{self.strategy_mode}_{self.target_year}"
            
            logger.info(f"Podłączanie do badania Optuny: {study_name} w bazie danych...")

            # Standardowy sampler TPE (bez zmian)
            sampler = optuna.samplers.TPESampler(
                n_startup_trials=min(10, max(5, int(n_trials/5))), 
                multivariate=True,
                group=True
            )
            
            self.study = optuna.create_study(
                study_name=study_name,
                storage=self.storage_url, # <-- ZAPIS DO BAZY (JEDYNA ZMIANA)
                load_if_exists=True,      # <-- WCZYTAJ HISTORIĘ JEŚLI ISTNIEJE
                direction='maximize',
                sampler=sampler
                # BRAK Prunera (usunięto zgodnie z życzeniem)
            )
            
            logger.info(f"Optuna załadowana. Liczba dotychczasowych prób w historii: {len(self.study.trials)}")
            
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
            
            end_msg = f"🏁 ZAKOŃCZONO! Najlepszy PF ({self.strategy_mode}): {best_value:.4f}"
            logger.info(end_msg)
            append_scan_log(self.session, end_msg)
            
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            safe_params['strategy_mode'] = self.strategy_mode
            
            append_scan_log(self.session, f"🏆 Zwycięskie Parametry (z historii {len(self.study.trials)} prób):\n{json.dumps(safe_params, indent=2)}")

            trials_data = self._collect_trials_data()
            sensitivity_report = self._run_sensitivity_analysis(trials_data)
            
            self._finalize_job(best_trial, sensitivity_report)

        except Exception as e:
            self.session.rollback()
            error_msg = f"❌ OPTIMIZER V14 AWARIA: {str(e)}"
            logger.error(error_msg, exc_info=True)
            append_scan_log(self.session, error_msg)
            self._mark_job_failed()
            raise

    # ... Metody pomocnicze (bez zmian) ...
    def _load_macro_context(self):
        append_scan_log(self.session, "📊 Pobieranie danych Makro...")
        macro = {'vix': 20.0, 'yield_10y': 4.0, 'inflation': 3.0, 'spy_df': pd.DataFrame()}
        with get_db_session() as session:
            client = backtest_engine.AlphaVantageClient()
            spy_raw = get_raw_data_with_cache(session, client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
            if spy_raw:
                macro['spy_df'] = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
                macro['spy_df'].index = pd.to_datetime(macro['spy_df'].index)
                macro['spy_df'].sort_index(inplace=True)
            if self.strategy_mode == 'AQM':
                yield_raw = get_raw_data_with_cache(session, client, 'TREASURY_YIELD', 'TREASURY_YIELD', 'get_treasury_yield', interval='monthly')
                if yield_raw and 'data' in yield_raw:
                    try: macro['yield_10y'] = float(yield_raw['data'][0]['value'])
                    except: pass
                inf_raw = get_raw_data_with_cache(session, client, 'INFLATION', 'INFLATION', 'get_inflation_rate')
                if inf_raw and 'data' in inf_raw:
                    try: macro['inflation'] = float(inf_raw['data'][0]['value'])
                    except: pass
        return macro

    def _objective(self, trial):
        params = {}
        
        # === PRZYWRÓCONA ORYGINALNA PRZESTRZEŃ PARAMETRÓW ===
        # Zakresy są takie jak w poprzedniej wersji (z `step` usuniętym wcześniej na Twoje życzenie, 
        # ale bez wydłużania Max Hold i TP ponad standard).
        
        if self.strategy_mode == 'H3':
            params = {
                'h3_percentile': trial.suggest_float('h3_percentile', 0.85, 0.99), 
                'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -3.0, 0.5), 
                'h3_min_score': trial.suggest_float('h3_min_score', -0.5, 1.5),
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 10.0), # Stary limit 10.0
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.0, 5.0),
                'h3_max_hold': trial.suggest_int('h3_max_hold', 2, 15), # Stary limit 15 dni
            }
            
        elif self.strategy_mode == 'AQM':
            params = {
                'aqm_min_score': trial.suggest_float('aqm_min_score', 0.50, 0.95),
                'aqm_component_min': trial.suggest_float('aqm_component_min', 0.2, 0.8),
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 10.0), # Stary limit 10.0
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.5, 5.0),
                'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 15), # Stary limit 15 dni
            }

        start_ts = pd.Timestamp(f"{self.target_year}-01-01")
        end_ts = pd.Timestamp(f"{self.target_year}-12-31")
        
        result = self._run_simulation_unified(params, start_ts, end_ts)
        
        pf = result['profit_factor']
        trades = result['total_trades']
        
        if trial.number % 5 == 0:
            logger.info(f"⚡ Trial {trial.number}: PF={pf:.2f} (Trades: {trades})")

        if pf > self.best_score_so_far:
            self.best_score_so_far = pf
            self._update_best_score(pf)

        self._save_trial(trial, params, pf, trades, pf, result['win_rate'])
        
        if trades < 5: return 0.0 
        return pf

    # ... Reszta metod klasy (bez zmian) ...
    def _run_simulation_unified(self, params, start_ts, end_ts):
        trades_pnl = []
        tp_mult = params['h3_tp_multiplier']
        sl_mult = params['h3_sl_multiplier']
        max_hold = params['h3_max_hold']
        for ticker, df in self.data_cache.items():
            if df.empty: continue
            mask_date = (df.index >= start_ts) & (df.index <= end_ts)
            sim_df = df[mask_date]
            if len(sim_df) < 2: continue
            entry_mask = None
            if self.strategy_mode == 'H3':
                h3_p = params['h3_percentile']
                h3_m = params['h3_m_sq_threshold']
                h3_min = params['h3_min_score']
                entry_mask = ((sim_df['aqm_rank'] > h3_p) & (sim_df['m_sq_norm'] < h3_m) & (sim_df['aqm_score_h3'] > h3_min))
            elif self.strategy_mode == 'AQM':
                min_score = params['aqm_min_score']
                comp_min = params['aqm_component_min']
                cond_main = (sim_df['aqm_score'] > min_score)
                cond_comps = ((sim_df['qps'] > comp_min) & (sim_df['ves'] > comp_min) & (sim_df['mrs'] > comp_min))
                entry_mask = cond_main & cond_comps
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
        if not trades: return {'profit_factor': 0.0, 'total_trades': 0, 'win_rate': 0.0}
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        total_win = sum(wins)
        total_loss = abs(sum(losses))
        pf = total_win / total_loss if total_loss > 0 else 0.0
        win_rate = (len(wins) / len(trades)) * 100 if len(trades) > 0 else 0.0
        return {'profit_factor': pf, 'total_trades': len(trades), 'win_rate': win_rate}

    def _get_all_tickers(self):
        try:
            res_p1 = self.session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
            tickers_p1 = [r[0] for r in res_p1]
            if len(tickers_p1) > 0: return tickers_p1
            res_all = self.session.execute(text("SELECT ticker FROM companies LIMIT 100")).fetchall()
            return [r[0] for r in res_all]
        except Exception: return []

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
            if job: 
                job.best_score = float(score) 
                self.session.commit()
        except: self.session.rollback()

    def _save_trial(self, trial, params, pf, trades, score, win_rate):
        try:
            safe_pf = float(pf) if pf is not None and not np.isnan(pf) else 0.0
            safe_trades = int(trades) if trades is not None else 0
            safe_score = float(score) if score is not None and not np.isnan(score) else 0.0
            safe_win_rate = float(win_rate) if win_rate is not None and not np.isnan(win_rate) else 0.0
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in params.items()}
            trial_record = models.OptimizationTrial(
                job_id=self.job_id, trial_number=trial.number, params=safe_params,
                profit_factor=safe_pf, total_trades=safe_trades, win_rate=safe_win_rate,
                net_profit=0.0, state='COMPLETE', created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            if trial.number % 10 == 0: self.session.commit()
        except: self.session.rollback()

    def _finalize_job(self, best_trial, sensitivity_report):
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'COMPLETED'
            job.best_score = float(best_trial.value)
            best_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            job.configuration = {
                'best_params': best_params, 
                'sensitivity_analysis': sensitivity_report, 
                'version': 'V14_PERSISTENT', 
                'strategy': self.strategy_mode,
                'tickers_analyzed': self.tickers_count
            }
            self.session.commit()

    def _mark_job_failed(self):
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job: job.status = 'FAILED'; self.session.commit()
        except: self.session.rollback()
    
    def _preload_data_to_cache(self):
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_DATA_LOAD')
        msg = f"🔄 V13 PRELOAD: Pobieranie danych dla trybu {self.strategy_mode}..."
        logger.info(msg)
        append_scan_log(self.session, msg)
        tickers = self._get_all_tickers()
        tickers_to_load = tickers 
        total_tickers = len(tickers_to_load)
        if total_tickers == 0: return
        max_workers = 4 
        processed = 0
        loaded = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor: 
            futures = {executor.submit(self._load_ticker_data, ticker): ticker for ticker in tickers_to_load}
            for f in as_completed(futures):
                processed += 1
                try:
                    if f.result(): loaded += 1
                except: pass
                if processed % 10 == 0:
                    update_system_control(self.session, 'scan_progress_processed', str(processed))
                    update_system_control(self.session, 'scan_progress_total', str(total_tickers))
        self.tickers_count = len(self.data_cache)
        append_scan_log(self.session, f"✅ Cache gotowy: {self.tickers_count} / {total_tickers} spółek.")

    def _load_ticker_data(self, ticker):
        with get_db_session() as thread_session:
            try:
                time.sleep(0.1) 
                api_client = backtest_engine.AlphaVantageClient()
                daily_data = get_raw_data_with_cache(thread_session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                if not daily_data: return False
                h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, thread_session)
                weekly_df = pd.DataFrame()
                obv_df = pd.DataFrame()
                if self.strategy_mode == 'AQM':
                    w_raw = get_raw_data_with_cache(thread_session, api_client, ticker, 'WEEKLY_ADJUSTED', 'get_weekly_adjusted')
                    if w_raw: weekly_df = standardize_df_columns(pd.DataFrame.from_dict(w_raw.get('Weekly Adjusted Time Series', {}), orient='index'))
                    obv_raw = get_raw_data_with_cache(thread_session, api_client, ticker, 'OBV', 'get_obv')
                    if obv_raw:
                        obv_df = pd.DataFrame.from_dict(obv_raw.get('Technical Analysis: OBV', {}), orient='index')
                        obv_df.index = pd.to_datetime(obv_df.index)
                        obv_df.rename(columns={'OBV': 'OBV'}, inplace=True)
                processed_df = self._preprocess_ticker_unified(daily_data, h2_data, weekly_df, obv_df)
                if not processed_df.empty:
                    self.data_cache[ticker] = processed_df
                    return True
                return False
            except: return False

    def _preprocess_ticker_unified(self, daily_data, h2_data, weekly_df, obv_df) -> pd.DataFrame:
        try:
            daily_df = standardize_df_columns(pd.DataFrame.from_dict(daily_data.get('Time Series (Daily)', {}), orient='index'))
            if len(daily_df) < 100: return pd.DataFrame()
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.sort_index(inplace=True)
            daily_df['atr_14'] = calculate_atr(daily_df).ffill().fillna(0)

            if self.strategy_mode == 'H3':
                daily_df['price_gravity'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3 / daily_df['close'] - 1
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                daily_df['institutional_sync'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
                daily_df['retail_herding'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
                daily_df['daily_returns'] = daily_df['close'].pct_change()
                daily_df['market_temperature'] = daily_df['daily_returns'].rolling(window=30).std()
                if not news_df.empty:
                    nc = news_df.groupby(news_df.index.date).size()
                    nc.index = pd.to_datetime(nc.index)
                    nc = nc.reindex(daily_df.index, fill_value=0)
                    daily_df['information_entropy'] = nc.rolling(window=10).sum()
                else: daily_df['information_entropy'] = 0.0
                daily_df['avg_volume_10d'] = daily_df['volume'].rolling(window=10).mean()
                daily_df['vol_mean_200d'] = daily_df['avg_volume_10d'].rolling(window=200).mean()
                daily_df['vol_std_200d'] = daily_df['avg_volume_10d'].rolling(window=200).std()
                daily_df['normalized_volume'] = ((daily_df['avg_volume_10d'] - daily_df['vol_mean_200d']) / daily_df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                daily_df['normalized_news'] = 0.0 
                daily_df['m_sq'] = daily_df['normalized_volume'] 
                daily_df['nabla_sq'] = daily_df['price_gravity']
                daily_df = calculate_h3_metrics_v4(daily_df, {}) 
                daily_df['aqm_rank'] = daily_df['aqm_score_h3'].rolling(window=100).rank(pct=True).fillna(0)
                return daily_df[['open', 'high', 'low', 'close', 'atr_14', 'aqm_score_h3', 'aqm_rank', 'm_sq_norm']].dropna()

            elif self.strategy_mode == 'AQM':
                aqm_df = aqm_v4_logic.calculate_aqm_full_vector(
                    daily_df=daily_df, weekly_df=weekly_df, intraday_60m_df=pd.DataFrame(),
                    obv_df=obv_df, macro_data=self.macro_data, earnings_days_to=None
                )
                if 'atr' in aqm_df.columns: aqm_df['atr_14'] = aqm_df['atr']
                req_cols = ['open', 'high', 'low', 'close', 'atr_14', 'aqm_score', 'qps', 'ves', 'mrs', 'tcs']
                return aqm_df[req_cols].dropna()
            return pd.DataFrame()
        except: return pd.DataFrame()
