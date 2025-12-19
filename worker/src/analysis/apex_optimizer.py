import logging
import optuna
import json
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
import time
import os 
import math 

# Importy wewnętrzne
from .. import models
from . import backtest_engine
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    update_system_control, 
    append_scan_log, 
    calculate_atr,
    get_raw_data_with_cache,
    standardize_df_columns
)
# === IMPORTY SILNIKÓW FIZYCZNYCH (H3/AQM) ===
from . import aqm_v3_metrics 
from . import aqm_v3_h2_loader
from . import aqm_v4_logic
# ============================================
from .apex_audit import SensitivityAnalyzer
from ..database import get_db_session 

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

class QuantumOptimizer:
    """
    SERCE SYSTEMU APEX V20 (Unified Physics Engine)
    Optymalizator parametrów strategii.
    ZASADA: Używa DOKŁADNIE tych samych funkcji wektorowych co Sniper i Backtest.
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session 
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.best_score_so_far = -1.0
        self.data_cache = {}  
        self.tickers_count = 0
        
        # Flaga do debugowania dat (Sonda Diagnostyczna)
        self.debug_date_logged = False
        
        self.storage_url = os.getenv("DATABASE_URL")
        if self.storage_url and self.storage_url.startswith("postgres://"):
            self.storage_url = self.storage_url.replace("postgres://", "postgresql://", 1)
        
        self.job_config = {}
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job and job.configuration:
                self.job_config = job.configuration
        except: pass
        
        self.strategy_mode = self.job_config.get('strategy', 'H3')
        # Zabezpieczenie: Pobieramy scan_period, domyślnie FULL, ale logujemy to!
        self.scan_period = self.job_config.get('scan_period', 'FULL') 
        
        logger.info(f"QuantumOptimizer initialized: Job {job_id}, Mode {self.strategy_mode}, Period: {self.scan_period}, Year: {self.target_year}")

    def run(self, n_trials: int = 50):
        start_msg = f"🚀 OPTIMIZER V20: Start Zadania {self.job_id} (Strategia: {self.strategy_mode}, Okres: {self.scan_period} {self.target_year})..."
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        
        # Aktualizacja statusu
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            # 1. Makro (Teraz ładuje serie czasowe!)
            self.macro_data = self._load_macro_context()
            
            # 2. Cache Danych (SEKWENCYJNIE Z PEŁNĄ DIAGNOSTYKĄ)
            # Tutaj następuje wstępne obliczenie fizyki (Vector Engine)
            self._preload_data_to_cache_sequential()
            
            if not self.data_cache:
                err_msg = "⛔ BŁĄD KRYTYCZNY: Cache danych jest pusty! Sprawdź F1 lub limity API."
                append_scan_log(self.session, err_msg)
                self._mark_job_failed()
                return

            update_system_control(self.session, 'worker_status', 'OPTIMIZING_CALC')
            
            study_name = f"apex_opt_{self.strategy_mode}_{self.target_year}_{self.scan_period}"
            append_scan_log(self.session, f"⚙️ Inicjalizacja Optuny: {study_name}...")

            # Konfiguracja Samplera TPE
            sampler = optuna.samplers.TPESampler(
                n_startup_trials=min(10, max(5, int(n_trials/5))), 
                multivariate=False,
                group=False 
            )
            
            self.study = optuna.create_study(
                study_name=study_name,
                storage=self.storage_url,
                load_if_exists=True,
                direction='maximize',
                sampler=sampler
            )
            
            append_scan_log(self.session, f"🔥 Start symulacji ({n_trials} prób)...")
            
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
            
            # Wyciągamy statystyki najlepszej próby
            best_pf = best_trial.user_attrs.get("profit_factor", 0.0)
            best_trades = best_trial.user_attrs.get("trades", 0)
            
            end_msg = f"🏁 SUKCES! Najlepszy SmartScore: {best_value:.2f} (PF: {best_pf:.2f}, Transakcji: {best_trades})"
            append_scan_log(self.session, end_msg)
            
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            safe_params['strategy_mode'] = self.strategy_mode
            
            trials_data = self._collect_trials_data()
            sensitivity_report = self._run_sensitivity_analysis(trials_data)
            
            self._finalize_job(best_trial, sensitivity_report)

        except Exception as e:
            self.session.rollback()
            error_msg = f"❌ OPTIMIZER AWARIA: {str(e)}"
            logger.error(error_msg, exc_info=True)
            append_scan_log(self.session, error_msg)
            self._mark_job_failed()
            raise

    def _load_macro_context(self):
        append_scan_log(self.session, "📊 Ładowanie tła makroekonomicznego (Historycznego)...")
        macro = {
            'qqq_df': pd.DataFrame(), 
            'inflation_series': pd.Series(dtype=float),
            'yield_series': pd.Series(dtype=float),
            'fed_rate_series': pd.Series(dtype=float)
        }
        
        local_session = get_db_session()
        try:
            client = AlphaVantageClient()
            qqq_raw = get_raw_data_with_cache(local_session, client, 'QQQ', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
            if qqq_raw:
                macro['qqq_df'] = standardize_df_columns(pd.DataFrame.from_dict(qqq_raw.get('Time Series (Daily)', {}), orient='index'))
                macro['qqq_df'].index = pd.to_datetime(macro['qqq_df'].index)
                macro['qqq_df'].sort_index(inplace=True)
            
            from .backtest_engine import _parse_macro_to_series
            yield_raw = get_raw_data_with_cache(local_session, client, 'TREASURY_YIELD', 'TREASURY_YIELD', 'get_treasury_yield', interval='monthly')
            macro['yield_series'] = _parse_macro_to_series(yield_raw)
            inf_raw = get_raw_data_with_cache(local_session, client, 'INFLATION', 'INFLATION', 'get_inflation_rate')
            macro['inflation_series'] = _parse_macro_to_series(inf_raw)
            fed_raw = get_raw_data_with_cache(local_session, client, 'FEDERAL_FUNDS_RATE', 'FEDERAL_FUNDS_RATE', 'get_fed_funds_rate', interval='monthly')
            macro['fed_rate_series'] = _parse_macro_to_series(fed_raw)

        except Exception as e:
            append_scan_log(self.session, f"⚠️ Warning Makro: {e}")
        finally:
            local_session.close()
            
        return macro

    def _preload_data_to_cache_sequential(self):
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_DATA_LOAD')
        tickers = self._get_all_tickers()
        tickers = [t for t in tickers if t not in ['QQQ', 'SPY', 'IWM']]
        
        if not tickers:
            append_scan_log(self.session, "⚠️ Brak tickerów w bazie (Faza 1 pusta?).")
            return

        total_tickers = len(tickers)
        msg = f"🔄 Ładowanie danych i wstępne obliczenia dla {total_tickers} spółek..."
        logger.info(msg)
        append_scan_log(self.session, msg)
        
        loaded = 0
        errors = 0
        
        load_session = get_db_session()
        client = AlphaVantageClient()
        
        try:
            for i, ticker in enumerate(tickers):
                try:
                    success = self._load_single_ticker_data(load_session, client, ticker)
                    if success:
                        loaded += 1
                    else:
                        errors += 1
                except Exception as e:
                    errors += 1
                    logger.error(f"Critical error loading {ticker}: {e}")
                
                if (i + 1) % 10 == 0:
                    update_system_control(self.session, 'scan_progress_processed', str(i+1))
                    update_system_control(self.session, 'scan_progress_total', str(total_tickers))
        
        except Exception as e:
            append_scan_log(self.session, f"❌ Błąd pętli ładowania: {e}")
        finally:
            load_session.close()
        
        self.tickers_count = len(self.data_cache)
        summary = f"✅ Cache gotowy. Załadowano: {self.tickers_count}/{total_tickers} (Odrzucono: {errors})"
        logger.info(summary)
        append_scan_log(self.session, summary)

    def _load_single_ticker_data(self, session, client, ticker):
        daily_data = get_raw_data_with_cache(session, client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        if not daily_data:
            daily_data = get_raw_data_with_cache(session, client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
        if not daily_data: return False
        
        h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, client, session)
        weekly_df = pd.DataFrame()
        obv_df = pd.DataFrame()
        
        if self.strategy_mode == 'AQM':
            w_raw = get_raw_data_with_cache(session, client, ticker, 'WEEKLY_ADJUSTED', 'get_weekly_adjusted')
            if w_raw: weekly_df = standardize_df_columns(pd.DataFrame.from_dict(w_raw.get('Weekly Adjusted Time Series', {}), orient='index'))
            obv_raw = get_raw_data_with_cache(session, client, ticker, 'OBV', 'get_obv')
            if obv_raw:
                obv_df = pd.DataFrame.from_dict(obv_raw.get('Technical Analysis: OBV', {}), orient='index')
                if not obv_df.empty:
                    obv_df.index = pd.to_datetime(obv_df.index)
                    obv_df.rename(columns={'OBV': 'OBV'}, inplace=True)
        
        processed_df = self._preprocess_ticker_unified(daily_data, h2_data, weekly_df, obv_df)
        if not processed_df.empty:
            self.data_cache[ticker] = processed_df
            return True
        return False

    def _preprocess_ticker_unified(self, daily_data, h2_data, weekly_df, obv_df) -> pd.DataFrame:
        try:
            daily_df = standardize_df_columns(pd.DataFrame.from_dict(daily_data.get('Time Series (Daily)', {}), orient='index'))
            if len(daily_df) < 200: return pd.DataFrame()
            
            if not isinstance(daily_df.index, pd.DatetimeIndex):
                daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.index = daily_df.index.tz_localize(None) 
            daily_df.sort_index(inplace=True)
            daily_df['atr_14'] = calculate_atr(daily_df).ffill().fillna(0)

            if self.strategy_mode == 'H3':
                daily_df['price_gravity'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3 / daily_df['close'] - 1
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                daily_df['institutional_sync'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name) or 0.0, axis=1)
                daily_df['retail_herding'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name) or 0.0, axis=1)
                daily_df['daily_returns'] = daily_df['close'].pct_change().fillna(0)
                daily_df['market_temperature'] = daily_df['daily_returns'].rolling(window=30).std().fillna(0) 
                
                if not news_df.empty:
                    nc = news_df.groupby(news_df.index.date).size() 
                    nc.index = pd.to_datetime(nc.index)
                    nc = nc.reindex(daily_df.index, fill_value=0)
                    daily_df['information_entropy'] = nc.rolling(window=10).sum().fillna(0)
                else: daily_df['information_entropy'] = 0.0
                
                df_calc = aqm_v3_metrics.calculate_aqm_h3_vectorized(daily_df)
                df_calc['aqm_rank'] = df_calc['aqm_score_h3'].rolling(window=100, min_periods=20).rank(pct=True).fillna(0)
                result = df_calc[['open', 'high', 'low', 'close', 'atr_14', 'aqm_score_h3', 'aqm_rank', 'm_sq_norm']].fillna(0)
                if result.empty: return pd.DataFrame()
                return result

            elif self.strategy_mode == 'AQM':
                # (Logika AQM bez zmian)
                if not weekly_df.empty and isinstance(weekly_df.index, pd.DatetimeIndex): weekly_df.index = weekly_df.index.tz_localize(None)
                if not obv_df.empty and isinstance(obv_df.index, pd.DatetimeIndex): obv_df.index = obv_df.index.tz_localize(None)
                if weekly_df.empty:
                    weekly_df = daily_df.resample('W').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
                aqm_df = aqm_v4_logic.calculate_aqm_full_vector(daily_df=daily_df, weekly_df=weekly_df, intraday_60m_df=pd.DataFrame(), obv_df=obv_df, macro_data=self.macro_data, earnings_days_to=None)
                if aqm_df.empty: return pd.DataFrame()
                if 'atr' in aqm_df.columns: aqm_df['atr_14'] = aqm_df['atr']
                elif 'atr_14' not in aqm_df.columns: aqm_df['atr_14'] = daily_df['atr_14']
                req_cols = ['open', 'high', 'low', 'close', 'atr_14', 'aqm_score', 'qps', 'ras', 'vms', 'tcs']
                valid_cols = [c for c in req_cols if c in aqm_df.columns]
                return aqm_df[valid_cols].fillna(0)
            
            return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()

    def _objective(self, trial):
        params = {}
        if self.strategy_mode == 'H3':
            params = {
                'h3_percentile': trial.suggest_float('h3_percentile', 0.90, 0.99), 
                'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -1.0, 0.5), 
                'h3_min_score': trial.suggest_float('h3_min_score', -0.5, 0.4),
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 4.7, 20.0), 
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 2.0, 4.7),
                'h3_max_hold': trial.suggest_int('h3_max_hold', 2, 9), 
            }
        elif self.strategy_mode == 'AQM':
            params = {
                'aqm_min_score': trial.suggest_float('aqm_min_score', 0.60, 0.95),
                'aqm_vms_min': trial.suggest_float('aqm_vms_min', 0.30, 0.70),
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 3.0, 10.0),
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 2.0, 5.0),
                'h3_max_hold': trial.suggest_int('h3_max_hold', 2, 10),
            }

        # === WALIDACJA DATY (AUDYT) ===
        start_ts = pd.Timestamp(f"{self.target_year}-01-01")
        end_ts = pd.Timestamp(f"{self.target_year}-12-31")

        if self.scan_period == 'Q1': end_ts = pd.Timestamp(f"{self.target_year}-03-31")
        elif self.scan_period == 'Q2': start_ts = pd.Timestamp(f"{self.target_year}-04-01"); end_ts = pd.Timestamp(f"{self.target_year}-06-30")
        elif self.scan_period == 'Q3': start_ts = pd.Timestamp(f"{self.target_year}-07-01"); end_ts = pd.Timestamp(f"{self.target_year}-09-30")
        elif self.scan_period == 'Q4': start_ts = pd.Timestamp(f"{self.target_year}-10-01")
        
        # Logowanie diagnostyczne (tylko raz na proces, żeby nie spamować)
        if not self.debug_date_logged:
            logger.warning(f"🚨 DIAGNOSTYKA DAT: Wymuszony zakres symulacji: {start_ts} -> {end_ts}")
            append_scan_log(self.session, f"🚨 DIAGNOSTYKA DAT: Zakres {start_ts.date()} -> {end_ts.date()}")
            self.debug_date_logged = True

        result = self._run_simulation_unified(params, start_ts, end_ts)
        
        pf = result['profit_factor']
        trades = result['total_trades']
        score = pf * math.log10(trades + 1)
        
        trial.set_user_attr("profit_factor", pf)
        trial.set_user_attr("trades", trades)

        if trial.number % 5 == 0:
            logger.info(f"⚡ Trial {trial.number}: Score={score:.2f} (PF: {pf:.2f}, Trades: {trades})")

        if score > self.best_score_so_far:
            self.best_score_so_far = score
            self._update_best_score(score)

        self._save_trial(trial, params, pf, trades, score, result['win_rate'])
        
        if trades < 5: return 0.0
        return score

    def _run_simulation_unified(self, params, start_ts, end_ts):
        trades_pnl = []
        tp_mult = params['h3_tp_multiplier']
        sl_mult = params['h3_sl_multiplier']
        max_hold = params['h3_max_hold']
        
        debug_check_count = 0 

        for ticker, df in self.data_cache.items():
            if df.empty: continue
            
            # === FILTRACJA DATY ===
            mask_date = (df.index >= start_ts) & (df.index <= end_ts)
            sim_df = df[mask_date]
            
            # --- Sonda Diagnostyczna (Pierwsze 3 tickery w każdej iteracji) ---
            if debug_check_count < 3 and not sim_df.empty:
                first_date = sim_df.index[0]
                last_date = sim_df.index[-1]
                # Logujemy tylko jeśli debug mode jeszcze nie potwierdził dat dla tej próby
                # (tu uproszczone: logujemy co trial dla pewności w konsoli)
                # logger.info(f"DEBUG {ticker}: DataStart={first_date}, DataKoniec={last_date}, Wierszy={len(sim_df)}")
                debug_check_count += 1
            # -----------------------------------------------------------------
            
            if len(sim_df) < 2: continue
            
            entry_mask = None
            
            if self.strategy_mode == 'H3':
                h3_p = params['h3_percentile']
                h3_m = params['h3_m_sq_threshold']
                h3_min = params['h3_min_score']
                if 'aqm_score_h3' in sim_df.columns:
                    entry_mask = (
                        (sim_df['aqm_rank'] > h3_p) & 
                        (sim_df['m_sq_norm'] < h3_m) & 
                        (sim_df['aqm_score_h3'] > h3_min)
                    )
            elif self.strategy_mode == 'AQM':
                min_score = params['aqm_min_score']
                vms_min = params['aqm_vms_min']
                if 'aqm_score' in sim_df.columns:
                    entry_mask = (
                        (sim_df['aqm_score'] > min_score) &
                        (sim_df.get('vms', pd.Series(1.0)) > vms_min) &
                        (sim_df.get('tcs', pd.Series(1.0)) > 0.1)
                    )
            
            if entry_mask is None: continue

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
                
                if atr == 0 or entry_price == 0: continue
                
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
        except Exception as e:
            logger.error(f"Błąd pobierania tickerów: {e}")
            return []

    def _collect_trials_data(self):
        trials_data = []
        for t in self.study.trials:
            if t.state == optuna.trial.TrialState.COMPLETE:
                safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in t.params.items()}
                pf = t.user_attrs.get("profit_factor", float(t.value))
                trials_data.append({'params': safe_params, 'profit_factor': pf})
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
            safe_params['strategy_mode'] = self.strategy_mode 
            trial_record = models.OptimizationTrial(
                job_id=self.job_id, trial_number=trial.number, params=safe_params,
                profit_factor=safe_pf, total_trades=safe_trades, win_rate=safe_win_rate,
                net_profit=safe_score, 
                state='COMPLETE', created_at=datetime.now(timezone.utc)
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
            final_metrics = {
                'real_profit_factor': best_trial.user_attrs.get("profit_factor", 0.0),
                'real_trades': best_trial.user_attrs.get("trades", 0)
            }
            job.configuration = {
                'best_params': best_params, 
                'final_metrics': final_metrics,
                'sensitivity_analysis': sensitivity_report, 
                'version': 'V20_UNIFIED_PHYSICS', 
                'strategy': self.strategy_mode,
                'scan_period': self.scan_period, 
                'tickers_analyzed': self.tickers_count
            }
            self.session.commit()

    def _mark_job_failed(self):
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job: job.status = 'FAILED'; self.session.commit()
        except: self.session.rollback()
