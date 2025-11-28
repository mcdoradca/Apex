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

# Importy wewnętrzne
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
# Import logiki V4 (AQM)
from . import aqm_v4_logic
from .apex_audit import SensitivityAnalyzer
from ..database import get_db_session 

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

class QuantumOptimizer:
    """
    SERCE SYSTEMU APEX V13 - HYBRID MODE (H3 + AQM V4)
    - Pełna integracja AQM Logic zgodnie z PDF.
    - Pobieranie rzeczywistych danych Makro (Yields, Inflation) dla MRS.
    - Zoptymalizowany loader danych (unikający rate-limitów).
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session 
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.best_score_so_far = -1.0
        self.data_cache = {}  
        self.tickers_count = 0
        
        # Pobierz konfigurację zadania
        self.job_config = {}
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job and job.configuration:
                self.job_config = job.configuration
        except: pass
        
        self.strategy_mode = self.job_config.get('strategy', 'H3') 
        
        logger.info(f"QuantumOptimizer V13 initialized for Job {job_id} (Mode: {self.strategy_mode})")

    def run(self, n_trials: int = 50):
        start_msg = f"🚀 OPTIMIZER V13 ({self.strategy_mode}): Start {self.job_id} (Rok: {self.target_year}, Próby: {n_trials})"
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_INIT')
        
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            # 1. Pobierz kontekst makro (dla AQM)
            self.macro_data = self._load_macro_context()
            
            # 2. Załaduj dane spółek do RAM
            self._preload_data_to_cache()
            
            if not self.data_cache:
                raise Exception("Brak danych w cache. Sprawdź, czy Faza 1 zwróciła wyniki lub czy API działa.")

            update_system_control(self.session, 'worker_status', 'OPTIMIZING_CALC')
            msg_calc = f"✅ Dane w RAM ({len(self.data_cache)} spółek). Uruchamianie Optuny ({self.strategy_mode})..."
            logger.info(msg_calc)
            append_scan_log(self.session, msg_calc)

            self.study = optuna.create_study(
                direction='maximize',
                sampler=optuna.samplers.TPESampler(
                    n_startup_trials=min(10, max(5, int(n_trials/5))), 
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
            
            end_msg = f"🏁 ZAKOŃCZONO! Najlepszy PF ({self.strategy_mode}): {best_value:.4f}"
            logger.info(end_msg)
            append_scan_log(self.session, end_msg)
            
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            safe_params['strategy_mode'] = self.strategy_mode
            
            append_scan_log(self.session, f"🏆 Zwycięskie Parametry:\n{json.dumps(safe_params, indent=2)}")

            trials_data = self._collect_trials_data()
            sensitivity_report = self._run_sensitivity_analysis(trials_data)
            
            self._finalize_job(best_trial, sensitivity_report)

        except Exception as e:
            self.session.rollback()
            error_msg = f"❌ OPTIMIZER V13 AWARIA: {str(e)}"
            logger.error(error_msg, exc_info=True)
            append_scan_log(self.session, error_msg)
            self._mark_job_failed()
            raise

    def _load_macro_context(self):
        """
        Pobiera PEŁNE dane makroekonomiczne wymagane przez AQM MRS Score.
        """
        append_scan_log(self.session, "📊 Pobieranie danych Makro (SPY, Yields, Inflation)...")
        macro = {'vix': 20.0, 'yield_10y': 4.0, 'inflation': 3.0, 'spy_df': pd.DataFrame()}
        
        with get_db_session() as session:
            client = backtest_engine.AlphaVantageClient()
            
            # 1. SPY Data (Trend Rynkowy)
            spy_raw = get_raw_data_with_cache(session, client, 'SPY', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
            if spy_raw:
                macro['spy_df'] = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
                macro['spy_df'].index = pd.to_datetime(macro['spy_df'].index)
                macro['spy_df'].sort_index(inplace=True)
            
            # 2. Treasury Yield 10Y (Koszt kapitału) - Tylko dla AQM
            if self.strategy_mode == 'AQM':
                yield_raw = get_raw_data_with_cache(session, client, 'TREASURY_YIELD', 'TREASURY_YIELD', 'get_treasury_yield', interval='monthly')
                if yield_raw and 'data' in yield_raw:
                    try:
                        latest = float(yield_raw['data'][0]['value'])
                        macro['yield_10y'] = latest
                        logger.info(f"Macro: Treasury Yield 10Y = {latest}%")
                    except: pass

                # 3. Inflation (Inflacja) - Tylko dla AQM
                inf_raw = get_raw_data_with_cache(session, client, 'INFLATION', 'INFLATION', 'get_inflation_rate')
                if inf_raw and 'data' in inf_raw:
                    try:
                        latest = float(inf_raw['data'][0]['value'])
                        macro['inflation'] = latest
                        logger.info(f"Macro: Inflation = {latest}%")
                    except: pass
                    
        return macro

    def _preload_data_to_cache(self):
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_DATA_LOAD')
        msg = f"🔄 V13 PRELOAD: Pobieranie danych dla trybu {self.strategy_mode}..."
        logger.info(msg)
        append_scan_log(self.session, msg)
        
        tickers = self._get_all_tickers()
        # USUNIĘTO SZTYWNY LIMIT [:1000] - teraz bierzemy wszystko co dała Faza 1
        tickers_to_load = tickers 
        
        total_tickers = len(tickers_to_load)
        if total_tickers == 0:
            append_scan_log(self.session, "⚠️ OSTRZEŻENIE: Brak tickerów do załadowania. Uruchom najpierw Skaner Fazy 1.")
            return

        # Zmniejszamy liczbę workerów do 4, aby uniknąć Rate Limitów przy ciężkim AQM (4 requesty na ticker)
        max_workers = 4 
        
        processed = 0
        loaded = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor: 
            futures = {executor.submit(self._load_ticker_data, ticker): ticker for ticker in tickers_to_load}
            
            for f in as_completed(futures):
                processed += 1
                ticker = futures[f]
                try:
                    result = f.result()
                    if result:
                        loaded += 1
                except Exception as e:
                    logger.error(f"Błąd ładowania {ticker}: {e}")
                
                if processed % 10 == 0:
                    update_system_control(self.session, 'scan_progress_processed', str(processed))
                    update_system_control(self.session, 'scan_progress_total', str(total_tickers))
        
        self.tickers_count = len(self.data_cache)
        append_scan_log(self.session, f"✅ Cache gotowy: {self.tickers_count} / {total_tickers} spółek załadowanych poprawnie.")

    def _load_ticker_data(self, ticker):
        """
        Ładuje dane dla pojedynczego tickera w osobnym wątku.
        """
        with get_db_session() as thread_session:
            try:
                # Delikatny sleep, żeby rozłożyć zapytania w czasie (unikanie kolizji rate limit)
                time.sleep(0.1) 
                
                api_client = backtest_engine.AlphaVantageClient()
                
                # 1. Daily OHLCV (Baza)
                daily_data = get_raw_data_with_cache(thread_session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                if not daily_data: return False

                # 2. H2 Data (Insider/News) - Wspólne
                h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, thread_session)
                
                # 3. Dane specyficzne dla AQM (Weekly, OBV)
                weekly_df = pd.DataFrame()
                obv_df = pd.DataFrame()
                
                if self.strategy_mode == 'AQM':
                    # Weekly
                    w_raw = get_raw_data_with_cache(thread_session, api_client, ticker, 'WEEKLY_ADJUSTED', 'get_weekly_adjusted')
                    if w_raw: 
                        weekly_df = standardize_df_columns(pd.DataFrame.from_dict(w_raw.get('Weekly Adjusted Time Series', {}), orient='index'))
                    
                    # OBV (Kluczowe dla VES)
                    obv_raw = get_raw_data_with_cache(thread_session, api_client, ticker, 'OBV', 'get_obv')
                    if obv_raw:
                        obv_df = pd.DataFrame.from_dict(obv_raw.get('Technical Analysis: OBV', {}), orient='index')
                        obv_df.index = pd.to_datetime(obv_df.index)
                        obv_df.rename(columns={'OBV': 'OBV'}, inplace=True)

                # Przetwarzanie
                processed_df = self._preprocess_ticker_unified(daily_data, h2_data, weekly_df, obv_df)
                
                if not processed_df.empty:
                    self.data_cache[ticker] = processed_df
                    return True
                return False
                
            except Exception as e:
                # logger.error(f"Error loading {ticker}: {e}")
                return False

    def _preprocess_ticker_unified(self, daily_data, h2_data, weekly_df, obv_df) -> pd.DataFrame:
        try:
            # 1. Podstawa (OHLCV)
            daily_df = standardize_df_columns(pd.DataFrame.from_dict(daily_data.get('Time Series (Daily)', {}), orient='index'))
            # Potrzebujemy minimum historii
            if len(daily_df) < 100: return pd.DataFrame()
            
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.sort_index(inplace=True)
            daily_df['atr_14'] = calculate_atr(daily_df).ffill().fillna(0)

            # 2. Ścieżka H3 (Stara logika - dla kompatybilności)
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
                else:
                    daily_df['information_entropy'] = 0.0
                
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

            # 3. Ścieżka AQM (V4 Logic - Pełna zgodność z PDF)
            elif self.strategy_mode == 'AQM':
                # Wywołujemy logikę AQM, przekazując PRAWDZIWE dane makro
                aqm_df = aqm_v4_logic.calculate_aqm_full_vector(
                    daily_df=daily_df,
                    weekly_df=weekly_df,
                    intraday_60m_df=pd.DataFrame(), # Intraday opcjonalne (PDF: "użyj daily jeśli brak intraday")
                    obv_df=obv_df,
                    macro_data=self.macro_data,
                    earnings_days_to=None
                )
                
                if 'atr' in aqm_df.columns:
                    aqm_df['atr_14'] = aqm_df['atr']
                
                # Zwracamy kolumny potrzebne do symulacji
                req_cols = ['open', 'high', 'low', 'close', 'atr_14', 'aqm_score', 'qps', 'ves', 'mrs', 'tcs']
                return aqm_df[req_cols].dropna()

            return pd.DataFrame()
            
        except Exception:
            return pd.DataFrame()

    def _objective(self, trial):
        params = {}
        
        # === DEFINICJA PRZESTRZENI PARAMETRÓW ===
        
        if self.strategy_mode == 'H3':
            params = {
                'h3_percentile': trial.suggest_float('h3_percentile', 0.90, 0.99, step=0.01), 
                'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -2.0, 0.0, step=0.05), 
                'h3_min_score': trial.suggest_float('h3_min_score', 0.0, 1.0, step=0.05),
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 8.0, step=0.5),
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.0, 4.0, step=0.25),
                'h3_max_hold': trial.suggest_int('h3_max_hold', 2, 10),
            }
            
        elif self.strategy_mode == 'AQM':
            # Parametry z PDF
            params = {
                # Progi wejścia dla AQM
                'aqm_min_score': trial.suggest_float('aqm_min_score', 0.60, 0.90, step=0.05),
                'aqm_component_min': trial.suggest_float('aqm_component_min', 0.3, 0.7, step=0.1),
                
                # Zarządzanie pozycją
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 8.0, step=0.5),
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.5, 4.0, step=0.25),
                'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 10),
            }

        start_ts = pd.Timestamp(f"{self.target_year}-01-01")
        end_ts = pd.Timestamp(f"{self.target_year}-12-31")
        
        result = self._run_simulation_unified(params, start_ts, end_ts)
        
        pf = result['profit_factor']
        trades = result['total_trades']
        
        # Logowanie postępów (co 5 prób)
        if trial.number % 5 == 0:
            logger.info(f"⚡ Trial {trial.number}: PF={pf:.2f} (Trades: {trades})")

        if pf > self.best_score_so_far:
            self.best_score_so_far = pf
            self._update_best_score(pf)

        self._save_trial(trial, params, pf, trades, pf, result['win_rate'])
        
        # Kara za brak transakcji (żeby nie optymalizował pustych strategii)
        if trades < 5: return 0.0 
        return pf

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
            
            # === LOGIKA WEJŚCIA ===
            entry_mask = None
            
            if self.strategy_mode == 'H3':
                h3_p = params['h3_percentile']
                h3_m = params['h3_m_sq_threshold']
                h3_min = params['h3_min_score']
                entry_mask = (
                    (sim_df['aqm_rank'] > h3_p) & 
                    (sim_df['m_sq_norm'] < h3_m) & 
                    (sim_df['aqm_score_h3'] > h3_min)
                )
            
            elif self.strategy_mode == 'AQM':
                min_score = params['aqm_min_score']
                comp_min = params['aqm_component_min']
                
                cond_main = (sim_df['aqm_score'] > min_score)
                # PDF: "Wszystkie komponenty > 0.6" (tu parametryzowane)
                cond_comps = (
                    (sim_df['qps'] > comp_min) &
                    (sim_df['ves'] > comp_min) &
                    (sim_df['mrs'] > comp_min)
                )
                entry_mask = cond_main & cond_comps

            # === SYMULACJA ===
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
        """
        Pobiera tickery priorytetowo z wyników Fazy 1.
        """
        try:
            # 1. Priorytet: Kandydaci Fazy 1 (To jest lejek!)
            res_p1 = self.session.execute(text("SELECT ticker FROM phase1_candidates")).fetchall()
            tickers_p1 = [r[0] for r in res_p1]
            
            if len(tickers_p1) > 0:
                logger.info(f"Optimizer: Wybrano {len(tickers_p1)} tickerów z Fazy 1 (Skaner).")
                return tickers_p1
            
            # 2. Fallback: Jeśli Faza 1 pusta, weź 100 z bazy (do testów)
            logger.warning("Optimizer: Faza 1 pusta. Pobieram próbkę 100 tickerów z bazy.")
            res_all = self.session.execute(text("SELECT ticker FROM companies LIMIT 100")).fetchall()
            return [r[0] for r in res_all]
            
        except Exception as e: 
            logger.error(f"Error getting tickers: {e}")
            return []

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
                'version': 'V13_HYBRID', 
                'strategy': self.strategy_mode,
                'tickers_analyzed': self.tickers_count
            }
            self.session.commit()

    def _mark_job_failed(self):
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job: job.status = 'FAILED'; self.session.commit()
        except: self.session.rollback()
