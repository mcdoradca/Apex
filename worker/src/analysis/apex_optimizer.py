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

# Importy wewnętrzne
from .. import models
from . import backtest_engine
# Importujemy klienta bezpośrednio
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    update_system_control, 
    append_scan_log, 
    calculate_atr,
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
    SERCE SYSTEMU APEX V19.0 - TOTAL REPAIR
    1. H3 LOGIC RESTORED: Pełna reimplementacja logiki H3 wewnątrz klasy (niezależność od aqm_v4_logic).
    2. CACHE SYNC: Poprawiona kolejność ładowania danych (zgodność z Fazą 1).
    3. DIAGNOSTICS: Szczegółowe raportowanie przyczyn odrzucenia tickerów.
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session 
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.best_score_so_far = -1.0
        self.data_cache = {}  
        self.tickers_count = 0
        
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
        self.scan_period = self.job_config.get('scan_period', 'FULL') 
        
        logger.info(f"QuantumOptimizer initialized: Job {job_id}, Mode {self.strategy_mode}")

    def run(self, n_trials: int = 50):
        start_msg = f"🚀 OPTIMIZER V19: Start Zadania {self.job_id} (Strategia: {self.strategy_mode})..."
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        
        # Aktualizacja statusu
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            # 1. Makro
            self.macro_data = self._load_macro_context()
            
            # 2. Cache Danych (SEKWENCYJNIE Z PEŁNĄ DIAGNOSTYKĄ)
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
            
            end_msg = f"🏁 SUKCES! Najlepszy PF: {best_value:.4f}"
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
        append_scan_log(self.session, "📊 Ładowanie tła makroekonomicznego...")
        # Domyślne wartości (SAFE DEFAULTS)
        macro = {'vix': 20.0, 'yield_10y': 4.0, 'inflation': 2.5, 'fed_rate': 5.0, 'spy_df': pd.DataFrame()}
        
        local_session = get_db_session()
        try:
            client = AlphaVantageClient()
            # Próba pobrania QQQ z cache (Faza 1 często to ma)
            spy_raw = get_raw_data_with_cache(local_session, client, 'QQQ', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
            if spy_raw:
                macro['spy_df'] = standardize_df_columns(pd.DataFrame.from_dict(spy_raw.get('Time Series (Daily)', {}), orient='index'))
                macro['spy_df'].index = pd.to_datetime(macro['spy_df'].index)
                macro['spy_df'].sort_index(inplace=True)
            
            if self.strategy_mode == 'AQM':
                # Pobieranie wskaźników makro (z fallbackiem na brak danych)
                yield_raw = get_raw_data_with_cache(local_session, client, 'TREASURY_YIELD', 'TREASURY_YIELD', 'get_treasury_yield', interval='monthly')
                if yield_raw and 'data' in yield_raw:
                    try: macro['yield_10y'] = float(yield_raw['data'][0]['value'])
                    except: pass
                
                inf_raw = get_raw_data_with_cache(local_session, client, 'INFLATION', 'INFLATION', 'get_inflation_rate')
                if inf_raw and 'data' in inf_raw:
                    try: macro['inflation'] = float(inf_raw['data'][0]['value'])
                    except: pass
                
                fed_raw = get_raw_data_with_cache(local_session, client, 'FEDERAL_FUNDS_RATE', 'FEDERAL_FUNDS_RATE', 'get_fed_funds_rate', interval='monthly')
                if fed_raw and 'data' in fed_raw:
                    try: macro['fed_rate'] = float(fed_raw['data'][0]['value'])
                    except: pass

        except Exception as e:
            append_scan_log(self.session, f"⚠️ Warning Makro: {e}")
        finally:
            local_session.close()
            
        return macro

    def _preload_data_to_cache_sequential(self):
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_DATA_LOAD')
        tickers = self._get_all_tickers()
        
        if not tickers:
            append_scan_log(self.session, "⚠️ Brak tickerów w bazie (Faza 1 pusta?).")
            return

        total_tickers = len(tickers)
        msg = f"🔄 Ładowanie danych dla {total_tickers} spółek..."
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
        # 1. PRIORYTET: Cache Fazy 1 (DAILY_ADJUSTED)
        daily_data = get_raw_data_with_cache(session, client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        
        # Fallback: Jeśli nie ma adjusted, próbujemy OHLCV
        if not daily_data:
            daily_data = get_raw_data_with_cache(session, client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
        
        if not daily_data: 
            return False
        
        # 2. Pobierz dane H2 (Insider/News)
        h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, client, session)
        
        weekly_df = pd.DataFrame()
        obv_df = pd.DataFrame()
        
        # 3. Pobierz dane dodatkowe (tylko dla AQM)
        if self.strategy_mode == 'AQM':
            w_raw = get_raw_data_with_cache(session, client, ticker, 'WEEKLY_ADJUSTED', 'get_weekly_adjusted')
            if w_raw: 
                weekly_df = standardize_df_columns(pd.DataFrame.from_dict(w_raw.get('Weekly Adjusted Time Series', {}), orient='index'))
            
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
            
            if len(daily_df) < 50: return pd.DataFrame()
            
            if not isinstance(daily_df.index, pd.DatetimeIndex):
                daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.index = daily_df.index.tz_localize(None) 
            
            daily_df.sort_index(inplace=True)
            daily_df['atr_14'] = calculate_atr(daily_df).ffill().fillna(0)

            if self.strategy_mode == 'H3':
                # === H3 LOGIC RESTORED & HARDENED (SELF-CONTAINED) ===
                # Niezależna implementacja, odporna na zmiany w aqm_v4_logic.py
                
                # 1. Price Gravity
                daily_df['price_gravity'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3 / daily_df['close'] - 1
                
                # 2. H2 Data Integration
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')
                
                daily_df['institutional_sync'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name) or 0.0, axis=1)
                daily_df['retail_herding'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name) or 0.0, axis=1)
                
                # 3. Market Temp
                daily_df['daily_returns'] = daily_df['close'].pct_change().fillna(0)
                daily_df['market_temperature'] = daily_df['daily_returns'].rolling(window=30).std().fillna(0) 
                
                # 4. Entropy
                if not news_df.empty:
                    nc = news_df.groupby(news.index.date).size() 
                    nc.index = pd.to_datetime(nc.index)
                    nc = nc.reindex(daily_df.index, fill_value=0)
                    daily_df['information_entropy'] = nc.rolling(window=10).sum().fillna(0)
                else: daily_df['information_entropy'] = 0.0
                
                # 5. Volume M^2
                daily_df['avg_volume_10d'] = daily_df['volume'].rolling(window=10).mean().fillna(0)
                # Używamy min_periods, aby nie tracić danych na starcie
                daily_df['vol_mean_200d'] = daily_df['avg_volume_10d'].rolling(window=200, min_periods=20).mean().fillna(0)
                daily_df['vol_std_200d'] = daily_df['avg_volume_10d'].rolling(window=200, min_periods=20).std().fillna(1)
                
                daily_df['normalized_volume'] = ((daily_df['avg_volume_10d'] - daily_df['vol_mean_200d']) / daily_df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
                daily_df['m_sq'] = daily_df['normalized_volume'] 
                daily_df['nabla_sq'] = daily_df['price_gravity']
                
                # 6. AQM V3 FORMULA (Explicitly implemented here)
                # Mu Normalized
                daily_df['mu_normalized'] = (daily_df['institutional_sync'] - daily_df['institutional_sync'].rolling(100, min_periods=20).mean()) / daily_df['institutional_sync'].rolling(100, min_periods=20).std().fillna(1)
                daily_df['mu_normalized'] = daily_df['mu_normalized'].fillna(0)
                
                # Retail Cap
                daily_df['retail_herding_capped'] = daily_df['retail_herding'].clip(-1.0, 1.0)
                
                # J Calculation
                S = daily_df['information_entropy']
                Q = daily_df['retail_herding_capped']
                T = daily_df['market_temperature'].replace(0, np.nan)
                mu = daily_df['mu_normalized']
                
                daily_df['J'] = (S - (Q/T) + mu).fillna(0)
                
                # Component Normalization
                j_mean = daily_df['J'].rolling(100, min_periods=20).mean()
                j_std = daily_df['J'].rolling(100, min_periods=20).std().fillna(1)
                daily_df['J_norm'] = ((daily_df['J'] - j_mean) / j_std).fillna(0)
                
                nab_mean = daily_df['nabla_sq'].rolling(100, min_periods=20).mean()
                nab_std = daily_df['nabla_sq'].rolling(100, min_periods=20).std().fillna(1)
                daily_df['nabla_sq_norm'] = ((daily_df['nabla_sq'] - nab_mean) / nab_std).fillna(0)
                
                m_mean = daily_df['m_sq'].rolling(100, min_periods=20).mean()
                m_std = daily_df['m_sq'].rolling(100, min_periods=20).std().fillna(1)
                daily_df['m_sq_norm'] = ((daily_df['m_sq'] - m_mean) / m_std).fillna(0)
                
                # FINAL AQM H3 SCORE
                daily_df['aqm_score_h3'] = (daily_df['J_norm'] * 1.0) - (daily_df['nabla_sq_norm'] * 1.0) - (daily_df['m_sq_norm'] * 1.0)
                
                # Rank
                daily_df['aqm_rank'] = daily_df['aqm_score_h3'].rolling(window=100, min_periods=20).rank(pct=True).fillna(0)
                
                result = daily_df[['open', 'high', 'low', 'close', 'atr_14', 'aqm_score_h3', 'aqm_rank', 'm_sq_norm']].fillna(0)
                
                # Verification (Optional logging could go here)
                if result.empty: return pd.DataFrame()
                return result

            elif self.strategy_mode == 'AQM':
                # === AQM V2.0 Logic ===
                if not weekly_df.empty and isinstance(weekly_df.index, pd.DatetimeIndex):
                    weekly_df.index = weekly_df.index.tz_localize(None)
                if not obv_df.empty and isinstance(obv_df.index, pd.DatetimeIndex):
                    obv_df.index = obv_df.index.tz_localize(None)

                # Fallback dla Weekly/OBV
                if weekly_df.empty:
                    weekly_df = daily_df.resample('W').agg({
                        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
                    }).dropna()

                aqm_df = aqm_v4_logic.calculate_aqm_full_vector(
                    daily_df=daily_df,
                    weekly_df=weekly_df,
                    intraday_60m_df=pd.DataFrame(), 
                    obv_df=obv_df,
                    macro_data=self.macro_data,
                    earnings_days_to=None
                )
                
                if aqm_df.empty: return pd.DataFrame()
                
                if 'atr' in aqm_df.columns: 
                    aqm_df['atr_14'] = aqm_df['atr']
                elif 'atr_14' not in aqm_df.columns:
                    aqm_df['atr_14'] = daily_df['atr_14']

                req_cols = ['open', 'high', 'low', 'close', 'atr_14', 'aqm_score', 'qps', 'ras', 'vms', 'tcs']
                
                if not all(col in aqm_df.columns for col in req_cols):
                    return pd.DataFrame()
                    
                return aqm_df[req_cols].fillna(0)
            
            return pd.DataFrame()
        except Exception as e:
            # logger.error(f"Preprocessing Error: {e}")
            return pd.DataFrame()

    def _objective(self, trial):
        params = {}
        
        if self.strategy_mode == 'H3':
            # === POPRAWIONE ZAKRESY H3 (Max 9 dni, SL min 3.0) ===
            params = {
                'h3_percentile': trial.suggest_float('h3_percentile', 0.85, 0.99), 
                'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -3.0, 3.0), 
                'h3_min_score': trial.suggest_float('h3_min_score', -0.5, 1.5),
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 10.0), 
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 3.0, 6.0), # Minimum 3.0
                'h3_max_hold': trial.suggest_int('h3_max_hold', 2, 9), # Maksimum 9 dni
            }
            
        elif self.strategy_mode == 'AQM':
            # === POPRAWIONE ZAKRESY AQM (Max 9 dni, SL min 3.0) ===
            params = {
                'aqm_min_score': trial.suggest_float('aqm_min_score', 0.75, 0.95),
                'aqm_vms_min': trial.suggest_float('aqm_vms_min', 0.40, 0.80),
                'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 3.0, 8.0),
                'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 3.0, 6.0), # Minimum 3.0
                'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 9), # Maksimum 9 dni
            }

        start_ts = pd.Timestamp(f"{self.target_year}-01-01")
        end_ts = pd.Timestamp(f"{self.target_year}-12-31")

        if self.scan_period == 'Q1': end_ts = pd.Timestamp(f"{self.target_year}-03-31")
        elif self.scan_period == 'Q2': start_ts = pd.Timestamp(f"{self.target_year}-04-01"); end_ts = pd.Timestamp(f"{self.target_year}-06-30")
        elif self.scan_period == 'Q3': start_ts = pd.Timestamp(f"{self.target_year}-07-01"); end_ts = pd.Timestamp(f"{self.target_year}-09-30")
        elif self.scan_period == 'Q4': start_ts = pd.Timestamp(f"{self.target_year}-10-01")
        
        result = self._run_simulation_unified(params, start_ts, end_ts)
        
        pf = result['profit_factor']
        trades = result['total_trades']
        
        if trial.number % 5 == 0:
            logger.info(f"⚡ Trial {trial.number}: PF={pf:.2f} (T: {trades})")

        if pf > self.best_score_so_far:
            self.best_score_so_far = pf
            self._update_best_score(pf)

        self._save_trial(trial, params, pf, trades, pf, result['win_rate'])
        
        if trades < 3: return 0.0
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
            
            entry_mask = None
            if self.strategy_mode == 'H3':
                h3_p = params['h3_percentile']
                h3_m = params['h3_m_sq_threshold']
                h3_min = params['h3_min_score']
                
                # Używamy kolumn wyliczonych lokalnie
                if 'aqm_score_h3' in sim_df.columns:
                    entry_mask = ((sim_df['aqm_rank'] > h3_p) & (sim_df['m_sq_norm'] < h3_m) & (sim_df['aqm_score_h3'] > h3_min))
            
            elif self.strategy_mode == 'AQM':
                # === LOGIKA WEJŚCIA AQM V2 ===
                min_score = params['aqm_min_score']
                vms_min = params['aqm_vms_min']
                
                if 'ras' in sim_df.columns:
                    entry_mask = (
                        (sim_df['aqm_score'] > min_score) &
                        (sim_df['ras'] > 0.5) & 
                        (sim_df['tcs'] > 0.5) &
                        (sim_df['vms'] > vms_min)
                    )
                else:
                    entry_mask = (sim_df['aqm_score'] > min_score)
            
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
        except Exception as e:
            logger.error(f"Błąd pobierania tickerów: {e}")
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
            
            safe_params['strategy_mode'] = self.strategy_mode 

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
                'version': 'V19_TOTAL_REPAIR', 
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
