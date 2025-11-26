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
    calculate_h3_metrics_v4,  # Pełna logika V5
    get_raw_data_with_cache   # Potrzebne do ładowania danych w wątkach
)
from . import aqm_v3_metrics 
from . import aqm_v3_h2_loader
from .apex_audit import SensitivityAnalyzer
from ..database import get_db_session 

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

# ==============================================================================
# === NOWA KLASA: AdaptiveExecutor (Brakujący element dla Phase 3 Sniper) ===
# ==============================================================================
class AdaptiveExecutor:
    """
    Moduł adaptacji parametrów w czasie rzeczywistym (Live).
    Dostosowuje sztywne parametry strategii do bieżącego reżimu rynkowego (VIX, Trend).
    """
    def __init__(self, base_params: dict):
        self.base_params = base_params

    def get_adapted_params(self, market_context: dict) -> dict:
        """
        Zwraca zmodyfikowane parametry na podstawie kontekstu rynkowego.
        market_context: {'vix': float, 'trend': 'BULL'/'BEAR'/'NEUTRAL'}
        """
        adapted = self.base_params.copy()
        
        vix = market_context.get('vix', 20.0)
        trend = market_context.get('trend', 'NEUTRAL')
        
        # 1. Adaptacja do Zmienności (VIX)
        # Jeśli VIX jest wysoki (>25), rynek jest "dziki".
        # Reakcja: Zwiększamy SL (żeby nie wyrzuciło na szumie) i zwiększamy TP (większy potencjał).
        if vix > 25.0:
            # Zwiększamy mnożniki ATR
            adapted['h3_sl_multiplier'] = adapted.get('h3_sl_multiplier', 2.0) * 1.5
            adapted['h3_tp_multiplier'] = adapted.get('h3_tp_multiplier', 5.0) * 1.2
            # Zaostrzamy kryteria wejścia (tylko pewne sygnały)
            adapted['h3_percentile'] = min(0.99, adapted.get('h3_percentile', 0.95) + 0.02)
            
        # Jeśli VIX jest niski (<15), rynek jest "ospały".
        # Reakcja: Zmniejszamy TP (mniejsze ruchy) i ewentualnie SL (mniejszy szum).
        elif vix < 15.0:
            adapted['h3_tp_multiplier'] = adapted.get('h3_tp_multiplier', 5.0) * 0.8
            adapted['h3_sl_multiplier'] = max(1.5, adapted.get('h3_sl_multiplier', 2.0) * 0.8)

        # 2. Adaptacja do Trendu (SPY)
        # Jeśli trend jest spadkowy (BEAR), a my gramy LONG (domyślnie w H3):
        if trend == 'BEAR':
            # Bardzo rygorystyczne wejścia
            adapted['h3_min_score'] = max(0.5, adapted.get('h3_min_score', 0.0))
            # Szybsze realizowanie zysków
            adapted['h3_tp_multiplier'] = adapted.get('h3_tp_multiplier', 5.0) * 0.7
            
        return adapted

# ==============================================================================
# === ISTNIEJĄCA KLASA: QuantumOptimizer (Bez zmian) ===
# ==============================================================================

class QuantumOptimizer:
    """
    SERCE SYSTEMU APEX V4/V5 - PRZYSPIESZENIE 20x+
    - Równoległa optymalizacja bayesowska
    - Cache'owanie danych w pamięci RAM
    - UŻYWA PEŁNEJ LOGIKI H3 (J - m^2 - nabla^2) DLA ZGODNOŚCI Z FAZĄ 3
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session 
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.best_score_so_far = -1.0
        self.data_cache = {}  
        
        logger.info(f"QuantumOptimizer V5 initialized for Job {job_id}")

    def run(self, n_trials: int = 1000):
        """
        Uruchamia główny proces optymalizacji.
        """
        start_msg = f"🚀 QUANTUM OPTIMIZER V5: Start {self.job_id} (Rok: {self.target_year}, Próby: {n_trials})"
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_INIT')
        
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            # KROK 1: Ładowanie danych (z logowaniem postępu)
            self._preload_data_to_cache()
            
            # KROK 2: Optymalizacja
            update_system_control(self.session, 'worker_status', 'OPTIMIZING_CALC')
            msg_calc = "✅ Dane w pamięci. Uruchamianie algorytmu genetycznego Optuna..."
            logger.info(msg_calc)
            append_scan_log(self.session, msg_calc)

            self.study = optuna.create_study(
                direction='maximize',
                sampler=optuna.samplers.TPESampler(
                    n_startup_trials=min(50, max(10, int(n_trials/5))), # Dynamiczny startup
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
            
            # KROK 3: Zapis wyników
            if len(self.study.trials) == 0:
                raise Exception("Brak udanych prób optymalizacji (0 trials completed).")

            best_trial = self.study.best_trial
            best_value = float(best_trial.value)
            
            end_msg = f"🏁 QUANTUM OPTIMIZER V5: Zakończono! Najlepszy Score: {best_value:.4f}"
            logger.info(end_msg)
            append_scan_log(self.session, end_msg)
            
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            append_scan_log(self.session, f"🏆 Zwycięskie Parametry:\n{json.dumps(safe_params, indent=2)}")

            # KROK 4: Analiza Wrażliwości
            append_scan_log(self.session, "📊 Generowanie analizy wrażliwości...")
            trials_data = self._collect_trials_data()
            sensitivity_report = self._run_sensitivity_analysis(trials_data)
            
            self._finalize_job(best_trial, sensitivity_report)
            append_scan_log(self.session, "✅ Zadanie zakończone pomyślnie.")

        except Exception as e:
            self.session.rollback()
            error_msg = f"❌ QUANTUM OPTIMIZER V5 AWARIA: {str(e)}"
            logger.error(error_msg, exc_info=True)
            append_scan_log(self.session, error_msg)
            self._mark_job_failed()
            raise

    def _preload_data_to_cache(self):
        """Ładuje dane do cache RAM i raportuje postęp do UI"""
        update_system_control(self.session, 'worker_status', 'OPTIMIZING_DATA_LOAD')
        msg = "🔄 PRZYSPIESZENIE: Rozpoczynam ładowanie danych i pre-kalkulację H3..."
        logger.info(msg)
        append_scan_log(self.session, msg)
        
        tickers = self._get_all_tickers()
        
        if not tickers:
            msg_err = "⚠️ OSTRZEŻENIE: Brak tickerów w bazie danych! Optymalizacja nie ma na czym pracować."
            logger.warning(msg_err)
            append_scan_log(self.session, msg_err)
            return

        # Ograniczamy tickery dla wydajności
        tickers_to_load = tickers[:200] 
        append_scan_log(self.session, f"Znaleziono {len(tickers)} tickerów. Ładowanie {len(tickers_to_load)} najaktywniejszych do pamięci RAM...")

        loaded_count = 0
        with ThreadPoolExecutor(max_workers=5) as executor: 
            futures = []
            for ticker in tickers_to_load:
                futures.append(executor.submit(self._load_ticker_data, ticker))
            
            for i, future in enumerate(as_completed(futures)):
                try:
                    future.result()
                    loaded_count += 1
                    if loaded_count % 50 == 0:
                        append_scan_log(self.session, f"   ... załadowano {loaded_count}/{len(tickers_to_load)} tickerów")
                except Exception as e:
                    logger.warning(f"Błąd w wątku ładowania: {e}")
        
        msg_done = f"✅ Cache gotowy. Załadowano {len(self.data_cache)} pełnych zestawów danych."
        logger.info(msg_done)
        append_scan_log(self.session, msg_done)

    def _load_ticker_data(self, ticker):
        """Ładuje i PRZETWARZA PEŁNE DANE dla tickera"""
        with get_db_session() as thread_session:
            try:
                api_client = backtest_engine.AlphaVantageClient()
                
                daily_data = get_raw_data_with_cache(
                    thread_session, api_client, ticker, 
                    'DAILY_OHLCV', 'get_time_series_daily', outputsize='full'
                )
                h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, thread_session)
                
                if daily_data and h2_data:
                    processed_df = self._preprocess_ticker_full_h3(daily_data, h2_data)
                    if not processed_df.empty:
                        self.data_cache[ticker] = processed_df
                        
            except Exception as e:
                logger.error(f"Błąd w wątku load_ticker_data ({ticker}): {e}")

    def _get_all_tickers(self):
        """Pobiera tickery"""
        try:
            query = text("""
                (SELECT ticker FROM phase1_candidates)
                UNION 
                (SELECT ticker FROM portfolio_holdings)
                UNION
                (SELECT ticker FROM companies LIMIT 300)
                LIMIT 300
            """)
            result = self.session.execute(query)
            return [r[0] for r in result]
        except Exception as e:
            logger.error(f"Błąd pobierania tickerów: {e}")
            return []

    def _preprocess_ticker_full_h3(self, daily_data, h2_data) -> pd.DataFrame:
        """
        TWORZY PEŁNY DATAFRAME ZGODNY Z FAZĄ 3 (LIVE).
        """
        try:
            daily_df = standardize_df_columns(
                pd.DataFrame.from_dict(daily_data.get('Time Series (Daily)', {}), orient='index')
            )
            if len(daily_df) < 100: return pd.DataFrame()
            
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.sort_index(inplace=True)

            daily_df['atr_14'] = calculate_atr(daily_df).ffill().fillna(0)
            daily_df['price_gravity'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3 / daily_df['close'] - 1
            
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')
            
            daily_df['institutional_sync'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            daily_df['retail_herding'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            
            daily_df['daily_returns'] = daily_df['close'].pct_change()
            daily_df['market_temperature'] = daily_df['daily_returns'].rolling(window=30).std()
            
            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(daily_df.index, fill_value=0)
                daily_df['information_entropy'] = news_counts.rolling(window=10).sum()
            else:
                daily_df['information_entropy'] = 0.0
            
            daily_df['avg_volume_10d'] = daily_df['volume'].rolling(window=10).mean()
            daily_df['vol_mean_200d'] = daily_df['avg_volume_10d'].rolling(window=200).mean()
            daily_df['vol_std_200d'] = daily_df['avg_volume_10d'].rolling(window=200).std()
            daily_df['normalized_volume'] = ((daily_df['avg_volume_10d'] - daily_df['vol_mean_200d']) / daily_df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
            
            daily_df['normalized_news'] = 0.0 
            
            daily_df['m_sq'] = daily_df['normalized_volume'] + daily_df['normalized_news']
            daily_df['nabla_sq'] = daily_df['price_gravity']

            daily_df = calculate_h3_metrics_v4(daily_df, {}) 
            
            return daily_df.fillna(0)
            
        except Exception as e:
            return pd.DataFrame()

    def _objective(self, trial):
        """FUNKCJA CELU"""
        params = {
            'h3_percentile': trial.suggest_float('h3_percentile', 0.85, 0.98),
            'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -1.0, 0.0),
            'h3_min_score': trial.suggest_float('h3_min_score', 0.0, 2.0),
            'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 3.0, 8.0),
            'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.5, 4.0),
            'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 10),
        }

        start_date = f"{self.target_year}-01-01"
        end_date = f"{self.target_year}-12-31"
        
        try:
            sim_res = self._run_fast_simulation(params, start_date, end_date)
            pf = sim_res.get('profit_factor', 0.0)
            trades = sim_res.get('total_trades', 0)
            
            if trades < 50: return 0.0 
            
            final_score = pf 
            
            if trial.number % 10 == 0:
                log_msg = f"🔸 Próba {trial.number}: PF={pf:.2f}, Trades={trades}"
                append_scan_log(self.session, log_msg)

            if final_score > self.best_score_so_far:
                self.best_score_so_far = final_score
                self._update_best_score(final_score)

            self._save_trial(trial, params, pf, trades, final_score)
            return float(final_score)

        except Exception as e:
            return 0.0

    def _run_fast_simulation(self, params, start_date, end_date):
        trades_results = []
        tickers = list(self.data_cache.keys())
        
        for ticker in tickers:
            df = self.data_cache[ticker]
            if df.empty: continue
            
            try:
                current_thresholds = df['aqm_score_h3'].rolling(window=100).quantile(params['h3_percentile'])
                
                start_idx = df.index.searchsorted(pd.Timestamp(start_date))
                end_idx = df.index.searchsorted(pd.Timestamp(end_date))
                
                if start_idx >= end_idx: continue
                
                for i in range(start_idx, min(end_idx, len(df) - 1)):
                    score = df.iloc[i]['aqm_score_h3']
                    threshold = current_thresholds.iloc[i]
                    m_norm = df.iloc[i]['m_sq_norm'] 
                    
                    if pd.isna(score) or pd.isna(threshold): continue
                    
                    if (score > threshold) and \
                       (m_norm < params['h3_m_sq_threshold']) and \
                       (score > params['h3_min_score']):
                        
                        entry_price = df.iloc[i + 1]['open']
                        atr = df.iloc[i]['atr_14']
                        
                        if pd.isna(entry_price) or atr == 0: continue
                        
                        tp = entry_price + params['h3_tp_multiplier'] * atr
                        sl = entry_price - params['h3_sl_multiplier'] * atr
                        
                        pnl = 0.0
                        for j in range(1, params['h3_max_hold'] + 1):
                            if i + j >= len(df): break
                            candle = df.iloc[i + j]
                            
                            if candle['low'] <= sl:
                                pnl = (sl - entry_price) / entry_price
                                break
                            elif candle['high'] >= tp:
                                pnl = (tp - entry_price) / entry_price
                                break
                            elif j == params['h3_max_hold']:
                                pnl = (candle['close'] - entry_price) / entry_price
                        
                        trades_results.append(pnl * 100)
            except Exception:
                continue

        return self._calculate_stats(trades_results)

    def _calculate_stats(self, trades):
        if not trades: return {'profit_factor': 0.0, 'total_trades': 0}
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        total_win = sum(wins)
        total_loss = abs(sum(losses))
        pf = total_win / total_loss if total_loss > 0 else 0.0
        return {'profit_factor': pf, 'total_trades': len(trades), 'net_profit': sum(trades)}

    def _collect_trials_data(self):
        trials_data = []
        for t in self.study.trials:
            if t.state == optuna.trial.TrialState.COMPLETE:
                safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in t.params.items()}
                trials_data.append({'params': safe_params, 'profit_factor': float(t.value) if t.value is not None else 0.0})
        return trials_data

    def _run_sensitivity_analysis(self, trials_data):
        if len(trials_data) < 20: return {}
        try:
            analyzer = SensitivityAnalyzer()
            return analyzer.analyze_parameter_sensitivity(trials_data)
        except Exception as e:
            logger.error(f"Błąd analizy wrażliwości: {e}")
            return {}

    def _update_best_score(self, score):
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.best_score = float(score) if score is not None else 0.0
                self.session.commit()
        except Exception: self.session.rollback()

    def _save_trial(self, trial, params, pf, trades, score):
        try:
            safe_pf = float(pf) if pf is not None and not np.isnan(pf) else 0.0
            safe_trades = int(trades) if trades is not None else 0
            safe_score = float(score) if score is not None and not np.isnan(score) else 0.0
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in params.items()}

            trial_record = models.OptimizationTrial(
                job_id=self.job_id, trial_number=trial.number, params=safe_params,
                profit_factor=safe_pf, total_trades=safe_trades, win_rate=0.0, net_profit=0.0,
                state='COMPLETE' if safe_score > 0 else 'PRUNED', created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            if trial.number % 10 == 0: self.session.commit()
        except Exception: self.session.rollback()

    def _finalize_job(self, best_trial, sensitivity_report):
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'COMPLETED'
            job.best_score = float(best_trial.value) if best_trial.value is not None else 0.0
            best_params_safe = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            final_config = {'best_params': best_params_safe, 'sensitivity_analysis': sensitivity_report, 'optimization_version': 'V5_FULL_PHYSICS', 'total_trials_processed': len(self.study.trials)}
            job.configuration = final_config
            self.session.commit()

    def _mark_job_failed(self):
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job: job.status = 'FAILED'; self.session.commit()
        except: self.session.rollback()
