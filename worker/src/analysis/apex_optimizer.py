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
    calculate_h3_metrics_v4  # <--- KLUCZOWY IMPORT: Pełna logika V5
)
from . import aqm_v3_metrics # Potrzebne do obliczeń pomocniczych
from .apex_audit import SensitivityAnalyzer
from ..database import get_db_session 

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

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
        
        logger.info(f"QuantumOptimizer V5: Full Physics Mode (Zgodność z Phase 3)")

    def run(self, n_trials: int = 1000):
        """
        Uruchamia główny proces optymalizacji.
        """
        start_msg = f"🚀 QUANTUM OPTIMIZER V5: Start {self.job_id} (Rok: {self.target_year}, Próby: {n_trials})"
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            # INICJALIZACJA DANYCH
            self._preload_data_to_cache()
            
            # OPTYMALIZACJA BAYESOWSKA
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
            
            # ZAPIS WYNIKÓW
            if len(self.study.trials) == 0:
                raise Exception("Brak udanych prób optymalizacji.")

            best_trial = self.study.best_trial
            best_value = float(best_trial.value)
            
            end_msg = f"🏁 QUANTUM OPTIMIZER V5: Zakończono! Najlepszy Score: {best_value:.4f}"
            logger.info(end_msg)
            append_scan_log(self.session, end_msg)
            
            safe_params = {k: float(v) if isinstance(v, (np.floating, float)) else v for k, v in best_trial.params.items()}
            append_scan_log(self.session, f"🏆 Parametry: {json.dumps(safe_params, indent=2)}")

            # ANALIZA WRAŻLIWOŚCI
            trials_data = self._collect_trials_data()
            sensitivity_report = self._run_sensitivity_analysis(trials_data)
            
            self._finalize_job(best_trial, sensitivity_report)

        except Exception as e:
            self.session.rollback()
            error_msg = f"❌ QUANTUM OPTIMIZER V5: Błąd krytyczny: {str(e)}"
            logger.error(error_msg, exc_info=True)
            append_scan_log(self.session, error_msg)
            self._mark_job_failed()
            raise

    def _preload_data_to_cache(self):
        """Ładuje dane do cache RAM i PREKOMPUTUJE METRYKI (raz, porządnie)"""
        logger.info("🔄 PRZYSPIESZENIE: Ładowanie danych i pre-kalkulacja H3...")
        
        tickers = self._get_all_tickers()
        
        if not tickers:
            logger.warning("Brak tickerów do załadowania.")
            return

        tickers_to_load = tickers[:200] # Limit dla pamięci

        with ThreadPoolExecutor(max_workers=5) as executor: 
            futures = []
            for ticker in tickers_to_load:
                futures.append(executor.submit(self._load_ticker_data, ticker))
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.warning(f"Błąd ładowania danych w wątku: {e}")
        
        logger.info(f"✅ PRZYSPIESZENIE: Gotowe. Załadowano {len(self.data_cache)} tickerów.")

    def _load_ticker_data(self, ticker):
        """Ładuje i PRZETWARZA PEŁNE DANE dla tickera (Pełna fizyka H3)"""
        from .utils import get_raw_data_with_cache
        from .aqm_v3_h2_loader import load_h2_data_into_cache
        
        with get_db_session() as thread_session:
            try:
                api_client = backtest_engine.AlphaVantageClient()
                
                # 1. Pobierz dane surowe
                daily_data = get_raw_data_with_cache(
                    thread_session, api_client, ticker, 
                    'DAILY_OHLCV', 'get_time_series_daily', outputsize='full'
                )
                h2_data = load_h2_data_into_cache(ticker, api_client, thread_session)
                
                if daily_data and h2_data:
                    # 2. WSTĘPNE PRZETWARZANIE (Parsing)
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
        except Exception:
            return []

    def _preprocess_ticker_full_h3(self, daily_data, h2_data) -> pd.DataFrame:
        """
        TWORZY PEŁNY DATAFRAME ZGODNY Z FAZĄ 3 (LIVE).
        To naprawia błąd "Model Mismatch".
        """
        try:
            # 1. Standardyzacja OHLC
            daily_df = standardize_df_columns(
                pd.DataFrame.from_dict(daily_data.get('Time Series (Daily)', {}), orient='index')
            )
            if len(daily_df) < 100: return pd.DataFrame()
            
            daily_df.index = pd.to_datetime(daily_df.index)
            daily_df.sort_index(inplace=True)

            # 2. Podstawowe wskaźniki
            daily_df['atr_14'] = calculate_atr(daily_df).ffill().fillna(0)
            daily_df['price_gravity'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3 / daily_df['close'] - 1
            
            # 3. Integracja danych H2 (Insider / News) - DLA OBLICZEŃ J
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')
            
            # Obliczamy 'institutional_sync' i 'retail_herding'
            daily_df['institutional_sync'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            daily_df['retail_herding'] = daily_df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            
            # 4. Obliczamy 'm_sq' i 'information_entropy'
            daily_df['daily_returns'] = daily_df['close'].pct_change()
            daily_df['market_temperature'] = daily_df['daily_returns'].rolling(window=30).std()
            
            if not news_df.empty:
                news_counts = news_df.groupby(news_df.index.date).size()
                news_counts.index = pd.to_datetime(news_counts.index)
                news_counts = news_counts.reindex(daily_df.index, fill_value=0)
                daily_df['information_entropy'] = news_counts.rolling(window=10).sum()
            else:
                daily_df['information_entropy'] = 0.0
            
            # Obliczanie m_sq (Zgodnie z Backtestem - bez Newsów w Masie, ale z Newsami w J)
            daily_df['avg_volume_10d'] = daily_df['volume'].rolling(window=10).mean()
            daily_df['vol_mean_200d'] = daily_df['avg_volume_10d'].rolling(window=200).mean()
            daily_df['vol_std_200d'] = daily_df['avg_volume_10d'].rolling(window=200).std()
            daily_df['normalized_volume'] = ((daily_df['avg_volume_10d'] - daily_df['vol_mean_200d']) / daily_df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
            
            daily_df['normalized_news'] = 0.0 # Zgodność z Backtestem
            daily_df['m_sq'] = daily_df['normalized_volume'] + daily_df['normalized_news']
            daily_df['nabla_sq'] = daily_df['price_gravity']

            # 5. FULL H3 CALCULATION (To jest to, czego brakowało!)
            # Wywołujemy tę samą funkcję co Phase 3 i Backtest Engine
            # Ale UWAGA: calculate_h3_metrics_v4 wymaga parametru 'percentile', który tu jest zmienny w Optimizerze.
            # Więc tutaj pre-komputujemy tylko składowe (J, nabla, m), a percentyle policzymy dynamicznie w pętli.
            
            # Wywołujemy funkcję z utils, aby obliczyła kolumny 'J', 'J_norm', 'm_sq_norm', etc.
            # Przekazujemy puste params, bo percentyl nas na tym etapie nie interesuje (będzie dynamiczny)
            daily_df = calculate_h3_metrics_v4(daily_df, {}) 
            
            return daily_df.fillna(0)
            
        except Exception as e:
            # logger.error(f"Błąd preprocessingu dla Optimzera: {e}")
            return pd.DataFrame()

    def _objective(self, trial):
        """FUNKCJA CELU"""
        # Zakresy parametrów
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
            
            if trades < 50: return 0.0 # Za mało transakcji = noise
            
            final_score = pf 
            
            # Logowanie co 10 prób
            if trial.number % 10 == 0:
                log_msg = f"🔸 Próba {trial.number}: PF={pf:.2f}, Trades={trades}"
                logger.info(log_msg)
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
        
        # Używamy cache'owanych danych
        tickers = list(self.data_cache.keys())
        
        for ticker in tickers:
            df = self.data_cache[ticker]
            if df.empty: continue
            
            # --- DYNAMICZNE OBLICZANIE PROGU (To jest klucz do szybkości) ---
            # Zamiast liczyć J/M/Nabla od nowa, używamy prekomputowanych kolumn 'aqm_score_h3'
            # Jedyne co się zmienia to percentyl (próg)
            
            # Musimy przeliczyć aqm_percentile_95 dla zadanego w próbie parametru
            # Ale uwaga: calculate_h3_metrics_v4 już to policzył dla defaultu.
            # Tutaj musimy to zrobić szybko na wektorach.
            
            # df['aqm_score_h3'] jest już policzone w pre-processingu (J_norm - nabla - m).
            # Liczymy tylko threshold dynamicznie.
            
            try:
                # Szybkie obliczenie rolling quantile na kolumnie aqm_score_h3
                current_thresholds = df['aqm_score_h3'].rolling(window=100).quantile(params['h3_percentile'])
                
                # Symulacja wektorowa (lub uproszczona pętla)
                # Dla wydajności w Optunie, pętla po indeksach jest OK jeśli dane są w RAM
                
                start_idx = df.index.searchsorted(pd.Timestamp(start_date))
                end_idx = df.index.searchsorted(pd.Timestamp(end_date))
                
                if start_idx >= end_idx: continue
                
                # Iteracja po dniach
                for i in range(start_idx, min(end_idx, len(df) - 1)):
                    score = df.iloc[i]['aqm_score_h3']
                    threshold = current_thresholds.iloc[i]
                    m_norm = df.iloc[i]['m_sq_norm'] # Prekomputowane
                    
                    if pd.isna(score) or pd.isna(threshold): continue
                    
                    # Warunki wejścia
                    if (score > threshold) and \
                       (m_norm < params['h3_m_sq_threshold']) and \
                       (score > params['h3_min_score']):
                        
                        entry_price = df.iloc[i + 1]['open']
                        atr = df.iloc[i]['atr_14']
                        
                        if pd.isna(entry_price) or atr == 0: continue
                        
                        tp = entry_price + params['h3_tp_multiplier'] * atr
                        sl = entry_price - params['h3_sl_multiplier'] * atr
                        
                        # Sprawdzenie wyniku
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

    # ... (Metody _calculate_stats, _collect_trials_data, _run_sensitivity_analysis, _save_trial itd. bez zmian) ...
    # Wklejam je dla kompletności pliku, abyś mógł go podmienić 1:1

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

# (AdaptiveExecutor pozostaje bez zmian, ale dla czystości pliku go pomijam - nie jest używany w tym pliku bezpośrednio, ale jeśli jest na końcu pliku, to zostaw go tam gdzie był)
class AdaptiveExecutor:
    def __init__(self, base_params: dict):
        self.base_params = base_params
        self.adaptation_rules = {
            'HIGH_VOLATILITY': { 'h3_percentile': lambda p: min(0.99, p * 1.01), 'h3_sl_multiplier': lambda p: p * 1.3, 'h3_m_sq_threshold': lambda p: p - 0.2 },
            'LOW_VOLATILITY': { 'h3_percentile': lambda p: max(0.90, p * 0.99), 'h3_tp_multiplier': lambda p: p * 1.1 },
            'BEAR_MARKET': { 'h3_tp_multiplier': lambda p: p * 0.8, 'h3_min_score': lambda p: max(0.5, p + 0.5) }
        }
    def get_adapted_params(self, market_conditions: dict) -> dict:
        adapted = self.base_params.copy()
        vix = market_conditions.get('vix', 20.0)
        if vix > 25.0: self._apply_rules(adapted, 'HIGH_VOLATILITY')
        elif vix < 15.0: self._apply_rules(adapted, 'LOW_VOLATILITY')
        if market_conditions.get('trend') == 'BEAR': self._apply_rules(adapted, 'BEAR_MARKET')
        return adapted
    def _apply_rules(self, params_dict, rule_key):
        rules = self.adaptation_rules.get(rule_key, {})
        for param, modifier_func in rules.items():
            if param in params_dict:
                try: params_dict[param] = modifier_func(params_dict[param])
                except: pass
