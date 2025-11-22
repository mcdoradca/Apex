import logging
import optuna
from optuna.samplers import TPESampler
from optuna.pruners import HyperbandPruner
from optuna.exceptions import TrialPruned
import json
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timezone

# Importy wewnętrzne
from .. import models
from . import backtest_engine
from .utils import update_system_control, append_scan_log
from .apex_audit import SensitivityAnalyzer

logger = logging.getLogger(__name__)

optuna.logging.set_verbosity(optuna.logging.WARNING)

class OptimizationCallback:
    """
    Callback dla Optuny, który aktualizuje status w bazie danych po każdej próbie.
    Dzięki temu UI "żyje" i pokazuje postęp (np. 5/50).
    """
    def __init__(self, session: Session, job_id: str, total_trials: int):
        self.session = session
        self.job_id = job_id
        self.total_trials = total_trials

    def __call__(self, study, trial):
        try:
            # Obliczamy postęp
            completed = len(study.trials)
            best_val = study.best_value if len(study.trials) > 0 else 0.0
            
            # Aktualizujemy system_control (dla szybkiego podglądu w UI)
            progress_msg = f"Próba {completed}/{self.total_trials}. Najlepszy wynik: {best_val:.4f}"
            update_system_control(self.session, 'optimization_progress', progress_msg)
            
            # Aktualizujemy rekord Job
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.best_score = float(best_val)
                # Możemy tu zapisać też % postępu w polu statusu lub customowym
                self.session.commit()
                
        except Exception as e:
            logger.error(f"Błąd w callbacku optymalizacji: {e}")

class QuantumOptimizer:
    """
    Serce systemu Apex V4 (Advanced - Turbo).
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.preloaded_data = None

    def run(self, n_trials: int = 50):
        logger.info(f"QuantumOptimizer: Start zadania {self.job_id} (Rok: {self.target_year}, Próby: {n_trials})")
        
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        # Reset licznika postępu w UI
        update_system_control(self.session, 'optimization_progress', "Inicjalizacja danych...")
        
        try:
            logger.info("QuantumOptimizer: Wstępne ładowanie danych do pamięci RAM...")
            self.preloaded_data = backtest_engine.preload_optimization_data(self.session, str(self.target_year))
            
            if not self.preloaded_data:
                raise Exception("Nie udało się załadować danych do optymalizacji (pusty cache).")

            update_system_control(self.session, 'optimization_progress', "Dane załadowane. Start silnika Optuna...")

            sampler = TPESampler(seed=42, n_startup_trials=10) 
            pruner = HyperbandPruner(min_resource=1, max_resource=4, reduction_factor=3)
            
            self.study = optuna.create_study(
                direction='maximize',
                sampler=sampler,
                pruner=pruner
            )
            
            # Callbacks do aktualizacji UI
            callbacks = [OptimizationCallback(self.session, self.job_id, n_trials)]
            
            self.study.optimize(self._objective, n_trials=n_trials, timeout=3600, callbacks=callbacks)
            
            if len(self.study.trials) == 0:
                raise Exception("Brak zakończonych prób optymalizacji.")

            best_trial = self.study.best_trial
            best_params = best_trial.params
            best_value = best_trial.value
            
            logger.info(f"QuantumOptimizer: Zakończono. Najlepszy Wynik: {best_value:.4f}")
            update_system_control(self.session, 'optimization_progress', "Analiza wrażliwości...")

            # Analiza Wrażliwości
            trials_data = []
            for t in self.study.trials:
                if t.state == optuna.trial.TrialState.COMPLETE:
                    row = {'params': t.params, 'profit_factor': t.value} 
                    trials_data.append(row)
            
            sensitivity_report = {}
            try:
                if len(trials_data) >= 10:
                    analyzer = SensitivityAnalyzer()
                    sensitivity_report = analyzer.analyze_parameter_sensitivity(trials_data)
            except Exception as e:
                logger.error(f"Błąd analizy wrażliwości: {e}")

            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.status = 'COMPLETED'
                job.best_score = float(best_value)
                
                final_config = {
                    'best_params': best_params,
                    'sensitivity_analysis': sensitivity_report
                }
                job.configuration = final_config
                
                best_trial_db = self.session.query(models.OptimizationTrial).filter(
                    models.OptimizationTrial.job_id == self.job_id,
                    models.OptimizationTrial.trial_number == best_trial.number
                ).first()
                if best_trial_db:
                    job.best_trial_id = best_trial_db.id
                
                self.session.commit()
            
            update_system_control(self.session, 'optimization_progress', "Zakończono pomyślnie.")
            self.preloaded_data = None

        except Exception as e:
            logger.error(f"QuantumOptimizer: Błąd krytyczny: {e}", exc_info=True)
            update_system_control(self.session, 'optimization_progress', f"BŁĄD: {str(e)}")
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.status = 'FAILED'
                self.session.commit()
            self.preloaded_data = None

    def _objective(self, trial):
        params = {
            'h3_percentile': trial.suggest_float('h3_percentile', 0.90, 0.99),
            'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -1.5, 0.5),
            'h3_min_score': trial.suggest_float('h3_min_score', -1.0, 2.0),
            'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 8.0),
            'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.0, 4.0),
            'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 10),
            'min_inst_sync': trial.suggest_float('min_inst_sync', -0.5, 0.5),
            'max_retail_herding': trial.suggest_float('max_retail_herding', 0.5, 3.0),
        }

        periods = [
            (f"{self.target_year}-01-01", f"{self.target_year}-03-31"),
            (f"{self.target_year}-04-01", f"{self.target_year}-06-30"),
            (f"{self.target_year}-07-01", f"{self.target_year}-09-30"),
            (f"{self.target_year}-10-01", f"{self.target_year}-12-31")
        ]
        
        period_pfs = []
        total_trades_year = 0
        total_profit_year = 0.0
        cumulative_pf_sum = 0.0

        # Flaga sukcesu (czy dotrwaliśmy do końca bez pruning)
        is_pruned = False

        try:
            for step, (start_date, end_date) in enumerate(periods):
                period_params = params.copy()
                period_params['simulation_start_date'] = start_date
                period_params['simulation_end_date'] = end_date
                
                sim_res = backtest_engine.run_optimization_simulation_fast(
                    self.preloaded_data,
                    period_params
                )
                
                pf = sim_res.get('profit_factor', 0.0)
                trades = sim_res.get('total_trades', 0)
                
                if trades == 0: pf = 0.0
                
                period_pfs.append(pf)
                total_trades_year += trades
                total_profit_year += sim_res.get('net_profit', 0.0)
                cumulative_pf_sum += pf

                current_mean_pf = cumulative_pf_sum / (step + 1)
                trial.report(current_mean_pf, step)
                
                if trial.should_prune():
                    is_pruned = True
                    raise TrialPruned()

            # === DEBUGOWANIE i OBLICZENIA ===
            mean_pf = np.mean(period_pfs) if period_pfs else 0.0
            std_pf = np.std(period_pfs) if period_pfs else 0.0
            robust_score = mean_pf / (1.0 + std_pf)
            global_pf = mean_pf

            # Logowanie dla diagnostyki (Zgodnie z instrukcją użytkownika)
            logger.info(f"Trial {trial.number}: PF={mean_pf:.3f}, Std={std_pf:.3f}, Trades={total_trades_year}, Robust={robust_score:.3f}")

            # KROK 2: Wymaganie minimalnej liczby transakcji (zwiększone do 20)
            if total_trades_year < 20: 
                robust_score = 0.0
                global_pf = 0.0
                return 0.0 # Odrzucamy próbę

        except TrialPruned:
            # Zapisz próbę jako PRUNED, aby była widoczna w UI!
            is_pruned = True
            robust_score = 0.0 
            global_pf = 0.0
            # Kontynuujemy do zapisu w bazie
            
        except Exception as e:
            logger.error(f"Błąd w symulacji trial: {e}")
            raise

        # Zapis do bazy (również dla PRUNED)
        try:
            trial_record = models.OptimizationTrial(
                job_id=self.job_id,
                trial_number=trial.number,
                params=params,
                profit_factor=global_pf if not is_pruned else None,
                total_trades=total_trades_year,
                win_rate=0.0, 
                net_profit=total_profit_year,
                state='PRUNED' if is_pruned else 'COMPLETE', 
                created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            self.session.commit()
        except Exception:
            self.session.rollback()

        if is_pruned:
            raise TrialPruned()

        # KROK 1: Tymczasowo używamy zwykłego Profit Factor (mean_pf) jako celu zamiast robust_score
        return mean_pf

class AdaptiveExecutor:
    # (Bez zmian)
    def __init__(self, base_params: dict):
        self.base_params = base_params
        self.adaptation_rules = {
            'HIGH_VOLATILITY': { 
                'h3_percentile': lambda p: min(0.99, p * 1.01),
                'h3_sl_multiplier': lambda p: p * 1.3,
                'h3_m_sq_threshold': lambda p: p - 0.2
            },
            'LOW_VOLATILITY': {
                'h3_percentile': lambda p: max(0.90, p * 0.99),
                'h3_tp_multiplier': lambda p: p * 1.1
            },
            'BEAR_MARKET': {
                'h3_tp_multiplier': lambda p: p * 0.8,
                'h3_min_score': lambda p: max(0.5, p + 0.5)
            }
        }

    def get_adapted_params(self, market_conditions: dict) -> dict:
        adapted = self.base_params.copy()
        
        vix = market_conditions.get('vix', 20.0)
        if vix > 25.0:
            self._apply_rules(adapted, 'HIGH_VOLATILITY')
        elif vix < 15.0:
            self._apply_rules(adapted, 'LOW_VOLATILITY')
            
        trend = market_conditions.get('trend', 'NEUTRAL')
        if trend == 'BEAR':
            self._apply_rules(adapted, 'BEAR_MARKET')
            
        return adapted

    def _apply_rules(self, params_dict, rule_key):
        rules = self.adaptation_rules.get(rule_key, {})
        for param, modifier_func in rules.items():
            if param in params_dict:
                try:
                    original_val = float(params_dict[param])
                    new_val = modifier_func(original_val)
                    params_dict[param] = new_val
                except Exception:
                    pass
