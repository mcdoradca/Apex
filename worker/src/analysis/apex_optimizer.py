import logging
import optuna
import json
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timezone

# Importy wewnętrzne
from .. import models
from . import backtest_engine
from .utils import update_system_control, append_scan_log
# Importujemy narzędzia analityczne
from .apex_audit import SensitivityAnalyzer

logger = logging.getLogger(__name__)

# Wyłączamy nadmierne logowanie Optuny na konsolę
optuna.logging.set_verbosity(optuna.logging.WARNING)

class QuantumOptimizer:
    """
    Serce systemu Apex V4 (Advanced).
    Wykorzystuje Optymalizację Bayesowską (TPE) oraz Multi-Period Validation.
    
    ZMIANA V4.1: Nowa funkcja celu (Objective Function) zgodna z "Tajemniczą Rozmową".
    Zamiast szukać tylko 'Robust Score', szukamy balansu między wysokim PF 
    a optymalną liczbą transakcji (500-2000 rocznie).
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session
        self.job_id = job_id
        self.target_year = target_year
        self.study = None

    def run(self, n_trials: int = 50):
        """
        Uruchamia główny proces optymalizacji.
        """
        logger.info(f"QuantumOptimizer: Start zadania {self.job_id} (Rok: {self.target_year}, Próby: {n_trials})")
        
        # 1. Aktualizacja statusu zadania na RUNNING
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            # 2. Utworzenie badania (Study) Optuna
            # direction='maximize' ponieważ chcemy maksymalizować Score (PF po karach)
            self.study = optuna.create_study(direction='maximize')
            
            # 3. Uruchomienie pętli optymalizacyjnej
            self.study.optimize(self._objective, n_trials=n_trials)
            
            # 4. Zapisanie najlepszych wyników
            best_trial = self.study.best_trial
            best_params = best_trial.params
            best_value = best_trial.value
            
            logger.info(f"QuantumOptimizer: Zakończono. Najlepszy Wynik (Score): {best_value:.4f}")
            logger.info(f"Najlepsze parametry: {best_params}")

            # 5. Analiza Wrażliwości (Automatyczny Audyt V4)
            trials_data = []
            for t in self.study.trials:
                if t.state == optuna.trial.TrialState.COMPLETE:
                    row = {'params': t.params, 'profit_factor': t.value} 
                    trials_data.append(row)
            
            sensitivity_report = {}
            try:
                if len(trials_data) >= 10:
                    logger.info("Uruchamianie analizy wrażliwości (SensitivityAnalyzer)...")
                    analyzer = SensitivityAnalyzer()
                    sensitivity_report = analyzer.analyze_parameter_sensitivity(trials_data)
                else:
                    logger.warning("Za mało prób (<10) do rzetelnej analizy wrażliwości.")
            except Exception as e:
                logger.error(f"Błąd podczas analizy wrażliwości: {e}", exc_info=True)

            # 6. Aktualizacja rekordu Job w bazie
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

        except Exception as e:
            logger.error(f"QuantumOptimizer: Błąd krytyczny w procesie optymalizacji: {e}", exc_info=True)
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.status = 'FAILED'
                self.session.commit()
            raise e

    def _objective(self, trial):
        """
        ZMODYFIKOWANA Funkcja Celu (V4.1).
        
        Zasady z "Tajemniczej Rozmowy":
        1.  Wymuszamy zakres transakcji: 500 - 2000.
            - Jeśli mniej lub więcej -> Score = 0.0 (odrzucamy).
        2.  Idealny cel ("Sweet Spot"): 800 - 1200 transakcji.
        3.  Score = Średni PF * (1.0 - Kara_za_odchylenie_od_celu).
        """
        
        # === A. Definicja Przestrzeni Parametrów ===
        params = {
            'h3_percentile': trial.suggest_float('h3_percentile', 0.85, 0.99), # Szerszy zakres
            'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -2.0, 0.0),
            'h3_min_score': trial.suggest_float('h3_min_score', -1.0, 3.0),
            'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 10.0),
            'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.0, 5.0),
            'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 15),
        }

        # === B. Symulacja Kwartalna (Multi-Period) ===
        periods = [
            (f"{self.target_year}-01-01", f"{self.target_year}-03-31"),
            (f"{self.target_year}-04-01", f"{self.target_year}-06-30"),
            (f"{self.target_year}-07-01", f"{self.target_year}-09-30"),
            (f"{self.target_year}-10-01", f"{self.target_year}-12-31")
        ]
        
        period_pfs = []
        total_trades_year = 0
        total_profit_year = 0.0

        for start_date, end_date in periods:
            try:
                period_params = params.copy()
                period_params['simulation_start_date'] = start_date
                period_params['simulation_end_date'] = end_date
                
                sim_res = backtest_engine.run_optimization_simulation(
                    self.session,
                    str(self.target_year),
                    period_params
                )
                
                pf = sim_res.get('profit_factor', 0.0)
                trades = sim_res.get('total_trades', 0)
                
                if trades == 0:
                    pf = 0.0
                
                period_pfs.append(pf)
                total_trades_year += trades
                total_profit_year += sim_res.get('net_profit', 0.0)
                
            except Exception:
                period_pfs.append(0.0)

        # === C. Obliczenie Nowego Score (Logic V4.1) ===
        
        mean_pf = np.mean(period_pfs)
        
        # 1. TWARDY LIMIT TRANSAKCJI
        # Jeśli system generuje mniej niż 500 lub więcej niż 2000 transakcji rocznie, 
        # uznajemy to za błąd strategii (zbyt pasywna lub zbyt agresywna/śmieciowa).
        if total_trades_year < 500 or total_trades_year > 2000:
            final_score = 0.0
            
            # Logowanie odrzucenia (dla debugowania - widoczne w logach workera)
            # logger.info(f"Trial {trial.number} REJECTED: Trades={total_trades_year} (Out of bounds 500-2000)")
        
        else:
            # 2. OBLICZANIE KARY (Trade Penalty)
            # Idealny zakres ("Sweet Spot"): 800 - 1200 transakcji
            trade_penalty = 0.0
            
            if total_trades_year < 800:
                # Kara rośnie im bliżej 500. Przy 500 kara wynosi ok. 37% z (800-500)/800
                trade_penalty = (800 - total_trades_year) / 800.0
            elif total_trades_year > 1200:
                # Kara rośnie im bliżej 2000.
                trade_penalty = (total_trades_year - 1200) / 1200.0
            
            # Maksymalna kara to 50% wyniku (aby wysoki PF nadal miał znaczenie)
            impact_factor = 0.5 
            
            # 3. FINALNY SCORE
            # Bierzemy średni PF i odejmujemy karę.
            # Przykład: PF 2.0, idealna liczba transakcji -> Score = 2.0
            # Przykład: PF 2.0, liczba transakcji 600 (kara ~0.25 * 0.5 = 0.125) -> Score = 2.0 * 0.875 = 1.75
            final_score = mean_pf * (1.0 - (trade_penalty * impact_factor))
            
            logger.info(f"Trial {trial.number}: PF={mean_pf:.2f}, Trades={total_trades_year}, Penalty={trade_penalty:.2f}, Score={final_score:.3f}")

        # === D. Zapis Próby do Bazy Danych ===
        try:
            trial_record = models.OptimizationTrial(
                job_id=self.job_id,
                trial_number=trial.number,
                params=params,
                profit_factor=mean_pf, # Zapisujemy rzeczywisty PF, nie Score!
                total_trades=total_trades_year,
                win_rate=0.0, # Uproszczenie
                net_profit=total_profit_year,
                state='COMPLETE' if final_score > 0 else 'PRUNED', # PRUNED jeśli odrzucony przez limit
                created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            self.session.commit()
        except Exception as db_err:
            logger.error(f"Błąd zapisu próby do DB: {db_err}")
            self.session.rollback()

        return final_score

class AdaptiveExecutor:
    """
    Mózg operacyjny Apex V4.
    Dostosowuje parametry w czasie rzeczywistym w oparciu o VIX i Trend.
    """
    
    def __init__(self, base_params: dict):
        self.base_params = base_params
        # Reguły adaptacji
        self.adaptation_rules = {
            'HIGH_VOLATILITY': { # VIX > 25
                'h3_percentile': lambda p: min(0.99, p * 1.01),
                'h3_sl_multiplier': lambda p: p * 1.3,
                'h3_m_sq_threshold': lambda p: p - 0.2
            },
            'LOW_VOLATILITY': { # VIX < 15
                'h3_percentile': lambda p: max(0.90, p * 0.99),
                'h3_tp_multiplier': lambda p: p * 1.1
            },
            'BEAR_MARKET': { # Trend spadkowy
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
                    original_val = params_dict[param]
                    new_val = modifier_func(original_val)
                    params_dict[param] = new_val
                except Exception:
                    pass
