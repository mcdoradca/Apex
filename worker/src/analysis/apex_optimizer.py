import logging
import optuna
import json
import numpy as np
from sqlalchemy.orm import Session
from datetime import datetime, timezone

# Importy wewnętrzne
from .. import models
from . import backtest_engine  # Będziemy korzystać z silnika backtestu w trybie symulacji
from .utils import update_system_control, append_scan_log

logger = logging.getLogger(__name__)

# Wyłączamy nadmierne logowanie Optuny na konsolę
optuna.logging.set_verbosity(optuna.logging.WARNING)

class QuantumOptimizer:
    """
    Serce systemu Apex V4.
    Wykorzystuje Optymalizację Bayesowską (TPE) do przeszukiwania wielowymiarowej
    przestrzeni parametrów strategii H3 w celu znalezienia konfiguracji
    odpornych na zmiany rynkowe (Anty-kruchość).
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
            # direction='maximize' ponieważ chcemy maksymalizować Profit Factor / Robust Score
            self.study = optuna.create_study(direction='maximize')
            
            # 3. Uruchomienie pętli optymalizacyjnej
            # Przekazujemy metodę _objective jako cel
            self.study.optimize(self._objective, n_trials=n_trials)
            
            # 4. Zapisanie najlepszych wyników
            best_trial = self.study.best_trial
            best_params = best_trial.params
            best_value = best_trial.value
            
            logger.info(f"QuantumOptimizer: Zakończono. Najlepszy wynik: {best_value:.4f}")
            logger.info(f"Najlepsze parametry: {best_params}")

            # Aktualizacja rekordu Job w bazie
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.status = 'COMPLETED'
                job.best_score = float(best_value)
                job.configuration = best_params # Zapisujemy zwycięską konfigurację
                # Znajdź ID najlepszej próby w naszej tabeli trials (nie optuny)
                # Robimy to poprzez dopasowanie numeru próby
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
        Funkcja celu dla Optuny.
        1. Sugeruje parametry.
        2. Uruchamia "lekki" backtest (symulację).
        3. Zwraca wynik (Score) do zoptymalizowania.
        4. Zapisuje wynik próby w bazie danych.
        """
        
        # === A. Definicja Przestrzeni Parametrów (Search Space) ===
        # Zgodnie z plikiem "NowaStrategia ApexV4.txt"
        
        params = {
            # Parametry H3 Core
            'h3_percentile': trial.suggest_float('h3_percentile', 0.90, 0.99),
            'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -1.5, 0.5),
            'h3_min_score': trial.suggest_float('h3_min_score', -1.0, 2.0),
            'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 8.0),
            'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.0, 4.0),
            'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 10),
            
            # Parametry Apex Filters (Wymiar 1 i 2)
            # (Jeśli backtest_engine je obsługuje - założenie na przyszłość)
            # 'min_inst_sync': trial.suggest_float('min_inst_sync', -0.5, 0.5),
            # 'max_retail_herding': trial.suggest_float('max_retail_herding', 0.5, 3.0),
        }

        # === B. Uruchomienie Symulacji ===
        # Wywołujemy specjalny tryb silnika backtestu, który nie zapisuje transakcji do bazy,
        # a jedynie zwraca zagregowane statystyki.
        # Funkcja ta (backtest_engine.run_optimization_simulation) zostanie zaimplementowana w Kroku 2.2
        
        try:
            # Forward reference - zakładamy, że ta funkcja zostanie dodana w następnym kroku
            simulation_result = backtest_engine.run_optimization_simulation(
                self.session,
                str(self.target_year),
                params
            )
            
            profit_factor = simulation_result.get('profit_factor', 0.0)
            total_trades = simulation_result.get('total_trades', 0)
            win_rate = simulation_result.get('win_rate', 0.0)
            net_profit = simulation_result.get('net_profit', 0.0)

            # === C. Obliczenie Funkcji Celu (Score) ===
            # Karamy strategie z bardzo małą liczbą transakcji (overfitting)
            if total_trades < 10:
                score = 0.0
            else:
                # Głównym celem jest Profit Factor
                score = profit_factor

        except Exception as e:
            logger.warning(f"Trial {trial.number} failed: {e}")
            profit_factor = 0.0
            total_trades = 0
            win_rate = 0.0
            net_profit = 0.0
            score = 0.0

        # === D. Zapis Próby do Bazy Danych ===
        try:
            trial_record = models.OptimizationTrial(
                job_id=self.job_id,
                trial_number=trial.number,
                params=params,
                profit_factor=profit_factor,
                total_trades=total_trades,
                win_rate=win_rate,
                net_profit=net_profit,
                state='COMPLETE' if score > 0 else 'FAIL',
                created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            self.session.commit()
        except Exception as db_err:
            logger.error(f"Błąd zapisu próby do DB: {db_err}")
            self.session.rollback()

        return score

class AdaptiveExecutor:
    """
    Moduł odpowiedzialny za dostosowywanie "Zwycięskich Parametrów"
    do bieżących warunków rynkowych (VIX, Trend, Faza Rynku).
    """
    
    def __init__(self, base_params: dict):
        self.base_params = base_params
        # Reguły adaptacji (można je później przenieść do bazy/pliku konfiguracyjnego)
        self.adaptation_rules = {
            'HIGH_VOLATILITY': { # VIX > 25
                'h3_percentile': lambda p: min(0.99, p * 1.02), # Zaostrzamy wejście
                'h3_sl_multiplier': lambda p: p * 1.2, # Szerszy SL
                'h3_m_sq_threshold': lambda p: p - 0.2 # Bardziej rygorystyczny filtr masy
            },
            'LOW_VOLATILITY': { # VIX < 15
                'h3_percentile': lambda p: max(0.90, p * 0.98), # Poluzowujemy wejście
                'h3_tp_multiplier': lambda p: p * 1.1 # Szukamy większych ruchów
            },
            'BEAR_MARKET': { # Trend spadkowy
                'h3_tp_multiplier': lambda p: p * 0.8, # Szybsze wychodzenie
                'h3_min_score': lambda p: max(0.5, p + 0.5) # Wymagamy silniejszego sygnału
            }
        }

    def get_adapted_params(self, market_conditions: dict) -> dict:
        """
        Zwraca zmodyfikowany słownik parametrów w oparciu o warunki rynkowe.
        
        market_conditions: dict zawierający np. {'vix': 28.5, 'trend': 'BEAR'}
        """
        adapted = self.base_params.copy()
        
        # 1. Analiza Zmienności (VIX)
        vix = market_conditions.get('vix', 20.0)
        if vix > 25.0:
            self._apply_rules(adapted, 'HIGH_VOLATILITY')
        elif vix < 15.0:
            self._apply_rules(adapted, 'LOW_VOLATILITY')
            
        # 2. Analiza Trendu
        trend = market_conditions.get('trend', 'NEUTRAL')
        if trend == 'BEAR':
            self._apply_rules(adapted, 'BEAR_MARKET')
            
        return adapted

    def _apply_rules(self, params_dict, rule_key):
        rules = self.adaptation_rules.get(rule_key, {})
        for param, modifier_func in rules.items():
            if param in params_dict:
                original_val = params_dict[param]
                try:
                    new_val = modifier_func(original_val)
                    params_dict[param] = new_val
                    # logger.info(f"AdaptiveExecutor: Zmiana {param} {original_val} -> {new_val} (Reguła: {rule_key})")
                except Exception:
                    pass
