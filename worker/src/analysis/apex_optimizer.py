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
# Importujemy narzędzia analityczne (Krok 1.3 Mapy Drogowej - zakładamy że istnieją w apex_audit)
from .apex_audit import SensitivityAnalyzer

logger = logging.getLogger(__name__)

# Wyłączamy nadmierne logowanie Optuny na konsolę
optuna.logging.set_verbosity(optuna.logging.WARNING)

class QuantumOptimizer:
    """
    Serce systemu Apex V4 (Advanced).
    Wykorzystuje Optymalizację Bayesowską (TPE) oraz Multi-Period Validation
    do przeszukiwania przestrzeni parametrów strategii H3 w celu znalezienia 
    konfiguracji odpornych na zmiany rynkowe (Anty-kruchość).
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
            # direction='maximize' ponieważ chcemy maksymalizować Robust Score
            self.study = optuna.create_study(direction='maximize')
            
            # 3. Uruchomienie pętli optymalizacyjnej
            self.study.optimize(self._objective, n_trials=n_trials)
            
            # 4. Zapisanie najlepszych wyników
            best_trial = self.study.best_trial
            best_params = best_trial.params
            best_value = best_trial.value
            
            logger.info(f"QuantumOptimizer: Zakończono. Najlepszy Robust Score: {best_value:.4f}")
            logger.info(f"Najlepsze parametry: {best_params}")

            # 5. Analiza Wrażliwości (Automatyczny Audyt V4)
            # Pobieramy historię wszystkich prób do analizy przez Random Forest
            trials_data = []
            for t in self.study.trials:
                if t.state == optuna.trial.TrialState.COMPLETE:
                    # Przekazujemy parametry i wynik (Robust Score)
                    row = {'params': t.params, 'profit_factor': t.value} 
                    trials_data.append(row)
            
            sensitivity_report = {}
            try:
                if len(trials_data) >= 10:
                    logger.info("Uruchamianie analizy wrażliwości (SensitivityAnalyzer)...")
                    analyzer = SensitivityAnalyzer()
                    # Metoda analyze_parameter_sensitivity zwraca mapę ważności cech
                    sensitivity_report = analyzer.analyze_parameter_sensitivity(trials_data)
                else:
                    logger.warning("Za mało prób (<10) do rzetelnej analizy wrażliwości.")
            except Exception as e:
                logger.error(f"Błąd podczas analizy wrażliwości: {e}", exc_info=True)

            # 6. Aktualizacja rekordu Job w bazie
            # Odświeżamy obiekt sesji
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.status = 'COMPLETED'
                job.best_score = float(best_value)
                
                # Zapisujemy konfigurację zwycięską ORAZ raport z analizy wrażliwości
                final_config = {
                    'best_params': best_params,
                    'sensitivity_analysis': sensitivity_report
                }
                job.configuration = final_config # Zapis do pola JSONB
                
                # Linkujemy najlepszą próbę
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
        Funkcja celu (Robust Optimization).
        Zamiast jednego wyniku rocznego, dzieli rok na 4 kwartały, 
        symuluje każdy osobno i oblicza Robust Score.
        
        Robust Score = Mean(PF) / (1 + StdDev(PF))
        """
        
        # === A. Definicja Przestrzeni Parametrów (Search Space) ===
        params = {
            # Parametry H3 Core
            'h3_percentile': trial.suggest_float('h3_percentile', 0.90, 0.99),
            'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -1.5, 0.5),
            'h3_min_score': trial.suggest_float('h3_min_score', -1.0, 2.0),
            'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 2.0, 8.0),
            'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.0, 4.0),
            'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 10),
        }

        # === B. Multi-Period Simulation (Kwartalna Walidacja) ===
        # Definiujemy 4 okresy testowe dla zadanego roku
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
                # Kopiujemy parametry i dodajemy zakres dat dla danego kwartału
                # (Wymaga, aby backtest_engine obsługiwał simulation_start_date/end_date - zrobimy to w Kroku 2)
                period_params = params.copy()
                period_params['simulation_start_date'] = start_date
                period_params['simulation_end_date'] = end_date
                
                # Uruchamiamy "lekką" symulację
                sim_res = backtest_engine.run_optimization_simulation(
                    self.session,
                    str(self.target_year),
                    period_params
                )
                
                pf = sim_res.get('profit_factor', 0.0)
                trades = sim_res.get('total_trades', 0)
                
                # Jeśli w kwartale nie było transakcji, PF=0 (lub neutralnie 1.0? Tu przyjmujemy surowo 0)
                if trades == 0:
                    pf = 0.0
                
                period_pfs.append(pf)
                total_trades_year += trades
                total_profit_year += sim_res.get('net_profit', 0.0)
                
            except Exception as e:
                # logger.warning(f"Błąd symulacji okresu {start_date}: {e}")
                period_pfs.append(0.0)

        # === C. Obliczenie Robust Score ===
        # Kara za zbyt małą aktywność w skali roku (np. < 12 transakcji rocznie to overfitting/przypadek)
        if total_trades_year < 12: 
            robust_score = 0.0
            global_pf = 0.0
        else:
            mean_pf = np.mean(period_pfs)
            std_pf = np.std(period_pfs)
            
            # KLUCZOWY WZÓR APEX V4:
            # Nagradzamy wysoki średni PF, karzemy dużą zmienność wyników między kwartałami.
            # 1.0 w mianowniku zapobiega dzieleniu przez zero i stabilizuje wynik.
            robust_score = mean_pf / (1.0 + std_pf)
            
            # Estymata rocznego PF (średnia z kwartałów)
            global_pf = mean_pf

        # === D. Zapis Próby do Bazy Danych ===
        try:
            trial_record = models.OptimizationTrial(
                job_id=self.job_id,
                trial_number=trial.number,
                params=params,
                profit_factor=global_pf, # Zapisujemy średni PF
                total_trades=total_trades_year,
                win_rate=0.0, # Tu upraszczamy (trudno uśrednić win rate bez wag)
                net_profit=total_profit_year,
                state='COMPLETE' if robust_score > 0 else 'FAIL',
                created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            self.session.commit()
        except Exception as db_err:
            logger.error(f"Błąd zapisu próby do DB: {db_err}")
            self.session.rollback()

        return robust_score

class AdaptiveExecutor:
    """
    Mózg operacyjny Apex V4.
    Odpowiedzialny za dostosowywanie "Zwycięskich Parametrów" z optymalizacji
    do bieżących warunków rynkowych (VIX, Trend, Faza Rynku) w czasie rzeczywistym.
    """
    
    def __init__(self, base_params: dict):
        self.base_params = base_params
        # Reguły adaptacji oparte na analizie historycznej
        self.adaptation_rules = {
            'HIGH_VOLATILITY': { # VIX > 25
                # W stresie rynkowym:
                'h3_percentile': lambda p: min(0.99, p * 1.01), # Podnosimy próg wejścia (bądź bardziej wybredny)
                'h3_sl_multiplier': lambda p: p * 1.3, # Poszerzamy SL (unikaj noise stop-out)
                'h3_m_sq_threshold': lambda p: p - 0.2 # Wymagaj mniejszej "masy" (tłumu)
            },
            'LOW_VOLATILITY': { # VIX < 15
                # W ciszy rynkowej:
                'h3_percentile': lambda p: max(0.90, p * 0.99), # Poluzuj wejście
                'h3_tp_multiplier': lambda p: p * 1.1 # Szukaj dalszych zasięgów (swing)
            },
            'BEAR_MARKET': { # Trend spadkowy
                'h3_tp_multiplier': lambda p: p * 0.8, # Szybciej realizuj zyski (hit & run)
                'h3_min_score': lambda p: max(0.5, p + 0.5) # Wymagaj bardzo silnego sygnału
            }
        }

    def get_adapted_params(self, market_conditions: dict) -> dict:
        """
        Zwraca zmodyfikowany słownik parametrów w oparciu o warunki rynkowe.
        
        Args:
            market_conditions: dict np. {'vix': 28.5, 'trend': 'BEAR'}
        """
        adapted = self.base_params.copy()
        
        # 1. Analiza Zmienności (VIX)
        # Domyślnie 20 jeśli brak danych
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
                try:
                    original_val = params_dict[param]
                    new_val = modifier_func(original_val)
                    params_dict[param] = new_val
                except Exception:
                    pass
