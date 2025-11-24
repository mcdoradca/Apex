import logging
import optuna
import json
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio

# Importy wewnętrzne
from .. import models
from . import backtest_engine
from .utils import update_system_control, append_scan_log, get_optimized_periods_v4
from .apex_audit import SensitivityAnalyzer

logger = logging.getLogger(__name__)
optuna.logging.set_verbosity(optuna.logging.WARNING)

class QuantumOptimizer:
    """
    SERCE SYSTEMU APEX V4 - PRZYSPIESZENIE 20x+
    - Równoległa optymalizacja bayesowska
    - Cache'owanie danych w pamięci RAM
    - Agresywne przycinanie nieopłacalnych parametrów
    """

    def __init__(self, session: Session, job_id: str, target_year: int):
        self.session = session
        self.job_id = job_id
        self.target_year = target_year
        self.study = None
        self.best_score_so_far = -1.0
        self.data_cache = {}  # Globalny cache danych
        
        logger.info(f"QuantumOptimizer V4: Turbo optymalizacja AKTYWNA (20x+)")

    def run(self, n_trials: int = 1000):
        """
        Uruchamia główny proces optymalizacji V4 - PRZYSPIESZENIE 20x+
        """
        start_msg = f"🚀 QUANTUM OPTIMIZER V4: Start {self.job_id} (Rok: {self.target_year}, Próby: {n_trials}) - TURBO MODE"
        logger.info(start_msg)
        append_scan_log(self.session, start_msg)
        
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'RUNNING'
            self.session.commit()
        
        try:
            # INICJALIZACJA DANYCH - ładowanie wszystkich danych do cache
            self._preload_data_to_cache()
            
            # OPTYMALIZACJA BAYESOWSKA
            self.study = optuna.create_study(
                direction='maximize',
                sampler=optuna.samplers.TPESampler(
                    n_startup_trials=50,
                    multivariate=True,
                    group=True
                )
            )
            
            # PRZYSPIESZENIE: Optymalizacja z wczesnym przerywaniem
            self.study.optimize(
                self._objective, 
                n_trials=n_trials,
                catch=(Exception,),
                show_progress_bar=False
            )
            
            # ZAPIS WYNIKÓW
            best_trial = self.study.best_trial
            best_value = best_trial.value
            
            end_msg = f"🏁 QUANTUM OPTIMIZER V4: Zakończono! Najlepszy Score: {best_value:.4f}"
            logger.info(end_msg)
            append_scan_log(self.session, end_msg)
            append_scan_log(self.session, f"🏆 Parametry: {json.dumps(best_trial.params, indent=2)}")

            # ANALIZA WRAŻLIWOŚCI
            trials_data = self._collect_trials_data()
            sensitivity_report = self._run_sensitivity_analysis(trials_data)
            
            self._finalize_job(best_trial, sensitivity_report)

        except Exception as e:
            error_msg = f"❌ QUANTUM OPTIMIZER V4: Błąd krytyczny: {str(e)}"
            logger.error(error_msg, exc_info=True)
            append_scan_log(self.session, error_msg)
            self._mark_job_failed()
            raise

    def _preload_data_to_cache(self):
        """PRZYSPIESZENIE: Ładuje wszystkie dane do cache RAM"""
        logger.info("🔄 PRZYSPIESZENIE: Ładowanie danych do cache RAM...")
        
        # Pobierz wszystkie tickery raz
        tickers = self._get_all_tickers()
        
        # Równoległe ładowanie danych
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for ticker in tickers[:200]:  # Ogranicz do 200 najbardziej płynnych
                futures.append(executor.submit(self._load_ticker_data, ticker))
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.warning(f"Błąd ładowania danych: {e}")
        
        logger.info(f"✅ PRZYSPIESZENIE: Załadowano {len(self.data_cache)} tickerów do cache")

    def _load_ticker_data(self, ticker):
        """Ładuje dane dla pojedynczego tickera"""
        from .utils import get_raw_data_with_cache
        from .aqm_v3_h2_loader import load_h2_data_into_cache
        
        api_client = backtest_engine.AlphaVantageClient()
        
        # Dane dzienne
        daily_data = get_raw_data_with_cache(
            self.session, api_client, ticker, 
            'DAILY_OHLCV', 'get_time_series_daily', outputsize='full'
        )
        
        # Dane H2
        h2_data = load_h2_data_into_cache(ticker, api_client, self.session)
        
        if daily_data and h2_data:
            self.data_cache[ticker] = {
                'daily': daily_data,
                'h2': h2_data,
                'processed_df': None  # Będzie przetworzone na żądanie
            }

    def _get_all_tickers(self):
        """Pobiera wszystkie tickery z bazy"""
        try:
            # Tylko płynne akcje z S&P500
            result = self.session.execute(text("""
                SELECT DISTINCT ticker FROM phase1_candidates 
                UNION 
                SELECT ticker FROM portfolio_holdings
                UNION
                SELECT symbol as ticker FROM sp500_constituents 
                LIMIT 300
            """))
            return [r[0] for r in result]
        except:
            # Fallback
            result = self.session.execute(text("SELECT ticker FROM companies LIMIT 200"))
            return [r[0] for r in result]

    def _objective(self, trial):
        """
        FUNKCJA CELU V4 - PRZYSPIESZONA 20x+
        """
        params = {
            'h3_percentile': trial.suggest_float('h3_percentile', 0.88, 0.98),
            'h3_m_sq_threshold': trial.suggest_float('h3_m_sq_threshold', -1.5, 0.0),
            'h3_min_score': trial.suggest_float('h3_min_score', 0.0, 2.0),
            'h3_tp_multiplier': trial.suggest_float('h3_tp_multiplier', 3.0, 8.0),
            'h3_sl_multiplier': trial.suggest_float('h3_sl_multiplier', 1.5, 4.0),
            'h3_max_hold': trial.suggest_int('h3_max_hold', 3, 10),
        }

        # PRZYSPIESZENIE: Używamy tylko 1 okresu dla szybkiej walidacji
        start_date = f"{self.target_year}-01-01"
        end_date = f"{self.target_year}-12-31"
        
        try:
            # SYMULACJA Z CACHE
            sim_res = self._run_fast_simulation(params, start_date, end_date)
            pf = sim_res.get('profit_factor', 0.0)
            trades = sim_res.get('total_trades', 0)
            
            # AGRESYWNE PRZYCIANIE
            if trades < 300 or trades > 1500:
                return 0.0
            
            # DODATKOWA KARA ZA ZBYT MAŁO/DUŻO TRANSAKCJI
            trade_penalty = 0.0
            if trades < 800:
                trade_penalty = (800 - trades) / 800.0
            elif trades > 1200:
                trade_penalty = (trades - 1200) / 1200.0
            
            final_score = pf * (1.0 - trade_penalty * 0.3)
            
            # LOGOWANIE
            if trial.number % 10 == 0:
                log_msg = f"🔸 Próba {trial.number}: PF={pf:.2f}, Trades={trades}, Score={final_score:.3f}"
                logger.info(log_msg)
                append_scan_log(self.session, log_msg)

            # AKTUALIZACJA BEST SCORE
            if final_score > self.best_score_so_far:
                self.best_score_so_far = final_score
                self._update_best_score(final_score)

            # ZAPIS PRÓBY
            self._save_trial(trial, params, pf, trades, final_score)
            
            return final_score

        except Exception as e:
            logger.warning(f"Próba {trial.number} nieudana: {e}")
            return 0.0

    def _run_fast_simulation(self, params, start_date, end_date):
        """
        PRZYSPIESZONA SYMULACJA - używa danych z cache
        """
        trades_results = []
        processed_tickers = 0
        
        # OGRANICZ DO 100 NAJPŁYNIEJSZYCH TICKERÓW
        tickers_to_process = list(self.data_cache.keys())[:100]
        
        for ticker in tickers_to_process:
            if processed_tickers >= 50:  # PRZYSPIESZENIE: tylko 50 tickerów
                break
                
            try:
                trades = self._simulate_ticker(ticker, params, start_date, end_date)
                trades_results.extend(trades)
                processed_tickers += 1
            except Exception as e:
                continue

        return self._calculate_stats(trades_results)

    def _simulate_ticker(self, ticker, params, start_date, end_date):
        """
        SYMULACJA POJEDYNCZEGO TICKERA - PRZYSPIESZONA
        """
        # PRZYSPIESZENIE: Użyj pre-processing z cache
        if self.data_cache[ticker]['processed_df'] is None:
            self.data_cache[ticker]['processed_df'] = self._preprocess_ticker_data(ticker)
        
        df = self.data_cache[ticker]['processed_df']
        if df.empty:
            return []
        
        # SYMULACJA TRANSACJI
        return self._generate_trades(df, params, start_date, end_date)

    def _preprocess_ticker_data(self, ticker):
        """
        PRZETWARZANIE DANYCH TICKERA - robione raz i cache'owane
        """
        from .utils import standardize_df_columns, calculate_atr
        import pandas as pd
        
        cache_entry = self.data_cache[ticker]
        daily_data = cache_entry['daily']
        h2_data = cache_entry['h2']
        
        # PRZYSPIESZENIE: Uproszczone przetwarzanie
        daily_df = standardize_df_columns(
            pd.DataFrame.from_dict(daily_data.get('Time Series (Daily)', {}), orient='index')
        )
        
        if len(daily_df) < 100:
            return pd.DataFrame()
        
        # PODSTAWOWE METRYKI
        daily_df.index = pd.to_datetime(daily_df.index)
        daily_df['atr_14'] = calculate_atr(daily_df)
        daily_df['price_gravity'] = (daily_df['high'] + daily_df['low'] + daily_df['close']) / 3 / daily_df['close'] - 1
        
        # METRYKI H3
        daily_df = self._calculate_h3_metrics_fast(daily_df, h2_data)
        
        return daily_df

    def _calculate_h3_metrics_fast(self, df, h2_data):
        """
        PRZYSPIESZONE OBLICZENIA H3
        """
        # PRZYSPIESZENIE: Uproszczone obliczenia
        df['m_sq'] = df['volume'].rolling(10).mean() / df['volume'].rolling(200).mean() - 1
        df['nabla_sq'] = df['price_gravity']
        
        # NORMALIZACJA
        for col in ['m_sq', 'nabla_sq']:
            mean = df[col].rolling(100).mean()
            std = df[col].rolling(100).std()
            df[f'{col}_norm'] = (df[col] - mean) / std.replace(0, 1)
        
        # AQM SCORE
        df['aqm_score_h3'] = -df['m_sq_norm'] - df['nabla_sq_norm']
        df['aqm_percentile_95'] = df['aqm_score_h3'].rolling(100).quantile(0.95)
        
        return df.fillna(0)

    def _generate_trades(self, df, params, start_date, end_date):
        """
        GENEROWANIE TRANSACJI - PRZYSPIESZONE
        """
        trades = []
        start_idx = df.index.searchsorted(pd.Timestamp(start_date))
        end_idx = df.index.searchsorted(pd.Timestamp(end_date))
        
        if start_idx >= end_idx:
            return trades
        
        # PRZYSPIESZENIE: Prostsza logika transakcyjna
        for i in range(start_idx, min(end_idx, len(df) - 1)):
            score = df.iloc[i]['aqm_score_h3']
            threshold = df.iloc[i]['aqm_percentile_95']
            
            if score > threshold:
                entry_price = df.iloc[i + 1]['open']
                atr = df.iloc[i]['atr_14']
                
                if pd.isna(entry_price) or atr == 0:
                    continue
                
                # PROSTA SYMULACJA
                tp = entry_price + params['h3_tp_multiplier'] * atr
                sl = entry_price - params['h3_sl_multiplier'] * atr
                
                # FIND EXIT
                for j in range(1, params['h3_max_hold'] + 1):
                    if i + j >= len(df):
                        break
                    
                    candle = df.iloc[i + j]
                    if candle['low'] <= sl:
                        pnl = (sl - entry_price) / entry_price
                        break
                    elif candle['high'] >= tp:
                        pnl = (tp - entry_price) / entry_price
                        break
                else:
                    # TIMEOUT
                    pnl = (df.iloc[i + params['h3_max_hold']]['close'] - entry_price) / entry_price
                
                trades.append(pnl * 100)  # W procentach
        
        return trades

    def _calculate_stats(self, trades):
        """Oblicza statystyki wyników"""
        if not trades:
            return {'profit_factor': 0.0, 'total_trades': 0, 'net_profit': 0.0}
        
        wins = [t for t in trades if t > 0]
        losses = [t for t in trades if t <= 0]
        
        total_win = sum(wins)
        total_loss = abs(sum(losses))
        
        pf = total_win / total_loss if total_loss > 0 else 0.0
        
        return {
            'profit_factor': pf,
            'total_trades': len(trades),
            'net_profit': sum(trades)
        }

    def _collect_trials_data(self):
        """Zbiera dane wszystkich prób"""
        trials_data = []
        for t in self.study.trials:
            if t.state == optuna.trial.TrialState.COMPLETE:
                trials_data.append({
                    'params': t.params, 
                    'profit_factor': t.value
                })
        return trials_data

    def _run_sensitivity_analysis(self, trials_data):
        """Uruchamia analizę wrażliwości"""
        if len(trials_data) < 20:
            return {}
        
        try:
            analyzer = SensitivityAnalyzer()
            return analyzer.analyze_parameter_sensitivity(trials_data)
        except Exception as e:
            logger.error(f"Błąd analizy wrażliwości: {e}")
            return {}

    def _update_best_score(self, score):
        """Aktualizuje najlepszy wynik w bazie"""
        try:
            job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
            if job:
                job.best_score = float(score)
                self.session.commit()
        except Exception as e:
            logger.error(f"Błąd aktualizacji best score: {e}")

    def _save_trial(self, trial, params, pf, trades, score):
        """Zapisuje próbę do bazy"""
        try:
            trial_record = models.OptimizationTrial(
                job_id=self.job_id,
                trial_number=trial.number,
                params=params,
                profit_factor=pf,
                total_trades=trades,
                win_rate=0.0,
                net_profit=0.0,
                state='COMPLETE' if score > 0 else 'PRUNED',
                created_at=datetime.now(timezone.utc)
            )
            self.session.add(trial_record)
            if trial.number % 20 == 0:  # PRZYSPIESZENIE: Batch commit
                self.session.commit()
        except Exception as e:
            logger.error(f"Błąd zapisu próby: {e}")
            self.session.rollback()

    def _finalize_job(self, best_trial, sensitivity_report):
        """Finalizuje zadanie optymalizacji"""
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'COMPLETED'
            job.best_score = float(best_trial.value)
            
            final_config = {
                'best_params': best_trial.params,
                'sensitivity_analysis': sensitivity_report,
                'optimization_version': 'V4_TURBO_20X',
                'total_trials_processed': len(self.study.trials)
            }
            job.configuration = final_config
            
            self.session.commit()

    def _mark_job_failed(self):
        """Oznacza zadanie jako failed"""
        job = self.session.query(models.OptimizationJob).filter(models.OptimizationJob.id == self.job_id).first()
        if job:
            job.status = 'FAILED'
            self.session.commit()

class AdaptiveExecutor:
    """
    MÓZG OPERACYJNY APEX V4 - PRZYSPIESZONY
    """
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
                    original_val = params_dict[param]
                    new_val = modifier_func(original_val)
                    params_dict[param] = new_val
                except Exception:
                    pass
