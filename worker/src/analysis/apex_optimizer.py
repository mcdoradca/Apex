import itertools
import logging
import random
from .phase3_sniper import Phase3Sniper

logger = logging.getLogger(__name__)

class ApexOptimizer:
    """
    Moduł AI Optimizer (Grid Search).
    Automatycznie testuje tysiące kombinacji parametrów, aby znaleźć "Złoty Setup".
    """

    def __init__(self, db_client):
        self.db = db_client
        # Przestrzeń poszukiwań - zakresy dla Grid Search
        # Dostosowane pod szybkie testy na mobile
        self.search_space = {
            'min_inst_sync': [-0.2, 0.0, 0.2, 0.5],
            'max_retail_herding': [1.5, 2.0, 5.0],
            'max_price_gravity': [0.10, 0.15, 0.30]
        }

    def run_optimization(self, candidates, date_range):
        """
        Uruchamia proces optymalizacji.
        """
        # Generowanie wszystkich permutacji parametrów
        keys, values = zip(*self.search_space.items())
        combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        logger.info(f"Apex Optimizer: Start. Testowanie {len(combinations)} scenariuszy.")
        
        results = []

        # Iteracja po każdej kombinacji parametrów
        for params in combinations:
            # Symulujemy wynik strategii dla danych parametrów
            sim_result = self._simulate_scenario(params, candidates, date_range)
            
            # Zapisujemy tylko sensowne wyniki (min. 5 transakcji)
            if sim_result['trades'] >= 5:
                results.append({
                    "params": params,
                    "pf": sim_result['pf'],
                    "net_profit": sim_result['pnl'],
                    "trades": sim_result['trades']
                })

        # Sortowanie wyników - Najważniejszy jest Profit Factor (PF)
        results.sort(key=lambda x: x['pf'], reverse=True)
        
        best_config = results[0] if results else None
        
        logger.info(f"Optymalizacja zakończona. Najlepszy PF: {best_config['pf'] if best_config else 0}")

        return {
            "best_configuration": best_config,
            "total_scenarios": len(combinations),
            "top_3_configs": results[:3] if len(results) >= 3 else results
        }

    def _simulate_scenario(self, params, candidates, date_range):
        """
        Symuluje działanie Snipera z konkretnymi filtrami.
        Wersja uproszczona (Szybka Symulacja) na potrzeby testów interfejsu.
        """
        sniper = Phase3Sniper(self.db, strategy_config=params)
        
        # W pełnej wersji tutaj byłaby pętla po datach z bazy danych.
        # Tutaj, aby nie blokować Twojego telefonu na 20 minut, robimy symulację
        # na podstawie logiki Snipera (czy zaakceptowałby sygnał?)
        
        accepted_trades = 0
        total_pnl = 0.0
        wins = 0
        losses = 0
        gross_win = 0.0
        gross_loss = 0.0

        # Mock data loop - w produkcji użyj self.db.get_historical_data
        # Sprawdzamy, czy Sniper zaakceptowałby losowe próbki danych rynkowych
        for _ in range(20): # Próbka 20 potencjalnych setupów
            mock_metrics = {
                'metric_inst_sync': random.uniform(-0.5, 1.0),
                'metric_retail_herding': random.uniform(0.5, 3.0),
                'metric_price_gravity': random.uniform(0.0, 0.4),
                'metric_aqm_score_h3': random.uniform(-1.0, 2.0)
            }
            
            # Kluczowy moment: Sniper decyduje czy wchodzić
            is_valid, _ = sniper.validate_setup(mock_metrics)
            
            if is_valid:
                accepted_trades += 1
                # Symulacja wyniku transakcji (obarczona pewnym szumem)
                # Zakładamy, że dobre filtry zwiększają szansę na zysk
                # Im wyższy InstSync tym większa szansa na win w modelu
                prob_win = 0.5 + (mock_metrics['metric_inst_sync'] * 0.2)
                is_win = random.random() < prob_win
                
                pnl = random.uniform(1.0, 5.0) if is_win else random.uniform(-2.0, -1.0)
                total_pnl += pnl
                
                if pnl > 0:
                    wins += 1
                    gross_win += pnl
                else:
                    losses += 1
                    gross_loss += abs(pnl)

        pf = gross_win / gross_loss if gross_loss != 0 else 0.0
        
        return {
            "pf": round(pf, 2),
            "pnl": round(total_pnl, 2),
            "trades": accepted_trades
        }
