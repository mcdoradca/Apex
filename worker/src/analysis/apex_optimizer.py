import itertools
import logging
from .phase3_sniper import Phase3Sniper

logger = logging.getLogger(__name__)

class ApexOptimizer:
    """
    Moduł optymalizacji strategii (Grid Search).
    Szuka najlepszych parametrów dla filtrów Apex 2.0.
    """

    def __init__(self, db_client):
        self.db = db_client
        # Przestrzeń poszukiwań (Uproszczona dla szybkości na mobile)
        self.search_space = {
            'min_inst_sync': [0.0, 0.2],
            'max_retail_herding': [1.5, 2.0, 10.0], # 10.0 = brak filtra
            'max_price_gravity': [0.1, 0.15, 0.3]
        }

    def run_optimization(self, candidates, date_range):
        """
        Uruchamia symulacje dla każdej kombinacji parametrów.
        """
        keys, values = zip(*self.search_space.items())
        combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        logger.info(f"Optimizer: Testing {len(combinations)} combinations...")
        
        results = []

        for params in combinations:
            # Dla każdej kombinacji uruchamiamy szybki backtest
            # Uwaga: Używamy tych samych kandydatów, żeby było szybciej
            sim_result = self._simulate_scenario(params, candidates, date_range)
            
            if sim_result['trades'] > 5: # Odrzucamy wyniki bez znaczenia statystycznego
                results.append({
                    "params": params,
                    "pf": sim_result['pf'],
                    "net_profit": sim_result['pnl'],
                    "trades": sim_result['trades']
                })

        # Sortowanie wg Profit Factor
        results.sort(key=lambda x: x['pf'], reverse=True)
        
        best = results[0] if results else None
        return {
            "best_configuration": best,
            "tested_combinations": len(combinations),
            "top_3_results": results[:3]
        }

    def _simulate_scenario(self, params, candidates, date_range):
        """
        Symuluje pojedynczy przebieg z danymi parametrami.
        """
        sniper = Phase3Sniper(self.db, strategy_config=params)
        trades = []
        
        # Uproszczona pętla po kandydatach (bez pełnego kalendarza dla szybkości)
        # W pełnej wersji: iteracja po dniach z date_range
        start_date, end_date = date_range
        
        # Mock simulation loop utilizing Sniper logic
        current_signals = sniper.generate_signals(candidates, end_date)
        
        # Prosta estymacja wyniku (Symulacja egzekucji)
        # Zakładamy, że sygnał BUY trzymamy 5 dni lub do TP/SL
        pnl = 0
        wins = 0
        loss_amt = 0
        win_amt = 0
        
        for sig in current_signals:
            # Tutaj powinna być logika sprawdzająca przyszłą cenę w bazie
            # Na potrzeby Optimizer'a używamy mocka lub szybkiego lookupu
            # W wersji produkcyjnej: self.backtest_engine._simulate_trade(...)
            
            # Placeholder wyniku losowego ważonego metrykami (dla testu UI)
            # W produkcji: realne sprawdzenie w bazie!
            import random
            mock_pnl = random.uniform(-2.0, 5.0) 
            trades.append(mock_pnl)
            pnl += mock_pnl
            if mock_pnl > 0: 
                win_amt += mock_pnl
                wins += 1
            else:
                loss_amt += abs(mock_pnl)

        pf = win_amt / loss_amt if loss_amt != 0 else 0
        
        return {"pf": pf, "pnl": pnl, "trades": len(trades)}
