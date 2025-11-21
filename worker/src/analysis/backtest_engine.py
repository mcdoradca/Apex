import logging
import pandas as pd
from datetime import timedelta
from .phase1_scanner import Phase1Scanner
from .phase3_sniper import Phase3Sniper
# Importujemy Twoje nowe moduły analityczne
from .apex_audit import ApexAudit
from .apex_optimizer import ApexOptimizer

logger = logging.getLogger(__name__)

class BacktestEngine:
    """
    Główny silnik symulacyjny APEX.
    Łączy stabilną logikę H3 (Phase3Sniper) z nową warstwą optymalizacyjną (Apex 2.0).
    
    Zasada działania:
    1. Skaner (Phase 1) -> Kandydaci
    2. Sniper (Phase 3 - H3 PDF Logic) -> Sygnały bazowe
    3. Apex Filter (Apex 2.0 - InstSync/Herding) -> Sygnały potwierdzone
    4. Egzekucja -> Backtest/Audit
    """
    
    def __init__(self, db_client):
        self.db = db_client
        self.scanner = Phase1Scanner(db_client)
        self.sniper = Phase3Sniper(db_client) # Oryginalny H3 Sniper bez zmian w kodzie
        
        # Inicjalizacja modułów pomocniczych
        self.optimizer = ApexOptimizer(db_client)
        self.auditor = ApexAudit()

    def run(self, config):
        """
        Główny punkt wejścia (Entry Point).
        Obsługuje logikę wyboru trybu pracy: Backtest vs Optimizer vs Audit.
        """
        mode = config.get('mode', 'backtest')
        setup_name = config.get('setupName', 'Unnamed')
        
        logger.info(f"=== ENGINE START: {setup_name} | MODE: {mode.upper()} ===")
        
        try:
            # TRYB 1: Optymalizacja (AI Grid Search)
            if mode == 'optimizer':
                # Optimizer używa silnika do przetestowania wielu wariantów
                return self.optimizer.run_optimization(
                    candidates=self.db.get_top_liquid_tickers(limit=50), # Pobranie listy płynnych
                    date_range=(config.get('startDate'), config.get('endDate'))
                )

            # TRYB 2 i 3: Symulacja Czasowa (Backtest / Audit)
            # Oba wymagają pełnego przebiegu przez historię
            simulation_results = self._run_full_simulation(config)
            
            if mode == 'audit':
                # Przekazujemy surowe transakcje do analizy w ApexAudit
                return self.auditor.analyze(simulation_results)
            
            # Domyślnie zwracamy standardowe metryki
            return self._calculate_basic_stats(simulation_results)

        except Exception as e:
            logger.error(f"Critical Backtest Engine Error: {e}", exc_info=True)
            raise e

    def _run_full_simulation(self, config):
        """
        Pełna pętla symulacyjna dzień po dniu.
        To tutaj następuje połączenie metody H3 z filtrami Apex.
        """
        start_date = pd.to_datetime(config.get('startDate'))
        end_date = pd.to_datetime(config.get('endDate'))
        
        # Parametry filtrów z UI (Apex 2.0)
        apex_params = config.get('params', {})
        
        logger.info(f"Running Simulation {start_date.date()} -> {end_date.date()} with params: {apex_params}")

        current_date = start_date
        open_positions = []
        closed_history = []

        # --- GŁÓWNA PĘTLA CZASOWA ---
        while current_date <= end_date:
            
            # 1. ZARZĄDZANIE POZYCJAMI (Egzekucja wyjść)
            # Sprawdzamy TP/SL/Time Exit dla każdej otwartej pozycji
            active_positions = []
            for trade in open_positions:
                update = self._manage_position(trade, current_date)
                if update['is_closed']:
                    closed_history.append(update['trade_data'])
                else:
                    active_positions.append(update['trade_data'])
            open_positions = active_positions

            # 2. GENEROWANIE SYGNAŁÓW (Tylko dni robocze)
            if current_date.weekday() < 5:
                try:
                    # Faza 1: Skanowanie (Istniejąca metoda)
                    candidates = self.scanner.scan_market(current_date)
                    
                    if candidates:
                        # Faza 3: H3 Sniper (Istniejąca metoda z PDF)
                        # Zwraca sygnały spełniające warunek "Załamanie Superpozycji"
                        raw_signals = self.sniper.generate_signals(candidates, current_date)
                        
                        # --- TU JEST TWOJE ROZWIĄZANIE (APEX INTEGRATION) ---
                        # Filtrujemy sygnały H3 przez parametry z Optimizera
                        valid_signals = self._apply_apex_filters(raw_signals, apex_params)
                        
                        # Otwarcie pozycji dla przefiltrowanych sygnałów
                        for signal in valid_signals:
                            new_trade = self._open_new_trade(signal, current_date)
                            open_positions.append(new_trade)
                            
                except Exception as step_error:
                    logger.warning(f"Simulation step warning on {current_date.date()}: {step_error}")

            current_date += timedelta(days=1)

        # 3. FINALIZACJA (Wymuszone zamknięcie na koniec testu)
        for trade in open_positions:
            closed_trade = self._force_close(trade, end_date)
            closed_history.append(closed_trade)

        logger.info(f"Simulation finished. Total trades: {len(closed_history)}")
        return closed_history

    def _apply_apex_filters(self, signals, params):
        """
        Kluczowa metoda integracyjna.
        Odrzuca sygnały H3, które nie spełniają kryteriów Apex Optimizer (InstSync, Herding).
        """
        if not signals:
            return []
            
        # Pobranie progów z konfiguracji (lub domyślne 'luźne' wartości)
        min_inst_sync = params.get('min_inst_sync', -99.0)
        max_herding = params.get('max_retail_herding', 99.0)
        max_gravity = params.get('max_price_gravity', 99.0)
        
        approved_signals = []
        
        for signal in signals:
            # Wyciągamy metadane sygnału (zakładamy, że Phase3Sniper dołącza je w 'meta')
            meta = signal.get('meta', {})
            
            val_inst = meta.get('inst_sync', 0.0)
            val_herding = meta.get('retail_herding', 0.0)
            val_gravity = meta.get('price_gravity', 0.0)
            
            # Logika filtracji Apex 2.0
            if val_inst < min_inst_sync:
                continue # Odrzuć: Instytucje grają przeciwko nam
                
            if val_herding > max_herding:
                continue # Odrzuć: Zbyt duży tłum (pułapka)
                
            if abs(val_gravity) > max_gravity:
                continue # Odrzuć: Cena zbyt odchylona
                
            approved_signals.append(signal)
            
        return approved_signals

    def _manage_position(self, trade, current_date):
        """Obsługa logiki wyjścia (TP / SL / Time Exit 5 dni)."""
        ticker = trade['ticker']
        
        # Pobranie danych rynkowych (OHLC)
        daily_price = self.db.get_daily_price(ticker, current_date)
        
        if not daily_price:
            return {'is_closed': False, 'trade_data': trade}

        # Logika PDF: Wyjście czasowe po 5 dniach
        days_active = (current_date - pd.to_datetime(trade['entry_date'])).days
        
        hit_sl = daily_price['low'] <= trade['stop_loss']
        hit_tp = daily_price['high'] >= trade['take_profit']
        
        status = None
        exit_price = None
        
        if hit_sl:
            status = "CLOSED_SL"
            exit_price = trade['stop_loss']
        elif hit_tp:
            status = "CLOSED_TP"
            exit_price = trade['take_profit']
        elif days_active >= 5:
            status = "CLOSED_EXPIRED"
            exit_price = daily_price['close']
            
        if status:
            pnl = ((exit_price - trade['entry_price']) / trade['entry_price']) * 100
            trade.update({
                'status': status,
                'close_date': current_date,
                'exit_price': exit_price,
                'profit_loss': pnl
            })
            return {'is_closed': True, 'trade_data': trade}
            
        return {'is_closed': False, 'trade_data': trade}

    def _open_new_trade(self, signal, date):
        """Konwersja sygnału na strukturę pozycji."""
        return {
            'ticker': signal['ticker'],
            'entry_date': date,
            'entry_price': signal['entry_price'],
            'stop_loss': signal['stop_loss'],
            'take_profit': signal['take_profit'],
            'status': 'OPEN',
            # Zachowujemy metadane dla ApexAudit
            'meta': signal.get('meta', {})
        }

    def _force_close(self, trade, date):
        """Zamyka pozycje na koniec testu."""
        price = self.db.get_daily_price(trade['ticker'], date)
        exit_p = price['close'] if price else trade['entry_price']
        pnl = ((exit_p - trade['entry_price']) / trade['entry_price']) * 100
        
        trade.update({
            'status': 'CLOSED_FORCE',
            'close_date': date,
            'exit_price': exit_p,
            'profit_loss': pnl
        })
        return trade

    def _calculate_basic_stats(self, trades):
        """Prosta statystyka dla UI."""
        if not trades:
            return {"profit_factor": 0.0, "total_trades": 0}
            
        wins = sum(t['profit_loss'] for t in trades if t['profit_loss'] > 0)
        losses = abs(sum(t['profit_loss'] for t in trades if t['profit_loss'] <= 0))
        
        return {
            "profit_factor": round(wins / losses, 2) if losses > 0 else 999.0,
            "total_trades": len(trades),
            "total_pnl": round(sum(t['profit_loss'] for t in trades), 2),
            "win_rate": round(len([t for t in trades if t['profit_loss'] > 0]) / len(trades) * 100, 1)
        }
