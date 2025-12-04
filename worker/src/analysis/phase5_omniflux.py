import logging
import pandas as pd
import time
import json
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta

# Importy z projektu
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models
from .utils import (
    standardize_df_columns, 
    append_scan_log, 
    update_scan_progress, 
    send_telegram_alert,
    update_system_control,
    get_system_control_value # Potrzebne do odczytu stanu
)
from .flux_physics import calculate_flux_vectors

logger = logging.getLogger(__name__)

# === KONFIGURACJA OMNI-FLUX (V5) ===
CAROUSEL_SIZE = 8          
CYCLE_DELAY = 0.5          
FLUX_THRESHOLD_ENTRY = 70  
MACRO_CACHE_DURATION = 300 

class OmniFluxAnalyzer:
    """
    APEX OMNI-FLUX ENGINE (Phase 5) - Wersja Persistent (Naprawiona Amnezja)
    """

    def __init__(self, session: Session, api_client: AlphaVantageClient):
        self.session = session
        self.client = api_client
        
        # Domyślny stan
        self.active_pool = []       
        self.reserve_pool = []      
        self.macro_context = {'bias': 'NEUTRAL', 'last_updated': 0}
        
        # === KLUCZOWE: ODTWARZANIE STANU PRZY INICJALIZACJI ===
        # Zapobiega resetowaniu puli przy każdym cyklu workera
        self._load_state()

    def _load_state(self):
        """Próbuje odtworzyć stan Karuzeli z bazy danych."""
        try:
            raw_json = get_system_control_value(self.session, 'phase5_monitor_state')
            if raw_json:
                state = json.loads(raw_json)
                
                if 'active_pool' in state and isinstance(state['active_pool'], list):
                    self.active_pool = state['active_pool']
                
                if 'reserve_pool' in state and isinstance(state['reserve_pool'], list):
                    self.reserve_pool = state['reserve_pool']
                
                if 'macro_bias' in state:
                    self.macro_context['bias'] = state['macro_bias']
                
                # Walidacja świeżości (np. reset po 10 min nieaktywności)
                if 'last_updated' in state and (time.time() - state['last_updated'] > 600):
                     logger.info("Faza 5: Stan przestarzały. Resetowanie puli.")
                     self.active_pool = []
                     self.reserve_pool = []

        except Exception as e:
            logger.warning(f"Faza 5: Błąd odczytu stanu (Start czysty): {e}")
            self.active_pool = []

    def _refresh_macro_context(self):
        now = time.time()
        if now - self.macro_context['last_updated'] < MACRO_CACHE_DURATION:
            return

        try:
            fx_data = self.client.get_intraday(symbol='EUR', market='USD', interval='60min', outputsize='compact')
            if fx_data and 'Time Series (60min)' in fx_data:
                df_fx = standardize_df_columns(pd.DataFrame.from_dict(fx_data.get('Time Series (60min)', {}), orient='index'))
                if not df_fx.empty and len(df_fx) > 1:
                    df_fx = df_fx.sort_index()
                    if df_fx['close'].iloc[-1] > df_fx['close'].iloc[-2]: 
                        self.macro_context['bias'] = 'BULLISH'
                    else:
                        self.macro_context['bias'] = 'BEARISH'
            self.macro_context['last_updated'] = now
        except Exception as e:
            logger.warning(f"Faza 5: Błąd makro: {e}")

    def _initialize_pools(self):
        """Ładuje kandydatów tylko jeśli pula jest pusta."""
        try:
            # Pobieramy kandydatów z bazy
            p1_rows = self.session.execute(text("SELECT ticker FROM phase1_candidates ORDER BY sector_trend_score DESC NULLS LAST LIMIT 40")).fetchall()
            px_rows = self.session.execute(text("SELECT ticker FROM phasex_candidates ORDER BY last_pump_percent DESC NULLS LAST LIMIT 20")).fetchall()
            
            combined_tickers = list(set([r[0] for r in p1_rows] + [r[0] for r in px_rows]))
            
            active_sigs = [r[0] for r in self.session.execute(text("SELECT ticker FROM trading_signals WHERE status IN ('ACTIVE', 'PENDING')")).fetchall()]
            holdings = [r[0] for r in self.session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()]
            exclude = set(active_sigs + holdings)
            
            # Jeśli rezerwa jest pusta, napełnij ją
            if not self.reserve_pool:
                self.reserve_pool = [t for t in combined_tickers if t not in exclude]
            
            # Jeśli aktywna pula niepełna, dobierz
            while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
                ticker = self.reserve_pool.pop(0)
                self.active_pool.append({
                    'ticker': ticker, 
                    'fails': 0, 
                    'added_at': time.time(),
                    'price': 0.0,
                    'elasticity': 0.0,
                    'velocity': 0.0,
                    'flux_score': 0
                })
                
            msg = f"🌊 Faza 5 (Omni-Flux): Inicjalizacja. Aktywne: {len(self.active_pool)}, Rezerwa: {len(self.reserve_pool)}"
            logger.info(msg)
            append_scan_log(self.session, msg)
            
            # ZAPISZ STAN NATYCHMIAST PO INICJALIZACJI
            self._save_state()

        except Exception as e:
            logger.error(f"Faza 5: Błąd inicjalizacji: {e}")

    def run_cycle(self):
        # Jeśli pusto, inicjuj
        if not self.active_pool:
            self._initialize_pools()
            if not self.active_pool: return

        self._refresh_macro_context()
        
        tickers_to_remove = []
        signals_generated = 0

        for item in self.active_pool:
            ticker = item['ticker']
            time.sleep(CYCLE_DELAY)
            
            try:
                # === POPRAWKA WYDAJNOŚCI: outputsize='compact' ===
                # Pobiera tylko 100 ostatnich świec (wystarczy dla VWAP 50 i Vel 20)
                # Zamiast 'full' (30 dni), co dławiło worker.
                data = self.client.get_intraday(symbol=ticker, interval='5min', outputsize='compact')
                
                if not data or 'Time Series (5min)' not in data:
                    item['fails'] += 1
                    if item['fails'] >= 2: tickers_to_remove.append(ticker)
                    continue
                
                item['fails'] = 0 

                df = standardize_df_columns(pd.DataFrame.from_dict(data['Time Series (5min)'], orient='index'))
                df.index = pd.to_datetime(df.index)
                df.sort_index(inplace=True)

                metrics = calculate_flux_vectors(df)
                
                # Aktualizacja obiektu stanu
                item['price'] = float(df['close'].iloc[-1]) if not df.empty else 0.0
                item['elasticity'] = float(metrics.get('elasticity', 0.0))
                item['velocity'] = float(metrics.get('velocity', 0.0))
                item['flux_score'] = int(metrics.get('flux_score', 0))
                item['last_check'] = time.time()
                
                # Logika decyzyjna
                flux_score = item['flux_score']
                
                if flux_score >= FLUX_THRESHOLD_ENTRY:
                    if self.macro_context['bias'] != 'BEARISH':
                        metrics['price'] = item['price']
                        self._generate_signal(ticker, metrics)
                        signals_generated += 1
                        tickers_to_remove.append(ticker)
                
                elif flux_score < 20: 
                    tickers_to_remove.append(ticker)
                
                elif (time.time() - item['added_at']) > 1800: # 30 min bez akcji
                    tickers_to_remove.append(ticker)

            except Exception as e:
                logger.error(f"Faza 5: Błąd analizy {ticker}: {e}")
                item['fails'] += 1

        # Rotacja
        if tickers_to_remove:
            self.active_pool = [x for x in self.active_pool if x['ticker'] not in tickers_to_remove]
            
            while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
                new_ticker = self.reserve_pool.pop(0)
                self.active_pool.append({
                    'ticker': new_ticker, 'fails': 0, 'added_at': time.time(),
                    'price': 0.0, 'elasticity': 0.0, 'velocity': 0.0, 'flux_score': 0
                })
            
            if signals_generated > 0:
                append_scan_log(self.session, f"🌊 Faza 5: Rotacja. Nowe sygnały: {signals_generated}.")

        # === ZAPIS STANU NA KONIEC CYKLU (Persistence) ===
        self._save_state()

    def _save_state(self):
        """Zrzuca pełny stan (Active + Reserve + Macro) do bazy."""
        try:
            state_data = {
                "active_pool": self.active_pool,
                "reserve_pool": self.reserve_pool, # Ważne: zapisujemy też kolejkę!
                "macro_bias": self.macro_context.get('bias', 'NEUTRAL'),
                "reserve_count": len(self.reserve_pool),
                "last_updated": time.time()
            }
            update_system_control(self.session, 'phase5_monitor_state', json.dumps(state_data))
        except Exception as e:
            logger.error(f"Faza 5: Błąd zapisu stanu: {e}")

    def _generate_signal(self, ticker: str, metrics: dict):
        try:
            price = metrics.get('price', 0)
            if price == 0: return

            elasticity_abs = abs(metrics.get('elasticity', 1.0))
            sl_pct = 0.01 + (elasticity_abs * 0.005) 
            
            sl_price = price * (1 - sl_pct)
            tp_price = price * (1 + (sl_pct * 2.0)) 
            
            score = int(metrics.get('flux_score', 0))
            reason = metrics.get('signal_type', 'FLUX')
            
            note = f"STRATEGIA: FLUX V5\nTYP: {reason} | SCORE: {score}/100\nELASTICITY: {metrics.get('elasticity', 0):.2f}\nVELOCITY: {metrics.get('velocity', 0):.2f}x"

            exists = self.session.execute(
                text("SELECT 1 FROM trading_signals WHERE ticker=:t AND status IN ('ACTIVE', 'PENDING')"), 
                {'t': ticker}
            ).fetchone()
            
            if exists: return

            stmt = text("""
                INSERT INTO trading_signals (
                    ticker, status, generation_date, updated_at, 
                    entry_price, stop_loss, take_profit, risk_reward_ratio, 
                    notes, expiration_date, expected_profit_factor, expected_win_rate
                ) VALUES (
                    :ticker, 'PENDING', NOW(), NOW(),
                    :entry, :sl, :tp, 2.0,
                    :note, NOW() + INTERVAL '1 day', 3.0, 65.0
                )
            """)
            self.session.execute(stmt, {'ticker': ticker, 'entry': price, 'sl': sl_price, 'tp': tp_price, 'note': note})
            self.session.commit()
            
            msg = f"🌊 FLUX SYGNAŁ: {ticker} ({reason})"
            logger.info(msg)
            append_scan_log(self.session, msg)
            send_telegram_alert(msg)

        except Exception as e:
            logger.error(f"Faza 5: Błąd generowania sygnału: {e}")
            self.session.rollback()

def run_phase5_cycle(session: Session, api_client: AlphaVantageClient):
    analyzer = OmniFluxAnalyzer(session, api_client)
    analyzer.run_cycle()
