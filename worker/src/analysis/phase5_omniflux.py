import logging
import pandas as pd
import time
import json
from collections import deque
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
    get_system_control_value
)
from .flux_physics import calculate_flux_vectors, calculate_ofp

logger = logging.getLogger(__name__)

# === KONFIGURACJA OMNI-FLUX (V5.4 - SMART API MANAGEMENT) ===
CAROUSEL_SIZE = 10         
RADAR_DELAY_BASE = 3.0     
SNIPER_COOLDOWN = 180      
FLUX_THRESHOLD_ENTRY = 70  
MACRO_CACHE_DURATION = 300 
DEFAULT_RR = 2.5           
DEFAULT_SL_PCT = 0.015     

# Dynamiczny limit zapytań (z Twojej wersji)
API_CALLS_LIMIT_PER_MIN = 120 

class OmniFluxAnalyzer:
    """
    APEX OMNI-FLUX ENGINE (V5.4)
    Architektura: Radar (Bulk) + Dynamic Sniper Budget + Smart Filtering
    """

    def __init__(self, session: Session, api_client: AlphaVantageClient):
        self.session = session
        self.client = api_client
        
        # Stan wewnętrzny
        self.active_pool = []       
        self.reserve_pool = []      
        self.macro_context = {'bias': 'NEUTRAL', 'last_updated': 0}
        
        # Historia zapytań do API
        self.api_call_timestamps = deque() 

        self._load_state()

    def _record_api_call(self):
        self.api_call_timestamps.append(time.time())

    def _can_make_api_call(self) -> bool:
        now = time.time()
        while self.api_call_timestamps and self.api_call_timestamps[0] < now - 60:
            self.api_call_timestamps.popleft()
        return len(self.api_call_timestamps) < API_CALLS_LIMIT_PER_MIN

    def _load_state(self):
        try:
            raw_json = get_system_control_value(self.session, 'phase5_monitor_state')
            if raw_json:
                state = json.loads(raw_json)
                self.active_pool = state.get('active_pool', [])
                self.reserve_pool = state.get('reserve_pool', [])
                self.macro_context['bias'] = state.get('macro_bias', 'NEUTRAL')
                if time.time() - state.get('last_updated', 0) > 1800:
                     logger.info("Faza 5: Stan przestarzały. Reset puli.")
                     self.active_pool = []
                     self.reserve_pool = []
        except Exception as e:
            logger.warning(f"Faza 5: Błąd odczytu stanu: {e}")
            self.active_pool = []

    def _refresh_macro_context(self):
        now = time.time()
        if now - self.macro_context['last_updated'] < MACRO_CACHE_DURATION: return
        if not self._can_make_api_call(): return

        try:
            self._record_api_call()
            fx_data = self.client.get_intraday(symbol='FXE', interval='60min', outputsize='compact')
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
        try:
            p1_rows = self.session.execute(text("SELECT ticker FROM phase1_candidates ORDER BY sector_trend_score DESC NULLS LAST LIMIT 40")).fetchall()
            px_rows = self.session.execute(text("SELECT ticker FROM phasex_candidates ORDER BY last_pump_percent DESC NULLS LAST LIMIT 20")).fetchall()
            combined_tickers = list(set([r[0] for r in p1_rows] + [r[0] for r in px_rows]))
            active_sigs = [r[0] for r in self.session.execute(text("SELECT ticker FROM trading_signals WHERE status IN ('ACTIVE', 'PENDING')")).fetchall()]
            holdings = [r[0] for r in self.session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()]
            exclude = set(active_sigs + holdings)
            self.reserve_pool = [t for t in combined_tickers if t not in exclude]
            while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
                ticker = self.reserve_pool.pop(0)
                self.active_pool.append({
                    'ticker': ticker, 'fails': 0, 'added_at': time.time(), 'last_sniper_check': 0,
                    'price': 0.0, 'elasticity': 0.0, 'velocity': 0.0, 'flux_score': 0, 'ofp': 0.0,
                    'stop_loss': None, 'take_profit': None, 'risk_reward': None     
                })
            msg = f"🌊 Faza 5 (Smart API): Inicjalizacja. Aktywne: {len(self.active_pool)}, Rezerwa: {len(self.reserve_pool)}"
            logger.info(msg)
            append_scan_log(self.session, msg)
            self._save_state()
        except Exception as e:
            logger.error(f"Faza 5: Błąd inicjalizacji: {e}")

    def run_cycle(self):
        if not self.active_pool:
            self._initialize_pools()
            if not self.active_pool: 
                time.sleep(5)
                return

        self._refresh_macro_context()
        
        if not self._can_make_api_call():
            logger.warning("Faza 5: Limit API (lokalny) osiągnięty. Czekam...")
            time.sleep(2.0)
            return

        tickers = [item['ticker'] for item in self.active_pool]
        self._record_api_call()
        radar_hits = self.client.get_bulk_quotes_parsed(tickers) 
        radar_map = {d['symbol']: d for d in radar_hits}
        
        tickers_to_remove = []
        signals_generated = 0
        sniper_queue = []

        for item in self.active_pool:
            ticker = item['ticker']
            radar_data = radar_map.get(ticker)
            if radar_data:
                new_price = radar_data.get('price', 0.0)
                bid_sz = radar_data.get('bid_size', 0.0)
                ask_sz = radar_data.get('ask_size', 0.0)
                volume = radar_data.get('volume', 0.0)
                if volume < 1000:
                     item['fails'] += 1
                     continue

                ofp = 0.0
                if bid_sz > 0 and ask_sz > 0:
                    ofp = calculate_ofp(bid_sz, ask_sz)
                
                old_price = item.get('price', 0.0)
                price_change_pct = abs((new_price - old_price) / old_price) if old_price > 0 else 0
                
                item['price'] = new_price
                item['ofp'] = ofp
                item['fails'] = 0 
                
                priority_score = 0
                needs_update = False
                
                if item.get('elasticity') == 0: 
                    priority_score += 100; needs_update = True
                elif price_change_pct > 0.003: 
                    priority_score += 40; needs_update = True
                elif item.get('flux_score', 0) > 60:
                    priority_score += 30; needs_update = True
                elif abs(ofp) > 0.4: 
                    priority_score += 20; needs_update = True
                elif (time.time() - item.get('last_sniper_check', 0)) > SNIPER_COOLDOWN:
                    priority_score += 10; needs_update = True
                
                if needs_update:
                    sniper_queue.append({'item': item, 'priority': priority_score})
            else:
                item['fails'] += 1
                if item['fails'] >= 3: tickers_to_remove.append(ticker)

        sniper_queue.sort(key=lambda x: x['priority'], reverse=True)
        api_budget = max(0, API_CALLS_LIMIT_PER_MIN - len(self.api_call_timestamps))
        max_snipes = min(api_budget, 3) 
        targets = sniper_queue[:max_snipes]
        
        for target_obj in targets:
            item = target_obj['item']
            ticker = item['ticker']
            try:
                self._record_api_call()
                raw_intraday = self.client.get_intraday(ticker, interval='5min', outputsize='compact')
                if raw_intraday and 'Time Series (5min)' in raw_intraday:
                    df = standardize_df_columns(pd.DataFrame.from_dict(raw_intraday['Time Series (5min)'], orient='index'))
                    df.index = pd.to_datetime(df.index)
                    df.sort_index(inplace=True)
                    metrics = calculate_flux_vectors(df, current_ofp=item['ofp'])
                    
                    price = item['price']
                    sl_price = price * (1 - DEFAULT_SL_PCT)
                    risk_usd = price - sl_price 
                    tp_price = price + (risk_usd * DEFAULT_RR)
                    
                    item['elasticity'] = float(metrics.get('elasticity', 0.0))
                    item['velocity'] = float(metrics.get('velocity', 0.0))
                    item['flux_score'] = int(metrics.get('flux_score', 0))
                    item['last_sniper_check'] = time.time() 
                    item['stop_loss'] = sl_price             
                    item['take_profit'] = tp_price           
                    item['risk_reward'] = DEFAULT_RR         
                    
                    if item['flux_score'] >= FLUX_THRESHOLD_ENTRY:
                        if self.macro_context['bias'] != 'BEARISH':
                            metrics['price'] = item['price']
                            if self._generate_signal(ticker, metrics, sl_price, tp_price):
                                signals_generated += 1
            except Exception as e:
                logger.error(f"Faza 5 Sniper Error ({ticker}): {e}")

        for item in self.active_pool:
            if self.reserve_pool and item.get('flux_score', 0) < 30 and (time.time() - item.get('added_at', 0)) > 1500:
                if item['ticker'] not in tickers_to_remove: tickers_to_remove.append(item['ticker'])

        if tickers_to_remove:
            self._rotate_pool(tickers_to_remove)
            if signals_generated > 0: append_scan_log(self.session, f"🌊 Faza 5: Wygenerowano {signals_generated} sygnałów.")

        self._save_state()
        
        current_load = len(self.api_call_timestamps)
        delay = RADAR_DELAY_BASE
        if current_load > (API_CALLS_LIMIT_PER_MIN * 0.8): delay = RADAR_DELAY_BASE * 2.0
        time.sleep(delay)

    def _rotate_pool(self, remove_list):
        remove_list = list(set(remove_list))
        self.active_pool = [x for x in self.active_pool if x['ticker'] not in remove_list]
        while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
            new_ticker = self.reserve_pool.pop(0)
            self.active_pool.append({
                'ticker': new_ticker, 'fails': 0, 'added_at': time.time(), 'last_sniper_check': 0,
                'price': 0.0, 'elasticity': 0.0, 'velocity': 0.0, 'flux_score': 0, 'ofp': 0.0,
                'stop_loss': None, 'take_profit': None, 'risk_reward': None
            })
        if not self.reserve_pool and len(self.active_pool) < CAROUSEL_SIZE:
             self._initialize_pools()

    def _save_state(self):
        try:
            state_data = {
                "active_pool": self.active_pool,
                "reserve_pool": self.reserve_pool,
                "macro_bias": self.macro_context.get('bias', 'NEUTRAL'),
                "reserve_count": len(self.reserve_pool),
                "last_updated": time.time()
            }
            update_system_control(self.session, 'phase5_monitor_state', json.dumps(state_data))
        except Exception as e:
            logger.error(f"Faza 5: Błąd zapisu stanu: {e}")

    def _generate_signal(self, ticker: str, metrics: dict, sl_price: float, tp_price: float) -> bool:
        try:
            price = metrics.get('price', 0)
            if price == 0: return False

            reason = metrics.get('signal_type', 'FLUX')
            score = int(metrics.get('flux_score', 0))
            ofp_val = metrics.get('ofp', 0.0)
            velocity_val = metrics.get('velocity', 0.0)
            elast_val = metrics.get('elasticity', 0.0)
            
            # === ROZSZERZONA NOTATKA DLA "LEARNING LOOP" ===
            # Dodajemy tagi VELOCITY i ELASTICITY w formacie łatwym do parsowania.
            note = (
                f"STRATEGIA: FLUX V5.4\n"
                f"TYP: {reason} | SCORE: {score}/100\n"
                f"OFP: {ofp_val:.2f} (Presja)\n"
                f"ELASTICITY: {elast_val:.2f}σ\n"
                f"VELOCITY: {velocity_val:.2f}"
            )

            exists = self.session.execute(
                text("SELECT 1 FROM trading_signals WHERE ticker=:t AND status IN ('ACTIVE', 'PENDING') AND notes LIKE '%STRATEGIA: FLUX%'"), 
                {'t': ticker}
            ).fetchone()
            
            if exists: 
                update_stmt = text("""
                    UPDATE trading_signals SET
                        updated_at = NOW(), notes = :note,
                        entry_price = :entry, stop_loss = :sl, take_profit = :tp
                    WHERE ticker = :ticker AND status IN ('ACTIVE', 'PENDING') AND notes LIKE '%STRATEGIA: FLUX%'
                """)
                self.session.execute(update_stmt, {'ticker': ticker, 'entry': price, 'sl': sl_price, 'tp': tp_price, 'note': note})
                self.session.commit()
                return True
            
            stmt = text("""
                INSERT INTO trading_signals (
                    ticker, status, generation_date, updated_at, 
                    entry_price, stop_loss, take_profit, risk_reward_ratio, 
                    notes, expiration_date, expected_profit_factor, expected_win_rate
                ) VALUES (
                    :ticker, 'PENDING', NOW(), NOW(),
                    :entry, :sl, :tp, :rr,
                    :note, NOW() + INTERVAL '1 day', 3.5, 60.0
                )
            """)
            self.session.execute(stmt, {'ticker': ticker, 'entry': price, 'sl': sl_price, 'tp': tp_price, 'rr': DEFAULT_RR, 'note': note})
            self.session.commit()
            
            msg = f"🌊 FLUX SYGNAŁ: {ticker} ({reason}) OFP:{ofp_val:.2f}"
            logger.info(msg)
            append_scan_log(self.session, msg)
            send_telegram_alert(msg)
            return True

        except Exception as e:
            logger.error(f"Faza 5: Błąd generowania/aktualizacji sygnału: {e}")
            self.session.rollback()
            return False

def run_phase5_cycle(session: Session, api_client: AlphaVantageClient):
    analyzer = OmniFluxAnalyzer(session, api_client)
    analyzer.run_cycle()
