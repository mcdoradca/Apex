import logging
import pandas as pd
import time
import json
from collections import deque
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone

# Importy z projektu
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models
from .utils import (
    standardize_df_columns, 
    append_scan_log, 
    send_telegram_alert, 
    update_system_control,
    get_system_control_value
)
from .flux_physics import calculate_flux_vectors, calculate_ofp

logger = logging.getLogger(__name__)

# === KONFIGURACJA OMNI-FLUX ===
CAROUSEL_SIZE = 10         
RADAR_DELAY_BASE = 3.0     
SNIPER_COOLDOWN = 180      
FLUX_THRESHOLD_ENTRY = 70  
MACRO_CACHE_DURATION = 300 
DEFAULT_RR = 2.5           
DEFAULT_SL_PCT = 0.015     

# Limit dla Premium (z zapasem bezpieczeństwa)
API_CALLS_LIMIT_PER_MIN = 140 

_GLOBAL_FLUX_ANALYZER = None

class OmniFluxAnalyzer:
    """
    APEX OMNI-FLUX ENGINE (V6.0 - Pure Premium Logic)
    Przywrócono natywną obsługę Bulk Quotes bez protez.
    """

    def __init__(self, api_client: AlphaVantageClient):
        self.session = None 
        self.client = api_client
        self.active_pool = []       
        self.reserve_pool = []      
        self.macro_context = {'bias': 'NEUTRAL', 'last_updated': 0}
        self.api_call_timestamps = deque() 
        self.state_loaded = False

    def _record_api_call(self):
        self.api_call_timestamps.append(time.time())

    def _can_make_api_call(self) -> bool:
        now = time.time()
        while self.api_call_timestamps and self.api_call_timestamps[0] < now - 60:
            self.api_call_timestamps.popleft()
        return len(self.api_call_timestamps) < API_CALLS_LIMIT_PER_MIN

    def _load_state(self):
        try:
            if not self.session: return
            raw_json = get_system_control_value(self.session, 'phase5_monitor_state')
            if raw_json:
                state = json.loads(raw_json)
                self.active_pool = state.get('active_pool', [])
                self.reserve_pool = state.get('reserve_pool', [])
                self.macro_context['bias'] = state.get('macro_bias', 'NEUTRAL')
                
                if time.time() - state.get('last_updated', 0) > 86400:
                     logger.info("Faza 5: Stan przestarzały (>24h). Reset puli.")
                     self.active_pool = []
                     self.reserve_pool = []
        except Exception as e:
            logger.warning(f"Faza 5: Błąd odczytu stanu: {e}")
            self.active_pool = []

    def _save_state(self):
        try:
            if not self.session: return
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

    def _refresh_macro_context(self):
        # Pobieramy dane makro, ale nie używamy ich do blokowania (tylko informacyjnie)
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
            current_active = [x['ticker'] for x in self.active_pool]
            
            exclude = set(active_sigs + holdings + current_active)
            new_candidates = [t for t in combined_tickers if t not in exclude]
            
            current_reserve_set = set(self.reserve_pool)
            added_count = 0
            
            for t in new_candidates:
                if t not in current_reserve_set:
                    self.reserve_pool.append(t)
                    current_reserve_set.add(t)
                    added_count += 1
            
            while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
                ticker = self.reserve_pool.pop(0)
                self.active_pool.append({
                    'ticker': ticker, 'fails': 0, 'added_at': time.time(), 'last_sniper_check': 0,
                    'price': 0.0, 'elasticity': 0.0, 'velocity': 0.0, 'flux_score': 0, 'ofp': 0.0,
                    'stop_loss': 0.0, 'take_profit': 0.0, 'risk_reward': 0.0     
                })
            
            if added_count > 0:
                msg = f"🌊 Faza 5 (Refill): Dodano {added_count} kandydatów. Aktywne: {len(self.active_pool)}"
                logger.info(msg)
                append_scan_log(self.session, msg)
                self._save_state()
            
        except Exception as e:
            logger.error(f"Faza 5: Błąd inicjalizacji: {e}")

    def run_cycle_step(self, session: Session):
        self.session = session
        
        try:
            if not self.state_loaded:
                self._load_state()
                self.state_loaded = True

            if not self.active_pool or len(self.reserve_pool) < 5:
                self._initialize_pools()
                
            if not self.active_pool: 
                return 

            self._refresh_macro_context()
            
            if not self._can_make_api_call():
                return 

            # 3. RADAR (Bulk Quotes - PREMIUM ONLY)
            # Tutaj polegamy wyłącznie na stabilności AlphaVantageClient
            tickers = [item['ticker'] for item in self.active_pool]
            if not tickers: return

            self._record_api_call()
            radar_hits = self.client.get_bulk_quotes_parsed(tickers) 
            radar_map = {d['symbol']: d for d in radar_hits}
            
            tickers_to_remove = []
            signals_generated = 0
            sniper_queue = []

            # 4. ANALIZA RADARU
            for item in self.active_pool:
                ticker = item['ticker']
                radar_data = radar_map.get(ticker)
                
                if radar_data:
                    new_price = float(radar_data.get('price') or 0.0)
                    bid_sz = float(radar_data.get('bid_size') or 0.0)
                    ask_sz = float(radar_data.get('ask_size') or 0.0)
                    volume = float(radar_data.get('volume') or 0.0)
                    
                    if volume < 100: # Minimalny filtr na martwe spółki
                         item['fails'] = (item.get('fails') or 0) + 1
                         continue

                    ofp = 0.0
                    if bid_sz > 0 and ask_sz > 0:
                        ofp = calculate_ofp(bid_sz, ask_sz)
                    
                    old_price = float(item.get('price') or 0.0)
                    price_change_pct = abs((new_price - old_price) / old_price) if old_price > 0 else 0.0
                    
                    item['price'] = new_price
                    item['ofp'] = ofp
                    item['fails'] = 0 
                    
                    flux_score = float(item.get('flux_score') or 0.0)
                    last_check = float(item.get('last_sniper_check') or 0.0)
                    elasticity = float(item.get('elasticity') or 0.0)
                    current_ofp = float(item.get('ofp') or 0.0)

                    priority_score = 0
                    needs_update = False
                    
                    # Logika priorytetów Snajpera
                    if old_price == 0 and new_price > 0:
                        priority_score += 100; needs_update = True
                    elif price_change_pct > 0.003: 
                        priority_score += 40; needs_update = True 
                    elif flux_score > 60:
                        priority_score += 30; needs_update = True 
                    elif abs(current_ofp) > 0.4: 
                        priority_score += 20; needs_update = True
                    elif (time.time() - last_check) > SNIPER_COOLDOWN:
                        priority_score += 10; needs_update = True 
                    
                    if needs_update:
                        sniper_queue.append({'item': item, 'priority': priority_score})
                else:
                    item['fails'] = (item.get('fails') or 0) + 1
                    if item['fails'] >= 5: tickers_to_remove.append(ticker)

            # 5. SNIPER
            sniper_queue.sort(key=lambda x: x['priority'], reverse=True)
            api_budget = max(0, API_CALLS_LIMIT_PER_MIN - len(self.api_call_timestamps))
            max_snipes = min(api_budget, 4) # Zwiększono limit dla Premium
            
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
                        
                        safe_ofp = float(item.get('ofp') or 0.0)
                        metrics = calculate_flux_vectors(df, current_ofp=safe_ofp)
                        
                        price = float(item.get('price') or df['close'].iloc[-1])
                        sl_price = price * (1 - DEFAULT_SL_PCT)
                        tp_price = price + ((price - sl_price) * DEFAULT_RR)
                        
                        item['elasticity'] = float(metrics.get('elasticity') or 0.0)
                        item['velocity'] = float(metrics.get('velocity') or 0.0)
                        item['flux_score'] = int(metrics.get('flux_score') or 0)
                        item['last_sniper_check'] = time.time() 
                        item['stop_loss'] = sl_price             
                        item['take_profit'] = tp_price           
                        item['risk_reward'] = DEFAULT_RR         
                        
                        # Generowanie sygnału - WYŁĄCZONO ZABEZPIECZENIE BEARISH
                        if item['flux_score'] >= FLUX_THRESHOLD_ENTRY:
                            # if self.macro_context['bias'] != 'BEARISH': <--- USUNIĘTO
                            metrics['price'] = price
                            if self._generate_signal(ticker, metrics, sl_price, tp_price):
                                signals_generated += 1
                except Exception as e:
                    logger.error(f"Faza 5 Sniper Error ({ticker}): {e}")

            # 6. ROTACJA
            for item in self.active_pool:
                score = float(item.get('flux_score') or 0.0)
                added_at = float(item.get('added_at') or 0.0)
                
                if self.reserve_pool and score < 30 and (time.time() - added_at) > 1500:
                    if item['ticker'] not in tickers_to_remove: tickers_to_remove.append(item['ticker'])

            if tickers_to_remove:
                self._rotate_pool(tickers_to_remove)
                if signals_generated > 0: 
                    append_scan_log(self.session, f"🌊 Faza 5: Wygenerowano {signals_generated} sygnałów.")

            self._save_state()
            
        finally:
            self.session = None

    def _rotate_pool(self, remove_list):
        remove_list = list(set(remove_list))
        self.active_pool = [x for x in self.active_pool if x['ticker'] not in remove_list]
        
        while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
            new_ticker = self.reserve_pool.pop(0)
            self.active_pool.append({
                'ticker': new_ticker, 'fails': 0, 'added_at': time.time(), 'last_sniper_check': 0,
                'price': 0.0, 'elasticity': 0.0, 'velocity': 0.0, 'flux_score': 0, 'ofp': 0.0,
                'stop_loss': 0.0, 'take_profit': 0.0, 'risk_reward': 0.0
            })
            
        if not self.reserve_pool and len(self.active_pool) < CAROUSEL_SIZE:
             self._initialize_pools()

    def _generate_signal(self, ticker: str, metrics: dict, sl_price: float, tp_price: float) -> bool:
        try:
            if not self.session: return False
            price = metrics.get('price', 0)
            if price == 0: return False

            reason = metrics.get('signal_type', 'FLUX')
            score = int(metrics.get('flux_score', 0))
            ofp_val = metrics.get('ofp', 0.0)
            velocity_val = metrics.get('velocity', 0.0)
            elast_val = metrics.get('elasticity', 0.0)
            
            note = (
                f"STRATEGIA: FLUX V6.0\n"
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
            if self.session: self.session.rollback()
            return False

def run_phase5_cycle(session: Session, api_client: AlphaVantageClient):
    global _GLOBAL_FLUX_ANALYZER
    if _GLOBAL_FLUX_ANALYZER is None:
        _GLOBAL_FLUX_ANALYZER = OmniFluxAnalyzer(api_client)
    _GLOBAL_FLUX_ANALYZER.run_cycle_step(session)
