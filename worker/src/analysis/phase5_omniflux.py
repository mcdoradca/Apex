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
    get_system_control_value
)
from .flux_physics import calculate_flux_vectors, calculate_ofp

logger = logging.getLogger(__name__)

# === KONFIGURACJA OMNI-FLUX (V5.5 - ROTATION FIX & SORTING) ===
CAROUSEL_SIZE = 8          
RADAR_DELAY = 4.0          
SNIPER_COOLDOWN = 120      
FLUX_THRESHOLD_ENTRY = 65  
MACRO_CACHE_DURATION = 300 
DEFAULT_RR = 2.5           
DEFAULT_SL_PCT = 0.015     

MAX_SNIPES_PER_CYCLE = 2   

# === NOWE LIMITY ROTACJI ===
ROTATION_TIMEOUT_BORING = 60   # 1 minuta dla słabych (<30 pkt)
ROTATION_TIMEOUT_WARM = 300    # 5 minut dla średnich (30-64 pkt)

class OmniFluxAnalyzer:
    """
    APEX OMNI-FLUX ENGINE (V5.5 - Priority Display)
    Architektura: Radar (Bulk) + Prioritized Sniper Queue + Aggressive Rotation
    """

    def __init__(self, session: Session, api_client: AlphaVantageClient):
        self.session = session
        self.client = api_client
        
        # Domyślny stan
        self.active_pool = []       
        self.reserve_pool = []      
        self.macro_context = {'bias': 'NEUTRAL', 'last_updated': 0}
        self.cycle_counter = 0 # Licznik do logowania
        
        self._load_state()

    def _load_state(self):
        """Odtwarza stan z bazy i SANITYZUJE DANE."""
        try:
            raw_json = get_system_control_value(self.session, 'phase5_monitor_state')
            if raw_json:
                state = json.loads(raw_json)
                self.active_pool = state.get('active_pool', [])
                self.reserve_pool = state.get('reserve_pool', [])
                self.macro_context['bias'] = state.get('macro_bias', 'NEUTRAL')
                
                for item in self.active_pool:
                    if item.get('flux_score') is None: item['flux_score'] = 0
                    if item.get('elasticity') is None: item['elasticity'] = 0.0
                    if item.get('velocity') is None: item['velocity'] = 0.0
                    if item.get('last_sniper_check') is None: item['last_sniper_check'] = 0
                    if item.get('fails') is None: item['fails'] = 0
                    if item.get('added_at') is None: item['added_at'] = time.time()
                
                # Walidacja świeżości (reset po 15 min bezczynności)
                if time.time() - state.get('last_updated', 0) > 900:
                     logger.info("Faza 5: Stan przestarzały. Reset puli.")
                     self.active_pool = []
                     self.reserve_pool = []
        except Exception as e:
            logger.warning(f"Faza 5: Błąd odczytu stanu: {e}")
            self.active_pool = []

    def _refresh_macro_context(self):
        """Sprawdza sentyment makro (EUR/USD) raz na 5 minut."""
        now = time.time()
        last_upd = self.macro_context.get('last_updated') or 0
        
        if now - last_upd < MACRO_CACHE_DURATION:
            return

        try:
            # FXE (Euro Trust) jako proxy dla EUR/USD
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
        """Napełnia pulę startową z Fazy 1 i X."""
        try:
            # Pobieramy kandydatów. Jeśli Faza 1 jest "pusta" (brak trend score), SQL zwróci alfabetycznie.
            # To jest częste przy starcie.
            p1_rows = self.session.execute(text("SELECT ticker FROM phase1_candidates ORDER BY sector_trend_score DESC NULLS LAST LIMIT 40")).fetchall()
            px_rows = self.session.execute(text("SELECT ticker FROM phasex_candidates ORDER BY last_pump_percent DESC NULLS LAST LIMIT 20")).fetchall()
            
            combined_tickers = list(set([r[0] for r in p1_rows] + [r[0] for r in px_rows]))
            
            active_sigs = [r[0] for r in self.session.execute(text("SELECT ticker FROM trading_signals WHERE status IN ('ACTIVE', 'PENDING')")).fetchall()]
            holdings = [r[0] for r in self.session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()]
            exclude = set(active_sigs + holdings)
            
            self.reserve_pool = [t for t in combined_tickers if t not in exclude]
            
            # Wypełnij Active Pool
            while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
                ticker = self.reserve_pool.pop(0)
                self.active_pool.append({
                    'ticker': ticker, 
                    'fails': 0, 
                    'added_at': time.time(),
                    'last_sniper_check': 0,
                    'price': 0.0, 
                    'elasticity': 0.0, 
                    'velocity': 0.0, 
                    'flux_score': 0,
                    'ofp': 0.0,
                    'stop_loss': None,      
                    'take_profit': None,    
                    'risk_reward': None     
                })
            
            msg = f"🌊 Faza 5: Inicjalizacja. Aktywne: {len(self.active_pool)}"
            logger.info(msg)
            append_scan_log(self.session, msg)
            self._save_state()

        except Exception as e:
            logger.error(f"Faza 5: Błąd inicjalizacji: {e}")

    def run_cycle(self):
        self.cycle_counter += 1
        
        if not self.active_pool:
            self._initialize_pools()
            if not self.active_pool: return

        self._refresh_macro_context()
        
        # 1. RADAR SCAN
        tickers = [item['ticker'] for item in self.active_pool]
        radar_hits = self.client.get_bulk_quotes_parsed(tickers) 
        radar_map = {d['symbol']: d for d in radar_hits}
        
        tickers_to_remove = []
        signals_generated = 0
        sniper_queue = []

        # 2. UPDATE & SCORE
        for item in self.active_pool:
            ticker = item['ticker']
            radar_data = radar_map.get(ticker)
            
            if radar_data:
                new_price = radar_data.get('price', 0.0)
                bid_sz = radar_data.get('bid_size', 0.0)
                ask_sz = radar_data.get('ask_size', 0.0)
                ofp = calculate_ofp(bid_sz, ask_sz)
                old_price = item.get('price', 0.0)
                price_change_pct = abs((new_price - old_price) / old_price) if old_price > 0 else 0
                
                item['price'] = new_price
                item['ofp'] = ofp
                item['fails'] = 0 
                
                priority_score = 0
                needs_update = False
                
                elast = item.get('elasticity') or 0.0
                flx_scr = item.get('flux_score') or 0
                last_chk = item.get('last_sniper_check') or 0
                
                # Logika Priorytetów Odświeżania
                if elast == 0: 
                    priority_score += 100; needs_update = True
                elif abs(ofp) > 0.4: 
                    priority_score += 50; needs_update = True
                elif price_change_pct > 0.002: 
                    priority_score += 40; needs_update = True
                elif flx_scr > 60:
                    priority_score += 30; needs_update = True
                elif (time.time() - last_chk) > SNIPER_COOLDOWN:
                    priority_score += 10; needs_update = True
                
                if needs_update:
                    sniper_queue.append({'item': item, 'priority': priority_score})
            else:
                item['fails'] = (item.get('fails') or 0) + 1
                if item['fails'] >= 3: tickers_to_remove.append(ticker)

        # 3. SNIPER SHOTS (Pobieranie Intraday)
        sniper_queue.sort(key=lambda x: x['priority'], reverse=True)
        targets = sniper_queue[:MAX_SNIPES_PER_CYCLE]
        
        if targets:
            logger.info(f"Faza 5: Aktualizacja Intraday dla {len(targets)} celów.")

        for target_obj in targets:
            item = target_obj['item']
            ticker = item['ticker']
            
            try:
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
                        bias = self.macro_context.get('bias', 'NEUTRAL')
                        if bias != 'BEARISH':
                            metrics['price'] = item['price']
                            if self._generate_signal(ticker, metrics, sl_price, tp_price):
                                signals_generated += 1
            except Exception as e:
                logger.error(f"Faza 5 Sniper Error ({ticker}): {e}")

        # === 4. WYMUSZONE SORTOWANIE (TWOJA PROŚBA) ===
        # Sortujemy aktywną pulę tak, aby spółki z Flux Score > 64 były ZAWSZE PIERWSZE.
        # W drugiej kolejności decyduje wysokość wyniku.
        self.active_pool.sort(key=lambda x: (
            1 if (x.get('flux_score') or 0) >= 64 else 0, # Najpierw Zielone Kafelki
            x.get('flux_score') or 0                      # Potem najwyższy wynik
        ), reverse=True)

        # === 5. AGRESYWNA ROTACJA (NAPRAWA KARUZELI) ===
        for item in self.active_pool:
            flx = item.get('flux_score') or 0
            added = item.get('added_at') or 0
            life_time = time.time() - added
            
            should_remove = False
            
            # Warunek A: Słabe spółki (<30) usuwamy po 60 sekundach
            if flx < 30 and life_time > ROTATION_TIMEOUT_BORING:
                should_remove = True
            
            # Warunek B: Średnie spółki (30-64) usuwamy po 5 minutach
            elif flx < 64 and life_time > ROTATION_TIMEOUT_WARM:
                should_remove = True
            
            # Spółki > 64 (Zielone) zostają, dopóki wynik nie spadnie!

            if should_remove and self.reserve_pool:
                if item['ticker'] not in tickers_to_remove:
                    tickers_to_remove.append(item['ticker'])

        if tickers_to_remove:
            self._rotate_pool(tickers_to_remove)
            if signals_generated > 0:
                append_scan_log(self.session, f"🌊 Faza 5: Wygenerowano {signals_generated} sygnałów.")
            else:
                # Logujemy rotację, żebyś widział że system działa
                top_removed = tickers_to_remove[:3]
                msg_more = f" (+{len(tickers_to_remove)-3} inni)" if len(tickers_to_remove) > 3 else ""
                logger.info(f"Faza 5: Rotacja {', '.join(top_removed)}{msg_more}")

        # === 6. PULS SYSTEMU (LOGOWANIE) ===
        if self.cycle_counter % 15 == 0: # Co ok. 60 sekund
            top_stocks = [f"{i['ticker']}({i.get('flux_score',0)})" for i in self.active_pool[:5]]
            pulse_msg = f"🌊 F5 PULS: {', '.join(top_stocks)}... (Rezerwa: {len(self.reserve_pool)})"
            append_scan_log(self.session, pulse_msg)

        self._save_state()
        time.sleep(RADAR_DELAY)

    def _rotate_pool(self, remove_list):
        remove_list = list(set(remove_list))
        # Usuń stare
        self.active_pool = [x for x in self.active_pool if x['ticker'] not in remove_list]
        
        # Dodaj nowe z rezerwy
        while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
            new_ticker = self.reserve_pool.pop(0)
            # Jeśli rezerwa się kończy, wrzucamy usuniętą spółkę na koniec kolejki (Recykling)
            # aby uniknąć pustego ekranu
            self.reserve_pool.append(new_ticker) 
            
            self.active_pool.append({
                'ticker': new_ticker, 'fails': 0, 'added_at': time.time(), 'last_sniper_check': 0,
                'price': 0.0, 'elasticity': 0.0, 'velocity': 0.0, 'flux_score': 0, 'ofp': 0.0,
                'stop_loss': None, 'take_profit': None, 'risk_reward': None
            })

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
            
            note = f"STRATEGIA: FLUX V5.5\nTYP: {reason} | SCORE: {score}/100\nOFP: {ofp_val:.2f} (Presja)\nELASTICITY: {metrics.get('elasticity', 0):.2f}σ"

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
