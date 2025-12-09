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

# === KONFIGURACJA OMNI-FLUX (V5.3 - API SAFE MODE) ===
CAROUSEL_SIZE = 8          # Rozmiar aktywnej puli
RADAR_DELAY = 4.0          # ZWIĘKSZONO: Opóźnienie pętli (sekundy), aby dać oddech innym agentom
SNIPER_COOLDOWN = 120      # ZWIĘKSZONO: Rzadsze odświeżanie "stale data" (co 2 min)
FLUX_THRESHOLD_ENTRY = 70  # Min. score do sygnału
MACRO_CACHE_DURATION = 300 
DEFAULT_RR = 2.5           
DEFAULT_SL_PCT = 0.015     

# NOWOŚĆ: Limit zapytań Intraday na jeden cykl pętli
MAX_SNIPES_PER_CYCLE = 2   

class OmniFluxAnalyzer:
    """
    APEX OMNI-FLUX ENGINE (V5.3 - API Traffic Control)
    Architektura: Radar (Bulk) + Prioritized Sniper Queue
    """

    def __init__(self, session: Session, api_client: AlphaVantageClient):
        self.session = session
        self.client = api_client
        
        # Domyślny stan
        self.active_pool = []       
        self.reserve_pool = []      
        self.macro_context = {'bias': 'NEUTRAL', 'last_updated': 0}
        
        self._load_state()

    def _load_state(self):
        """Odtwarza stan z bazy i SANITYZUJE DANE (Naprawa błędu NoneType)."""
        try:
            raw_json = get_system_control_value(self.session, 'phase5_monitor_state')
            if raw_json:
                state = json.loads(raw_json)
                self.active_pool = state.get('active_pool', [])
                self.reserve_pool = state.get('reserve_pool', [])
                self.macro_context['bias'] = state.get('macro_bias', 'NEUTRAL')
                
                # === FIX: Sanityzacja Active Pool ===
                # Zapobiega błędom TypeError: '>' not supported between instances of 'NoneType' and 'int'
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
        # Zabezpieczenie przed None w last_updated
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
                    # EUR up (FXE up) = USD down = Risk On
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
            
            msg = f"🌊 Faza 5 (Traffic Control): Inicjalizacja. Aktywne: {len(self.active_pool)}"
            logger.info(msg)
            append_scan_log(self.session, msg)
            self._save_state()

        except Exception as e:
            logger.error(f"Faza 5: Błąd inicjalizacji: {e}")

    def run_cycle(self):
        """
        GŁÓWNA PĘTLA HYBRYDOWA Z LIMITAMI API.
        """
        # 1. Inicjalizacja
        if not self.active_pool:
            self._initialize_pools()
            if not self.active_pool: return

        self._refresh_macro_context()
        
        # 2. RADAR SCAN (1 API Call - Tani)
        tickers = [item['ticker'] for item in self.active_pool]
        radar_hits = self.client.get_bulk_quotes_parsed(tickers) 
        radar_map = {d['symbol']: d for d in radar_hits}
        
        tickers_to_remove = []
        signals_generated = 0
        
        # Lista kandydatów do "strzału snajperskiego" (Pobranie Intraday)
        sniper_queue = []

        # 3. AKTUALIZACJA RADARU I IDENTYFIKACJA KANDYDATÓW
        for item in self.active_pool:
            ticker = item['ticker']
            radar_data = radar_map.get(ticker)
            
            if radar_data:
                # Aktualizacja lekkich danych
                new_price = radar_data.get('price', 0.0)
                bid_sz = radar_data.get('bid_size', 0.0)
                ask_sz = radar_data.get('ask_size', 0.0)
                
                # Oblicz OFP
                ofp = calculate_ofp(bid_sz, ask_sz)
                
                old_price = item.get('price', 0.0)
                # Zwiększamy próg czułości zmiany ceny do 0.2%, żeby nie spamować API szumem
                price_change_pct = abs((new_price - old_price) / old_price) if old_price > 0 else 0
                
                item['price'] = new_price
                item['ofp'] = ofp
                item['fails'] = 0 
                
                # OCENA PRIORYTETU DO POBRANIA INTRADAY
                priority_score = 0
                needs_update = False
                
                # Bezpieczne pobieranie wartości (FIX NoneType > int)
                elast = item.get('elasticity') or 0.0
                flx_scr = item.get('flux_score') or 0
                last_chk = item.get('last_sniper_check') or 0
                
                # Priorytet 1: Inicjalizacja (Brak danych)
                if elast == 0: 
                    priority_score += 100
                    needs_update = True
                
                # Priorytet 2: Silna Presja (OFP)
                elif abs(ofp) > 0.4: 
                    priority_score += 50
                    needs_update = True
                
                # Priorytet 3: Ruch cenowy > 0.2%
                elif price_change_pct > 0.002: 
                    priority_score += 40
                    needs_update = True
                
                # Priorytet 4: Blisko sygnału (Safe Check)
                elif flx_scr > 60:
                    priority_score += 30
                    needs_update = True
                    
                # Priorytet 5: Przestarzałe dane (Cooldown)
                elif (time.time() - last_chk) > SNIPER_COOLDOWN:
                    priority_score += 10
                    needs_update = True
                
                if needs_update:
                    sniper_queue.append({
                        'item': item,
                        'priority': priority_score
                    })
            else:
                item['fails'] = (item.get('fails') or 0) + 1
                if item['fails'] >= 3: tickers_to_remove.append(ticker)

        # 4. WYKONANIE "STRZAŁÓW" SNIGPERSKICH (LIMITOWANE!)
        # Sortujemy kolejkę wg priorytetu (malejąco)
        sniper_queue.sort(key=lambda x: x['priority'], reverse=True)
        
        # Bierzemy tylko top N z kolejki
        targets = sniper_queue[:MAX_SNIPES_PER_CYCLE]
        
        if targets:
            logger.info(f"Faza 5: Wybrano {len(targets)} celów do aktualizacji Intraday (z {len(sniper_queue)} oczekujących).")

        for target_obj in targets:
            item = target_obj['item']
            ticker = item['ticker']
            
            try:
                # CIĘŻKIE ZAPYTANIE API (Intraday)
                raw_intraday = self.client.get_intraday(ticker, interval='5min', outputsize='compact')
                
                if raw_intraday and 'Time Series (5min)' in raw_intraday:
                    df = standardize_df_columns(pd.DataFrame.from_dict(raw_intraday['Time Series (5min)'], orient='index'))
                    df.index = pd.to_datetime(df.index)
                    df.sort_index(inplace=True)
                    
                    metrics = calculate_flux_vectors(df, current_ofp=item['ofp'])
                    
                    # Obliczenia SL/TP
                    price = item['price']
                    sl_price = price * (1 - DEFAULT_SL_PCT)
                    risk_usd = price - sl_price 
                    tp_price = price + (risk_usd * DEFAULT_RR)
                    
                    # Aktualizacja stanu
                    item['elasticity'] = float(metrics.get('elasticity', 0.0))
                    item['velocity'] = float(metrics.get('velocity', 0.0))
                    item['flux_score'] = int(metrics.get('flux_score', 0))
                    item['last_sniper_check'] = time.time() 
                    item['stop_loss'] = sl_price             
                    item['take_profit'] = tp_price           
                    item['risk_reward'] = DEFAULT_RR         
                    
                    # Generowanie sygnału
                    if item['flux_score'] >= FLUX_THRESHOLD_ENTRY:
                        # Sprawdzenie makro (bezpieczne pobranie)
                        bias = self.macro_context.get('bias', 'NEUTRAL')
                        if bias != 'BEARISH':
                            metrics['price'] = item['price']
                            if self._generate_signal(ticker, metrics, sl_price, tp_price):
                                signals_generated += 1
                
            except Exception as e:
                logger.error(f"Faza 5 Sniper Error ({ticker}): {e}")

        # 5. ROTACJA (Stygnące spółki)
        for item in self.active_pool:
            # Bezpieczne pobranie wartości
            flx = item.get('flux_score') or 0
            added = item.get('added_at') or 0
            
            # Jeśli Flux Score < 30 i siedzimy w puli > 25 min -> Wylot
            if self.reserve_pool and flx < 30 and (time.time() - added) > 1500:
                if item['ticker'] not in tickers_to_remove:
                    tickers_to_remove.append(item['ticker'])

        # 6. Zarządzanie Pulą
        if tickers_to_remove:
            self._rotate_pool(tickers_to_remove)
            if signals_generated > 0:
                append_scan_log(self.session, f"🌊 Faza 5: Wygenerowano {signals_generated} sygnałów.")

        # 7. Zapis Stanu
        self._save_state()
        
        # Pacing pętli
        time.sleep(RADAR_DELAY)

    def _rotate_pool(self, remove_list):
        """Usuwa zużyte tickery i dobiera nowe z rezerwy."""
        remove_list = list(set(remove_list))
        self.active_pool = [x for x in self.active_pool if x['ticker'] not in remove_list]
        
        while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
            new_ticker = self.reserve_pool.pop(0)
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
        """Generuje sygnał w bazie (Upsert)."""
        try:
            price = metrics.get('price', 0)
            if price == 0: return False

            reason = metrics.get('signal_type', 'FLUX')
            score = int(metrics.get('flux_score', 0))
            ofp_val = metrics.get('ofp', 0.0)
            
            note = f"STRATEGIA: FLUX V5.3\nTYP: {reason} | SCORE: {score}/100\nOFP: {ofp_val:.2f} (Presja)\nELASTICITY: {metrics.get('elasticity', 0):.2f}σ"

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
