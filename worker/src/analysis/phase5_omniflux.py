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

# === KONFIGURACJA OMNI-FLUX (V5.2 - SL/TP W KAFLU) ===
CAROUSEL_SIZE = 8          # Rozmiar aktywnej puli
RADAR_DELAY = 2.0          # Co ile sekund skanujemy wszystkich (Bulk)
SNIPER_COOLDOWN = 60       # Co ile sekund wymuszamy odświeżenie Intraday (VWAP)
FLUX_THRESHOLD_ENTRY = 70  # Min. score do sygnału
MACRO_CACHE_DURATION = 300 
DEFAULT_RR = 2.5           # Domyślny stosunek Risk:Reward dla Flux
DEFAULT_SL_PCT = 0.015     # Domyślny SL 1.5% ceny dla Intraday

class OmniFluxAnalyzer:
    """
    APEX OMNI-FLUX ENGINE (V5.2)
    Architektura: Radar (Bulk) + Sniper (Intraday on Trigger)
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
        """Odtwarza stan z bazy."""
        try:
            raw_json = get_system_control_value(self.session, 'phase5_monitor_state')
            if raw_json:
                state = json.loads(raw_json)
                self.active_pool = state.get('active_pool', [])
                self.reserve_pool = state.get('reserve_pool', [])
                self.macro_context['bias'] = state.get('macro_bias', 'NEUTRAL')
                
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
        if now - self.macro_context['last_updated'] < MACRO_CACHE_DURATION:
            return

        try:
            # === POPRAWKA: Usunięto parametr 'market', zmieniono symbol na 'FXE' (Euro ETF) ===
            # Alpha Vantage Intraday (Stocks) nie obsługuje par walutowych bezpośrednio ani parametru 'market'.
            # FXE (Invesco CurrencyShares Euro Trust) jest proxy dla EUR/USD dostępnym w tym endpoincie.
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
                    'last_sniper_check': 0, # Timestamp ostatniego pełnego skanu intraday
                    'price': 0.0, 
                    'elasticity': 0.0, 
                    'velocity': 0.0, 
                    'flux_score': 0,
                    'ofp': 0.0,
                    'stop_loss': None,      # NOWOŚĆ
                    'take_profit': None,    # NOWOŚĆ
                    'risk_reward': None     # NOWOŚĆ
                })
            
            msg = f"🌊 Faza 5 (Radar & Sniper): Inicjalizacja. Aktywne: {len(self.active_pool)}"
            logger.info(msg)
            append_scan_log(self.session, msg)
            self._save_state()

        except Exception as e:
            logger.error(f"Faza 5: Błąd inicjalizacji: {e}")

    def run_cycle(self):
        """
        GŁÓWNA PĘTLA HYBRYDOWA.
        1. Radar: Pobiera Bulk Quotes dla wszystkich.
        2. Analiza: Liczy OFP i sprawdza triggery.
        3. Sniper: Doczytuje Intraday tylko dla wybranych.
        """
        # 1. Inicjalizacja
        if not self.active_pool:
            self._initialize_pools()
            if not self.active_pool: return

        self._refresh_macro_context()
        
        # 2. RADAR SCAN (1 API Call dla wszystkich)
        # Przygotuj listę tickerów
        tickers = [item['ticker'] for item in self.active_pool]
        radar_hits = self.client.get_bulk_quotes_parsed(tickers) 
        
        # Mapa wyników: {ticker: data}
        radar_map = {d['symbol']: d for d in radar_hits}
        
        tickers_to_remove = []
        signals_generated = 0
        
        # 3. ITERACJA PO SPÓŁKACH
        for item in self.active_pool:
            ticker = item['ticker']
            radar_data = radar_map.get(ticker)
            
            should_snipe = False
            
            if radar_data:
                # A. Aktualizacja danych z Radaru (Lekkie)
                new_price = radar_data.get('price', 0.0)
                bid_sz = radar_data.get('bid_size', 0.0)
                ask_sz = radar_data.get('ask_size', 0.0)
                
                # Oblicz OFP (Order Flow Pressure)
                ofp = calculate_ofp(bid_sz, ask_sz)
                
                # Sprawdź zmianę ceny od ostatniego zapisanego stanu
                old_price = item.get('price', 0.0)
                price_change_pct = abs((new_price - old_price) / old_price) if old_price > 0 else 0
                
                # Aktualizuj stan lokalny (dla UI)
                item['price'] = new_price
                item['ofp'] = ofp
                item['fails'] = 0 
                
                # B. SNIPER TRIGGERS (Czy strzelać Intraday?)
                # 1. Inicjalizacja: Brak Elasticity (pierwszy raz)
                if item.get('elasticity') == 0: 
                    should_snipe = True
                
                # 2. Pressure Trigger: Silne OFP sugeruje ruch
                elif abs(ofp) > 0.4: 
                    should_snipe = True
                
                # 3. Volatility Trigger: Cena ruszyła się > 0.1%
                elif price_change_pct > 0.001: 
                    should_snipe = True
                
                # 4. Score Trigger: Jeśli setup był blisko (Score > 60), sprawdzaj częściej
                elif item.get('flux_score', 0) > 60:
                    should_snipe = True
                    
                # 5. Stale Data: Odśwież VWAP co minutę (nawet jak stoi)
                elif (time.time() - item.get('last_sniper_check', 0)) > SNIPER_COOLDOWN:
                    should_snipe = True
            
            else:
                # Brak danych w Radarze (błąd API lub ticker)
                item['fails'] += 1
                if item['fails'] >= 3: tickers_to_remove.append(ticker)
                continue

            # C. SNIPER EXECUTION (Ciężkie zapytanie)
            if should_snipe:
                try:
                    # Pobierz pełne świece (kosztuje 1 API call)
                    raw_intraday = self.client.get_intraday(ticker, interval='5min', outputsize='compact')
                    
                    if raw_intraday and 'Time Series (5min)' in raw_intraday:
                        df = standardize_df_columns(pd.DataFrame.from_dict(raw_intraday['Time Series (5min)'], orient='index'))
                        df.index = pd.to_datetime(df.index)
                        df.sort_index(inplace=True)
                        
                        # Przekazujemy OFP do fizyki
                        metrics = calculate_flux_vectors(df, current_ofp=item['ofp'])
                        
                        # DYNAMICZNE OBLICZENIE SL/TP
                        price = item['price']
                        # SL na podstawie % ceny (np. 1.5%)
                        sl_price = price * (1 - DEFAULT_SL_PCT)
                        # Ryzyko w USD
                        risk_usd = price - sl_price 
                        # TP na podstawie RR
                        tp_price = price + (risk_usd * DEFAULT_RR)
                        
                        # Aktualizacja pełnego stanu
                        item['elasticity'] = float(metrics.get('elasticity', 0.0))
                        item['velocity'] = float(metrics.get('velocity', 0.0))
                        item['flux_score'] = int(metrics.get('flux_score', 0))
                        item['last_sniper_check'] = time.time() # Reset licznika snipera
                        item['stop_loss'] = sl_price             # NOWOŚĆ
                        item['take_profit'] = tp_price           # NOWOŚĆ
                        item['risk_reward'] = DEFAULT_RR         # NOWOŚĆ
                        
                        # D. SYGNAŁY
                        # Generujemy sygnał w bazie *tylko*, jeśli jest akcja i spełnione warunki makro
                        if item['flux_score'] >= FLUX_THRESHOLD_ENTRY:
                            if self.macro_context['bias'] != 'BEARISH':
                                metrics['price'] = item['price']
                                # Zrezygnujemy z usuwania z puli tutaj,
                                # aby kafel był widoczny jako aktywny dopóki jest w puli.
                                if self._generate_signal(ticker, metrics, sl_price, tp_price):
                                    signals_generated += 1
                                    # NIE USUWAJ STĄD: tickers_to_remove.append(ticker)
                    
                except Exception as e:
                    logger.error(f"Faza 5 Sniper ({ticker}): {e}")
                    # Nie usuwamy od razu, może to chwilowy błąd
            
            # E. ROTACJA (Stygnące spółki)
            # Jeśli Flux Score < 30 i siedzimy w puli > 20 min -> Wylot
            if self.reserve_pool and item.get('flux_score', 0) < 30 and (time.time() - item.get('added_at', 0)) > 1200:
                tickers_to_remove.append(ticker)

        # 4. Zarządzanie Pulą
        if tickers_to_remove:
            self._rotate_pool(tickers_to_remove)
            if signals_generated > 0:
                append_scan_log(self.session, f"🌊 Faza 5: Wygenerowano {signals_generated} sygnałów.")

        # 5. Zapis Stanu (dla UI)
        self._save_state()
        
        # Pacing pętli (dla Radaru)
        time.sleep(RADAR_DELAY)

    def _rotate_pool(self, remove_list):
        """Usuwa zużyte tickery i dobiera nowe z rezerwy."""
        # Usuń duplikaty
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

    # === ZMIENIONA FUNKCJA GENEROWANIA SYGNAŁU ===
    def _generate_signal(self, ticker: str, metrics: dict, sl_price: float, tp_price: float) -> bool:
        """
        Generuje sygnał w bazie. ZAUWAŻ: Nie usuwamy już stąd sygnałów FLUX,
        będą one aktywne dopóki nie zostaną zamknięte przez Strażnika lub ręcznie.
        """
        try:
            price = metrics.get('price', 0)
            if price == 0: return False

            reason = metrics.get('signal_type', 'FLUX')
            score = int(metrics.get('flux_score', 0))
            ofp_val = metrics.get('ofp', 0.0)
            
            note = f"STRATEGIA: FLUX V5.2\nTYP: {reason} | SCORE: {score}/100\nOFP: {ofp_val:.2f} (Presja)\nELASTICITY: {metrics.get('elasticity', 0):.2f}σ"

            # Sprawdzamy czy sygnał FLUX już istnieje (żeby nie dodawać duplikatów)
            exists = self.session.execute(
                text("""
                    SELECT 1 FROM trading_signals 
                    WHERE ticker=:t 
                    AND status IN ('ACTIVE', 'PENDING') 
                    AND notes LIKE '%STRATEGIA: FLUX%'
                """), 
                {'t': ticker}
            ).fetchone()
            
            if exists: 
                # Jeśli sygnał FLUX już istnieje, aktualizujemy go, zamiast tworzyć nowy!
                update_stmt = text("""
                    UPDATE trading_signals SET
                        updated_at = NOW(),
                        notes = :note,
                        entry_price = :entry,
                        stop_loss = :sl,
                        take_profit = :tp
                    WHERE ticker = :ticker 
                    AND status IN ('ACTIVE', 'PENDING') 
                    AND notes LIKE '%STRATEGIA: FLUX%'
                """)
                self.session.execute(update_stmt, {
                    'ticker': ticker, 
                    'entry': price, 
                    'sl': sl_price, 
                    'tp': tp_price, 
                    'note': note
                })
                self.session.commit()
                return True
            
            # Jeśli nie istnieje, tworzymy nowy
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
    # Wrapper dla workera
    analyzer = OmniFluxAnalyzer(session, api_client)
    analyzer.run_cycle()
