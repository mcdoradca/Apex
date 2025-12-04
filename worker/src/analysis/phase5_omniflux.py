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
    update_system_control # Dodano do zapisu stanu dla UI
)
from .flux_physics import calculate_flux_vectors

logger = logging.getLogger(__name__)

# === KONFIGURACJA OMNI-FLUX (V5) ===
# Dostosowana do limitu Premium (150 req/min) z bezpiecznym buforem
CAROUSEL_SIZE = 8          # Liczba spółek w aktywnej rotacji
CYCLE_DELAY = 0.5          # Opóźnienie między zapytaniami w batchu (sztuczne dławienie)
FLUX_THRESHOLD_ENTRY = 70  # Min. punktacja do wejścia
MACRO_CACHE_DURATION = 300 # 5 minut cache dla makro

class OmniFluxAnalyzer:
    """
    APEX OMNI-FLUX ENGINE (Phase 5)
    
    Architektura: Active Pool Rotation (Karuzela).
    Cel: Skalpowanie Intraday na podstawie przepływu (Flux) ceny, wolumenu i sentymentu.
    """

    def __init__(self, session: Session, api_client: AlphaVantageClient):
        self.session = session
        self.client = api_client
        self.active_pool = []       # Lista słowników: {'ticker': 'AAPL', 'fails': 0, 'added_at': timestamp, 'metrics': {...}}
        self.reserve_pool = []      # Kolejka tickerów do wejścia
        self.macro_context = {
            'bias': 'NEUTRAL',
            'last_updated': 0
        }
        
    def _refresh_macro_context(self):
        """Sprawdza ogólny sentyment walutowy (Dolar) raz na 5 minut."""
        now = time.time()
        if now - self.macro_context['last_updated'] < MACRO_CACHE_DURATION:
            return

        try:
            # Sprawdzamy EUR/USD jako proxy dla siły dolara (Risk-On/Off)
            fx_data = self.client.get_intraday(symbol='EUR', market='USD', interval='60min', outputsize='compact')
            
            if fx_data and 'Time Series (60min)' in fx_data:
                df_fx = standardize_df_columns(pd.DataFrame.from_dict(fx_data.get('Time Series (60min)', {}), orient='index'))
                if not df_fx.empty and len(df_fx) > 1:
                    df_fx = df_fx.sort_index()
                    last_close = df_fx['close'].iloc[-1]
                    prev_close = df_fx['close'].iloc[-2]
                    
                    # Jeśli EUR rośnie -> Dolar słabnie -> Risk ON dla Stocks
                    if last_close > prev_close: 
                        self.macro_context['bias'] = 'BULLISH'
                    else:
                        self.macro_context['bias'] = 'BEARISH'
            
            self.macro_context['last_updated'] = now
            # logger.info(f"Faza 5: Makro Bias zaktualizowany -> {self.macro_context['bias']}")

        except Exception as e:
            logger.warning(f"Faza 5: Błąd odświeżania makro: {e}")

    def _initialize_pools(self):
        """
        Ładuje kandydatów z Fazy 1 (Trend) i Fazy X (Zmienność) do rezerwy.
        """
        try:
            # 1. Pobierz Top 40 z Fazy 1 (Najlepszy Trend Sektorowy)
            p1_rows = self.session.execute(text(
                "SELECT ticker FROM phase1_candidates ORDER BY sector_trend_score DESC NULLS LAST LIMIT 40"
            )).fetchall()
            
            # 2. Pobierz Top 20 z Fazy X (Największe Pompy - Zmienność)
            px_rows = self.session.execute(text(
                "SELECT ticker FROM phasex_candidates ORDER BY last_pump_percent DESC NULLS LAST LIMIT 20"
            )).fetchall()
            
            # 3. Połącz i usuń duplikaty
            combined_tickers = list(set([r[0] for r in p1_rows] + [r[0] for r in px_rows]))
            
            # 4. Filtruj: Usuń te, które już mamy w portfelu lub aktywnych sygnałach
            active_sigs = [r[0] for r in self.session.execute(text("SELECT ticker FROM trading_signals WHERE status IN ('ACTIVE', 'PENDING')")).fetchall()]
            holdings = [r[0] for r in self.session.execute(text("SELECT ticker FROM portfolio_holdings")).fetchall()]
            
            exclude_set = set(active_sigs + holdings)
            self.reserve_pool = [t for t in combined_tickers if t not in exclude_set]
            
            # 5. Napełnij aktywną pulę do pełna (8 sztuk)
            while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
                ticker = self.reserve_pool.pop(0)
                # Inicjalizacja pustych metryk dla UI
                self.active_pool.append({
                    'ticker': ticker, 
                    'fails': 0, 
                    'added_at': time.time(),
                    'price': 0.0,
                    'elasticity': 0.0,
                    'velocity': 0.0,
                    'flux_score': 0
                })
                
            msg = f"🌊 Faza 5 (Omni-Flux): Zainicjowano pulę. Aktywne: {len(self.active_pool)}, Rezerwa: {len(self.reserve_pool)}"
            logger.info(msg)
            append_scan_log(self.session, msg)

        except Exception as e:
            logger.error(f"Faza 5: Błąd inicjalizacji puli: {e}")

    def run_cycle(self):
        """
        Wykonuje JEDEN cykl skanowania aktywnej puli.
        Zaprojektowane do wywoływania w pętli głównej workera.
        """
        # Jeśli pusto, spróbuj zainicjować
        if not self.active_pool:
            self._initialize_pools()
            if not self.active_pool:
                # Jeśli nadal pusto, to znaczy że nie ma kandydatów w bazie
                # Spróbuj zrzucić pusty stan do UI, żeby nie wisiało "Ładowanie..."
                self._save_state()
                return

        self._refresh_macro_context()
        
        tickers_to_remove = []
        signals_generated = 0

        # Iteracja po "Karuzeli"
        for item in self.active_pool:
            ticker = item['ticker']
            
            # Sztuczne opóźnienie dla bezpieczeństwa API (pacing)
            time.sleep(CYCLE_DELAY)
            
            try:
                # 1. Pobierz Intraday 5min (PREMIUM - Full history dla precyzji VWAP)
                data = self.client.get_intraday(symbol=ticker, interval='5min', outputsize='full')
                
                if not data or 'Time Series (5min)' not in data:
                    item['fails'] += 1
                    # Jeśli 2 razy pod rząd błąd danych -> wylot
                    if item['fails'] >= 2: 
                        tickers_to_remove.append(ticker)
                    continue
                
                # Reset licznika błędów po sukcesie
                item['fails'] = 0

                # 2. Przetwórz dane do DataFrame
                df = standardize_df_columns(pd.DataFrame.from_dict(data['Time Series (5min)'], orient='index'))
                df.index = pd.to_datetime(df.index)
                df.sort_index(inplace=True) # Chronologicznie

                # 3. Oblicz Fizykę Flux (używając nowego modułu)
                metrics = calculate_flux_vectors(df)
                
                flux_score = metrics['flux_score']
                current_price = df['close'].iloc[-1] if not df.empty else 0.0
                
                # === AKTUALIZACJA STANU DLA UI ===
                item['price'] = float(current_price)
                item['elasticity'] = float(metrics.get('elasticity', 0.0))
                item['velocity'] = float(metrics.get('velocity', 0.0))
                item['flux_score'] = int(flux_score)
                # =================================
                
                # 4. Decyzja Strategiczna
                
                # WARUNEK WEJŚCIA (Signal)
                if flux_score >= FLUX_THRESHOLD_ENTRY:
                    # Dodatkowy filtr Makro (tylko dla Longów) - nie walcz z dolarem
                    if self.macro_context['bias'] != 'BEARISH':
                        metrics['price'] = current_price # Dodajemy cenę do metrics dla generatora sygnału
                        self._generate_signal(ticker, metrics)
                        signals_generated += 1
                        tickers_to_remove.append(ticker) # Sygnał wygenerowany -> zdejmij z karuzeli
                    else:
                        # Mamy sygnał techniczny, ale makro jest przeciwko -> trzymaj w puli, może makro się zmieni
                        pass

                # WARUNEK WYRZUCENIA (Dead Stock)
                # Jeśli Flux Score jest bardzo niski (<20), szkoda zapytań -> wymień na innego
                elif flux_score < 20:
                    tickers_to_remove.append(ticker)
                
                # WARUNEK STAGNACJI (Time-out)
                # Jeśli siedzi w karuzeli > 30 minut i nic nie robi -> wymień
                elif (time.time() - item['added_at']) > 1800:
                    tickers_to_remove.append(ticker)

            except Exception as e:
                logger.error(f"Faza 5: Błąd analizy {ticker}: {e}")
                item['fails'] += 1

        # === OBSŁUGA ROTACJI (KARUZELA) ===
        if tickers_to_remove:
            # 1. Usuń zużyte tickery z aktywnej puli
            original_len = len(self.active_pool)
            self.active_pool = [x for x in self.active_pool if x['ticker'] not in tickers_to_remove]
            removed_count = original_len - len(self.active_pool)
            
            # 2. Dobierz świeże z rezerwy
            added_count = 0
            while len(self.active_pool) < CAROUSEL_SIZE and self.reserve_pool:
                new_ticker = self.reserve_pool.pop(0)
                self.active_pool.append({
                    'ticker': new_ticker, 
                    'fails': 0, 
                    'added_at': time.time(),
                    'price': 0.0,
                    'elasticity': 0.0,
                    'velocity': 0.0,
                    'flux_score': 0
                })
                added_count += 1
            
            # 3. Recykling (Opcjonalnie): Te wyrzucone (jeśli nie sygnał) wracają na koniec kolejki
            # W tej wersji: po prostu odpadają, żeby dać szansę innym.
            
            if signals_generated > 0:
                msg = f"🌊 Faza 5: Rotacja. Wymieniono {removed_count} tickerów. Nowe sygnały: {signals_generated}."
                append_scan_log(self.session, msg)

        # Raportowanie postępu (symboliczne, bo to proces ciągły)
        update_scan_progress(self.session, len(self.reserve_pool), 100)
        
        # === ZAPIS STANU DLA UI ===
        self._save_state()

    def _save_state(self):
        """Serializuje stan monitora do bazy danych, aby Frontend mógł go wyświetlić."""
        try:
            state_data = {
                "active_pool": self.active_pool,
                "macro_bias": self.macro_context.get('bias', 'NEUTRAL'),
                "reserve_count": len(self.reserve_pool),
                "last_updated": time.time()
            }
            # Używamy system_control jako kanału komunikacji z UI
            update_system_control(self.session, 'phase5_monitor_state', json.dumps(state_data))
        except Exception as e:
            logger.error(f"Faza 5: Błąd zapisu stanu do UI: {e}")

    def _generate_signal(self, ticker: str, metrics: dict):
        """Generuje i zapisuje sygnał Flux."""
        try:
            price = metrics.get('price', 0)
            if price == 0: return

            # Dynamiczny SL/TP na podstawie zmienności (Elasticity jako proxy zmienności)
            # Jeśli Elasticity jest wysokie, zmienność jest duża -> szerszy SL
            elasticity_abs = abs(metrics.get('elasticity', 1.0))
            sl_pct = 0.01 + (elasticity_abs * 0.005) # Min 1%, max w górę
            
            sl_price = price * (1 - sl_pct)
            tp_price = price * (1 + (sl_pct * 2.0)) # RR = 2.0
            
            score = int(metrics.get('flux_score', 0))
            reason = metrics.get('signal_type', 'FLUX')
            
            note = (
                f"STRATEGIA: FLUX V5\n"
                f"TYP: {reason} | SCORE: {score}/100\n"
                f"ELASTICITY: {metrics.get('elasticity', 0):.2f} sigma\n"
                f"VELOCITY: {metrics.get('velocity', 0):.2f}x avg\n"
                f"VWAP GAP: {metrics.get('vwap_gap_percent', 0):.2f}%"
            )

            # Sprawdź czy już nie ma aktywnego
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
            
            self.session.execute(stmt, {
                'ticker': ticker,
                'entry': price, 'sl': sl_price, 'tp': tp_price,
                'note': note
            })
            self.session.commit()
            
            alert_msg = f"🌊 FLUX SYGNAŁ: {ticker}\nTyp: {reason}\nCena: {price:.2f}\nScore: {score}"
            logger.info(alert_msg)
            append_scan_log(self.session, alert_msg)
            send_telegram_alert(alert_msg)

        except Exception as e:
            logger.error(f"Faza 5: Błąd generowania sygnału dla {ticker}: {e}")
            self.session.rollback()

# Wrapper do łatwego wywołania z main.py
def run_phase5_cycle(session: Session, api_client: AlphaVantageClient):
    """Entry point dla Fazy 5."""
    analyzer = OmniFluxAnalyzer(session, api_client)
    # Uruchamiamy jeden pełny cykl (przejście po 8 spółkach)
    # Worker będzie to wołał w pętli while
    analyzer.run_cycle()
