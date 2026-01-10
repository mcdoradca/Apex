import logging
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional

# Importy z systemu Apex
from ..models import SdarCandidate, TradingSignal
from .utils import get_raw_data_with_cache, standardize_df_columns
# === Moduł Taktyczny ===
from .phase_tactical import TacticalBridge

logger = logging.getLogger(__name__)

# === KONFIGURACJA SDAR ===
SDAR_RAW_INTERVAL = '5min'      
SDAR_VIRTUAL_TIMEFRAME = '4h'   
SDAR_MIN_CANDLES = 20           
BATCH_DELAY = 1.0               

class SDARAnalyzer:
    """
    Silnik Analityczny SDAR (System Detekcji Anomalii Rynkowych).
    Wersja 2.1 (Adaptive Scoring + Dynamic TTL).
    """

    def __init__(self, session: Session, api_client):
        self.session = session
        self.client = api_client
        self.tactical = TacticalBridge() 

    def run_sdar_cycle(self, limit: Optional[int] = None) -> List[str]:
        limit_str = f"{limit}" if limit else "ALL"
        logger.info(f"🚀 SDAR v2.1: Start analizy (Limit: {limit_str}, Adaptive Logic: ON)")

        candidates = self._fetch_candidates(limit)
        if not candidates:
            logger.warning("SDAR: Brak kandydatów spełniających wymogi płynności.")
            return []

        logger.info(f"SDAR: Znaleziono {len(candidates)} kandydatów do analizy.")
        processed_tickers = []

        for i, ticker in enumerate(candidates):
            try:
                # 0. Risk Guard: Earnings Filter
                if self._is_near_earnings(ticker):
                    continue # Cicho pomijamy

                time.sleep(BATCH_DELAY)
                result = self.analyze_ticker(ticker)
                
                if result:
                    self._save_result(result)
                    
                    # === MOST EGZEKUCYJNY (Decyzja o Setupie) ===
                    self._bridge_to_execution(result)
                    
                    processed_tickers.append(ticker)
                    
                    # Logujemy tylko istotne wyniki (>40), żeby nie śmiecić
                    if result.total_anomaly_score > 40:
                        logger.info(f"✅ SDAR [{i+1}/{len(candidates)}]: {ticker} | Score: {result.total_anomaly_score:.1f} | Action: {result.tactical_action}")

            except Exception as e:
                logger.error(f"❌ SDAR Error dla {ticker}: {str(e)}", exc_info=True)
                self.session.rollback()

        return processed_tickers

    def analyze_ticker(self, ticker: str) -> Optional[SdarCandidate]:
        # A. Pobranie danych
        df_5min, df_virtual = self._get_market_data(ticker)
        
        if df_virtual is None or len(df_virtual) < 10:
            return None

        # B. Dane News
        news_data = self._get_news_data(ticker)

        # C. Obliczenia Filarów
        sai = self._calculate_sai(df_virtual, df_5min) 
        spd = self._calculate_spd(df_virtual, news_data)
        me  = self._calculate_me(df_virtual, news_data)

        # === D. SCORING ADAPTACYJNY ("Silent Assassin") ===
        sai_val = float(sai['score'])
        spd_val = float(spd['score'])
        me_val = float(me['score'])
        
        # 1. TRYB CICHY (Silent Mode)
        # Jeśli jest duża akumulacja (SAI >= 60) ale cisza w newsach (SPD < 30)
        # Nagradzamy "ciszę" zamiast karać za brak newsów.
        if sai_val >= 60 and spd_val < 30:
            raw_total_score = (sai_val * 0.8) + (me_val * 0.2)
            # Bonus za ciszę absolutną (SPD bliskie 0)
            if spd_val < 10: raw_total_score += 10
        
        # 2. TRYB GŁOŚNY (Loud Mode)
        # Jeśli jest potężny news (SPD >= 70), ignorujemy brak techniki
        elif spd_val >= 70:
            raw_total_score = (spd_val * 0.7) + (sai_val * 0.2) + (me_val * 0.1)
            
        # 3. TRYB HYBRYDOWY (Standard)
        else:
            raw_total_score = (sai_val * 0.4) + (spd_val * 0.4) + (me_val * 0.2)
        
        # Kara za pułapkę (Bull Trap)
        if me['is_trap']:
            raw_total_score *= 0.5

        # === E. MODUŁ TAKTYCZNY + TTL ===
        current_price = df_5min['close'].iloc[-1]
        
        plan = self.tactical.generate_plan(
            ticker, float(current_price), df_5min,
            sai_val, spd_val, me_val
        )

        t_action = "WAIT"
        t_entry, t_sl, t_tp, t_rr = None, None, None, None
        t_comm = None
        
        # Jeśli plan istnieje, mapujemy go
        if plan:
            t_action = plan.action
            t_entry = plan.entry_price
            t_sl = plan.stop_loss
            t_tp = plan.take_profit
            t_rr = plan.risk_reward
            # Dodajemy TTL do komentarza dla informacji w tabeli
            t_comm = f"{plan.comment} (TTL: {plan.ttl_days}d)"

        # Tworzymy obiekt kandydata
        candidate = SdarCandidate(
            ticker=ticker,
            sai_score=sai_val,
            spd_score=spd_val,
            me_score=me_val,
            total_anomaly_score=float(raw_total_score),
            atr_compression=float(sai['atr_compression']),
            obv_slope=float(sai['obv_slope']),
            price_stability=float(sai['price_stability']),
            smart_money_flow=float(sai['smart_money_flow']), 
            sentiment_shock=float(spd['sentiment_shock']),
            news_volume_spike=float(spd['news_count']),
            price_resilience=float(spd['resilience_score']),
            last_sentiment_score=float(spd['last_sentiment']),
            metric_rsi=float(me['rsi']),
            metric_apo=float(me['apo']),
            
            tactical_action=t_action,
            entry_price=t_entry,
            stop_loss=t_sl,
            take_profit=t_tp,
            risk_reward_ratio=t_rr,
            tactical_comment=t_comm,
            analysis_date=datetime.now()
        )
        
        # Hack: Doklejamy obiekt planu do kandydata w pamięci (nie do bazy),
        # aby przekazać `ttl_days` do funkcji `_bridge_to_execution`.
        candidate._plan_object = plan 
        
        return candidate

    # --- EXECUTION BRIDGE (FILTR JAKOŚCI) ---
    
    def _bridge_to_execution(self, candidate: SdarCandidate):
        """
        Zamienia wynik analizy na sygnał, JEŚLI spełnia ostre kryteria.
        """
        # 1. Czy jest akcja?
        if not candidate.tactical_action or candidate.tactical_action in ['WAIT', 'SKIP', 'OBSERVE']:
            return

        # 2. FILTR JAKOŚCI (Threshold)
        # Odrzucamy wszystko z wynikiem < 45. To wytnie słabe setupy (np. ZYME).
        if candidate.total_anomaly_score < 45:
            return

        # 3. Filtr R:R (już sprawdzony w module taktycznym, ale dla pewności)
        if not candidate.risk_reward_ratio or candidate.risk_reward_ratio < 2.0:
            return

        # 4. Sprawdzenie duplikatów
        existing_signal = self.session.query(TradingSignal).filter(
            TradingSignal.ticker == candidate.ticker,
            TradingSignal.status.in_(['PENDING', 'ACTIVE'])
        ).first()

        if existing_signal: return 

        # 5. Wyliczenie daty wygaśnięcia (Dynamic TTL)
        expiration_dt = None
        if hasattr(candidate, '_plan_object') and candidate._plan_object:
            # Używamy TTL wyliczonego z fizyki rynku (ATR)
            ttl_days = candidate._plan_object.ttl_days
            expiration_dt = datetime.now(timezone.utc) + timedelta(days=ttl_days)
        else:
            # Fallback
            expiration_dt = datetime.now(timezone.utc) + timedelta(days=5)

        try:
            new_signal = TradingSignal(
                ticker=candidate.ticker,
                status='PENDING', 
                entry_price=candidate.entry_price,
                stop_loss=candidate.stop_loss,
                take_profit=candidate.take_profit,
                risk_reward_ratio=candidate.risk_reward_ratio,
                notes=f"SDAR v2 {candidate.tactical_action}: {candidate.tactical_comment} | Score:{candidate.total_anomaly_score:.0f}",
                entry_zone_bottom=candidate.entry_price, 
                entry_zone_top=candidate.entry_price,
                generation_date=datetime.now(timezone.utc),
                expiration_date=expiration_dt # <--- ZAPIS DYNAMICZNEGO TTL
            )
            
            self.session.add(new_signal)
            self.session.commit()
            logger.info(f"⚡ BRIDGE: Utworzono Sygnał {candidate.ticker} (R:R {candidate.risk_reward_ratio:.1f}, Ważny do: {expiration_dt.strftime('%Y-%m-%d')})")
            
        except Exception as e:
            logger.error(f"BRIDGE Error saving signal for {candidate.ticker}: {e}")
            self.session.rollback()

    # --- FILARY (STANDARDOWE METODY) ---
    
    def _calculate_sai(self, df_virtual: pd.DataFrame, df_5min: pd.DataFrame) -> Dict:
        df = df_virtual.copy()
        df['price_change'] = df['close'].diff()
        df['obv_dir'] = np.where(df['price_change'] > 0, 1, -1)
        df['obv_dir'] = np.where(df['price_change'] == 0, 0, df['obv_dir'])
        df['obv'] = (df['volume'] * df['obv_dir']).cumsum().fillna(0)
        
        df_5min['cum_pv'] = (df_5min['close'] * df_5min['volume']).cumsum()
        df_5min['cum_vol'] = df_5min['volume'].cumsum()
        df_5min['vwap_precise'] = df_5min['cum_pv'] / df_5min['cum_vol']
        current_vwap = df_5min['vwap_precise'].iloc[-1] if not df_5min.empty else 0

        df['tr'] = np.maximum(df['high'] - df['low'], 
                   np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
        df['atr'] = df['tr'].rolling(window=14).mean()

        subset = df.iloc[-10:].copy()
        if len(subset) < 5: 
            return {'score':0, 'atr_compression':0, 'obv_slope':0, 'price_stability':0, 'smart_money_flow':0}
        
        x = np.arange(len(subset))
        try:
            slope_price = np.polyfit(x, subset['close'], 1)[0] / subset['close'].mean() * 100
            slope_obv = np.polyfit(x, subset['obv'], 1)[0] / (subset['volume'].mean() + 1)
            slope_atr = np.polyfit(x, subset['atr'].fillna(0), 1)[0]
        except Exception:
            slope_price, slope_obv, slope_atr = 0.0, 0.0, 0.0
        
        last_price = subset['close'].iloc[-1]
        vwap_support = 1 if last_price > current_vwap else 0

        score = 0
        if slope_obv > 0.1: 
            if abs(slope_price) < 0.5: score += 40 
            elif slope_price < 0: score += 50      
        if slope_atr < 0: score += 20
        if vwap_support: score += 10

        return {
            'score': float(min(score, 100)),
            'atr_compression': float(slope_atr),
            'obv_slope': float(slope_obv),
            'price_stability': float(1.0 / (abs(slope_price) + 0.01)),
            'smart_money_flow': float(current_vwap)
        }

    def _calculate_spd(self, df: pd.DataFrame, news_data: List[Dict]) -> Dict:
        if not news_data or df.empty:
            return {'score':0, 'sentiment_shock':0, 'news_count':0, 'resilience_score':0, 'last_sentiment':0}

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=48)
        
        valid_news = []
        for n in news_data:
            try:
                pub_date_str = n.get('time_published')
                pub_date = datetime.strptime(pub_date_str, '%Y%m%dT%H%M%S').replace(tzinfo=timezone.utc)
                if pub_date >= cutoff:
                    valid_news.append(n)
            except: pass
            
        sorted_news = sorted(valid_news, key=lambda x: x.get('time_published', ''))
        if len(sorted_news) < 2:
             return {'score':0, 'sentiment_shock':0, 'news_count':len(sorted_news), 'resilience_score':0, 'last_sentiment':0}

        sentiments = []
        for n in sorted_news:
            try: sentiments.append(float(n.get('overall_sentiment_score', 0)))
            except: pass
            
        last_sent = sentiments[-1] if sentiments else 0.0
        prev_avg = np.mean(sentiments[:-1]) if len(sentiments) > 1 else 0.0
        sentiment_shock = last_sent - prev_avg

        lookback = min(6, len(df))
        price_change_24h = (df['close'].iloc[-1] / df['close'].iloc[-lookback] - 1) if lookback > 0 else 0.0
        
        resilience_score = 0
        if sentiment_shock < -0.15: 
            if price_change_24h > -0.01:
                resilience_score = 100 
                
        return {
            'score': float(80 if resilience_score > 0 else 0),
            'sentiment_shock': float(sentiment_shock),
            'news_count': float(len(valid_news)),
            'resilience_score': float(resilience_score),
            'last_sentiment': float(last_sent)
        }

    def _calculate_me(self, df: pd.DataFrame, news_data: List[Dict]) -> Dict:
        if len(df) < 30:
             return {'score':50, 'is_trap':False, 'rsi':0, 'apo':0}

        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / (loss + 0.00001)
        df['rsi'] = 100 - (100 / (1 + rs))
        
        df['apo'] = df['close'].rolling(window=12).mean() - df['close'].rolling(window=26).mean()
        
        current_rsi = df['rsi'].iloc[-1]
        current_apo = df['apo'].iloc[-1]
        prev_apo = df['apo'].iloc[-2]
        price_new_high = df['close'].iloc[-1] > df['close'].iloc[-5:].max()
        
        is_trap = False
        me_score = 50 
        if price_new_high:
            if current_apo < prev_apo and current_rsi > 70:
                is_trap = True
                me_score = 0
        elif current_apo > prev_apo and current_apo > 0:
            me_score = 80

        return {'score': float(me_score), 'is_trap': is_trap, 'rsi': float(current_rsi), 'apo': float(current_apo)}

    def _get_market_data(self, ticker: str):
        raw_data = get_raw_data_with_cache(
            self.session, self.client, ticker,
            'INTRADAY_5',
            lambda t: self.client.get_intraday(t, interval='5min', outputsize='full'),
            expiry_hours=1
        )
        if not raw_data or 'Time Series (5min)' not in raw_data:
            return None, None
            
        df_5min = pd.DataFrame.from_dict(raw_data['Time Series (5min)'], orient='index')
        df_5min = standardize_df_columns(df_5min)
        df_5min.index = pd.to_datetime(df_5min.index)
        df_5min.sort_index(inplace=True)
        
        for c in ['open', 'high', 'low', 'close', 'volume']:
            df_5min[c] = pd.to_numeric(df_5min[c], errors='coerce')
        df_5min.dropna(inplace=True)

        df_virtual = df_5min.resample(SDAR_VIRTUAL_TIMEFRAME, label='right', closed='right').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()

        return df_5min, df_virtual

    def _get_news_data(self, ticker: str) -> List[Dict]:
        try:
            resp = self.client.get_news_sentiment(ticker, limit=50)
            if resp and 'feed' in resp: return resp['feed']
        except: pass
        return []
    
    def _fetch_candidates(self, limit: Optional[int]) -> List[str]:
        base_query = "SELECT ticker FROM phase1_candidates WHERE volume > 100000 ORDER BY score DESC"
        if limit: query = text(f"{base_query} LIMIT {limit}")
        else: query = text(base_query)
        result = self.session.execute(query).fetchall()
        return [r[0] for r in result]

    def _is_near_earnings(self, ticker: str) -> bool:
        try:
            query = text("SELECT days_to_earnings FROM phase1_candidates WHERE ticker=:t")
            res = self.session.execute(query, {'t': ticker}).first()
            if res and res[0] is not None:
                if -2 <= res[0] <= 1: return True
            return False
        except: return False

    def _save_result(self, result: SdarCandidate):
        self.session.merge(result)
        self.session.commit()
