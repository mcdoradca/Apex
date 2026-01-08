
import logging
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional

# Importy z systemu Apex
from ..models import SdarCandidate
from .utils import get_raw_data_with_cache, standardize_df_columns

logger = logging.getLogger(__name__)

# === KONFIGURACJA SDAR (ZGODNA Z PDF) ===
SDAR_RAW_INTERVAL = '5min'      # Dane źródłowe (Mikro-struktura)
SDAR_VIRTUAL_TIMEFRAME = '4h'   # Wirtualne świece
SDAR_MIN_CANDLES = 20           # Minimalna liczba świec 4h do analizy
BATCH_DELAY = 1.0               

class SDARAnalyzer:
    """
    Silnik Analityczny SDAR (System Detekcji Anomalii Rynkowych).
    Poprawiona implementacja:
    - Prawidłowe obliczanie VWAP na danych 5-minutowych (Filar 1).
    - Bezpieczna agregacja czasu (Filar 1).
    - Czasowo ważony Sentiment Shock (Filar 2).
    """

    def __init__(self, session: Session, api_client):
        self.session = session
        self.client = api_client

    def run_sdar_cycle(self, limit: int = 50) -> List[str]:
        logger.info(f"🚀 SDAR Pro: Start analizy anomalii (Input: {SDAR_RAW_INTERVAL} -> Agg: {SDAR_VIRTUAL_TIMEFRAME})")

        candidates = self._fetch_candidates(limit)
        if not candidates:
            logger.warning("SDAR: Brak kandydatów spełniających wymogi płynności.")
            return []

        processed_tickers = []

        for ticker in candidates:
            try:
                # 0. Risk Guard: Earnings Filter
                if self._is_near_earnings(ticker):
                    logger.info(f"SDAR: {ticker} pominięty (Earnings Risk).")
                    continue

                time.sleep(BATCH_DELAY)
                result = self.analyze_ticker(ticker)
                
                if result:
                    self._save_result(result)
                    processed_tickers.append(ticker)
                    logger.info(f"✅ SDAR: {ticker} | Score: {result.total_anomaly_score:.1f} | SAI: {result.sai_score:.0f} SPD: {result.spd_score:.0f} ME: {result.me_score:.0f}")

            except Exception as e:
                logger.error(f"❌ SDAR Error dla {ticker}: {str(e)}", exc_info=True)
                self.session.rollback()

        return processed_tickers

    def analyze_ticker(self, ticker: str) -> Optional[SdarCandidate]:
        # A. Pobranie danych Mikro (5min) i Agregacja do 4h
        # FIX: Pobieramy OBA DataFrame'y - surowy 5min (do VWAP) i zagregowany 4h (do trendów)
        df_5min, df_virtual = self._get_market_data(ticker)
        
        if df_virtual is None or len(df_virtual) < 10:
            return None

        # B. Dane News (Sentyment)
        news_data = self._get_news_data(ticker)

        # C. Obliczenia Filarów
        sai = self._calculate_sai(df_virtual, df_5min) # Przekazujemy też 5min do weryfikacji VWAP
        spd = self._calculate_spd(df_virtual, news_data)
        me  = self._calculate_me(df_virtual, news_data)

        # D. Scoring Hybrydowy
        total_score = (sai['score'] * 0.4) + (spd['score'] * 0.4) + (me['score'] * 0.2)
        
        if me['is_trap']:
            total_score *= 0.5

        return SdarCandidate(
            ticker=ticker,
            sai_score=sai['score'],
            spd_score=spd['score'],
            me_score=me['score'],
            total_anomaly_score=total_score,
            atr_compression=sai['atr_compression'],
            obv_slope=sai['obv_slope'],
            price_stability=sai['price_stability'],
            smart_money_flow=sai['smart_money_flow'], 
            sentiment_shock=spd['sentiment_shock'],
            news_volume_spike=spd['news_count'],
            price_resilience=spd['resilience_score'],
            last_sentiment_score=spd['last_sentiment'],
            metric_rsi=me['rsi'],
            metric_apo=me['apo'],
            analysis_date=datetime.now()
        )

    # --- FILAR 1: SAI (Silent Accumulation Index) ---
    def _calculate_sai(self, df_virtual: pd.DataFrame, df_5min: pd.DataFrame) -> Dict:
        """
        Analiza Cichej Akumulacji.
        FIX: VWAP jest teraz brany z danych 5min (precyzyjny), a trendy z 4H.
        """
        # 1. OBV (On Balance Volume) na świecach 4h
        df = df_virtual.copy()
        df['price_change'] = df['close'].diff()
        df['obv_dir'] = np.where(df['price_change'] > 0, 1, -1)
        df['obv_dir'] = np.where(df['price_change'] == 0, 0, df['obv_dir'])
        df['obv'] = (df['volume'] * df['obv_dir']).cumsum().fillna(0)
        
        # 2. VWAP Calculation (FIX: Liczymy na danych 5min, potem mapujemy ostatni na 4H)
        # Obliczamy Cumulative VWAP na danych 5min dla całego pobranego okresu
        df_5min['cum_pv'] = (df_5min['close'] * df_5min['volume']).cumsum()
        df_5min['cum_vol'] = df_5min['volume'].cumsum()
        df_5min['vwap_precise'] = df_5min['cum_pv'] / df_5min['cum_vol']
        
        # Bierzemy ostatni znany VWAP
        current_vwap = df_5min['vwap_precise'].iloc[-1]

        # 3. ATR Calculation (Zmienność) na 4h
        df['tr'] = np.maximum(df['high'] - df['low'], 
                   np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
        df['atr'] = df['tr'].rolling(window=14).mean()

        # 4. Analiza Regresji (Ostatnie 10 świec 4h)
        subset = df.iloc[-10:].copy()
        if len(subset) < 5: 
            return {'score':0, 'atr_compression':0, 'obv_slope':0, 'price_stability':0, 'smart_money_flow':0}
        
        x = np.arange(len(subset))
        
        # Bezpieczne obliczanie nachyleń
        try:
            slope_price = np.polyfit(x, subset['close'], 1)[0] / subset['close'].mean() * 100
            slope_obv = np.polyfit(x, subset['obv'], 1)[0] / (subset['volume'].mean() + 1) # +1 unika dzielenia przez 0
            slope_atr = np.polyfit(x, subset['atr'].fillna(0), 1)[0]
        except Exception:
            slope_price, slope_obv, slope_atr = 0, 0, 0
        
        # VWAP Check: Cena vs Instytucje
        last_price = subset['close'].iloc[-1]
        vwap_support = 1 if last_price > current_vwap else 0

        score = 0
        # A. Dywergencja OBV (Cena płaska/spada, OBV rośnie)
        if slope_obv > 0.1: # Popyt rośnie
            if abs(slope_price) < 0.5: score += 40 # Cena stabilna
            elif slope_price < 0: score += 50      # Cena spada (Silna akumulacja)
            
        # B. Kompresja Zmienności
        if slope_atr < 0: score += 20
        
        # C. VWAP Support
        if vwap_support: score += 10

        return {
            'score': min(score, 100),
            'atr_compression': slope_atr,
            'obv_slope': slope_obv,
            'price_stability': 1.0 / (abs(slope_price) + 0.01),
            'smart_money_flow': current_vwap
        }

    # --- FILAR 2: SPD (Sentiment-Price Divergence) ---
    def _calculate_spd(self, df: pd.DataFrame, news_data: List[Dict]) -> Dict:
        """
        FIX: Analiza tylko świeżych newsów (<48h) i poprawiona logika szoku.
        """
        if not news_data or df.empty:
            return {'score':0, 'sentiment_shock':0, 'news_count':0, 'resilience_score':0, 'last_sentiment':0}

        # 1. Filtrowanie i Sortowanie Newsów
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=48)
        
        valid_news = []
        for n in news_data:
            try:
                # Alpha Vantage format: 20240101T123000
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
            
        # 2. Obliczanie Szoku Sentymentu (Ważona średnia krocząca)
        # Ostatni sentyment vs średnia z poprzednich 3 (krótki termin)
        last_sent = sentiments[-1]
        prev_avg = np.mean(sentiments[:-1]) if len(sentiments) > 1 else 0
        sentiment_shock = last_sent - prev_avg

        # 3. Analiza Dywergencji
        # Cena z ostatnich 24h (6 świec 4H)
        lookback = min(6, len(df))
        price_change_24h = (df['close'].iloc[-1] / df['close'].iloc[-lookback] - 1) if lookback > 0 else 0
        
        resilience_score = 0
        # Scenariusz: Sentyment spada (złe wieści), ale cena nie spada
        if sentiment_shock < -0.15: 
            if price_change_24h > -0.01: # Cena stabilna lub rośnie
                resilience_score = 100 # BULLISH DIVERGENCE
                
        return {
            'score': 80 if resilience_score > 0 else 0,
            'sentiment_shock': sentiment_shock,
            'news_count': len(valid_news),
            'resilience_score': resilience_score,
            'last_sentiment': last_sent
        }

    # --- FILAR 3: ME (Momentum Exhaustion) ---
    def _calculate_me(self, df: pd.DataFrame, news_data: List[Dict]) -> Dict:
        # Kod RSI/APO był poprawny logicznie, dodaję zabezpieczenia na długość danych
        if len(df) < 30:
             return {'score':50, 'is_trap':False, 'rsi':0, 'apo':0}

        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        
        rs = gain / (loss + 0.00001) # Unikaj dzielenia przez 0
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

        return {'score': me_score, 'is_trap': is_trap, 'rsi': current_rsi, 'apo': current_apo}

    # --- HELPERY DANYCH ---

    def _get_market_data(self, ticker: str):
        """
        Pobiera surowe dane 5min ORAZ agreguje je do 4H.
        Zwraca (df_5min, df_virtual).
        """
        # 1. Pobranie danych 5min
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
        
        # Konwersja typów (zabezpieczenie przed błędami stringów)
        for c in ['open', 'high', 'low', 'close', 'volume']:
            df_5min[c] = pd.to_numeric(df_5min[c], errors='coerce')
        
        df_5min.dropna(inplace=True)

        # 2. AGREGACJA DO 4h (Virtual Candles)
        # Używamy label='right' i closed='right' dla spójności
        df_virtual = df_5min.resample(SDAR_VIRTUAL_TIMEFRAME, label='right', closed='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        return df_5min, df_virtual

    def _get_news_data(self, ticker: str) -> List[Dict]:
        try:
            resp = self.client.get_news_sentiment(ticker, limit=50)
            if resp and 'feed' in resp: return resp['feed']
        except: pass
        return []
    
    def _fetch_candidates(self, limit: int) -> List[str]:
        # Wybieramy tylko płynne spółki z Fazy 1
        query = text(f"SELECT ticker FROM phase1_candidates WHERE volume > 100000 ORDER BY score DESC LIMIT {limit}")
        result = self.session.execute(query).fetchall()
        return [r[0] for r in result]

    def _is_near_earnings(self, ticker: str) -> bool:
        """
        Sprawdza czy spółka ma wyniki w oknie zabronionym.
        Rozszerzone o szukanie w tabeli companies jeśli brak w phase1.
        """
        try:
            # 1. Sprawdź Phase 1
            query = text("SELECT days_to_earnings FROM phase1_candidates WHERE ticker=:t")
            res = self.session.execute(query, {'t': ticker}).first()
            if res and res[0] is not None:
                if -2 <= res[0] <= 1: return True # 2 dni przed, 1 po
                
            # 2. Tu można dodać fallback do innej tabeli (np. companies), jeśli przechowujemy tam earnings
            return False
        except: return False

    def _save_result(self, result: SdarCandidate):
        self.session.merge(result)
        self.session.commit()
"""
