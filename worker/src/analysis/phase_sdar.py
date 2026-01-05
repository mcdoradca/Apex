import logging
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional

# Importy z systemu Apex
from ..models import SdarCandidate
from .utils import get_raw_data_with_cache, standardize_df_columns

logger = logging.getLogger(__name__)

# === KONFIGURACJA SDAR (ZGODNA Z PDF) ===
SDAR_RAW_INTERVAL = '5min'      # Dane źródłowe (Mikro-struktura)
SDAR_VIRTUAL_TIMEFRAME = '4H'   # Wirtualna świeca (Agregacja)
SDAR_LOOKBACK_CANDLES = 100     # Ile świec 4H analizować wstecz
BATCH_DELAY = 1.0               # Zwiększono opóźnienie dla bezpieczeństwa API

class SDARAnalyzer:
    """
    Silnik Analityczny SDAR (System Detekcji Anomalii Rynkowych) - Wersja Zgodna z PDF.
    
    Zaimplementowane Filary:
    1. SAI (Silent Accumulation): Dane 5min -> Agregacja 4H -> Precyzyjny VWAP i OBV.
    2. SPD (Sentiment Divergence): Analiza Szoku Sentymentu (Druga Pochodna).
    3. ME (Momentum Exhaustion): Wykrywanie pułapek byka (RSI + APO).
    4. Risk Guard: Filtr wyników finansowych (Earnings).
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
                # 0. Risk Guard: Earnings Filter (PDF Faza 4 pkt 3)
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
        # A. Pobranie danych Mikro (5min) i Agregacja do 4H
        df_virtual = self._get_virtual_candles(ticker)
        if df_virtual is None or len(df_virtual) < 50:
            return None

        # B. Dane News (Sentyment)
        news_data = self._get_news_data(ticker)

        # C. Obliczenia Filarów
        sai = self._calculate_sai(df_virtual)        # Filar 1: Cicha Akumulacja
        spd = self._calculate_spd(df_virtual, news_data) # Filar 2: Dywergencja Sentymentu
        me  = self._calculate_me(df_virtual, news_data)  # Filar 3: Wyczerpanie Pędu (RSI+APO)

        # D. Scoring Hybrydowy
        # PDF: Szukamy potwierdzenia w dysharmonii.
        # Bazowy wynik to SAI (Fundament techniczny). 
        # SPD działa jako mnożnik "szansy" (Bullish Divergence).
        # ME działa jako filtr negatywny (Bull Trap).
        
        total_score = (sai['score'] * 0.4) + (spd['score'] * 0.4) + (me['score'] * 0.2)
        
        # Jeśli wykryto wyczerpanie pędu (Bull Trap), drastycznie obniż wynik
        if me['is_trap']:
            total_score *= 0.5

        return SdarCandidate(
            ticker=ticker,
            
            # Wyniki
            sai_score=sai['score'],
            spd_score=spd['score'],
            me_score=me['score'],
            total_anomaly_score=total_score,
            
            # Detale SAI
            atr_compression=sai['atr_compression'],
            obv_slope=sai['obv_slope'],
            price_stability=sai['price_stability'],
            smart_money_flow=sai['smart_money_flow'], # VWAP Logic
            
            # Detale SPD
            sentiment_shock=spd['sentiment_shock'],
            news_volume_spike=spd['news_count'],
            price_resilience=spd['resilience_score'],
            last_sentiment_score=spd['last_sentiment'],
            
            # Detale ME
            metric_rsi=me['rsi'],
            metric_apo=me['apo'],
            
            analysis_date=datetime.now()
        )

    # --- FILAR 1: SAI (Silent Accumulation Index) ---
    def _calculate_sai(self, df: pd.DataFrame) -> Dict:
        """
        Analiza na świecach wirtualnych 4H.
        Wykrywa: Płaska cena + Rosnący OBV + Malejący ATR + VWAP Support.
        """
        # 1. OBV & VWAP Calculation
        # OBV liczymy na już zagregowanych danych
        df['price_change'] = df['close'].diff()
        df['obv_dir'] = np.where(df['price_change'] > 0, 1, -1)
        df['obv_dir'] = np.where(df['price_change'] == 0, 0, df['obv_dir'])
        df['obv'] = (df['volume'] * df['obv_dir']).cumsum()
        
        # VWAP (uproszczony dla świec 4H - suma (P*V) / suma V w oknie kroczącym)
        df['pv'] = df['close'] * df['volume']
        df['vwap'] = df['pv'].rolling(window=20).sum() / df['volume'].rolling(window=20).sum()

        # 2. ATR Calculation (Zmienność)
        df['tr'] = np.maximum(df['high'] - df['low'], 
                   np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
        df['atr'] = df['tr'].rolling(window=14).mean()

        # 3. Analiza Regresji (Ostatnie 10 świec 4H ~ tydzień handlowy)
        subset = df.iloc[-10:].copy()
        if len(subset) < 10: return {'score':0, 'atr_compression':0, 'obv_slope':0, 'price_stability':0, 'smart_money_flow':0}
        
        x = np.arange(len(subset))
        
        # Normalizowane nachylenia
        slope_price = np.polyfit(x, subset['close'], 1)[0] / subset['close'].mean() * 100
        slope_obv = np.polyfit(x, subset['obv'], 1)[0] / subset['volume'].mean() # Relatywne do wolumenu
        slope_atr = np.polyfit(x, subset['atr'], 1)[0]
        
        # VWAP Check: Czy cena jest powyżej VWAP? (Instytucjonalne wsparcie)
        last_price = subset['close'].iloc[-1]
        last_vwap = subset['vwap'].iloc[-1]
        vwap_support = 1 if last_price > last_vwap else 0

        score = 0
        # A. Dywergencja OBV (Cena płaska/spada, OBV rośnie)
        if slope_obv > 0.5: # Silny napływ
            if abs(slope_price) < 0.2: score += 40 # Cena stabilna
            elif slope_price < 0: score += 50      # Cena spada (Silniejszy sygnał)
            
        # B. Kompresja Zmienności (Cisza przed burzą)
        if slope_atr < 0: score += 20
        
        # C. VWAP Support
        if vwap_support: score += 10

        return {
            'score': min(score, 100),
            'atr_compression': slope_atr,
            'obv_slope': slope_obv,
            'price_stability': 1.0 / (abs(slope_price) + 0.01),
            'smart_money_flow': last_vwap
        }

    # --- FILAR 2: SPD (Sentiment-Price Divergence) ---
    def _calculate_spd(self, df: pd.DataFrame, news_data: List[Dict]) -> Dict:
        """
        Badanie 'Sentiment Shock' i 'Price Resilience'.
        """
        if not news_data or df.empty:
            return {'score':0, 'sentiment_shock':0, 'news_count':0, 'resilience_score':0, 'last_sentiment':0}

        # 1. Obliczanie Szoku Sentymentu (Pochodna zmian)
        # Sortujemy newsy chronologicznie
        sorted_news = sorted(news_data, key=lambda x: x.get('time_published', ''))
        sentiments = []
        for n in sorted_news[-10:]: # Ostatnie 10 newsów
            try:
                s = float(n.get('overall_sentiment_score', 0))
                sentiments.append(s)
            except: pass
            
        sentiment_shock = 0
        if len(sentiments) >= 2:
            # Druga pochodna (zmiana zmiany) lub po prostu dynamika ostatnich zmian
            # Jeśli ostatni jest mocno ujemny, a poprzedni był neutralny -> Szok negatywny
            sentiment_shock = sentiments[-1] - np.mean(sentiments[:-1])

        # 2. Analiza Dywergencji (Reakcja Ceny na ostatnie newsy)
        # Jeśli newsy są negatywne (Shock < -0.2), a cena w ostatnich świecach 4H nie spada
        recent_price_change = df['close'].iloc[-1] / df['close'].iloc[-3] - 1 # Zmiana z ostatnich 12h (3 świece 4H)
        
        resilience_score = 0
        if sentiment_shock < -0.2: # Atak złych newsów
            if recent_price_change > -0.01: # Cena spadła mniej niż 1% lub wzrosła
                resilience_score = 100 # BULLISH DIVERGENCE
                
        # Wynik
        final_score = 0
        if resilience_score > 0: final_score = 80 # Wysoki wynik za dywergencję
        
        return {
            'score': final_score,
            'sentiment_shock': sentiment_shock,
            'news_count': len(news_data),
            'resilience_score': resilience_score,
            'last_sentiment': sentiments[-1] if sentiments else 0
        }

    # --- FILAR 3: ME (Momentum Exhaustion - NOWOŚĆ) ---
    def _calculate_me(self, df: pd.DataFrame, news_data: List[Dict]) -> Dict:
        """
        Wykrywanie Bull Trap (PDF 2.3).
        Składniki: RSI + APO.
        """
        # 1. RSI (Relative Strength Index)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 2. APO (Absolute Price Oscillator) - SMA Fast - SMA Slow
        df['apo'] = df['close'].rolling(window=12).mean() - df['close'].rolling(window=26).mean()
        
        current_rsi = df['rsi'].iloc[-1]
        current_apo = df['apo'].iloc[-1]
        prev_apo = df['apo'].iloc[-2]
        price_new_high = df['close'].iloc[-1] > df['close'].iloc[-5:].max() # Czy mamy lokalny szczyt?
        
        is_trap = False
        me_score = 50 # Neutralny
        
        # Logika Bull Trap: Cena robi szczyt, ale APO spada (dywergencja pędu) + RSI wykupione
        if price_new_high:
            if current_apo < prev_apo: # Momentum spada
                if current_rsi > 70:   # Wykupienie
                    is_trap = True
                    me_score = 0 # Dyskwalifikacja
        else:
            # Jeśli nie ma pułapki, a APO rośnie -> pozytywne momentum
            if current_apo > prev_apo and current_apo > 0:
                me_score = 80

        return {
            'score': me_score,
            'is_trap': is_trap,
            'rsi': current_rsi,
            'apo': current_apo
        }

    # --- HELPERY DANYCH ---

    def _get_virtual_candles(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Pobiera surowe dane 5min i agreguje je do 4H (Virtual Candles).
        To realizuje postulat '1.1 Dane Cenowe o Wysokiej Częstotliwości'.
        """
        # 1. Pobranie danych 5min
        raw_data = get_raw_data_with_cache(
            self.session, self.client, ticker,
            'INTRADAY_5',
            lambda t: self.client.get_intraday(t, interval='5min', outputsize='full'),
            expiry_hours=1
        )
        
        if not raw_data or 'Time Series (5min)' not in raw_data:
            return None
            
        # 2. Konwersja do DataFrame
        df = pd.DataFrame.from_dict(raw_data['Time Series (5min)'], orient='index')
        df = standardize_df_columns(df)
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        
        # Konwersja typów
        cols = ['open', 'high', 'low', 'close', 'volume']
        for c in cols: df[c] = pd.to_numeric(df[c])

        # 3. AGREGACJA DO 4H (Virtual Candles)
        # Resampling z logiką OHLCV
        df_virtual = df.resample(SDAR_VIRTUAL_TIMEFRAME).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        return df_virtual

    def _get_news_data(self, ticker: str) -> List[Dict]:
        # Bez zmian - pobieramy newsy jak wcześniej
        try:
            resp = self.client.get_news_sentiment(ticker, limit=50)
            if resp and 'feed' in resp: return resp['feed']
        except: pass
        return []
    
    def _fetch_candidates(self, limit: int) -> List[str]:
        # Dodatkowy filtr wolumenu (zgodnie z Risk Check)
        query = text(f"SELECT ticker FROM phase1_candidates WHERE volume > 100000 ORDER BY score DESC LIMIT {limit}")
        result = self.session.execute(query).fetchall()
        return [r[0] for r in result]

    def _is_near_earnings(self, ticker: str) -> bool:
        """
        Sprawdza czy spółka ma wyniki w oknie zabronionym (PDF Faza 4 pkt 3).
        Korzysta z danych zapisanych w tabeli TradingSignal (jeśli są) lub Phase1.
        """
        try:
            # Próbujemy pobrać datę earnings z bazy (Phase1Candidate ma pole days_to_earnings)
            query = text("SELECT days_to_earnings FROM phase1_candidates WHERE ticker=:t")
            res = self.session.execute(query, {'t': ticker}).first()
            
            if res and res[0] is not None:
                days = res[0]
                # Blokujemy: 2 dni przed (days <= 2) i 1 dzień po (days >= -1) -> Zakres [-1, 2]
                if -1 <= days <= 2:
                    return True
        except: pass
        return False

    def _save_result(self, result: SdarCandidate):
        self.session.merge(result)
        self.session.commit()
