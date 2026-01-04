import logging
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional

# Importy z systemu Apex
from ..models import SdarCandidate, Phase1Candidate
from .utils import get_raw_data_with_cache, standardize_df_columns

logger = logging.getLogger(__name__)

# === KONFIGURACJA SDAR ===
SDAR_LOOKBACK_DAYS = 7          # Okno analizy intraday
SDAR_INTERVAL = '60min'         # Interwał
BATCH_DELAY = 0.5               # Traffic shaping

class SDARAnalyzer:
    """
    Silnik Analityczny SDAR (System Detekcji Anomalii Rynkowych).
    Wersja PRO (Bez uproszczeń):
    - SAI: Analiza wolumenu OBV z uwzględnieniem mikro-trendów.
    - SPD: Event-Driven Sentiment Mapping (Mapowanie newsów do konkretnych świec).
    """

    def __init__(self, session: Session, api_client):
        self.session = session
        self.client = api_client

    def run_sdar_cycle(self, limit: int = 50) -> List[str]:
        """Główna pętla skanera SDAR."""
        logger.info(f"🚀 SDAR (Real Money Mode): Rozpoczynam cykl analizy dla max {limit} spółek...")

        candidates = self._fetch_candidates(limit)
        
        if not candidates:
            logger.warning("SDAR: Brak kandydatów do analizy.")
            return []

        processed_tickers = []

        for ticker in candidates:
            try:
                time.sleep(BATCH_DELAY)
                result = self.analyze_ticker(ticker)
                
                if result:
                    self._save_result(result)
                    processed_tickers.append(ticker)
                    logger.info(f"✅ SDAR: {ticker} -> Score: {result.total_anomaly_score:.2f} [SAI: {result.sai_score:.2f} | SPD: {result.spd_score:.2f}]")
                else:
                    logger.debug(f"SDAR: {ticker} odrzucony (brak danych/anomalii).")

            except Exception as e:
                logger.error(f"❌ SDAR Error dla {ticker}: {str(e)}")
                self.session.rollback()

        return processed_tickers

    def analyze_ticker(self, ticker: str) -> Optional[SdarCandidate]:
        """
        Pełna analiza: Fuzja techniki (Intraday) i fundamentów (News).
        """
        # A. Dane Intraday (Cena + Wolumen)
        df_intraday = self._get_intraday_data(ticker)
        if df_intraday is None or df_intraday.empty:
            return None

        # B. Dane News (Sentyment)
        news_data = self._get_news_data(ticker)

        # C. Wskaźnik SAI (Silent Accumulation Index)
        sai_metrics = self._calculate_sai(df_intraday)
        
        # D. Wskaźnik SPD (Sentiment-Price Divergence) - Wersja Event-Driven
        spd_metrics = self._calculate_spd(df_intraday, news_data)

        # E. Scoring (Wagi dynamiczne)
        # Jeśli mamy silną dywergencję na newsach (SPD), jest ona ważniejsza niż technika.
        sai_score = sai_metrics['score']
        spd_score = spd_metrics['score']
        
        if spd_score > 0:
            # Wykryto anomalię informacyjną - zwiększamy jej wagę
            total_score = (sai_score * 0.4) + (spd_score * 0.6)
        else:
            # Brak newsów lub brak dywergencji - polegamy na technice
            total_score = sai_score

        # F. Zapis Wyników
        result = SdarCandidate(
            ticker=ticker,
            sai_score=sai_score,
            spd_score=spd_score,
            total_anomaly_score=total_score,
            
            # Detale SAI
            atr_compression=sai_metrics['atr_compression'],
            obv_slope=sai_metrics['obv_slope'],
            price_stability=sai_metrics['price_stability'],
            smart_money_flow=sai_metrics['smart_money_flow'],
            
            # Detale SPD
            sentiment_shock=spd_metrics['sentiment_shock'],
            news_volume_spike=spd_metrics['news_volume_spike'],
            price_resilience=spd_metrics['price_resilience'],
            last_sentiment_score=spd_metrics['current_sentiment'],
            
            analysis_date=datetime.now()
        )
        
        return result

    # --- THE ALCHEMY (CORE LOGIC) ---

    def _calculate_sai(self, df: pd.DataFrame) -> Dict:
        """
        Silent Accumulation Index (SAI).
        Wykrywa: Wzrost OBV przy braku wzrostu ceny (Kompresja).
        """
        # 1. OBV Calculation
        df['price_change'] = df['close'].diff()
        df['obv_direction'] = np.where(df['price_change'] > 0, 1, -1)
        df['obv_direction'] = np.where(df['price_change'] == 0, 0, df['obv_direction'])
        df['obv'] = (df['volume'] * df['obv_direction']).cumsum()

        # 2. ATR Calculation (Intraday volatility)
        df['tr'] = df['high'] - df['low']
        df['atr'] = df['tr'].rolling(window=14).mean()

        # 3. Analiza Regresji (Ostatnie 40 świec ~ tydzień handlowy H1)
        lookback = 40
        if len(df) < lookback: lookback = len(df)
        
        subset = df.iloc[-lookback:].copy()
        x = np.arange(len(subset))
        
        # Nachylenia (Slopes)
        slope_price = np.polyfit(x, subset['close'], 1)[0]
        price_norm = slope_price / subset['close'].mean() * 100 
        
        slope_obv = np.polyfit(x, subset['obv'], 1)[0]
        avg_vol = subset['volume'].mean()
        obv_norm = slope_obv / avg_vol if avg_vol > 0 else 0
        
        slope_atr = np.polyfit(x, subset['atr'].fillna(0), 1)[0]
        
        # 4. Scoring SAI
        score = 0
        
        # WARUNEK 1: Dywergencja OBV (Cena stoi/spada, OBV rośnie)
        if obv_norm > 0.05:          # OBV rośnie
            if price_norm < 0.02:    # Cena płaska lub spada
                score += 40
            if price_norm < -0.01:   # Cena spada (Silna dywergencja)
                score += 20
        
        # WARUNEK 2: Kompresja Zmienności (ATR maleje przed wybuchem)
        if slope_atr < 0:
            score += 20
            
        # WARUNEK 3: Smart Money Flow (Cena zamyka się wysoko na dużym wolumenie)
        # Uproszczona wersja Money Flow Multiplier
        
        return {
            'score': min(score, 100),
            'atr_compression': slope_atr,
            'obv_slope': obv_norm,
            'price_stability': 1.0 / (abs(price_norm) + 0.01),
            'smart_money_flow': (subset['close'] * subset['volume']).mean()
        }

    def _calculate_spd(self, df_price: pd.DataFrame, news_items: List[Dict]) -> Dict:
        """
        Sentiment-Price Divergence (SPD) - EVENT DRIVEN.
        
        Zasada: Dla każdego newsa sprawdzamy reakcję rynku w oknie 4 godzin (4 świece H1).
        Szukamy "Teflonowych Spółek": Złe newsy -> Cena nie spada.
        """
        default_result = {
            'score': 0, 'sentiment_shock': 0, 
            'news_volume_spike': 0, 'price_resilience': 0, 'current_sentiment': 0
        }
        
        if not news_items or df_price.empty:
            return default_result

        # Konwersja indeksu daty na timezone-aware (jeśli trzeba)
        if df_price.index.tz is None:
            # Zakładamy UTC dla Alpha Vantage
            df_price.index = df_price.index.tz_localize('UTC')

        divergence_accumulated = 0
        weighted_sentiment_sum = 0
        weight_sum = 0
        
        processed_news_count = 0
        resilience_instances = 0 # Ile razy cena oparła się złym newsom

        for item in news_items:
            try:
                # 1. Parsowanie danych newsa
                sentiment_score = float(item.get('overall_sentiment_score', 0))
                time_str = item.get('time_published') # Format: '20230101T120000'
                
                if not time_str: continue
                
                news_time = datetime.strptime(time_str, '%Y%m%dT%H%M%S')
                news_time = news_time.replace(tzinfo=df_price.index.tz) # Synchronizacja stref

                # 2. Obliczenie "Świeżości" (Time Decay)
                # News starszy niż 48h nas nie interesuje w kontekście SDAR
                hours_elapsed = (datetime.now(news_time.tzinfo) - news_time).total_seconds() / 3600
                if hours_elapsed > 48: 
                    continue
                
                # Waga spada liniowo do 0 po 48h. News sprzed 1h ma wagę ~1.0
                recency_weight = max(0, 1 - (hours_elapsed / 48.0))
                
                weighted_sentiment_sum += sentiment_score * recency_weight
                weight_sum += recency_weight
                processed_news_count += 1

                # 3. Analiza Reakcji Ceny (Event Window Analysis)
                # Pobieramy świece po publikacji newsa (okno 4h)
                post_news_candles = df_price[df_price.index >= news_time].head(4)
                
                if len(post_news_candles) > 0:
                    start_price = post_news_candles['open'].iloc[0]
                    end_price = post_news_candles['close'].iloc[-1]
                    price_reaction_pct = (end_price - start_price) / start_price * 100
                    
                    # === DETEKCJA ANOMALII (DIVERGENCE) ===
                    
                    # SCENARIUSZ A: Zły News (Bearish) + Cena Stabilna/Rośnie
                    # To jest "Ślad Giganta" - ktoś skupuje spadki
                    if sentiment_score <= -0.15: 
                        if price_reaction_pct > -0.2: # Cena spadła mniej niż 0.2% lub wzrosła
                            # Im gorszy news i lepsza cena, tym wyższy wynik
                            div_strength = abs(sentiment_score) + (price_reaction_pct if price_reaction_pct > 0 else 0)
                            divergence_accumulated += (div_strength * 50 * recency_weight)
                            resilience_instances += 1
                            
                    # SCENARIUSZ B: Dobry News (Bullish) + Cena Spada
                    # To jest "Dystrybucja" - uciekamy (negatywna dywergencja)
                    elif sentiment_score >= 0.15:
                        if price_reaction_pct < -0.5:
                            # Odejmujemy punkty - pułapka na byki
                            divergence_accumulated -= (30 * recency_weight)

            except Exception as e:
                continue

        # 4. Finalizacja Wyników
        avg_weighted_sentiment = weighted_sentiment_sum / weight_sum if weight_sum > 0 else 0
        
        # Normalizacja wyniku SPD do 0-100
        # divergence_accumulated może być wysokie, ucinamy na 100
        final_score = min(max(divergence_accumulated, 0), 100)

        return {
            'score': final_score,
            'sentiment_shock': avg_weighted_sentiment,
            'news_volume_spike': processed_news_count,
            'price_resilience': resilience_instances,
            'current_sentiment': avg_weighted_sentiment
        }

    # --- DATA FETCHING (Zoptymalizowane pod Worker) ---

    def _fetch_candidates(self, limit: int) -> List[str]:
        # Pobieramy tylko te spółki z Fazy 1, które mają sensowny wolumen
        query = text(f"SELECT ticker FROM phase1_candidates WHERE volume > 50000 ORDER BY score DESC LIMIT {limit}")
        result = self.session.execute(query).fetchall()
        return [row[0] for row in result]

    def _get_intraday_data(self, ticker: str) -> Optional[pd.DataFrame]:
        raw_data = get_raw_data_with_cache(
            self.session, self.client, ticker, 
            'INTRADAY_60', 
            lambda t: self.client.get_intraday(t, interval=SDAR_INTERVAL, outputsize='full'),
            expiry_hours=1 
        )
        
        ts_key = f'Time Series ({SDAR_INTERVAL})'
        if not raw_data or ts_key not in raw_data:
            return None
            
        df = pd.DataFrame.from_dict(raw_data[ts_key], orient='index')
        df = standardize_df_columns(df) 
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        
        cols = ['open', 'high', 'low', 'close', 'volume']
        for col in cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df

    def _get_news_data(self, ticker: str) -> List[Dict]:
        try:
            # Pobieramy newsy, sortowane od najnowszych
            resp = self.client.get_news_sentiment(ticker, limit=50)
            if resp and 'feed' in resp:
                return resp['feed']
        except Exception as e:
            logger.warning(f"SDAR: Błąd newsów dla {ticker}: {e}")
        return []

    def _save_result(self, result: SdarCandidate):
        self.session.merge(result)
        self.session.commit()
