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
SDAR_LOOKBACK_DAYS = 7          # Okno analizy intraday (ostatnie 7 dni)
SDAR_INTERVAL = '60min'         # Interwał świec dla precyzyjnej analizy
BATCH_DELAY = 0.5               # Opóźnienie między tickerami (Traffic shaping)

class SDARAnalyzer:
    """
    Silnik Analityczny SDAR (System Detekcji Anomalii Rynkowych).
    Implementuje autorskie wskaźniki:
    - SAI (Silent Accumulation Index): Szuka rosnącego OBV przy płaskiej cenie i malejącym ATR.
    - SPD (Sentiment-Price Divergence): Szuka rozbieżności między sentymentem newsów a reakcją ceny.
    """

    def __init__(self, session: Session, api_client):
        self.session = session
        self.client = api_client

    def run_sdar_cycle(self, limit: int = 50) -> List[str]:
        """
        Główna pętla skanera SDAR.
        Pobiera kandydatów z Fazy 1 (lub innych źródeł) i przeprowadza głęboką analizę korelacji.
        """
        logger.info(f"🚀 SDAR (Nowa Idea): Rozpoczynam cykl analizy dla max {limit} spółek...")

        # 1. Pobieranie kandydatów (Na razie bierzemy najlepszych z Fazy 1)
        # Możemy tu dodać logikę pobierania wszystkich, ale zaczynamy od selekcji dla oszczędności API.
        candidates = self._fetch_candidates(limit)
        
        if not candidates:
            logger.warning("SDAR: Brak kandydatów do analizy.")
            return []

        processed_tickers = []

        for ticker in candidates:
            try:
                # Spowolnienie dla ochrony limitów API
                time.sleep(BATCH_DELAY)
                
                result = self.analyze_ticker(ticker)
                
                if result:
                    self._save_result(result)
                    processed_tickers.append(ticker)
                    logger.info(f"✅ SDAR: {ticker} przeanalizowany. Score: {result.total_anomaly_score:.2f} (SAI: {result.sai_score:.2f}, SPD: {result.spd_score:.2f})")
                else:
                    logger.debug(f"SDAR: {ticker} brak danych lub błąd analizy.")

            except Exception as e:
                logger.error(f"❌ SDAR Error dla {ticker}: {str(e)}")
                self.session.rollback()

        logger.info(f"🏁 SDAR: Zakończono cykl. Przeanalizowano: {len(processed_tickers)} spółek.")
        return processed_tickers

    def analyze_ticker(self, ticker: str) -> Optional[SdarCandidate]:
        """
        Przeprowadza pełną analizę jednego waloru.
        Łączy dane techniczne (Intraday) i sentymentalne (News).
        """
        # A. Pobierz dane Intraday (Cena + Wolumen)
        df_intraday = self._get_intraday_data(ticker)
        if df_intraday is None or df_intraday.empty:
            return None

        # B. Pobierz dane o Newsach (Sentyment)
        news_data = self._get_news_data(ticker)

        # C. Oblicz Wskaźnik SAI (Silent Accumulation)
        sai_metrics = self._calculate_sai(df_intraday)
        
        # D. Oblicz Wskaźnik SPD (Sentiment Divergence)
        spd_metrics = self._calculate_spd(df_intraday, news_data)

        # E. Fuzja wyników (Total Score)
        # Waga: SAI (60%) + SPD (40%). 
        # Jeśli SPD jest 0 (brak newsów), SAI przejmuje 100%.
        
        sai_score = sai_metrics['score']
        spd_score = spd_metrics['score']
        
        if spd_score != 0:
            total_score = (sai_score * 0.6) + (spd_score * 0.4)
        else:
            total_score = sai_score

        # F. Budowa obiektu modelu
        result = SdarCandidate(
            ticker=ticker,
            
            # Główne Wyniki
            sai_score=sai_score,
            spd_score=spd_score,
            total_anomaly_score=total_score,
            
            # Detale SAI
            atr_compression=sai_metrics['atr_compression'],
            obv_slope=sai_metrics['obv_slope'],
            price_stability=sai_metrics['price_stability'],
            smart_money_flow=sai_metrics['smart_money_flow'], # VWAP/OBV delta
            
            # Detale SPD
            sentiment_shock=spd_metrics['sentiment_shock'],
            news_volume_spike=spd_metrics['news_volume_spike'],
            price_resilience=spd_metrics['price_resilience'],
            
            last_sentiment_score=spd_metrics['current_sentiment'],
            
            analysis_date=datetime.now()
        )
        
        return result

    # --- IMPLEMENTACJA ALGORYTMÓW (THE ALCHEMY) ---

    def _calculate_sai(self, df: pd.DataFrame) -> Dict:
        """
        Oblicza Silent Accumulation Index.
        Szukamy: Rosnący OBV + Płaska/Malejąca Cena + Malejący ATR (Kompresja).
        """
        # 1. Obliczenie OBV (On-Balance Volume)
        # OBV = Poprzedni OBV + (Vol jeśli Close > PrevClose) - (Vol jeśli Close < PrevClose)
        df['price_change'] = df['close'].diff()
        df['obv_direction'] = np.where(df['price_change'] > 0, 1, -1)
        df['obv_direction'] = np.where(df['price_change'] == 0, 0, df['obv_direction'])
        df['obv'] = (df['volume'] * df['obv_direction']).cumsum()

        # 2. Obliczenie ATR (Zmienność) - Uproszczony na Intraday (High-Low)
        df['tr'] = df['high'] - df['low']
        df['atr'] = df['tr'].rolling(window=14).mean()

        # 3. Analiza Trendów (Regresja Liniowa na ostatnich X świecach)
        lookback = 40 # ok. tydzień dla świec H1 (5-8h sesji dziennie)
        if len(df) < lookback:
            lookback = len(df)
        
        subset = df.iloc[-lookback:].copy()
        x = np.arange(len(subset))
        
        # Nachylenie ceny (znormalizowane)
        slope_price = np.polyfit(x, subset['close'], 1)[0]
        price_norm = slope_price / subset['close'].mean() * 100 # % zmiany na świecę
        
        # Nachylenie OBV (znormalizowane)
        slope_obv = np.polyfit(x, subset['obv'], 1)[0]
        # Normalizacja OBV jest trudna, używamy relacji do średniego wolumenu
        avg_vol = subset['volume'].mean()
        obv_norm = slope_obv / avg_vol if avg_vol > 0 else 0
        
        # Nachylenie ATR (Kompresja)
        slope_atr = np.polyfit(x, subset['atr'].fillna(0), 1)[0]
        
        # 4. Punktacja SAI
        # Warunek idealny: Cena płaska (bliska 0), OBV rośnie (>0), ATR maleje (<0)
        
        score = 0
        
        # A. Dywergencja OBV vs Cena
        # Jeśli cena spada/płaska a OBV rośnie -> BARDZO DOBRZE
        if obv_norm > 0.1:
            if price_norm < 0.05: # Cena nie rośnie szybko lub spada
                score += 50
            if price_norm < -0.01: # Cena spada przy rosnącym OBV (Ukryta akumulacja)
                score += 20
        
        # B. Kompresja Zmienności
        if slope_atr < 0:
            score += 20
            
        # C. Skalowanie wyniku (0-100)
        # Normalizacja smart_money_flow
        smart_money = (subset['close'] * subset['volume']).mean() # Uproszczone

        return {
            'score': min(score, 100),
            'atr_compression': slope_atr,
            'obv_slope': obv_norm,
            'price_stability': 1.0 / (abs(price_norm) + 0.01), # Im mniejsza zmiana ceny, tym wyższa stabilność
            'smart_money_flow': smart_money
        }

    def _calculate_spd(self, df_price: pd.DataFrame, news_items: List[Dict]) -> Dict:
        """
        Oblicza Sentiment-Price Divergence.
        Szukamy: Zły sentyment + Brak spadku ceny = Siła.
        """
        default_result = {
            'score': 0, 'sentiment_shock': 0, 
            'news_volume_spike': 0, 'price_resilience': 0, 'current_sentiment': 0
        }
        
        if not news_items:
            return default_result

        # 1. Agregacja Sentymentu
        sentiments = []
        for item in news_items:
            try:
                val = float(item.get('overall_sentiment_score', 0))
                sentiments.append(val)
            except:
                continue
        
        if not sentiments:
            return default_result

        avg_sentiment = np.mean(sentiments)
        
        # 2. Reakcja Ceny (Ostatnie 24h - ok. 8 świec handlowych dla interwału H1)
        # Uwaga: To jest uproszczenie. W idealnym świecie mapowalibyśmy newsy do świec.
        price_change_recent = 0
        if len(df_price) >= 8:
            price_change_recent = (df_price['close'].iloc[-1] - df_price['close'].iloc[-8]) / df_price['close'].iloc[-8] * 100
        
        score = 0
        resilience = 0
        
        # Scenariusz A: Negatywny Sentyment, Cena Stabilna/Rośnie (Bullish Divergence)
        if avg_sentiment < -0.15:
            if price_change_recent > -0.5: # Cena nie spadła mocno
                resilience = 1
                score = 70 + (price_change_recent * 10) # Bonus za wzrost
                
        # Scenariusz B: Neutralny Sentyment, Cena Rośnie (Organiczny wzrost)
        elif -0.15 <= avg_sentiment <= 0.15:
            if price_change_recent > 2.0:
                score = 50
        
        return {
            'score': min(max(score, 0), 100),
            'sentiment_shock': avg_sentiment, # Traktujemy obecny średni jako 'shock' w uproszczeniu
            'news_volume_spike': len(news_items),
            'price_resilience': resilience,
            'current_sentiment': avg_sentiment
        }

    # --- POMOCNICZE (DATA FETCHING) ---

    def _fetch_candidates(self, limit: int) -> List[str]:
        # Pobieramy tickery z tabeli Phase1Candidate (posortowane wg score)
        # To zapewnia, że analizujemy spółki, które już wstępnie rokują
        query = text(f"SELECT ticker FROM phase1_candidates ORDER BY score DESC LIMIT {limit}")
        result = self.session.execute(query).fetchall()
        return [row[0] for row in result]

    def _get_intraday_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Pobiera i formatuje dane Intraday."""
        raw_data = get_raw_data_with_cache(
            self.session, self.client, ticker, 
            'INTRADAY_60', # Klucz cache
            lambda t: self.client.get_intraday(t, interval=SDAR_INTERVAL, outputsize='full'),
            expiry_hours=1 # Dane intraday starzeją się szybko
        )
        
        ts_key = f'Time Series ({SDAR_INTERVAL})'
        if not raw_data or ts_key not in raw_data:
            return None
            
        df = pd.DataFrame.from_dict(raw_data[ts_key], orient='index')
        df = standardize_df_columns(df) # Zamienia '1. open' -> 'open'
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        
        # Konwersja na float
        cols = ['open', 'high', 'low', 'close', 'volume']
        for col in cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df

    def _get_news_data(self, ticker: str) -> List[Dict]:
        """Pobiera newsy z Alpha Vantage."""
        # Newsy cache'ujemy rzadziej lub wcale w tej fazie, 
        # ale dla wydajności użyjemy cache jeśli dostępny w utils (tu direct call dla świeżości)
        try:
            # Pobieramy 50 ostatnich newsów
            resp = self.client.get_news_sentiment(ticker, limit=50)
            if resp and 'feed' in resp:
                return resp['feed']
        except Exception as e:
            logger.warning(f"SDAR: Nie udało się pobrać newsów dla {ticker}: {e}")
        return []

    def _save_result(self, result: SdarCandidate):
        """Zapisuje wynik do bazy (Upsert)."""
        self.session.merge(result)
        self.session.commit()
