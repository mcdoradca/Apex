import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ==================================================================================
# ADAPTIVE QUANTUM MOMENTUM (AQM) - LOGIKA CORE (ZGODNA Z PDF)
# ==================================================================================

def calculate_aqm_full_vector(
    daily_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    intraday_60m_df: pd.DataFrame,
    obv_df: pd.DataFrame,
    macro_data: Dict[str, Any], # VIX, SPY_DF, SECTOR
    earnings_days_to: Optional[int]
) -> pd.DataFrame:
    """
    Oblicza pełny wektor AQM dla każdego dnia w historii.
    Zwraca DataFrame z kolumnami: aqm_score, qps, ves, mrs, tcs.
    """
    try:
        # Kopiujemy df, żeby nie modyfikować oryginału
        df = daily_df.copy()
        
        # === 1. QUANTUM PRIME SCORE (QPS) - 40% wagi ===
        # Ocena spójności momentum na wielu timeframe'ach (Daily + Weekly)
        # (W pełnej wersji PDF jest też 5min/15min, tu upraszczamy do D/W dla backtestu EOD)
        
        # Obliczamy wskaźniki dla Daily
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
        
        # RSI 14 (wektorowo)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # MACD
        exp12 = df['close'].ewm(span=12, adjust=False).mean()
        exp26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp12 - exp26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        
        # ATR % (Zmienność)
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        df['atr'] = true_range.rolling(14).mean()
        df['atr_percent'] = df['atr'] / df['close']

        # Logika QPS (Wektorowa)
        def calc_qps_row(row):
            # Trend (50%)
            trend = 0.0
            if row['close'] > row['ema_20']: trend += 0.25
            if row['close'] > row['ema_50']: trend += 0.25
            if row['ema_20'] > row['ema_50']: trend += 0.25
            if row['ema_50'] > row['ema_200']: trend += 0.25
            
            # Momentum (30%)
            mom = 0.0
            if 50 < row['rsi'] < 70: mom += 0.5
            if row['macd'] > row['macd_signal']: mom += 0.5
            
            # Zmienność (20%)
            vol = 0.0
            if row['atr_percent'] < 0.05: vol = 1.0
            elif row['atr_percent'] < 0.08: vol = 0.7
            else: vol = 0.3
            
            # (Uwaga: Tu powinniśmy uśredniać z Weekly, na razie proxy Daily * 1.0)
            daily_score = (trend * 0.5) + (mom * 0.3) + (vol * 0.2)
            return daily_score # Uproszczenie: QPS = Daily Score

        df['qps'] = df.apply(calc_qps_row, axis=1)

        # === 2. VOLUME ENTROPY SCORE (VES) - 30% wagi ===
        # Analiza OBV i Volume Ratio
        
        # Dołączamy OBV (jeśli dostępne)
        if obv_df is not None and not obv_df.empty:
            # Upewniamy się, że indeksy się zgadzają
            df = df.join(obv_df['OBV'], rsuffix='_obv')
            df['obv'] = df['OBV'] # Ujednolicenie
        else:
            # Fallback: Liczymy przybliżone OBV
            df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()

        df['obv_20_ma'] = df['obv'].rolling(20).mean()
        df['obv_50_ma'] = df['obv'].rolling(50).mean()
        # Trend OBV (nachylenie liniowej regresji z 10 dni - uproszczone jako różnica)
        df['obv_trend'] = df['obv'].diff(10)

        df['volume_20_ma'] = df['volume'].rolling(20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_20_ma']

        # AD Line (Accumulation/Distribution)
        # AD = ((Close - Low) - (High - Close)) / (High - Low) * Volume
        ad_mult = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low']).replace(0, 1)
        df['ad_line'] = (ad_mult * df['volume']).cumsum()
        df['ad_line_20_ma'] = df['ad_line'].rolling(20).mean()
        df['ad_line_trend'] = df['ad_line'].diff(5)

        def calc_ves_row(row):
            # OBV (40%)
            obv_s = 0.0
            if row['obv'] > row['obv_20_ma']: obv_s += 0.4
            if row['obv_trend'] > 0: obv_s += 0.3
            if row['obv_20_ma'] > row['obv_50_ma']: obv_s += 0.3
            
            # Volume Ratio (30%)
            vol_s = 0.0
            if row['volume_ratio'] > 1.2: vol_s = 0.3
            elif row['volume_ratio'] > 0.8: vol_s = 0.2
            else: vol_s = 0.1
            
            # AD (30%)
            ad_s = 0.0
            if row['ad_line'] > row['ad_line_20_ma']: ad_s += 0.3
            if row['ad_line_trend'] > 0: ad_s += 0.2
            
            return obv_s + vol_s + ad_s

        df['ves'] = df.apply(calc_ves_row, axis=1)

        # === 3. MARKET REGIME SCORE (MRS) - 20% wagi ===
        # Adaptacja do VIX i SPY
        
        spy_df = macro_data.get('spy_df')
        vix_val = macro_data.get('vix', 20.0) # Domyślnie 20 (neutral)
        sector_trend = macro_data.get('sector_trend', 0.0) # -1.0 do 1.0
        
        # Obliczamy SMA 200 dla SPY
        is_bull_spy = True
        if spy_df is not None and not spy_df.empty:
            spy_curr = spy_df['close'].iloc[-1]
            spy_ma200 = spy_df['close'].rolling(200).mean().iloc[-1]
            if not pd.isna(spy_ma200) and spy_curr < spy_ma200:
                is_bull_spy = False
        
        # Określanie reżimu
        # BULL: VIX < 18 i SPY > SMA200
        # BEAR: VIX > 25 lub SPY < SMA50 (u nas SMA200)
        # VOLATILE: Reszta
        
        regime = 'VOLATILE'
        if vix_val < 18 and is_bull_spy: regime = 'BULL'
        elif vix_val > 25 or not is_bull_spy: regime = 'BEAR'
        
        # Beta (uproszczona korelacja z SPY z 60 dni)
        # Tutaj dla uproszczenia backtestu przyjmujemy stałą logikę per ticker
        # W pełnej wersji liczylibyśmy rolling beta.
        
        def calc_mrs_row(row):
            score = 0.0
            # Bazowa punktacja za reżim
            if regime == 'BULL':
                # Faworyzujemy (zakładamy że nasdaq to growth)
                score += 0.7
                # Bonus za sektor
                if sector_trend > 0: score += 0.3
            elif regime == 'BEAR':
                # Defensive logic
                # W bessie AQM jest surowsze, więc bazowy score niższy, chyba że fundamenty strong
                score += 0.3 
                if sector_trend > 0.5: score += 0.2 # Tylko super silne sektory
            else: # VOLATILE
                score += 0.5
                if sector_trend > 0: score += 0.4
            
            return min(1.0, score) # Max 1.0

        df['mrs'] = df.apply(calc_mrs_row, axis=1)

        # === 4. TEMPORAL COHERENCE SCORE (TCS) - 10% wagi ===
        # Earnings i Seasonality
        
        def calc_tcs_row(row):
            score = 1.0
            # Jeśli mamy dane o earnings (dynamiczne days_to)
            # W backteście trudno o historyczne 'days_to_earnings' dla każdego dnia.
            # Używamy parametru 'earnings_days_to' przekazanego z zewnątrz (tylko dla ostatniego dnia/live)
            
            # W backteście historycznym TCS jest trudny do odtworzenia bez historycznej bazy dat wyników.
            # Zgodnie z PDF: Unikaj 5 dni przed.
            # Przyjmujemy neutralne 1.0 dla historii, chyba że znamy datę.
            return score

        df['tcs'] = df.apply(calc_tcs_row, axis=1)
        
        # === FINAL AQM SCORE ===
        # Wzór: QPS * VES * MRS * TCS
        
        df['aqm_score'] = df['qps'] * df['ves'] * df['mrs'] * df['tcs']
        
        return df[['open', 'high', 'low', 'close', 'volume', 'atr', 'aqm_score', 'qps', 'ves', 'mrs', 'tcs']].dropna()

    except Exception as e:
        logger.error(f"Błąd obliczania AQM Full Vector: {e}", exc_info=True)
        return pd.DataFrame()
