import logging
import pandas as pd
import numpy as np
from typing import Dict, Any

logger = logging.getLogger(__name__)

# ==================================================================================
# APEX FLUX PHYSICS (V5 CORE MATH)
# ==================================================================================
# Ten moduł zawiera czystą matematykę dla Fazy 5 (Omni-Flux).
# Nie zależy od bazy danych ani API. Służy tylko do obliczeń.
# ==================================================================================

def calculate_flux_vectors(intraday_df: pd.DataFrame, daily_df: pd.DataFrame = None) -> Dict[str, Any]:
    """
    Oblicza wektory Flux (Przepływu) dla strategii Intraday V5.
    
    Kluczowe Wskaźniki:
    1. Elasticity (E): Odchylenie ceny od VWAP w jednostkach zmienności (Sigma).
    2. Velocity (V): Dynamika wolumenu (stosunek bieżącego do średniej).
    3. Momentum (M): Krótkoterminowy RSI/Trend.
    
    Zwraca słownik z surowymi metrykami i Flux Score (0-100).
    """
    metrics = {
        'flux_score': 0.0,
        'elasticity': 0.0,
        'velocity': 0.0,
        'vwap_gap_percent': 0.0,
        'signal_type': 'WAIT',
        'confidence': 0.0
    }
    
    if intraday_df is None or intraday_df.empty or len(intraday_df) < 50:
        return metrics

    try:
        # Kopia robocza, żeby nie psuć oryginału
        df = intraday_df.copy()
        
        # Konwersja na liczby (na wszelki wypadek)
        for col in ['close', 'high', 'low', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # 1. VWAP (Volume Weighted Average Price) - Lokalne okno 50 świec (Intraday Trend)
        v = df['volume'].values
        tp = (df['high'] + df['low'] + df['close']) / 3
        
        # Używamy rolling sum do symulacji VWAP w oknie
        df['cum_vol'] = df['volume'].rolling(window=50).sum()
        df['cum_vol_price'] = (tp * v).rolling(window=50).sum()
        df['vwap'] = df['cum_vol_price'] / df['cum_vol']
        
        current_price = df['close'].iloc[-1]
        current_vwap = df['vwap'].iloc[-1]
        
        if pd.isna(current_vwap): current_vwap = current_price # Fallback
        
        # 2. Elasticity (Sprężystość)
        # Obliczamy lokalne odchylenie standardowe ceny (zmienność)
        std_dev = df['close'].rolling(window=50).std().iloc[-1]
        if std_dev == 0: std_dev = current_price * 0.01 # Fallback 1%
        
        elasticity = (current_price - current_vwap) / std_dev
        metrics['elasticity'] = elasticity
        
        metrics['vwap_gap_percent'] = ((current_price - current_vwap) / current_vwap) * 100

        # 3. Velocity (Prędkość Wolumenu)
        current_vol = df['volume'].iloc[-1]
        # Średnia z poprzednich 10 świec (bez bieżącej, żeby wykryć nagły skok)
        avg_vol = df['volume'].rolling(window=10).mean().shift(1).iloc[-1]
        
        if avg_vol > 0:
            velocity = current_vol / avg_vol
        else:
            velocity = 0.0
            
        metrics['velocity'] = velocity

        # 4. Momentum (RSI 9 - szybki)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=9).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=9).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]

        # === LOGIKA DECYZYJNA (Flux Scoring) ===
        score = 0.0
        sig_type = "WAIT"
        
        # A. FLUX BREAKOUT (Wybicie z momentum)
        # Warunki: Cena nad VWAP (E > 0.5), Wolumen rośnie (V > 1.8), RSI zdrowe (50-75)
        if 0.5 < elasticity < 2.5: 
            if velocity > 1.8:
                if 50 < current_rsi < 80:
                    base = 70
                    # Bonus za wolumen (max +20 pkt)
                    vol_bonus = min(20, (velocity - 1.8) * 10)
                    score = base + vol_bonus
                    sig_type = "FLUX_BREAKOUT"
        
        # B. FLUX REVERSION (Powrót do VWAP - Dip Buy)
        # Warunki: Cena pod VWAP (E < -1.0), Wolumen maleje (V < 0.8 - sprzedaż wysycha), RSI niskie
        elif -2.5 < elasticity < -1.0:
            if velocity < 0.8: # Brak presji podażowej
                if current_rsi < 40:
                    base = 65
                    # Bonus za wyprzedanie RSI (max +15 pkt)
                    rsi_bonus = min(15, (40 - current_rsi))
                    score = base + rsi_bonus
                    sig_type = "FLUX_DIP_BUY"
        
        # C. FLUX MOMENTUM (Kontynuacja silnego trendu)
        # Warunki: Cena wysoko nad VWAP (E > 2.5), ale Wolumen wciąż ogromny (V > 3.0) - FOMO
        elif elasticity > 2.5:
            if velocity > 3.0:
                score = 60 # Ryzykowne, ale możliwe
                sig_type = "FLUX_MOMENTUM"

        metrics['flux_score'] = min(100.0, score)
        metrics['signal_type'] = sig_type
        
        # Pewność sygnału (zależy od jakości danych i siły sygnału)
        metrics['confidence'] = score / 100.0

        return metrics

    except Exception as e:
        logger.error(f"Flux Physics Error: {e}")
        return metrics
