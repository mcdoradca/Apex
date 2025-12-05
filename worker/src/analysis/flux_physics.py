import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ==================================================================================
# APEX FLUX PHYSICS (V5 CORE MATH + ORDER FLOW)
# ==================================================================================

def calculate_ofp(bid_size: float, ask_size: float) -> float:
    """
    Oblicza Order Flow Pressure (OFP) na podstawie wielkości zleceń.
    Zwraca wartość z zakresu [-1.0, 1.0].
    
    > 0: Przewaga Kupujących (Bid > Ask) - Popyt
    < 0: Przewaga Sprzedających (Ask > Bid) - Podaż
    0: Równowaga
    """
    try:
        total_size = bid_size + ask_size
        if total_size <= 0:
            return 0.0
        
        # Wzór: (Bid - Ask) / (Bid + Ask)
        # Jeśli Bid=1000, Ask=200 -> (800 / 1200) = +0.66 (Silne Kupno)
        # Jeśli Bid=100, Ask=900 -> (-800 / 1000) = -0.80 (Silna Sprzedaż)
        return (bid_size - ask_size) / total_size
    except Exception:
        return 0.0

def calculate_flux_vectors(
    intraday_df: pd.DataFrame, 
    daily_df: pd.DataFrame = None,
    current_ofp: Optional[float] = None
) -> Dict[str, Any]:
    """
    Oblicza wektory Flux (Przepływu) dla strategii Intraday V5.
    Teraz uwzględnia również Order Flow Pressure (OFP) jeśli dostępne.
    """
    metrics = {
        'flux_score': 0.0,
        'elasticity': 0.0,
        'velocity': 0.0,
        'vwap_gap_percent': 0.0,
        'signal_type': 'WAIT',
        'confidence': 0.0,
        'ofp': 0.0
    }
    
    # Wymagamy minimum 50 świec do VWAP
    if intraday_df is None or intraday_df.empty or len(intraday_df) < 50:
        return metrics

    try:
        # Kopia robocza
        df = intraday_df.copy()
        
        # Konwersja na liczby
        for col in ['close', 'high', 'low', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # 1. VWAP (Lokalny - 50 świec)
        v = df['volume'].values
        tp = (df['high'] + df['low'] + df['close']) / 3
        
        df['cum_vol'] = df['volume'].rolling(window=50).sum()
        df['cum_vol_price'] = (tp * v).rolling(window=50).sum()
        df['vwap'] = df['cum_vol_price'] / df['cum_vol']
        
        current_price = df['close'].iloc[-1]
        current_vwap = df['vwap'].iloc[-1]
        
        if pd.isna(current_vwap): current_vwap = current_price
        
        # 2. Elasticity (Sprężystość)
        # Odległość od VWAP w jednostkach odchylenia standardowego (Sigma)
        std_dev = df['close'].rolling(window=50).std().iloc[-1]
        if std_dev == 0: std_dev = current_price * 0.01 
        
        elasticity = (current_price - current_vwap) / std_dev
        metrics['elasticity'] = elasticity
        metrics['vwap_gap_percent'] = ((current_price - current_vwap) / current_vwap) * 100

        # 3. Velocity (Prędkość Wolumenu)
        current_vol = df['volume'].iloc[-1]
        avg_vol = df['volume'].rolling(window=20).mean().shift(1).iloc[-1]
        
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

        # 5. OFP Integration
        if current_ofp is not None:
            metrics['ofp'] = current_ofp

        # === LOGIKA DECYZYJNA (Flux Scoring) ===
        score = 0.0
        sig_type = "WAIT"
        
        # A. FLUX BREAKOUT (Wybicie z Momentum)
        if 0.5 < elasticity < 2.5: 
            if velocity > 1.8:
                if 50 < current_rsi < 80:
                    base = 70
                    vol_bonus = min(20, (velocity - 1.8) * 10)
                    score = base + vol_bonus
                    sig_type = "FLUX_BREAKOUT"
        
        # B. FLUX DIP BUY (Kupno w Korekcie)
        elif -2.5 < elasticity < -1.0:
            if velocity < 0.8: 
                if current_rsi < 40:
                    base = 65
                    rsi_bonus = min(15, (40 - current_rsi))
                    score = base + rsi_bonus
                    sig_type = "FLUX_DIP_BUY"
        
        # C. FLUX MOMENTUM
        elif elasticity > 2.5:
            if velocity > 3.0:
                score = 60 
                sig_type = "FLUX_MOMENTUM"

        # === MODYFIKACJA OFP (Order Flow Pressure) ===
        # Jeśli mamy dane OFP, wpływają one na ostateczny Score
        if current_ofp is not None:
            # Pozytywne OFP (Bid > Ask) wspiera Longa
            if current_ofp > 0.3:
                score += 10 # Silne wsparcie popytu
            elif current_ofp > 0.1:
                score += 5  # Umiarkowane wsparcie
            # Negatywne OFP (Ask > Bid) zabija Longa
            elif current_ofp < -0.3:
                score -= 20 # Silna ściana podaży (blokuje wzrost)
                sig_type = "OFP_BLOCKED"
            elif current_ofp < -0.1:
                score -= 10

        metrics['flux_score'] = min(100.0, max(0.0, score))
        metrics['signal_type'] = sig_type
        metrics['confidence'] = score / 100.0

        return metrics

    except Exception as e:
        logger.error(f"Flux Physics Error: {e}")
        return metrics
