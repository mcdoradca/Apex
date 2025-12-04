import logging
import pandas as pd
import numpy as np
from typing import Dict, Any

logger = logging.getLogger(__name__)

# ==================================================================================
# APEX FLUX PHYSICS (V5 CORE MATH)
# ==================================================================================

def calculate_flux_vectors(intraday_df: pd.DataFrame, daily_df: pd.DataFrame = None) -> Dict[str, Any]:
    """
    Oblicza wektory Flux (Przepływu) dla strategii Intraday V5.
    """
    metrics = {
        'flux_score': 0.0,
        'elasticity': 0.0,
        'velocity': 0.0,
        'vwap_gap_percent': 0.0,
        'signal_type': 'WAIT',
        'confidence': 0.0
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
        std_dev = df['close'].rolling(window=50).std().iloc[-1]
        if std_dev == 0: std_dev = current_price * 0.01 
        
        elasticity = (current_price - current_vwap) / std_dev
        metrics['elasticity'] = elasticity
        metrics['vwap_gap_percent'] = ((current_price - current_vwap) / current_vwap) * 100

        # 3. Velocity (Prędkość Wolumenu)
        # POPRAWKA ZGODNA ZE SPECYFIKACJĄ: Okno 20 (było 10)
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

        # === LOGIKA DECYZYJNA (Flux Scoring) ===
        score = 0.0
        sig_type = "WAIT"
        
        # A. FLUX BREAKOUT
        if 0.5 < elasticity < 2.5: 
            if velocity > 1.8:
                if 50 < current_rsi < 80:
                    base = 70
                    vol_bonus = min(20, (velocity - 1.8) * 10)
                    score = base + vol_bonus
                    sig_type = "FLUX_BREAKOUT"
        
        # B. FLUX DIP BUY
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

        metrics['flux_score'] = min(100.0, score)
        metrics['signal_type'] = sig_type
        metrics['confidence'] = score / 100.0

        return metrics

    except Exception as e:
        logger.error(f"Flux Physics Error: {e}")
        return metrics
