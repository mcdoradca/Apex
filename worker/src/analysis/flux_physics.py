import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ==================================================================================
# APEX FLUX PHYSICS (V5 CORE MATH + ORDER FLOW) - SANITIZED
# ==================================================================================

def calculate_ofp(bid_size: float, ask_size: float) -> float:
    """
    Oblicza Order Flow Pressure (OFP).
    """
    try:
        # Sanityzacja wejścia
        b = float(bid_size or 0.0)
        a = float(ask_size or 0.0)
        
        total_size = b + a
        if total_size <= 0:
            return 0.0
        
        return (b - a) / total_size
    except Exception:
        return 0.0

def calculate_flux_vectors(
    intraday_df: pd.DataFrame, 
    daily_df: pd.DataFrame = None,
    current_ofp: Optional[float] = None
) -> Dict[str, Any]:
    """
    Oblicza wektory Flux. GWARANTUJE zwrócenie typów liczbowych (nie None).
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
    
    if intraday_df is None or intraday_df.empty or len(intraday_df) < 50:
        return metrics

    try:
        df = intraday_df.copy()
        
        # Konwersja na liczby
        for col in ['close', 'high', 'low', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            
        # 1. VWAP
        v = df['volume'].values
        tp = (df['high'] + df['low'] + df['close']) / 3
        
        df['cum_vol'] = df['volume'].rolling(window=50).sum()
        df['cum_vol_price'] = (tp * v).rolling(window=50).sum()
        df['vwap'] = df['cum_vol_price'] / df['cum_vol']
        
        current_price = df['close'].iloc[-1]
        current_vwap = df['vwap'].iloc[-1]
        
        if pd.isna(current_vwap) or current_vwap == 0: 
            current_vwap = current_price
        
        # 2. Elasticity
        std_dev = df['close'].rolling(window=50).std().iloc[-1]
        if pd.isna(std_dev) or std_dev == 0: 
            std_dev = current_price * 0.01 
        
        elasticity = (current_price - current_vwap) / std_dev
        
        # 3. Velocity
        current_vol = df['volume'].iloc[-1]
        avg_vol = df['volume'].rolling(window=20).mean().shift(1).iloc[-1]
        
        velocity = 0.0
        if avg_vol > 0:
            velocity = current_vol / avg_vol

        # 4. Momentum (RSI 9)
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=9).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=9).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1]
        if pd.isna(current_rsi): current_rsi = 50.0

        # 5. OFP Integration
        safe_ofp = float(current_ofp or 0.0)
        metrics['ofp'] = safe_ofp

        # === LOGIKA DECYZYJNA ===
        score = 0.0
        sig_type = "WAIT"
        
        if 0.5 < elasticity < 2.5: 
            if velocity > 1.8:
                if 50 < current_rsi < 80:
                    base = 70
                    vol_bonus = min(20, (velocity - 1.8) * 10)
                    score = base + vol_bonus
                    sig_type = "FLUX_BREAKOUT"
        
        elif -2.5 < elasticity < -1.0:
            if velocity < 0.8: 
                if current_rsi < 40:
                    base = 65
                    rsi_bonus = min(15, (40 - current_rsi))
                    score = base + rsi_bonus
                    sig_type = "FLUX_DIP_BUY"
        
        elif elasticity > 2.5:
            if velocity > 3.0:
                score = 60 
                sig_type = "FLUX_MOMENTUM"

        if safe_ofp > 0.3: score += 10
        elif safe_ofp > 0.1: score += 5
        elif safe_ofp < -0.3:
            score -= 20
            sig_type = "OFP_BLOCKED"
        elif safe_ofp < -0.1: score -= 10

        # Zapisz wyniki (zawsze float)
        metrics['flux_score'] = float(min(100.0, max(0.0, score)))
        metrics['elasticity'] = float(elasticity)
        metrics['velocity'] = float(velocity)
        metrics['signal_type'] = sig_type
        metrics['confidence'] = float(score / 100.0)
        
        # Wypełnij ewentualne NaN
        for k, v in metrics.items():
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                metrics[k] = 0.0

        return metrics

    except Exception as e:
        logger.error(f"Flux Physics Error: {e}")
        # Return neutral zeros on error
        return metrics
