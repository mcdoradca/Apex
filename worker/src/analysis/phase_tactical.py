import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

@dataclass
class TacticalPlan:
    action: str          # BUY_LIMIT, BUY_STOP, WAIT, SKIP
    entry_price: float
    stop_loss: float
    take_profit: float
    risk_reward: float
    ttl_days: int        
    comment: str

class TacticalBridge:
    """
    Moduł Taktyczny dla SDAR v2.4 (Fast Breakout Fix).
    Zmniejszony bufor wybicia dla szybszej reakcji.
    """

    def generate_plan(self, ticker: str, current_price: float, df_5min: pd.DataFrame, 
                      sai_score: float, spd_score: float, me_score: float) -> Optional[TacticalPlan]:
        
        # 1. Oblicz ATR (Zmienność)
        df = df_5min.copy()
        df['tr'] = np.maximum(df['high'] - df['low'], 
                   np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
        
        atr_window = 288 
        atr_series = df['tr'].rolling(window=atr_window).mean()
        
        if atr_series.empty: return None
        atr = atr_series.iloc[-1]
        
        if np.isnan(atr) or atr == 0: return None 

        daily_volatility = atr * 10 

        # 2. Analiza Wolumenu (POC) i Struktury
        last_day = df.iloc[-288:] 
        if last_day.empty: return None
        
        volume_profile = last_day.groupby('close')['volume'].sum()
        poc_price = volume_profile.idxmax() if not volume_profile.empty else current_price

        local_min = last_day['low'].min()
        local_max = last_day['high'].max()

        action = "WAIT"
        entry, sl, tp = 0.0, 0.0, 0.0
        ttl_factor = 1.0 

        # === STRATEGIA A: ŁOWIENIE (Fishing / Silent Mode) ===
        # Tutaj wchodzimy "na dołku" lub w korekcie - tu opóźnienie nie jest problemem
        if sai_score >= 60 and spd_score < 50:
            entry = poc_price
            
            # Jeśli cena jest daleko od bazy, czekamy na limit
            if current_price > entry + (2.0 * atr):
                action = "BUY_LIMIT"
                comment = "Sniper: Waiting for Pullback"
                ttl_factor = 2.0 
            else:
                # Jesteśmy w strefie - wchodzimy z rynku
                entry = current_price
                action = "MARKET_BUY"
                comment = "Silent Accumulation Zone"
                ttl_factor = 1.2

            sl = local_min - (1.5 * atr)
            tp = local_max + (2.0 * atr)

        # === STRATEGIA B: WYBICIE (Breakout / Loud Mode) ===
        # FIX: "Lateness" - Zmniejszamy bufor z 0.5 ATR na 0.1 ATR
        elif spd_score >= 70:
            # Wcześniej: entry = local_max + (0.5 * atr) -> ZBYT WOLNE!
            # Teraz: Wchodzimy tuż po przebiciu szczytu (+ mały szum)
            entry = local_max + (0.1 * atr) 
            
            action = "BUY_STOP"
            comment = "Momentum Breakout (Fast)"
            ttl_factor = 0.8 
            
            # SL musi być teraz liczony od ENTRY, a nie od Current Price
            # Skoro wchodzimy na szczycie, SL dajemy pod ostatnią korektą (np. -1.5 ATR)
            sl = entry - (1.5 * atr)
            
            # TP dynamiczne: Celujemy w zasięg 3-4x ryzyko
            tp = entry + (4.0 * (entry - sl)) 

        else:
            return TacticalPlan("SKIP", 0.0, 0.0, 0.0, 0.0, 0, "Weak Edge")

        # === FIX: MINIMALNE RYZYKO (RR Guard) ===
        min_risk_dist = max(entry * 0.005, 0.02)
        current_risk = entry - sl
        
        if current_risk < min_risk_dist:
            sl = entry - min_risk_dist
            
        # 3. Walidacja Ryzyka
        risk = entry - sl
        reward = tp - entry
        
        if risk <= 0.0001: return None 
        
        rr_ratio = reward / risk
        min_rr = 2.5 if "Fishing" in comment else 2.0
        
        if rr_ratio < min_rr:
            return TacticalPlan("SKIP", float(round(entry, 2)), float(round(sl, 2)), float(round(tp, 2)), float(round(rr_ratio, 2)), 0, f"Low R:R ({rr_ratio:.2f} < {min_rr})")

        # 4. TTL Calculation
        distance_to_target = abs(tp - entry)
        if daily_volatility > 0:
            estimated_days = distance_to_target / daily_volatility
        else:
            estimated_days = 5.0
            
        final_ttl = int(np.ceil(estimated_days * ttl_factor * 1.5))
        final_ttl = max(2, min(14, final_ttl))

        return TacticalPlan(
            action=action, 
            entry_price=float(round(entry, 2)), 
            stop_loss=float(round(sl, 2)), 
            take_profit=float(round(tp, 2)), 
            risk_reward=float(round(rr_ratio, 2)), 
            ttl_days=final_ttl,
            comment=comment
        )
