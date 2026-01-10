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
    ttl_days: int        # NOWOŚĆ: Szacowany czas życia setupu
    comment: str

class TacticalBridge:
    """
    Moduł Taktyczny dla SDAR v2.2 (Real Physics TTL).
    Skalibrowany mnożnik zmienności (Volatility Scaler).
    """

    def generate_plan(self, ticker: str, current_price: float, df_5min: pd.DataFrame, 
                      sai_score: float, spd_score: float, me_score: float) -> Optional[TacticalPlan]:
        
        # 1. Oblicz ATR (Zmienność) - Nasz "Prędkościomierz"
        df = df_5min.copy()
        df['tr'] = np.maximum(df['high'] - df['low'], 
                   np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
        
        # Używamy dłuższego okna ATR (288 świec = 24h), żeby wygładzić szum intraday
        atr_window = 288 
        atr_series = df['tr'].rolling(window=atr_window).mean()
        
        if atr_series.empty: return None
        atr = atr_series.iloc[-1]
        
        if np.isnan(atr) or atr == 0: return None 

        # === FIX TTL: KALIBRACJA ZMIENNOŚCI DZIENNEJ ===
        # Wcześniej: atr * 30 (Zbyt szybko!)
        # Teraz: atr * 10 (Zgodnie z zasadą sqrt(czas): sqrt(78 świec 5min) ~= 8.8)
        daily_volatility = atr * 10 

        # 2. Znajdź "Volume Node" (POC) - Wsparcie Instytucjonalne
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
        if sai_score >= 60 and spd_score < 50:
            entry = poc_price
            
            if current_price > entry + (2.0 * atr):
                action = "BUY_LIMIT"
                comment = "Sniper: Waiting for Pullback to POC"
                ttl_factor = 2.0 
            else:
                entry = current_price
                action = "MARKET_BUY"
                comment = "Silent Accumulation Zone"
                ttl_factor = 1.2

            sl = local_min - (1.5 * atr)
            tp = local_max + (2.0 * atr)

        # === STRATEGIA B: WYBICIE (Breakout / Loud Mode) ===
        elif spd_score >= 70:
            entry = local_max + (0.5 * atr) 
            action = "BUY_STOP"
            comment = "Momentum Breakout (News Driven)"
            ttl_factor = 0.8 
            
            sl = current_price - (2.0 * atr)
            tp = entry + (4.0 * (entry - sl)) 

        else:
            return TacticalPlan("SKIP", 0.0, 0.0, 0.0, 0.0, 0, "Weak Edge")

        # === FIX 2: MINIMALNE RYZYKO (Likwidacja R:R 27.0) ===
        # Stop Loss nie może być bliżej niż 0.5% ceny wejścia lub 2 centy.
        min_risk_dist = max(entry * 0.005, 0.02)
        
        current_risk = entry - sl
        
        # Jeśli wyliczony SL jest zbyt ciasny (np. 1 cent), poszerzamy go
        if current_risk < min_risk_dist:
            sl = entry - min_risk_dist
            
        # 3. Walidacja Ryzyka (Matematyka Zysku)
        risk = entry - sl
        reward = tp - entry
        
        # Ochrona przed dzieleniem przez zero
        if risk <= 0.0001: return None
        
        rr_ratio = reward / risk
        
        min_rr = 2.5 if "Fishing" in comment or "Accumulation" in comment else 2.0
        
        if rr_ratio < min_rr:
            return TacticalPlan("SKIP", float(round(entry, 2)), float(round(sl, 2)), float(round(tp, 2)), float(round(rr_ratio, 2)), 0, f"Low R:R ({rr_ratio:.2f} < {min_rr})")

        # === 4. DYNAMICZNY TTL (FIZYKA RUCHU) ===
        distance_to_target = abs(tp - entry)
        
        if daily_volatility > 0:
            estimated_days = distance_to_target / daily_volatility
        else:
            estimated_days = 5.0
            
        final_ttl = int(np.ceil(estimated_days * ttl_factor * 1.5))
        
        # Bezpieczniki: Widełki 2 - 14 dni
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
