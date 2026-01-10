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
    Moduł Taktyczny dla SDAR v2.0 (Dynamic TTL & Strict Rules).
    """

    def generate_plan(self, ticker: str, current_price: float, df_5min: pd.DataFrame, 
                      sai_score: float, spd_score: float, me_score: float) -> Optional[TacticalPlan]:
        
        # 1. Oblicz ATR (Zmienność) - Nasz "Prędkościomierz"
        df = df_5min.copy()
        df['tr'] = np.maximum(df['high'] - df['low'], 
                   np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
        
        # ATR z 14 okresów (5min) to trochę mało dla TTL dziennego.
        # Lepiej użyć większej próbki lub przeskalować.
        # Przyjmijmy ATR z ostatnich 24h (288 świec 5min).
        atr_window = 288 
        atr_series = df['tr'].rolling(window=atr_window).mean()
        
        if atr_series.empty: return None
        atr = atr_series.iloc[-1]
        
        if np.isnan(atr) or atr == 0: return None 

        # Szacowana dzienna zmienność (Daily Range)
        # Zakładamy, że ATR(24h) na 5min to średnia zmienność świecy. 
        # Zmienność dzienna to w przybliżeniu suma ruchów lub ATR dzienny.
        # Uproszczenie: Daily Move ~ 30 * ATR_5min (dla aktywnych sesji)
        daily_volatility = atr * 30 

        # 2. Volume Node (POC) - Wsparcie Instytucjonalne
        last_day = df.iloc[-288:] 
        volume_profile = last_day.groupby('close')['volume'].sum()
        poc_price = volume_profile.idxmax() if not volume_profile.empty else current_price

        local_min = last_day['low'].min()
        local_max = last_day['high'].max()

        action = "WAIT"
        entry, sl, tp = 0.0, 0.0, 0.0
        ttl_factor = 1.0 # Mnożnik czasu (Fishing trwa dłużej)

        # === STRATEGIA A: ŁOWIENIE (Fishing / Silent Mode) ===
        # Wymagamy teraz BARDZO silnej akumulacji (SAI >= 60), żeby w ogóle o tym myśleć.
        if sai_score >= 60 and spd_score < 50:
            entry = poc_price
            
            # Jeśli cena jest daleko, ustawiamy limit i czekamy cierpliwie
            if current_price > entry + (2.0 * atr):
                action = "BUY_LIMIT"
                comment = "Sniper: Waiting for Pullback to POC"
                ttl_factor = 2.0 # Dajemy mu więcej czasu na powrót
            else:
                # Jesteśmy w strefie
                entry = current_price
                action = "MARKET_BUY"
                comment = "Silent Accumulation Zone"
                ttl_factor = 1.2

            # SL ciasny, pod dołkiem
            sl = local_min - (1.5 * atr)
            # TP ambitne (górna banda + bonus za kompresję)
            tp = local_max + (2.0 * atr)

        # === STRATEGIA B: WYBICIE (Breakout / Loud Mode) ===
        # Tylko przy potężnym sentymencie (SPD >= 70)
        elif spd_score >= 70:
            entry = local_max + (0.5 * atr) # Kupujemy dopiero jak przebije z impetem
            action = "BUY_STOP"
            comment = "Momentum Breakout (News Driven)"
            ttl_factor = 0.8 # Wybicie musi być szybkie, albo jest fałszywe
            
            sl = current_price - (2.0 * atr)
            tp = entry + (4.0 * (entry - sl)) # Celujemy w księżyc

        else:
            # Odsiewamy "przeciętniaków"
            return TacticalPlan("SKIP", 0.0, 0.0, 0.0, 0.0, 0, "Weak Edge")

        # 3. Walidacja Ryzyka (Matematyka Zysku)
        risk = entry - sl
        reward = tp - entry
        
        if risk <= 0.0001: return None 
        
        rr_ratio = reward / risk
        
        # ZAOLSTRZENIE: Dla strategii Fishing (gdzie łapiemy spadający nóż) chcemy R:R min 2.5
        min_rr = 2.5 if "Fishing" in comment else 2.0
        
        if rr_ratio < min_rr:
            return TacticalPlan("SKIP", entry, sl, tp, rr_ratio, 0, f"Low R:R ({rr_ratio:.2f} < {min_rr})")

        # === 4. DYNAMICZNY TTL (FIZYKA RUCHU) ===
        # Jak daleko mamy do celu?
        distance_to_target = abs(tp - entry)
        
        # Ile dni zajmie pokonanie tego dystansu przy obecnej zmienności?
        if daily_volatility > 0:
            estimated_days = distance_to_target / daily_volatility
        else:
            estimated_days = 5.0 # Fallback
            
        # Aplikujemy czynnik strategii i bufor
        final_ttl = int(np.ceil(estimated_days * ttl_factor * 1.5))
        
        # Bezpieczniki (nie mniej niż 2 dni, nie więcej niż 14)
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
