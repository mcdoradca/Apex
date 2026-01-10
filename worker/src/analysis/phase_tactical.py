
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Optional

@dataclass
class TacticalPlan:
    action: str          # BUY_LIMIT, BUY_STOP, WAIT, OBSERVE
    entry_price: float
    stop_loss: float
    take_profit: float
    risk_reward: float
    comment: str

class TacticalBridge:
    """
    Moduł Taktyczny dla SDAR.
    Zamienia abstrakcyjne wyniki (SAI/SPD) na konkretne poziomy cenowe.
    Używa: ATR (Zmienność) + Volume Nodes (Skupiska Wolumenu).
    """

    def generate_plan(self, ticker: str, current_price: float, df_5min: pd.DataFrame, 
                      sai_score: float, spd_score: float, me_score: float) -> Optional[TacticalPlan]:
        
        # 1. Oblicz ATR (Zmienność) - Twój "margines błędu"
        df = df_5min.copy()
        # True Range calculation
        df['tr'] = np.maximum(df['high'] - df['low'], 
                   np.maximum(abs(df['high'] - df['close'].shift()), abs(df['low'] - df['close'].shift())))
        
        # Bierzemy ostatni dostępny ATR (14 okresów)
        atr_series = df['tr'].rolling(window=14).mean()
        if atr_series.empty:
            return None
            
        atr = atr_series.iloc[-1]
        
        if np.isnan(atr) or atr == 0:
            return None # Brak danych do wyceny ryzyka

        # 2. Znajdź "Volume Node" (Cena, gdzie przemieliło się najwięcej akcji w ostatnich 24h)
        # To jest potencjalne wsparcie instytucjonalne (Tam gdzie "Gruby" kupował)
        last_day = df.iloc[-288:] # Ostatnie ~24h (przy świecach 5min: 12h sesji * 24 świece/h)
        if last_day.empty:
            return None
            
        volume_profile = last_day.groupby('close')['volume'].sum()
        if volume_profile.empty:
            poc_price = current_price
        else:
            poc_price = volume_profile.idxmax() # Point of Control (Cena z max wolumenem)

        # 3. Znajdź lokalne ekstrema (Struktura Rynku)
        local_min = last_day['low'].min()
        local_max = last_day['high'].max()

        # === STRATEGIA A: ŁOWIENIE (Fishing) - Dominacja SAI ===
        # Scenariusz: Instytucje akumulują. My chcemy kupić tanio, razem z nimi.
        if sai_score >= 40 and spd_score < 60:
            # Entry: Na poziomie POC (tam gdzie jest wolumen) lub lekko pod rynkiem
            entry = poc_price
            
            # Jeśli obecna cena jest dużo wyższa (>1.5 ATR) od POC, to czekamy na korektę
            if current_price > entry + (1.5 * atr):
                action = "BUY_LIMIT"
                comment = "Fishing at Volume Node (POC)"
            else:
                # Jesteśmy blisko, można brać
                entry = current_price
                action = "MARKET_BUY" # Lub agresywny limit
                comment = "Accumulation Zone Entry"

            # Stop Loss: Poniżej lokalnego dołka o 1 ATR (żeby szum nas nie wyrzucił)
            sl = local_min - (1.0 * atr)
            
            # Cel: Górna banda konsolidacji (Range Play)
            tp = local_max 

        # === STRATEGIA B: WYBICIE (Breakout) - Dominacja SPD ===
        # Scenariusz: Rynek jest sprężyną. Czekamy na eksplozję.
        elif spd_score >= 60:
            # Entry: Powyżej lokalnego szczytu (potwierdzenie siły)
            entry = local_max + (0.2 * atr) 
            action = "BUY_STOP"
            comment = "Momentum Breakout (Resilience)"
            
            # Stop Loss: W połowie drogi (nie chcemy, żeby cena wróciła do bazy)
            sl = current_price - (2.0 * atr)
            
            # Cel: Niebo (Agresywny TP - podążanie za trendem)
            tp = entry + (3.0 * (entry - sl))

        # === STRATEGIA C: NEUTRALNA / PUŁAPKA ===
        else:
            return TacticalPlan("WAIT", 0.0, 0.0, 0.0, 0.0, "No clear tactical edge")

        # 4. Walidacja Ryzyka (Risk Management Guard)
        # Czy zysk jest przynajmniej 1.5x - 2x większy niż ryzyko?
        risk = entry - sl
        reward = tp - entry
        
        if risk <= 0: 
            return None # Błąd logiczny
        
        rr_ratio = reward / risk
        
        # Expert Filter: Odrzucamy słabe układy (Zgodnie z PDF: R:R min 2.0)
        # Wcześniej było 1.5, co naruszało zasady "Matematyki Zysku"
        if rr_ratio < 2.0:
            # FIX: Rzutowanie na float() naprawia błąd "schema np does not exist"
            return TacticalPlan("SKIP", float(round(entry, 2)), float(round(sl, 2)), float(round(tp, 2)), float(round(rr_ratio, 2)), f"Low R:R ({rr_ratio:.2f})")

        # FIX: Rzutowanie na float() naprawia błąd "schema np does not exist"
        return TacticalPlan(action, float(round(entry, 2)), float(round(sl, 2)), float(round(tp, 2)), float(round(rr_ratio, 2)), comment)
