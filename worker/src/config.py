# Centralny plik konfiguracyjny dla parametrów analitycznych Silnika.

# ==================================================================
# === ZMIANA (NA TWOJE ŻĄDANIE): Zmiana zakresu cenowego Fazy 1 ===
# ==================================================================
class Phase1Config:
    # Zmieniono z 1.00 na 0.50
    MIN_PRICE = 0.50
    # Zmieniono z 75.00 na 40.00
    MAX_PRICE = 40.00 
    MIN_VOLUME = 500000 # Bez zmian
    # ZMNIEJSZONO: z 2.0% do 1.5%, aby wychwycić wcześniejsze ruchy.
    MIN_DAY_CHANGE_PERCENT = 1.5
    # ZMNIEJSZONO: z 1.5 do 1.3, aby łapać rosnące zainteresowanie.
    MIN_VOLUME_RATIO = 1.3
    # ZWIĘKSZONO: z 0.10 do 0.12, aby dopuścić nieco bardziej zmienne akcje.
    MAX_VOLATILITY_ATR_PERCENT = 0.12  # 12%
    MIN_RELATIVE_STRENGTH = 1.5 # vs QQQ, 5-dniowa (Bez zmian na razie)
# ==================================================================

# === FAZA 2: Parametry Silnika Scoringowego (zgodnie z dokumentem optymalizacji) ===
# --- ZMODYFIKOWANO 19.10.2025 w celu poszerzenia lejka ---
class Phase2Config:
    # ZMNIEJSZONO: z 5 do 4, aby zakwalifikować więcej spółek do Fazy 3.
    MIN_APEX_SCORE_TO_QUALIFY = 4

# ==================================================================
# === FAZA 3: NOWA STRUKTURA (Wg Sugestii Mega Agenta AI) ===
# ==================================================================
class Phase3Config:
    """
    Zawiera teraz sub-klasy dla parametrów specyficznych dla strategii,
    zgodnie z sugestiami optymalizacyjnymi Mega Agenta AI.
    """
    
    # --- Konfiguracja dla 'BREAKOUT' ---
    class Breakout:
        # SUGESTIA AI 1: Zmniejsz R/R z 1.5 do 1.1, aby zwiększyć Win Rate
        TARGET_RR_RATIO = 1.1 
        # Domyślny czas trzymania
        MAX_HOLD_DAYS = 7 
        # Mnożnik ATR do ustawienia Stop-Loss
        ATR_MULTIPLIER_FOR_SL = 1.5 

    # --- Konfiguracja dla 'EMA_BOUNCE' ---
    class EmaBounce:
        # Domyślne R/R
        TARGET_RR_RATIO = 1.5 
        # SUGESTIA AI 3: Skróć czas trzymania z 7 do 4 dni
        MAX_HOLD_DAYS = 4 
        # Mnożnik ATR do ustawienia Stop-Loss
        ATR_MULTIPLIER_FOR_SL = 1.5 
        # Okres EMA używany w setupie
        EMA_PERIOD = 9 
        # SUGESTIA AI 2: Dodatkowy filtr Fazy 3. 
        # Ignoruj odbicia, jeśli zmienność jest zbyt niska (poniżej 20% ATR).
        # To ma zredukować "over-trading" i filtrować szum.
        MIN_ATR_PERCENT_FILTER = 0.20 # 20%

    # --- Konfiguracja dla 'FIB_H1' (na razie bez zmian) ---
    class FibH1:
        # Używamy TARGET_RR_RATIO = 1.5 (z Breakout) do obliczenia TP,
        # ale strategia polega na dojściu do 'impulse_high'
        TARGET_RR_RATIO = 1.5 
        MAX_HOLD_DAYS = 7
        ATR_MULTIPLIER_FOR_SL = 1.5
# ==================================================================


# === Mapowanie Sektorów na ETFy (używane w Fazie 2) ===
SECTOR_TO_ETF_MAP = {
    "Technology": "XLK",
    "Health Care": "XLV",
    "Financials": "XLF",
    "Consumer Discretionary": "XLY",
    "Communication Services": "XLC",
    "Industrials": "XLI",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Materials": "XLB"
}
DEFAULT_MARKET_ETF = "QQQ"

# === Konfiguracja Workera ===
ANALYSIS_SCHEDULE_TIME_CET = "02:30"
COMMAND_CHECK_INTERVAL_SECONDS = 5
