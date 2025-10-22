# Centralny plik konfiguracyjny dla parametrów analitycznych Silnika.

# === FAZA 1: Parametry Skanera Impulsu (zgodnie z dokumentem optymalizacji) ===
# --- ZMODYFIKOWANO 19.10.2025 w celu poszerzenia lejka dla strategii "małych zysków" ---
class Phase1Config:
    MIN_PRICE = 1.00
    MAX_PRICE = 75.00 # Bez zmian
    MIN_VOLUME = 500000 # Bez zmian
    # ZMNIEJSZONO: z 2.0% do 1.5%, aby wychwycić wcześniejsze ruchy.
    MIN_DAY_CHANGE_PERCENT = 1.5
    # ZMNIEJSZONO: z 1.5 do 1.3, aby łapać rosnące zainteresowanie.
    MIN_VOLUME_RATIO = 1.3
    # ZWIĘKSZONO: z 0.10 do 0.12, aby dopuścić nieco bardziej zmienne akcje.
    MAX_VOLATILITY_ATR_PERCENT = 0.12  # 12%
    MIN_RELATIVE_STRENGTH = 1.5 # vs QQQ, 5-dniowa (Bez zmian na razie)

# === FAZA 2: Parametry Silnika Scoringowego (zgodnie z dokumentem optymalizacji) ===
# --- ZMODYFIKOWANO 19.10.2025 w celu poszerzenia lejka ---
class Phase2Config:
    # ZMNIEJSZONO: z 5 do 4, aby zakwalifikować więcej spółek do Fazy 3.
    MIN_APEX_SCORE_TO_QUALIFY = 4

# === FAZA 3: Parametry Agenta Snajperskiego (zgodnie z dokumentem optymalizacji) ===
# --- ZMODYFIKOWANO 19.10.2025 w celu dodania nowych setupów i celów R/R ---
class Phase3Config:
    # ZMNIEJSZONO: z 1.0 do 0.85 w celu przetestowania bardziej agresywnego podejścia.
    MIN_RISK_REWARD_RATIO = 0.85
    ATR_MULTIPLIER_FOR_SL = 1.5 # Mnożnik ATR do ustawienia Stop-Loss (Bez zmian na razie)
    # ==================================================================
    #  NOWY PARAMETR: Docelowy stosunek R/R dla setupów Breakout i EMA Bounce
    # ==================================================================
    TARGET_RR_RATIO = 1.5 # Celujemy w 1.5:1 dla szybszych zysków
    EMA_PERIOD = 9 # Okres EMA używany w setupie EMA Bounce
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
ANALYSIS_SCHEDULE_TIME_CET = "22:30"
COMMAND_CHECK_INTERVAL_SECONDS = 5
