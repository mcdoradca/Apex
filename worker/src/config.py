# Centralny plik konfiguracyjny dla parametrów analitycznych Silnika.

# === FAZA 1: Parametry Skanera Impulsu (zgodnie z dokumentem optymalizacji) ===
class Phase1Config:
    MIN_PRICE = 1.00
    MAX_PRICE = 75.00
    MIN_VOLUME = 1000000
    MIN_DAY_CHANGE_PERCENT = 3.0
    MIN_VOLUME_RATIO = 2.0
    MAX_VOLATILITY_ATR_PERCENT = 0.10  # 10%
    MIN_RELATIVE_STRENGTH = 1.5 # vs QQQ, 5-dniowa

# === FAZA 2: Parametry Silnika Scoringowego (zgodnie z dokumentem optymalizacji) ===
class Phase2Config:
    MIN_APEX_SCORE_TO_QUALIFY = 5

# === FAZA 3: Parametry Agenta Snajperskiego (zgodnie z dokumentem optymalizacji) ===
class Phase3Config:
    MIN_RISK_REWARD_RATIO = 1.0
    ATR_MULTIPLIER_FOR_SL = 1.5 # Mnożnik ATR do ustawienia Stop-Loss

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
