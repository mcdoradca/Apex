# Centralny plik konfiguracyjny dla parametrów analitycznych Silnika.

# === FAZA 1: Parametry Skanera Impulsu ===
# Zaktualizowano zgodnie z ostateczną specyfikacją techniczną.
MIN_PRICE = 0.50
MAX_PRICE = 25.00
MIN_VOLUME = 100000
MIN_DAY_CHANGE_PERCENT = 5.0

# === FAZA 2: Mapowanie Sektorów na ETFy ===
# Używane do analizy siły względnej.
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
# Domyślny ETF, jeśli sektor nie zostanie znaleziony
DEFAULT_MARKET_ETF = "QQQ"

# === FAZA 2: Progi Scoringowe ===
MIN_APEX_SCORE_TO_QUALIFY = 7 # Minimalny łączny wynik do kwalifikacji do Fazy 3

# === FAZA 3: Parametry Agenta Snajperskiego ===
MIN_RISK_REWARD_RATIO = 1.2

# === Konfiguracja Workera ===
ANALYSIS_SCHEDULE_TIME_CET = "22:30" # Czas CET
COMMAND_CHECK_INTERVAL_SECONDS = 5 # Co ile sekund worker sprawdza polecenia

