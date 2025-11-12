# Centralny plik konfiguracyjny dla parametrów analitycznych Silnika.

# ==================================================================
# === DEKONSTRUKCJA (KROK 4) ===
# Stare klasy konfiguracyjne (Phase1Config, Phase2Config, Phase3Config)
# zostały usunięte, ponieważ stara logika została wygaszona.
# Nowe filtry Fazy 1 są wbudowane bezpośrednio w `phase1_scanner.py`.
# ==================================================================

# class Phase1Config:
    # ... (USUNIĘTE) ...

# class Phase2Config:
    # ... (USUNIĘTE) ...

# class Phase3Config:
    # ... (USUNIĘTE) ...

# ==================================================================


# === Mapowanie Sektorów na ETFy ===
# (Nadal używane przez `backtest_engine.py`)
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
DEFAULT_MARKET_ETF = "QQQ" # (Nadal używane przez backtest_engine)

# === Konfiguracja Workera ===
# (Nadal używane przez `worker/src/main.py`)
ANALYSIS_SCHEDULE_TIME_CET = "02:30"
COMMAND_CHECK_INTERVAL_SECONDS = 5
