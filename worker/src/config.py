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


# === Mapowanie Sektorów na ETFy (APEX V5 UPDATE) ===
# Rozszerzona lista mapowania sektorów Alpha Vantage na fundusze SPDR (State Street).
# Używane przez Strażnika Sektora do oceny kondycji rynku.
SECTOR_TO_ETF_MAP = {
    "Technology": "XLK",
    "Life Sciences": "XLV",
    "Healthcare": "XLV",
    "Health Care": "XLV",
    "Financials": "XLF",
    "Financial Services": "XLF",
    "Consumer Discretionary": "XLY",
    "Consumer Cyclical": "XLY", 
    "Communication Services": "XLC",
    "Industrials": "XLI",
    "Consumer Staples": "XLP",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Materials": "XLB",
    "Basic Materials": "XLB"
}

# Domyślny ETF rynkowy (Benchmark), gdy sektor jest nieznany
DEFAULT_MARKET_ETF = "QQQ" 

# === Konfiguracja Workera ===
ANALYSIS_SCHEDULE_TIME_CET = "02:30"
COMMAND_CHECK_INTERVAL_SECONDS = 5
