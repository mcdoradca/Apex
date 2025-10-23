# === POPRAWKA BŁĘDU #5: Usunięcie klienta AV z serwisu API ===
#
# Ten plik jest celowo opróżniany (lub usuwany).
# Serwis API nie powinien mieć bezpośredniego dostępu do klucza API Alpha Vantage.
# Cała logika pobierania danych została przeniesiona do serwisu 'worker'.
# Serwis 'api' będzie teraz czytał ceny z tabeli 'live_price_cache' w bazie danych.
#
# Proszę usunąć ten plik z katalogu api/src/ po wdrożeniu wszystkich zmian.

import logging
logger = logging.getLogger(__name__)
logger.warning("Plik 'api/src/alpha_vantage_client.py' jest przestarzały i powinien zostać usunięty. Serwis API nie powinien już kontaktować się bezpośrednio z Alpha Vantage.")

# Pusta klasa, aby uniknąć błędów importu przed refaktoryzacją
class AlphaVantageClient:
    def __init__(self, *args, **kwargs):
        logger.error("Próba utworzenia instancji przestarzałego AlphaVantageClient w serwisie API!")
        raise DeprecationWarning("AlphaVantageClient został usunięty z serwisu API.")

    def __getattr__(self, name):
        # Ta metoda przechwytuje próby wywołania nieistniejących metod
        logger.error(f"Próba wywołania metody '{name}' na przestarzałym AlphaVantageClient w API!")
        raise DeprecationWarning(f"Metoda '{name}' została usunięta z AlphaVantageClient w serwisie API.")

# Można też usunąć całą zawartość i zostawić tylko komentarz

