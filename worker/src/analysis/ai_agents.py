
import logging

logger = logging.getLogger(__name__)

# ==================================================================
# MODUŁ AI (WYGASZONY / DEPRECATED)
# ==================================================================
# Decyzja projektowa: Rezygnacja z Gemini/LLM na rzecz natywnych 
# wskaźników Alpha Vantage (NLP) oraz logiki algorytmicznej.
#
# Ten plik pozostaje jako bezpieczna zaślepka (stub) dla zachowania 
# kompatybilności importów w projekcie. Nie wykonuje żadnych 
# połączeń zewnętrznych.
# ==================================================================

def _run_news_analysis_agent(ticker: str, headline: str, summary: str, url: str) -> dict:
    """
    Zaślepka dla agenta newsowego AI.
    Zawsze zwraca sentyment NEUTRAL, aby nie wpływać na decyzje.
    """
    # logger.debug(f"AI Agent (News) disabled. Skipping analysis for {ticker}.")
    return {"sentiment": "NEUTRAL", "reason": "AI Module Disabled (No-AI Mode)"}

def _run_macro_analysis_agent(inflation: dict, fed_rate: dict, yield_10y: dict, unemployment: dict) -> dict:
    """
    Zaślepka dla agenta makro.
    Zwraca RISK_ON domyślnie, aby nie blokować skanerów (Faza 0 przepuszcza).
    """
    # logger.debug("AI Agent (Macro) disabled. Defaulting to RISK_ON.")
    return {"sentiment": "RISK_ON", "reason": "AI Module Disabled (No-AI Mode)"}

# Można tu dodać inne funkcje stubowe, jeśli zajdzie potrzeba, 
# np. dla optymalizatora portfela, jeśli był oparty o LLM.
