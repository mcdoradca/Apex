{
type: uploaded file
fileName: mcdoradca/apex/Apex-4dfc50d9f4f4e8f2b1ee4b40873ece5dd0ad9ef0/worker/src/analysis/phase0_macro_agent.py
fullContent:
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
# Importy narzędziowe
from ..analysis.utils import (
    append_scan_log, send_telegram_alert, update_system_control
)

logger = logging.getLogger(__name__)

def run_macro_analysis(session: Session, api_client: AlphaVantageClient) -> str:
    """
    Agent Makro Fazy 0 (Wersja 'No-AI').
    Pobiera wskaźniki makro dla celów logowania i świadomości sytuacyjnej.
    Logika decyzyjna AI została wyłączona - Agent działa w trybie PASS-THROUGH (zawsze RISK_ON).
    """
    logger.info("AGENT FAZY 0 (MAKRO): Pobieranie wskaźników ekonomicznych...")
    
    try:
        # === KROK 1: Pobieranie danych makro ===
        # Dane są pobierane, abyś miał podgląd sytuacji rynkowej w logach/UI.
        inflation_data = api_client.get_inflation_rate(interval='monthly')
        fed_rate_data = api_client.get_fed_funds_rate(interval='monthly')
        yield_data = api_client.get_treasury_yield(interval='monthly', maturity='10year')
        unemployment_data = api_client.get_unemployment()

        # Funkcja pomocnicza do bezpiecznego wyciągania wartości z JSON-a AV
        def get_val(data_dict):
            try:
                if not data_dict or 'data' not in data_dict: return 'N/A'
                val = data_dict['data'][0].get('value', 'N/A')
                date = data_dict['data'][0].get('date', '?')
                return f"{val}% ({date})"
            except: 
                return 'N/A'

        # Ekstrakcja wartości dla Logów
        inf_str = get_val(inflation_data)
        fed_str = get_val(fed_rate_data)
        yield_str = get_val(yield_data)
        unemp_str = get_val(unemployment_data)

        # Raport widoczny w konsoli i UI
        macro_report = (
            f"RAPORT MAKRO: "
            f"Inflacja: {inf_str} | "
            f"Stopy FED: {fed_str} | "
            f"10Y Yield: {yield_str} | "
            f"Bezrobocie: {unemp_str}"
        )
        
        logger.info(f"AGENT FAZY 0 (MAKRO): {macro_report}")
        append_scan_log(session, macro_report)

        # === KROK 2: Decyzja (Hardcoded RISK_ON) ===
        # Zgodnie z decyzją o wyłączeniu AI, Faza 0 nie blokuje handlu.
        # Ustawiamy status RISK_ON, ale zachowujemy dane w systemie.
        
        status_msg = "RISK_ON (Mode: No-AI)"
        update_system_control(session, 'macro_sentiment', f"{status_msg} | Ostatni odczyt: {datetime.now().strftime('%H:%M')}")
        
        return "RISK_ON"

    except Exception as e:
        logger.error(f"AGENT FAZY 0 (MAKRO): Błąd pobierania danych: {e}", exc_info=True)
        # Bezpiecznik: W razie błędu API, również nie blokujemy systemu.
        return "RISK_ON"

}
