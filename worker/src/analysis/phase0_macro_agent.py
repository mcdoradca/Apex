import logging
from sqlalchemy.orm import Session
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
# Importujemy "mózg" Agenta Makro (z Kroku C)
from ..analysis.ai_agents import _run_macro_analysis_agent
# Importujemy alerty
from ..analysis.utils import (
    append_scan_log, send_telegram_alert, update_system_control
)

logger = logging.getLogger(__name__)

def run_macro_analysis(session: Session, api_client: AlphaVantageClient) -> str:
    """
    Uruchamia Agenta Makro Fazy 0.
    Pobiera kluczowe wskaźniki makro, wysyła je do analizy AI (Gemini)
    i zwraca ostateczny sentyment rynkowy ('RISK_ON' lub 'RISK_OFF').
    """
    logger.info("AGENT FAZY 0 (MAKRO): Rozpoczynanie analizy...")
    
    try:
        # === KROK 1: Pobieranie danych makro (Krok B) ===
        # Używamy funkcji dodanych do alpha_vantage_client.py
        
        # Pobieramy najnowsze dane (limit 1, Alpha Vantage zwróci najnowsze)
        cpi_data = api_client.get_cpi(interval='monthly')
        fed_rate_data = api_client.get_fed_funds_rate(interval='monthly')
        yield_data = api_client.get_treasury_yield(interval='monthly', maturity='10year')
        unemployment_data = api_client.get_unemployment()

        # Sprawdzenie, czy wszystkie dane dotarły
        if not all([cpi_data, fed_rate_data, yield_data, unemployment_data]):
            logger.error("AGENT FAZY 0 (MAKRO): Błąd krytyczny. Nie udało się pobrać jednego lub więcej wskaźników makro. Skanowanie domyślnie dozwolone (RISK_ON).")
            # Bezpiecznik: Jeśli Alpha Vantage zawiedzie, domyślnie przepuszczamy skanowanie.
            return "RISK_ON"

        # === KROK 2: Trening i Decyzja AI (Krok C) ===
        # Wysyłamy surowe dane do "mózgu" AI, który wytrenowaliśmy w ai_agents.py
        
        logger.info("AGENT FAZY 0 (MAKRO): Dane pobrane. Wysyłanie do analizy AI (Gemini)...")
        analysis = _run_macro_analysis_agent(
            cpi=cpi_data,
            fed_rate=fed_rate_data,
            yield_10y=yield_data,
            unemployment=unemployment_data
        )

        # === KROK 3: Reakcja na decyzję AI ===
        sentiment = analysis.get('sentiment', 'RISK_ON') # Domyślnie RISK_ON w razie błędu Gemini
        reason = analysis.get('reason', 'Brak szczegółowego powodu.')

        if sentiment == 'RISK_OFF':
            # Sytuacja niebezpieczna - blokujemy skanowanie
            alert_msg = f"🛡️ FAZA 0: TRYB RISK-OFF 🛡️\nSkanowanie EOD wstrzymane.\nPowód: {reason}"
            logger.warning(f"AGENT FAZY 0 (MAKRO): {alert_msg}")
            
            # Zapisz w logach UI i wyślij na Telegram
            append_scan_log(session, alert_msg)
            send_telegram_alert(alert_msg)
            
            # Zapisz globalny status makro (opcjonalne, ale przydatne)
            update_system_control(session, 'macro_sentiment', f"RISK_OFF ({datetime.now().isoformat()})")
            
            return "RISK_OFF"

        else:
            # Sytuacja sprzyjająca - kontynuujemy
            log_msg = f"Faza 0: TRYB RISK-ON. Warunki sprzyjające.\nPowód: {reason}"
            logger.info(f"AGENT FAZY 0 (MAKRO): {log_msg}")
            
            append_scan_log(session, log_msg)
            update_system_control(session, 'macro_sentiment', f"RISK_ON ({datetime.now().isoformat()})")
            
            return "RISK_ON"

    except Exception as e:
        logger.error(f"AGENT FAZY 0 (MAKRO): Nieoczekiwany błąd krytyczny: {e}", exc_info=True)
        # Bezpiecznik: W razie nieoczekiwanego błędu, domyślnie pozwól na skanowanie
        return "RISK_ON"
