import os
import time
import schedule
import logging
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
import json
from sqlalchemy import text

# Przenosimy logikę analizy do dedykowanego modułu, aby zachować czystość
from .analysis import on_demand_analyzer 
from .analysis import phase1_scanner, phase2_engine, phase3_sniper
from .analysis.utils import update_system_control, check_for_commands, report_heartbeat, get_system_control_value, append_scan_log
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .database import get_db_session, engine

# ... (reszta importów i konfiguracji bez zmian) ...

# Globalny stan workera
current_state = "IDLE"
api_client = AlphaVantageClient(api_key=API_KEY)

def handle_on_demand_analysis(session: Session):
    """Sprawdza i wykonuje zlecenia analizy na żądanie."""
    ticker_to_analyze = get_system_control_value(session, 'on_demand_request')
    
    if ticker_to_analyze and ticker_to_analyze != 'NONE':
        logger.info(f"On-demand request found for: {ticker_to_analyze}. Starting analysis.")
        append_scan_log(session, f"Otrzymano zlecenie analizy na żądanie dla {ticker_to_analyze}...")
        
        try:
            # Używamy tej samej logiki, którą błędnie umieściłem w API
            results = on_demand_analyzer.perform_full_analysis(ticker_to_analyze, api_client)
            
            # Zapisz wyniki w nowej tabeli
            stmt = text("""
                INSERT INTO on_demand_results (ticker, analysis_data, last_updated)
                VALUES (:ticker, :data, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
            """)
            session.execute(stmt, {'ticker': ticker_to_analyze, 'data': json.dumps(results)})
            session.commit()
            
            logger.info(f"Successfully saved on-demand analysis for {ticker_to_analyze}.")
            append_scan_log(session, f"Analiza dla {ticker_to_analyze} zakończona i zapisana.")

        except Exception as e:
            logger.error(f"Error during on-demand analysis for {ticker_to_analyze}: {e}", exc_info=True)
            # W przypadku błędu, również zapisujemy informację
            error_result = {"error": True, "message": str(e), "ticker": ticker_to_analyze}
            stmt = text("""
                INSERT INTO on_demand_results (ticker, analysis_data, last_updated)
                VALUES (:ticker, :data, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
            """)
            session.execute(stmt, {'ticker': ticker_to_analyze, 'data': json.dumps(error_result)})
            session.commit()
        
        # Oznacz zlecenie jako wykonane (lub w trakcie, jeśli frontend ma odpytywać dalej)
        # W naszym przypadku, frontend będzie odpytywał o wynik, więc nie resetujemy polecenia tutaj
        # Zostanie ono zresetowane przez API po odczytaniu wyniku.

def run_full_analysis_cycle():
    # ... (bez zmian) ...

def main_loop():
    """Główna, niekończąca się pętla sterująca pracą workera."""
    global current_state
    logger.info("Worker started. Initializing...")
    
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    logger.info(f"Scheduled job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")

    with get_db_session() as initial_session:
        update_system_control(initial_session, 'worker_status', 'IDLE')
        update_system_control(initial_session, 'worker_command', 'NONE')
        update_system_control(initial_session, 'on_demand_request', 'NONE') # Inicjalizacja
        update_system_control(initial_session, 'current_phase', 'NONE')
        report_heartbeat(initial_session)

    while True:
        with get_db_session() as session:
            try:
                # 1. Sprawdź polecenia sterujące (start/pause/resume)
                command_triggered_run, new_state = check_for_commands(session, current_state)
                current_state = new_state

                if command_triggered_run:
                    run_full_analysis_cycle()
                
                # 2. Sprawdź zlecenia analizy na żądanie
                if current_state != "PAUSED":
                    handle_on_demand_analysis(session)
                
                # 3. Uruchom zaplanowane zadania
                if current_state != "PAUSED":
                    schedule.run_pending()
                
                # 4. Zgłoś heartbeat
                report_heartbeat(session)
            except Exception as loop_error:
                logger.error(f"Error in main worker loop: {loop_error}", exc_info=True)
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        main_loop()
    else:
        logger.critical("Worker cannot start because database connection was not established.")

