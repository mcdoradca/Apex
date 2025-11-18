import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone, timedelta # Dodano timedelta
from dotenv import load_dotenv
from sqlalchemy import text, select, func

from .models import Base
from .database import get_db_session, engine

# KROK 1: Importujemy nasz nowy monitor
from .analysis import (
    phase1_scanner, 
    phase2_engine, # Import POZOSTAJE (na razie) na potrzeby `ai_agents`
    phase3_sniper, 
    ai_agents, 
    utils,
    news_agent, # <-- ZMIANA: Import nowego Agenta (Kategoria 2)
    phase0_macro_agent, # <-- POPRAWKA: Import Fazy 0
    virtual_agent, # <-- KROK 4 (Wirtualny Agent): Import nowego modułu
    backtest_engine, # <-- NOWY IMPORT (Krok 2 - Backtest)
    ai_optimizer, # <-- NOWY IMPORT (Krok 5 - Mega Agent)
    h3_deep_dive_agent # <-- KROK 2: NOWY IMPORT (H3 Deep Dive)
)
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .data_ingestion.data_initializer import initialize_database_if_empty

# USUNIĘTO: Zmienna TICKERS_PER_BATCH nie jest już potrzebna
# USUNIĘTO: Zmienna catalyst_monitor_running nie jest już potrzebna

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not API_KEY:
    logger.critical("ALPHAVANTAGE_API_KEY environment variable not set. Exiting.")
    sys.exit(1)

current_state = "IDLE"
api_client = AlphaVantageClient(api_key=API_KEY)


# ==================================================================
# === DEKONSTRUKCJA (KROK 7) ===
# Cała funkcja `handle_ai_analysis_request` została usunięta,
# ponieważ była powiązana z wygaszoną funkcją analizy na żądanie.
# ==================================================================
# def handle_ai_analysis_request(session):
# ... (kod usunięty) ...
# ==================================================================


# ==================================================================
# === NOWA FUNKCJA (Krok 2 - Backtest) ===
# ==================================================================
def handle_backtest_request(session, api_client) -> str:
    """
    Sprawdza i wykonuje nowe zlecenie backtestu historycznego.
    Zwraca 'BUSY', jeśli backtest jest w toku, lub 'IDLE', jeśli nie.
    """
    # ZMIANA (Dynamiczny Rok): Ta zmienna będzie teraz zawierać rok, np. "2010"
    period_to_test = utils.get_system_control_value(session, 'backtest_request') 
    
    if period_to_test and period_to_test not in ['NONE', 'PROCESSING']:
        logger.warning(f"Zlecenie Backtestu Historycznego otrzymane dla: {period_to_test}.")
        
        # ==================================================================
        # === POPRAWKA (TimeoutError): Ustawienie globalnej blokady ===
        # ==================================================================
        # Zablokuj workera na czas testu
        utils.update_system_control(session, 'worker_status', 'BUSY_BACKTEST') # <-- NOWY STATUS
        # ==================================================================
        
        utils.update_system_control(session, 'current_phase', 'BACKTESTING')
        utils.update_system_control(session, 'backtest_request', 'PROCESSING')
        utils.append_scan_log(session, f"Rozpoczynanie Backtestu Historycznego dla '{period_to_test}'...")

        try:
            # Uruchom silnik backtestu (to jest operacja blokująca)
            # ZMIANA (Dynamiczny Rok): Przekazujemy rok (np. "2010") do silnika
            backtest_engine.run_historical_backtest(session, api_client, period_to_test) 
            
            logger.info(f"Backtest Historyczny dla {period_to_test} zakończony pomyślnie.")
            utils.append_scan_log(session, f"Backtest Historyczny dla '{period_to_test}' zakończony.")
        except Exception as e:
            logger.error(f"Krytyczny błąd podczas Backtestu Historycznego dla {period_to_test}: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD KRYTYCZNY Backtestu: {e}")
        finally:
            # Zawsze resetuj flagi po zakończeniu (nawet po błędzie)
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'backtest_request', 'NONE')
            return 'IDLE' # Właśnie skończyliśmy

    elif period_to_test == 'PROCESSING':
        return 'BUSY' # Backtest wciąż działa
        
    return 'IDLE' # Brak zlecenia
# ==================================================================

# ==================================================================
# === NOWA FUNKCJA (Krok 5 - Mega Agent) ===
# ==================================================================
def handle_ai_optimizer_request(session) -> str:
    """
    Sprawdza i wykonuje nowe zlecenie analizy Mega Agenta AI.
    Zwraca 'BUSY', jeśli analiza jest w toku, lub 'IDLE', jeśli nie.
    """
    request_status = utils.get_system_control_value(session, 'ai_optimizer_request') 
    
    if request_status and request_status == 'REQUESTED':
        logger.warning("🤖 Zlecenie Mega Agenta AI otrzymane. Rozpoczynanie...")
        
        # ==================================================================
        # === POPRAWKA (TimeoutError): Ustawienie globalnej blokady ===
        # ==================================================================
        # Zablokuj workera na czas analizy
        utils.update_system_control(session, 'worker_status', 'BUSY_AI_OPTIMIZER') # <-- NOWY STATUS
        # ==================================================================
        
        utils.update_system_control(session, 'current_phase', 'AI_OPTIMIZING')
        utils.update_system_control(session, 'ai_optimizer_request', 'PROCESSING')
        utils.append_scan_log(session, "Rozpoczynanie analizy przez Mega Agenta AI...")

        try:
            # Uruchom silnik Mega Agenta (to jest operacja blokująca)
            ai_optimizer.run_ai_optimization_analysis(session)
            
            logger.info("🤖 Analiza Mega Agenta AI zakończona pomyślnie.")
            utils.append_scan_log(session, "🤖 Analiza Mega Agenta AI zakończona.")
        except Exception as e:
            logger.error(f"Krytyczny błąd podczas analizy Mega Agenta AI: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD KRYTYCZNY Mega Agenta: {e}")
            utils.update_system_control(session, 'ai_optimizer_report', f"BŁĄD KRYTYCZNY: {e}")
        finally:
            # Zawsze resetuj flagi po zakończeniu (nawet po błędzie)
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'ai_optimizer_request', 'NONE') # Ustaw na NONE, a nie PROCESSING
            return 'IDLE' # Właśnie skończyliśmy

    elif request_status == 'PROCESSING':
        return 'BUSY' # Analiza wciąż działa
        
    return 'IDLE' # Brak zlecenia
# ==================================================================

# ==================================================================
# === NOWA FUNKCJA (Krok 2 - H3 Deep Dive) ===
# ==================================================================
def handle_h3_deep_dive_request(session) -> str:
    """
    Sprawdza i wykonuje nowe zlecenie analizy H3 Deep Dive dla danego roku.
    Zwraca 'BUSY', jeśli analiza jest w toku, lub 'IDLE', jeśli nie.
    """
    # Wartość flagi będzie rokiem, np. "2023"
    year_to_analyze_str = utils.get_system_control_value(session, 'h3_deep_dive_request') 
    
    if year_to_analyze_str and year_to_analyze_str not in ['NONE', 'PROCESSING']:
        try:
            # Walidacja, czy to jest rok (liczba)
            year_to_analyze = int(year_to_analyze_str)
            logger.warning(f"Zlecenie H3 Deep Dive otrzymane dla roku: {year_to_analyze}.")
        except ValueError:
            logger.error(f"Otrzymano nieprawidłową wartość dla H3 Deep Dive: {year_to_analyze_str}. Oczekiwano roku.")
            utils.update_system_control(session, 'h3_deep_dive_report', f"BŁĄD: Otrzymano nieprawidłowy rok {year_to_analyze_str}")
            utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')
            return 'IDLE'

        # Zablokuj workera na czas analizy
        utils.update_system_control(session, 'worker_status', 'BUSY_DEEP_DIVE')
        utils.update_system_control(session, 'current_phase', 'DEEP_DIVE_H3')
        utils.update_system_control(session, 'h3_deep_dive_request', 'PROCESSING')
        utils.append_scan_log(session, f"Rozpoczynanie analizy H3 Deep Dive dla roku '{year_to_analyze}'...")

        try:
            # Uruchom agenta analitycznego (to jest operacja blokująca)
            h3_deep_dive_agent.run_h3_deep_dive_analysis(session, year_to_analyze)
            
            logger.info(f"Analiza H3 Deep Dive dla {year_to_analyze} zakończona pomyślie.")
            utils.append_scan_log(session, f"Analiza H3 Deep Dive dla '{year_to_analyze}' zakończona.")
        except Exception as e:
            logger.error(f"Krytyczny błąd podczas analizy H3 Deep Dive dla {year_to_analyze}: {e}", exc_info=True)
            utils.append_scan_log(session, f"BŁĄD KRYTYCZNY H3 Deep Dive: {e}")
            utils.update_system_control(session, 'h3_deep_dive_report', f"BŁĄD KRYTYCZNY: {e}")
        finally:
            # Zawsze resetuj flagi po zakończeniu (nawet po błędzie)
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'h3_deep_dive_request', 'NONE')
            return 'IDLE' # Właśnie skończyliśmy

    elif year_to_analyze_str == 'PROCESSING':
        return 'BUSY' # Analiza wciąż działa
        
    return 'IDLE' # Brak zlecenia
# ==================================================================


def run_full_analysis_cycle():
    global current_state

    # ==================================================================
    # POPRAWKA (Problem "Spamu 1600 Alertów")
    # ==================================================================
    # Czyścimy pamięć alertów Telegrama na początku każdego cyklu EOD.
    # Robimy to *przed* otwarciem sesji, ponieważ funkcja nie wymaga DB.
    utils.clear_alert_memory_cache()
    logger.info("Telegram alert memory cache cleared for the new 24h cycle.")
    # ==================================================================
    
    session = get_db_session()
    try:
        # ==================================================================
        # === POPRAWKA (TimeoutError) ===
        # Sprawdź, czy inne zadanie (Backtest/AI Optimizer) nie blokuje workera
        # ==================================================================
        worker_status = utils.get_system_control_value(session, 'worker_status')
        if worker_status not in ['IDLE', 'ERROR']: # Pozwól na uruchomienie tylko jeśli jest IDLE lub ERROR
            logger.warning(f"Analysis cycle skipped because worker is busy: {worker_status}")
            session.close()
            return
        # ==================================================================
        
        logger.info("Cleaning tables and expiring old setups before new analysis cycle...")

        # ==================================================================
        # NOWA POPRAWKA: Implementacja 7-dniowej "daty ważności"
        # ==================================================================
        # Unieważnij wszystkie sygnały PENDING starsze niż 7 dni
        stmt_expire_old = text("""
            UPDATE trading_signals
            SET status = 'INVALIDATED',
                notes = 'Setup unieważniony (przedawniony). Wygasł po 7 dniach.',
                updated_at = NOW()
            WHERE status = 'PENDING'
            AND generation_date < NOW() - INTERVAL '7 days';
        """)
        expire_result = session.execute(stmt_expire_old)
        if expire_result.rowcount > 0:
             logger.info(f"Expired {expire_result.rowcount} old PENDING setups (older than 7 days).")
        # ==================================================================

        
        # Czyścimy only przestarzałe dane Fazy 1 i Fazy 2
        session.execute(text("DELETE FROM phase2_results WHERE analysis_date < CURRENT_DATE - INTERVAL '1 day';"))
        session.execute(text("DELETE FROM phase1_candidates WHERE analysis_date < CURRENT_DATE - INTERVAL '1 day';"))
        # Czyścimy stare wiadomości, aby umożliwić ponowną analizę
        session.execute(text("DELETE FROM processed_news WHERE processed_at < NOW() - INTERVAL '3 days';"))
        
        # Usuń stare sygnały (starsze niż 3 dni), które są już nieaktywne (w tym te, które właśnie unieważniliśmy)
        # ZMIANA: Czyścimy wszystko co NIE JEST PENDING/ACTIVE i jest starsze niż 3 dni
        session.execute(text("""
            DELETE FROM trading_signals 
            WHERE status NOT IN ('ACTIVE', 'PENDING') 
            AND generation_date < NOW() - INTERVAL '3 days';
        """))
        
        session.commit()
        logger.info("Daily tables cleaned and old setups expired. Proceeding with analysis.")
    except Exception as e:
        logger.error(f"Could not clean tables before run: {e}", exc_info=True)
        session.rollback()
 
    # Ta funkcja sprawdzała status "RUNNING", ale teraz worker_status na górze
    # robi to lepiej, więc ta kontrola jest (prawie) zbędna, ale ją zostawiamy.
    if utils.get_system_control_value(session, 'worker_status') == 'RUNNING':
        logger.warning("Analysis cycle already in progress. Skipping scheduled run.")
        session.close()
        return

    try:
        # ==================================================================
        # POPRAWKA 1 (Problem 2): Uruchomienie Agenta Fazy 0 (Makro)
        # ==================================================================
        logger.info("Starting Phase 0: Macro Agent...")
        utils.update_system_control(session, 'current_phase', 'PHASE_0')
        utils.append_scan_log(session, "Faza 0: Uruchamianie Agenta Makro...")
        
        macro_sentiment = phase0_macro_agent.run_macro_analysis(session, api_client)
        
        if macro_sentiment == 'RISK_OFF':
            logger.warning("Phase 0 returned RISK_OFF. Halting full analysis cycle.")
            utils.append_scan_log(session, "Faza 0: RISK_OFF. Skanowanie EOD wstrzymane.")
            # Zakończ cykl, ale ustaw status na IDLE (to nie jest błąd)
            current_state = "IDLE"
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            session.close()
            return
        
        logger.info("Phase 0 returned RISK_ON. Proceeding with scan.")
        utils.append_scan_log(session, "Faza 0: RISK_ON. Warunki sprzyjające, kontynuacja skanowania.")
        # ==================================================================
        # Koniec Poprawki 1
        # ==================================================================

        # ==================================================================
        # === NAPRAWA (Krok 10) ===
        # Usunięto cały blok logiczny "Strażnika Rynku", który sprawdzał
        # `market_status` i `is_eod_window`. Skanowanie EOD (Faza 1)
        # będzie teraz uruchamiane zawsze, niezależnie od statusu rynku,
        # ponieważ opiera się na danych `get_daily_adjusted`.
        # ==================================================================
        # logger.info("Checking market status before starting Phase 1 scan...")
        # market_info = utils.get_market_status_and_time(api_client)
        # ... (cały blok `if market_status not in ...` został usunięty) ...
        # ==================================================================


        logger.info("Starting full analysis cycle...")
        current_state = "RUNNING"
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.update_system_control(session, 'scan_log', '')
        
        # ==================================================================
        # === DEKONSTRUKCJA (KROK 2B) ===
        # Logika Fazy 3 (Sygnały) jest teraz całkowicie wygaszona.
        # ==================================================================
        # logger.info("Trwałe sygnały Fazy 3 są aktywne (nie wygasają co noc).")
        # ==================================================================
        
        utils.append_scan_log(session, "Rozpoczynanie nowego cyklu analizy...")
        
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            raise Exception("Phase 1 found no candidates. Halting cycle.")

        # ==================================================================
        # === DEKONSTRUKCJA (KROK 11) ===
        # Fizycznie usunęliśmy wywołania Fazy 2 i Fazy 3, ponieważ
        # nowa logika Fazy 1 jest jedyną wymaganą.
        # ==================================================================
        
        logger.info("DEKONSTRUKCJA: Cykl EOD zatrzymany po Fazie 1 (zgodnie z planem).")
        utils.append_scan_log(session, "Faza 1 zakończona. Faza 2 i 3 są wyłączone.")

        # utils.update_system_control(session, 'current_phase', 'PHASE_2')
        # qualified_data = phase2_engine.run_analysis(session, candidate_tickers, lambda: current_state, api_client)
        # if not qualified_data:
        #     raise Exception("Phase 2 qualified no stocks. Halting cycle.")

        # utils.update_system_control(session, 'current_phase', 'PHASE_3')
        # phase3_sniper.run_tactical_planning(session, qualified_data, lambda: current_state, api_client)

        # ==================================================================
        
        utils.append_scan_log(session, "Cykl analizy zakończony pomyślnie.")
    except Exception as e:
        logger.error(f"An error occurred during the analysis: {e}", exc_info=True)
        utils.update_system_control(session, 'worker_status', 'ERROR')
        utils.append_scan_log(session, f"BŁĄD KRYTYCZNY: {e}")
    finally:
        current_state = "IDLE"
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'current_phase', 'NONE')
        utils.update_system_control(session, 'scan_progress_processed', '0')
        utils.update_system_control(session, 'scan_progress_total', '0')
        session.close()


# ==================================================================
# KROK 3 (KAT. 2): Usunięcie starej funkcji 'run_catalyst_monitor_job'
# ==================================================================
# USUNIĘTO: def run_catalyst_monitor_job(): ...


def main_loop():
    global current_state, api_client
    logger.info("Worker started. Initializing...")
    
    with get_db_session() as session:
        logger.info("Verifying database tables for Worker...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified.")

        initialize_database_if_empty(session, api_client)
        
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    
    # ==================================================================
    # === DEKONSTRUKCJA (KROK 2B) ===
    # Monitory Fazy 3 są teraz wygaszone, ale zostawiamy je w harmonogramie
    # (schedule), aby nie powodować dalszych błędów. Ich funkcje
    # po prostu natychmiast zwrócą `return`.
    # ==================================================================
    schedule.every(10).seconds.do(lambda: phase3_sniper.monitor_entry_triggers(get_db_session(), api_client))
    schedule.every(15).minutes.do(lambda: phase3_sniper.monitor_fib_confirmations(get_db_session(), api_client))
    # ==================================================================

    # ==================================================================
    # KROK 3 (KAT. 2): Aktywacja nowego "Ultra Agenta Newsowego"
    # ==================================================================
    # POPRAWKA 3 (Problem 1: Częstotliwość): Uruchamiamy agenta newsowego co 2 minuty (było 5)
    schedule.every(2).minutes.do(lambda: news_agent.run_news_agent_cycle(get_db_session(), api_client))
    
    # ==================================================================
    # KROK 4 (Wirtualny Agent): Aktywacja dobowego monitora agenta
    # ==================================================================
    # Uruchamiamy o 23:00 CET, po zamknięciu rynku, ale przed głównym skanem
    schedule.every().day.at("23:00", "Europe/Warsaw").do(lambda: virtual_agent.run_virtual_trade_monitor(get_db_session(), api_client))
    # ==================================================================
    
    logger.info(f"Scheduled job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Real-Time Entry Trigger Monitor scheduled every 10 seconds (NOW DEACTIVATED).")
    logger.info("H1 Fib Confirmation Monitor scheduled every 15 minutes (NOW DEACTIVATED).")
    logger.info("Ultra News Agent (Kategoria 2) scheduled every 2 minutes.")
    logger.info("🤖 Virtual Agent Monitor scheduled every day at 23:00 CET.") # <-- NOWY LOG


    with get_db_session() as initial_session:
        
        # ==================================================================
        # === KROK 2 (REWOLUCJA): "TWARDY RESET" (OPCJA 2) ===
        # Ten blok kodu został USUNIĘTY w tej "czystej" wersji.
        # ==================================================================
        
        
        utils.update_system_control(initial_session, 'worker_status', 'IDLE')
        utils.update_system_control(initial_session, 'worker_command', 'NONE')
        # ==================================================================
        # === DEKONSTRUKCJA (KROK 7) ===
        # Usunięto flagę `ai_analysis_request`
        # ==================================================================
        # utils.update_system_control(initial_session, 'ai_analysis_request', 'NONE')
        # ==================================================================
        utils.update_system_control(initial_session, 'current_phase', 'NONE')
        utils.update_system_control(initial_session, 'system_alert', 'NONE')
        utils.update_system_control(initial_session, 'backtest_request', 'NONE') # <-- NOWA WARTOŚĆ (Krok 2)
        # ==================================================================
        # === NOWA WARTOŚĆ (Krok 5 - Mega Agent) ===
        # ==================================================================
        utils.update_system_control(initial_session, 'ai_optimizer_request', 'NONE')
        utils.update_system_control(initial_session, 'ai_optimizer_report', 'NONE') # Upewnij się, że raport też jest czysty
        # ==================================================================
        # === NOWA WARTOŚĆ (Krok 2 - H3 Deep Dive) ===
        # ==================================================================
        utils.update_system_control(initial_session, 'h3_deep_dive_request', 'NONE')
        utils.update_system_control(initial_session, 'h3_deep_dive_report', 'NONE')
        # ==================================================================
        utils.report_heartbeat(initial_session)

    while True:
        with get_db_session() as session:
            try:
                # ==================================================================
                # === NOWA LOGIKA PĘTLI GŁÓWNEJ (Krok 5 - Mega Agent) ===
                # === POPRAWKA (TimeoutError) ===
                # ==================================================================
                
                # Krok 1: Sprawdź komendy ręczne (Start/Stop)
                command_triggered_run, new_state = utils.check_for_commands(session, current_state)
                current_state = new_state
                
                # Krok 2: Sprawdź zlecenia o wysokim priorytecie (blokujące)
                # Te funkcje teraz same ustawiają status 'BUSY'
                backtest_status = handle_backtest_request(session, api_client)
                optimizer_status = handle_ai_optimizer_request(session)
                deep_dive_status = handle_h3_deep_dive_request(session) # <-- KROK 2: NOWE WYWOŁANIE
                
                # Pobierz aktualny status (mógł zostać zmieniony przez funkcje powyżej)
                worker_status = utils.get_system_control_value(session, 'worker_status')

                # Krok 3: Sprawdź globalną blokadę
                # Jeśli trwa backtest LUB analiza AI LUB system jest zapauzowany,
                # nie rób nic innego, tylko raportuj heartbeat i śpij.
                if worker_status.startswith('BUSY_') or current_state == 'PAUSED':
                    utils.report_heartbeat(session) 
                    time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)
                    continue # Pomiń resztę pętli

                # Krok 4: Jeśli system jest wolny (IDLE/RUNNING), uruchom normalne operacje
                if command_triggered_run:
                    # Uruchomiono ręcznie pełny cykl EOD
                    run_full_analysis_cycle()
                
                # ==================================================================
                # === DEKONSTRUKCJA (KROK 7) ===
                # Usunięto wywołanie `handle_ai_analysis_request`
                # ==================================================================
                # handle_ai_analysis_request(session)
                # ==================================================================
                schedule.run_pending()
                
                utils.report_heartbeat(session) 
                # ==================================================================
                # === KONIEC NOWEJ LOGIKI PĘTLI GŁÓWNEJ ===
                # ==================================================================
            except Exception as loop_error:
                logger.error(f"Error in main worker loop: {loop_error}", exc_info=True)
        
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        main_loop()
    else:
        logger.critical("Could not connect to database on startup. Worker exiting.")
        sys.exit(1)
