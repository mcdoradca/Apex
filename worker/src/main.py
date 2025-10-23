import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
# POPRAWKA BŁĘDU #5: Import potrzebny do UPSERT w cache
from sqlalchemy import text, insert, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

# POPRAWKA BŁĘDU #5 i #6: Upewniamy się, że importujemy WSZYSTKIE modele
# Dodano LivePriceCache
from .models import Base, Company, Phase1Candidate, Phase2Result, TradingSignal, PortfolioHolding, LivePriceCache
from .database import get_db_session, engine

from .analysis import phase1_scanner, phase2_engine, phase3_sniper, ai_agents, utils
from .config import ANALYSIS_SCHEDULE_TIME_CET, COMMAND_CHECK_INTERVAL_SECONDS
from .data_ingestion.alpha_vantage_client import AlphaVantageClient
from .data_ingestion.data_initializer import initialize_database_if_empty


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

if not API_KEY:
    logger.critical("ALPHAVANTAGE_API_KEY environment variable not set for Worker. Exiting.")
    sys.exit(1)

current_state = "IDLE"
api_client = AlphaVantageClient(api_key=API_KEY)

def handle_ai_analysis_request(session):
    """Sprawdza i wykonuje nową analizę AI na żądanie."""
    ticker_to_analyze = utils.get_system_control_value(session, 'ai_analysis_request')
    # Sprawdzamy, czy jest zlecenie i nie jest to 'NONE' ani 'PROCESSING'
    if ticker_to_analyze and ticker_to_analyze not in ['NONE', 'PROCESSING']:
        logger.info(f"AI analysis request received for: {ticker_to_analyze}.")
        # Oznaczamy jako przetwarzane
        utils.update_system_control(session, 'ai_analysis_request', 'PROCESSING')

        # Zapisujemy tymczasowy status 'PROCESSING' do bazy
        temp_result = {"status": "PROCESSING", "message": "Rozpoczynanie analizy przez agentów AI..."}
        stmt_temp = text("""
            INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
            VALUES (:ticker, :data::jsonb, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
                analysis_data = EXCLUDED.analysis_data,
                last_updated = NOW();
        """)
        try:
            session.execute(stmt_temp, {'ticker': ticker_to_analyze, 'data': json.dumps(temp_result)})
            session.commit()
        except Exception as e_temp:
             logger.error(f"Failed to set PROCESSING status for AI analysis ({ticker_to_analyze}): {e_temp}")
             session.rollback()
             # Resetujemy zlecenie, aby można było spróbować ponownie
             utils.update_system_control(session, 'ai_analysis_request', 'NONE')
             return # Przerywamy, jeśli nie udało się zapisać statusu

        # Uruchamiamy analizę
        try:
            results = ai_agents.run_ai_analysis(ticker_to_analyze, api_client) # Ta funkcja zwraca już dict
            # Zapisujemy wynik analizy (UPSERT)
            stmt = text("""
                INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
                VALUES (:ticker, :data::jsonb, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    analysis_data = EXCLUDED.analysis_data,
                    last_updated = NOW();
            """)
            # Używamy json.dumps do serializacji wyniku dict na string JSON dla bazy
            session.execute(stmt, {'ticker': ticker_to_analyze, 'data': json.dumps(results)})
            session.commit()
            logger.info(f"Successfully saved AI analysis for {ticker_to_analyze}.")
        except Exception as e:
            logger.error(f"Error during AI analysis for {ticker_to_analyze}: {e}", exc_info=True)
            # Zapisujemy status błędu
            error_result = {"status": "ERROR", "message": f"Błąd agenta AI: {str(e)}", "ticker": ticker_to_analyze}
            stmt_err = text("""
                INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
                VALUES (:ticker, :data::jsonb, NOW())
                ON CONFLICT (ticker) DO UPDATE SET
                    analysis_data = EXCLUDED.analysis_data,
                    last_updated = NOW();
            """)
            try:
                session.execute(stmt_err, {'ticker': ticker_to_analyze, 'data': json.dumps(error_result)})
                session.commit()
            except Exception as e_err_save:
                 logger.error(f"Failed to save ERROR status for AI analysis ({ticker_to_analyze}): {e_err_save}")
                 session.rollback()
        finally:
             # Niezależnie od wyniku, resetujemy zlecenie
             utils.update_system_control(session, 'ai_analysis_request', 'NONE')


def run_full_analysis_cycle():
    """Uruchamia pełny cykl analizy: czyszczenie, Faza 1, Faza 2, Faza 3 (EOD)."""
    global current_state
    session = get_db_session()
    try:
        # Sprawdzamy, czy inny cykl już nie trwa
        current_worker_status = utils.get_system_control_value(session, 'worker_status')
        if current_worker_status == 'RUNNING':
            logger.warning("Analysis cycle already in progress. Skipping scheduled/command run.")
            session.close()
            return

        logger.info("Starting full analysis cycle...")
        current_state = "RUNNING"
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.clear_scan_log(session) # Czyścimy logi przed nowym cyklem
        utils.append_scan_log(session, "Rozpoczynanie nowego cyklu analizy...")

        # Czyszczenie starych danych dziennych
        logger.info("Cleaning daily tables before new analysis cycle...")
        # Czyścimy tylko dla bieżącego dnia, na wypadek gdyby cykl uruchamiano wielokrotnie
        session.execute(text("DELETE FROM phase2_results WHERE analysis_date = CURRENT_DATE;"))
        session.execute(text("DELETE FROM phase1_candidates WHERE DATE(analysis_date) = CURRENT_DATE;"))
        # Oznaczanie starych sygnałów jako wygasłe
        session.execute(text("UPDATE trading_signals SET status = 'EXPIRED' WHERE status IN ('ACTIVE', 'TRIGGERED')")) # Zmieniono warunek
        session.commit()
        logger.info("Daily tables cleaned and old signals marked as expired.")
        utils.append_scan_log(session, "Wyczyszczono tabele dzienne i oznaczono stare sygnały jako EXPIRED.")

        # --- Faza 1 ---
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        utils.append_scan_log(session, "Uruchamianie Fazy 1: Skaner Momentum...")
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            # Nie rzucamy wyjątku, tylko logujemy i kończymy cykl
            logger.warning("Phase 1 found no candidates. Analysis cycle finished early.")
            utils.append_scan_log(session, "Faza 1 nie znalazła kandydatów. Zakończono cykl.")
            raise Exception("Phase 1 found no candidates.") # Rzucamy wyjątek, aby przejść do 'finally'

        # --- Faza 2 ---
        utils.update_system_control(session, 'current_phase', 'PHASE_2')
        utils.append_scan_log(session, f"Uruchamianie Fazy 2: Analiza Jakościowa dla {len(candidate_tickers)} kandydatów...")
        qualified_tickers = phase2_engine.run_analysis(session, candidate_tickers, lambda: current_state, api_client)
        if not qualified_tickers:
            logger.warning("Phase 2 qualified no stocks. Analysis cycle finished early.")
            utils.append_scan_log(session, "Faza 2 nie zakwalifikowała żadnej spółki. Zakończono cykl.")
            raise Exception("Phase 2 qualified no stocks.")

        # --- Faza 3 (EOD Scan) ---
        utils.update_system_control(session, 'current_phase', 'PHASE_3')
        utils.append_scan_log(session, f"Uruchamianie Fazy 3: Skaner Taktyczny EOD dla {len(qualified_tickers)} spółek...")
        phase3_sniper.run_tactical_planning(session, qualified_tickers, lambda: current_state, api_client)

        utils.append_scan_log(session, "Pełny cykl analizy zakończony pomyślnie.")
        logger.info("Full analysis cycle completed successfully.")

    except Exception as e:
        # Logujemy błąd i ustawiamy status ERROR, ale tylko jeśli błąd nie był "Phase 1/2 finished early"
        if "Phase 1 found no candidates" not in str(e) and "Phase 2 qualified no stocks" not in str(e):
            logger.error(f"An error occurred during the analysis cycle: {e}", exc_info=True)
            try:
                utils.update_system_control(session, 'worker_status', 'ERROR')
                utils.append_scan_log(session, f"BŁĄD KRYTYCZNY CYKLU: {e}")
                session.commit() # Commit statusu błędu
            except Exception as e_log:
                 logger.error(f"Failed to log critical cycle error to DB: {e_log}")
                 session.rollback()
        # Jeśli błąd był spodziewany (brak kandydatów), nie ustawiamy statusu ERROR
        else:
            logger.info(f"Analysis cycle ended normally: {e}") # Logujemy powód zakończenia
            # Upewniamy się, że status jest IDLE, a nie np. RUNNING
            try:
                utils.update_system_control(session, 'worker_status', 'IDLE')
                session.commit()
            except Exception as e_idle:
                 logger.error(f"Failed to ensure IDLE status after early cycle end: {e_idle}")
                 session.rollback()

    finally:
        # Zawsze resetujemy stan do IDLE na koniec cyklu (nawet po błędzie)
        current_state = "IDLE"
        try:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'scan_progress_processed', '0')
            utils.update_system_control(session, 'scan_progress_total', '0')
            session.commit() # Commit finalnego statusu
        except Exception as e_final:
            logger.error(f"Failed to reset worker status after cycle: {e_final}")
            session.rollback()
        finally:
             session.close() # Zawsze zamykamy sesję


# === POPRAWKA BŁĘDU #5: Nowa funkcja do buforowania cen ===
def cache_live_prices(session: Session, api_client: AlphaVantageClient):
    """
    Pobiera listę wszystkich istotnych tickerów (Fazy 1-3, Portfel)
    i aktualizuje ich ceny w tabeli 'live_price_cache'. Uruchamiana cyklicznie.
    """
    # Sprawdzamy, czy worker nie jest w trakcie głównego cyklu analizy
    try:
        current_worker_status = utils.get_system_control_value(session, 'worker_status')
        if current_worker_status == 'RUNNING':
            logger.debug("Price caching skipped: Main analysis cycle is running.") # Zmieniono na DEBUG
            return
        # Sprawdźmy też, czy nie jest PAUSED
        if current_state == 'PAUSED':
            logger.debug("Price caching skipped: Worker is PAUSED.")
            return

    except Exception as e_status_check:
        logger.error(f"Error checking worker status before caching prices: {e_status_check}")
        # Kontynuujemy ostrożnie, ale logujemy problem
        pass

    logger.info("Running live price caching cycle...")
    try:
        # 1. Zbierz wszystkie unikalne tickery do monitorowania
        # Używamy zapytań ORM dla bezpieczeństwa i czytelności
        tickers_p1 = [r.ticker for r in session.query(Phase1Candidate.ticker).filter(func.date(Phase1Candidate.analysis_date) == func.current_date()).distinct()]
        tickers_p2 = [r.ticker for r in session.query(Phase2Result.ticker).filter(Phase2Result.analysis_date == func.current_date()).distinct()] # Zakładamy, że data jest max date
        tickers_p3 = [r.ticker for r in session.query(TradingSignal.ticker).filter(TradingSignal.status.in_(['ACTIVE', 'PENDING', 'TRIGGERED'])).distinct()]
        tickers_pf = [r.ticker for r in session.query(PortfolioHolding.ticker).distinct()]

        # Łączymy listy i tworzymy unikalny, posortowany zbiór tickerów
        unique_tickers = sorted(list(set(tickers_p1 + tickers_p2 + tickers_p3 + tickers_pf)))

        if not unique_tickers:
            logger.info("No relevant tickers found to cache prices for.")
            return

        logger.info(f"Caching prices for {len(unique_tickers)} unique tickers...")

        # 2. Pobierz ceny dla tickerów (w małych paczkach dla API)
        prices_to_cache = []
        fetch_count = 0
        chunk_size = 50 # Pobieramy w paczkach po 50
        for i in range(0, len(unique_tickers), chunk_size):
            chunk = unique_tickers[i:i+chunk_size]
            logger.debug(f"Fetching price chunk: {', '.join(chunk)}")
            try:
                # Używamy get_bulk_quotes dla wydajności, jeśli jest dostępny i działa
                # Ale fallback na indywidualne get_live_quote_details jest bezpieczniejszy
                for ticker in chunk:
                    # Mała pauza między indywidualnymi zapytaniami
                    time.sleep(0.3) # Około 200 zapytań/minutę max, rate limiter i tak zadziała
                    try:
                        quote_data = api_client.get_live_quote_details(ticker) # Ta funkcja ma wbudowany rate limiter i fallback
                        if quote_data and quote_data.get("live_price") is not None:
                            fetch_count += 1
                            prices_to_cache.append({
                                'ticker': ticker,
                                'quote_data': json.dumps(quote_data),
                                'last_updated': datetime.now(timezone.utc)
                            })
                        else:
                            logger.warning(f"Failed to fetch valid quote_data for cache ({ticker}). API Response: {quote_data}")
                    except Exception as e_ticker:
                         logger.error(f"Exception while fetching price for cache ({ticker}): {e_ticker}", exc_info=False)
            except Exception as e_chunk:
                logger.error(f"Error processing price fetch chunk: {e_chunk}", exc_info=True)


        if not prices_to_cache:
            logger.warning("No prices were successfully fetched to cache in this cycle.")
            return

        # 3. Zapisz/Zaktualizuj ceny w bazie (UPSERT)
        # Używamy SQLAlchemy ORM + PostgreSQL dialect for UPSERT
        stmt = pg_insert(LivePriceCache).values(prices_to_cache)
        # Definiujemy, co ma się stać w przypadku konfliktu (ten sam ticker)
        update_dict = {
            # Aktualizujemy tylko te kolumny
            LivePriceCache.quote_data: stmt.excluded.quote_data,
            LivePriceCache.last_updated: stmt.excluded.last_updated
        }
        # Finalny statement UPSERT
        final_stmt = stmt.on_conflict_do_update(
            index_elements=[LivePriceCache.ticker], # Klucz konfliktu
            set_=update_dict # Co aktualizować
        )
        # Wykonujemy statement
        session.execute(final_stmt)
        session.commit()
        logger.info(f"Successfully cached {fetch_count}/{len(unique_tickers)} prices using UPSERT.")

    except Exception as e:
        logger.error(f"Error during price caching cycle: {e}", exc_info=True)
        session.rollback()


def main_loop():
    """Główna pętla workera."""
    global current_state
    logger.info("Worker starting up...")

    # Czekamy chwilę, aby dać czas bazie danych na start (szczególnie w Render)
    time.sleep(10)

    # Inicjalizacja przy starcie
    session = get_db_session()
    try:
        logger.info("Verifying database schema for Worker...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database schema verified/created by Worker.")

        initialize_database_if_empty(session, api_client)

        # Inicjalizacja statusu workera
        utils.update_system_control(session, 'worker_status', 'IDLE')
        utils.update_system_control(session, 'worker_command', 'NONE')
        utils.update_system_control(session, 'ai_analysis_request', 'NONE')
        utils.update_system_control(session, 'current_phase', 'NONE')
        utils.update_system_control(session, 'system_alert', 'NONE')
        utils.report_heartbeat(session)
        session.commit()
        logger.info("Worker status initialized to IDLE.")
    except Exception as e_init:
        logger.critical(f"FATAL: Error during worker initialization: {e_init}", exc_info=True)
        session.rollback()
        session.close()
        sys.exit(1) # Zakończ, jeśli inicjalizacja zawiedzie
    finally:
        session.close()


    # Konfiguracja harmonogramu zadań
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    schedule.every(1).minute.do(lambda: phase3_sniper.monitor_entry_triggers(get_db_session(), api_client))
    schedule.every(30).seconds.do(lambda: cache_live_prices(get_db_session(), api_client))

    logger.info(f"Scheduled full analysis job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Real-Time Entry Trigger Monitor scheduled every 1 minute.")
    logger.info("Live Price Caching scheduled every 30 seconds.")
    logger.info("Worker initialization complete. Entering main loop.")

    # Główna pętla działania
    while True:
        session = get_db_session()
        try:
            # Sprawdzanie poleceń z API
            command_triggered_run, new_state = utils.check_for_commands(session, current_state)
            current_state = new_state # Aktualizujemy stan globalny

            # Uruchomienie cyklu analizy, jeśli zażądano przez komendę
            if command_triggered_run:
                # run_full_analysis_cycle zarządza własną sesją i statusem
                run_full_analysis_cycle()
                # Po zakończeniu cyklu, odświeżamy stan globalny
                current_state = utils.get_system_control_value(session, 'worker_status') or "IDLE"

            # Wykonywanie zadań tylko jeśli worker nie jest wstrzymany
            if current_state != "PAUSED":
                # Obsługa zleceń analizy AI
                handle_ai_analysis_request(session) # Ta funkcja zarządza własną transakcją
                # Uruchomienie zaplanowanych zadań (główny cykl, monitor, cache)
                schedule.run_pending()

            # Zawsze raportuj heartbeat
            utils.report_heartbeat(session)
            session.commit() # Commit heartbeatu i potencjalnych zmian statusu

        except Exception as loop_error:
            logger.error(f"Error in main worker loop: {loop_error}", exc_info=True)
            try:
                 session.rollback() # Wycofaj zmiany z tej iteracji pętli
            except Exception as rb_err:
                 logger.error(f"Error during rollback in main loop: {rb_err}")
        finally:
             try:
                 session.close() # Zawsze zamykamy sesję na końcu iteracji
             except Exception as close_err:
                  logger.error(f"Error closing session in main loop: {close_err}")

        # Czekanie przed następną iteracją pętli
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        try:
            main_loop()
        except KeyboardInterrupt:
            logger.info("Worker stopped manually.")
        except Exception as main_e:
             logger.critical(f"Critical unhandled error caused worker exit: {main_e}", exc_info=True)
             sys.exit(1)
    else:
        logger.critical("Worker cannot start because database connection was not established during initialization.")
        sys.exit(1)

