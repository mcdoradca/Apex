import os
import time
import schedule
import logging
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import text, insert, update, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import bindparam
from typing import List, Optional, Dict, Any # Dodano Dict, Any

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

# === PRZYWRÓCONA FUNKCJA POMOCNICZA Z JSON.DUMPS (JAK W ORYGINALE) ===
def _update_price_cache_for_ticker(session: Session, ticker: str, quote_data: Dict[str, Any]):
    """Pomocnicza funkcja do zapisu/aktualizacji cache dla pojedynczego tickera."""
    if not ticker or not quote_data or quote_data.get("live_price") is None:
        logger.warning(f"[_update_price_cache] Invalid data provided for ticker '{ticker}'. Skipping cache update.")
        return
    try:
        data_to_cache = {
            'ticker': ticker,
            # PRZYWRÓCONO: json.dumps() musi być tutaj, bo w wersji oryginalnej był zapisywany string
            'quote_data': json.dumps(quote_data),
            'last_updated': datetime.now(timezone.utc)
        }
        stmt = pg_insert(LivePriceCache).values(data_to_cache)
        update_dict = {
            LivePriceCache.quote_data: stmt.excluded.quote_data,
            LivePriceCache.last_updated: stmt.excluded.last_updated
        }
        final_stmt = stmt.on_conflict_do_update(
            index_elements=[LivePriceCache.ticker],
            set_=update_dict
        )
        session.execute(final_stmt)
        session.commit()
        logger.info(f"[_update_price_cache] Successfully updated cache for {ticker}.")
    except Exception as e:
        logger.error(f"[_update_price_cache] Error updating cache for {ticker}: {e}", exc_info=True)
        session.rollback()
# === KONIEC PRZYWRÓCONEJ FUNKCJI ===


# === NOWA FUNKCJA DLA KROKU 1 ===
def handle_high_frequency_cache(session: Session, api_client: AlphaVantageClient):
    """
    Szybka pętla (uruchamiana co ~5s) do odświeżania ceny
    tylko dla aktywnie oglądanego tickera w widoku AI.
    """
    active_ticker = None
    try:
        # 1. Sprawdź, czy jakiś ticker jest aktywnie oglądany
        # Używamy get_system_control_value, który nie robi commita
        active_ticker = utils.get_system_control_value(session, 'ai_analysis_request')

        # 2. Działaj tylko jeśli ticker jest ustawiony (nie 'NONE' i nie 'PROCESSING')
        if active_ticker and active_ticker not in ['NONE', 'PROCESSING']:
            logger.info(f"[HF-CACHE] Aktywnie oglądany ticker: {active_ticker}. Uruchamianie szybkiego odświeżania.")
            
            # 3. Pobierz najnowszą cenę
            quote_data = api_client.get_live_quote_details(active_ticker)
            
            # 4. Zaktualizuj cache
            # Używamy funkcji _update_price_cache_for_ticker, która zarządza własnym commitem/rollbackiem.
            _update_price_cache_for_ticker(session, active_ticker, quote_data)
            
    except Exception as e:
        # Logujemy błąd, ale nie zatrzymujemy głównej pętli workera
        logger.error(f"[HF-CACHE] Błąd podczas szybkiego buforowania dla {active_ticker or 'N/A'}: {e}", exc_info=False)
        # Wycofujemy, jeśli błąd wystąpił przed wywołaniem _update_price_cache_for_ticker
        session.rollback()


# --- Funkcja obsługi analizy AI na żądanie (bez zmian) ---
def handle_ai_analysis_request(session: Session):
    """Sprawdza, wykonuje analizę AI i **natychmiast zapisuje cenę do cache**."""
    ticker_to_analyze = utils.get_system_control_value(session, 'ai_analysis_request')
    if ticker_to_analyze and ticker_to_analyze not in ['NONE', 'PROCESSING']:
        logger.info(f"AI analysis request received for: {ticker_to_analyze}.")
        utils.update_system_control(session, 'ai_analysis_request', 'PROCESSING')
        session.commit()

        temp_result = {"status": "PROCESSING", "message": "Rozpoczynanie analizy przez agentów AI..."}
        stmt_temp = text("""
            INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
            VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET
            analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
        """)
        try:
            # Używamy json.dumps, bo analysis_data jest TEXT/JSONB i potrzebuje stringa
            session.execute(stmt_temp, {'ticker': ticker_to_analyze, 'data': json.dumps(temp_result)})
            session.commit()
        except Exception as e_temp:
             logger.error(f"Failed to set PROCESSING status for AI analysis ({ticker_to_analyze}): {e_temp}", exc_info=True)
             session.rollback()
             utils.update_system_control(session, 'ai_analysis_request', 'NONE')
             try: session.commit()
             except Exception as e_commit_reset: logger.error(f"Failed commit AI req status reset: {e_commit_reset}"); session.rollback()
             return

        # --- GŁÓWNA LOGIKA ---
        analysis_result_data: Optional[Dict[str, Any]] = None
        initial_quote_data: Optional[Dict[str, Any]] = None
        try:
            # Uruchamiamy analizę - oczekujemy, że zwróci dict
            analysis_result_data = ai_agents.run_ai_analysis(ticker_to_analyze, api_client)

            # Sprawdzamy, czy analiza się powiodła i czy zawiera quote_data
            if analysis_result_data and analysis_result_data.get("status") == "DONE":
                initial_quote_data = analysis_result_data.get("quote_data") # Wyciągamy quote_data z wyniku
                if initial_quote_data:
                    # **Natychmiast zapisujemy cenę do cache**
                    logger.info(f"Analysis DONE for {ticker_to_analyze}. Immediately caching fetched price data.")
                    _update_price_cache_for_ticker(session, ticker_to_analyze, initial_quote_data)
                    # UWAGA: _update_price_cache_for_ticker zarządza własnym commitem/rollbackiem
                else:
                    logger.warning(f"AI analysis result for {ticker_to_analyze} is DONE but missing 'quote_data'. Cache not updated immediately.")

                # Zapisujemy wynik analizy AI
                stmt = text("""
                    INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
                    VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET
                    analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
                """)
                # Używamy json.dumps, bo analysis_data jest TEXT/JSONB i potrzebuje stringa
                session.execute(stmt, {'ticker': ticker_to_analyze, 'data': json.dumps(analysis_result_data)})
                session.commit()
                logger.info(f"Successfully saved AI analysis for {ticker_to_analyze} (price cached).")
            elif analysis_result_data: # Jeśli status nie jest DONE, zapiszmy to co mamy (np. ERROR)
                 stmt_other = text("""
                    INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
                    VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET
                    analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
                 """)
                 session.execute(stmt_other, {'ticker': ticker_to_analyze, 'data': json.dumps(analysis_result_data)})
                 session.commit()
                 logger.warning(f"Saved AI analysis result with status '{analysis_result_data.get('status')}' for {ticker_to_analyze}.")
            else:
                 raise ValueError("ai_agents.run_ai_analysis returned None or empty data.")

        except Exception as e:
            logger.error(f"Error during AI analysis execution or saving for {ticker_to_analyze}: {e}", exc_info=True)
            # Zapisz status błędu (jeśli analiza nie zwróciła już błędu)
            if not analysis_result_data or analysis_result_data.get("status") != "ERROR":
                error_result = {"status": "ERROR", "message": f"Błąd wykonania analizy: {str(e)}", "ticker": ticker_to_analyze}
                stmt_err = text("""
                    INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
                    VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE SET
                    analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
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
             try: session.commit()
             except Exception as e_commit_final: logger.error(f"Failed commit final AI req status reset: {e_commit_final}"); session.rollback()
        # --- KONIEC GŁÓWNEJ LOGIKI ---


# --- Funkcja głównego cyklu analizy (bez zmian) ---
def run_full_analysis_cycle():
    """Uruchamia pełny cykl analizy: czyszczenie, Faza 1, Faza 2, Faza 3 (EOD)."""
    global current_state
    session = get_db_session()
    try:
        current_worker_status = utils.get_system_control_value(session, 'worker_status')
        if current_worker_status == 'RUNNING':
            logger.warning("Analysis cycle already in progress. Skipping scheduled/command run.")
            session.close()
            return

        logger.info("Starting full analysis cycle...")
        current_state = "RUNNING"
        utils.update_system_control(session, 'worker_status', 'RUNNING')
        utils.clear_scan_log(session)
        utils.append_scan_log(session, "Rozpoczynanie nowego cyklu analizy...")

        logger.info("Cleaning daily tables before new analysis cycle...")
        session.execute(text("DELETE FROM phase2_results WHERE analysis_date = CURRENT_DATE;"))
        session.execute(text("DELETE FROM phase1_candidates WHERE DATE(analysis_date) = CURRENT_DATE;"))
        session.execute(text("UPDATE trading_signals SET status = 'EXPIRED' WHERE status IN ('ACTIVE', 'TRIGGERED')"))
        session.commit()
        logger.info("Daily tables cleaned and old signals marked as expired.")
        utils.append_scan_log(session, "Wyczyszczono tabele dzienne i oznaczono stare sygnały jako EXPIRED.")

        # --- Faza 1 ---
        utils.update_system_control(session, 'current_phase', 'PHASE_1')
        utils.append_scan_log(session, "Uruchamianie Fazy 1: Skaner Momentum...")
        candidate_tickers = phase1_scanner.run_scan(session, lambda: current_state, api_client)
        if not candidate_tickers:
            logger.warning("Phase 1 found no candidates. Analysis cycle finished early.")
            utils.append_scan_log(session, "Faza 1 nie znalazła kandydatów. Zakończono cykl.")
            raise Exception("Phase 1 found no candidates.")

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
        if "Phase 1 found no candidates" not in str(e) and "Phase 2 qualified no stocks" not in str(e):
            logger.error(f"An error occurred during the analysis cycle: {e}", exc_info=True)
            try:
                utils.update_system_control(session, 'worker_status', 'ERROR')
                utils.append_scan_log(session, f"BŁĄD KRYTYCZNY CYKLU: {e}")
                session.commit()
            except Exception as e_log:
                 logger.error(f"Failed to log critical cycle error to DB: {e_log}")
                 session.rollback()
        else:
            logger.info(f"Analysis cycle ended normally: {e}")
            try:
                utils.update_system_control(session, 'worker_status', 'IDLE')
                session.commit()
            except Exception as e_idle:
                 logger.error(f"Failed to ensure IDLE status after early cycle end: {e_idle}")
                 session.rollback()

    finally:
        current_state = "IDLE"
        try:
            utils.update_system_control(session, 'worker_status', 'IDLE')
            utils.update_system_control(session, 'current_phase', 'NONE')
            utils.update_system_control(session, 'scan_progress_processed', '0')
            utils.update_system_control(session, 'scan_progress_total', '0')
            session.commit()
        except Exception as e_final:
            logger.error(f"Failed to reset worker status after cycle: {e_final}")
            session.rollback()
        finally:
             session.close()


# --- PRZYWRÓCONA FUNKCJA buforowania cen (ORYGINALNA, NIEZOPTYMALIZOWANA) ---
def cache_live_prices(session: Session, api_client: AlphaVantageClient):
    """
    Pobiera listę istotnych tickerów i aktualizuje ich ceny w 'live_price_cache'.
    """
    try:
        current_worker_status = utils.get_system_control_value(session, 'worker_status')
        if current_worker_status == 'RUNNING':
            logger.debug("Regular price caching skipped: Main analysis cycle is running.")
            return
        if current_state == 'PAUSED':
            logger.debug("Regular price caching skipped: Worker is PAUSED.")
            return

    except Exception as e_status_check:
        logger.error(f"Error checking worker status before caching prices: {e_status_check}")
        pass

    logger.info("Running live price caching cycle...")
    try:
        # 1. Zbierz tickery (bez zmian)
        try: tickers_p1 = [r.ticker for r in session.query(Phase1Candidate.ticker).filter(func.date(Phase1Candidate.analysis_date) == func.current_date()).distinct()]; logger.debug(f"Tickers from Phase1: {tickers_p1}")
        except Exception as e_p1: logger.error(f"Error fetching tickers from Phase1: {e_p1}"); tickers_p1 = []
        try: tickers_p2 = [r.ticker for r in session.query(Phase2Result.ticker).filter(Phase2Result.analysis_date == func.current_date()).distinct()]; logger.debug(f"Tickers from Phase2: {tickers_p2}")
        except Exception as e_p2: logger.error(f"Error fetching tickers from Phase2: {e_p2}"); tickers_p2 = []
        try: tickers_p3 = [r.ticker for r in session.query(TradingSignal.ticker).filter(TradingSignal.status.in_(['ACTIVE', 'PENDING', 'TRIGGERED'])).distinct()]; logger.debug(f"Tickers from Phase3: {tickers_p3}")
        except Exception as e_p3: logger.error(f"Error fetching tickers from Phase3: {e_p3}"); tickers_p3 = []
        try: tickers_pf = [r.ticker for r in session.query(PortfolioHolding.ticker).distinct()]; logger.debug(f"Tickers from Portfolio: {tickers_pf}")
        except Exception as e_pf: logger.error(f"Error fetching tickers from Portfolio: {e_pf}"); tickers_pf = []
        unique_tickers_raw = list(set(tickers_p1 + tickers_p2 + tickers_p3 + tickers_pf))

        # Filtrowanie (bez zmian)
        valid_tickers = []
        for ticker in unique_tickers_raw:
            if isinstance(ticker, str) and ticker.isupper() and 1 <= len(ticker) <= 5 and ticker != "STRING": valid_tickers.append(ticker)
            elif ticker: logger.warning(f"Invalid ticker format found during cache collection: '{ticker}' (type: {type(ticker)}). Skipping.")
        unique_tickers = sorted(valid_tickers)

        if not unique_tickers:
            logger.info("No valid relevant tickers found to cache prices for.")
            return

        logger.info(f"Regularly caching prices for {len(unique_tickers)} unique valid tickers: {', '.join(unique_tickers)}")

        # 2. Pobierz ceny (PRZYWRÓCONO: N zapytań zamiast BULK)
        prices_to_cache = []
        fetch_count = 0
        chunk_size = 50 # Używamy małych chunków, aby nie przeciążać AV
        for i in range(0, len(unique_tickers), chunk_size):
            chunk = unique_tickers[i:i+chunk_size]
            logger.debug(f"Fetching price chunk: {', '.join(chunk)}")
            try:
                for ticker in chunk:
                    time.sleep(0.4)
                    try:
                        # PRZYWRÓCONO: Wywołanie funkcji pojedynczego pobierania
                        quote_data = api_client.get_live_quote_details(ticker) 
                        if quote_data and quote_data.get("live_price") is not None:
                            fetch_count += 1
                            prices_to_cache.append({
                                'ticker': ticker,
                                # PRZYWRÓCONO: json.dumps() do zapisu w bazie
                                'quote_data': json.dumps(quote_data),
                                'last_updated': datetime.now(timezone.utc)
                            })
                        else: logger.warning(f"Failed to fetch valid quote_data for cache ({ticker}). API Response: {quote_data}")
                    except Exception as e_ticker: logger.error(f"Exception while fetching price for cache ({ticker}): {e_ticker}", exc_info=False)
            except Exception as e_chunk: logger.error(f"Error processing price fetch chunk: {e_chunk}", exc_info=True)

        if not prices_to_cache:
            logger.warning("No prices were successfully fetched to cache in this cycle.")
            return

        # 3. Zapisz/Zaktualizuj ceny w bazie (UPSERT - bez zmian)
        stmt = pg_insert(LivePriceCache).values(prices_to_cache)
        update_dict = { LivePriceCache.quote_data: stmt.excluded.quote_data, LivePriceCache.last_updated: stmt.excluded.last_updated }
        final_stmt = stmt.on_conflict_do_update( index_elements=[LivePriceCache.ticker], set_=update_dict )
        session.execute(final_stmt)
        session.commit()
        logger.info(f"Successfully regularly cached {fetch_count}/{len(unique_tickers)} prices using UPSERT.")

    except Exception as e:
        logger.error(f"Error during price caching cycle: {e}", exc_info=True)
        session.rollback()


# --- Główna pętla workera (MODYFIKACJA) ---
def main_loop():
    """Główna pętla workera."""
    global current_state
    logger.info("Worker starting up...")
    time.sleep(10)

    session = get_db_session()
    try:
        logger.info("Verifying database schema for Worker...")
        Base.metadata.create_all(bind=engine)
        logger.info("Database schema verified/created by Worker.")
        initialize_database_if_empty(session, api_client)
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
        session.rollback(); session.close(); sys.exit(1)
    finally: session.close()

    # Harmonogram zadań
    schedule.every().day.at(ANALYSIS_SCHEDULE_TIME_CET, "Europe/Warsaw").do(run_full_analysis_cycle)
    schedule.every(30).seconds.do(lambda: phase3_sniper.monitor_entry_triggers(get_db_session(), api_client))
    schedule.every(30).seconds.do(lambda: cache_live_prices(get_db_session(), api_client)) # Regularne cachowanie

    logger.info(f"Scheduled full analysis job set for {ANALYSIS_SCHEDULE_TIME_CET} CET daily.")
    logger.info("Real-Time Entry Trigger Monitor scheduled every 30 seconds.")
    logger.info("Live Price Caching scheduled every 30 seconds.")
    logger.info("Worker initialization complete. Entering main loop.")

    # Główna pętla
    while True:
        session = get_db_session()
        try:
            command_triggered_run, new_state = utils.check_for_commands(session, current_state)
            current_state = new_state
            if command_triggered_run:
                run_full_analysis_cycle()
                current_state_from_db = utils.get_system_control_value(session, 'worker_status')
                current_state = current_state_from_db if current_state_from_db else "IDLE"
            
            # === POCZĄTEK MODYFIKACJI KROKU 1 ===
            if current_state != "PAUSED":
                # 1. Obsługa zlecenia analizy (AI Request)
                handle_ai_analysis_request(session) # Ta funkcja zarządza własnym commitem/rollbackiem
                
                # 2. NOWA FUNKCJA: Szybkie buforowanie dla aktywnego tickera AI
                # Ta funkcja jest wywoływana co 5 sekund (zgodnie z pętlą)
                handle_high_frequency_cache(session, api_client) # Zarządza własnym commitem/rollbackiem

                # 3. Obsługa zadań cyklicznych (główna analiza, F3, Portfel)
                schedule.run_pending()
            # === KONIEC MODYFIKACJI KROKU 1 ===

            utils.report_heartbeat(session)
            session.commit() # Commit heartbeatu
        except Exception as loop_error:
            logger.error(f"Error in main worker loop: {loop_error}", exc_info=True)
            try: session.rollback()
            except Exception as rb_err: logger.error(f"Error during rollback in main loop: {rb_err}")
        finally:
             try: session.close()
             except Exception as close_err: logger.error(f"Error closing session in main loop: {close_err}")
        time.sleep(COMMAND_CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    if engine:
        try: main_loop()
        except KeyboardInterrupt: logger.info("Worker stopped manually.")
        except Exception as main_e: logger.critical(f"Critical unhandled error caused worker exit: {main_e}", exc_info=True); sys.exit(1)
    else: logger.critical("Worker cannot start: DB connection failed during init."); sys.exit(1)
