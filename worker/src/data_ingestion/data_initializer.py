import logging
import csv
from io import StringIO
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect
import os 

logger = logging.getLogger(__name__)

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"

def _run_schema_and_index_migration(session: Session):
    """
    Zapewnia, że schemat bazy danych i niezbędne indeksy są aktualne.
    Naprawiona wersja: Wymusza dodanie brakujących kolumn poprzez bezpośrednie połączenie.
    """
    try:
        logger.info("Starting database schema and index migration (BRUTE FORCE MODE)...")
        
        engine = session.get_bind()
        
        # Funkcja pomocnicza do bezpiecznego dodawania kolumn na osobnym połączeniu
        def safe_add_column(table_name, col_name, col_type):
            try:
                with engine.connect() as conn:
                    # Sprawdzamy wewnątrz bloku connect, aby mieć świeży stan
                    inspector = inspect(engine)
                    if table_name in inspector.get_table_names():
                        current_columns = [c['name'] for c in inspector.get_columns(table_name)]
                        
                        if col_name not in current_columns:
                            logger.info(f"Migracja: Dodawanie kolumny '{col_name}' do tabeli '{table_name}'...")
                            # Wymuszamy autocommit dla DDL
                            conn.execute(text("COMMIT")) 
                            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
                            conn.execute(text("COMMIT"))
                            logger.info(f"Migracja: Sukces - dodano '{col_name}'.")
                        else:
                            pass # Kolumna już jest, wszystko ok
            except Exception as e:
                logger.error(f"Migracja BŁĄD (niekrytyczny) dla '{col_name}' w '{table_name}': {e}")

        # === 1. TRADING SIGNALS (Brakujące kolumny RE-CHECK) ===
        safe_add_column('trading_signals', 'entry_zone_bottom', 'NUMERIC(12, 2)')
        safe_add_column('trading_signals', 'entry_zone_top', 'NUMERIC(12, 2)')
        safe_add_column('trading_signals', 'updated_at', 'TIMESTAMP WITH TIME ZONE DEFAULT NOW()')
        safe_add_column('trading_signals', 'highest_price_since_entry', 'NUMERIC(12, 2)')
        safe_add_column('trading_signals', 'is_trailing_active', 'BOOLEAN DEFAULT FALSE')
        safe_add_column('trading_signals', 'earnings_date', 'DATE')
        safe_add_column('trading_signals', 'expiration_date', 'TIMESTAMP WITH TIME ZONE')
        # TE DWIE KOLUMNY POWODUJĄ BŁĄD - MUSZĄ ZOSTAĆ DODANE:
        safe_add_column('trading_signals', 'expected_profit_factor', 'NUMERIC(10, 4)')
        safe_add_column('trading_signals', 'expected_win_rate', 'NUMERIC(10, 4)')

        # === 2. COMPANIES ===
        safe_add_column('companies', 'sector_etf', 'VARCHAR(10)')

        # === 3. PHASE 1 CANDIDATES ===
        safe_add_column('phase1_candidates', 'sector_ticker', 'VARCHAR(10)')
        safe_add_column('phase1_candidates', 'sector_trend_score', 'NUMERIC(5, 2)')
        safe_add_column('phase1_candidates', 'days_to_earnings', 'INTEGER')

        # === 4. PHASE X CANDIDATES (BIOX) ===
        safe_add_column('phasex_candidates', 'last_pump_date', 'DATE')
        safe_add_column('phasex_candidates', 'last_pump_percent', 'NUMERIC(10, 2)')

        # === 5. VIRTUAL TRADES (METRYKI + RE-CHECK) ===
        metrics_cols = [
            ("metric_atr_14", "NUMERIC(12, 6)"),
            ("metric_time_dilation", "NUMERIC(12, 6)"),
            ("metric_price_gravity", "NUMERIC(12, 6)"),
            ("metric_td_percentile_90", "NUMERIC(12, 6)"),
            ("metric_pg_percentile_90", "NUMERIC(12, 6)"),
            ("metric_inst_sync", "NUMERIC(12, 6)"),
            ("metric_retail_herding", "NUMERIC(12, 6)"),
            ("metric_aqm_score_h3", "NUMERIC(12, 6)"),
            ("metric_aqm_percentile_95", "NUMERIC(12, 6)"),
            ("metric_J_norm", "NUMERIC(12, 6)"),
            ("metric_nabla_sq_norm", "NUMERIC(12, 6)"),
            ("metric_m_sq_norm", "NUMERIC(12, 6)"),
            ("metric_J", "NUMERIC(12, 6)"),
            ("metric_J_threshold_2sigma", "NUMERIC(12, 6)"),
            # Re-check columns
            ("expected_profit_factor", "NUMERIC(10, 4)"),
            ("expected_win_rate", "NUMERIC(10, 4)"),
            ("ai_audit_report", "TEXT"),
            ("ai_audit_date", "TIMESTAMP WITH TIME ZONE"),
            ("ai_optimization_suggestion", "JSONB")
        ]
        for col, type_def in metrics_cols:
            safe_add_column('virtual_trades', col, type_def)

        # === INDEKSY ===
        try:
            with engine.connect() as conn:
                 conn.execute(text("COMMIT"))
                 conn.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_active_pending_ticker
                    ON trading_signals (ticker)
                    WHERE status IN ('ACTIVE', 'PENDING');
                 """))
                 conn.execute(text("COMMIT"))
        except Exception as e:
            logger.warning(f"Indeks migration warning: {e}")

        logger.info("Database schema migration completed.")

    except Exception as e:
        logger.critical(f"FATAL: Error during database schema/index migration: {e}", exc_info=True)
        pass

def force_reset_simulation_data(session: Session):
    """
    !!! UWAGA: FUNKCJA DESTRUKCYJNA !!!
    """
    if os.getenv("APEX_ALLOW_DATA_RESET") != "TRUE":
        return
        
    logger.warning("⚠️⚠️⚠️ TWARDY RESET ZOSTAŁ WYWOŁANY PRZEZ UŻYTKOWNIKA ⚠️⚠️⚠️")
    
    try:
        tables_to_clear = [
            "optimization_trials", 
            "optimization_jobs",   
            "virtual_trades",      
            "trading_signals",     
            "phase1_candidates",   
            "phase2_results",
            "processed_news",
            "phasex_candidates"
        ]
        
        for table in tables_to_clear:
            engine = session.get_bind()
            inspector = inspect(engine)
            if table in inspector.get_table_names():
                session.execute(text(f"TRUNCATE TABLE {table} CASCADE;"))
            
        session.execute(text("UPDATE system_control SET value='0' WHERE key LIKE 'scan_progress_%'"))
        session.execute(text("UPDATE system_control SET value='NONE' WHERE key IN ('worker_command', 'optimization_request', 'backtest_request', 'h3_deep_dive_request')"))
        session.execute(text("UPDATE system_control SET value='IDLE' WHERE key='worker_status'"))
        
        session.commit()
        logger.warning("✅✅✅ TWARDY RESET ZAKOŃCZONY PRAWIDŁOWO. ✅✅✅")
        
    except Exception as e:
        logger.error(f"Błąd podczas resetu bazy: {e}", exc_info=True)
        session.rollback()

def initialize_database_if_empty(session: Session, api_client):
    """
    Inicjalizuje bazę danych przy starcie Workera.
    """
    # 1. Migracja schematu (Zawsze uruchamiana, bezpieczna)
    _run_schema_and_index_migration(session)

    # 2. Seedowanie firm (jeśli pusta)
    try:
        engine = session.get_bind()
        inspector = inspect(engine)
        if 'companies' not in inspector.get_table_names():
             return

        count_result = session.execute(text("SELECT COUNT(*) FROM companies")).scalar_one()
        if count_result > 0:
            logger.info(f"Database already seeded with {count_result} companies. No action needed.")
            return

        logger.info("Table 'companies' is empty. Initializing with official data from NASDAQ...")
        response = requests.get(NASDAQ_LISTED_URL, timeout=60)
        response.raise_for_status()

        lines = response.text.strip().split('\n')
        clean_lines = lines[:-1]
        reader = csv.DictReader(StringIO('\n'.join(clean_lines)), delimiter='|')

        companies_to_insert = []
        excluded_count = 0
        
        for row in reader:
            try:
                symbol = row.get('Symbol')
                security_name = row.get('Security Name', '').lower()
                excluded_keywords = ['warrant', 'right', 'unit', 'note', 'fund', 'etf']

                if (row.get('ETF') == 'Y' or row.get('Test Issue') == 'Y' or not symbol):
                    excluded_count += 1; continue
                if '.' in symbol or '$' in symbol or len(symbol) > 5:
                    excluded_count += 1; continue
                if any(keyword in security_name for keyword in excluded_keywords):
                    excluded_count += 1; continue
                
                companies_to_insert.append({
                    "ticker": symbol, 
                    "company_name": row.get('Security Name'), 
                    "exchange": "NASDAQ"
                })

            except Exception:
                excluded_count += 1
        
        if not companies_to_insert:
            return

        insert_stmt = text("""
            INSERT INTO companies (ticker, company_name, exchange, industry, sector)
            VALUES (:ticker, :company_name, :exchange, 'N/A', 'N/A')
            ON CONFLICT (ticker) DO NOTHING;
        """)
        session.execute(insert_stmt, companies_to_insert)
        session.commit()
        logger.info(f"Successfully inserted {len(companies_to_insert)} companies. Excluded: {excluded_count}.")

    except Exception as e:
        logger.error(f"An error occurred during data initialization: {e}", exc_info=True)
        session.rollback()
