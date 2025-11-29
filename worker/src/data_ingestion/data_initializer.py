import logging
import csv
from io import StringIO
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect
import os # Wymagany do odczytu zmiennych środowiskowych

logger = logging.getLogger(__name__)

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"

def _run_schema_and_index_migration(session: Session):
    """
    Zapewnia, że schemat bazy danych i niezbędne indeksy są aktualne.
    Ta funkcja jest BEZPIECZNA i nie usuwa danych.
    """
    try:
        logger.info("Starting database schema and index migration...")
        
        engine = session.get_bind()
        inspector = inspect(engine)
        
        # === MIGRACJA TABELI 1: trading_signals (V6 Update - Expiration) ===
        if 'trading_signals' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('trading_signals')]
            
            # Apex V3 Columns
            if 'entry_zone_bottom' not in columns:
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS entry_zone_bottom NUMERIC(12, 2)"))
            if 'entry_zone_top' not in columns:
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS entry_zone_top NUMERIC(12, 2)"))
            if 'updated_at' not in columns:
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()"))

            # === APEX V5 NEW COLUMNS ===
            if 'highest_price_since_entry' not in columns:
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS highest_price_since_entry NUMERIC(12, 2)"))
            if 'is_trailing_active' not in columns:
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS is_trailing_active BOOLEAN DEFAULT FALSE"))
            if 'earnings_date' not in columns:
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS earnings_date DATE"))
            
            # === APEX V6 NEW COLUMNS (TTL) ===
            if 'expiration_date' not in columns:
                logger.warning("Migration V6: Adding column 'expiration_date' to trading_signals.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS expiration_date TIMESTAMP WITH TIME ZONE"))

        # === MIGRACJA TABELI 2: companies (V5 Update) ===
        if 'companies' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('companies')]
            if 'sector_etf' not in columns:
                logger.warning("Migration V5: Adding column 'sector_etf' to companies.")
                session.execute(text("ALTER TABLE companies ADD COLUMN IF NOT EXISTS sector_etf VARCHAR(10)"))

        # === MIGRACJA TABELI 3: phase1_candidates (V5 Update) ===
        if 'phase1_candidates' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('phase1_candidates')]
            if 'sector_ticker' not in columns:
                session.execute(text("ALTER TABLE phase1_candidates ADD COLUMN IF NOT EXISTS sector_ticker VARCHAR(10)"))
            if 'sector_trend_score' not in columns:
                session.execute(text("ALTER TABLE phase1_candidates ADD COLUMN IF NOT EXISTS sector_trend_score NUMERIC(5, 2)"))
            if 'days_to_earnings' not in columns:
                session.execute(text("ALTER TABLE phase1_candidates ADD COLUMN IF NOT EXISTS days_to_earnings INTEGER"))

        # === MIGRACJA INDEKSÓW ===
        index_name = 'uq_active_pending_ticker'
        session.execute(text("ALTER TABLE trading_signals DROP CONSTRAINT IF EXISTS trading_signals_ticker_key;"))
        create_index_sql = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
            ON trading_signals (ticker)
            WHERE status IN ('ACTIVE', 'PENDING');
        """)
        session.execute(create_index_sql)
        
        # === MIGRACJA TABELI 4: virtual_trades (Metryki) ===
        if 'virtual_trades' in inspector.get_table_names():
            metric_columns_to_add = [
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
                ("metric_J_threshold_2sigma", "NUMERIC(12, 6)")
            ]
            for col_name, col_type in metric_columns_to_add:
                try:
                    sql_command = f'ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}'
                    session.execute(text(sql_command))
                except Exception:
                    pass 

        session.commit()
        logger.info("Database schema migration (V6 included) completed successfully.")

    except Exception as e:
        logger.critical(f"FATAL: Error during database schema/index migration: {e}", exc_info=True)
        session.rollback()
        pass

def force_reset_simulation_data(session: Session):
    """
    !!! UWAGA: FUNKCJA DESTRUKCYJNA !!!
    Usuwa wszystkie wyniki symulacji. Teraz wymaga zmiennej środowiskowej
    'APEX_ALLOW_DATA_RESET' ustawionej na 'TRUE'.
    """
    if os.getenv("APEX_ALLOW_DATA_RESET") != "TRUE":
        logger.critical("❌❌❌ TWARDY RESET ODRZUCONY! Ustaw APEX_ALLOW_DATA_RESET=TRUE, aby kontynuować. ❌❌❌")
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
            "processed_news"       
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
    # 1. Migracja schematu
    _run_schema_and_index_migration(session)

    # === TWARDY RESET WYŁĄCZONY ===
    # force_reset_simulation_data(session) # <--- Zakomentowane, aby nie czyścić danych przy każdym starcie!
    # ==============================

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
