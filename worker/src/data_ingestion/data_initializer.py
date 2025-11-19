import logging
import csv
from io import StringIO
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

logger = logging.getLogger(__name__)

# URL do oficjalnego pliku z listą instrumentów notowanych na NASDAQ
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
        
        # === MIGRACJA TABELI 1: trading_signals ===
        if 'trading_signals' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('trading_signals')]
            
            if 'entry_zone_bottom' not in columns:
                logger.warning("Migration needed: Adding column 'entry_zone_bottom'.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS entry_zone_bottom NUMERIC(12, 2)"))
            if 'entry_zone_top' not in columns:
                logger.warning("Migration needed: Adding column 'entry_zone_top'.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS entry_zone_top NUMERIC(12, 2)"))
            if 'updated_at' not in columns:
                logger.warning("Migration needed: Adding column 'updated_at' to 'trading_signals' table.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()"))

        # === MIGRACJA TABELI 2: Stworzenie indeksu ===
        index_name = 'uq_active_pending_ticker'
        session.execute(text("ALTER TABLE trading_signals DROP CONSTRAINT IF EXISTS trading_signals_ticker_key;"))
        create_index_sql = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
            ON trading_signals (ticker)
            WHERE status IN ('ACTIVE', 'PENDING');
        """)
        session.execute(create_index_sql)
        
        # === MIGRACJA TABELI 3: virtual_trades (Metryki) ===
        if 'virtual_trades' in inspector.get_table_names():
            metric_columns_to_add = [
                ("metric_atr_14", "NUMERIC(12, 6)"),
                # H1
                ("metric_time_dilation", "NUMERIC(12, 6)"),
                ("metric_price_gravity", "NUMERIC(12, 6)"),
                ("metric_td_percentile_90", "NUMERIC(12, 6)"),
                ("metric_pg_percentile_90", "NUMERIC(12, 6)"),
                # H2
                ("metric_inst_sync", "NUMERIC(12, 6)"),
                ("metric_retail_herding", "NUMERIC(12, 6)"),
                # H3
                ("metric_aqm_score_h3", "NUMERIC(12, 6)"),
                ("metric_aqm_percentile_95", "NUMERIC(12, 6)"),
                ("metric_J_norm", "NUMERIC(12, 6)"),
                ("metric_nabla_sq_norm", "NUMERIC(12, 6)"),
                ("metric_m_sq_norm", "NUMERIC(12, 6)"),
                # H4
                ("metric_J", "NUMERIC(12, 6)"),
                ("metric_J_threshold_2sigma", "NUMERIC(12, 6)")
            ]
            
            for col_name, col_type in metric_columns_to_add:
                try:
                    sql_command = f'ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}'
                    session.execute(text(sql_command))
                except Exception:
                    pass # Ignoruj błędy, jeśli kolumna już istnieje

        session.commit()
        logger.info("Schema migration completed successfully.")

    except Exception as e:
        logger.critical(f"FATAL: Error during database schema/index migration: {e}", exc_info=True)
        session.rollback()
        raise

def initialize_database_if_empty(session: Session, api_client):
    """
    Inicjalizuje bazę danych.
    ZMIANA: Usunięto wszelkie funkcje czyszczące (DELETE).
    Dane historyczne są teraz BEZPIECZNE i trwałe.
    """
    # 1. Migracja schematu (bezpieczna)
    _run_schema_and_index_migration(session)

    # 2. Sprawdzenie i seedowanie firm (bezpieczne - uruchamia się tylko raz na pustej bazie)
    try:
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
