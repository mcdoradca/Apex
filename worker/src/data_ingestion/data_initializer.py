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
    To jest ostateczna naprawa błędu "ON CONFLICT".
    """
    try:
        # --- Krok 1: Sprawdzenie i dodanie brakujących kolumn (jeśli istnieją) ---
        engine = session.get_bind()
        inspector = inspect(engine)
        if 'trading_signals' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('trading_signals')]
            missing_columns = []
            if 'entry_zone_bottom' not in columns:
                missing_columns.append(("entry_zone_bottom", "NUMERIC(12, 2)"))
            if 'entry_zone_top' not in columns:
                missing_columns.append(("entry_zone_top", "NUMERIC(12, 2)"))

            if missing_columns:
                logger.warning("Migration needed: Adding missing columns to trading_signals.")
                for col_name, col_type in missing_columns:
                    session.execute(text(f"ALTER TABLE trading_signals ADD COLUMN {col_name} {col_type}"))
                    logger.info(f"Successfully added column '{col_name}'.")
                session.commit()

        # --- Krok 2: OSTATECZNA NAPRAWA - Zapewnienie istnienia częściowego indeksu unikalnego ---
        index_name = 'uq_active_pending_ticker'
        logger.info(f"Attempting to create or verify partial unique index '{index_name}'...")

        # Najpierw usuńmy stare, potencjalnie konfliktowe ograniczenie unikalności, jeśli istnieje
        try:
            session.execute(text("ALTER TABLE trading_signals DROP CONSTRAINT IF EXISTS trading_signals_ticker_key;"))
            session.commit()
            logger.info("Successfully removed old unique constraint if it existed.")
        except Exception as e:
            logger.warning(f"Could not drop old constraint (this is likely fine): {e}")
            session.rollback()

        # Teraz spróbujmy stworzyć poprawny indeks.
        # Używamy `CREATE UNIQUE INDEX IF NOT EXISTS`, co jest bezpieczne.
        create_index_sql = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
            ON trading_signals (ticker)
            WHERE status IN ('ACTIVE', 'PENDING');
        """)

        session.execute(create_index_sql)
        session.commit()
        logger.info(f"Successfully created or verified the existence of index '{index_name}'.")

    except Exception as e:
        logger.critical(f"FATAL: Error during database schema/index migration: {e}", exc_info=True)
        session.rollback()


def initialize_database_if_empty(session: Session, api_client):
    """
    Sprawdza, czy tabela 'companies' jest pusta. Jeśli tak, pobiera oficjalną
    listę spółek z nasdaqtrader.com, filtruje ją i zapisuje w bazie.
    """
    # 1. ZAWSZE URUCHAMIAJ MIGRACJĘ PRZY STARCIE, ABY ZAPEWNIĆ SPÓJNOŚĆ
    _run_schema_and_index_migration(session)

    # 2. Następnie sprawdź, czy baza danych jest załadowana danymi (companies)
    try:
        count_result = session.execute(text("SELECT COUNT(*) FROM companies")).scalar_one()

        if count_result > 0:
            logger.info(f"Database already seeded. Found {count_result} companies. Skipping initialization.")
            return

        logger.info("Table 'companies' is empty. Initializing with official data from NASDAQ...")

        try:
            response = requests.get(NASDAQ_LISTED_URL, timeout=60)
            response.raise_for_status()
            logger.info("Successfully downloaded official NASDAQ listed symbols file.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch official list from nasdaqtrader.com: {e}")
            return

        lines = response.text.strip().split('\n')
        clean_lines = lines[:-1]

        csv_file = StringIO('\n'.join(clean_lines))
        reader = csv.DictReader(csv_file, delimiter='|')

        companies_to_insert = []
        for row in reader:
            ticker = row.get('Symbol')
            is_etf = row.get('ETF') == 'Y'
            is_standard_stock = (
                ticker and 1 <= len(ticker) <= 5 and
                ticker.isalpha() and ticker.isupper()
            )

            if not is_etf and is_standard_stock:
                companies_to_insert.append({
                    "ticker": ticker, "company_name": row.get('Security Name'), "exchange": "NASDAQ",
                })

        final_count = len(companies_to_insert)
        logger.info(f"Found {final_count} clean, standard common stocks to insert into the database.")

        if not companies_to_insert:
            logger.warning("No valid companies found after filtering. Halting initialization.")
            return

        insert_stmt = text("""
            INSERT INTO companies (ticker, company_name, exchange, industry, sector)
            VALUES (:ticker, :company_name, :exchange, 'N/A', 'N/A')
            ON CONFLICT (ticker) DO NOTHING;
        """)

        session.execute(insert_stmt, companies_to_insert)
        session.commit()

        logger.info(f"Successfully inserted {final_count} companies into the database.")

    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}", exc_info=True)
        session.rollback()
