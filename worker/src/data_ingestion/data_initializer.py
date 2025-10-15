import logging
import csv
from io import StringIO
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

logger = logging.getLogger(__name__)

# URL do oficjalnego pliku z listą instrumentów notowanych na NASDAQ
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"

def _run_schema_migration(session: Session):
    """
    Sprawdza i dodaje brakujące kolumny do tabeli trading_signals.
    To naprawia błąd "UndefinedColumn".
    """
    engine = session.get_bind()
    inspector = inspect(engine)
    
    try:
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
                    try:
                        # Użycie ALTER TABLE do dodania kolumn i ustawienie, że mogą być NULL
                        session.execute(text(f"ALTER TABLE trading_signals ADD COLUMN {col_name} {col_type}"))
                        session.commit()
                        logger.info(f"Successfully added column '{col_name}' to 'trading_signals'.")
                    except Exception as e:
                        session.rollback()
                        logger.error(f"Failed to add column {col_name}: {e}")
            else:
                logger.info("Schema migration for 'trading_signals' not needed.")
        else:
            logger.info("Table 'trading_signals' not found. Will be created by models.Base.metadata.create_all.")

    except Exception as e:
        logger.critical(f"FATAL: Error during database schema migration: {e}", exc_info=True)
        session.rollback()


def initialize_database_if_empty(session: Session, api_client):
    """
    Sprawdza, czy tabela 'companies' jest pusta. Jeśli tak, pobiera oficjalną
    listę spółek z nasdaqtrader.com, filtruje ją i zapisuje w bazie.
    """
    # 1. NAJPIERW URUCHOM MIGRACJĘ SCHEMATU DLA ISTNIEJĄCYCH TABEL (np. trading_signals)
    _run_schema_migration(session) 
    
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

        # Plik jest w formacie pipe-delimited (|), ma nagłówek i stopkę
        lines = response.text.strip().split('\n')
        
        # --- OSTATECZNA POPRAWKA ---
        # Usuwamy TYLKO ostatnią linię (stopka z datą), zostawiając nagłówek dla parsera.
        # To rozwiązuje problem niepoprawnego odczytywania struktury pliku.
        clean_lines = lines[:-1]
        # --- KONIEC POPRAWKI ---
        
        # Używamy StringIO, aby traktować listę linii jak plik
        csv_file = StringIO('\n'.join(clean_lines))
        # Używamy DictReader z separatorem '|'
        reader = csv.DictReader(csv_file, delimiter='|')
        
        companies_to_insert = []
        for row in reader:
            ticker = row.get('Symbol')
            # Dwustopniowy filtr, który przepuszcza tylko standardowe akcje
            is_etf = row.get('ETF') == 'Y'
            
            is_standard_stock = (
                ticker and
                1 <= len(ticker) <= 5 and
                ticker.isalpha() and
                ticker.isupper()
            )
            
            if not is_etf and is_standard_stock:
                companies_to_insert.append({
                    "ticker": ticker,
                    "company_name": row.get('Security Name'),
                    "exchange": "NASDAQ",
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
