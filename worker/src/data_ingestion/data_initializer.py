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
    Ta funkcja działa teraz w swojej własnej, niezależnej transakcji.
    """
    try:
        logger.info("Starting database schema and index migration...")
        
        # --- Krok 1: Sprawdzenie i dodanie brakujących kolumn (jeśli istnieją) ---
        engine = session.get_bind()
        inspector = inspect(engine)
        
        if 'trading_signals' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('trading_signals')]
            
            if 'entry_zone_bottom' not in columns:
                logger.warning("Migration needed: Adding column 'entry_zone_bottom'.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN entry_zone_bottom NUMERIC(12, 2)"))
            if 'entry_zone_top' not in columns:
                logger.warning("Migration needed: Adding column 'entry_zone_top'.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN entry_zone_top NUMERIC(12, 2)"))
                
            # ==================================================================
            # KROK 4 (Migracja): Dodanie brakującej kolumny "updated_at"
            # To jest polecenie, którego brak powoduje wszystkie błędy.
            # ==================================================================
            if 'updated_at' not in columns:
                logger.warning("Migration needed: Adding column 'updated_at' to 'trading_signals' table.")
                # Dodajemy kolumnę z domyślną wartością NOW(), aby uniknąć problemów z istniejącymi wierszami
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()"))
                logger.info("Successfully added 'updated_at' column.")
            # ==================================================================

        # --- Krok 2: OSTATECZNA NAPRAWA - Zapewnienie istnienia częściowego indeksu unikalnego ---
        index_name = 'uq_active_pending_ticker'
        logger.info(f"Attempting to create or verify partial unique index '{index_name}'...")
        
        # Usunięcie starego, potencjalnie konfliktowego ograniczenia
        session.execute(text("ALTER TABLE trading_signals DROP CONSTRAINT IF EXISTS trading_signals_ticker_key;"))
        
        # Stworzenie poprawnego indeksu
        create_index_sql = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
            ON trading_signals (ticker)
            WHERE status IN ('ACTIVE', 'PENDING');
        """)
        session.execute(create_index_sql)
        
        session.commit() # Zapisujemy zmiany w schemacie
        logger.info(f"Successfully committed schema and index migrations.")

    except Exception as e:
        logger.critical(f"FATAL: Error during database schema/index migration: {e}", exc_info=True)
        session.rollback()
        # Rzucamy błąd dalej, aby zatrzymać aplikację, jeśli migracja się nie powiedzie
        raise

def initialize_database_if_empty(session: Session, api_client):
    """
    Sprawdza, czy tabela 'companies' jest pusta i w razie potrzeby ją uzupełnia.
    Migracja schematu jest teraz wywoływana osobno.
    
    NOWA LOGIKA: Ta funkcja będzie teraz *zawsze* czyścić i przeładowywać
    tabelę 'companies' przy starcie workera, aby zapewnić czystość danych.
    """
    # 1. ZAWSZE URUCHAMIAJ MIGRACJĘ PRZY STARCIE W OSOBNEJ TRANSAKCJI
    _run_schema_and_index_migration(session)

    # 2. Teraz, w nowej transakcji, sprawdź i uzupełnij dane
    try:
        # ==================================================================
        # === NOWA LOGIKA: WYMUSZONE CZYSZCZENIE (ZGODNIE Z PANA PROŚBĄ) ===
        # ==================================================================
        # Usuwamy starą "brudną" listę 4133 spółek przy każdym starcie.
        # CASCADE zapewnia, że stare wyniki Fazy 1/Fazy 2 również zostaną usunięte.
        logger.warning("Forcing TRUNCATE on 'companies' table to ensure clean data...")
        session.execute(text("TRUNCATE TABLE companies RESTART IDENTITY CASCADE;"))
        session.commit()
        logger.info("Successfully truncated 'companies' table.")
        # ==================================================================

        # Sprawdzenie (głównie dla logiki) - teraz zawsze zwróci 0
        count_result = session.execute(text("SELECT COUNT(*) FROM companies")).scalar_one()
        if count_result > 0:
            # Ten kod jest teraz technicznie nieosiągalny, ale zostawiamy go dla bezpieczeństwa
            logger.info(f"Database already seeded with {count_result} companies. Skipping data initialization.")
            return

        logger.info("Table 'companies' is empty. Initializing with official data from NASDAQ...")
        response = requests.get(NASDAQ_LISTED_URL, timeout=60)
        response.raise_for_status()

        lines = response.text.strip().split('\n')
        clean_lines = lines[:-1]
        reader = csv.DictReader(StringIO('\n'.join(clean_lines)), delimiter='|')

        companies_to_insert = []
        excluded_count = 0
        
        # ==================================================================
        # === NOWY, RYGORYSTYCZNY FILTR (ZGODNIE Z PANA SUGESTIĄ) ===
        # ==================================================================
        for row in reader:
            try:
                symbol = row.get('Symbol')
                security_name = row.get('Security Name', '').lower()
                
                # Definiujemy złe słowa kluczowe, które oznaczają instrumenty inne niż akcje
                excluded_keywords = ['warrant', 'right', 'unit', 'note', 'fund', 'etf']

                # WARUNEK 1: Podstawowa walidacja (czy to nie ETF i nie Test)
                if (row.get('ETF') == 'Y' or 
                    row.get('Test Issue') == 'Y' or 
                    not symbol):
                    excluded_count += 1
                    continue
                    
                # WARUNEK 2: Walidacja symbolu (filtr .PAR, $ itp.)
                if '.' in symbol or '$' in symbol or len(symbol) > 5:
                    excluded_count += 1
                    continue
                    
                # WARUNEK 3: Walidacja nazwy (filtr Warrantów, Praw, Funduszy itp.)
                if any(keyword in security_name for keyword in excluded_keywords):
                    excluded_count += 1
                    continue
                
                # Jeśli wszystko przeszło, dodajemy spółkę
                companies_to_insert.append({
                    "ticker": symbol, 
                    "company_name": row.get('Security Name'), 
                    "exchange": "NASDAQ"
                })

            except Exception as e:
                logger.warning(f"Błąd parsowania wiersza w data_initializer: {e} | Wiersz: {row}")
                excluded_count += 1
        # ==================================================================
        
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
        logger.info(f"Successfully inserted {len(companies_to_insert)} companies into the database.")
        logger.info(f"Excluded {excluded_count} non-stock instruments (ETFs, Warrants, Tests, etc.).")

    except Exception as e:
        logger.error(f"An error occurred during data initialization (schema migration was not affected): {e}", exc_info=True)
        session.rollback()
