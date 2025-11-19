import logging
import csv
from io import StringIO
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

logger = logging.getLogger(__name__)

# URL do oficjalnego pliku z listą instrumentów notowanych na NASDAQ
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"

# ==================================================================
# === NOWA FUNKCJA: Selektywne czyszczenie strategii H1, H2, H4 ===
# ==================================================================
def _clean_legacy_strategies(session: Session):
    """
    Usuwa z bazy danych wyniki strategii H1, H2 i H4, pozostawiając H3.
    Uruchamiana przy starcie systemu.
    """
    logger.info("🧹 CLEANUP: Rozpoczynanie selektywnego usuwania strategii H1, H2, H4...")
    
    strategies_to_remove = [
        '%AQM_V3_H1_GRAVITY_MEAN_REVERSION%',      # H1
        '%AQM_V3_H2_CONTRARIAN_ENTANGLEMENT%',     # H2
        '%AQM_V3_H4_INFO_THERMO%'                  # H4
    ]
    
    total_deleted = 0
    try:
        for pattern in strategies_to_remove:
            # Używamy LIKE, aby dopasować format "BACKTEST_2023_AQM_..."
            stmt = text("DELETE FROM virtual_trades WHERE setup_type LIKE :pattern")
            result = session.execute(stmt, {'pattern': pattern})
            if result.rowcount > 0:
                logger.info(f"   > Usunięto {result.rowcount} wierszy dla wzorca: {pattern}")
                total_deleted += result.rowcount
        
        if total_deleted > 0:
            session.commit()
            logger.info(f"🧹 CLEANUP: Pomyślnie usunięto łącznie {total_deleted} starych transakcji.")
        else:
            logger.info("🧹 CLEANUP: Nie znaleziono danych H1/H2/H4 do usunięcia.")
            
    except Exception as e:
        logger.error(f"Błąd podczas czyszczenia strategii: {e}")
        session.rollback()
# ==================================================================


def _run_schema_and_index_migration(session: Session):
    """
    Zapewnia, że schemat bazy danych i niezbędne indeksy są aktualne.
    Ta funkcja działa teraz w swojej własnej, niezależnej transakcji.
    """
    try:
        logger.info("Starting database schema and index migration...")
        
        engine = session.get_bind()
        inspector = inspect(engine)
        
        # === MIGRACJA TABELI 1: trading_signals ===
        if 'trading_signals' in inspector.get_table_names():
            columns = [col['name'] for col in inspector.get_columns('trading_signals')]
            
            # Używamy IF NOT EXISTS dla bezpieczeństwa
            if 'entry_zone_bottom' not in columns:
                logger.warning("Migration needed: Adding column 'entry_zone_bottom'.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS entry_zone_bottom NUMERIC(12, 2)"))
            if 'entry_zone_top' not in columns:
                logger.warning("Migration needed: Adding column 'entry_zone_top'.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS entry_zone_top NUMERIC(12, 2)"))
            if 'updated_at' not in columns:
                logger.warning("Migration needed: Adding column 'updated_at' to 'trading_signals' table.")
                session.execute(text("ALTER TABLE trading_signals ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()"))
                logger.info("Successfully added 'updated_at' column.")

        # === MIGRACJA TABELI 2: Stworzenie indeksu (jeśli go brakuje) ===
        index_name = 'uq_active_pending_ticker'
        logger.info(f"Attempting to create or verify partial unique index '{index_name}'...")
        # Usunięcie starego, potencjalnie konfliktowego ograniczenia (bezpieczne, jeśli nie istnieje)
        session.execute(text("ALTER TABLE trading_signals DROP CONSTRAINT IF EXISTS trading_signals_ticker_key;"))
        # Stworzenie poprawnego indeksu
        create_index_sql = text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
            ON trading_signals (ticker)
            WHERE status IN ('ACTIVE', 'PENDING');
        """)
        session.execute(create_index_sql)
        
        # ==================================================================
        # === OSTATECZNA NAPRAWA BŁĘDU (DuplicateColumn / UndefinedColumn) ===
        # Używamy teraz polecenia 'ADD COLUMN IF NOT EXISTS', które jest
        # idempotentne i obsługiwane przez PostgreSQL.
        # ==================================================================
        
        if 'virtual_trades' in inspector.get_table_names():
            logger.info("Checking schema for 'virtual_trades' table using robust migration...")
            
            # Lista wszystkich 14 nowych kolumn (używamy NUMERIC(12, 6) dla większej precyzji)
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
            
            # Pętla dodająca brakujące kolumny w sposób odporny na błędy
            columns_added_count = 0
            for col_name, col_type in metric_columns_to_add:
                try:
                    # Używamy cudzysłowów, aby zachować wielkość liter (np. "metric_J_norm")
                    # To polecenie DODA kolumnę, jeśli jej nie ma, i NIE ZGŁOSI BŁĘDU, jeśli już istnieje.
                    sql_command = f'ALTER TABLE virtual_trades ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}'
                    session.execute(text(sql_command))
                    columns_added_count += 1 # Liczymy próby, niekoniecznie sukcesy
                except Exception as e:
                    # Ten błąd nie powinien się już zdarzyć, ale zabezpieczamy
                    logger.error(f"Failed to execute 'ADD COLUMN IF NOT EXISTS' for {col_name}: {e}")
                    session.rollback() # Wycofaj tylko tę jedną nieudaną operację
            
            if columns_added_count > 0:
                logger.info(f"Migration: Successfully executed 'ADD IF NOT EXISTS' for all {columns_added_count} metric columns.")
            
        # ==================================================================
        # === KONIEC OSTATECZNEJ NAPRAWY ===
        # ==================================================================

        session.commit() # Zapisujemy wszystkie zmiany w schemacie
        logger.info(f"Successfully committed all schema and index migrations.")

    except Exception as e:
        logger.critical(f"FATAL: Error during database schema/index migration: {e}", exc_info=True)
        session.rollback()
        # Rzucamy błąd dalej, aby zatrzymać aplikację, jeśli migracja się nie powiedzie
        raise

def initialize_database_if_empty(session: Session, api_client):
    """
    Sprawdza, czy tabela 'companies' jest pusta i w razie potrzeby ją uzupełnia.
    Migracja schematu jest teraz wywoływana osobno.
    
    NOWA LOGIKA: Powrót do bezpiecznego trybu "run-once".
    Agresywne czyszczenie zostało USUNIĘTE, aby chronić 
    wyniki backtestów (virtual_trades).
    """
    # 1. ZAWSZE URUCHAMIAJ MIGRACJĘ PRZY STARCIE W OSOBNEJ TRANSAKCJI
    _run_schema_and_index_migration(session)
    
    # ==================================================================
    # === NOWOŚĆ: Selektywne czyszczenie H1, H2, H4 ===
    # Uruchamiane zawsze przy starcie workera.
    # ==================================================================
    _clean_legacy_strategies(session)
    # ==================================================================

    # 2. Teraz, w nowej transakcji, sprawdź i uzupełnij dane
    try:
        # ==================================================================
        # === POWRÓT DO BEZPIECZNEJ LOGIKI (OCHRONA DANYCH) ===
        # Sprawdzamy, czy baza danych już zawiera spółki.
        # ==================================================================
        count_result = session.execute(text("SELECT COUNT(*) FROM companies")).scalar_one()
        if count_result > 0:
            # Jeśli baza MA dane (nawet te 3600 "brudnych"), nic nie rób.
            # Pozwól, aby Faza 1 je przefiltrowała.
            logger.info(f"Database already seeded with {count_result} companies. Skipping data initialization.")
            return
        # ==================================================================

        # Ten kod uruchomi się tylko raz, jeśli baza danych jest FIZYCZNIE pusta
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
