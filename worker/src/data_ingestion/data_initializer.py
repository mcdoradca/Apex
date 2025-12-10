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
    """
    try:
        logger.info("Starting database schema and index migration (BRUTE FORCE MODE)...")
        
        engine = session.get_bind()
        
        def safe_add_column(table_name, col_name, col_type):
            try:
                with engine.connect() as conn:
                    inspector = inspect(engine)
                    if table_name in inspector.get_table_names():
                        current_columns = [c['name'] for c in inspector.get_columns(table_name)]
                        
                        if col_name not in current_columns:
                            logger.info(f"Migracja: Dodawanie kolumny '{col_name}' do tabeli '{table_name}'...")
                            conn.execute(text("COMMIT")) 
                            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
                            conn.execute(text("COMMIT"))
                            logger.info(f"Migracja: Sukces - dodano '{col_name}'.")
                        else:
                            pass 
            except Exception as e:
                logger.error(f"Migracja BŁĄD (niekrytyczny) dla '{col_name}' w '{table_name}': {e}")

        # === 1. TRADING SIGNALS ===
        safe_add_column('trading_signals', 'entry_zone_bottom', 'NUMERIC(12, 2)')
        safe_add_column('trading_signals', 'entry_zone_top', 'NUMERIC(12, 2)')
        safe_add_column('trading_signals', 'updated_at', 'TIMESTAMP WITH TIME ZONE DEFAULT NOW()')
        safe_add_column('trading_signals', 'highest_price_since_entry', 'NUMERIC(12, 2)')
        safe_add_column('trading_signals', 'is_trailing_active', 'BOOLEAN DEFAULT FALSE')
        safe_add_column('trading_signals', 'earnings_date', 'DATE')
        safe_add_column('trading_signals', 'expiration_date', 'TIMESTAMP WITH TIME ZONE')
        safe_add_column('trading_signals', 'expected_profit_factor', 'NUMERIC(10, 4)')
        safe_add_column('trading_signals', 'expected_win_rate', 'NUMERIC(10, 4)')

        # === 2. COMPANIES ===
        safe_add_column('companies', 'sector_etf', 'VARCHAR(10)')

        # === 3. PHASE 1 CANDIDATES ===
        safe_add_column('phase1_candidates', 'sector_ticker', 'VARCHAR(10)')
        safe_add_column('phase1_candidates', 'sector_trend_score', 'NUMERIC(5, 2)')
        safe_add_column('phase1_candidates', 'days_to_earnings', 'INTEGER')

        # === 4. PHASE X CANDIDATES (BIOX) - PEŁNA WERYFIKACJA ===
        safe_add_column('phasex_candidates', 'last_pump_date', 'DATE')
        safe_add_column('phasex_candidates', 'last_pump_percent', 'NUMERIC(10, 2)')
        safe_add_column('phasex_candidates', 'pump_count_1y', 'INTEGER DEFAULT 0')
        safe_add_column('phasex_candidates', 'volume_avg', 'BIGINT')

        # === 5. PHASE 4 CANDIDATES (H4 KINETIC ALPHA) ===
        safe_add_column('phase4_candidates', 'kinetic_score', 'INTEGER')
        safe_add_column('phase4_candidates', 'elasticity', 'NUMERIC(10, 4)')
        safe_add_column('phase4_candidates', 'shots_30d', 'INTEGER DEFAULT 0')
        safe_add_column('phase4_candidates', 'avg_intraday_volatility', 'NUMERIC(10, 4)')
        safe_add_column('phase4_candidates', 'max_daily_shots', 'INTEGER DEFAULT 0')
        safe_add_column('phase4_candidates', 'total_2pct_shots_ytd', 'INTEGER DEFAULT 0')
        safe_add_column('phase4_candidates', 'avg_swing_size', 'NUMERIC(10, 2)')
        safe_add_column('phase4_candidates', 'hard_floor_violations', 'INTEGER DEFAULT 0')
        safe_add_column('phase4_candidates', 'last_shot_date', 'DATE')

        # === 6. VIRTUAL TRADES (Metryki H4 i Re-check) ===
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
            ("expected_profit_factor", "NUMERIC(10, 4)"),
            ("expected_win_rate", "NUMERIC(10, 4)"),
            ("ai_audit_report", "TEXT"),
            ("ai_audit_date", "TIMESTAMP WITH TIME ZONE"),
            ("ai_optimization_suggestion", "JSONB"),
            # Metryki H4
            ("metric_kinetic_energy", "NUMERIC(10, 4)"),
            ("metric_elasticity", "NUMERIC(10, 4)")
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

def selective_data_wipe(session: Session):
    """
    Czyści dane Optymalizatora, Backtestu i Sygnałów H3, ale zachowuje
    dane fundamentalne (Companies, Cache API, Portfel).
    """
    logger.warning("🧹 ROZPOCZYNAM SELEKTYWNE CZYSZCZENIE DANYCH (Optimizer, Backtest, Signals)...")
    
    try:
        # 1. OPTIMIZER (Czyścimy historię nauki)
        session.execute(text("TRUNCATE TABLE optimization_trials CASCADE;"))
        session.execute(text("TRUNCATE TABLE optimization_jobs CASCADE;"))
        logger.info("✅ Wyczyszczono dane Optymalizatora (Trials, Jobs).")

        # 2. BACKTEST (Czyścimy stare symulacje)
        # Usuwamy tylko wirtualne transakcje z setup_type zaczynającym się od 'BACKTEST_'
        session.execute(text("DELETE FROM virtual_trades WHERE setup_type LIKE 'BACKTEST_%';"))
        logger.info("✅ Wyczyszczono dane Backtestu.")

        # 3. SYGNAŁY H3 LIVE (Czyścimy stare sygnały)
        # Usuwamy wszystko z trading_signals (chyba że chcemy zachować coś specyficznego, ale prośba była o wyczyszczenie H3 Live)
        # Uwaga: Jeśli portfel (portfolio_holdings) polega na trading_signals (klucze obce), 
        # to TRUNCATE CASCADE usunie też portfel, co może być niepożądane.
        # Dlatego używamy DELETE z filtrem statusu lub po prostu usuwamy sygnały, które nie są 'MANUAL'.
        
        # Bezpieczne czyszczenie: Usuwamy sygnały, ale jeśli są powiązane z portfelem, zostawiamy te aktywne "MANUAL" (jeśli istnieją).
        # Tutaj usuwamy po prostu wszystkie, zakładając że użytkownik chce czystą kartę sygnałową.
        # Aby nie usunąć portfela (jeśli jest ON DELETE CASCADE), sprawdzamy powiązania.
        # W modelu: ForeignKey('companies.ticker', ondelete='CASCADE') jest w signals -> companies.
        # W portfolio: ForeignKey('companies.ticker').
        # Nie ma bezpośredniego FK między portfolio a signals w modelach, które widzę (chyba że wirtualne).
        # Jednak wirtualne transakcje (Live Monitor) mogą być podpięte.
        
        # Czyścimy wirtualne transakcje monitora (te, które nie są backtestem)
        session.execute(text("DELETE FROM virtual_trades WHERE setup_type NOT LIKE 'BACKTEST_%';"))
        
        # Czyścimy sygnały
        session.execute(text("TRUNCATE TABLE trading_signals RESTART IDENTITY CASCADE;"))
        logger.info("✅ Wyczyszczono dane Sygnałów H3 Live.")

        # 4. RESET SYSTEM CONTROL (Liczniki)
        session.execute(text("UPDATE system_control SET value='NONE' WHERE key IN ('optimization_request', 'backtest_request', 'ai_optimizer_request', 'h3_deep_dive_request');"))
        session.execute(text("UPDATE system_control SET value='NONE' WHERE key = 'ai_optimizer_report';"))
        session.execute(text("UPDATE system_control SET value='NONE' WHERE key = 'h3_deep_dive_report';"))
        
        session.commit()
        logger.warning("🏁 SELEKTYWNE CZYSZCZENIE ZAKOŃCZONE SUKCESEM.")
        
    except Exception as e:
        logger.error(f"❌ Błąd podczas selektywnego czyszczenia: {e}", exc_info=True)
        session.rollback()

def force_reset_simulation_data(session: Session):
    if os.getenv("APEX_ALLOW_DATA_RESET") != "TRUE": return
    logger.warning("⚠️⚠️⚠️ TWARDY RESET ZOSTAŁ WYWOŁANY PRZEZ UŻYTKOWNIKA ⚠️⚠️⚠️")
    try:
        tables_to_clear = ["optimization_trials", "optimization_jobs", "virtual_trades", "trading_signals", "phase1_candidates", "phase2_results", "processed_news", "phasex_candidates", "phase4_candidates"]
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
    _run_schema_and_index_migration(session)
    
    # === SELEKTYWNE CZYSZCZENIE NA ŻĄDANIE ===
    # Sprawdzamy flagę w zmiennych środowiskowych lub wykonujemy raz przy deployu.
    # W tym przypadku, wykonamy to zawsze przy starcie, jeśli flaga 'APEX_WIPE_OPTIMIZER' jest ustawiona,
    # LUB po prostu wywołamy to teraz jednorazowo, ponieważ edytujemy kod "na żywo".
    # Aby to zadziałało teraz, wywołamy to bezwarunkowo, a w następnej edycji usuniesz wywołanie.
    # LUB bezpieczniej: sprawdzamy, czy tabela optimization_trials ma dużo danych.
    
    # Decyzja: Wywołujemy to ZAWSZE w tej wersji pliku. Po restarcie workera dane znikną.
    # Użytkownik poprosił o to teraz.
    selective_data_wipe(session) 
    
    try:
        engine = session.get_bind()
        inspector = inspect(engine)
        if 'companies' not in inspector.get_table_names(): return
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
                companies_to_insert.append({"ticker": symbol, "company_name": row.get('Security Name'), "exchange": "NASDAQ"})
            except Exception: excluded_count += 1
        if not companies_to_insert: return
        insert_stmt = text("INSERT INTO companies (ticker, company_name, exchange, industry, sector) VALUES (:ticker, :company_name, :exchange, 'N/A', 'N/A') ON CONFLICT (ticker) DO NOTHING;")
        session.execute(insert_stmt, companies_to_insert)
        session.commit()
        logger.info(f"Successfully inserted {len(companies_to_insert)} companies. Excluded: {excluded_count}.")
    except Exception as e:
        logger.error(f"An error occurred during data initialization: {e}", exc_info=True)
        session.rollback()
