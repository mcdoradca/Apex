import logging
import csv
from io import StringIO
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

def initialize_database_if_empty(session: Session, api_client):
    """
    Sprawdza, czy tabela 'companies' jest pusta. Jeśli tak, pobiera listę
    spółek z Alpha Vantage, stosując ulepszony filtr, i zapisuje ją w bazie.
    """
    try:
        count_result = session.execute(text("SELECT COUNT(*) FROM companies")).scalar_one()
        
        if count_result > 0:
            logger.info(f"Database already seeded. Found {count_result} companies. Skipping initialization.")
            return

        logger.info("Table 'companies' is empty. Initializing with data from Alpha Vantage...")
        
        params = {"function": "LISTING_STATUS", "apikey": api_client.api_key}
        response = api_client._make_raw_request(params)
        
        if response is None:
            logger.error("Failed to fetch listing status from Alpha Vantage. Database remains empty.")
            return
        
        csv_file = StringIO(response.text)
        reader_list = list(csv.DictReader(csv_file))
        
        if not reader_list:
            logger.error("CSV file from Alpha Vantage seems to be empty or corrupted.")
            return

        companies_to_insert = []
        for row in reader_list:
            ticker = row.get('symbol')
            # --- POPRAWKA: Ulepszony, bardziej rygorystyczny filtr ---
            # Przepuszczamy tylko akcje, które wyglądają na standardowe, aby utrzymać
            # bazę danych w czystości od samego początku.
            is_standard_stock = (
                ticker and
                '.' not in ticker and
                1 <= len(ticker) <= 5 and
                ticker.isalpha() and
                ticker.isupper()
            )
            if (row.get('exchange') == 'NASDAQ' and 
                row.get('status') == 'Active' and 
                is_standard_stock):
            # --- KONIEC POPRAWKI ---
                companies_to_insert.append({
                    "ticker": ticker,
                    "company_name": row.get('name'),
                    "exchange": row.get('exchange'),
                })
        
        logger.info(f"Found {len(companies_to_insert)} active, standard NASDAQ companies to insert into the database.")

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

    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}", exc_info=True)
        session.rollback()

