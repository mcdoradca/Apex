import logging
import csv
from io import StringIO
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

def initialize_database_if_empty(session: Session, api_client):
    """
    Sprawdza, czy tabela 'companies' jest pusta. Jeśli tak, pobiera listę
    spółek z Alpha Vantage i zapisuje ją w bazie danych.
    """
    try:
        count_result = session.execute(text("SELECT COUNT(*) FROM companies")).scalar_one()
        
        if count_result > 0:
            logger.info(f"Database already seeded. Found {count_result} companies. Skipping initialization.")
            return

        logger.info("Table 'companies' is empty. Initializing with data from Alpha Vantage...")
        
        # Pobranie listy spółek jako plik CSV z Alpha Vantage
        # Używamy surowego zapytania, ponieważ klient API oczekuje JSONa
        params = {"function": "LISTING_STATUS", "apikey": api_client.api_key}
        response = api_client._make_raw_request(params) # Używamy nowej, surowej metody
        
        if response is None:
            logger.error("Failed to fetch listing status from Alpha Vantage. Database remains empty.")
            return

        # Przetwarzanie odpowiedzi CSV
        csv_file = StringIO(response.text)
        reader = csv.DictReader(csv_file)
        
        companies_to_insert = []
        for row in reader:
            # Filtrujemy tylko spółki z giełdy NASDAQ, które są aktywne
            if row.get('exchange') == 'NASDAQ' and row.get('status') == 'Active':
                companies_to_insert.append({
                    "ticker": row['symbol'],
                    "company_name": row['name'],
                    "exchange": row['exchange'],
                    "assetType": row['assetType'], # Zmieniamy klucz, aby pasował do modelu
                })

        if not companies_to_insert:
            logger.warning("No active NASDAQ companies found in the API response.")
            return

        # Wstawienie spółek do bazy danych
        # Używamy surowego SQL dla wydajności przy dużej liczbie wierszy
        insert_stmt = text("""
            INSERT INTO companies (ticker, company_name, exchange, industry, sector)
            VALUES (:ticker, :company_name, :exchange, 'N/A', 'N/A')
            ON CONFLICT (ticker) DO NOTHING;
        """)
        
        # Używamy transakcji, aby wstawić wszystkie dane za jednym razem
        with session.begin():
            for company in companies_to_insert:
                session.execute(insert_stmt, {
                    'ticker': company['ticker'],
                    'company_name': company['company_name'],
                    'exchange': company['exchange']
                })
        
        logger.info(f"Successfully inserted {len(companies_to_insert)} companies into the database.")

    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}", exc_info=True)
        session.rollback()
