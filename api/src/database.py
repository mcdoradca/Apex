import os
import time
import logging
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

# Konfiguracja loggera
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.critical("DATABASE_URL environment variable not set. Exiting.")
    sys.exit(1)

# Logika ponawiania połączenia z bazą danych przy starcie aplikacji
engine = None
RETRY_COUNT = 5
RETRY_DELAY = 5
for i in range(RETRY_COUNT):
    try:
        # === UAKTUALNIENIE KONFIGURACJI (Synchronizacja z Workerem) ===
        # pool_size=20: Zwiększona liczba stałych połączeń (dla obsługi ruchu HTTP)
        # max_overflow=30: Zwiększony bufor dla nagłych skoków zapytań
        # pool_pre_ping=True: Zapobiega błędom "closed connection" poprzez testowanie połączenia
        engine = create_engine(
            DATABASE_URL,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True 
        )
        with engine.connect():
            logger.info("Successfully connected to the database (Enhanced Pool Config).")
            break
    except OperationalError as e:
        logger.warning(f"Database connection failed (attempt {i+1}/{RETRY_COUNT}): {e}. Retrying in {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)

if not engine:
    logger.critical("Could not connect to the database after multiple retries. Exiting.")
    sys.exit(1)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Funkcja do dostarczania sesji bazy danych do endpointów
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
