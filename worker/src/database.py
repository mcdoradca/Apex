import os
import time
import logging
import sys
from sqlalchemy import create_engine
# ZMIANA: Dodajemy niezbędne importy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logger.critical("DATABASE_URL environment variable not set. Exiting.")
    sys.exit(1)

engine = None
RETRY_COUNT = 5
RETRY_DELAY = 5
for i in range(RETRY_COUNT):
    try:
        # === POPRAWKA: Konfiguracja puli połączeń (QueuePool) ===
        # pool_size=10: Utrzymuj do 10 otwartych połączeń (zamiast domyślnych 5)
        # max_overflow=20: Pozwól na 20 dodatkowych "tymczasowych" połączeń w szczycie (zamiast 10)
        # pool_timeout=30: Czekaj max 30s na wolne połączenie
        # pool_recycle=1800: Odświeżaj połączenia co 30 minut (dla stabilności na Render)
        engine = create_engine(
            DATABASE_URL,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True # Automatycznie sprawdzaj czy połączenie żyje przed użyciem
        )
        with engine.connect():
            logger.info("Successfully connected to the database (Enhanced Pool Config).")
            break
    except OperationalError as e:
        logger.warning(f"Database connection failed (attempt {i+1}/{RETRY_COUNT}): {e}. Retrying in {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)

if not engine:
    logger.critical("Could not connect to the database after multiple retries.")
    pass

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ZMIANA: Definiujemy 'Base', aby modele mogły z niego korzystać
Base = declarative_base()

def get_db_session():
    return SessionLocal()
