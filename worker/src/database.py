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
        engine = create_engine(DATABASE_URL)
        with engine.connect():
            logger.info("Successfully connected to the database.")
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
