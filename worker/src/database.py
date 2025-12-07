import os
import time
import logging
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

# Ładowanie zmiennych środowiskowych (.env)
load_dotenv()

# Konfiguracja loggera
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO)

# Pobranie adresu bazy danych
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    logger.critical("DATABASE_URL environment variable not set. Exiting.")
    sys.exit(1)

# === FIX DLA RENDER / SQLALCHEMY ===
# SQLAlchemy 1.4+ usunęło obsługę 'postgres://', wymagane jest 'postgresql://'
# Render domyślnie podaje 'postgres://', więc musimy to naprawić w locie.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# === KONFIGURACJA SILNIKA (ENGINE) ===
# pool_size=10: Bezpieczny limit dla API i Workera na darmowych/starter planach.
# max_overflow=10: Bufor na nagłe skoki ruchu.
# pool_pre_ping=True: KLUCZOWE. Sprawdza czy połączenie żyje przed użyciem (zapobiega błędom "server closed connection").
# pool_recycle=1800: Odświeża połączenia co 30 min, aby uniknąć timeoutów po stronie serwera SQL.

engine = None
RETRY_COUNT = 5
RETRY_DELAY = 5

for i in range(RETRY_COUNT):
    try:
        engine = create_engine(
            DATABASE_URL,
            pool_size=10,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True 
        )
        # Próba otwarcia połączenia testowego
        with engine.connect() as connection:
            logger.info("✅ Successfully connected to the database.")
            break
    except OperationalError as e:
        logger.warning(f"⚠️ Database connection failed (attempt {i+1}/{RETRY_COUNT}): {e}")
        if i < RETRY_COUNT - 1:
            logger.info(f"Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
        else:
            logger.critical("❌ Could not connect to the database after multiple retries.")
            sys.exit(1) # Zabij proces, aby platforma (Render) zrestartowała go

# === SESJA I BAZA ===
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# === FUNKCJE POMOCNICZE ===

def get_db():
    """
    Generator sesji dla FastAPI (Dependency Injection).
    Zamyka sesję automatycznie po zakończeniu żądania.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db_session():
    """
    Funkcja dla Workera i skryptów (Ręczne zarządzanie).
    Zwraca nową sesję. Pamiętaj o zamknięciu jej ręcznie!
    """
    return SessionLocal()
