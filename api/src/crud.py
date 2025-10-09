from sqlalchemy.orm import Session
from sqlalchemy import text
from . import models

# Funkcje do wykonywania operacji na bazie danych (Create, Read, Update, Delete).

def get_system_control_value(db: Session, key: str) -> str | None:
    """Odczytuje pojedynczą wartość z tabeli system_control."""
    result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
    return result[0] if result else None

def set_system_control_value(db: Session, key: str, value: str):
    """Aktualizuje lub wstawia wartość w tabeli system_control (UPSERT)."""
    stmt = text("""
        INSERT INTO system_control (key, value, updated_at)
        VALUES (:key, :value, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = NOW();
    """)
    db.execute(stmt, {'key': key, 'value': value})
    db.commit()

def get_active_signals(db: Session) -> list[models.TradingSignal]:
    """Pobiera wszystkie aktywne sygnały transakcyjne."""
    return db.query(models.TradingSignal).filter(models.TradingSignal.status == 'ACTIVE').all()

