from sqlalchemy.orm import Session
from sqlalchemy import text
from . import models
from typing import Optional, Any, Dict

# Funkcje do wykonywania operacji na bazie danych (Create, Read, Update, Delete).

def get_system_control_value(db: Session, key: str) -> Optional[str]:
# ... existing code ...
    result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
    return result[0] if result else None

def set_system_control_value(db: Session, key: str, value: str):
# ... existing code ...
        SET value = EXCLUDED.value, updated_at = NOW();
    """)
    db.execute(stmt, {'key': key, 'value': value})
    db.commit()

def get_active_signals(db: Session) -> list[models.TradingSignal]:
# ... existing code ...
    """Pobiera wszystkie aktywne sygnały transakcyjne."""
    return db.query(models.TradingSignal).filter(models.TradingSignal.status == 'ACTIVE').all()

# NOWA FUNKCJA DO POBIERANIA WYNIKÓW ANALIZY NA ŻĄDANIE
def get_on_demand_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    """Odczytuje wynik analizy na żądanie dla danego tickera."""
    result = db.query(models.OnDemandAnalysisResult).filter(models.OnDemandAnalysisResult.ticker == ticker).first()
    return result.analysis_data if result else None
