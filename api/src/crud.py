from sqlalchemy.orm import Session
from sqlalchemy import text, func, delete
from . import models
from typing import Optional, Any, Dict, List

# --- NOWE FUNKCJE ---
def get_phase1_candidates(db: Session) -> List[models.Phase1Candidate]:
    """Pobiera wszystkich kandydatów z ostatniego skanowania Fazy 1."""
    return db.query(models.Phase1Candidate).order_by(models.Phase1Candidate.score.desc()).all()

def delete_phase1_candidate(db: Session, ticker: str):
    """Usuwa kandydata Fazy 1 z listy."""
    stmt = delete(models.Phase1Candidate).where(models.Phase1Candidate.ticker == ticker)
    db.execute(stmt)
    db.commit()
    return {"status": "ok"}

# ... reszta istniejącego kodu bez zmian ...
def get_system_control_value(db: Session, key: str) -> Optional[str]:
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
    return db.query(models.TradingSignal).filter(models.TradingSignal.status == 'ACTIVE').order_by(models.TradingSignal.generation_date.desc()).all()

def get_on_demand_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    """Odczytuje wynik analizy na żądanie dla danego tickera."""
    result = db.query(models.OnDemandAnalysisResult).filter(models.OnDemandAnalysisResult.ticker == ticker).first()
    return result.analysis_data if result else None

def get_qualified_stocks(db: Session) -> list[models.ApexScore]:
    """Pobiera wszystkie spółki zakwalifikowane w Fazie 2 z ostatniego dnia analizy."""
    latest_analysis_date = db.query(func.max(models.ApexScore.analysis_date)).scalar()
    
    if not latest_analysis_date:
        return []
        
    return db.query(models.ApexScore).filter(
        models.ApexScore.analysis_date == latest_analysis_date,
        models.ApexScore.is_qualified == True
    ).order_by(models.ApexScore.total_score.desc()).all()

