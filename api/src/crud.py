from sqlalchemy.orm import Session
from sqlalchemy import text, desc
from . import models
from typing import Optional, Any, Dict, List

# --- NOWE FUNKCJE DLA KANDYDATÓW FAZY 1 ---
def get_phase1_candidates(db: Session) -> List[models.Phase1Candidate]:
    """Pobiera wszystkich kandydatów z Fazy 1, sortując od najlepszego wyniku."""
    return db.query(models.Phase1Candidate).order_by(desc(models.Phase1Candidate.score)).all()

def delete_phase1_candidate(db: Session, ticker: str):
    """Usuwa pojedynczego kandydata z listy Fazy 1."""
    db.query(models.Phase1Candidate).filter(models.Phase1Candidate.ticker == ticker).delete()
    db.commit()
    return {"message": f"Candidate {ticker} deleted successfully."}

# --- ISTNIEJĄCE FUNKCJE ---
def get_system_control_value(db: Session, key: str) -> Optional[str]:
    result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
    return result[0] if result else None

def set_system_control_value(db: Session, key: str, value: str):
    stmt = text("""
        INSERT INTO system_control (key, value, updated_at)
        VALUES (:key, :value, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = EXCLUDED.value, updated_at = NOW();
    """)
    db.execute(stmt, {'key': key, 'value': value})
    db.commit()

def get_active_signals(db: Session) -> list[models.TradingSignal]:
    return db.query(models.TradingSignal).filter(models.TradingSignal.status == 'ACTIVE').all()

def get_qualified_stocks(db: Session) -> list[models.ApexScore]:
    """Pobiera wszystkie spółki, które zakwalifikowały się w Fazie 2."""
    return db.query(models.ApexScore).filter(models.ApexScore.is_qualified == True).order_by(desc(models.ApexScore.total_score)).all()

def get_on_demand_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    result = db.query(models.OnDemandAnalysisResult).filter(models.OnDemandAnalysisResult.ticker == ticker).first()
    return result.analysis_data if result else None

