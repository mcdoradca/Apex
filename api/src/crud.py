from sqlalchemy.orm import Session
from sqlalchemy import text, desc, func
from . import models
from typing import Optional, Any, Dict, List
from datetime import date

# --- FUNKCJE DLA KANDYDATÓW FAZY 1 ---
def get_phase1_candidates(db: Session) -> List[models.Phase1Candidate]:
    """Pobiera wszystkich kandydatów z Fazy 1 z najnowszego dnia analizy."""
    latest_date = db.query(func.max(models.Phase1Candidate.analysis_date)).scalar()
    if not latest_date:
        return []
    return db.query(models.Phase1Candidate).filter(models.Phase1Candidate.analysis_date == latest_date).all()

def delete_phase1_candidate(db: Session, ticker: str):
    """Usuwa pojedynczego kandydata z listy Fazy 1."""
    db.query(models.Phase1Candidate).filter(models.Phase1Candidate.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Candidate {ticker} from Phase 1 deleted."}

# --- FUNKCJE DLA WYNIKÓW FAZY 2 ---
def get_phase2_results(db: Session) -> List[models.Phase2Result]:
    """Pobiera wszystkie wyniki Fazy 2 z najnowszego dnia analizy."""
    latest_date = db.query(func.max(models.Phase2Result.analysis_date)).scalar()
    if not latest_date:
        return []
    return db.query(models.Phase2Result).filter(models.Phase2Result.analysis_date == latest_date, models.Phase2Result.is_qualified == True).order_by(desc(models.Phase2Result.total_score)).all()

def delete_phase2_result(db: Session, ticker: str):
    """Usuwa pojedynczy wynik Fazy 2."""
    db.query(models.Phase2Result).filter(models.Phase2Result.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Result {ticker} from Phase 2 deleted."}

# --- FUNKCJE DLA SYGNAŁÓW FAZY 3 ---
def get_active_signals(db: Session) -> list[models.TradingSignal]:
    """Pobiera aktywne sygnały (Wyniki Fazy 3)."""
    return db.query(models.TradingSignal).filter(models.TradingSignal.status == 'ACTIVE').order_by(desc(models.TradingSignal.generation_date)).all()

def delete_trading_signal(db: Session, signal_id: int):
    """Usuwa (deaktywuje) sygnał Fazy 3."""
    signal = db.query(models.TradingSignal).filter(models.TradingSignal.id == signal_id).first()
    if signal:
        signal.status = 'DELETED'
        db.commit()
        return {"message": f"Signal {signal_id} for {signal.ticker} marked as deleted."}
    return None


# --- FUNKCJE KONTROLI SYSTEMU ---
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

# --- FUNKCJE ANALIZY NA ŻĄDANIE ---
def get_on_demand_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    result = db.query(models.OnDemandAnalysisResult).filter(models.OnDemandAnalysisResult.ticker == ticker).first()
    return result.analysis_data if result else None

def get_phase3_on_demand_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    result = db.query(models.Phase3OnDemandResult).filter(models.Phase3OnDemandResult.ticker == ticker).first()
    return result.analysis_data if result else None

# --- FUNKCJA ZBIORCZA ---
def get_consolidated_details(db: Session, ticker: str) -> Dict[str, Any]:
    """Pobiera wszystkie dostępne dane dla danego tickera z różnych faz."""
    today = date.today()
    details = {"ticker": ticker}
    details['phase1_data'] = db.query(models.Phase1Candidate).filter(models.Phase1Candidate.ticker == ticker, models.Phase1Candidate.analysis_date == today).first()
    details['phase2_data'] = db.query(models.Phase2Result).filter(models.Phase2Result.ticker == ticker, models.Phase2Result.analysis_date == today).first()
    details['phase3_signal'] = db.query(models.TradingSignal).filter(models.TradingSignal.ticker == ticker, models.TradingSignal.status == 'ACTIVE').order_by(desc(models.TradingSignal.generation_date)).first()
    details['on_demand_analysis'] = get_on_demand_result(db, ticker)
    details['phase3_on_demand_analysis'] = get_phase3_on_demand_result(db, ticker)
    return details

