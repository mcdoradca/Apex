from sqlalchemy.orm import Session
from sqlalchemy import text, desc, func
from . import models, schemas
from typing import Optional, Any, Dict, List
from datetime import date
import json

# --- FUNKCJE DLA KANDYDATÓW FAZY 1 ---
def get_phase1_candidates(db: Session) -> List[models.Phase1Candidate]:
    latest_date = db.query(func.max(models.Phase1Candidate.analysis_date)).scalar()
    if not latest_date: return []
    return db.query(models.Phase1Candidate).filter(models.Phase1Candidate.analysis_date == latest_date).all()

# --- FUNKCJE DLA WYNIKÓW FAZY 2 ---
def get_phase2_results(db: Session) -> List[models.Phase2Result]:
    latest_date = db.query(func.max(models.Phase2Result.analysis_date)).scalar()
    if not latest_date: return []
    return db.query(models.Phase2Result).filter(
        models.Phase2Result.analysis_date == latest_date, 
        models.Phase2Result.is_qualified == True
    ).order_by(desc(models.Phase2Result.total_score)).all()

# --- FUNKCJE DLA SYGNAŁÓW FAZY 3 ---
def get_active_pending_and_watchlist_signals(db: Session) -> list[models.TradingSignal]:
    return db.query(models.TradingSignal).filter(
        models.TradingSignal.status.in_(['ACTIVE', 'PENDING', 'WATCHLIST'])
    ).order_by(desc(models.TradingSignal.generation_date)).all()

# --- FUNKCJE KONTROLI SYSTEMU ---
def get_system_control_value(db: Session, key: str) -> Optional[str]:
    result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
    return result[0] if result else None

def set_system_control_value(db: Session, key: str, value: str):
    stmt = text("""
        INSERT INTO system_control (key, value, updated_at) VALUES (:key, :value, NOW())
        ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW();
    """)
    db.execute(stmt, {'key': key, 'value': value})
    db.commit()

# --- NOWE FUNKCJE ANALIZY AI ---
def save_ai_analysis_result(db: Session, ticker: str, data: dict):
    stmt = text("""
        INSERT INTO ai_analysis_results (ticker, analysis_data, last_updated)
        VALUES (:ticker, :data, NOW()) ON CONFLICT (ticker) DO UPDATE 
        SET analysis_data = EXCLUDED.analysis_data, last_updated = NOW();
    """)
    db.execute(stmt, {'ticker': ticker, 'data': json.dumps(data)})
    db.commit()

def get_ai_analysis_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    result = db.query(models.AIAnalysisResult).filter(models.AIAnalysisResult.ticker == ticker).first()
    return result.analysis_data if result else None

# --- NOWA FUNKCJA WATCHLIST ---
def add_to_watchlist(db: Session, ticker: str):
    # Sprawdź, czy już istnieje aktywny sygnał lub na watchlist
    exists = db.query(models.TradingSignal).filter(
        models.TradingSignal.ticker == ticker,
        models.TradingSignal.status.in_(['ACTIVE', 'PENDING', 'WATCHLIST'])
    ).first()

    if not exists:
        new_signal = models.TradingSignal(
            ticker=ticker,
            status='WATCHLIST',
            notes=f'Ręcznie dodany do listy obserwacyjnej przez użytkownika.'
        )
        db.add(new_signal)
        db.commit()
        return {"message": "Dodano do listy obserwacyjnej."}
    return {"message": "Ticker jest już na liście obserwacyjnej lub aktywny."}


# --- FUNKCJA ZBIORCZA ---
def get_consolidated_details(db: Session, ticker: str) -> Dict[str, Any]:
    details = {"ticker": ticker}
    today = date.today()
    details['phase1_data'] = db.query(models.Phase1Candidate).filter(models.Phase1Candidate.ticker == ticker, func.date(models.Phase1Candidate.analysis_date) == today).first()
    details['phase2_data'] = db.query(models.Phase2Result).filter(models.Phase2Result.ticker == ticker).order_by(desc(models.Phase2Result.analysis_date)).first()
    details['phase3_signal'] = db.query(models.TradingSignal).filter(models.TradingSignal.ticker == ticker, models.TradingSignal.status.in_(['ACTIVE', 'PENDING', 'WATCHLIST'])).first()
    
    # Dodano pobieranie nowej analizy
    ai_result_raw = get_ai_analysis_result(db, ticker)
    if ai_result_raw:
        # Pydantic nie lubi podwójnego JSON.loads()
        details['ai_analysis'] = schemas.AIAnalysisResult(**ai_result_raw) if isinstance(ai_result_raw, dict) else schemas.AIAnalysisResult(**json.loads(ai_result_raw))

    return details
