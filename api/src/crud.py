from sqlalchemy.orm import Session
from sqlalchemy import text, desc, func
from . import models
from typing import Optional, Any, Dict, List
from datetime import date

# ==============================================================================
# OSTATECZNA NAPRAWA: Ręczna konwersja obiektów SQLAlchemy na słowniki.
# Ta metoda eliminuje wszystkie potencjalne błędy automatycznej serializacji
# w FastAPI/Pydantic, gwarantując, że dane wysyłane do frontendu są
# zawsze w prostym i bezpiecznym formacie.
# ==============================================================================

def get_phase1_candidates(db: Session) -> List[Dict[str, Any]]:
    """Pobiera wszystkich kandydatów z Fazy 1 z najnowszego dnia analizy."""
    latest_date_obj = db.query(func.max(models.Phase1Candidate.analysis_date)).scalar()
    if not latest_date_obj:
        return []
    
    # SQLAlchemy v2 może zwracać datę z dokładnością do mikrosekund, PostgreSQL niekoniecznie.
    # Upewniamy się, że porównujemy daty w spójny sposób.
    candidates_from_db = db.query(models.Phase1Candidate).filter(
        func.date_trunc('second', models.Phase1Candidate.analysis_date) == func.date_trunc('second', latest_date_obj)
    ).all()
    
    results = []
    for c in candidates_from_db:
        results.append({
            "ticker": c.ticker,
            "price": c.price,
            "change_percent": c.change_percent,
            "volume": c.volume,
            "score": c.score,
            "analysis_date": c.analysis_date.isoformat() if c.analysis_date else None
        })
    return results

def get_phase2_results(db: Session) -> List[Dict[str, Any]]:
    """Pobiera wszystkie wyniki Fazy 2 z najnowszego dnia analizy."""
    latest_date = db.query(func.max(models.Phase2Result.analysis_date)).scalar()
    if not latest_date:
        return []
    
    results_from_db = db.query(models.Phase2Result).filter(
        models.Phase2Result.analysis_date == latest_date, 
        models.Phase2Result.is_qualified == True
    ).order_by(desc(models.Phase2Result.total_score)).all()

    results = []
    for r in results_from_db:
        results.append({
            "ticker": r.ticker,
            "analysis_date": r.analysis_date.isoformat() if r.analysis_date else None,
            "catalyst_score": r.catalyst_score,
            "relative_strength_score": r.relative_strength_score,
            "energy_compression_score": r.energy_compression_score,
            "total_score": r.total_score,
            "is_qualified": r.is_qualified
        })
    return results

def get_active_and_pending_signals(db: Session) -> List[Dict[str, Any]]:
    """Pobiera aktywne i oczekujące sygnały (Wyniki Fazy 3)."""
    signals_from_db = db.query(models.TradingSignal).filter(
        models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
    ).order_by(desc(models.TradingSignal.generation_date)).all()
    
    results = []
    for signal in signals_from_db:
        results.append({
            "id": signal.id,
            "ticker": signal.ticker,
            "generation_date": signal.generation_date.isoformat() if signal.generation_date else None,
            "status": signal.status,
            "entry_price": signal.entry_price,
            "stop_loss": signal.stop_loss,
            "take_profit": signal.take_profit,
            "risk_reward_ratio": signal.risk_reward_ratio,
            "signal_candle_timestamp": signal.signal_candle_timestamp.isoformat() if signal.signal_candle_timestamp else None,
            "entry_zone_bottom": signal.entry_zone_bottom,
            "entry_zone_top": signal.entry_zone_top,
            "notes": signal.notes
        })
    return results


def delete_phase1_candidate(db: Session, ticker: str):
    db.query(models.Phase1Candidate).filter(models.Phase1Candidate.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Candidate {ticker} from Phase 1 deleted."}

def delete_phase2_result(db: Session, ticker: str):
    db.query(models.Phase2Result).filter(models.Phase2Result.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Result {ticker} from Phase 2 deleted."}

def delete_trading_signal(db: Session, signal_id: int):
    signal = db.query(models.TradingSignal).filter(models.TradingSignal.id == signal_id).first()
    if signal:
        signal.status = 'DELETED'
        db.commit()
        return {"message": f"Signal {signal_id} for {signal.ticker} marked as deleted."}
    return None

def get_system_control_value(db: Session, key: str) -> Optional[str]:
    result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
    return result[0] if result else None

def set_system_control_value(db: Session, key: str, value: str):
    stmt = text("""
        INSERT INTO system_control (key, value, updated_at)
        VALUES (:key, :value, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = :value, updated_at = NOW();
    """)
    db.execute(stmt, {'key': key, 'value': value})
    db.commit()

def get_ai_analysis_result(db: Session, ticker: str) -> Optional[Dict[str, Any]]:
    result = db.query(models.AIAnalysisResult).filter(models.AIAnalysisResult.ticker == ticker).first()
    return result.analysis_data if result else None
