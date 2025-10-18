from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import List, Optional, Any

class AIAnalysisRequestResponse(BaseModel):
    message: str
    ticker: str

class OnDemandRequest(BaseModel):
    ticker: str

class Progress(BaseModel):
    processed: int
    total: int

class WorkerStatus(BaseModel):
    status: str
    phase: str
    progress: Progress
    last_heartbeat_utc: str
    log: str

class SystemAlert(BaseModel):
    message: str

# --- Schematy Wyników Analiz ---

class Phase1Candidate(BaseModel):
    ticker: str
    price: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None
    score: int
    analysis_date: datetime

    model_config = ConfigDict(from_attributes=True)

class Phase2Result(BaseModel):
    ticker: str
    analysis_date: date
    catalyst_score: int
    relative_strength_score: int
    energy_compression_score: int
    total_score: int
    is_qualified: bool

    model_config = ConfigDict(from_attributes=True)

class TradingSignal(BaseModel):
    id: int
    ticker: str
    generation_date: str
    status: str
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    signal_candle_timestamp: Optional[str] = None
    entry_zone_bottom: Optional[float] = None
    entry_zone_top: Optional[float] = None
    notes: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

# ==================================================================
#  POPRAWKA BŁĘDU 500 (Wersja 3.0)
#  Wszystkie pola oprócz 'status' są teraz opcjonalne (`Optional[...]`).
#  To pozwala na poprawną walidację obiektów tymczasowych
#  (np. {"status": "PROCESSING", "message": "..."})
#  oraz obiektów błędu ({"status": "ERROR", "message": "..."}).
# ==================================================================
class AIAnalysisResult(BaseModel):
    # Jedyne pole wymagane w każdym stanie (PROCESSING, ERROR, DONE)
    status: str
    
    # Pola dla statusu "PROCESSING" lub "ERROR"
    message: Optional[str] = None 

    # Pola dla statusu "DONE"
    ticker: Optional[str] = None
    overall_score: Optional[int] = None
    max_score: Optional[int] = None
    final_score_percent: Optional[int] = None
    recommendation: Optional[str] = None
    recommendation_details: Optional[str] = None
    agents: Optional[dict] = None
    analysis_timestamp_utc: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
