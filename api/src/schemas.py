from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import List, Optional, Any

# ==============================================================================
# KRYTYCZNA POPRAWKA: Dodanie brakującego schematu odpowiedzi dla zlecenia analizy AI.
# To jest ostateczne rozwiązanie błędu 'AttributeError'.
# ==============================================================================
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
    last_heartbeat_utc: str # Zmieniono na string, aby uniknąć problemów z deserializacją
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
    generation_date: datetime
    status: str
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    signal_candle_timestamp: Optional[datetime] = None
    entry_zone_bottom: Optional[float] = None
    entry_zone_top: Optional[float] = None
    notes: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

class AIAnalysisResult(BaseModel):
    # Pydantic automatycznie zwaliduje, czy 'analysis_data' to słownik (JSON)
    analysis_data: dict
    
    model_config = ConfigDict(from_attributes=True)

