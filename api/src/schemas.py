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
    # =================================================================
    # KRYTYCZNA POPRAWKA: Zmiana typu daty na 'str'
    # To zapobiega błędom serializacji, które powodują awarię API (Błąd 500).
    # =================================================================
    generation_date: str
    status: str
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    signal_candle_timestamp: Optional[str] = None # Zmieniono na str
    entry_zone_bottom: Optional[float] = None
    entry_zone_top: Optional[float] = None
    notes: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

# ==================================================================
#  POPRAWKA BŁĘDU 500 (ResponseValidationError)
#  Zmieniono ten model, aby pasował do "płaskiej" struktury JSON
#  zwracanej przez worker/ai_agents.py i oczekiwanej przez index.html
# ==================================================================
class AIAnalysisResult(BaseModel):
    # Schemat dla udanej analizy (status: "DONE")
    status: str
    ticker: str
    overall_score: str
    final_score_percent: int
    recommendation: str
    recommendation_details: str
    agents: List[Any]  # Można tu zdefiniować dokładniejszy schemat Agenta
    analysis_timestamp_utc: str

    # Pola dla statusu "PROCESSING" lub "ERROR"
    message: Optional[str] = None 
    
    model_config = ConfigDict(from_attributes=True)
