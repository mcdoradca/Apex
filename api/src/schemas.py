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
#  OSTATECZNA POPRAWKA (2.0)
#  Ten schemat pasuje teraz do nowej, poprawnej struktury zwracanej
#  przez plik 'worker/src/analysis/ai_agents.py'.
# ==================================================================
class AIAnalysisResult(BaseModel):
    # Schemat dla statusu "DONE"
    status: str
    ticker: str
    overall_score: int                 # ZMIANA: z str na int
    max_score: int                     # ZMIANA: dodane pole
    final_score_percent: int
    recommendation: str
    recommendation_details: str
    agents: dict                       # ZMIANA: z List[Any] na dict
    analysis_timestamp_utc: str

    # Pola dla statusu "PROCESSING" lub "ERROR"
    message: Optional[str] = None 
    
    model_config = ConfigDict(from_attributes=True)
