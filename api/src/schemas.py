from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import List, Optional

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

class Progress(BaseModel):
    processed: int
    total: int

class WorkerStatus(BaseModel):
    status: str
    phase: str
    progress: Progress
    last_heartbeat_utc: datetime
    log: str

class OnDemandRequest(BaseModel):
    ticker: str

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
    
class LivePrice(BaseModel):
    ticker: str
    price: float

# NOWY SCHEMAT: Wynik analizy AI
class AIAnalysisResult(BaseModel):
    ticker: str
    overall_score: int
    max_score: int
    recommendation: str
    agents: dict
    analysis_timestamp_utc: datetime

class ConsolidatedTickerDetails(BaseModel):
    ticker: str
    phase1_data: Optional[Phase1Candidate] = None
    phase2_data: Optional[Phase2Result] = None
    phase3_signal: Optional[TradingSignal] = None
    ai_analysis: Optional[AIAnalysisResult] = None
    
class SystemAlert(BaseModel):
    message: str
