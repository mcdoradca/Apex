from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import List, Optional, Any

# --- Schemat dla cen na żywo ---
class LivePrice(BaseModel):
    ticker: str
    price: float

# --- Schematy dla poszczególnych faz ---
class Phase1Candidate(BaseModel):
    ticker: str
    price: float
    change_percent: float
    volume: int
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
    entry_price: float
    stop_loss: float
    take_profit: float
    risk_reward_ratio: float
    signal_candle_timestamp: datetime

    model_config = ConfigDict(from_attributes=True)

# --- Schematy dla analiz na żądanie ---
class OnDemandRequest(BaseModel):
    ticker: str

# --- Schematy zbiorcze i statusowe ---
class ConsolidatedTickerDetails(BaseModel):
    ticker: str
    phase1_data: Optional[Phase1Candidate] = None
    phase2_data: Optional[Phase2Result] = None
    phase3_signal: Optional[TradingSignal] = None
    on_demand_analysis: Optional[Any] = None
    phase3_on_demand_analysis: Optional[Any] = None

class Progress(BaseModel):
    processed: int
    total: int

class WorkerStatus(BaseModel):
    status: str
    phase: str
    progress: Progress
    last_heartbeat_utc: Optional[str] = None
    log: str

