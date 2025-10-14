from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import List, Optional, Dict, Any

# Schemat dla Kandydatów Fazy 1 (zgodny z nowym modelem)
class Phase1Candidate(BaseModel):
    ticker: str
    price: Optional[float]
    change_percent: Optional[float]
    volume: Optional[int]
    analysis_date: date

    model_config = ConfigDict(from_attributes=True)

# Schemat dla Wyników Fazy 2
class Phase2Result(BaseModel):
    ticker: str
    analysis_date: date
    momentum_score: int
    compression_score: int
    catalyst_score: int
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
    entry_price: float
    stop_loss: float
    take_profit: float
    risk_reward_ratio: float
    signal_candle_timestamp: datetime
    details_json: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)

# --- NOWE SCHEMATY ZBIORCZE ---
class ConsolidatedTickerDetails(BaseModel):
    ticker: str
    phase1_data: Optional[Phase1Candidate] = None
    phase2_data: Optional[Phase2Result] = None
    phase3_signal: Optional[TradingSignal] = None
    on_demand_analysis: Optional[Dict[str, Any]] = None
    phase3_on_demand_analysis: Optional[Dict[str, Any]] = None

class LivePricesResponse(BaseModel):
    prices: Dict[str, Optional[float]]
