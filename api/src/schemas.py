from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import List, Optional

# Schemat dla nowej tabeli Kandydatów Fazy 1
class Phase1Candidate(BaseModel):
    ticker: str
    price: float
    change_percent: float
    volume: int
    score: int
    analysis_date: datetime

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

    model_config = ConfigDict(from_attributes=True)

class ApexScore(BaseModel):
    ticker: str
    analysis_date: datetime
    total_score: int
    is_qualified: bool

    model_config = ConfigDict(from_attributes=True)

