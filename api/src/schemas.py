from pydantic import BaseModel, ConfigDict
from datetime import datetime, date
from typing import List, Optional

# --- NOWY ELEMENT ---
# Schemat dla pojedynczego kandydata z Fazy 1
class Phase1Candidate(BaseModel):
    ticker: str
    price: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None
    score: Optional[int] = None
    analysis_date: datetime

    model_config = ConfigDict(from_attributes=True)

# ... reszta istniejącego kodu bez zmian ...
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
    analysis_date: date
    total_score: int

    model_config = ConfigDict(from_attributes=True)

