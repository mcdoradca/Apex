from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

# Schematy Pydantic do walidacji danych wejściowych i wyjściowych API.

class Progress(BaseModel):
    processed: int
    total: int

class WorkerStatus(BaseModel):
    status: str
    phase: str
    progress: Progress
    last_heartbeat_utc: datetime
    log: str

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

    class Config:
        orm_mode = True

