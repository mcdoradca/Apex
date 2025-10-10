from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import List, Optional

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

    # POPRAWKA: Zaktualizowano konfigurację Pydantic v2, 'orm_mode' to 'from_attributes'
    model_config = ConfigDict(from_attributes=True)

