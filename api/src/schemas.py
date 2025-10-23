from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime, date
from typing import List, Optional, Any, Dict

# === Schematy dla danych wejściowych transakcji ===

class TransactionBase(BaseModel):
    ticker: str = Field(..., description="Ticker spółki", examples=["AAPL"])
    quantity: int = Field(..., gt=0, description="Liczba akcji (musi być > 0)", examples=[10])
    # Używamy float dla Pydantic, CRUD użyje Decimal
    price_per_share: float = Field(..., gt=0, description="Cena za jedną akcję (musi być > 0)", examples=[175.50])

class BuyRequest(TransactionBase):
    pass

class SellRequest(TransactionBase):
    pass


# === Schematy dla Portfela (PortfolioHolding) ===

class PortfolioHoldingBase(BaseModel):
    ticker: str
    quantity: int
    # float jest OK, bo ORM/CRUD używa Decimal w bazie
    average_buy_price: float

class PortfolioHolding(PortfolioHoldingBase):
    # Używamy datetime
    first_purchase_date: datetime
    last_updated: datetime

    model_config = ConfigDict(from_attributes=True)


# === Schematy dla Historii Transakcji (TransactionHistory) ===

class TransactionHistoryBase(BaseModel):
    ticker: str
    transaction_type: str # BUY lub SELL
    quantity: int
    price_per_share: float # float jest OK

class TransactionHistory(TransactionHistoryBase):
    id: int
    transaction_date: datetime
    profit_loss_usd: Optional[float] = None # float jest OK

    model_config = ConfigDict(from_attributes=True)


# === Schematy dla Danych Cenowych ===

class LiveQuoteSession(BaseModel):
    price: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

class LiveQuoteDetails(BaseModel):
    symbol: str
    # Używamy aliasu, aby JSON miał 'market_status', ale wewnętrznie jest inna nazwa
    market_status_internal: str = Field(..., alias="market_status")
    regular_session: LiveQuoteSession
    extended_session: LiveQuoteSession
    live_price: Optional[float] = None
    # Dodajemy pola czasu obliczone przez workera
    time_ny: Optional[str] = None
    date_ny: Optional[str] = None
    # Dodajemy czas ostatniej aktualizacji z cache
    last_updated_utc: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# === Schematy dla Kontroli i Statusu ===

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
    last_heartbeat_utc: str # Pozostaje stringiem ISO
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
    analysis_date: datetime # Używamy datetime

    model_config = ConfigDict(from_attributes=True)

class Phase2Result(BaseModel):
    ticker: str
    analysis_date: date # Pozostaje date
    catalyst_score: int
    relative_strength_score: int
    energy_compression_score: int
    total_score: int
    is_qualified: bool

    model_config = ConfigDict(from_attributes=True)

class TradingSignal(BaseModel):
    id: int
    ticker: str
    generation_date: datetime # Używamy datetime
    status: str
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    signal_candle_timestamp: Optional[datetime] = None # Używamy datetime
    entry_zone_bottom: Optional[float] = None
    entry_zone_top: Optional[float] = None
    notes: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

class AIAnalysisResult(BaseModel):
    # Jedyne pole wymagane w każdym stanie (PROCESSING, ERROR, DONE)
    status: str

    # Pola dla statusu "PROCESSING" lub "ERROR"
    message: Optional[str] = None

    # Pola dla statusu "DONE"
    ticker: Optional[str] = None
    # Używamy zagnieżdżonego schematu LiveQuoteDetails dla walidacji danych z cache
    quote_data: Optional[LiveQuoteDetails] = None
    market_info: Optional[Dict[str, Any]] = None # To pole jest teraz mniej istotne, bo dane są w quote_data
    overall_score: Optional[int] = None
    max_score: Optional[int] = None
    final_score_percent: Optional[int] = None
    recommendation: Optional[str] = None
    recommendation_details: Optional[str] = None
    agents: Optional[Dict[str, Any]] = None
    analysis_timestamp_utc: Optional[str] = None # Pozostaje string ISO

    model_config = ConfigDict(from_attributes=True)

