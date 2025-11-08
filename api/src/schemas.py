from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime, date
from typing import List, Optional, Any, Dict # Dodano Dict i Field

# === Schematy dla danych wejściowych transakcji ===

class TransactionBase(BaseModel):
    ticker: str = Field(..., description="Ticker spółki")
    quantity: int = Field(..., gt=0, description="Liczba akcji (musi być > 0)")
    price_per_share: float = Field(..., gt=0, description="Cena za jedną akcję (musi być > 0)")

class BuyRequest(TransactionBase):
    pass

class SellRequest(TransactionBase):
    pass


# === Schematy dla Portfela (PortfolioHolding) ===

class PortfolioHoldingBase(BaseModel):
    ticker: str
    quantity: int
    average_buy_price: float

class PortfolioHolding(PortfolioHoldingBase):
    first_purchase_date: datetime
    last_updated: datetime
    take_profit: Optional[float] = None # <-- POPRAWIONE POLE (Take Profit)

    model_config = ConfigDict(from_attributes=True) # Umożliwia tworzenie z obiektów ORM


# === Schematy dla Historii Transakcji (TransactionHistory) ===

class TransactionHistoryBase(BaseModel):
    ticker: str
    transaction_type: str # BUY lub SELL
    quantity: int
    price_per_share: float

class TransactionHistory(TransactionHistoryBase):
    id: int
    transaction_date: datetime
    profit_loss_usd: Optional[float] = None # Zysk/strata tylko dla SELL

    model_config = ConfigDict(from_attributes=True)


# ==========================================================
# === Pozostałe, istniejące schematy (bez zmian) ===
# ==========================================================

class AIAnalysisRequestResponse(BaseModel):
    message: str
    ticker: str

class OnDemandRequest(BaseModel):
    ticker: str

# ==========================================================
# === ZMIANA (Dynamiczny Rok): Schemat Zlecenia Backtestu ===
# ==========================================================
class BacktestRequest(BaseModel):
    # Zmieniono 'period_name' na 'year'
    year: str = Field(..., description="Rok do testowania (np. 2010)", min_length=4, max_length=4)
# ==========================================================

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

class AIAnalysisResult(BaseModel):
    # Jedyne pole wymagane w każdym stanie (PROCESSING, ERROR, DONE)
    status: str

    # Pola dla statusu "PROCESSING" lub "ERROR"
    message: Optional[str] = None

    # Pola dla statusu "DONE"
    ticker: Optional[str] = None
    quote_data: Optional[Dict[str, Any]] = None # Zmieniono na Dict[str, Any]
    market_info: Optional[Dict[str, Any]] = None # Zmieniono na Dict[str, Any]
    overall_score: Optional[int] = None
    max_score: Optional[int] = None
    final_score_percent: Optional[int] = None
    recommendation: Optional[str] = None
    recommendation_details: Optional[str] = None
    agents: Optional[Dict[str, Any]] = None # Zmieniono na Dict[str, Any]
    analysis_timestamp_utc: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

# ==========================================================
# === KROK 5 (Wirtualny Agent): Schematy dla Wyników Agenta ===
# ==========================================================

class VirtualTrade(BaseModel):
    """Schemat dla pojedynczej wirtualnej transakcji (wiersz z tabeli)"""
    id: int
    signal_id: Optional[int] = None
    ticker: str
    status: str
    setup_type: Optional[str] = None
    entry_price: float
    stop_loss: float
    take_profit: Optional[float] = None
    open_date: datetime
    close_date: Optional[datetime] = None
    close_price: Optional[float] = None
    final_profit_loss_percent: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class VirtualAgentStats(BaseModel):
    """Schemat dla zagregowanych statystyk wydajności"""
    total_trades: int
    win_rate_percent: float
    total_p_l_percent: float
    average_p_l_percent: float
    average_win_percent: float
    average_loss_percent: float
    profit_factor: float
    by_setup: Dict[str, Any] # Miejsce na statystyki per-strategia (np. {'EMA_BOUNCE': {'win_rate': 50.0, ...}})

class VirtualAgentReport(BaseModel):
    """Główny schemat odpowiedzi, łączący statystyki i listę transakcji"""
    stats: VirtualAgentStats
    trades: List[VirtualTrade]
