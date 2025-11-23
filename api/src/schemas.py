from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime, date
from typing import List, Optional, Any, Dict

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
    take_profit: Optional[float] = None 

    model_config = ConfigDict(from_attributes=True)


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
# === Pozostałe, istniejące schematy ===
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


# ==========================================================
# KROK 5 (Wirtualny Agent): Schematy Raportu
# ==========================================================

class VirtualTrade(BaseModel):
    id: int
    ticker: str
    status: str
    setup_type: str
    entry_price: float
    stop_loss: float
    take_profit: float
    open_date: datetime
    close_date: Optional[datetime] = None
    close_price: Optional[float] = None
    final_profit_loss_percent: Optional[float] = None
    
    # Metryki (Głębokie Logowanie)
    metric_atr_14: Optional[float] = None
    metric_time_dilation: Optional[float] = None
    metric_price_gravity: Optional[float] = None
    metric_td_percentile_90: Optional[float] = None
    metric_pg_percentile_90: Optional[float] = None
    metric_inst_sync: Optional[float] = None
    metric_retail_herding: Optional[float] = None
    metric_aqm_score_h3: Optional[float] = None
    metric_aqm_percentile_95: Optional[float] = None
    metric_J_norm: Optional[float] = None
    metric_nabla_sq_norm: Optional[float] = None
    metric_m_sq_norm: Optional[float] = None
    metric_J: Optional[float] = None
    metric_J_threshold_2sigma: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)

# NOWOŚĆ: Ścisły model dla statystyk pojedynczego setupu
# To zapobiega wyciekom Decimal do JSON
class VirtualAgentSetupStats(BaseModel):
    total_trades: int
    win_rate_percent: float
    total_p_l_percent: float
    profit_factor: float

class VirtualAgentStats(BaseModel):
    total_trades: int
    win_rate_percent: float
    total_p_l_percent: float
    profit_factor: float
    # ZMIANA: Zamiast Dict[str, Any] używamy konkretnego modelu
    by_setup: Dict[str, VirtualAgentSetupStats]

class VirtualAgentReport(BaseModel):
    stats: VirtualAgentStats 
    trades: List[VirtualTrade] 
    total_trades_count: int 

# ==========================================================
# ZMIANA (Dynamiczne Parametry): Schemat Zlecenia Backtestu
# ==========================================================
class BacktestRequest(BaseModel):
    year: str = Field(..., description="Rok do przetestowania, np. '2010'")
    # NOWOŚĆ: Opcjonalny słownik parametrów
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Dynamiczne parametry strategii (np. progi H3)")

# ==========================================================
# === SCHEMATY (Mega Agent) ===
# ==========================================================

class AIOptimizerRequest(BaseModel):
    pass

class AIOptimizerReport(BaseModel):
    status: str 
    report_text: Optional[str] = None
    last_updated: Optional[datetime] = None

# ==========================================================
# === SCHEMATY (H3 Deep Dive) ===
# ==========================================================

class H3DeepDiveRequest(BaseModel):
    year: int = Field(..., description="Rok do analizy, np. 2023", ge=2000, le=2100)

class H3DeepDiveReport(BaseModel):
    status: str 
    report_text: Optional[str] = None
    last_updated: Optional[datetime] = None

# ==========================================================
# === NOWOŚĆ: SCHEMATY DLA APEX V4 (Quantum Optimization) ===
# ==========================================================

class OptimizationRequest(BaseModel):
    target_year: int = Field(..., description="Rok, na którym ma być przeprowadzona optymalizacja (np. 2023)", ge=2000, le=2100)
    # ZMIANA: Zwiększono limit le=500 na le=5000 dla planu Standard
    n_trials: int = Field(default=50, description="Liczba prób algorytmu Optuna", ge=10, le=5000)
    # Opcjonalnie można nadpisać domyślną przestrzeń poszukiwań
    parameter_space: Optional[Dict[str, Any]] = None

class OptimizationTrial(BaseModel):
    id: int
    trial_number: int
    params: Dict[str, Any]
    profit_factor: Optional[float] = None
    total_trades: Optional[int] = None
    win_rate: Optional[float] = None
    net_profit: Optional[float] = None
    state: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

class OptimizationJob(BaseModel):
    id: str
    status: str
    target_year: int
    total_trials: int
    best_score: Optional[float] = None
    created_at: datetime
    # Dodano pole configuration, aby przekazać wyniki analizy wrażliwości do frontendu
    configuration: Optional[Dict[str, Any]] = None 

    model_config = ConfigDict(from_attributes=True)

class OptimizationJobDetail(OptimizationJob):
    """Pełny widok zadania wraz z listą wszystkich prób"""
    trials: List[OptimizationTrial] = []
