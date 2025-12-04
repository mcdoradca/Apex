from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime, date
from typing import List, Optional, Any, Dict

class TransactionBase(BaseModel):
    ticker: str = Field(..., description="Ticker spółki")
    quantity: int = Field(..., gt=0, description="Liczba akcji (musi być > 0)")
    price_per_share: float = Field(..., gt=0, description="Cena za jedną akcję (musi być > 0)")

class BuyRequest(TransactionBase):
    pass

class SellRequest(TransactionBase):
    pass

class PortfolioHoldingBase(BaseModel):
    ticker: str
    quantity: int
    average_buy_price: float

class PortfolioHolding(PortfolioHoldingBase):
    first_purchase_date: datetime
    last_updated: datetime
    take_profit: Optional[float] = None 
    notes: Optional[str] = None 

    model_config = ConfigDict(from_attributes=True)

class TransactionHistoryBase(BaseModel):
    ticker: str
    transaction_type: str 
    quantity: int
    price_per_share: float

class TransactionHistory(TransactionHistoryBase):
    id: int
    transaction_date: datetime
    profit_loss_usd: Optional[float] = None 

    model_config = ConfigDict(from_attributes=True)

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

class Phase1Candidate(BaseModel):
    ticker: str
    price: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None
    score: int
    sector_ticker: Optional[str] = None
    sector_trend_score: Optional[float] = None
    days_to_earnings: Optional[int] = None
    analysis_date: datetime

    model_config = ConfigDict(from_attributes=True)

class PhaseXCandidate(BaseModel):
    ticker: str
    price: Optional[float] = None
    volume_avg: Optional[int] = None
    pump_count_1y: int
    last_pump_date: Optional[date] = None
    last_pump_percent: Optional[float] = None
    analysis_date: datetime

    model_config = ConfigDict(from_attributes=True)

# === FAZA 4: KINETIC ALPHA (H4) - NOWOŚĆ ===
class Phase4Candidate(BaseModel):
    ticker: str
    price: Optional[float] = None
    kinetic_score: int
    elasticity: Optional[float] = None
    shots_30d: int
    avg_intraday_volatility: Optional[float] = None
    
    # Nowe metryki analityczne
    max_daily_shots: int
    total_2pct_shots_ytd: int # Tu mapujemy z kolumny total_2pct_shots_30d lub ytd w zależności od nazwy w DB
    avg_swing_size: Optional[float] = None
    hard_floor_violations: int
    
    last_shot_date: Optional[date] = None
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
    expiration_date: Optional[datetime] = None
    
    # === RE-CHECK DATA (Oczekiwania w Sygnale) ===
    expected_profit_factor: Optional[float] = None
    expected_win_rate: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)

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

    # === NOWOŚĆ: RE-CHECK RESULT (Raport Audytora) ===
    expected_profit_factor: Optional[float] = None
    expected_win_rate: Optional[float] = None
    ai_audit_report: Optional[str] = None
    ai_optimization_suggestion: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)

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
    by_setup: Dict[str, VirtualAgentSetupStats]

class VirtualAgentReport(BaseModel):
    stats: VirtualAgentStats 
    trades: List[VirtualTrade] 
    total_trades_count: int 

class BacktestRequest(BaseModel):
    year: str = Field(..., description="Rok do przetestowania, np. '2010'")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Dynamiczne parametry strategii")

class AIOptimizerRequest(BaseModel):
    pass

class AIOptimizerReport(BaseModel):
    status: str 
    report_text: Optional[str] = None
    last_updated: Optional[datetime] = None

class H3DeepDiveRequest(BaseModel):
    year: int = Field(..., description="Rok do analizy, np. 2023", ge=2000, le=2100)

class H3DeepDiveReport(BaseModel):
    status: str 
    report_text: Optional[str] = None
    last_updated: Optional[datetime] = None

class OptimizationRequest(BaseModel):
    target_year: int = Field(..., description="Rok optymalizacji", ge=2000, le=2100)
    n_trials: int = Field(default=50, description="Liczba prób", ge=10, le=5000)
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
    configuration: Optional[Dict[str, Any]] = None 

    model_config = ConfigDict(from_attributes=True)

class OptimizationJobDetail(OptimizationJob):
    trials: List[OptimizationTrial] = []
