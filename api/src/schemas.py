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

# ==========================================================
# === DEKONSTRUKCJA (KROK 8) ===
# Usunięto schematy `AIAnalysisRequestResponse`, `OnDemandRequest`
# oraz `AIAnalysisResult`, ponieważ były powiązane
# z usuniętymi endpointami API.
# ==========================================================
# class AIAnalysisRequestResponse(BaseModel):
#     message: str
#     ticker: str
#
# class OnDemandRequest(BaseModel):
#     ticker: str
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
    generation_date: datetime # Zmieniono na datetime dla spójności
    status: str
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_reward_ratio: Optional[float] = None
    signal_candle_timestamp: Optional[datetime] = None # Zmieniono na datetime
    entry_zone_bottom: Optional[float] = None
    entry_zone_top: Optional[float] = None
    notes: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

# ==========================================================
# class AIAnalysisResult(BaseModel):
#     ... (USUNIĘTE) ...
# ==========================================================


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
    
    # ==========================================================
    # === AKTUALIZACJA (GŁĘBOKIE LOGOWANIE) ===
    # Dodajemy wszystkie nowe pola metryk, aby API mogło je odczytać
    # ==========================================================
    
    # Wspólne
    metric_atr_14: Optional[float] = None
    
    # H1
    metric_time_dilation: Optional[float] = None
    metric_price_gravity: Optional[float] = None
    metric_td_percentile_90: Optional[float] = None
    metric_pg_percentile_90: Optional[float] = None

    # H2
    metric_inst_sync: Optional[float] = None
    metric_retail_herding: Optional[float] = None
    
    # H3
    metric_aqm_score_h3: Optional[float] = None
    metric_aqm_percentile_95: Optional[float] = None
    metric_J_norm: Optional[float] = None
    metric_nabla_sq_norm: Optional[float] = None
    metric_m_sq_norm: Optional[float] = None
    
    # H4
    metric_J: Optional[float] = None
    metric_J_threshold_2sigma: Optional[float] = None
    # ==========================================================

    model_config = ConfigDict(from_attributes=True)

class VirtualAgentStats(BaseModel):
    total_trades: int
    win_rate_percent: float
    total_p_l_percent: float
    profit_factor: float
    by_setup: Dict[str, Any] # Tu będą statystyki dla każdego setup_type

# ==========================================================
# === AKTUALIZACJA (STRONICOWANIE) ===
# Schemat raportu jest teraz podzielony
# ==========================================================
class VirtualAgentReport(BaseModel):
    stats: VirtualAgentStats # Statystyki są globalne
    trades: List[VirtualTrade] # To będzie tylko bieżąca strona transakcji
    total_trades_count: int # Całkowita liczba transakcji (dla paginacji)
# ==========================================================


# ==========================================================
# ZMIANA (Dynamiczny Rok): Schemat Zlecenia Backtestu
# ==========================================================
class BacktestRequest(BaseModel):
    year: str = Field(..., description="Rok do przetestowania, np. '2010'")

# ==========================================================
# === NOWE SCHEMATY (Krok 2 - Mega Agent) ===
# ==========================================================

class AIOptimizerRequest(BaseModel):
    """
    Puste ciało, sam fakt wysłania tego żądania
    jest traktowany jako zlecenie analizy.
    """
    pass

class AIOptimizerReport(BaseModel):
    """
    Schemat odpowiedzi zwracający raport tekstowy od Mega Agenta.
    """
    status: str # np. 'DONE', 'PROCESSING', 'NONE'
    report_text: Optional[str] = None
    last_updated: Optional[datetime] = None
