from sqlalchemy import (
    Column, String, VARCHAR, TIMESTAMP, NUMERIC, BIGINT, DATE,
    Boolean, INTEGER, TEXT, ForeignKey, Index, func, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
from .database import Base

# === TABELA SPÓŁEK (FUNDAMENTALNA) ===
class Company(Base):
    __tablename__ = 'companies'
    ticker = Column(VARCHAR(50), primary_key=True)
    company_name = Column(VARCHAR(255))
    exchange = Column(VARCHAR(50))
    sector = Column(VARCHAR(100))
    industry = Column(VARCHAR(255))
    sector_etf = Column(VARCHAR(10), nullable=True)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

# === FAZA 1: KANDYDACI EOD ===
class Phase1Candidate(Base):
    __tablename__ = 'phase1_candidates'
    ticker = Column(VARCHAR(50), primary_key=True)
    price = Column(NUMERIC(12, 4))
    change_percent = Column(NUMERIC(10, 4))
    volume = Column(BIGINT)
    score = Column(INTEGER)
    sector_ticker = Column(VARCHAR(10), nullable=True)
    sector_trend_score = Column(NUMERIC(5, 2), nullable=True)
    days_to_earnings = Column(INTEGER, nullable=True)
    analysis_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

# === FAZA X: KANDYDACI BIOX (PUMP HUNTER) ===
class PhaseXCandidate(Base):
    __tablename__ = 'phasex_candidates'
    ticker = Column(VARCHAR(50), primary_key=True)
    price = Column(NUMERIC(12, 4))
    volume_avg = Column(BIGINT, nullable=True)
    pump_count_1y = Column(INTEGER, default=0, comment="Ile razy urosła >20% w ciągu roku")
    last_pump_date = Column(DATE, nullable=True, comment="Data ostatniego skoku")
    last_pump_percent = Column(NUMERIC(10, 2), nullable=True, comment="Wielkość ostatniego skoku w %")
    analysis_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

# === FAZA SDAR: SYSTEM DETEKCJI ANOMALII RYNKOWYCH (POPRAWIONA) ===
class SdarCandidate(Base):
    __tablename__ = 'sdar_candidates'
    ticker = Column(VARCHAR(50), primary_key=True)
    
    # Wyniki Główne
    sai_score = Column(NUMERIC(10, 4), comment="Silent Accumulation Index")
    spd_score = Column(NUMERIC(10, 4), comment="Sentiment-Price Divergence")
    me_score  = Column(NUMERIC(10, 4), comment="Momentum Exhaustion Score (Nowość)")
    total_anomaly_score = Column(NUMERIC(10, 4))

    # Komponenty SAI (Techniczne)
    atr_compression = Column(NUMERIC(10, 4))
    obv_slope = Column(NUMERIC(10, 4))
    price_stability = Column(NUMERIC(10, 4))
    smart_money_flow = Column(NUMERIC(12, 4)) # VWAP based logic

    # Komponenty SPD (Sentymentalne)
    sentiment_shock = Column(NUMERIC(10, 4))
    news_volume_spike = Column(NUMERIC(10, 4))
    price_resilience = Column(NUMERIC(10, 4))
    last_sentiment_score = Column(NUMERIC(5, 4))
    
    # Komponenty ME (Momentum Exhaustion) - Zgodne z PDF 2.3
    metric_rsi = Column(NUMERIC(10, 4), comment="RSI 14 na interwale 4H")
    metric_apo = Column(NUMERIC(10, 4), comment="Absolute Price Oscillator")

    analysis_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

# === FAZA 4: KANDYDACI KINETIC ALPHA (H4) ===
class Phase4Candidate(Base):
    __tablename__ = 'phase4_candidates'
    ticker = Column(VARCHAR(50), primary_key=True)
    price = Column(NUMERIC(12, 4))
    
    kinetic_score = Column(INTEGER, comment="Ocena energii kinetycznej (0-100)")
    elasticity = Column(NUMERIC(10, 4), comment="Wskaźnik sprężystości")
    shots_30d = Column(INTEGER, default=0, comment="Strzały >2% w 30 dni")
    avg_intraday_volatility = Column(NUMERIC(10, 4))
    
    max_daily_shots = Column(INTEGER, default=0)
    total_2pct_shots_ytd = Column(INTEGER, default=0)
    avg_swing_size = Column(NUMERIC(10, 2))
    hard_floor_violations = Column(INTEGER, default=0)
    
    last_shot_date = Column(DATE, nullable=True)
    analysis_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

# === FAZA 2: WYNIKI (KOMPATYBILNOŚĆ WSTECZNA) ===
class Phase2Result(Base):
    __tablename__ = 'phase2_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_date = Column(DATE, primary_key=True)
    catalyst_score = Column(INTEGER)
    relative_strength_score = Column(INTEGER)
    energy_compression_score = Column(INTEGER)
    total_score = Column(INTEGER)
    is_qualified = Column(Boolean)

# === SYGNAŁY TRADINGOWE (LIVE) ===
class TradingSignal(Base):
    __tablename__ = 'trading_signals'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'))
    generation_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())
    status = Column(VARCHAR(50), default='PENDING') 
    entry_price = Column(NUMERIC(12, 2), nullable=True)
    stop_loss = Column(NUMERIC(12, 2), nullable=True)
    take_profit = Column(NUMERIC(12, 2), nullable=True)
    risk_reward_ratio = Column(NUMERIC(5, 2), nullable=True)
    signal_candle_timestamp = Column(PG_TIMESTAMP(timezone=True), nullable=True)
    
    # Strefy wejścia (dla setupów, które nie są 'punktowe')
    entry_zone_bottom = Column(NUMERIC(12, 2), nullable=True)
    entry_zone_top = Column(NUMERIC(12, 2), nullable=True)
    
    notes = Column(TEXT, nullable=True)
    
    # Zarządzanie pozycją
    highest_price_since_entry = Column(NUMERIC(12, 2), nullable=True)
    is_trailing_active = Column(Boolean, default=False)
    earnings_date = Column(DATE, nullable=True)
    expiration_date = Column(PG_TIMESTAMP(timezone=True), nullable=True)
    
    # Oczekiwania (dla Re-check Audytora)
    expected_profit_factor = Column(NUMERIC(10, 4), nullable=True)
    expected_win_rate = Column(NUMERIC(10, 4), nullable=True)
    
    __table_args__ = (
        Index('uq_active_pending_ticker', 'ticker', unique=True, postgresql_where=status.in_(['ACTIVE', 'PENDING'])),
    )

# === STEROWANIE SYSTEMEM I STAN ===
class SystemControl(Base):
    __tablename__ = 'system_control'
    key = Column(VARCHAR(50), primary_key=True)
    value = Column(TEXT)
    updated_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class AIAnalysisResult(Base):
    __tablename__ = 'ai_analysis_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class ProcessedNews(Base):
    __tablename__ = 'processed_news'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), nullable=False, index=True)
    news_hash = Column(VARCHAR(64), nullable=False, index=True)
    processed_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    sentiment = Column(VARCHAR(50), nullable=False)
    headline = Column(TEXT, nullable=True)
    source_url = Column(TEXT, nullable=True)
    __table_args__ = (UniqueConstraint('ticker', 'news_hash', name='uq_ticker_news_hash'),)

# === PORTFEL INWESTYCYJNY (LIVE) ===
class PortfolioHolding(Base):
    __tablename__ = 'portfolio_holdings'
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    quantity = Column(INTEGER, nullable=False)
    average_buy_price = Column(NUMERIC(12, 4), nullable=False)
    first_purchase_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class TransactionHistory(Base):
    __tablename__ = 'transaction_history'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='SET NULL'), nullable=True) 
    transaction_type = Column(VARCHAR(10), nullable=False) 
    quantity = Column(INTEGER, nullable=False)
    price_per_share = Column(NUMERIC(12, 4), nullable=False)
    transaction_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    related_portfolio_ticker = Column(VARCHAR(50), nullable=True)
    profit_loss_usd = Column(NUMERIC(14, 2), nullable=True)

# === WIRTUALNY PORTFEL I BACKTEST (SYMULACJA) ===
class VirtualTrade(Base):
    __tablename__ = 'virtual_trades'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    signal_id = Column(INTEGER, ForeignKey('trading_signals.id', ondelete='SET NULL'), nullable=True)
    ticker = Column(VARCHAR(50), nullable=False, index=True)
    status = Column(VARCHAR(50), nullable=False, default='OPEN', index=True)
    setup_type = Column(VARCHAR(100), nullable=True)
    entry_price = Column(NUMERIC(12, 2), nullable=False)
    stop_loss = Column(NUMERIC(12, 2), nullable=False)
    take_profit = Column(NUMERIC(12, 2), nullable=True)
    open_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    close_date = Column(PG_TIMESTAMP(timezone=True), nullable=True)
    close_price = Column(NUMERIC(12, 2), nullable=True)
    final_profit_loss_percent = Column(NUMERIC(8, 2), nullable=True)
    
    # Metryki H1-H3 (AQM Legacy & V3)
    metric_atr_14 = Column(NUMERIC(10, 4), nullable=True)
    metric_time_dilation = Column(NUMERIC(10, 4), nullable=True)
    metric_price_gravity = Column(NUMERIC(10, 4), nullable=True)
    metric_td_percentile_90 = Column(NUMERIC(10, 4), nullable=True)
    metric_pg_percentile_90 = Column(NUMERIC(10, 4), nullable=True)
    metric_inst_sync = Column(NUMERIC(10, 4), nullable=True)
    metric_retail_herding = Column(NUMERIC(10, 4), nullable=True)
    metric_aqm_score_h3 = Column(NUMERIC(10, 4), nullable=True)
    metric_aqm_percentile_95 = Column(NUMERIC(10, 4), nullable=True)
    metric_J_norm = Column(NUMERIC(10, 4), nullable=True)
    metric_nabla_sq_norm = Column(NUMERIC(10, 4), nullable=True)
    metric_m_sq_norm = Column(NUMERIC(10, 4), nullable=True)
    metric_J = Column(NUMERIC(10, 4), nullable=True)
    metric_J_threshold_2sigma = Column(NUMERIC(10, 4), nullable=True)
    
    # Metryki H4 (Kinetic Alpha)
    metric_kinetic_energy = Column(NUMERIC(10, 4), nullable=True)
    metric_elasticity = Column(NUMERIC(10, 4), nullable=True)

    # Metryki F5 (Omni-Flux)
    metric_flux_score = Column(NUMERIC(10, 4), nullable=True)
    metric_flux_velocity = Column(NUMERIC(10, 4), nullable=True)
    metric_flux_ofp = Column(NUMERIC(10, 4), nullable=True)

    # Audyt AI (Re-check)
    expected_profit_factor = Column(NUMERIC(10, 4), nullable=True)
    expected_win_rate = Column(NUMERIC(10, 4), nullable=True)
    ai_audit_report = Column(TEXT, nullable=True)
    ai_audit_date = Column(PG_TIMESTAMP(timezone=True), nullable=True)
    ai_optimization_suggestion = Column(JSONB, nullable=True)

# === CACHE ALPHA VANTAGE ===
class AlphaVantageCache(Base):
    __tablename__ = 'alpha_vantage_cache'
    ticker = Column(VARCHAR(50), primary_key=True, nullable=False, index=True)
    data_type = Column(VARCHAR(50), primary_key=True, nullable=False)
    raw_data_json = Column(JSONB, nullable=False)
    last_fetched = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())
    __table_args__ = (UniqueConstraint('ticker', 'data_type', name='uq_av_cache_entry'),)

# === OPTYMALIZATOR (QUANTUM JOB) ===
class OptimizationJob(Base):
    __tablename__ = 'optimization_jobs'
    id = Column(String(36), primary_key=True)
    created_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    status = Column(String(20), default='PENDING')
    target_year = Column(INTEGER, nullable=False)
    total_trials = Column(INTEGER, nullable=False)
    best_trial_id = Column(INTEGER, nullable=True)
    best_score = Column(NUMERIC(10, 4), nullable=True)
    configuration = Column(JSONB, nullable=True) 

class OptimizationTrial(Base):
    __tablename__ = 'optimization_trials'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey('optimization_jobs.id', ondelete='CASCADE'), nullable=False, index=True)
    trial_number = Column(INTEGER, nullable=False)
    params = Column(JSONB, nullable=False)
    profit_factor = Column(NUMERIC(10, 4), nullable=True)
    total_trades = Column(INTEGER, nullable=True)
    win_rate = Column(NUMERIC(10, 4), nullable=True)
    net_profit = Column(NUMERIC(14, 2), nullable=True)
    state = Column(String(20), default='COMPLETE')
    created_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
