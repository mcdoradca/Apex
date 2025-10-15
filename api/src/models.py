from sqlalchemy import (
    Column, String, VARCHAR, TIMESTAMP, NUMERIC, BIGINT, DATE,
    Boolean, INTEGER, TEXT, ForeignKey
)
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
from sqlalchemy.sql import func
from .database import Base

class Company(Base):
    __tablename__ = 'companies'
    ticker = Column(VARCHAR(50), primary_key=True)
    company_name = Column(VARCHAR(255))
    exchange = Column(VARCHAR(50))
    sector = Column(VARCHAR(100))
    industry = Column(VARCHAR(255))
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class Phase1Candidate(Base):
    __tablename__ = 'phase1_candidates'
    ticker = Column(VARCHAR(50), primary_key=True)
    price = Column(NUMERIC(12, 4))
    change_percent = Column(NUMERIC(10, 4))
    volume = Column(BIGINT)
    score = Column(INTEGER)
    analysis_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

# Poprawiona tabela dla Fazy 2
class Phase2Result(Base):
    __tablename__ = 'phase2_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_date = Column(DATE, primary_key=True)
    catalyst_score = Column(INTEGER)
    relative_strength_score = Column(INTEGER) # Dodana brakująca kolumna
    energy_compression_score = Column(INTEGER) # Dodana brakująca kolumna
    total_score = Column(INTEGER)
    is_qualified = Column(Boolean)

class TradingSignal(Base):
    __tablename__ = 'trading_signals'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'))
    generation_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    status = Column(VARCHAR(50), default='ACTIVE') # ACTIVE, EXECUTED, CANCELLED, DELETED
    entry_price = Column(NUMERIC(12, 2))
    stop_loss = Column(NUMERIC(12, 2))
    take_profit = Column(NUMERIC(12, 2))
    risk_reward_ratio = Column(NUMERIC(5, 2))
    signal_candle_timestamp = Column(PG_TIMESTAMP(timezone=True))
    notes = Column(TEXT)

class SystemControl(Base):
    __tablename__ = 'system_control'
    key = Column(VARCHAR(50), primary_key=True)
    value = Column(TEXT)
    updated_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class OnDemandAnalysisResult(Base):
    __tablename__ = 'on_demand_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class Phase3OnDemandResult(Base):
    __tablename__ = 'phase3_on_demand_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

