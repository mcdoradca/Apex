from sqlalchemy import (
    Column, String, VARCHAR, TIMESTAMP, NUMERIC, BIGINT, DATE,
    Boolean, INTEGER, TEXT, ForeignKey
)
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
from sqlalchemy.sql import func
from .database import Base

# Definicja tabeli 'companies' - przechowuje listę wszystkich spółek
class Company(Base):
    __tablename__ = 'companies'
    ticker = Column(VARCHAR(50), primary_key=True)
    company_name = Column(VARCHAR(255))
    exchange = Column(VARCHAR(50))
    sector = Column(VARCHAR(100))
    industry = Column(VARCHAR(255))
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

# ZMODYFIKOWANA TABELA: Przechowuje wyniki Fazy 1 z dodatkowymi danymi
class Phase1Candidate(Base):
    __tablename__ = 'phase1_candidates'
    ticker = Column(VARCHAR(50), primary_key=True)
    price = Column(NUMERIC(12, 4))
    change_percent = Column(NUMERIC(10, 4))
    volume = Column(BIGINT)
    score = Column(INTEGER) # Zachowane dla spójności
    analysis_date = Column(DATE, server_default=func.now(), primary_key=True)

# NOWA TABELA: Przechowuje wyniki scoringu Fazy 2
class Phase2Result(Base):
    __tablename__ = 'phase2_results'
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    analysis_date = Column(DATE, primary_key=True)
    momentum_score = Column(INTEGER)
    compression_score = Column(INTEGER)
    catalyst_score = Column(INTEGER)
    total_score = Column(INTEGER)
    is_qualified = Column(Boolean)

# ... reszta modeli bez zmian, ale dodane dla kompletności ...

class PriceHistoryDaily(Base):
    __tablename__ = 'price_history_daily'
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    date = Column(DATE, primary_key=True)
    open = Column(NUMERIC(12, 4))
    high = Column(NUMERIC(12, 4))
    low = Column(NUMERIC(12, 4))
    close = Column(NUMERIC(12, 4))
    adjusted_close = Column(NUMERIC(12, 4))
    volume = Column(BIGINT)
    dividend_amount = Column(NUMERIC(10, 4))
    split_coefficient = Column(NUMERIC(10, 4))

class Fundamentals(Base):
    __tablename__ = 'fundamentals'
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    market_cap = Column(BIGINT)
    pe_ratio = Column(NUMERIC(10, 2))
    eps = Column(NUMERIC(10, 2))
    profit_margin = Column(NUMERIC(10, 4))
    dividend_yield = Column(NUMERIC(10, 4))
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class TradingSignal(Base):
    __tablename__ = 'trading_signals'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'))
    generation_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    status = Column(VARCHAR(50), default='ACTIVE')
    entry_price = Column(NUMERIC(12, 2))
    stop_loss = Column(NUMERIC(12, 2))
    take_profit = Column(NUMERIC(12, 2))
    risk_reward_ratio = Column(NUMERIC(5, 2))
    signal_candle_timestamp = Column(PG_TIMESTAMP(timezone=True))
    # Dodajemy pole na szczegóły, np. użyty ATR
    details_json = Column(JSONB)


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

# NOWA TABELA: Do przechowywania wyników Fazy 3 na żądanie
class Phase3OnDemandResult(Base):
    __tablename__ = 'phase3_on_demand_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())
