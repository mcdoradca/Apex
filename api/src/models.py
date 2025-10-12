from sqlalchemy import (
    Column, String, VARCHAR, TIMESTAMP, NUMERIC, BIGINT, DATE,
    Boolean, INTEGER, TEXT, ForeignKey, Float
)
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
from sqlalchemy.sql import func
from .database import Base

# --- NOWA TABELA ---
# Tabela do przechowywania wyników z Fazy 1
class Phase1Candidate(Base):
    __tablename__ = 'phase1_candidates'
    ticker = Column(VARCHAR(50), primary_key=True)
    price = Column(Float)
    change_percent = Column(Float)
    volume = Column(BIGINT)
    score = Column(INTEGER, default=0) # Domyślnie 0, do rozbudowy
    analysis_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

# ... reszta istniejącego kodu bez zmian ...
class Company(Base):
    __tablename__ = 'companies'
    ticker = Column(VARCHAR(50), primary_key=True)
    company_name = Column(VARCHAR(255))
    exchange = Column(VARCHAR(50))
    sector = Column(VARCHAR(100))
    industry = Column(VARCHAR(255))
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

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

class PriceHistoryIntraday(Base):
    __tablename__ = 'price_history_intraday'
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    datetime = Column(PG_TIMESTAMP(timezone=True), primary_key=True)
    open = Column(NUMERIC(12, 4))
    high = Column(NUMERIC(12, 4))
    low = Column(NUMERIC(12, 4))
    close = Column(NUMERIC(12, 4))
    volume = Column(BIGINT)

class Fundamentals(Base):
    __tablename__ = 'fundamentals'
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    market_cap = Column(BIGINT)
    pe_ratio = Column(NUMERIC(10, 2))
    eps = Column(NUMERIC(10, 2))
    profit_margin = Column(NUMERIC(10, 4))
    dividend_yield = Column(NUMERIC(10, 4))
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class SentimentAnalysis(Base):
    __tablename__ = 'sentiment_analysis'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'))
    publish_time = Column(PG_TIMESTAMP(timezone=True))
    url = Column(TEXT)
    title = Column(TEXT)
    summary = Column(TEXT)
    sentiment_score = Column(NUMERIC(5, 4))
    relevance_score = Column(NUMERIC(5, 4))

class ApexScore(Base):
    __tablename__ = 'apex_scores'
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    analysis_date = Column(DATE, primary_key=True)
    catalyst_score = Column(INTEGER)
    relative_strength_score = Column(INTEGER)
    energy_compression_score = Column(INTEGER)
    quality_control_score = Column(INTEGER)
    total_score = Column(INTEGER)
    is_qualified = Column(Boolean)

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

class PredatorWatchlist(Base):
    __tablename__ = 'predator_watchlist'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), unique=True, nullable=False)
    added_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

class SystemControl(Base):
    __tablename__ = 'system_control'
    key = Column(VARCHAR(50), primary_key=True)
    value = Column(TEXT)
    updated_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class AlphaVantageMetadata(Base):
    __tablename__ = 'alpha_vantage_metadata'
    field_name_app = Column(VARCHAR(100), primary_key=True)
    api_function = Column(VARCHAR(100))
    field_name_api = Column(VARCHAR(100))

class OnDemandAnalysisResult(Base):
    __tablename__ = 'on_demand_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

