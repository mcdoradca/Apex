from sqlalchemy import (
    Column, String, VARCHAR, TIMESTAMP, NUMERIC, BIGINT, DATE,
    Boolean, INTEGER, TEXT, ForeignKey, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
from sqlalchemy.sql import func
from .database import Base

# Kompletna definicja wszystkich tabel bazy danych jako modeli SQLAlchemy.

class Company(Base):
# ... existing code ...
    industry = Column(VARCHAR(255))
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class PriceHistoryDaily(Base):
# ... existing code ...
    dividend_amount = Column(NUMERIC(10, 4))
    split_coefficient = Column(NUMERIC(10, 4))

class PriceHistoryIntraday(Base):
# ... existing code ...
    low = Column(NUMERIC(12, 4))
    close = Column(NUMERIC(12, 4))
    volume = Column(BIGINT)

class Fundamentals(Base):
# ... existing code ...
    dividend_yield = Column(NUMERIC(10, 4))
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class SentimentAnalysis(Base):
# ... existing code ...
    sentiment_score = Column(NUMERIC(5, 4))
    relevance_score = Column(NUMERIC(5, 4))

class ApexScore(Base):
# ... existing code ...
    total_score = Column(INTEGER)
    is_qualified = Column(Boolean)

class TradingSignal(Base):
# ... existing code ...
    risk_reward_ratio = Column(NUMERIC(5, 2))
    signal_candle_timestamp = Column(PG_TIMESTAMP(timezone=True))

class PredatorWatchlist(Base):
# ... existing code ...
    ticker = Column(VARCHAR(10), unique=True, nullable=False)
    added_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

class SystemControl(Base):
# ... existing code ...
    key = Column(VARCHAR(50), primary_key=True)
    value = Column(TEXT)
    updated_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class AlphaVantageMetadata(Base):
# ... existing code ...
    api_function = Column(VARCHAR(100))
    field_name_api = Column(VARCHAR(100))

# NOWA TABELA DO PRZECHOWYWANIA WYNIKÓW ANALIZY NA ŻĄDANIE
class OnDemandAnalysisResult(Base):
    __tablename__ = 'on_demand_results'
    ticker = Column(VARCHAR(10), primary_key=True)
    analysis_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())
