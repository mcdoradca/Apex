from sqlalchemy import (
    Column, String, VARCHAR, TIMESTAMP, NUMERIC, BIGINT, DATE,
    Boolean, INTEGER, TEXT, ForeignKey, Index, func # Dodano import func
)
# ZMIANA: Import PG_TIMESTAMP bezpośrednio
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
# Usunięto import func stąd, bo jest wyżej
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

class Phase2Result(Base):
    __tablename__ = 'phase2_results'
    ticker = Column(VARCHAR(50), primary_key=True)
    analysis_date = Column(DATE, primary_key=True)
    catalyst_score = Column(INTEGER)
    relative_strength_score = Column(INTEGER)
    energy_compression_score = Column(INTEGER)
    total_score = Column(INTEGER)
    is_qualified = Column(Boolean)

class TradingSignal(Base):
    __tablename__ = 'trading_signals'
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'))
    generation_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    status = Column(VARCHAR(50), default='PENDING') # PENDING, ACTIVE, TRIGGERED, EXPIRED, CANCELLED
    entry_price = Column(NUMERIC(12, 2), nullable=True)
    stop_loss = Column(NUMERIC(12, 2), nullable=True)
    take_profit = Column(NUMERIC(12, 2), nullable=True)
    risk_reward_ratio = Column(NUMERIC(5, 2), nullable=True)
    signal_candle_timestamp = Column(PG_TIMESTAMP(timezone=True), nullable=True)
    entry_zone_bottom = Column(NUMERIC(12, 2), nullable=True)
    entry_zone_top = Column(NUMERIC(12, 2), nullable=True)
    notes = Column(TEXT, nullable=True)

    __table_args__ = (
        Index(
            'uq_active_pending_trigger_ticker', # Zmieniono nazwę indeksu dla jasności
            'ticker',
            unique=True,
            # Dodano TRIGGERED do warunku, aby zapobiec duplikatom przed wygaśnięciem
            postgresql_where=text("status IN ('ACTIVE', 'PENDING', 'TRIGGERED')") # Poprawiony syntax dla postgresql_where
        ),
    )

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


# === MODELE DLA PORTFELA I HISTORII TRANSAKCJI ===
# (Te modele są teraz spójne z worker/src/models.py)

class PortfolioHolding(Base):
    """
    Tabela przechowująca aktualnie otwarte pozycje w portfelu.
    """
    __tablename__ = 'portfolio_holdings'

    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), primary_key=True)
    quantity = Column(INTEGER, nullable=False, comment="Całkowita liczba posiadanych akcji")
    average_buy_price = Column(NUMERIC(12, 4), nullable=False, comment="Średnia ważona cena zakupu za akcję")
    first_purchase_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), comment="Data pierwszego zakupu tej pozycji")
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), comment="Data ostatniej aktualizacji tej pozycji (zakup/sprzedaż częściowa)")

class TransactionHistory(Base):
    """
    Tabela przechowująca historię wszystkich zrealizowanych transakcji (kupna i sprzedaży).
    """
    __tablename__ = 'transaction_history'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='SET NULL'), nullable=True, comment="Ticker (może być NULL, jeśli spółka zostanie usunięta)")
    transaction_type = Column(VARCHAR(10), nullable=False, comment="'BUY' lub 'SELL'")
    quantity = Column(INTEGER, nullable=False, comment="Liczba akcji w tej konkretnej transakcji")
    price_per_share = Column(NUMERIC(12, 4), nullable=False, comment="Cena za akcję w tej transakcji")
    transaction_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), comment="Data i czas wykonania transakcji")
    profit_loss_usd = Column(NUMERIC(14, 2), nullable=True, comment="Zrealizowany zysk/strata w USD dla transakcji sprzedaży")


# === POPRAWKA BŁĘDU #5: Dodanie tabeli cache dla cen ===

class LivePriceCache(Base):
    """
    Tabela przechowująca najnowsze dane cenowe, aktualizowana przez Workera.
    Serwis API czyta tylko z tej tabeli, aby chronić klucz API.
    """
    __tablename__ = 'live_price_cache'

    ticker = Column(VARCHAR(50), primary_key=True)
    # Przechowujemy cały obiekt JSON zwrócony przez get_live_quote_details
    quote_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

