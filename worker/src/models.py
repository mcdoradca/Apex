from sqlalchemy import (
    Column, String, VARCHAR, TIMESTAMP, NUMERIC, BIGINT, DATE,
    Boolean, INTEGER, TEXT, ForeignKey, Index, func, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
# ZMIANA: Importujemy 'Base' z LOKALNEGO pliku database.py Workera
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

    updated_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    status = Column(VARCHAR(50), default='PENDING') 
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
            'uq_active_pending_ticker',
            'ticker',
            unique=True,
            postgresql_where=status.in_(['ACTIVE', 'PENDING'])
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

class ProcessedNews(Base):
    """
    Przechowuje "pamięć" Agencji Prasowej, aby nie wysyłać
    wielokrotnie alertów o tej samej wiadomości.
    """
    __tablename__ = 'processed_news'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    ticker = Column(VARCHAR(50), ForeignKey('companies.ticker', ondelete='CASCADE'), nullable=False, index=True)
    news_hash = Column(VARCHAR(64), nullable=False, index=True, comment="SHA-256 hash of the news URL or headline")
    processed_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    sentiment = Column(VARCHAR(50), nullable=False, comment="'POSITIVE', 'NEGATIVE', 'NEUTRAL'")
    headline = Column(TEXT, nullable=True)
    source_url = Column(TEXT, nullable=True)

    __table_args__ = (
        UniqueConstraint('ticker', 'news_hash', name='uq_ticker_news_hash'),
    )

class PortfolioHolding(Base):
    """
    Tabela przechowująca aktualnie otwarte pozycje w portfelu.
    Każdy wiersz reprezentuje *całkowitą* pozycję w danym tickerze.
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
    related_portfolio_ticker = Column(VARCHAR(50), nullable=True, comment="Opcjonalne powiązanie z pozycją w portfelu (na przyszłość)")
    profit_loss_usd = Column(NUMERIC(14, 2), nullable=True, comment="Zrealizowany zysk/strata w USD dla transakcji sprzedaży")

class VirtualTrade(Base):
    """
    Przechowuje wyniki Wirtualnego Agenta (Paper Tradingu).
    Każdy wiersz to jedna "wirtualna" transakcja oparta na sygnale.
    """
    __tablename__ = 'virtual_trades'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    signal_id = Column(INTEGER, ForeignKey('trading_signals.id', ondelete='SET NULL'), nullable=True)
    ticker = Column(VARCHAR(50), nullable=False, index=True)
    status = Column(VARCHAR(50), nullable=False, default='OPEN', index=True)
    setup_type = Column(VARCHAR(100), nullable=True, comment="Np. EMA_BOUNCE, FIB_H1")
    entry_price = Column(NUMERIC(12, 2), nullable=False)
    stop_loss = Column(NUMERIC(12, 2), nullable=False)
    take_profit = Column(NUMERIC(12, 2), nullable=True)
    open_date = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), comment="Data aktywacji sygnału")
    close_date = Column(PG_TIMESTAMP(timezone=True), nullable=True, comment="Data zamknięcia pozycji")
    close_price = Column(NUMERIC(12, 2), nullable=True, comment="Cena, po której pozycja została zamknięta")
    final_profit_loss_percent = Column(NUMERIC(8, 2), nullable=True, comment="Ostateczny zysk/strata w %")

    # Metryki Dnia D
    metric_atr_14 = Column(NUMERIC(10, 4), nullable=True)
    # Metryki dla H1
    metric_time_dilation = Column(NUMERIC(10, 4), nullable=True)
    metric_price_gravity = Column(NUMERIC(10, 4), nullable=True)
    metric_td_percentile_90 = Column(NUMERIC(10, 4), nullable=True)
    metric_pg_percentile_90 = Column(NUMERIC(10, 4), nullable=True)
    # Metryki dla H2
    metric_inst_sync = Column(NUMERIC(10, 4), nullable=True)
    metric_retail_herding = Column(NUMERIC(10, 4), nullable=True)
    # Metryki dla H3
    metric_aqm_score_h3 = Column(NUMERIC(10, 4), nullable=True)
    metric_aqm_percentile_95 = Column(NUMERIC(10, 4), nullable=True)
    metric_J_norm = Column(NUMERIC(10, 4), nullable=True)
    metric_nabla_sq_norm = Column(NUMERIC(10, 4), nullable=True)
    metric_m_sq_norm = Column(NUMERIC(10, 4), nullable=True)
    # Metryki dla H4
    metric_J = Column(NUMERIC(10, 4), nullable=True)
    metric_J_threshold_2sigma = Column(NUMERIC(10, 4), nullable=True)

class AlphaVantageCache(Base):
    """
    Przechowuje surowe dane (JSONB) z Alpha Vantage dla backtestingu.
    """
    __tablename__ = 'alpha_vantage_cache'
    ticker = Column(VARCHAR(50), primary_key=True, nullable=False, index=True)
    data_type = Column(VARCHAR(50), primary_key=True, nullable=False)
    raw_data_json = Column(JSONB, nullable=False)
    last_fetched = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        UniqueConstraint('ticker', 'data_type', name='uq_av_cache_entry'),
    )

# === NOWE MODELE DLA APEX V4 (Quantum Optimization) ===

class OptimizationJob(Base):
    """
    Reprezentuje pojedynczą sesję optymalizacyjną (np. 'Optymalizacja H3 na 2023').
    Przechowuje konfigurację i status całego zadania.
    """
    __tablename__ = 'optimization_jobs'

    id = Column(String(36), primary_key=True, comment="UUID zadania")
    created_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())
    status = Column(String(20), default='PENDING', comment="'PENDING', 'RUNNING', 'COMPLETED', 'FAILED'")
    target_year = Column(INTEGER, nullable=False, comment="Rok, na którym przeprowadzono optymalizację")
    total_trials = Column(INTEGER, nullable=False, comment="Liczba zaplanowanych prób Optuny")
    best_trial_id = Column(INTEGER, nullable=True, comment="ID najlepszej próby (po zakończeniu)")
    best_score = Column(NUMERIC(10, 4), nullable=True, comment="Najlepszy wynik (np. Profit Factor)")
    
    # Przechowujemy parametry study (np. zakresy) jako JSON
    configuration = Column(JSONB, nullable=True) 

class OptimizationTrial(Base):
    """
    Pojedyncza próba (Trial) w ramach zadania optymalizacyjnego.
    Odpowiada jednemu 'runowi' backtestu z konkretnym zestawem parametrów.
    """
    __tablename__ = 'optimization_trials'

    id = Column(INTEGER, primary_key=True, autoincrement=True)
    job_id = Column(String(36), ForeignKey('optimization_jobs.id', ondelete='CASCADE'), nullable=False, index=True)
    trial_number = Column(INTEGER, nullable=False)
    
    # Parametry użyte w tej próbie (np. h3_percentile=0.98)
    params = Column(JSONB, nullable=False)
    
    # Wyniki
    profit_factor = Column(NUMERIC(10, 4), nullable=True)
    total_trades = Column(INTEGER, nullable=True)
    win_rate = Column(NUMERIC(10, 4), nullable=True)
    net_profit = Column(NUMERIC(14, 2), nullable=True)
    
    # Status próby
    state = Column(String(20), default='COMPLETE', comment="'COMPLETE', 'PRUNED', 'FAIL'")
    created_at = Column(PG_TIMESTAMP(timezone=True), server_default=func.now())

# === KONIEC NOWYCH MODELI ===
