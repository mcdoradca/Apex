# ... existing code ...
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP, JSONB
from sqlalchemy.sql import func
# ZMIANA: Importujemy 'Base' z LOKALNEGO pliku database.py Workera
# ... existing code ...
    analysis_data = Column(JSONB)
    last_updated = Column(PG_TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

# === POPRAWKA BŁĘDU #6: Dodanie brakujących modeli ===
# Modele te istniały w `api`, ale brakowało ich w `worker`.

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
