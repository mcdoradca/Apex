from sqlalchemy.orm import Session
from sqlalchemy import text, desc, func, update, delete, Row
from . import models, schemas
from typing import Optional, Any, Dict, List
from datetime import date, datetime, timezone, timedelta 
import logging
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
import io
import csv
from typing import Generator
import uuid # Do generowania ID zadań
import math # === DODANO IMPORT MATH DO SPRAWDZANIA INF/NAN ===

logger = logging.getLogger(__name__)

# Funkcja pomocnicza do bezpiecznej konwersji na Decimal
def to_decimal(value, precision='0.0001') -> Optional[Decimal]:
    """Konwertuje float lub str na Decimal z określoną precyją."""
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal(precision), rounding=ROUND_HALF_UP)
    except Exception:
        logger.error(f"Nie można przekonwertować {value} na Decimal.")
        return None

# === NOWE FUNKCJE POMOCNICZE DO SANITYZACJI (Fix JSON Error) ===

def _safe_float_stat(val) -> float:
    """
    Bezpieczna konwersja na float dla statystyk.
    Zamienia Infinity/NaN na 0.0, aby uniknąć błędu JSON.
    """
    if val is None: return 0.0
    try:
        f = float(val)
        if math.isinf(f) or math.isnan(f): return 0.0
        return f
    except Exception:
        return 0.0

def _sanitize_trade_metrics(trade: models.VirtualTrade):
    """
    Czyści obiekt VirtualTrade z wartości NaN/Infinity w metrykach (in-place).
    Postgres pozwala na NaN/Inf, ale JSON nie.
    """
    metric_fields = [
        'metric_atr_14', 'metric_time_dilation', 'metric_price_gravity',
        'metric_td_percentile_90', 'metric_pg_percentile_90',
        'metric_inst_sync', 'metric_retail_herding',
        'metric_aqm_score_h3', 'metric_aqm_percentile_95',
        'metric_J_norm', 'metric_nabla_sq_norm', 'metric_m_sq_norm',
        'metric_J', 'metric_J_threshold_2sigma'
    ]
    
    for field in metric_fields:
        val = getattr(trade, field)
        if val is not None:
            try:
                f_val = float(val)
                if math.isinf(f_val) or math.isnan(f_val):
                    setattr(trade, field, None) # JSON null jest bezpieczny
            except Exception:
                setattr(trade, field, None)

# ==========================================================
# === FUNKCJE CRUD DLA OPTYMALIZACJI (APEX V4) ===
# ==========================================================

def create_optimization_job(db: Session, request: schemas.OptimizationRequest) -> models.OptimizationJob:
    """
    Tworzy nowe zadanie optymalizacji w bazie danych.
    """
    job_id = str(uuid.uuid4())
    
    new_job = models.OptimizationJob(
        id=job_id,
        target_year=request.target_year,
        total_trials=request.n_trials,
        status='PENDING',
        configuration=request.parameter_space, # Zapisujemy niestandardową przestrzeń jako JSON
        created_at=datetime.now(timezone.utc)
    )
    
    db.add(new_job)
    try:
        db.commit()
        db.refresh(new_job)
        logger.info(f"Utworzono zadanie optymalizacji: {job_id}")
        return new_job
    except Exception as e:
        db.rollback()
        logger.error(f"Błąd tworzenia zadania optymalizacji: {e}", exc_info=True)
        raise

def get_optimization_job(db: Session, job_id: str) -> Optional[models.OptimizationJob]:
    """Pobiera zadanie optymalizacji po ID."""
    return db.query(models.OptimizationJob).filter(models.OptimizationJob.id == job_id).first()

def get_latest_optimization_job(db: Session) -> Optional[models.OptimizationJob]:
    """Pobiera najnowsze zadanie optymalizacji."""
    return db.query(models.OptimizationJob).order_by(desc(models.OptimizationJob.created_at)).first()

def get_optimization_trials(db: Session, job_id: str) -> List[models.OptimizationTrial]:
    """Pobiera listę prób dla danego zadania."""
    return db.query(models.OptimizationTrial).filter(
        models.OptimizationTrial.job_id == job_id
    ).order_by(models.OptimizationTrial.trial_number).all()

# ==========================================================
# === POZOSTAŁE FUNKCJE CRUD (Bez zmian) ===
# ==========================================================

def get_portfolio_holdings(db: Session) -> List[schemas.PortfolioHolding]:
    results = db.query(
        models.PortfolioHolding,
        models.TradingSignal.take_profit
    ).outerjoin(
        models.TradingSignal,
        (models.PortfolioHolding.ticker == models.TradingSignal.ticker) &
        (models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])) 
    ).order_by(models.PortfolioHolding.ticker).all()

    holdings_with_tp = []
    for (holding, take_profit) in results:
        holding_schema = schemas.PortfolioHolding.model_validate(holding)
        holding_schema.take_profit = float(take_profit) if take_profit is not None else None
        holdings_with_tp.append(holding_schema)
    
    return holdings_with_tp

def get_transaction_history(db: Session, limit: int = 100) -> List[models.TransactionHistory]:
    return db.query(models.TransactionHistory).order_by(desc(models.TransactionHistory.transaction_date)).limit(limit).all()

def record_buy_transaction(db: Session, buy_request: schemas.BuyRequest) -> models.PortfolioHolding:
    ticker = buy_request.ticker.strip().upper()
    quantity_bought = buy_request.quantity
    price_per_share = to_decimal(buy_request.price_per_share)

    if price_per_share is None:
        raise ValueError("Nieprawidłowa cena zakupu.")

    company_exists = db.query(models.Company).filter(models.Company.ticker == ticker).first()
    if not company_exists:
        logger.warning(f"Ticker {ticker} not found in 'companies'. Adding automatically.")
        new_company = models.Company(
            ticker=ticker,
            company_name=f"{ticker} (Dodany przez Portfel)",
            exchange="N/A", industry="N/A", sector="N/A"
        )
        db.add(new_company)

    db_history = models.TransactionHistory(
        ticker=ticker, transaction_type='BUY', quantity=quantity_bought,
        price_per_share=price_per_share, transaction_date=datetime.now(timezone.utc)
    )
    db.add(db_history)
    try:
        db.commit()
        db.refresh(db_history)
    except Exception as e:
        db.rollback()
        logger.error(f"Nie udało się zapisać historii BUY: {e}", exc_info=True)
        raise

    holding = db.query(models.PortfolioHolding).filter(models.PortfolioHolding.ticker == ticker).with_for_update().first()

    if holding:
        current_quantity = Decimal(holding.quantity)
        current_avg_price = Decimal(holding.average_buy_price)
        new_quantity_dec = Decimal(quantity_bought)
        total_cost_before = current_quantity * current_avg_price
        cost_of_new_shares = new_quantity_dec * price_per_share
        new_total_quantity = current_quantity + new_quantity_dec
        new_total_cost = total_cost_before + cost_of_new_shares
        new_average_price = (new_total_cost / new_total_quantity).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP) if new_total_quantity > 0 else Decimal(0)

        holding.quantity = int(new_total_quantity)
        holding.average_buy_price = new_average_price
        holding.last_updated = datetime.now(timezone.utc)
    else:
        holding = models.PortfolioHolding(
            ticker=ticker, quantity=quantity_bought, average_buy_price=price_per_share,
            first_purchase_date=datetime.now(timezone.utc), last_updated=datetime.now(timezone.utc)
        )
        db.add(holding)

    try:
        db.commit()
        db.refresh(holding)
        return holding
    except Exception as e:
        db.rollback()
        logger.error(f"Nie udało się zaktualizować portfela: {e}", exc_info=True)
        raise

def record_sell_transaction(db: Session, sell_request: schemas.SellRequest) -> Optional[models.PortfolioHolding]:
    ticker = sell_request.ticker.strip().upper()
    quantity_sold = sell_request.quantity
    price_per_share = to_decimal(sell_request.price_per_share)

    if price_per_share is None:
        raise ValueError("Nieprawidłowa cena sprzedaży.")

    holding = db.query(models.PortfolioHolding).filter(models.PortfolioHolding.ticker == ticker).with_for_update().first()

    if not holding:
        raise ValueError(f"Nie posiadasz akcji {ticker}.")
    if holding.quantity < quantity_sold:
        raise ValueError(f"Próba sprzedaży {quantity_sold}, posiadasz {holding.quantity}.")

    average_buy_price = Decimal(holding.average_buy_price)
    quantity_sold_dec = Decimal(quantity_sold)
    profit_loss = (price_per_share - average_buy_price) * quantity_sold_dec
    profit_loss = profit_loss.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

    db_history = models.TransactionHistory(
        ticker=ticker, transaction_type='SELL', quantity=quantity_sold,
        price_per_share=price_per_share, transaction_date=datetime.now(timezone.utc),
        profit_loss_usd=profit_loss
    )
    db.add(db_history)
    try:
        db.commit()
        db.refresh(db_history)
    except Exception as e:
        db.rollback()
        logger.error(f"Nie udało się zapisać historii SELL: {e}", exc_info=True)
        raise

    remaining_quantity = holding.quantity - quantity_sold

    if remaining_quantity == 0:
        db.delete(holding)
        try:
             db.commit()
             return None
        except Exception as e:
            db.rollback()
            logger.error(f"Nie udało się usunąć pozycji: {e}", exc_info=True)
            raise
    else:
        holding.quantity = remaining_quantity
        holding.last_updated = datetime.now(timezone.utc)
        try:
            db.commit()
            db.refresh(holding)
            return holding
        except Exception as e:
            db.rollback()
            logger.error(f"Nie udało się zaktualizować pozycji: {e}", exc_info=True)
            raise

def get_phase1_candidates(db: Session) -> List[Dict[str, Any]]:
    candidates_from_db = db.query(models.Phase1Candidate).filter(
        models.Phase1Candidate.analysis_date >= func.current_date()
    ).order_by(models.Phase1Candidate.ticker).all()

    return [
        {
            "ticker": c.ticker,
            "price": float(c.price) if c.price is not None else None,
            "change_percent": float(c.change_percent) if c.change_percent is not None else None,
            "volume": c.volume,
            "score": c.score,
            "analysis_date": c.analysis_date.isoformat() if c.analysis_date else None
        } for c in candidates_from_db
    ]

def get_phase2_results(db: Session) -> List[Dict[str, Any]]:
    latest_date = db.query(func.max(models.Phase2Result.analysis_date)).scalar()
    if not latest_date:
        return []

    results_from_db = db.query(models.Phase2Result).filter(
        models.Phase2Result.analysis_date == latest_date,
        models.Phase2Result.is_qualified == True
    ).order_by(desc(models.Phase2Result.total_score)).all()

    return [
        {
            "ticker": r.ticker,
            "analysis_date": r.analysis_date.isoformat() if r.analysis_date else None,
            "catalyst_score": r.catalyst_score,
            "relative_strength_score": r.relative_strength_score,
            "energy_compression_score": r.energy_compression_score,
            "total_score": r.total_score,
            "is_qualified": r.is_qualified
        } for r in results_from_db
    ]

def get_active_and_pending_signals(db: Session) -> List[Dict[str, Any]]:
    signals_from_db = db.query(models.TradingSignal).filter(
        models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
    ).order_by(models.TradingSignal.ticker).all()

    return [
        {
            "id": signal.id,
            "ticker": signal.ticker,
            "generation_date": signal.generation_date.isoformat() if signal.generation_date else None,
            "status": signal.status,
            "entry_price": float(signal.entry_price) if signal.entry_price is not None else None,
            "stop_loss": float(signal.stop_loss) if signal.stop_loss is not None else None,
            "take_profit": float(signal.take_profit) if signal.take_profit is not None else None,
            "risk_reward_ratio": float(signal.risk_reward_ratio) if signal.risk_reward_ratio is not None else None,
            "signal_candle_timestamp": signal.signal_candle_timestamp.isoformat() if signal.signal_candle_timestamp else None,
            "entry_zone_bottom": float(signal.entry_zone_bottom) if signal.entry_zone_bottom is not None else None,
            "entry_zone_top": float(signal.entry_zone_top) if signal.entry_zone_top is not None else None,
            "notes": signal.notes
        } for signal in signals_from_db
    ]

def get_discarded_signals_count_24h(db: Session) -> int:
    try:
        discarded_statuses = ['INVALIDATED', 'COMPLETED']
        time_24_hours_ago = datetime.now(timezone.utc) - timedelta(days=1)
        count = db.query(func.count(models.TradingSignal.id)).filter(
            models.TradingSignal.status.in_(discarded_statuses),
            models.TradingSignal.updated_at >= time_24_hours_ago
        ).scalar()
        
        return count if count is not None else 0
    except Exception as e:
        logger.error(f"Error counting discarded signals: {e}", exc_info=True)
        return 0

def delete_phase1_candidate(db: Session, ticker: str):
    db.query(models.Phase1Candidate).filter(models.Phase1Candidate.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Candidate {ticker} from Phase 1 deleted."}

def delete_phase2_result(db: Session, ticker: str):
    db.query(models.Phase2Result).filter(models.Phase2Result.ticker == ticker).delete(synchronize_session=False)
    db.commit()
    return {"message": f"Result {ticker} from Phase 2 deleted."}

def delete_trading_signal(db: Session, signal_id: int):
    signal = db.query(models.TradingSignal).filter(models.TradingSignal.id == signal_id).first()
    if signal and signal.status in ['ACTIVE', 'PENDING']:
        signal.status = 'CANCELLED'
        signal.notes = (signal.notes or "") + " Ręcznie anulowany."
        db.commit()
        logger.info(f"Signal {signal_id} for {signal.ticker} marked as CANCELLED.")
        return {"message": f"Signal {signal_id} for {signal.ticker} marked as cancelled."}
    logger.warning(f"Signal {signal_id} not found or already closed/cancelled.")
    return None

def get_system_control_value(db: Session, key: str) -> Optional[str]:
    result = db.query(models.SystemControl.value).filter(models.SystemControl.key == key).first()
    return result[0] if result else None

def set_system_control_value(db: Session, key: str, value: str):
    stmt = text("""
        INSERT INTO system_control (key, value, updated_at)
        VALUES (:key, :value, NOW())
        ON CONFLICT (key) DO UPDATE
        SET value = :value, updated_at = NOW();
    """)
    try:
        db.execute(stmt, [{'key': key, 'value': str(value)}]) 
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Error setting system control value for key {key}: {e}", exc_info=True)
        raise

def _calculate_stats_from_rows(trades_rows: List[Row]) -> schemas.VirtualAgentStats:
    total_trades = len(trades_rows)
    if total_trades == 0:
        return schemas.VirtualAgentStats(
            total_trades=0, win_rate_percent=0.0, total_p_l_percent=0.0,
            profit_factor=0.0, by_setup={}
        )

    wins = 0
    losses = 0
    total_p_l = Decimal(0)
    total_win_p_l = Decimal(0)
    total_loss_p_l = Decimal(0)
    
    setup_stats = defaultdict(lambda: {
        'trades': 0, 'wins': 0, 'total_p_l': Decimal(0), 'total_loss_p_l': Decimal(0), 'total_win_p_l': Decimal(0)
    })

    valid_trades_count = 0
    for trade in trades_rows:
        if trade.final_profit_loss_percent is None:
            continue

        valid_trades_count += 1
        p_l = Decimal(trade.final_profit_loss_percent)
        
        total_p_l += p_l
        setup_type = trade.setup_type or "UNKNOWN"
        setup_stats[setup_type]['trades'] += 1
        setup_stats[setup_type]['total_p_l'] += p_l

        if p_l > 0:
            wins += 1
            total_win_p_l += p_l
            setup_stats[setup_type]['wins'] += 1
            setup_stats[setup_type]['total_win_p_l'] += p_l
        elif p_l < 0:
            losses += 1
            total_loss_p_l += p_l 
            setup_stats[setup_type]['total_loss_p_l'] += p_l

    # --- SANITYZACJA STATYSTYK (Fix JSON Error) ---
    
    raw_win_rate = (wins / valid_trades_count) * 100 if valid_trades_count > 0 else 0
    raw_pf = float(abs(total_win_p_l / total_loss_p_l)) if total_loss_p_l != 0 else 0.0
    
    win_rate = _safe_float_stat(raw_win_rate)
    profit_factor = _safe_float_stat(raw_pf)
    safe_total_pl = _safe_float_stat(total_p_l)

    by_setup_processed = {}
    for setup, data in setup_stats.items():
        s_raw_win_rate = (data['wins'] / data['trades']) * 100 if data['trades'] > 0 else 0
        s_raw_pf = float(abs(data['total_win_p_l'] / data['total_loss_p_l'])) if data['total_loss_p_l'] != 0 else 0.0
        
        by_setup_processed[setup] = {
            'total_trades': data['trades'],
            'win_rate_percent': _safe_float_stat(s_raw_win_rate),
            'total_p_l_percent': _safe_float_stat(data['total_p_l']),
            'profit_factor': _safe_float_stat(s_raw_pf)
        }

    return schemas.VirtualAgentStats(
        total_trades=valid_trades_count,
        win_rate_percent=win_rate,
        total_p_l_percent=safe_total_pl,
        profit_factor=profit_factor,
        by_setup=by_setup_processed
    )

def get_virtual_agent_report(db: Session, page: int = 1, page_size: int = 200) -> schemas.VirtualAgentReport:
    try:
        stats_query_result = db.query(
            models.VirtualTrade.final_profit_loss_percent,
            models.VirtualTrade.setup_type
        ).filter(models.VirtualTrade.status != 'OPEN').all()
        
        stats = _calculate_stats_from_rows(stats_query_result)
        total_trades_count = len(stats_query_result)

        if total_trades_count == 0:
             return schemas.VirtualAgentReport(stats=stats, trades=[], total_trades_count=0)

        offset = (page - 1) * page_size
        paged_trades = db.query(models.VirtualTrade).filter(
            models.VirtualTrade.status != 'OPEN'
        ).order_by(desc(models.VirtualTrade.close_date)).offset(offset).limit(page_size).all()
        
        # --- SANITYZACJA TRANSAKCJI (Fix JSON Error) ---
        # Czyścimy wyniki zapytania "w locie", zanim trafią do Pydantic -> JSON
        for trade in paged_trades:
            _sanitize_trade_metrics(trade)
        
        return schemas.VirtualAgentReport(
            stats=stats,
            trades=paged_trades, 
            total_trades_count=total_trades_count 
        )
    except Exception as e:
        logger.error(f"Nie można wygenerować raportu Wirtualnego Agenta: {e}", exc_info=True)
        # Zwracamy pusty raport zamiast błędu 500, jeśli coś poszło nie tak
        empty_stats = schemas.VirtualAgentStats(
            total_trades=0, win_rate_percent=0.0, total_p_l_percent=0.0,
            profit_factor=0.0, by_setup={}
        )
        return schemas.VirtualAgentReport(stats=empty_stats, trades=[], total_trades_count=0)

def get_ai_optimizer_report(db: Session) -> schemas.AIOptimizerReport:
    try:
        report_row = db.query(models.SystemControl).filter(
            models.SystemControl.key == 'ai_optimizer_report'
        ).first()

        if not report_row or not report_row.value or report_row.value == 'NONE':
            return schemas.AIOptimizerReport(status="NONE")
        
        if report_row.value == 'PROCESSING':
             return schemas.AIOptimizerReport(status="PROCESSING", last_updated=report_row.updated_at)

        return schemas.AIOptimizerReport(
            status="DONE",
            report_text=report_row.value,
            last_updated=report_row.updated_at
        )
        
    except Exception as e:
        logger.error(f"Nie można pobrać raportu Mega Agenta: {e}", exc_info=True)
        return schemas.AIOptimizerReport(
            status="ERROR",
            report_text=f"Błąd serwera podczas odczytu raportu: {e}"
        )

def get_h3_deep_dive_report(db: Session) -> schemas.H3DeepDiveReport:
    try:
        report_row = db.query(models.SystemControl).filter(
            models.SystemControl.key == 'h3_deep_dive_report'
        ).first()

        if not report_row or not report_row.value or report_row.value == 'NONE':
            return schemas.H3DeepDiveReport(status="NONE")
        
        if report_row.value == 'PROCESSING':
             return schemas.H3DeepDiveReport(status="PROCESSING", last_updated=report_row.updated_at)
        
        if report_row.value.startswith("BŁĄD:"):
            return schemas.H3DeepDiveReport(
                status="ERROR",
                report_text=report_row.value,
                last_updated=report_row.updated_at
            )

        return schemas.H3DeepDiveReport(
            status="DONE",
            report_text=report_row.value,
            last_updated=report_row.updated_at
        )
        
    except Exception as e:
        logger.error(f"Nie można pobrać raportu H3 Deep Dive: {e}", exc_info=True)
        return schemas.H3DeepDiveReport(
            status="ERROR",
            report_text=f"Błąd serwera podczas odczytu raportu: {e}"
        )

def stream_all_trades_as_csv(db: Session) -> Generator[str, None, None]:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    
    header = [column.name for column in models.VirtualTrade.__table__.columns]
    
    writer.writerow(header)
    buffer.seek(0)
    yield buffer.getvalue()
    buffer.truncate(0)
    buffer.seek(0)
    
    try:
        trades_stream = db.query(models.VirtualTrade).order_by(models.VirtualTrade.id).yield_per(100)
        
        for trade in trades_stream:
            row_data = [getattr(trade, col) for col in header]
            writer.writerow(row_data)
            
            buffer.seek(0)
            yield buffer.getvalue()
            
            buffer.truncate(0)
            buffer.seek(0)
            
    except Exception as e:
        logger.error(f"Błąd podczas streamowania danych CSV: {e}", exc_info=True)
        writer.writerow([f"BŁĄD PODCZAS STREAMOWANIA: {e}"])
        buffer.seek(0)
        yield buffer.getvalue()
