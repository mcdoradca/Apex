import logging
import re
from sqlalchemy.orm import Session
from sqlalchemy import Row, text
from datetime import datetime, timezone, timedelta
import csv
from io import StringIO
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import safe_float
from .. import models

logger = logging.getLogger(__name__)

def _parse_setup_type_from_notes(notes: str) -> str:
    if not notes: return "UNKNOWN"
    notes_lower = notes.lower()
    if "flux" in notes_lower: return "OMNI_FLUX" # Zmiana nazwy dla porządku
    if "biox" in notes_lower: return "BIOX_PUMP"
    if "aqm" in notes_lower: return "AQM_V4"
    if "h3" in notes_lower: return "H3_SNIPER"
    return "OTHER"

def _parse_metrics_from_notes(notes: str) -> dict:
    """
    Ekstrakcja parametrów z notatki sygnału.
    Obsługuje formaty H3 (AQM) oraz F5 (Flux, Velocity, OFP).
    """
    metrics = {}
    if not notes: return metrics
    
    # 1. H3 / AQM
    aqm_match = re.search(r'AQM(?: H3)?:?\s*([0-9\.]+)', notes)
    if aqm_match:
        try: metrics['metric_aqm_score_h3'] = float(aqm_match.group(1))
        except: pass
        
    # 2. FLUX (Faza 5)
    # Wzorce: SCORE: 75/100, OFP: 0.45, VELOCITY: 2.10
    
    score_match = re.search(r'SCORE:\s*(\d+)', notes)
    if score_match:
        try: metrics['metric_flux_score'] = float(score_match.group(1))
        except: pass
        
    ofp_match = re.search(r'OFP:\s*([+\-]?\d+\.?\d*)', notes)
    if ofp_match:
        try: metrics['metric_flux_ofp'] = float(ofp_match.group(1))
        except: pass
        
    vel_match = re.search(r'VELOCITY:\s*([0-9\.]+)', notes)
    if vel_match:
        try: metrics['metric_flux_velocity'] = float(vel_match.group(1))
        except: pass

    # Elasticity jest wspólne dla H4 i F5, ale w bazie mamy metric_elasticity
    elast_match = re.search(r'ELASTICITY:\s*([+\-]?\d+\.?\d*)', notes)
    if elast_match:
        try: metrics['metric_elasticity'] = float(elast_match.group(1))
        except: pass

    return metrics

def open_virtual_trade(session: Session, signal: models.TradingSignal):
    signal_id = signal.id
    try:
        existing_trade = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.signal_id == signal_id
        ).first()
        
        if existing_trade: return

        setup_type = _parse_setup_type_from_notes(signal.notes)
        parsed_metrics = _parse_metrics_from_notes(signal.notes)
        
        entry_price_for_trade = signal.entry_price if signal.entry_price is not None else signal.entry_zone_top

        if entry_price_for_trade is None or signal.stop_loss is None:
            logger.error(f"[Virtual Agent] Nie można otworzyć wirtualnej transakcji dla {signal.ticker}. Brak ceny.")
            return
            
        new_trade = models.VirtualTrade(
            signal_id=signal_id,
            ticker=signal.ticker,
            status='OPEN', 
            setup_type=setup_type,
            entry_price=entry_price_for_trade,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            open_date=datetime.now(timezone.utc),
            
            expected_profit_factor=signal.expected_profit_factor,
            expected_win_rate=signal.expected_win_rate,
            
            # Mapowanie metryk
            metric_aqm_score_h3=parsed_metrics.get('metric_aqm_score_h3'),
            
            # Nowe metryki Flux
            metric_flux_score=parsed_metrics.get('metric_flux_score'),
            metric_flux_velocity=parsed_metrics.get('metric_flux_velocity'),
            metric_flux_ofp=parsed_metrics.get('metric_flux_ofp'),
            metric_elasticity=parsed_metrics.get('metric_elasticity')
        )
        
        session.add(new_trade)
        session.commit()
        
        logger.info(f"✅ [Virtual Agent] Transakcja OTWARTA: {signal.ticker} ({setup_type})")

    except Exception as e:
        logger.error(f"[Virtual Agent] Błąd krytyczny: {e}", exc_info=True)
        session.rollback()

def _parse_bulk_quotes_for_virtual_agent(csv_text: str) -> dict:
    if not csv_text or "symbol" not in csv_text: return {}
    csv_file = StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    price_dict = {}
    for row in reader:
        ticker = row.get('symbol')
        price = safe_float(row.get('close'))
        if ticker and price is not None:
            price_dict[ticker] = price
    return price_dict

def run_virtual_trade_monitor(session: Session, api_client: AlphaVantageClient):
    logger.info("🤖 [Virtual Agent] Uruchamianie monitora...")
    
    try:
        active_signals = session.query(models.TradingSignal).filter(models.TradingSignal.status == 'ACTIVE').all()
        for sig in active_signals: open_virtual_trade(session, sig)

        open_trades = session.query(models.VirtualTrade).filter(models.VirtualTrade.status == 'OPEN').all()
        if not open_trades: return

        tickers_to_check_expiry = []
        now = datetime.now(timezone.utc)

        for trade in open_trades:
            signal = session.query(models.TradingSignal).filter(models.TradingSignal.id == trade.signal_id).first()

            if signal and signal.status == 'COMPLETED':
                logger.info(f"🤖 [Virtual Agent] {trade.ticker} zamknieta (TP).")
                trade.status = 'CLOSED_TP'
                trade.close_date = signal.updated_at
                trade.close_price = signal.take_profit 
            
            elif signal and signal.status == 'INVALIDATED':
                logger.info(f"🤖 [Virtual Agent] {trade.ticker} zamknieta (SL).")
                trade.status = 'CLOSED_SL'
                trade.close_date = signal.updated_at
                trade.close_price = signal.stop_loss 

            elif signal and signal.status == 'EXPIRED':
                 tickers_to_check_expiry.append(trade.ticker)

            elif (now - trade.open_date) > timedelta(days=14):
                tickers_to_check_expiry.append(trade.ticker)
            
            elif not signal:
                 tickers_to_check_expiry.append(trade.ticker)
            
            if trade.status != 'OPEN' and trade.close_price is not None:
                try:
                    p_l_percent = ((trade.close_price - trade.entry_price) / trade.entry_price) * 100
                    trade.final_profit_loss_percent = p_l_percent
                except: trade.final_profit_loss_percent = 0

        session.commit()

        if tickers_to_check_expiry:
            unique_tickers = list(set(tickers_to_check_expiry))
            bulk_csv = api_client.get_bulk_quotes(unique_tickers)
            if bulk_csv:
                parsed_prices = _parse_bulk_quotes_for_virtual_agent(bulk_csv)
                expired_trades = session.query(models.VirtualTrade).filter(
                    models.VirtualTrade.status == 'OPEN',
                    models.VirtualTrade.ticker.in_(unique_tickers)
                ).all()

                for trade in expired_trades:
                    current_price = parsed_prices.get(trade.ticker)
                    if current_price:
                        trade.status = 'CLOSED_EXPIRED'
                        trade.close_date = now
                        trade.close_price = current_price
                        try:
                            p_l_percent = ((trade.close_price - trade.entry_price) / trade.entry_price) * 100
                            trade.final_profit_loss_percent = p_l_percent
                        except: trade.final_profit_loss_percent = 0
            session.commit()
        
    except Exception as e:
        logger.error(f"🤖 [Virtual Agent] Błąd krytyczny: {e}", exc_info=True)
        session.rollback()
