import logging
import time
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float, get_performance
from ..config import SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF, MIN_APEX_SCORE_TO_QUALIFY

logger = logging.getLogger(__name__)

def _calculate_catalyst_score(ticker: str, api_client: AlphaVantageClient) -> int:
    try:
        news_data = api_client.get_news_sentiment(ticker)
        if not news_data or not news_data.get('feed'): return 0
        
        relevant_scores = [item['overall_sentiment_score'] for item in news_data['feed'] if item.get('overall_sentiment_score') is not None]
        if not relevant_scores: return 0
        
        avg_sentiment = sum(relevant_scores) / len(relevant_scores)
        if avg_sentiment >= 0.35: return 3
        if avg_sentiment >= 0.15: return 2
        if avg_sentiment > 0.05: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating catalyst score for {ticker}: {e}")
        return 0

def _calculate_relative_strength_score(ticker: str, sector: str, ticker_data, api_client: AlphaVantageClient) -> int:
    """Zmodyfikowana funkcja, aby przyjmować już pobrane dane tickera."""
    try:
        market_data = api_client.get_daily_adjusted(DEFAULT_MARKET_ETF, 'compact')
        sector_etf = SECTOR_TO_ETF_MAP.get(sector, DEFAULT_MARKET_ETF)
        sector_data = api_client.get_daily_adjusted(sector_etf, 'compact')

        ticker_perf = get_performance(ticker_data, 21)
        market_perf = get_performance(market_data, 21)
        sector_perf = get_performance(sector_data, 21)

        if ticker_perf is None or market_perf is None or sector_perf is None: return 0

        score = 0
        if ticker_perf > market_perf: score += 1
        if ticker_perf > sector_perf: score += 2
        return score
    except Exception as e:
        logger.error(f"Error calculating relative strength for {ticker}: {e}")
        return 0

def _calculate_energy_compression_score(ticker: str, api_client: AlphaVantageClient) -> int:
    try:
        bbands_data = api_client.get_bollinger_bands(ticker)
        tech_analysis = bbands_data.get('Technical Analysis: BBANDS')
        if not tech_analysis: return 0
        
        latest_key = list(tech_analysis.keys())[0]
        latest_bands = tech_analysis[latest_key]
        
        upper = safe_float(latest_bands.get('Real Upper Band'))
        lower = safe_float(latest_bands.get('Real Lower Band'))
        middle = safe_float(latest_bands.get('Real Middle Band'))

        if middle == 0 or upper is None or lower is None: return 0
        
        bandwidth = (upper - lower) / middle
        return 2 if bandwidth < 0.10 else 0
    except Exception as e:
        logger.error(f"Error calculating energy compression for {ticker}: {e}")
        return 0

def _calculate_quality_control_score(ticker: str, overview_data) -> int:
    """Zmodyfikowana funkcja, aby przyjmować już pobrane dane fundamentalne."""
    try:
        if not overview_data: return 0

        profit_margin = safe_float(overview_data.get('ProfitMargin', '-1'))
        pe_ratio = safe_float(overview_data.get('PERatio', '999'))
        
        score = 0
        if profit_margin is not None and profit_margin > 0: score += 1
        if pe_ratio is not None and 0 < pe_ratio < 100: score += 1
        return score
    except Exception as e:
        logger.error(f"Error calculating quality control for {ticker}: {e}")
        return 0

def run_analysis(session: Session, candidate_tickers: list[str], get_current_state, api_client: AlphaVantageClient) -> list[str]:
    logger.info("Running Phase 2: APEX Predator Quality Analysis...")
    append_scan_log(session, "Faza 2: Rozpoczynanie analizy jakościowej...")

    total_candidates = len(candidate_tickers)
    update_scan_progress(session, 0, total_candidates)
    qualified_tickers = []
    processed_count = 0

    for ticker in candidate_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            # --- POPRAWKA: Inteligentne sprawdzanie dostępności danych ---
            # 1. Pobieramy kluczowe dane na samym początku.
            logger.info(f"[DIAG] Phase 2: Fetching primary data for {ticker}...")
            overview_data = api_client.get_company_overview(ticker)
            daily_data = api_client.get_daily_adjusted(ticker, 'compact')

            # 2. Sprawdzamy, czy otrzymaliśmy niezbędne minimum do analizy ("czy pokój nie jest pusty").
            if not overview_data or not daily_data:
                log_msg = f"{ticker} - Pominięty. Brak kluczowych danych fundamentalnych lub cenowych z API."
                logger.warning(f"[DIAG] Skipping {ticker} due to missing primary data (overview or daily).")
                append_scan_log(session, log_msg)
                processed_count += 1
                update_scan_progress(session, processed_count, total_candidates)
                continue # Przejdź do następnego tickera

            # --- KONIEC POPRAWKI ---

            sector_row = session.execute(text("SELECT sector FROM companies WHERE ticker = :ticker"), {'ticker': ticker}).fetchone()
            sector = sector_row[0] if sector_row else "N/A"
            
            catalyst_score = _calculate_catalyst_score(ticker, api_client)
            # Przekazujemy już pobrane dane, aby uniknąć ponownych zapytań
            strength_score = _calculate_relative_strength_score(ticker, sector, daily_data, api_client)
            compression_score = _calculate_energy_compression_score(ticker, api_client)
            quality_score = _calculate_quality_control_score(ticker, overview_data)
            
            total_score = catalyst_score + strength_score + compression_score + quality_score
            is_qualified = total_score >= MIN_APEX_SCORE_TO_QUALIFY
            
            stmt = text("""
                INSERT INTO apex_scores (ticker, analysis_date, catalyst_score, relative_strength_score, energy_compression_score, quality_control_score, total_score, is_qualified)
                VALUES (:ticker, :date, :c_score, :rs_score, :ec_score, :qc_score, :total, :qual)
                ON CONFLICT (ticker, analysis_date) DO UPDATE SET
                catalyst_score = EXCLUDED.catalyst_score, relative_strength_score = EXCLUDED.relative_strength_score,
                energy_compression_score = EXCLUDED.energy_compression_score, quality_control_score = EXCLUDED.quality_control_score,
                total_score = EXCLUDED.total_score, is_qualified = EXCLUDED.is_qualified;
            """)
            session.execute(stmt, {'ticker': ticker, 'date': date.today(), 'c_score': catalyst_score, 'rs_score': strength_score, 
                                   'ec_score': compression_score, 'qc_score': quality_score, 'total': total_score, 'qual': is_qualified})
            session.commit()

            log_msg = f"{ticker} - Wynik APEX: {total_score}/10."
            if is_qualified:
                qualified_tickers.append(ticker)
                log_msg += " Kwalifikacja do APEX Elita."
            else:
                log_msg += " Odrzucony."
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 2: {e}")
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_candidates)
    
    final_log = f"Faza 2 zakończona. Zakwalifikowano {len(qualified_tickers)} spółek do APEX Elita."
    logger.info(final_log)
    append_scan_log(session, final_log)
    return qualified_tickers
