mport logging
import time
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float, get_performance
from ..config import SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF, MIN_APEX_SCORE_TO_QUALIFY

logger = logging.getLogger(__name__)

# --- NOWE, ZAAWANSOWANE AGENTY SCORINGOWE ZGODNE Z DOKUMENTEM STRATEGICZNYM ---

def _calculate_catalyst_score(ticker: str, api_client: AlphaVantageClient) -> int:
    """Agent Katalizatora - progresywna ocena sentymentu."""
    try:
        news_data = api_client.get_news_sentiment(ticker)
        if not news_data or not news_data.get('feed'): return 0
        
        relevant_scores = [item['overall_sentiment_score'] for item in news_data['feed'] if item.get('overall_sentiment_score') is not None]
        if not relevant_scores: return 0
        
        avg_sentiment = sum(relevant_scores) / len(relevant_scores)
        if avg_sentiment >= 0.50: return 3
        if avg_sentiment >= 0.20: return 2
        if avg_sentiment > 0.05: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating catalyst score for {ticker}: {e}")
        return 0

def _calculate_momentum_strength_score(ticker: str, api_client: AlphaVantageClient) -> int:
    """Agent Siły Względnej - ocena na podstawie RSI(9) i przewagi nad rynkiem."""
    score = 0
    try:
        # 1. Analiza RSI
        rsi_data = api_client.get_rsi(ticker, time_period=9)
        if rsi_data and 'Technical Analysis: RSI' in rsi_data:
            latest_rsi = safe_float(list(rsi_data['Technical Analysis: RSI'].values())[0]['RSI'])
            if latest_rsi:
                if latest_rsi > 50: score += 1
                if latest_rsi > 60: score += 1
        
        # 2. Analiza siły względnej vs QQQ
        ticker_data = api_client.get_daily_adjusted(ticker, 'compact')
        qqq_data = api_client.get_daily_adjusted(DEFAULT_MARKET_ETF, 'compact')
        
        ticker_perf = get_performance(ticker_data, 5)
        qqq_perf = get_performance(qqq_data, 5)

        if ticker_perf is not None and qqq_perf is not None and qqq_perf > 0:
             # Dodajemy warunek qqq_perf > 0, aby uniknąć dzielenia przez zero i fałszywych sygnałów na spadającym rynku
            if ticker_perf > (qqq_perf * 1.5):
                score += 2
        
        return score
    except Exception as e:
        logger.error(f"Error calculating momentum strength for {ticker}: {e}")
        return 0

def _calculate_energy_compression_score(ticker: str, api_client: AlphaVantageClient) -> int:
    """Agent Kompresji Energii - adaptacyjna ocena na podstawie rangi percentylowej BBW."""
    try:
        bbands_data = api_client.get_bollinger_bands(ticker, time_period=20)
        tech_analysis = bbands_data.get('Technical Analysis: BBANDS')
        if not tech_analysis: return 0
        
        bandwidth_history = []
        for bands in list(tech_analysis.values())[:100]: # Analizujemy ostatnie 100 okresów
            upper = safe_float(bands.get('Real Upper Band'))
            lower = safe_float(bands.get('Real Lower Band'))
            middle = safe_float(bands.get('Real Middle Band'))
            if middle and upper and lower and middle > 0:
                bandwidth = (upper - lower) / middle
                bandwidth_history.append(bandwidth)
        
        if not bandwidth_history: return 0
        
        current_bbw = bandwidth_history[0]
        historical_bbw = np.array(bandwidth_history)
        
        percentile_rank = (np.sum(historical_bbw < current_bbw) / len(historical_bbw)) * 100

        if percentile_rank < 10: return 3
        if percentile_rank < 25: return 2
        if percentile_rank < 40: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating energy compression for {ticker}: {e}")
        return 0

def run_analysis(session: Session, candidate_tickers: list[str], get_current_state, api_client: AlphaVantageClient) -> list[str]:
    logger.info("Running Phase 2: APEX Predator Quality Analysis...")
    append_scan_log(session, "Faza 2: Rozpoczynanie analizy jakościowej APEX...")

    total_candidates = len(candidate_tickers)
    update_scan_progress(session, 0, total_candidates)
    qualified_tickers = []
    processed_count = 0

    session.execute(text("DELETE FROM phase2_results WHERE analysis_date = :today"), {'today': date.today()})
    session.commit()

    for ticker in candidate_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            momentum_score = _calculate_momentum_strength_score(ticker, api_client)
            compression_score = _calculate_energy_compression_score(ticker, api_client)
            catalyst_score = _calculate_catalyst_score(ticker, api_client)
            
            total_score = momentum_score + compression_score + catalyst_score
            is_qualified = total_score >= MIN_APEX_SCORE_TO_QUALIFY
            
            stmt = text("""
                INSERT INTO phase2_results (ticker, analysis_date, momentum_score, compression_score, catalyst_score, total_score, is_qualified)
                VALUES (:ticker, :date, :m_score, :c_score, :cat_score, :total, :qual)
                ON CONFLICT (ticker, analysis_date) DO UPDATE SET
                momentum_score = EXCLUDED.momentum_score, compression_score = EXCLUDED.compression_score,
                catalyst_score = EXCLUDED.catalyst_score, total_score = EXCLUDED.total_score, is_qualified = EXCLUDED.is_qualified;
            """)
            session.execute(stmt, {
                'ticker': ticker, 'date': date.today(), 'm_score': momentum_score, 
                'c_score': compression_score, 'cat_score': catalyst_score, 
                'total': total_score, 'qual': is_qualified
            })
            session.commit()

            log_msg = f"{ticker} - Wynik APEX: {total_score}/10 (M:{momentum_score}, C:{compression_score}, K:{catalyst_score})."
            if is_qualified:
                qualified_tickers.append(ticker)
                log_msg += " Kwalifikacja do Fazy 3."
            else:
                log_msg += " Odrzucony."
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error processing {ticker} in Phase 2: {e}")
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_candidates)
    
    final_log = f"Faza 2 zakończona. Zakwalifikowano {len(qualified_tickers)} spółek."
    logger.info(final_log)
    append_scan_log(session, final_log)
    return qualified_tickers
