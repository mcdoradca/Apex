import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import update_scan_progress, append_scan_log, safe_float, get_performance
from ..config import Phase2Config, SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

def _calculate_catalyst_score(ticker: str, api_client: AlphaVantageClient) -> int:
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

def _calculate_relative_strength_score(ticker: str, ticker_data_raw, qqq_data_raw, api_client: AlphaVantageClient) -> int:
    score = 0
    try:
        # RSI
        rsi_data = api_client.get_rsi(ticker, time_period=9)
        if rsi_data and 'Technical Analysis: RSI' in rsi_data:
            latest_rsi = safe_float(list(rsi_data['Technical Analysis: RSI'].values())[0]['RSI'])
            if latest_rsi:
                if latest_rsi > 60: score += 2
                elif latest_rsi > 50: score += 1
        
        # Performance vs QQQ
        ticker_perf = get_performance(ticker_data_raw, 5)
        qqq_perf = get_performance(qqq_data_raw, 5)

        if ticker_perf is not None and qqq_perf is not None:
            if ticker_perf > (qqq_perf * 1.5):
                score += 2

    except Exception as e:
        logger.error(f"Error calculating relative strength score for {ticker}: {e}")
    return score


def _calculate_energy_compression_score(ticker: str, api_client: AlphaVantageClient) -> int:
    try:
        bbands_data = api_client.get_bollinger_bands(ticker, time_period=20)
        # --- POCZĄTEK POPRAWKI ---
        if not bbands_data: # Dodatkowe zabezpieczenie przed pustą odpowiedzią API
            logger.warning(f"Received no Bollinger Bands data for {ticker}. Skipping compression score.")
            return 0
        # --- KONIEC POPRAWKI ---
        tech_analysis = bbands_data.get('Technical Analysis: BBANDS')
        if not tech_analysis or len(tech_analysis) < 100: return 0
        
        bbw_values = []
        for date_str, values in tech_analysis.items():
            upper = safe_float(values.get('Real Upper Band'))
            lower = safe_float(values.get('Real Lower Band'))
            middle = safe_float(values.get('Real Middle Band'))
            if upper and lower and middle and middle > 0:
                bbw = (upper - lower) / middle
                bbw_values.append(bbw)
        
        if not bbw_values: return 0
        
        bbw_series = pd.Series(bbw_values)
        current_bbw = bbw_series.iloc[0]
        percentile_rank = bbw_series.rank(pct=True).iloc[0]

        if percentile_rank < 0.10: return 3
        if percentile_rank < 0.25: return 2
        if percentile_rank < 0.40: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating energy compression score for {ticker}: {e}")
        return 0

def run_analysis(session: Session, candidate_tickers: list[str], get_current_state, api_client: AlphaVantageClient) -> list[str]:
    logger.info("Running Phase 2: APEX Predator Quality Analysis...")
    append_scan_log(session, "Faza 2: Rozpoczynanie analizy jakościowej...")

    total_candidates = len(candidate_tickers)
    update_scan_progress(session, 0, total_candidates)
    qualified_tickers = []
    processed_count = 0

    try:
        qqq_data_raw = api_client.get_daily_adjusted('QQQ', outputsize='compact')
        if not qqq_data_raw:
            raise Exception("Could not fetch QQQ data for Phase 2 analysis.")
    except Exception as e:
        logger.error(f"Critical error fetching QQQ data in Phase 2: {e}", exc_info=True)
        append_scan_log(session, "BŁĄD KRYTYCZNY: Nie można pobrać danych dla QQQ w Fazie 2.")
        return []

    for ticker in candidate_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            ticker_data_raw = api_client.get_daily_adjusted(ticker, 'full') # 'full' for BBW history
            if not ticker_data_raw:
                append_scan_log(session, f"{ticker} - Pominięty. Brak danych cenowych.")
                continue

            catalyst_score = _calculate_catalyst_score(ticker, api_client)
            strength_score = _calculate_relative_strength_score(ticker, ticker_data_raw, qqq_data_raw, api_client)
            compression_score = _calculate_energy_compression_score(ticker, api_client)
            
            total_score = catalyst_score + strength_score + compression_score
            is_qualified = total_score >= Phase2Config.MIN_APEX_SCORE_TO_QUALIFY
            
            stmt = text("""
                INSERT INTO phase2_results (ticker, analysis_date, catalyst_score, relative_strength_score, energy_compression_score, total_score, is_qualified)
                VALUES (:ticker, :date, :c_score, :rs_score, :ec_score, :total, :qual)
                ON CONFLICT (ticker, analysis_date) DO UPDATE SET
                catalyst_score = EXCLUDED.catalyst_score, relative_strength_score = EXCLUDED.relative_strength_score,
                energy_compression_score = EXCLUDED.energy_compression_score, total_score = EXCLUDED.total_score, 
                is_qualified = EXCLUDED.is_qualified;
            """)
            session.execute(stmt, {
                'ticker': ticker, 'date': date.today(), 'c_score': catalyst_score, 
                'rs_score': strength_score, 'ec_score': compression_score, 
                'total': total_score, 'qual': is_qualified
            })
            session.commit()

            log_msg = f"{ticker} - Wynik APEX: {total_score}/10 (K:{catalyst_score}, S:{strength_score}, E:{compression_score})."
            if is_qualified:
                qualified_tickers.append(ticker)
                log_msg += " Kwalifikacja do APEX Elita."
            else:
                log_msg += " Odrzucony."
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 2: {e}", exc_info=True)
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_candidates)
    
    final_log = f"Faza 2 zakończona. Zakwalifikowano {len(qualified_tickers)} spółek do APEX Elita."
    logger.info(final_log)
    append_scan_log(session, final_log)
    return qualified_tickers
