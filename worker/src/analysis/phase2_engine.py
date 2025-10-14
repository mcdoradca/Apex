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

# --- ZAAWANSOWANI AGENCI SCORINGOWI (ZGODNIE Z PDF) ---

def _calculate_catalyst_score(ticker: str, api_client: AlphaVantageClient) -> int:
    """Agent Katalizatora - Logika progresywna."""
    try:
        news_data = api_client.get_news_sentiment(ticker)
        if not news_data or not news_data.get('feed'): return 0
        
        relevant_scores = [item['overall_sentiment_score'] for item in news_data['feed'] if item.get('overall_sentiment_score') is not None]
        if not relevant_scores: return 0
        
        avg_sentiment = sum(relevant_scores) / len(relevant_scores)

        if avg_sentiment > 0.50: return 3
        if avg_sentiment > 0.20: return 2
        if avg_sentiment > 0.05: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating catalyst score for {ticker}: {e}", exc_info=True)
        return 0

def _calculate_relative_strength_score(ticker_data: dict, qqq_data: dict) -> int:
    """Agent Siły Względnej - Logika progresywna z RSI(9)."""
    try:
        score = 0
        
        # 1. Analiza RSI
        rsi_series = ticker_data.get('rsi_9')
        if rsi_series is not None and not rsi_series.empty:
            latest_rsi = rsi_series.iloc[-1]
            if latest_rsi > 50: score += 1
            if latest_rsi > 60: score += 1
        
        # 2. Porównanie stopy zwrotu z QQQ
        ticker_perf_5d = get_performance(ticker_data, 5)
        qqq_perf_5d = get_performance(qqq_data, 5)

        if ticker_perf_5d is not None and qqq_perf_5d is not None:
            # Unikamy dzielenia przez zero i obsługujemy ujemne zwroty QQQ
            if qqq_perf_5d > 0 and ticker_perf_5d > (qqq_perf_5d * 1.5):
                score += 2
            # Jeśli rynek spada, a spółka rośnie, to jest to bardzo silny sygnał
            elif qqq_perf_5d <= 0 and ticker_perf_5d > 0:
                score += 2
                
        return score
    except Exception as e:
        logger.error(f"Error calculating relative strength score: {e}", exc_info=True)
        return 0


def _calculate_energy_compression_score(ticker_data: dict) -> int:
    """Agent Kompresji Energii - Logika adaptacyjna z rangą percentylową BBW."""
    try:
        bbands_data = ticker_data.get('bbands_20')
        if bbands_data is None: return 0

        # Oblicz Bollinger Band Width (BBW)
        bbands_data['bandwidth'] = (bbands_data['Real Upper Band'] - bbands_data['Real Lower Band']) / bbands_data['Real Middle Band']
        
        if len(bbands_data) < 100: return 0 # Potrzebujemy historii do obliczenia rangi

        # Ranga percentylowa dla ostatniej wartości w oknie 100-okresowym
        latest_bandwidth = bbands_data['bandwidth'].iloc[-1]
        historical_bandwidth = bbands_data['bandwidth'].iloc[-100:]
        
        percentile_rank = historical_bandwidth.rank(pct=True).iloc[-1] * 100

        if percentile_rank < 10: return 3
        if percentile_rank < 25: return 2
        if percentile_rank < 40: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating energy compression score: {e}", exc_info=True)
        return 0


def run_analysis(session: Session, candidate_tickers: list[str], get_current_state, api_client: AlphaVantageClient) -> list[str]:
    logger.info("Running Phase 2: APEX Predator Quality Analysis (v2.0)...")
    append_scan_log(session, "Faza 2 (v2.0): Rozpoczynanie zaawansowanej analizy scoringowej...")

    total_candidates = len(candidate_tickers)
    update_scan_progress(session, 0, total_candidates)
    qualified_tickers = []
    processed_count = 0

    # Wyczyść stare wyniki Fazy 2 przed nową analizą
    try:
        session.execute(text("DELETE FROM phase2_results"))
        session.commit()
    except Exception as e:
        logger.error(f"Could not clear old Phase 2 results: {e}")
        session.rollback()

    # Pobierz dane QQQ raz, aby zaoszczędzić zapytania API
    try:
        qqq_data = {
            'Time Series (Daily)': api_client.get_daily_adjusted('QQQ', 'compact').get('Time Series (Daily)')
        }
        if not qqq_data['Time Series (Daily)']: raise ValueError("QQQ data is empty")
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to fetch QQQ data for Phase 2. Halting. Error: {e}")
        append_scan_log(session, "BŁĄD KRYTYCZNY: Nie można pobrać danych dla QQQ. Faza 2 przerwana.")
        return []

    for ticker in candidate_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            # --- Zunifikowane pobieranie danych dla tickera ---
            daily_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
            rsi_data_raw = api_client.get_rsi(ticker, time_period=9) # RSI(9)
            bbands_data_raw = api_client.get_bollinger_bands(ticker, time_period=20)

            if not daily_data_raw or 'Time Series (Daily)' not in daily_data_raw:
                append_scan_log(session, f"{ticker}: Pomięty - brak podstawowych danych cenowych.")
                continue
            
            # Konwersja do DataFrame dla łatwiejszych obliczeń
            ticker_data = {
                'Time Series (Daily)': daily_data_raw.get('Time Series (Daily)')
            }
            if rsi_data_raw and 'Technical Analysis: RSI' in rsi_data_raw:
                ticker_data['rsi_9'] = pd.DataFrame.from_dict(rsi_data_raw['Technical Analysis: RSI'], orient='index').astype(float)
            
            if bbands_data_raw and 'Technical Analysis: BBANDS' in bbands_data_raw:
                 ticker_data['bbands_20'] = pd.DataFrame.from_dict(bbands_data_raw['Technical Analysis: BBANDS'], orient='index').astype(float)


            # --- Obliczanie wyników od poszczególnych agentów ---
            catalyst_score = _calculate_catalyst_score(ticker, api_client)
            strength_score = _calculate_relative_strength_score(ticker_data, qqq_data)
            compression_score = _calculate_energy_compression_score(ticker_data)
            
            total_score = catalyst_score + strength_score + compression_score
            is_qualified = total_score >= Phase2Config.MIN_APEX_SCORE_TO_QUALIFY
            
            # Zapisz wynik do bazy danych
            stmt = text("""
                INSERT INTO phase2_results (ticker, analysis_date, catalyst_score, relative_strength_score, 
                                            energy_compression_score, total_score, is_qualified)
                VALUES (:ticker, :date, :c_score, :rs_score, :ec_score, :total, :qual)
            """)
            session.execute(stmt, {
                'ticker': ticker, 'date': date.today(), 'c_score': catalyst_score, 
                'rs_score': strength_score, 'ec_score': compression_score, 
                'total': total_score, 'qual': is_qualified
            })
            session.commit()

            log_msg = f"{ticker} - APEX Score: {total_score}/10."
            if is_qualified:
                qualified_tickers.append(ticker)
                log_msg += " Kwalifikacja do Fazy 3."
            else:
                log_msg += " Odrzucony."
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 2: {e}", exc_info=True)
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_candidates)
    
    final_log = f"Faza 2 zakończona. Zakwalifikowano {len(qualified_tickers)} spółek do Fazy 3."
    logger.info(final_log)
    append_scan_log(session, final_log)
    return qualified_tickers
