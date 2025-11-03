import logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date
# KROK 3 ZMIANA: Dodajemy List, Tuple
from typing import List, Tuple, Dict, Any 
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
# KROK 3 ZMIANA: Importujemy nowe funkcje analityczne
from .utils import (
    update_scan_progress, append_scan_log, safe_float, get_performance,
    standardize_df_columns, calculate_rsi, calculate_bbands
)
from ..config import Phase2Config, SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

def _calculate_catalyst_score(ticker: str, api_client: AlphaVantageClient) -> int:
    """Oblicza wynik sentymentu. Wymaga API."""
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

# KROK 3 ZMIANA: Funkcja przyjmuje teraz DataFrame (daily_df) i dane raw (do get_performance)
# Nie potrzebuje już api_client
def _calculate_relative_strength_score(ticker: str, daily_df: pd.DataFrame, ticker_data_raw: dict, qqq_data_raw: dict) -> int:
    """Oblicza siłę względną. Używa lokalnego RSI."""
    score = 0
    try:
        # KROK 3 ZMIANA: Obliczamy RSI lokalnie
        rsi_series = calculate_rsi(daily_df, period=9)
        if not rsi_series.empty and not pd.isna(rsi_series.iloc[-1]):
            latest_rsi = rsi_series.iloc[-1]
            if latest_rsi > 60: score += 2
            elif latest_rsi > 50: score += 1
        
        # Performance vs QQQ (ta funkcja nadal używa danych RAW, co jest w porządku)
        ticker_perf = get_performance(ticker_data_raw, 5)
        qqq_perf = get_performance(qqq_data_raw, 5)

        if ticker_perf is not None and qqq_perf is not None:
            if ticker_perf > (qqq_perf * 1.5):
                score += 2

    except Exception as e:
        logger.error(f"Error calculating relative strength score for {ticker}: {e}")
    return score


# KROK 3 ZMIANA: Funkcja przyjmuje teraz DataFrame (daily_df) i nie potrzebuje api_client
def _calculate_energy_compression_score(ticker: str, daily_df: pd.DataFrame) -> int:
    """Oblicza kompresję energii. Używa lokalnego BBands."""
    try:
        # KROK 3 ZMIANA: Obliczamy BBands lokalnie
        upper_band, middle_band, lower_band = calculate_bbands(daily_df, period=20)
        
        if middle_band.empty or middle_band.isna().all():
            logger.warning(f"Nie można obliczyć lokalnego BBands dla {ticker}.")
            return 0
            
        # Obliczamy BBW (Bollinger Band Width)
        bbw = (upper_band - lower_band) / middle_band
        
        # Usuwamy wartości NaN, które powstają na początku serii
        bbw_values = bbw.dropna()
        
        if len(bbw_values) < 100: # Potrzebujemy historii do obliczenia rangi
            logger.warning(f"Niewystarczająca historia BBW dla {ticker} (potrzeba 100, jest {len(bbw_values)}).")
            return 0
        
        # Używamy ostatnich 100 dni do obliczenia rangi procentowej
        bbw_series_100d = bbw_values.iloc[-100:]
        current_bbw = bbw_series_100d.iloc[-1]
        percentile_rank = bbw_series_100d.rank(pct=True).iloc[-1]

        if percentile_rank < 0.10: return 3
        if percentile_rank < 0.25: return 2
        if percentile_rank < 0.40: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating energy compression score for {ticker}: {e}")
        return 0

# KROK 3 ZMIANA: Funkcja zwraca teraz List[Tuple[str, pd.DataFrame]]
def run_analysis(session: Session, candidate_tickers: list[str], get_current_state, api_client: AlphaVantageClient) -> List[Tuple[str, pd.DataFrame]]:
    logger.info("Running Phase 2: APEX Predator Quality Analysis...")
    append_scan_log(session, "Faza 2: Rozpoczynanie analizy jakościowej...")

    total_candidates = len(candidate_tickers)
    update_scan_progress(session, 0, total_candidates)
    
    # KROK 3 ZMIANA: Nowa lista na wyniki
    qualified_data: List[Tuple[str, pd.DataFrame]] = []
    processed_count = 0

    try:
        # Pobieramy dane dla QQQ (benchmark) raz, na początku
        qqq_data_raw = api_client.get_daily_adjusted('QQQ', outputsize='compact')
        if not qqq_data_raw or 'Time Series (Daily)' not in qqq_data_raw:
            raise Exception("Could not fetch QQQ data for Phase 2 analysis.")
        
        # KROK 3 ZMIANA: Przetwarzamy QQQ DataFrame raz
        qqq_df_raw = pd.DataFrame.from_dict(qqq_data_raw['Time Series (Daily)'], orient='index')
        qqq_df = standardize_df_columns(qqq_df_raw)
        
    except Exception as e:
        logger.error(f"Critical error fetching QQQ data in Phase 2: {e}", exc_info=True)
        append_scan_log(session, "BŁĄD KRYTYCZNY: Nie można pobrać danych dla QQQ w Fazie 2.")
        return []

    for ticker in candidate_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            # 1. Pobieramy dane OHLCV (nadal 'full' dla historii BBW)
            ticker_data_raw = api_client.get_daily_adjusted(ticker, 'full') 
            if not ticker_data_raw or 'Time Series (Daily)' not in ticker_data_raw:
                append_scan_log(session, f"{ticker} - Pominięty. Brak danych cenowych.")
                continue
                
            # 2. KROK 3 ZMIANA: Przetwarzamy na DataFrame
            daily_df_raw = pd.DataFrame.from_dict(ticker_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df_raw)
            
            if len(daily_df) < 100: # Wymagane dla 100-dniowej historii BBW
                 append_scan_log(session, f"{ticker} - Pominięty. Niewystarczająca historia danych ({len(daily_df)} dni).")
                 continue

            # 3. Pobieramy dane Sentymentu (jedyne drugie zapytanie API)
            catalyst_score = _calculate_catalyst_score(ticker, api_client)
            
            # 4. KROK 3 ZMIANA: Obliczamy resztę lokalnie
            strength_score = _calculate_relative_strength_score(ticker, daily_df, ticker_data_raw, qqq_data_raw)
            compression_score = _calculate_energy_compression_score(ticker, daily_df)
            
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
                # KROK 3 ZMIANA: Zapisujemy tickera ORAZ pobrane dane
                qualified_data.append((ticker, daily_df))
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
    
    final_log = f"Faza 2 zakończona. Zakwalifikowano {len(qualified_data)} spółek do APEX Elita."
    logger.info(final_log)
    append_scan_log(session, final_log)
    
    # KROK 3 ZMIANA: Zwracamy listę (ticker, df)
    return qualified_data
