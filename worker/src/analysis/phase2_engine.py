mport logging
import time
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date
# KROK 3 ZMIANA: Importujemy List i Tuple do zdefiniowania nowego typu zwracanego
from typing import List, Tuple

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
# KROK 3 ZMIANA: Importujemy nowe, lokalne kalkulatory
from .utils import (
    update_scan_progress, append_scan_log, safe_float, 
    standardize_df_columns, calculate_rsi, calculate_bbands
)
from ..config import Phase2Config, SECTOR_TO_ETF_MAP, DEFAULT_MARKET_ETF

logger = logging.getLogger(__name__)

def _calculate_catalyst_score(ticker: str, api_client: AlphaVantageClient) -> int:
    """Oblicza wynik sentymentu (bez zmian)."""
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

def _calculate_relative_strength_score(ticker: str, daily_df: pd.DataFrame, qqq_perf: float, api_client: AlphaVantageClient) -> int:
    """
    KROK 3 ZMIANA: Oblicza siłę względną lokalnie, używając DataFrame.
    """
    score = 0
    try:
        # 1. Oblicz RSI lokalnie (zamiast wywołania API)
        rsi_series = calculate_rsi(daily_df['close'], period=9)
        if not rsi_series.empty:
            latest_rsi = rsi_series.iloc[-1]
            if latest_rsi > 60: score += 2
            elif latest_rsi > 50: score += 1
        
        # 2. Oblicz Performance vs QQQ lokalnie (5 dni)
        # Upewnijmy się, że mamy wystarczająco danych
        if len(daily_df) > 5:
            # Dane zostały już ustandaryzowane i są numeryczne
            ticker_perf = (daily_df['close'].iloc[-1] - daily_df['close'].iloc[-6]) / daily_df['close'].iloc[-6] * 100
            
            if ticker_perf > (qqq_perf * 1.5):
                score += 2
        else:
             logger.warning(f"Za mało danych dla {ticker} do obliczenia 5-dniowej wydajności.")

    except Exception as e:
        logger.error(f"Error calculating relative strength score for {ticker}: {e}", exc_info=True)
    return score


def _calculate_energy_compression_score(ticker: str, daily_df: pd.DataFrame, api_client: AlphaVantageClient) -> int:
    """
    KROK 3 ZMIANA: Oblicza kompresję energii lokalnie, używając DataFrame.
    """
    try:
        # 1. Sprawdź, czy mamy wystarczająco danych
        if len(daily_df) < 100: 
            logger.warning(f"Za mało danych (< 100) dla {ticker} do obliczenia BBands.")
            return 0
        
        # 2. Oblicz BBands i BBW lokalnie (zamiast wywołania API)
        #    POPRAWKA BŁĘDU: Oczekujemy 4 wartości (middle, upper, lower, bbw)
        middle_band, upper_band, lower_band, bbw_series = calculate_bbands(daily_df['close'], period=20)
        
        # 3. Usuń wartości NaN, które powstają na początku serii
        bbw_series = bbw_series.dropna()
        if bbw_series.empty:
            logger.warning(f"Seria BBW dla {ticker} jest pusta po usunięciu NaN.")
            return 0
        
        # 4. Oblicz rangę procentową
        percentile_rank = bbw_series.rank(pct=True).iloc[-1] # Bierzemy ostatnią (najnowszą) wartość

        if percentile_rank < 0.10: return 3
        if percentile_rank < 0.25: return 2
        if percentile_rank < 0.40: return 1
        return 0
    except Exception as e:
        logger.error(f"Error calculating energy compression score for {ticker}: {e}", exc_info=True)
    return 0

def run_analysis(session: Session, candidate_tickers: list[str], get_current_state, api_client: AlphaVantageClient) -> List[Tuple[str, pd.DataFrame]]:
    """
    KROK 3 ZMIANA: Główna funkcja Fazy 2.
    - Oblicza RSI i BBands lokalnie.
    - Zwraca listę krotek: [(ticker, daily_df), ...]
    """
    logger.info("Running Phase 2: APEX Predator Quality Analysis...")
    append_scan_log(session, "Faza 2: Rozpoczynanie analizy jakościowej...")

    total_candidates = len(candidate_tickers)
    update_scan_progress(session, 0, total_candidates)
    
    # KROK 3 ZMIANA: Zmieniamy nazwę na qualified_data i typ na listę krotek
    qualified_data: List[Tuple[str, pd.DataFrame]] = []
    processed_count = 0

    try:
        # Pobieramy dane QQQ raz, na początku
        qqq_data_raw = api_client.get_daily_adjusted('QQQ', outputsize='compact')
        if not qqq_data_raw or 'Time Series (Daily)' not in qqq_data_raw:
            raise Exception("Could not fetch QQQ data for Phase 2 analysis.")
        
        # POPRAWKA BŁĘDU: Musimy przetworzyć (standaryzować) dane QQQ
        qqq_df = pd.DataFrame.from_dict(qqq_data_raw['Time Series (Daily)'], orient='index')
        qqq_df = standardize_df_columns(qqq_df) # Konwersja na liczby
        
        if len(qqq_df) < 6:
            raise Exception("Not enough QQQ data to calculate 5-day performance.")
            
        # Obliczamy wydajność QQQ (już jako liczba, a nie string)
        qqq_perf = (qqq_df['close'].iloc[-1] - qqq_df['close'].iloc[-6]) / qqq_df['close'].iloc[-6] * 100
        logger.info(f"QQQ 5-day performance calculated: {qqq_perf:.2f}%")

    except Exception as e:
        logger.error(f"Critical error fetching or processing QQQ data in Phase 2: {e}", exc_info=True)
        append_scan_log(session, "BŁĄD KRYTYCZNY: Nie można pobrać lub przetworzyć danych dla QQQ w Fazie 2.")
        return []

    for ticker in candidate_tickers:
        if get_current_state() == 'PAUSED':
            while get_current_state() == 'PAUSED': time.sleep(1)

        try:
            # 1. Pobieramy dane historyczne (tylko 1 wywołanie API)
            ticker_data_raw = api_client.get_daily_adjusted(ticker, 'full') # 'full' dla historii BBands
            if not ticker_data_raw or 'Time Series (Daily)' not in ticker_data_raw:
                append_scan_log(session, f"{ticker} - Pominięty. Brak danych cenowych.")
                continue
            
            # 2. POPRAWKA BŁĘDU: Standaryzujemy dane (konwersja na liczby)
            daily_df = pd.DataFrame.from_dict(ticker_data_raw['Time Series (Daily)'], orient='index')
            daily_df = standardize_df_columns(daily_df)
            
            if daily_df.empty:
                append_scan_log(session, f"{ticker} - Pominięty. Puste dane po standaryzacji.")
                continue

            # 3. Pobieramy sentyment (drugie i ostatnie wywołanie API)
            catalyst_score = _calculate_catalyst_score(ticker, api_client)
            
            # 4. Obliczenia lokalne (przekazujemy przetworzony DataFrame)
            strength_score = _calculate_relative_strength_score(ticker, daily_df, qqq_perf, api_client)
            compression_score = _calculate_energy_compression_score(ticker, daily_df, api_client)
            
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
                # KROK 3 ZMIANA: Dodajemy krotkę (ticker, dane) do listy
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
    
    # KROK 3 ZMIANA: Zwracamy listę krotek
    return qualified_data

