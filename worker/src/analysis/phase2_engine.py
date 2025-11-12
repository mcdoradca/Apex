import logging
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
    """
    DEKONSTRUKCJA: Ta funkcja jest już nieużywana.
    Logika Fazy 2 została wyłączona, aby zrobić miejsce dla nowego modelu AQM.
    """
    # ... (oryginalny kod zostaje, ale nie jest wywoływany) ...
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
    DEKONSTRUKCJA: Ta funkcja jest już nieużywana.
    Logika Fazy 2 została wyłączona, aby zrobić miejsce dla nowego modelu AQM.
    """
    # ... (oryginalny kod zostaje, ale nie jest wywoływany) ...
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
    DEKONSTRUKCJA: Ta funkcja jest już nieużywana.
    Logika Fazy 2 została wyłączona, aby zrobić miejsce dla nowego modelu AQM.
    """
    # ... (oryginalny kod zostaje, ale nie jest wywoływany) ...
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
    DEKONSTRUKCJA (KROK 3): Faza 2 została zmodyfikowana.
    
    Ta faza nie wykonuje już starej logiki scoringowej (Catalyst, RS, Energy).
    Jej jedynym zadaniem jest teraz pobranie pełnych danych historycznych (daily_df)
    dla *każdego* kandydata z Fazy 1 i przekazanie ich dalej.
    
    Filtrowanie i scoring zostaną wykonane w Fazie 3 przez nowy model AQM.
    """
    logger.info("Running Phase 2: Data Pre-load Stage (Old Scoring Bypassed)...")
    append_scan_log(session, "Faza 2: Ładowanie danych EOD (Stary scoring wyłączony)...")

    total_candidates = len(candidate_tickers)
    update_scan_progress(session, 0, total_candidates)
    
    # KROK 3 ZMIANA: Zmieniamy nazwę na qualified_data i typ na listę krotek
    qualified_data: List[Tuple[str, pd.DataFrame]] = []
    processed_count = 0

    # ==================================================================
    # === DEKONSTRUKCJA (KROK 3) ===
    # Usuwamy pobieranie danych QQQ, ponieważ stara logika scoringu
    # (która go używała) jest wyłączona.
    # ==================================================================
    # try:
    #     qqq_data_raw = api_client.get_daily_adjusted('QQQ', outputsize='compact')
    #     ... (cały blok try/except dla QQQ usunięty) ...
    # except Exception as e:
    #     ...
    # ==================================================================

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

            # ==================================================================
            # === DEKONSTRUKCJA (KROK 3) ===
            # Wyłączamy całą starą logikę scoringu.
            # ==================================================================
            
            # 3. (WYŁĄCZONE) Pobieranie sentymentu
            # catalyst_score = _calculate_catalyst_score(ticker, api_client)
            
            # 4. (WYŁĄCZONE) Obliczenia lokalne
            # strength_score = _calculate_relative_strength_score(ticker, daily_df, qqq_perf, api_client)
            # compression_score = _calculate_energy_compression_score(ticker, daily_df, api_client)
            
            # total_score = catalyst_score + strength_score + compression_score
            # is_qualified = total_score >= Phase2Config.MIN_APEX_SCORE_TO_QUALIFY
            
            # Zastępujemy starą logikę: Kwalifikujemy *każdy* ticker, dla którego mamy dane.
            is_qualified = True
            total_score = 0 # Wartość zastępcza, nieużywana
            # ==================================================================
            
            stmt = text("""
                INSERT INTO phase2_results (ticker, analysis_date, catalyst_score, relative_strength_score, energy_compression_score, total_score, is_qualified)
                VALUES (:ticker, :date, :c_score, :rs_score, :ec_score, :total, :qual)
                ON CONFLICT (ticker, analysis_date) DO UPDATE SET
                catalyst_score = EXCLUDED.catalyst_score, relative_strength_score = EXCLUDED.relative_strength_score,
                energy_compression_score = EXCLUDED.energy_compression_score, total_score = EXCLUDED.total_score, 
                is_qualified = EXCLUDED.is_qualified;
            """)
            session.execute(stmt, {
                'ticker': ticker, 'date': date.today(), 
                'c_score': 0, # Zapisujemy 0
                'rs_score': 0, # Zapisujemy 0
                'ec_score': 0, # Zapisujemy 0
                'total': total_score, 
                'qual': is_qualified
            })
            session.commit()

            log_msg = f"{ticker} - Faza 2: Dane EOD załadowane."
            if is_qualified:
                # KROK 3 ZMIANA: Dodajemy krotkę (ticker, dane) do listy
                qualified_data.append((ticker, daily_df))
                log_msg += " Przekazano do Fazy 3."
            # (Blok 'else' (odrzucony) nie jest już potrzebny)
            
            append_scan_log(session, log_msg)

        except Exception as e:
            logger.error(f"Error processing ticker {ticker} in Phase 2: {e}", exc_info=True)
            session.rollback()
        finally:
            processed_count += 1
            update_scan_progress(session, processed_count, total_candidates)
    
    final_log = f"Faza 2 (Ładowanie Danych) zakończona. Przekazano {len(qualified_data)} spółek do Fazy 3."
    logger.info(final_log)
    append_scan_log(session, final_log)
    
    # KROK 3 ZMIANA: Zwracamy listę krotek
    return qualified_data
