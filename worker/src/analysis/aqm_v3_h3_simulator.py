import logging
import pandas as pd
import numpy as np
from scipy.stats import zscore
from sqlalchemy.orm import Session
from sqlalchemy import text
# ==================================================================
# === POPRAWKA (NameError): Dodano 'Tuple' do importu ===
# ==================================================================
from typing import Dict, Any, Optional, Tuple
# ==================================================================

# Importujemy modele i funkcje pomocnicze
from .. import models
# Importujemy "czyste" funkcje obliczeniowe z metryk AQM
from . import aqm_v3_metrics
# Importujemy funkcję egzekucji transakcji z symulatora H1
from .aqm_v3_h1_simulator import _resolve_trade
# Importujemy ATR z utils
from .utils import calculate_atr

logger = logging.getLogger(__name__)

def _calculate_h3_components_for_day(
    current_date: pd.Timestamp,
    daily_df: pd.DataFrame,
    insider_df: pd.DataFrame,
    news_df: pd.DataFrame,
    intraday_5min_df: pd.DataFrame
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Oblicza *nienormalizowane* wartości J, ∇² i m² dla pojedynczego dnia.
    """
    try:
        # 1. Oblicz ∇² (Grawitacja Cenowa) - jest już na daily_df
        # Używamy .loc, aby bezpiecznie pobrać wartość dla DOKŁADNEJ daty
        nabla_sq = daily_df.loc[current_date]['price_gravity']
        
        # 2. Oblicz m² (Gęstość Uwagi) - Wymaga 200 dni historii
        # Musimy zapewnić, że mamy co najmniej 200 dni danych PRZED current_date
        daily_view_m_sq = daily_df.loc[:current_date].iloc[-200:]
        news_view_m_sq = news_df.loc[:current_date].iloc[-200:] # Uproszczenie, news_df może być rzadki
        
        if len(daily_view_m_sq) < 200:
            return None, None, None # Niewystarczająca historia dla Z-Score w m²

        m_sq = aqm_v3_metrics.calculate_attention_density_from_data(
            daily_view_m_sq,
            news_view_m_sq,
            current_date
        )
        
        # 3. Oblicz J (Zmiana Entropii)
        
        # S (Entropia) - 100 ostatnich newsów
        news_view_s = news_df.loc[:current_date].iloc[-100:]
        S = aqm_v3_metrics.calculate_information_entropy_from_data(news_view_s)
        
        # Q (Retail Herding) - ostatnie 7 dni
        Q = aqm_v3_metrics.calculate_retail_herding_from_data(news_df, current_date)
        
        # T (Temperatura Rynku) - ostatnie 30 dni danych 5-min
        T = aqm_v3_metrics.calculate_market_temperature_from_data(intraday_5min_df, current_date)
        
        # μ (Institutional Sync) - ostatnie 90 dni
        mu = aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, current_date)
        
        # ΔN (Odwrotność m²)
        if m_sq is None or m_sq == 0:
            return None, None, None # Nie można obliczyć J bez m²
        
        delta_N = 1.0 / m_sq
        
        # Walidacja komponentów J
        if any(v is None for v in [S, Q, T, mu, delta_N]):
            return None, None, None # Brakuje danych do obliczenia J
            
        if T == 0:
            return None, None, None # Dzielenie przez zero

        # Sztywna Formuła Analityczna: J = S - (Q / T) + (μ * ΔN)
        J = S - (Q / T) + (mu * delta_N)
        
        if pd.isna(J) or pd.isna(nabla_sq) or pd.isna(m_sq):
            return None, None, None
            
        return J, nabla_sq, m_sq

    except KeyError:
        # logger.warning(f"[Backtest H3] Brak danych (KeyError) dla {current_date.date()}.")
        return None, None, None
    except Exception as e:
        logger.error(f"[Backtest H3] Błąd w _calculate_h3_components_for_day: {e}", exc_info=True)
        return None, None, None

def _simulate_trades_h3(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], # Oczekujemy pełnego słownika z cache
    year: str
) -> int:
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEJ SPÓŁKI
    i szuka setupów zgodnych z Hipotezą H3 (Uproszczony Model Pola).
    """
    trades_found = 0
    
    # 1. Wyodrębnij wszystkie potrzebne DataFrame'y z cache
    daily_df = historical_data.get("daily")
    insider_df = historical_data.get("insider_df")
    news_df = historical_data.get("news_df")
    intraday_5min_df = historical_data.get("intraday_5min_df") # Wymagany dla 'T'
    # bbands_df = historical_data.get("bbands_df") # Niewymagany dla H3

    # Wymagamy wszystkich zestawów danych do uruchomienia testu H3
    if daily_df is None or insider_df is None or news_df is None or intraday_5min_df is None:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, brak kompletnych danych (Daily, Insider, News lub Intraday 5min).")
        return 0
        
    if daily_df.empty or insider_df.empty or news_df.empty or intraday_5min_df.empty:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, jeden z DataFrame'ów jest pusty.")
        return 0

    # 2. Definicje okien (zgodnie ze specyfikacją)
    # m² (attention_density) wymaga 200-dniowego okna dla swojego Z-Score,
    # a sam AQM_V3_SCORE wymaga 100-dniowego okna dla percentyla.
    # Używamy 200 jako minimalnego wymaganego bufora historii.
    history_window = 200 
    percentile_window = 100 # Okno dla Z-Score i Percentyla (wg specyfikacji H3)
    
    # Zapewniamy, że mamy wystarczająco danych (200 dni historii + 100 dni okna percentyla + 1 dzień bieżący)
    if len(daily_df) < history_window + percentile_window + 1:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, za mało danych ({len(daily_df)}) do testu H3.")
        return 0

    # 3. Główna pętla symulacyjna
    # Zaczynamy od indeksu, który pozwala na 100-dniowe okno percentyla ORAZ
    # 200-dniowe okno dla obliczenia m² (attention_density).
    # `i` reprezentuje Dzień D (Skanowanie na CLOSE)
    for i in range(history_window + percentile_window, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        # Listy do przechowywania 100-dniowej historii komponentów
        j_history = []
        nabla_history = []
        m_history = []
        
        components_calculated = True

        # 4. Oblicz 100-dniową historię komponentów (dla Z-Score i Percentyla)
        # Iterujemy od (i - 100) do i (włącznie), aby uzyskać 101 punktów danych
        for j in range(i - percentile_window, i + 1):
            current_date_j = daily_df.index[j]
            
            # Oblicz komponenty J, ∇², m² dla dnia 'j'
            J_j, nabla_sq_j, m_sq_j = _calculate_h3_components_for_day(
                current_date_j,
                daily_df,
                insider_df,
                news_df,
                intraday_5min_df
            )
            
            if J_j is None or nabla_sq_j is None or m_sq_j is None:
                components_calculated = False
                break # Przerwij pętlę komponentów, jeśli brakuje danych
                
            j_history.append(J_j)
            nabla_history.append(nabla_sq_j)
            m_history.append(m_sq_j)

        if not components_calculated:
            # logger.warning(f"[Backtest H3] Pominięto Dzień {daily_df.index[i].date()} dla {ticker}, błąd obliczania komponentów.")
            continue # Przejdź do następnego dnia D

        # 5. Normalizacja (Z-Score)
        # Konwertujemy na serie, aby łatwo obliczyć Z-Score
        j_series = pd.Series(j_history)
        nabla_series = pd.Series(nabla_history)
        m_series = pd.Series(m_history)

        # Używamy `scipy.stats.zscore`. `ddof=1` dla próbki (standardowe odchylenie)
        j_norm_series = pd.Series(zscore(j_series, ddof=1))
        nabla_norm_series = pd.Series(zscore(nabla_series, ddof=1))
        m_norm_series = pd.Series(zscore(m_series, ddof=1))
        
        # 6. Oblicz AQM_V3_SCORE (zgodnie ze specyfikacją)
        # Wagi w1=1.0, w2=1.0, w3=1.0
        aqm_score_series = (1.0 * j_norm_series) - (1.0 * nabla_norm_series) - (1.0 * m_norm_series)
        
        # 7. Zastosuj Warunki H3 (Logika Sygnału)
        
        # Pobierz bieżącą (ostatnią) wartość z 101-dniowej serii
        current_aqm_score = aqm_score_series.iloc[-1]
        
        # Oblicz 95. percentyl z *całej* 101-dniowej serii
        percentile_95 = aqm_score_series.quantile(0.95)

        # Sygnał KUPNA = Warunek 1 (AQM_V3_SCORE > 95. percentyl)
        if current_aqm_score > percentile_95:
            
            # --- ZNALEZIONO SYGNAŁ H3 ---
            
            # 8. Pobierz Parametry Transakcji (z Dnia D i D+1)
            try:
                candle_D = daily_df.iloc[i]
                candle_D_plus_1 = daily_df.iloc[i + 1]
                
                entry_price = candle_D_plus_1['open']
                atr_value = candle_D['atr_14']
                
                # Walidacja danych (bardzo ważna)
                if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                    logger.warning(f"[Backtest H3] Pominięto sygnał dla {ticker} (Dzień {candle_D.name.date()}). Brak danych OPEN(D+1) lub ATR(D).")
                    continue
                
                # Używamy parametrów ze Specyfikacji H3
                take_profit = entry_price + (5.0 * atr_value)
                stop_loss = entry_price - (2.0 * atr_value)
                max_hold_days = 5
                
                setup_h3 = {
                    "ticker": ticker,
                    "setup_type": "AQM_V3_H3_QUANTUM_FIELD", 
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                }
                
                # 9. Przekaż do _resolve_trade (zapożyczonego z symulatora H1)
                # Przekazujemy pełny DataFrame i indeks dnia WEJŚIA (D+1)
                trade = _resolve_trade(
                    daily_df, 
                    i + 1, # Indeks Dnia D+1 (start pętli w _resolve_trade)
                    setup_h3, 
                    max_hold_days, 
                    year, 
                    direction='LONG'
                )
                if trade:
                    session.add(trade)
                    trades_found += 1
                    
            except IndexError:
                # Dzień D był ostatnim dniem w danych, nie ma D+1
                continue
            except Exception as e:
                logger.error(f"[Backtest H3] Błąd podczas tworzenia setupu dla {ticker} (Dzień {candle_D.name.date()}): {e}", exc_info=True)
                session.rollback()

    if trades_found > 0:
        try:
            session.commit()
            logger.info(f"[Backtest H3] Pomyślnie zapisano {trades_found} transakcji H3 dla {ticker} (Rok: {year}).")
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji H3 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
