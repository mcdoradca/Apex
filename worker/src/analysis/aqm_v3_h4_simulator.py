import logging
import pandas as pd
import numpy as np
from scipy.stats import zscore
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional, Tuple

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
    Oblicza *nienormalizowane* wartości J (entropy_change), ∇² (price_gravity) 
    i m² (attention_density) dla pojedynczego dnia, zgodnie z Mapą Warstwy Danych.
    """
    try:
        # --- 1. Oblicz ∇² (Grawitacja Cenowa) ---
        # ∇² jest aliasem dla price_gravity (Wymiar 1.2), obliczanym w daily_df
        # Korzystamy z ffill, aby znaleźć cenę VWAP i Close z najbliższego dnia
        nabla_sq = daily_df.loc[current_date]['price_gravity']
        
        if pd.isna(nabla_sq):
            return None, None, None

        # --- 2. Oblicz m² (Gęstość Uwagi) ---
        # m² jest aliasem dla attention_density (Wymiar 7.1).
        # Wymaga 200 dni historii do obliczenia Z-Score.
        # Używamy .loc[:current_date] i .iloc[-200:], aby pobrać właściwy widok historyczny
        
        # Tworzymy widoki danych kończące się na current_date
        daily_view_m_sq = daily_df.loc[:current_date]
        news_view_m_sq = news_df.loc[:current_date] 
        
        if len(daily_view_m_sq) < 200 or len(news_view_m_sq) < 200:
            # Nie wystarczająca historia dla Z-Score w m² ( attention_density)
            return None, None, None 

        m_sq = aqm_v3_metrics.calculate_attention_density_from_data(
            daily_view_m_sq,
            news_view_m_sq,
            current_date.to_pydatetime()
        )
        
        if m_sq is None:
            return None, None, None

        # --- 3. Oblicz J (Zmiana Entropii) ---
        
        # a) S (Entropia Informacyjna) - Proxy: COUNT(artykułów z ostatnich 10 dni) (str. 23)
        # news_view_j jest używany do obliczenia S, Q i μ
        news_view_j = news_df.loc[:current_date]
        S = aqm_v3_metrics.calculate_information_entropy_from_data(news_view_j)
        
        # b) Q (Przepływ Sentymentu) - retail_herding (ostatnie 7 dni)
        Q = aqm_v3_metrics.calculate_retail_herding_from_data(news_view_j, current_date.to_pydatetime())
        
        # c) T (Temperatura Rynku) - STDEV(returns_5min) (ostatnie 30 dni)
        T = aqm_v3_metrics.calculate_market_temperature_from_data(intraday_5min_df, current_date.to_pydatetime())
        
        # d) μ (Potencjał Insiderów) - institutional_sync (ostatnie 90 dni)
        mu = aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, current_date.to_pydatetime())
        
        # e) ΔN (Przewaga Informacyjna) - Stała 1.0 (str. 23)
        delta_N = 1.0 
        
        # Walidacja komponentów J
        if any(v is None for v in [S, Q, T, mu]):
            return None, None, None # Brakuje danych do obliczenia J
            
        if T == 0:
            return None, None, None # Dzielenie przez zero

        # Sztywna Formuła Analityczna (Prawo 2): J = S - (Q / T) + (μ * ΔN)
        # UWAGA: W PDF (str. 6) wzór to: $S-(Q/T)+(\mu^{*}\Delta N)$. Zgodnie z PDF (str. 23) używamy: $J=S-(Q/T)+(\mu*\Delta N)$
        J = S - (Q / T) + (mu * delta_N)
        
        if pd.isna(J):
            return None, None, None
            
        return float(J), float(nabla_sq), float(m_sq)

    except KeyError:
        # Błąd KeyError oznacza, że dla tej daty brakuje wpisu w daily_df (co jest normalne dla dni wolnych)
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
    
    daily_df = historical_data.get("daily")
    insider_df = historical_data.get("insider_df")
    news_df = historical_data.get("news_df")
    intraday_5min_df = historical_data.get("intraday_5min_df") 

    if daily_df is None or insider_df is None or news_df is None or intraday_5min_df is None:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, brak kompletnych danych (Daily, Insider, News lub Intraday 5min).")
        return 0
        
    if daily_df.empty or insider_df.empty or news_df.empty or intraday_5min_df.empty:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, jeden z DataFrame'ów jest pusty.")
        return 0

    history_window = 200 
    percentile_window = 100 
    
    if len(daily_df) < history_window + percentile_window + 1:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, za mało danych ({len(daily_df)}) do testu H3 (wymagane 301+).")
        return 0

    # Główna pętla symulacyjna
    for i in range(history_window + percentile_window, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        j_history = []
        nabla_history = []
        m_history = []
        
        components_calculated = True

        # 4. Oblicz 100-dniową historię komponentów (dla Z-Score i Percentyla)
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
                break 
                
            j_history.append(J_j)
            nabla_history.append(nabla_sq_j)
            m_history.append(m_sq_j)

        if not components_calculated:
            continue

        # 5. Normalizacja (Z-Score)
        j_series = pd.Series(j_history)
        nabla_series = pd.Series(nabla_history)
        m_series = pd.Series(m_history)

        # Używamy `scipy.stats.zscore`. `ddof=1` dla próbki (standardowe odchylenie)
        # Należy obsłużyć przypadek, gdy std=0
        
        j_norm_series = (j_series - j_series.mean()) / j_series.std(ddof=1) if j_series.std(ddof=1) != 0 else pd.Series(0, index=j_series.index)
        nabla_norm_series = (nabla_series - nabla_series.mean()) / nabla_series.std(ddof=1) if nabla_series.std(ddof=1) != 0 else pd.Series(0, index=nabla_series.index)
        m_norm_series = (m_series - m_series.mean()) / m_series.std(ddof=1) if m_series.std(ddof=1) != 0 else pd.Series(0, index=m_series.index)
        
        # Zastąp NaN wartościami 0 (powstają, gdy std=0)
        j_norm_series.fillna(0, inplace=True)
        nabla_norm_series.fillna(0, inplace=True)
        m_norm_series.fillna(0, inplace=True)
        
        # 6. Oblicz AQM_V3_SCORE (zgodnie ze specyfikacją - wagi 1.0)
        aqm_score_series = (1.0 * j_norm_series) - (1.0 * nabla_norm_series) - (1.0 * m_norm_series)
        
        # 7. Zastosuj Warunki H3 (Logika Sygnału)
        
        current_aqm_score = aqm_score_series.iloc[-1]
        
        # Oblicz 95. percentyl z *całej* 101-dniowej serii (w tym bieżący dzień)
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
                
                # 9. Przekaż do _resolve_trade
                trade = _resolve_trade(
                    daily_df, 
                    i + 1, 
                    setup_h3, 
                    max_hold_days, 
                    year, 
                    direction='LONG'
                )
                if trade:
                    session.add(trade)
                    trades_found += 1
                    
            except IndexError:
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
