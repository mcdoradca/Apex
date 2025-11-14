import logging
import pandas as pd
import numpy as np
from scipy.stats import zscore
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional, Tuple

# Importujemy modele i funkcje pomocnicze
from .. import models
# Importujemy "czyste" funkcje obliczeniowe z metryk AQM (nowy docelowy dom)
from . import aqm_v3_metrics 
# Importujemy funkcję egzekucji transakcji z symulatora H1
from .aqm_v3_h1_simulator import _resolve_trade
# Importujemy ATR z utils
from .utils import calculate_atr

logger = logging.getLogger(__name__)

# ==================================================================
# === USUNIĘTO: Funkcja _calculate_h3_components_for_day została przeniesiona do aqm_v3_metrics.py ===
# ==================================================================

def _simulate_trades_h3(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], # Oczekujemy pełnego słownika z cache
    year: str
) -> int:
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEJ SPÓŁKI
    i szuka setupów zgodnych z Hipotezy H3 (Uproszczony Model Pola).
    
    KLUCZOWA POPRAWKA: Zabezpiecza pętlę historyczną przed przerwaniem przez brakujące dane (newsy/insider).
    """
    trades_found = 0
    
    daily_df = historical_data.get("daily")
    insider_df = historical_data.get("insider_df")
    news_df = historical_data.get("news_df")
    # Ten DF jest teraz pusty (po Krok 4/4), ale przekazujemy go dla spójności
    intraday_5min_df = historical_data.get("intraday_5min_df") 

    if daily_df is None or insider_df is None or news_df is None:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, brak kompletnych danych (Daily, Insider lub News).")
        return 0
        
    if daily_df.empty:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, DataFrame 'daily' jest pusty.")
        return 0

    history_window = 200 
    percentile_window = 100 
    
    if len(daily_df) < history_window + percentile_window + 1:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, za mało danych ({len(daily_df)}) do testu H3 (wymagane {history_window + percentile_window + 1}+).")
        return 0

    # Główna pętla symulacyjna
    for i in range(history_window + percentile_window, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        j_history = []
        nabla_history = []
        m_history = []
        
        # 4. Oblicz 100-dniową historię komponentów (dla Z-Score i Percentyla)
        start_idx = i - percentile_window
        end_idx = i + 1
        
        # Używamy full_daily_df, aby pobrać wszystkie dane historyczne do indeksu i
        daily_view_hist = daily_df.iloc[:end_idx] 
        # News/Insider views są globalne, filtrowane w metrykach
        
        # Zmienna do śledzenia brakujących danych, ale nie przerywająca pętli od razu
        data_valid = True

        for j in range(start_idx, end_idx): # Iteruj po indeksach (dniach)
            current_date_j = daily_df.index[j]
            
            # Wycinek danych dziennych kończący się na dacie j (potrzebne dla price_gravity, market_temp)
            daily_view_j = daily_df.loc[daily_df.index <= current_date_j]
            
            # Przekaż datę J, ale filtruj dane wewnątrz metryki (dla większej czystości)
            # Używamy teraz daily_df zamiast intraday do market_temp
            J_j, nabla_sq_j, m_sq_j = aqm_v3_metrics.calculate_h3_components_for_day(
                current_date_j,
                daily_view_j,        # Widok Dzienny do daty J (dla price_gravity, market_temp)
                insider_df,          # Pełna historia insider
                news_df,             # Pełna historia news
                daily_df,            # Pełny DF (dla BBANDS/History - potrzebne do entropii)
                intraday_5min_df     # (Pusty) DF
            )
            
            # === KLUCZOWA ZMIANA LOGIKI PĘTLI: Zabezpieczenie przed None ===
            
            # Jeśli brakuje danych, ustaw na 0.0 (neutralny wpływ) i kontynuuj
            if J_j is None:
                J_j = 0.0 
                data_valid = False
            if nabla_sq_j is None:
                nabla_sq_j = 0.0
                data_valid = False
            if m_sq_j is None:
                m_sq_j = 0.0
                data_valid = False
                
            j_history.append(J_j)
            nabla_history.append(nabla_sq_j)
            m_history.append(m_sq_j)

        if not data_valid and (j_history.count(0.0) / len(j_history) > 0.5):
            # Jeśli ponad 50% historycznych punktów to 0.0, pomiń ten cykl (za mało informacji)
            # To jest nasza nowa linia obrony przed "pustymi DataFrame'ami"
            logger.warning(f"[Backtest H3] Pominięto {ticker} (Dzień D: {daily_df.index[i].date()}). Ponad 50% historii metryk to 0.0 (brak danych news/insider).")
            continue


        # 5. Normalizacja (Z-Score)
        j_series = pd.Series(j_history)
        nabla_series = pd.Series(nabla_history)
        m_series = pd.Series(m_history)

        # Używamy `scipy.stats.zscore`. `ddof=1` dla próbki (standardowe odchylenie)
        # Należy obsłużyć przypadek, gdy std=0
        
        j_norm_series = (j_series - j_series.mean()) / j_series.std(ddof=1) if j_series.std(ddof=1) != 0 else pd.Series(0, index=j_series.index)
        nabla_norm_series = (nabla_series - nabla_series.mean()) / nabla_series.std(ddof=1) if nabla_series.std(ddof=1) != 0 else pd.Series(0, index=nabla_series.index)
        m_norm_series = (m_series - m_series.mean()) / m_series.std(ddof=1) if m_series.std(ddof=1) != 0 else pd.Series(0, index=nabla_series.index)
        
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
