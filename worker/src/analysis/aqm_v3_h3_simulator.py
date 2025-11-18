import logging
import pandas as pd
import numpy as np
# ==================================================================
# === REFAKTORYZACJA (WYDAJNOŚĆ): Usunięto scipy (Z-Score) ===
# Obliczenia Z-Score są teraz robione wektorowo przez Pandas
# ==================================================================
# from scipy.stats import zscore
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional, Tuple

# Importujemy modele i funkcje pomocnicze
from .. import models
# ==================================================================
# === REFAKTORYZACJA (WYDAJNOŚĆ): Usunięto import aqm_v3_metrics ===
# Obliczenia są teraz wykonywane w backtest_engine
# ==================================================================
# from . import aqm_v3_metrics 
# ==================================================================
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
    i szuka setupów zgodnych z Hipotezą H3 (Uproszczony Model Pola).
    
    REFAKTORYZACJA: Ta funkcja odczytuje teraz wstępnie obliczone kolumny
    'J', 'nabla_sq', 'm_sq' i wykonuje na nich szybkie obliczenia kroczące.
    """
    trades_found = 0
    
    daily_df = historical_data.get("daily")

    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Nie potrzebujemy już insider_df, news_df itd. ===
    # ==================================================================
    if daily_df is None:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, brak kompletnych danych (Daily).")
        return 0
        
    if daily_df.empty:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, DataFrame 'daily' jest pusty.")
        return 0
    # ==================================================================

    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Ustawienie bufora i okien ===
    # ==================================================================
    history_buffer = 201 # Bezpieczny bufor dla wszystkich metryk (ten sam co w H2)
    percentile_window = 100 # Wg specyfikacji (100-dniowy percentyl)
    # ==================================================================
    
    if len(daily_df) < history_buffer + 1:
        logger.warning(f"[Backtest V3][H3] Pominięto {ticker}, za mało danych ({len(daily_df)}) do testu H3 (wymagane {history_buffer + 1}+).")
        return 0

    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Wstępne obliczenie Z-Scores i AQM_SCORE ===
    # Wykonujemy obliczenia kroczące (rolling) wektorowo (szybko) zamiast pętli w pętli
    # ==================================================================
    logger.info(f"[{ticker}] H3: Obliczanie kroczących Z-Scores i AQM_Score (okno {percentile_window}d)...")
    
    # 1. Stwórz kroczące Z-Scores
    # (wartość - śr. krocząca) / odch. standardowe kroczące
    j_mean = daily_df['J'].rolling(window=percentile_window).mean()
    j_std = daily_df['J'].rolling(window=percentile_window).std(ddof=1)
    j_norm = (daily_df['J'] - j_mean) / j_std
    
    nabla_mean = daily_df['nabla_sq'].rolling(window=percentile_window).mean()
    nabla_std = daily_df['nabla_sq'].rolling(window=percentile_window).std(ddof=1)
    nabla_norm = (daily_df['nabla_sq'] - nabla_mean) / nabla_std
    
    m_mean = daily_df['m_sq'].rolling(window=percentile_window).mean()
    m_std = daily_df['m_sq'].rolling(window=percentile_window).std(ddof=1)
    m_norm = (daily_df['m_sq'] - m_mean) / m_std

    # 2. Zastąp nieskończoności (z dzielenia przez 0 std) i NaNy
    # Używamy .replace zamiast .fillna(0, inplace=True) aby uniknąć ostrzeżeń
    j_norm = j_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    nabla_norm = nabla_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    m_norm = m_norm.replace([np.inf, -np.inf], np.nan).fillna(0)

    # 3. Oblicz AQM_V3_SCORE (zgodnie ze specyfikacją - wagi 1.0)
    aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
    
    # 4. Oblicz 95. percentyl (kroczący)
    percentile_95_series = aqm_score_series.rolling(window=percentile_window).quantile(0.95)

    # ==================================================================
    
    # Główna pętla symulacyjna
    for i in range(history_buffer, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        # ==================================================================
        # === REFAKTORYZACJA (WYDAJNOŚĆ): Odczyt wstępnie obliczonych wartości ===
        # ==================================================================
        
        # 7. Zastosuj Warunki H3 (Logika Sygnału)
        
        current_aqm_score = aqm_score_series.iloc[i]
        percentile_95 = percentile_95_series.iloc[i]
        
        # === TEST 1 (Analityk): Odczyt m_norm dla filtru ===
        current_m_norm = m_norm.iloc[i]
        # ===================================================

        # ==================================================================

        # Sygnał KUPNA = Warunek 1 (AQM_V3_SCORE > 95. percentyl)
        if pd.isna(current_aqm_score) or pd.isna(percentile_95):
            continue
        
        # === TEST 1 (Analityk): Dodano warunek (current_m_norm < -1.0) ===
        # Hipoteza: Unikajmy sygnałów, które są już "zbyt popularne" (wysokie m_sq).
        # Uwaga: m_norm jest odejmowane we wzorze na AQM_SCORE (J - nabla - m), 
        # więc niskie m_norm (ujemne) zwiększa wynik AQM.
        # Ale analityk prosił o warunek `m_sq_norm < -1.0`.
        # Jeśli m_sq_norm jest < -1.0, to znaczy, że uwaga jest BARDZO NISKA (poniżej średniej).
        # To zgodne z logiką "unikania popularności".
        if (current_aqm_score > percentile_95) and (current_m_norm < -1.0):
        # ==================================================================
            
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
                
                # ==================================================================
                # === NOWA LOGIKA: Przygotowanie setupu z metrykami do logowania ===
                # === POPRAWKA: Konwertujemy wszystko na float() ===
                # ==================================================================
                setup_h3 = {
                    "ticker": ticker,
                    # === ZMIANA NAZWY SETUPU DLA TESTU 1 ===
                    "setup_type": "AQM_V3_H3_QUANTUM_FIELD_TEST1", 
                    # =======================================
                    "entry_price": float(entry_price),
                    "stop_loss": float(stop_loss),
                    "take_profit": float(take_profit),
                    
                    # --- Dodatkowe metryki do logowania (BEZPIECZNA KONWERSJA) ---
                    "metric_atr_14": float(atr_value),
                    "metric_aqm_score_h3": float(current_aqm_score),
                    "metric_aqm_percentile_95": float(percentile_95),
                    "metric_J_norm": float(j_norm.iloc[i]),
                    "metric_nabla_sq_norm": float(nabla_norm.iloc[i]),
                    "metric_m_sq_norm": float(current_m_norm),
                }
                # ==================================================================

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
            logger.info(f"[Backtest H3] Pomyślnie zapisano {trades_found} transakcji H3 (TEST 1) dla {ticker} (Rok: {year}).")
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji H3 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
