import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional

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

logger = logging.getLogger(__name__)

def _simulate_trades_h4(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], # Oczekujemy pełnego słownika z cache
    year: str
) -> int:
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEJ SPÓŁKI
    i szuka setupów zgodnych z Hipotezą H4 (Termodynamika Informacji).
    
    REFAKTORYZACJA: Ta funkcja odczytuje teraz wstępnie obliczoną kolumnę 'J'
    i wykonuje na niej szybkie obliczenia kroczące (rolling).
    """
    trades_found = 0
    
    daily_df = historical_data.get("daily")

    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Nie potrzebujemy już insider_df, news_df itd. ===
    # ==================================================================
    if daily_df is None:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, brak kompletnych danych (Daily).")
        return 0
        
    if daily_df.empty:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, DataFrame 'daily' jest pusty.")
        return 0
    # ==================================================================

    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Ustawienie bufora i okien ===
    # ==================================================================
    history_buffer = 201 # Bezpieczny bufor dla wszystkich metryk (ten sam co w H2/H3)
    stats_window = 100 # Okno dla statystyk J (Avg, Stdev) wg specyfikacji H4
    # ==================================================================
    
    if len(daily_df) < history_buffer + 1:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, za mało danych ({len(daily_df)}) do testu H4 (wymagane {history_buffer + 1}+).")
        return 0

    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Wstępne obliczenie sygnału H4 ===
    # Wykonujemy obliczenia kroczące (rolling) wektorowo (szybko)
    # ==================================================================
    logger.info(f"[{ticker}] H4: Obliczanie kroczących statystyk 'J' (okno {stats_window}d)...")
    
    j_series = daily_df['J']
    
    # 1. Oblicz statystyki (zgodnie ze specyfikacją H4 - 100 dni)
    j_avg_100 = j_series.rolling(window=stats_window).mean() 
    j_stdev_100 = j_series.rolling(window=stats_window).std(ddof=1) 
        
    # 2. Zdefiniuj próg (2-sigma event)
    threshold_series = j_avg_100 + (2.0 * j_stdev_100)
    
    # 3. Stwórz serię sygnałów (True/False)
    is_signal_series = (j_series > threshold_series)
    # ==================================================================

    # 3. Główna pętla symulacyjna
    for i in range(history_buffer, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        # ==================================================================
        # === REFAKTORYZACJA (WYDAJNOŚĆ): Odczyt wstępnie obliczonego sygnału ===
        # ==================================================================
        is_signal = is_signal_series.iloc[i]
        # ==================================================================
        
        # 6. Zastosuj Sygnał H4
        if is_signal and pd.notna(is_signal): # Dodano pd.notna dla bezpieczeństwa
            
            # --- ZNALEZIONO SYGNAŁ H4 ---
            
            # 7. Pobierz Parametry Transakcji (z Dnia D i D+1)
            try:
                candle_D = daily_df.iloc[i]
                candle_D_plus_1 = daily_df.iloc[i + 1]
                
                entry_price = candle_D_plus_1['open']
                atr_value = candle_D['atr_14'] # ATR(14, D)
                
                # Walidacja danych
                if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                    continue
                
                # Parametry Egzekucji H4 (spójne z H2 i H3)
                take_profit = entry_price + (5.0 * atr_value)
                stop_loss = entry_price - (2.0 * atr_value)
                max_hold_days = 5
                
                # ==================================================================
                # === NOWA LOGIKA: Przygotowanie setupu z metrykami do logowania ===
                # === POPRAWKA: Konwertujemy wszystko na float() ===
                # ==================================================================
                setup_h4 = {
                    "ticker": ticker,
                    "setup_type": "AQM_V3_H4_INFO_THERMO", 
                    "entry_price": float(entry_price),
                    "stop_loss": float(stop_loss),
                    "take_profit": float(take_profit),
                    
                    # --- Dodatkowe metryki do logowania (BEZPIECZNA KONWERSJA) ---
                    "metric_atr_14": float(atr_value),
                    "metric_J": float(j_series.iloc[i]),
                    "metric_J_threshold_2sigma": float(threshold_series.iloc[i])
                }
                # ==================================================================
                
                # 8. Przekaż do _resolve_trade (zapożyczonego z symulatora H1)
                trade = _resolve_trade(
                    daily_df, 
                    i + 1, 
                    setup_h4, 
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
                logger.error(f"[Backtest H4] Błąd podczas tworzenia setupu dla {ticker} (Dzień {candle_D.name.date()}): {e}", exc_info=True)
                session.rollback()

    if trades_found > 0:
        try:
            session.commit()
            logger.info(f"[Backtest H4] Pomyślnie zapisano {trades_found} transakcji H4 dla {ticker} (Rok: {year}).")
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji H4 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
