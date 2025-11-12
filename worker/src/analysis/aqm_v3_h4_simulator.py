import logging
import pandas as pd
import numpy as np
# Nie potrzebujemy zscore (jak w H3), wystarczy .mean() i .std()
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional

# Importujemy modele i funkcje pomocnicze
from .. import models
# Importujemy "czysty" kalkulator komponentów z H3 (do obliczenia J)
from .aqm_v3_h3_simulator import _calculate_h3_components_for_day
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
    """
    trades_found = 0
    
    # 1. Wyodrębnij wszystkie potrzebne DataFrame'y z cache (wymagane do obliczenia 'J')
    daily_df = historical_data.get("daily")
    insider_df = historical_data.get("insider_df")
    news_df = historical_data.get("news_df")
    intraday_5min_df = historical_data.get("intraday_5min_df")

    # H4 zależy od 'J', a 'J' zależy od wszystkich tych komponentów
    if daily_df is None or insider_df is None or news_df is None or intraday_5min_df is None:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, brak kompletnych danych.")
        return 0
        
    if daily_df.empty or insider_df.empty or news_df.empty or intraday_5min_df.empty:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, jeden z DataFrame'ów jest pusty.")
        return 0

    # 2. Definicje okien (zgodnie ze specyfikacją H3 i H4)
    # Wymagamy 200 dni historii dla m² (część J)
    history_window = 200 
    # Wymagamy 100 dni historii dla statystyk J (Avg, Stdev)
    stats_window = 100
    
    # Zapewniamy, że mamy wystarczająco danych (200 dni bufora + 100 dni statystyk + 1 dzień bieżący)
    if len(daily_df) < history_window + stats_window + 1:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, za mało danych ({len(daily_df)}) do testu H4.")
        return 0

    # 3. Główna pętla symulacyjna
    # `i` reprezentuje Dzień D (Skanowanie na CLOSE)
    for i in range(history_window + stats_window, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        j_history = []
        components_calculated = True

        # 4. Oblicz 100-dniową historię komponentu J
        # Iterujemy od (i - 100) do i (włącznie), aby uzyskać 101 punktów danych
        # (Używamy 101, aby obliczyć statystyki dla 100 dni i mieć bieżącą wartość)
        for j in range(i - stats_window, i + 1):
            current_date_j = daily_df.index[j]
            
            # Używamy ponownie kalkulatora z H3, ale pobieramy tylko J
            J_j, _, _ = _calculate_h3_components_for_day(
                current_date_j,
                daily_df,
                insider_df,
                news_df,
                intraday_5min_df
            )
            
            if J_j is None:
                components_calculated = False
                break # Przerwij pętlę komponentów, jeśli brakuje danych
                
            j_history.append(J_j)

        if not components_calculated:
            # logger.warning(f"[Backtest H4] Pominięto Dzień {daily_df.index[i].date()} dla {ticker}, błąd obliczania J.")
            continue # Przejdź do następnego dnia D

        # 5. Oblicz Logikę Sygnału H4
        j_series = pd.Series(j_history)
        
        # Oblicz statystyki (zgodnie ze specyfikacją H4)
        j_avg_100 = j_series.mean()
        j_stdev_100 = j_series.std(ddof=1) # ddof=1 dla odchylenia standardowego próbki
        
        current_j = j_series.iloc[-1] # Wartość J(D)
        
        if pd.isna(current_j) or pd.isna(j_avg_100) or pd.isna(j_stdev_100) or j_stdev_100 == 0:
            continue # Nie można obliczyć sygnału (np. z powodu dzielenia przez zero w stdev)

        # Sztywny Warunek Analityka: J(D) > (J_avg_100 + (2.0 * J_stdev_100))
        is_signal = current_j > (j_avg_100 + (2.0 * j_stdev_100))

        # 6. Zastosuj Sygnał H4
        if is_signal:
            
            # --- ZNALEZIONO SYGNAŁ H4 ---
            
            # 7. Pobierz Parametry Transakcji (z Dnia D i D+1)
            try:
                candle_D = daily_df.iloc[i]
                candle_D_plus_1 = daily_df.iloc[i + 1]
                
                entry_price = candle_D_plus_1['open']
                atr_value = candle_D['atr_14'] # ATR(14, D)
                
                # Walidacja danych
                if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                    logger.warning(f"[Backtest H4] Pominięto sygnał dla {ticker} (Dzień {candle_D.name.date()}). Brak danych OPEN(D+1) lub ATR(D).")
                    continue
                
                # Parametry Egzekucji H4 (spójne z H2 i H3)
                take_profit = entry_price + (5.0 * atr_value)
                stop_loss = entry_price - (2.0 * atr_value)
                max_hold_days = 5
                
                setup_h4 = {
                    "ticker": ticker,
                    "setup_type": "AQM_V3_H4_INFO_THERMO", 
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                }
                
                # 8. Przekaż do _resolve_trade (zapożyczonego z symulatora H1)
                trade = _resolve_trade(
                    daily_df, 
                    i + 1, # Indeks Dnia D+1 (start pętli w _resolve_trade)
                    setup_h4, 
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
