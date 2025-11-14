import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional

# Importujemy modele i funkcje pomocnicze
from .. import models
# Importujemy "czysty" kalkulator komponentów z metryk AQM (nowy docelowy dom)
from . import aqm_v3_metrics 
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
    # Ten DF jest teraz pusty (po Krok 4/4), ale przekazujemy go dla spójności
    intraday_5min_df = historical_data.get("intraday_5min_df") 

    if daily_df is None or insider_df is None or news_df is None:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, brak kompletnych danych (Daily, Insider lub News).")
        return 0
        
    if daily_df.empty:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, DataFrame 'daily' jest pusty.")
        return 0

    # 2. Definicje okien
    history_window = 200 # Wymagane dla m² w obliczeniach J
    stats_window = 100 # Okno dla statystyk J (Avg, Stdev)
    
    # Zapewniamy, że mamy wystarczająco danych (200 + 100 + 1)
    if len(daily_df) < history_window + stats_window + 1:
        logger.warning(f"[Backtest V3][H4] Pominięto {ticker}, za mało danych ({len(daily_df)}) do testu H4 (wymagane {history_window + stats_window + 1}+).")
        return 0

    # 3. Główna pętla symulacyjna
    for i in range(history_window + stats_window, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        j_history = []
        data_valid = True # Flaga do śledzenia braków
        
        # 4. Oblicz 100-dniową historię komponentu J
        start_idx = i - stats_window
        end_idx = i + 1
        
        for j in range(start_idx, end_idx): # Iteruj po indeksach (dniach)
            current_date_j = daily_df.index[j]
            
            # Wycinek danych dziennych kończący się na dacie j 
            daily_view_j = daily_df.loc[daily_df.index <= current_date_j]
            
            # Używamy _calculate_h3_components_for_day z modułu metryk
            J_j, nabla_sq_j, m_sq_j = aqm_v3_metrics.calculate_h3_components_for_day(
                current_date_j,
                daily_view_j,        # Widok Dzienny do daty J
                insider_df,          # Pełna historia insider
                news_df,             # Pełna historia news
                daily_df,            # Pełny DF (dla BBANDS/History)
                intraday_5min_df     # (Pusty) DF
            )
            
            # === KLUCZOWA ZMIANA: Zabezpieczenie przed None ===
            # Jeśli brakuje danych, ustaw na 0.0 (neutralny wpływ)
            if J_j is None:
                J_j = 0.0 
                data_valid = False
            
            j_history.append(J_j)

        # Logika pomijania, jeśli ponad 50% historii to zera (brak danych)
        if not data_valid and (j_history.count(0.0) / len(j_history) > 0.5):
            logger.warning(f"[Backtest H4] Pominięto {ticker} (Dzień D: {daily_df.index[i].date()}). Ponad 50% historii J to 0.0 (brak danych news/insider).")
            continue

        # 5. Oblicz Logikę Sygnału H4 (Szok Informacyjny)
        j_series = pd.Series(j_history)
        
        # Oblicz statystyki (zgodnie ze specyfikacją H4 - 100 dni)
        # Ilosć dni do obliczenia średniej i stdev to 100 (iloc[:-1])
        j_avg_100 = j_series.iloc[:-1].mean() 
        j_stdev_100 = j_series.iloc[:-1].std(ddof=1) 
        
        current_j = j_series.iloc[-1] # Wartość J(D)

        # Używamy domyślnego stdev dla bezpieczeństwa
        if pd.isna(current_j) or pd.isna(j_avg_100) or pd.isna(j_stdev_100):
            continue 

        # Sztywny Warunek Analityka: J(D) > (J_avg_100 + (2.0 * J_stdev_100))
        if j_stdev_100 == 0:
            is_signal = False
        else:
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
