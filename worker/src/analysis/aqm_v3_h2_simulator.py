import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any

# Importujemy modele i funkcje pomocnicze
from .. import models
# Importujemy "czyste" funkcje obliczeniowe z Kroków 17 i 20a
from . import aqm_v3_metrics 
# Importujemy funkcję egzekucji transakcji z Krok 19a
from .aqm_v3_h1_simulator import _resolve_trade

logger = logging.getLogger(__name__)

# ==================================================================
# === KROK 21a: Implementacja Pętli Symulacyjnej dla Hipotezy H2 ===
# ==================================================================

def _simulate_trades_h2(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], # Oczekujemy słownika z cache
    year: str
) -> int:
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEGÓ SPÓŁKI
    i szuka setupów zgodnych z Hipotezą H2 (Splątanie Kwantowe).
    
    Wymaga, aby 'historical_data' (słownik) zawierał:
    - 'daily': Wzbogacony DF z 'atr_14'
    - 'insider_df': Pełna historia transakcji insiderów
    - 'news_df': Pełna historia sentymentu newsów
    """
    trades_found = 0
    
    daily_df = historical_data.get("daily")
    insider_df = historical_data.get("insider_df")
    news_df = historical_data.get("news_df")

    # Wymagamy wszystkich trzech zestawów danych do uruchomienia testu H2
    if daily_df is None or insider_df is None or news_df is None:
        logger.warning(f"[Backtest V3][H2] Pominięto {ticker}, brak kompletnych danych (Daily, Insider lub News).")
        return 0

    # Zaczynamy od 1 (aby mieć dane D-1), ale musimy też zapewnić, że mamy dane D+1
    for i in range(1, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        # 1. Pobierz dane dla Dnia D
        candle_D = daily_df.iloc[i]
        current_date = candle_D.name.to_pydatetime() # Pobierz datę jako obiekt datetime
        
        # 2. Oblicz Metrykę 2.1: institutional_sync (ostatnie 90 dni)
        # Używamy "czystej" funkcji z aqm_v3_metrics
        sync_score = aqm_v3_metrics.calculate_institutional_sync_from_data(
            insider_df, 
            current_date
        )
        
        # 3. Oblicz Metrykę 2.2: retail_herding (ostatnie 7 dni)
        # Używamy "czystej" funkcji z aqm_v3_metrics
        herding_score = aqm_v3_metrics.calculate_retail_herding_from_data(
            news_df,
            current_date
        )

        if sync_score is None or herding_score is None:
            continue # Błąd obliczeń, przejdź do następnego dnia

        # 4. Zastosuj Warunki H2 (wg Specyfikacji Analitycznej)
        
        # Warunek 1: "Wysoki institutional_sync"
        is_insider_buying = sync_score > 0.5
        
        # Warunek 2: "Niski retail_herding"
        is_retail_panicking = herding_score < -0.2
        
        # Sygnał KUPNA = Warunek 1 AND Warunek 2
        if is_insider_buying and is_retail_panicking:
            
            # --- ZNALEZIONO SYGNAŁ H2 ---
            
            # 5. Pobierz Parametry Transakcji (z Dnia D i D+1)
            try:
                candle_D_plus_1 = daily_df.iloc[i + 1]
                
                entry_price = candle_D_plus_1['open']
                atr_value = candle_D['atr_14']
                
                # Walidacja danych (bardzo ważna)
                if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                    logger.warning(f"[Backtest H2] Pominięto sygnał dla {ticker} (Dzień {candle_D.name.date()}). Brak danych OPEN(D+1) lub ATR(D).")
                    continue
                
                # Używamy parametrów ze Specyfikacji H2
                take_profit = entry_price + (5.0 * atr_value)
                stop_loss = entry_price - (2.0 * atr_value)
                max_hold_days = 5
                
                setup_h2 = {
                    "ticker": ticker,
                    "setup_type": "AQM_V3_H2_CONTRARIAN_ENTANGLEMENT", 
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                }
                
                # 6. Przekaż do _resolve_trade (zapożyczonego z symulatora H1)
                # Przekazujemy pełny DataFrame i indeks dnia WEJŚCIA (D+1)
                trade = _resolve_trade(
                    daily_df, 
                    i + 1, # Indeks Dnia D+1 (start pętli w _resolve_trade)
                    setup_h2, 
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
                logger.error(f"[Backtest H2] Błąd podczas tworzenia setupu dla {ticker} (Dzień {candle_D.name.date()}): {e}", exc_info=True)
                session.rollback()


    if trades_found > 0:
        try:
            session.commit()
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji H2 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
