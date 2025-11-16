import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any

# Importujemy modele i funkcje pomocnicze
from .. import models
# ==================================================================
# === REFAKTORYZACJA (WYDAJNOŚĆ): Usunięto import aqm_v3_metrics ===
# Obliczenia są teraz wykonywane w backtest_engine
# ==================================================================
# from . import aqm_v3_metrics 
# ==================================================================
# Importujemy funkcję egzekucji transakcji z Krok 19a
from .aqm_v3_h1_simulator import _resolve_trade

logger = logging.getLogger(__name__)

# ==================================================================
# === KROK 21a: Implementacja Pętli Symulacyjnej dla Hipotezy H2 ===
# === REFAKTORYZACJA (WYDAJNOŚĆ): Ta funkcja odczytuje teraz wstępnie obliczone metryki ===
# === ZMODYFIKOWANA O BEZPIECZNĄ KONWERSJĘ TYPÓW (float()) ===
# ==================================================================

def _simulate_trades_h2(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], # Oczekujemy słownika z cache
    year: str
) -> int:
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEGO SPÓŁKI
    i szuka setupów zgodnych z Hipotezą H2 (Splątanie Kwantowe).
    
    Wymaga, aby 'historical_data' (słownik) zawierał 'daily' DataFrame
    z wstępnie obliczonymi kolumnami:
    - 'atr_14'
    - 'institutional_sync'
    - 'retail_herding'
    """
    trades_found = 0
    
    daily_df = historical_data.get("daily")
    
    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Nie potrzebujemy już insider_df ani news_df ===
    # ==================================================================
    if daily_df is None:
        logger.warning(f"[Backtest V3][H2] Pominięto {ticker}, brak kompletnych danych (Daily).")
        return 0
    # ==================================================================

    # ==================================================================
    # === REFAKTORYZACJA (WYDAJNOŚĆ): Ustawienie bufora ===
    # Musimy poczekać, aż metryki (np. 200-dniowe) będą stabilne
    # 201 dni to bezpieczny bufor dla wszystkich metryk H1-H4
    # ==================================================================
    history_buffer = 201 
    
    # === NOWA MODYFIKACJA (Logowanie H2) ===
    # Dodajemy licznik, aby nie spamować logów
    log_counter = 0
    # ==================================================================
    
    # Zaczynamy od bufora, aby mieć dane D-1, ale musimy też zapewnić, że mamy dane D+1
    for i in range(history_buffer, len(daily_df) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        # 1. Pobierz dane dla Dnia D
        candle_D = daily_df.iloc[i]
        
        # ==================================================================
        # === REFAKTORYZACJA (WYDAJNOŚĆ): Odczyt zamiast obliczeń ===
        # ==================================================================
        # 2. Oblicz Metrykę 2.1: institutional_sync (odczyt z kolumny)
        sync_score = candle_D['institutional_sync']
        
        # 3. Oblicz Metrykę 2.2: retail_herding (odczyt z kolumny)
        herding_score = candle_D['retail_herding']
        # ==================================================================

        if pd.isna(sync_score) or pd.isna(herding_score):
            continue # Błąd obliczeń (NaN z pre-processingu), przejdź do następnego dnia
        
        # ==================================================================
        # === NOWA MODYFIKACJA (Logowanie H2) ===
        # Logujemy odczytane wartości raz na jakiś czas
        log_counter += 1
        if log_counter % 500 == 0: # Loguj co 500 dni (ok. 2 lata handlowe), aby nie zalać logów
            # Sprawdzamy tylko, czy wartości w ogóle są RÓŻNE OD ZERA
            if sync_score != 0.0 or herding_score != 0.0:
                logger.info(f"[Backtest H2 Debug] Ticker: {ticker} | Data: {candle_D.name.date()} | SyncScore: {sync_score:.4f} | HerdingScore: {herding_score:.4f}")
        # ==================================================================

        # 4. Zastosuj Warunki H2 (wg Specyfikacji Analitycznej)
        
        # Warunek 1: "Wysoki institutional_sync"
        # ORYGINAŁ: is_insider_buying = sync_score > 0.5
        is_insider_buying = sync_score > 0.3 # ZMIANA: Złagodzenie progu z 0.5 do 0.3
        
        # Warunek 2: "Niski retail_herding"
        # ORYGINAŁ: is_retail_panicking = herding_score < -0.2
        is_retail_panicking = herding_score < -0.1 # ZMIANA: Złagodzenie progu z -0.2 do -0.1
        
        # Sygnał KUPNA = Warunek 1 AND Warunek 2
        if is_insider_buying and is_retail_panicking:
            
            # --- ZNALEZIONO SYGNAŁ H2 ---
            # ==================================================================
            # === NOWA MODYFIKACJA (Logowanie H2) ===
            # Logujemy moment znalezienia sygnału
            logger.warning(f"[Backtest H2 SYGNAŁ!] Ticker: {ticker} | Data: {candle_D.name.date()} | Sync: {sync_score:.4f} (> 0.3) | Herding: {herding_score:.4f} (< -0.1)")
            # ==================================================================
            
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
                
                # ==================================================================
                # === NOWA LOGIKA: Przygotowanie setupu z metrykami do logowania ===
                # === POPRAWKA: Konwertujemy wszystko na float() ===
                # ==================================================================
                setup_h2 = {
                    "ticker": ticker,
                    "setup_type": "AQM_V3_H2_CONTRARIAN_ENTANGLEMENT", 
                    "entry_price": float(entry_price),
                    "stop_loss": float(stop_loss),
                    "take_profit": float(take_profit),
                    
                    # --- Dodatkowe metryki do logowania (BEZPIECZNA KONWERSJA) ---
                    "metric_atr_14": float(atr_value),
                    "metric_inst_sync": float(sync_score),
                    "metric_retail_herding": float(herding_score),
                }
                # ==================================================================
                
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
                # ==================================================================
                # === POPRAWKA BŁĘDU SYNTAX ERROR ===
                # (Zmieniono .True na =True)
                # ==================================================================
                logger.error(f"[Backtest H2] Błąd podczas tworzenia setupu dla {ticker} (Dzień {candle_D.name.date()}): {e}", exc_info=True)
                session.rollback()


    if trades_found > 0:
        try:
            session.commit()
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji H2 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
