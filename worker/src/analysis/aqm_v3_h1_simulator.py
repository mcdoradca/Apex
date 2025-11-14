import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any

# Importujemy modele i funkcje pomocnicze
from .. import models
from . import aqm_v3_metrics # Nasz nowy plik z metrykami V3
from .utils import calculate_atr # ATR jest potrzebny do SL

logger = logging.getLogger(__name__)

# ==================================================================
# === LOGIKA EGZEKUCJI TRANSAKCJI (Przeniesiona z backtest_engine) ===
# === ZMODYFIKOWANA O LOGOWANIE METRYK ===
# ==================================================================

def _resolve_trade(historical_data: pd.DataFrame, entry_index: int, setup: Dict[str, Any], max_hold_days: int, year: str, direction: str) -> models.VirtualTrade | None:
    """
    "Spogląda w przyszłość" (w danych historycznych), aby zobaczyć, jak
    dana transakcja by się zakończyła.
    
    ZMIANA (Głębokie Logowanie): Ta funkcja pobiera teraz dodatkowe
    metryki z `setup` dict i zapisuje je w obiekcie VirtualTrade.
    """
    try:
        # Pobieramy parametry ze specyfikacji H1
        entry_price = setup['entry_price'] # OPEN(D+1)
        stop_loss = setup['stop_loss']     # OPEN(D+1) - (2 * ATR(D))
        take_profit = setup['take_profit'] # VWAP(D)
        
        close_price = entry_price # Domyślna cena zamknięcia
        status = 'CLOSED_EXPIRED' # Domyślny status
        
        # Znajdź indeks świecy D+1 (czyli 'entry_index' w pełnym DataFrame)
        # Pętla musi zacząć sprawdzać SL/TP od dnia D+1 (włącznie)
        
        # +1, ponieważ specyfikacja mówi o 5 dniach *po* wejściu (D+1 do D+5)
        # Dzień 1 = entry_index (D+1)
        # Dzień 5 = entry_index + 4 (D+5)
        for i in range(0, max_hold_days): 
            current_day_index = entry_index + i
            
            if current_day_index >= len(historical_data):
                # Transakcja doszła do końca danych historycznych
                candle = historical_data.iloc[-1]
                close_price = candle['close']
                status = 'CLOSED_EXPIRED'
                break
            
            candle = historical_data.iloc[current_day_index]
            day_low = candle['low']
            day_high = candle['high']

            if direction == 'LONG':
                # === Logika H1 (Mean Reversion) ===
                
                # Warunek 1: Czy SL został trafiony?
                # Sprawdzamy LOW dnia (nawet w dniu wejścia D+1)
                if day_low <= stop_loss:
                    close_price = stop_loss
                    status = 'CLOSED_SL'
                    break
                    
                # Warunek 2: Czy TP został trafiony?
                # Sprawdzamy HIGH dnia
                if day_high >= take_profit:
                    close_price = take_profit
                    status = 'CLOSED_TP'
                    break
            
            # (Pomijamy logikę SHORT, H1 jest tylko LONG)

        else:
            # === Warunek 3: Wyjście Czasowe (Max Hold) ===
            # Jeśli pętla zakończyła się normalnie (bez break),
            # zamykamy po cenie CLOSE dnia D+5.
            
            # Indeks D+5 to entry_index + max_hold_days - 1
            final_index = min(entry_index + max_hold_days - 1, len(historical_data) - 1)
            candle = historical_data.iloc[final_index]
            close_price = candle['close']
            status = 'CLOSED_EXPIRED'

        # Obliczanie P/L
        if entry_price == 0:
            p_l_percent = 0.0
        else:
            p_l_percent = ((close_price - entry_price) / entry_price) * 100
        
        
        # ==================================================================
        # === NOWA LOGIKA: Tworzenie obiektu VirtualTrade z pełnym logowaniem metryk ===
        # ==================================================================
        trade = models.VirtualTrade(
            ticker=setup['ticker'],
            status=status,
            setup_type=f"BACKTEST_{year}_{setup['setup_type']}",
            entry_price=float(entry_price),
            stop_loss=float(stop_loss),
            take_profit=float(take_profit),
            open_date=historical_data.index[entry_index].to_pydatetime(), # Data znalezienia setupu
            close_date=candle.name.to_pydatetime(), # Data zamknięcia
            close_price=float(close_price),
            final_profit_loss_percent=float(p_l_percent),
            
            # --- ZAPIS METRYK DO BAZY DANYCH ---
            # Wspólne
            metric_atr_14=setup.get('metric_atr_14'),
            
            # H1
            metric_time_dilation=setup.get('metric_time_dilation'),
            metric_price_gravity=setup.get('metric_price_gravity'),
            metric_td_percentile_90=setup.get('metric_td_percentile_90'),
            metric_pg_percentile_90=setup.get('metric_pg_percentile_90'),
            
            # H2
            metric_inst_sync=setup.get('metric_inst_sync'),
            metric_retail_herding=setup.get('metric_retail_herding'),
            
            # H3
            metric_aqm_score_h3=setup.get('metric_aqm_score_h3'),
            metric_aqm_percentile_95=setup.get('metric_aqm_percentile_95'),
            metric_J_norm=setup.get('metric_J_norm'),
            metric_nabla_sq_norm=setup.get('metric_nabla_sq_norm'),
            metric_m_sq_norm=setup.get('metric_m_sq_norm'),
            
            # H4
            metric_J=setup.get('metric_J'),
            metric_J_threshold_2sigma=setup.get('metric_J_threshold_2sigma')
        )
        # ==================================================================
        
        return trade

    except Exception as e:
        logger.error(f"[Backtest] Błąd podczas rozwiązywania transakcji dla {setup.get('ticker')}: {e}", exc_info=True)
        return None

# ==================================================================
# === KROK 19: Implementacja Pętli Symulacyjnej dla Hipotezy H1 ===
# === ZMODYFIKOWANA O LOGOWANIE METRYK ===
# ==================================================================

def _simulate_trades_h1(
    session: Session, 
    ticker: str, 
    historical_data: pd.DataFrame, 
    year: str
) -> int:
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEJ SPÓŁKI
    i szuka setupów zgodnych z Hipotezą H1 (Struktura Czasoprzestrzeni).
    
    Wymaga, aby 'historical_data' zawierało już wstępnie obliczone kolumny:
    - 'time_dilation'
    - 'price_gravity'
    - 'atr_14'
    - 'vwap'
    """
    trades_found = 0
    history_window = 100 # Wg specyfikacji (100-dniowy percentyl)
    
    # Zaczynamy od 101, aby mieć 100 dni historii + 1 bieżący
    # I dodatkowy 1 dzień na dane D+1 (OPEN)
    for i in range(history_window, len(historical_data) - 1): 
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        
        # 1. Stwórz widok 100 dni historii (kończący się na Dniu D)
        # `i` jest indeksem Dnia D
        df_view = historical_data.iloc[i - history_window : i + 1]
        
        candle_D = df_view.iloc[-1]
        
        # 2. Pobierz metryki dla Dnia D (już obliczone)
        current_price_gravity = candle_D['price_gravity']
        current_time_dilation = candle_D['time_dilation']
        
        if pd.isna(current_price_gravity) or pd.isna(current_time_dilation):
            continue
            
        # 3. Oblicz 90. percentyl dla 100-dniowej historii (wg specyfikacji)
        gravity_percentile_90 = df_view['price_gravity'].quantile(0.90)
        dilation_percentile_90 = df_view['time_dilation'].quantile(0.90)
        
        # 4. Zastosuj Warunki H1
        
        # Warunek 1: "Wysokie price_gravity"
        is_gravity_high = current_price_gravity > gravity_percentile_90
        
        # Warunek 2: "Rosnące time_dilation"
        is_dilation_high = current_time_dilation > dilation_percentile_90
        
        # Sygnał KUPNA = Warunek 1 AND Warunek 2
        if is_gravity_high and is_dilation_high:
            
            # --- ZNALEZIONO SYGNAŁ H1 ---
            
            # 5. Pobierz Parametry Transakcji (z Dnia D i D+1)
            try:
                candle_D_plus_1 = historical_data.iloc[i + 1]
                
                entry_price = candle_D_plus_1['open']
                take_profit = candle_D['vwap'] # VWAP(D)
                atr_value = candle_D['atr_14']
                
                # Walidacja danych (bardzo ważna)
                if pd.isna(entry_price) or pd.isna(take_profit) or pd.isna(atr_value) or atr_value == 0:
                    logger.warning(f"[Backtest H1] Pominięto sygnał dla {ticker} (Dzień {candle_D.name.date()}). Brak danych OPEN(D+1), VWAP(D) lub ATR(D).")
                    continue
                    
                stop_loss = entry_price - (2.0 * atr_value)
                max_hold_days = 5 # Wg specyfikacji
                
                # ==================================================================
                # === NOWA LOGIKA: Przygotowanie setupu z metrykami do logowania ===
                # ==================================================================
                setup_h1 = {
                    "ticker": ticker,
                    "setup_type": "AQM_V3_H1_GRAVITY_MEAN_REVERSION", 
                    "entry_price": entry_price,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    
                    # --- Dodatkowe metryki do logowania ---
                    "metric_atr_14": atr_value,
                    "metric_time_dilation": current_time_dilation,
                    "metric_price_gravity": current_price_gravity,
                    "metric_td_percentile_90": dilation_percentile_90,
                    "metric_pg_percentile_90": gravity_percentile_90,
                }
                # ==================================================================

                # 6. Przekaż do _resolve_trade
                # Przekazujemy pełny DataFrame i indeks dnia WEJŚCIA (D+1)
                trade = _resolve_trade(
                    historical_data, 
                    i + 1, # Indeks Dnia D+1 (start pętli w _resolve_trade)
                    setup_h1, 
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
                logger.error(f"[Backtest H1] Błąd podczas tworzenia setupu dla {ticker} (Dzień {candle_D.name.date()}): {e}", exc_info=True)
                session.rollback()


    if trades_found > 0:
        try:
            session.commit()
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji H1 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
