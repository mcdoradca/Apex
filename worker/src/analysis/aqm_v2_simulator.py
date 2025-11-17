import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

# Importujemy modele i funkcje pomocnicze
from .. import models
from .utils import calculate_ema, calculate_atr
# Importujemy istniejący, generyczny resolver transakcji z AQM V3
from .aqm_v3_h1_simulator import _resolve_trade

logger = logging.getLogger(__name__)

# ==================================================================
# === FUNKCJE PARSOWANIA DANYCH (dla `backtest_engine`) ===
# === KOREKTA BŁĘDU LOGICZNEGO (Nieprawidłowe parsowanie) ===
# ==================================================================

def _parse_indicator_data(raw_data: Dict[str, Any], key_name: str, value_name: str) -> pd.DataFrame:
    """
    Generyczna funkcja do parsowania odpowiedzi JSON dla wskaźników jednowartościowych
    (np. RSI, OBV, AD, ATR) ORAZ wskaźników ekonomicznych (np. INFLATION).
    """
    try:
        # === KOREKTA: `raw_data` to pełna odpowiedź JSON. Musimy najpierw wyodrębnić `data`. ===
        data_payload = raw_data.get(key_name) # Pobieramy ładunek danych (może to być dict lub list)
        
        if not data_payload:
            # logger.warning(f"[Backtest V2] Parser: Nie znaleziono klucza '{key_name}' w surowych danych.")
            return pd.DataFrame(columns=[value_name]).set_index(pd.to_datetime([]))

        processed_data = []

        if isinstance(data_payload, dict):
            # Przypadek 1: Wskaźnik Techniczny (np. {"2025-11-17": {"RSI": "45.12"}, ...})
            for date_str, values in data_payload.items():
                try:
                    value = values.get(value_name)
                    if value is not None:
                        processed_data.append({
                            'date': pd.to_datetime(date_str),
                            value_name: pd.to_numeric(value, errors='coerce')
                        })
                except (ValueError, TypeError):
                    continue
        
        elif isinstance(data_payload, list):
            # Przypadek 2: Wskaźnik Ekonomiczny (np. [{"date": "...", "value": "..."}, ...])
            for item in data_payload:
                try:
                    date_str = item.get('date')
                    value = item.get('value')
                    if date_str is not None and value is not None:
                        processed_data.append({
                            'date': pd.to_datetime(date_str),
                            value_name: pd.to_numeric(value, errors='coerce')
                        })
                except (ValueError, TypeError):
                    continue
        
        else:
            # Nieznany format
            logger.error(f"[Backtest V2] Nieznany format danych w parserze dla klucza {key_name}: {type(data_payload)}")
            return pd.DataFrame(columns=[value_name]).set_index(pd.to_datetime([]))

        if not processed_data:
             # logger.warning(f"[Backtest V2] Parser: Nie znaleziono przetworzonych danych dla klucza '{key_name}'.")
             return pd.DataFrame(columns=[value_name]).set_index(pd.to_datetime([]))

        df = pd.DataFrame(processed_data)
        df.set_index('date', inplace=True)
        df = df[~df.index.duplicated(keep='first')] # Na wypadek duplikatów dat
        df.sort_index(inplace=True)
        return df
        # === KONIEC KOREKTY ===
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych wskaźnika ({key_name}): {e}", exc_info=True)
        return pd.DataFrame(columns=[value_name]).set_index(pd.to_datetime([]))

def _parse_macd_data(raw_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Specjalistyczna funkcja do parsowania odpowiedzi JSON dla wskaźnika MACD (3 linie).
    """
    try:
        # === KOREKTA: `raw_data` to pełna odpowiedź JSON. Musimy najpierw wyodrębnić `data`. ===
        data_payload = raw_data.get('Technical Analysis: MACD', {})
        if not data_payload:
            # logger.warning(f"[Backtest V2] Parser: Nie znaleziono klucza 'Technical Analysis: MACD' w surowych danych.")
            return pd.DataFrame(columns=['MACD', 'MACD_Hist', 'MACD_Signal']).set_index(pd.to_datetime([]))

        # === KOREKTA: Ta sama poprawka co w _parse_indicator_data (Przypadek 1: dict) ===
        processed_data = []
        for date_str, values in data_payload.items():
            try:
                processed_data.append({
                    'date': pd.to_datetime(date_str),
                    'MACD': pd.to_numeric(values.get('MACD'), errors='coerce'),
                    'MACD_Hist': pd.to_numeric(values.get('MACD_Hist'), errors='coerce'),
                    'MACD_Signal': pd.to_numeric(values.get('MACD_Signal'), errors='coerce')
                })
            except (ValueError, TypeError):
                continue
        
        if not processed_data:
            # logger.warning(f"[Backtest V2] Parser: Nie znaleziono przetworzonych danych dla MACD.")
            return pd.DataFrame(columns=['MACD', 'MACD_Hist', 'MACD_Signal']).set_index(pd.to_datetime([]))

        df = pd.DataFrame(processed_data)
        df.set_index('date', inplace=True)
        df = df[~df.index.duplicated(keep='first')]
        df.sort_index(inplace=True)
        return df
        # === KONIEC KOREKTY ===
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych MACD: {e}", exc_info=True)
        return pd.DataFrame(columns=['MACD', 'MACD_Hist', 'MACD_Signal']).set_index(pd.to_datetime([]))

# ==================================================================
# === FUNKCJE OBLICZENIOWE AQM V2 (Wektorowe) ===
# (Logika obliczeniowa pozostaje bez zmian)
# ==================================================================

def _calculate_qps_vectorized(daily_df: pd.DataFrame, weekly_df: pd.DataFrame, rsi_df: pd.DataFrame, macd_df: pd.DataFrame) -> pd.DataFrame:
    """
    (Warstwa 1) Oblicza Quantum Prime Score (QPS) metodą wektorową.
    """
    df = daily_df.copy()
    
    # 1. Analiza Daily (60% wagi QPS)
    df['qps_trend_d'] = np.where(
        (df['close'] > df['ema_50']) & (df['ema_50'] > df['ema_200']), 
        1, 0
    )
    # Łączymy dane RSI (jako 'asof' na wypadek brakujących dat)
    df = pd.merge_asof(df, rsi_df[['RSI']], left_index=True, right_index=True, direction='backward')
    df['qps_momentum_d'] = np.where(df['RSI'] > 55, 1, 0)
    
    # Łączymy dane MACD
    df = pd.merge_asof(df, macd_df[['MACD', 'MACD_Signal']], left_index=True, right_index=True, direction='backward')
    df['qps_macd_d'] = np.where(df['MACD'] > df['MACD_Signal'], 1, 0)

    # 2. Analiza Weekly (40% wagi QPS)
    weekly_df['qps_trend_w'] = np.where(
        (weekly_df['close'] > weekly_df['ema_20']) & (weekly_df['ema_20'] > weekly_df['ema_50']), 
        1, 0
    )
    # Łączymy dane Weekly z Daily (jako 'asof')
    df = pd.merge_asof(df, weekly_df[['qps_trend_w']], left_index=True, right_index=True, direction='backward')
    df['qps_trend_w'] = df['qps_trend_w'].fillna(0) # Wypełnij NaN na początku

    # 3. Finalny QPS
    daily_score = (df['qps_trend_d'] + df['qps_momentum_d'] + df['qps_macd_d']) / 3.0
    df['QPS'] = (daily_score * 0.6) + (df['qps_trend_w'] * 0.4)
    
    # Czyszczenie kolumn pomocniczych
    df.drop(columns=['qps_trend_d', 'qps_momentum_d', 'qps_macd_d', 'RSI', 'MACD', 'MACD_Signal', 'qps_trend_w'], inplace=True, errors='ignore')
    
    return df

def _calculate_vms_vectorized(daily_df: pd.DataFrame, obv_df: pd.DataFrame, ad_df: pd.DataFrame) -> pd.DataFrame:
    """
    (Warstwa 3) Oblicza Volume/Microstructure Score (VMS) metodą wektorową.
    """
    df = daily_df.copy()

    # Łączymy OBV i AD (jako 'asof' na wypadek brakujących dat)
    df = pd.merge_asof(df, obv_df[['OBV']], left_index=True, right_index=True, direction='backward')
    df = pd.merge_asof(df, ad_df[['AD']], left_index=True, right_index=True, direction='backward')

    # 1. Trend OBV (40% wagi VMS)
    df['obv_ema_20'] = calculate_ema(df['OBV'], 20)
    df['vms_obv'] = np.where(df['OBV'] > df['obv_ema_20'], 1, 0)

    # 2. Trend A/D (30% wagi VMS)
    df['ad_ema_20'] = calculate_ema(df['AD'], 20)
    df['vms_ad'] = np.where(df['AD'] > df['ad_ema_20'], 1, 0)

    # 3. Anomalia Wolumenu (30% wagi VMS)
    # Ignorujemy dni z wolumenem = 0 przy obliczaniu średniej
    df['avg_vol_20'] = df['volume'].replace(0, np.nan).rolling(window=20, min_periods=5).mean().ffill()
    df['vms_vol'] = np.where(df['volume'] > (df['avg_vol_20'] * 1.5), 1, 0)

    # 4. Finalny VMS
    df['VMS'] = (df['vms_obv'] * 0.4) + (df['vms_ad'] * 0.3) + (df['vms_vol'] * 0.3)
    
    # Czyszczenie kolumn pomocniczych
    df.drop(columns=['OBV', 'AD', 'obv_ema_20', 'vms_obv', 'ad_ema_20', 'vms_ad', 'avg_vol_20', 'vms_vol'], inplace=True, errors='ignore')
    
    return df

def _check_tcs(current_date: datetime.date, earnings_df: pd.DataFrame) -> bool:
    """
    (Warstwa 4) Sprawdza Temporal Coherence Score (TCS).
    Zwraca True, jeśli jesteśmy ZBYT BLISKO wyników (i sygnał należy pominąć).
    Zwraca False, jeśli data jest bezpieczna.
    """
    if earnings_df.empty:
        return False # Brak danych o wynikach = data jest bezpieczna

    # Sprawdź, czy data D (dzień sygnału) jest blisko daty raportu
    for report_date in earnings_df.index.date:
        if abs((current_date - report_date).days) <= 5: # 5 dni bufora (przed lub po)
            return True # Jesteśmy w buforze -> Pomiń sygnał
    
    return False # Data jest bezpieczna

# ==================================================================
# === GŁÓWNY SYMULATOR AQM V2 ===
# (Logika pozostaje bez zmian)
# ==================================================================

def _simulate_trades_aqm_v2(
    session: Session, 
    ticker: str, 
    data: Dict[str, pd.DataFrame], 
    year: str
) -> int:
    """
    Główna pętla symulacyjna dla strategii AQM V2.
    Iteruje dzień po dniu, oblicza 4 warstwy i generuje transakcje.
    """
    trades_found = 0
    
    # 1. Rozpakowanie danych (przygotowanych przez backtest_engine)
    daily_df = data.get("daily_v2")
    weekly_df = data.get("weekly_v2")
    ras_df = data.get("ras_df")
    earnings_df = data.get("earnings_v2")
    atr_df = data.get("atr_v2")
    
    if daily_df is None or weekly_df is None or ras_df is None or earnings_df is None or atr_df is None:
        logger.warning(f"[Backtest V2] Pominięto {ticker}, brak kompletnych danych (Daily, Weekly, RAS, Earnings, ATR).")
        return 0

    # 2. Wstępne obliczenia wektorowe (QPS i VMS)
    try:
        daily_df = _calculate_qps_vectorized(
            daily_df, 
            weekly_df, 
            data.get("rsi_v2"), 
            data.get("macd_v2")
        )
        daily_df = _calculate_vms_vectorized(
            daily_df, 
            data.get("obv_v2"), 
            data.get("ad_v2")
        )
    except Exception as e:
        logger.error(f"[Backtest V2] Błąd podczas obliczeń wektorowych dla {ticker}: {e}", exc_info=True)
        return 0 # Pomiń ticker, jeśli obliczenia wstępne zawiodą

    # Bezpieczny bufor, aby wszystkie wskaźniki (np. EMA 200) zdążyły się ustabilizować
    history_buffer = 201
    if len(daily_df) < history_buffer + 1:
        logger.warning(f"[Backtest V2] Za mało danych dla {ticker} ({len(daily_df)}) do rozpoczęcia symulacji.")
        return 0

    # 3. Główna pętla symulacyjna
    for i in range(history_buffer, len(daily_df) - 1): # -1, aby mieć dane D+1
        
        # --- Dzień D (Skanowanie na CLOSE) ---
        candle_D = daily_df.iloc[i]
        current_date = candle_D.name
        
        # --- Warstwa 2: Filtr Reżimu (RAS) ---
        ras_row = ras_df.asof(current_date)
        if ras_row is None or pd.isna(ras_row['RAS']):
            continue # Brak danych o reżimie na ten dzień
        ras_score = ras_row['RAS']
        
        if ras_score <= 0.5: # Filtr RISK_OFF
            continue

        # --- Warstwa 4: Filtr Czasowy (TCS) ---
        tcs_score = 1.0 # Domyślnie 1.0 (brak kary)
        if _check_tcs(current_date.date(), earnings_df):
            tcs_score = 0.1 # Jesteśmy w buforze wyników (kara)
        
        if tcs_score <= 0.5: # Filtr bliskości wyników
            continue
            
        # --- Obliczenie Finalnego Scoru ---
        qps_score = candle_D.get('QPS', 0)
        vms_score = candle_D.get('VMS', 0)
        
        if pd.isna(qps_score) or pd.isna(vms_score):
            continue

        final_score = (qps_score * 0.4) + (ras_score * 0.2) + (vms_score * 0.3) + (tcs_score * 0.1)
        
        # --- Warstwy 3 i 5: Filtry Wejścia ---
        if final_score > 0.85 and vms_score > 0.6:
            
            # --- ZNALEZIONO SYGNAŁ AQM V2 ---
            
            try:
                candle_D_plus_1 = daily_df.iloc[i + 1]
                entry_price = candle_D_plus_1['open']
                
                # Pobierz ATR(D)
                atr_row = atr_df.asof(current_date)
                if atr_row is None or pd.isna(atr_row['ATR']) or atr_row['ATR'] == 0:
                    logger.warning(f"[Backtest V2] Pominięto sygnał dla {ticker} (Dzień {current_date.date()}). Brak danych ATR.")
                    continue
                
                atr_value = atr_row['ATR']
                
                if pd.isna(entry_price):
                    continue
                    
                # Logika Wyjścia (zgodnie ze specyfikacją)
                stop_loss = entry_price - (atr_value * 2.0)
                take_profit = entry_price + (atr_value * 4.0)
                max_hold_days = 7 # 7 dni hold time
                
                setup_v2 = {
                    "ticker": ticker,
                    "setup_type": f"BACKTEST_{year}_AQM_V2", 
                    "entry_price": float(entry_price),
                    "stop_loss": float(stop_loss),
                    "take_profit": float(take_profit),
                    
                    # Logowanie metryk V2 do bazy danych (do analizy w Raporcie Agenta)
                    # Używamy tych samych nazw kolumn, co w V3, aby pasowały do tabeli
                    "metric_atr_14": float(atr_value),
                    "metric_time_dilation": float(qps_score), # Używamy pól V3 jako proxy
                    "metric_price_gravity": float(vms_score), # Używamy pól V3 jako proxy
                    "metric_J_norm": float(ras_score),      # Używamy pól V3 jako proxy
                    "metric_m_sq_norm": float(final_score), # Używamy pól V3 jako proxy
                }

                # 6. Przekaż do generycznego resolvera transakcji (zapożyczonego z V3)
                trade = _resolve_trade(
                    daily_df, 
                    i + 1, # Indeks Dnia D+1
                    setup_v2, 
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
                logger.error(f"[Backtest V2] Błąd podczas tworzenia setupu dla {ticker} (Dzień {current_date.date()}): {e}", exc_info=True)
                session.rollback()

    if trades_found > 0:
        try:
            session.commit()
            logger.info(f"[Backtest V2] Pomyślnie zapisano {trades_found} transakcji AQM V2 dla {ticker} (Rok: {year}).")
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji AQM V2 dla {ticker}: {e}")
            session.rollback()
        
    return trades_found
