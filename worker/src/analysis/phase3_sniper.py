import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

# Importy narzędziowe (te same co w backtest)
from .utils import (
    get_raw_data_with_cache,
    standardize_df_columns,
    calculate_atr,
    append_scan_log,
    update_scan_progress,
    send_telegram_alert
)
# Importy AQM
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands

logger = logging.getLogger(__name__)

# ============================================================================
# === STAŁE KONFIGURACYJNE H3 (LIVE) - Muszą być 1:1 z Backtest Defaults ===
# ============================================================================
H3_PERCENTILE = 0.95
H3_M_SQ_THRESHOLD = -0.5
H3_TP_MULT = 5.0
H3_SL_MULT = 2.0
H3_WINDOW = 100 # Okno dla percentyla i statystyk
H3_HISTORY_BUFFER = 201 # Wymagana historia (dla Z-Score 200d)

def _pre_calculate_metrics_live(daily_df: pd.DataFrame, insider_df: pd.DataFrame, news_df: pd.DataFrame) -> pd.DataFrame:
    """
    Kopia logiki z backtest_engine.py (1:1) dostosowana do kontekstu Live.
    Oblicza J, nabla_sq, m_sq dla całego DataFrame.
    """
    df = daily_df.copy()
    
    # 1. Timezone normalization
    if insider_df.index.tz is not None: insider_df = insider_df.tz_convert(None)
    if news_df.index.tz is not None: news_df = news_df.tz_convert(None)

    # 2. H2 Metrics (Institutional Sync, Retail Herding)
    try:
        df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
    except: df['institutional_sync'] = 0.0

    try:
        df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
    except: df['retail_herding'] = 0.0

    # 3. Market Temp (T) & Nabla (Price Gravity is already in df)
    df['daily_returns'] = df['close'].pct_change()
    df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
    # df['price_gravity'] powinno być już w daily_df z loadera
    df['nabla_sq'] = df.get('price_gravity', 0.0) 

    # 4. Volume & News for m_sq
    df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
    df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
    df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
    # Z-Score Volume
    df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)

    if not news_df.empty:
        news_counts_daily = news_df.groupby(news_df.index.date).size()
        news_counts_daily.index = pd.to_datetime(news_counts_daily.index)
        news_counts_daily = news_counts_daily.reindex(df.index, fill_value=0)
        
        df['information_entropy'] = news_counts_daily.rolling(window=10).sum()
        df['news_mean_200d'] = df['information_entropy'].rolling(window=200).mean()
        df['news_std_200d'] = df['information_entropy'].rolling(window=200).std()
        # Z-Score News
        df['normalized_news'] = ((df['information_entropy'] - df['news_mean_200d']) / df['news_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
    else:
        df['information_entropy'] = 0.0
        df['normalized_news'] = 0.0

    df['m_sq'] = df['normalized_volume'] + df['normalized_news']

    # 5. J Calculation
    S = df['information_entropy']
    Q = df['retail_herding']
    T = df['market_temperature']
    mu = df['institutional_sync']
    
    # J formula
    J = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
    df['J'] = J.fillna(S + (mu * 1.0))

    return df

def run_h3_live_scan(session: Session, candidates: List[str], api_client: AlphaVantageClient):
    """
    Główna funkcja Fazy 3 (H3 Live).
    Przetwarza kandydatów z Fazy 1 pod kątem setupów H3.
    """
    logger.info("Uruchamianie Fazy 3: H3 LIVE ENGINE...")
    append_scan_log(session, "Faza 3 (H3): Rozpoczynanie analizy kwantowej kandydatów...")
    
    signals_generated = 0
    total_candidates = len(candidates)

    for i, ticker in enumerate(candidates):
        if i % 10 == 0:
            update_scan_progress(session, i, total_candidates)
        
        try:
            # 1. POBIERANIE DANYCH (FRESH - 12h expiry for prices)
            # Wymuszamy świeże ceny (12h), ale News/Insider mogą być z cache (7 dni default)
            
            # A. Ceny Dzienne (OHLCV)
            daily_raw = get_raw_data_with_cache(
                session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', 
                expiry_hours=12, outputsize='full'
            )
            if not daily_raw: continue

            # B. Ceny Adjusted (dla Time Dilation)
            daily_adj_raw = get_raw_data_with_cache(
                session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', 
                expiry_hours=12, outputsize='full'
            )
            if not daily_adj_raw: continue
            
            # C. BBANDS (dla spójności, choć H3 bezpośrednio ich nie używa w core logic, 
            # ale backtest je ładuje, więc my też)
            bbands_raw = get_raw_data_with_cache(
                session, api_client, ticker, 'BBANDS', 'get_bollinger_bands', 
                expiry_hours=24, interval='daily', time_period=20
            )

            # D. Dane H2 (News/Insider) - tu używamy standardowego loadera z cachem
            # (Loader sam dba o parsowanie)
            h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
            
            # 2. PRZETWARZANIE DANYCH (Standaryzacja i Merge)
            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj.index = pd.to_datetime(daily_adj.index)
            
            # Sprawdź długość historii
            if len(daily_adj) < H3_HISTORY_BUFFER + 1:
                # Za mało danych do wyliczenia metryk 200-dniowych
                continue

            # Oblicz VWAP Proxy i Price Gravity (tak jak w backteście)
            daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            
            # Merge
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
            
            # Time Dilation (uproszczone dla Live - bez SPY, lub zakładamy SPY=1 dla normalizacji wewn.)
            # W backteście używamy SPY. Tutaj, dla uproszczenia i szybkości, użyjemy zmienności własnej
            # lub (lepiej) - jeśli chcemy 1:1, powinniśmy załadować SPY. 
            # Na razie zostawimy time_dilation = rolling_std (bez mianownika SPY), co jest akceptowalnym proxy
            # dla selekcji wewnątrz jednego dnia, LUB (bezpieczniej) ustawimy na 0, bo H3 core logic
            # używa głównie J, Nabla, m_sq.
            df['time_dilation'] = df['close'].pct_change().rolling(20).std() # Proxy

            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.fillna(0, inplace=True)

            # 3. OBLICZANIE METRYK H3 (J, Nabla, m_sq)
            # Używamy funkcji pomocniczej zdefiniowanej wyżej
            df = _pre_calculate_metrics_live(df, h2_data['insider_df'], h2_data['news_df'])
            
            # 4. NORMALIZACJA I AQM SCORE (Rolling Window)
            # To jest kluczowe - musimy policzyć percentyle na historii
            
            # J Norm
            j_mean = df['J'].rolling(window=H3_WINDOW).mean()
            j_std = df['J'].rolling(window=H3_WINDOW).std(ddof=1)
            j_norm = ((df['J'] - j_mean) / j_std).fillna(0)
            
            # Nabla Norm
            nabla_mean = df['nabla_sq'].rolling(window=H3_WINDOW).mean()
            nabla_std = df['nabla_sq'].rolling(window=H3_WINDOW).std(ddof=1)
            nabla_norm = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
            
            # m Norm
            m_mean = df['m_sq'].rolling(window=H3_WINDOW).mean()
            m_std = df['m_sq'].rolling(window=H3_WINDOW).std(ddof=1)
            m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)
            
            # AQM Score
            aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
            
            # Próg (Percentyl)
            threshold_series = aqm_score_series.rolling(window=H3_WINDOW).quantile(H3_PERCENTILE)

            # 5. SPRAWDZENIE SYGNAŁU (OSTATNIA ŚWIECA)
            # Interesuje nas tylko ostatni *zamknięty* dzień (indeks -1)
            last_idx = -1
            
            current_aqm = aqm_score_series.iloc[last_idx]
            current_thresh = threshold_series.iloc[last_idx]
            current_m = m_norm.iloc[last_idx]
            
            # Data sygnału (data ostatniej świecy)
            signal_date = df.index[last_idx]
            
            # WARUNKI H3
            if (current_aqm > current_thresh) and (current_m < H3_M_SQ_THRESHOLD):
                
                # Mamy sygnał!
                # Cena wejścia = OPEN następnego dnia (czyli jutro rano). 
                # Jako że nie znamy jutrzejszego OPEN, w systemie sygnałów
                # ustawiamy to jako 'market order' lub dzisiejsze CLOSE jako reference.
                # Lepiej: Ustawiamy entry_price = None (Market Open) lub estymację.
                # W backteście: entry = Open(D+1).
                # Tutaj: Zapisujemy sygnał, trader wchodzi na otwarciu.
                
                # Oblicz SL/TP na podstawie ATR z dzisiaj
                atr = df['atr_14'].iloc[last_idx]
                ref_price = df['close'].iloc[last_idx] # Cena odniesienia (Close D)
                
                # Parametry
                # TP = Entry + 5*ATR. Entry nieznane, więc używamy Ref (Close) jako przybliżenia dla UI
                # W praktyce egzekucja będzie na Open.
                take_profit = ref_price + (H3_TP_MULT * atr)
                stop_loss = ref_price - (H3_SL_MULT * atr)
                
                log_msg = f"H3 SIGNAL FOUND: {ticker} (AQM: {current_aqm:.2f} > {current_thresh:.2f})."
                logger.info(log_msg)
                
                # 6. ZAPIS DO BAZY
                # Sprawdź czy sygnał już istnieje dla tej daty
                existing = session.query(models.TradingSignal).filter(
                    models.TradingSignal.ticker == ticker,
                    models.TradingSignal.status.in_(['ACTIVE', 'PENDING']),
                    # Sprawdzamy czy sygnał nie jest "stary" (z dziś)
                    models.TradingSignal.generation_date >= datetime.now(timezone.utc) - timedelta(hours=20)
                ).first()
                
                if not existing:
                    new_signal = models.TradingSignal(
                        ticker=ticker,
                        status='PENDING',
                        generation_date=datetime.now(timezone.utc),
                        entry_price=ref_price, # Przybliżona (do weryfikacji na Open)
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        risk_reward_ratio=H3_TP_MULT/H3_SL_MULT, # 2.5
                        entry_zone_top=ref_price + (0.5 * atr), # Opcjonalne strefy
                        entry_zone_bottom=ref_price - (0.5 * atr),
                        notes=f"AQM H3 Live Setup. Score: {current_aqm:.2f}. J:{df['J'].iloc[-1]:.2f}, N:{df['nabla_sq'].iloc[-1]:.2f}, M:{df['m_sq'].iloc[-1]:.2f}"
                    )
                    session.add(new_signal)
                    session.commit()
                    signals_generated += 1
                    
                    # Alert Telegram
                    alert_msg = f"⚛️ H3 QUANTUM SIGNAL: {ticker}\nAQM Score: {current_aqm:.2f}\nTP: {take_profit:.2f} | SL: {stop_loss:.2f}"
                    send_telegram_alert(alert_msg)

        except Exception as e:
            logger.error(f"Błąd H3 Live dla {ticker}: {e}", exc_info=True)
            continue

    append_scan_log(session, f"Faza 3 (H3 Live) zakończona. Wygenerowano {signals_generated} sygnałów.")
    logger.info(f"Faza 3 zakończona. Sygnałów: {signals_generated}")
