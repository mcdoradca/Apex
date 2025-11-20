import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

from .utils import (
    get_raw_data_with_cache,
    standardize_df_columns,
    calculate_atr,
    append_scan_log,
    update_scan_progress,
    send_telegram_alert,
    safe_float
)
# Korzystamy z centralnych metryk, aby zachować spójność z Backtestem
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands

logger = logging.getLogger(__name__)

# ============================================================================
# === STAŁE KONFIGURACYJNE H3 (LIVE) ===
# ============================================================================
DEFAULT_PERCENTILE = 0.95
DEFAULT_M_SQ_THRESHOLD = -0.5
DEFAULT_MIN_SCORE = 0.0
DEFAULT_TP_MULT = 5.0
DEFAULT_SL_MULT = 2.0
# Musimy mieć wystarczająco dużo historii, aby obliczyć percentyle i Z-Score
H3_CALC_WINDOW = 100 
REQUIRED_HISTORY_SIZE = 201 

def _is_setup_still_valid(entry: float, sl: float, tp: float, current_price: float) -> tuple[bool, str]:
    """
    Krytyczna logika "Strażnika Ważności".
    Sprawdza, czy setup nie jest już 'spalony' przez aktualną cenę rynkową.
    Zwraca (True/False, Powód).
    """
    if current_price is None or entry == 0:
        return False, "Brak aktualnej ceny lub błędna cena wejścia"

    # 1. Sprawdź czy nie uderzyliśmy już w Stop Loss (Scenariusz Gap Down / Krach)
    # Zakładamy kierunek LONG
    if current_price <= sl:
        return False, f"SPALONY: Cena ({current_price:.2f}) przebiła już Stop Loss ({sl:.2f})."

    # 2. Sprawdź czy nie uderzyliśmy już w Take Profit (Ruch się odbył beze mnie)
    if current_price >= tp:
        return False, f"ZA PÓŹNO: Cena ({current_price:.2f}) osiągnęła już Take Profit ({tp:.2f})."

    # 3. Sprawdź czy Risk/Reward jest nadal opłacalny (Nie goń ceny)
    # Jeśli cena jest znacznie wyżej niż wejście, potencjalny zysk maleje, a ryzyko rośnie.
    # Akceptujemy lekkie odchylenie, ale bez przesady.
    
    potential_profit = tp - current_price
    potential_loss = current_price - sl
    
    if potential_loss == 0: return False, "Błąd matematyczny (SL na cenie)."
    
    current_rr = potential_profit / potential_loss
    
    # Wymagamy, aby w momencie wejścia RR był nadal przynajmniej 1.5 (nawet jeśli oryginalnie był 2.5)
    if current_rr < 1.5:
        return False, f"NIEOPŁACALNY: Cena uciekła ({current_price:.2f}). Aktualny R:R to tylko {current_rr:.2f}."

    return True, "OK"

def run_h3_live_scan(session: Session, candidates: List[str], api_client: AlphaVantageClient, parameters: Dict[str, Any] = None):
    """
    Główna pętla Fazy 3 (Live Sniper).
    Analizuje listę kandydatów pod kątem setupów H3, używając danych w czasie rzeczywistym.
    """
    logger.info("Uruchamianie Fazy 3: H3 LIVE SNIPER (Zlogiką Ważności)...")
    append_scan_log(session, "Faza 3 (H3): Rozpoczynanie analizy kwantowej...")
    
    # Konfiguracja parametrów
    params = parameters or {}
    h3_percentile = float(params.get('h3_percentile', DEFAULT_PERCENTILE))
    h3_m_sq_threshold = float(params.get('h3_m_sq_threshold', DEFAULT_M_SQ_THRESHOLD))
    h3_min_score = float(params.get('h3_min_score', DEFAULT_MIN_SCORE))
    h3_tp_mult = float(params.get('h3_tp_multiplier', DEFAULT_TP_MULT))
    h3_sl_mult = float(params.get('h3_sl_multiplier', DEFAULT_SL_MULT))
    
    signals_generated = 0
    total_candidates = len(candidates)

    for i, ticker in enumerate(candidates):
        if i % 5 == 0: update_scan_progress(session, i, total_candidates)
        
        try:
            # 1. Pobieranie Danych (Dimension 1, 3, 7)
            # Używamy outputsize='full', aby mieć historię do Z-Score'ów
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12, outputsize='full')
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12, outputsize='full')
            
            if not daily_raw or not daily_adj_raw:
                continue
            
            # 2. Pobieranie Danych (Dimension 3 - BBANDS) - NAPRAWIONE
            bbands_raw = get_raw_data_with_cache(session, api_client, ticker, 'BBANDS', 'get_bollinger_bands', expiry_hours=24, interval='daily', time_period=20)
            bbands_df = _parse_bbands(bbands_raw) # Teraz faktycznie to parsujemy i używamy

            # 3. Pobieranie Danych (Dimension 2 - H2 News/Insider)
            h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
            insider_df = h2_data.get('insider_df')
            news_df = h2_data.get('news_df')

            # 4. Przetwarzanie DataFrame
            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj.index = pd.to_datetime(daily_adj.index)

            # Sprawdzenie długości historii
            if len(daily_adj) < REQUIRED_HISTORY_SIZE:
                continue

            # Łączenie danych (Price Gravity wymaga VWAP Proxy z OHLCV)
            daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            # Wymiar 1.2: Price Gravity
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
            df['close'] = df[close_col] # Ujednolicenie

            # 5. Obliczenia Metryk (Wykorzystujemy logikę z aqm_v3_metrics.py ale zwektoryzowaną dla szybkości)
            # Tutaj musimy odtworzyć logikę "pre_calculate", ale poprawnie
            # Aby zachować spójność z Backtestem, używamy tej samej logiki co w backtest_engine._pre_calculate_metrics
            
            # a) Metryki H2 (J components)
            df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
            df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
            
            # b) Metryki H3/H4 (Market Temp, Nabla)
            df['daily_returns'] = df['close'].pct_change()
            df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
            df['nabla_sq'] = df['price_gravity']

            # c) Metryki m^2 (Attention Density)
            df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
            df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
            df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
            df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)

            if not news_df.empty:
                news_counts_daily = news_df.groupby(news_df.index.date).size()
                news_counts_daily.index = pd.to_datetime(news_counts_daily.index)
                news_counts_daily = news_counts_daily.reindex(df.index, fill_value=0)
                df['information_entropy'] = news_counts_daily.rolling(window=10).sum()
                df['news_mean_200d'] = df['information_entropy'].rolling(window=200).mean()
                df['news_std_200d'] = df['information_entropy'].rolling(window=200).std()
                df['normalized_news'] = ((df['information_entropy'] - df['news_mean_200d']) / df['news_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
            else:
                df['information_entropy'] = 0.0
                df['normalized_news'] = 0.0
            
            df['m_sq'] = df['normalized_volume'] + df['normalized_news']

            # d) Obliczenie J (Siła Napędowa)
            S = df['information_entropy']
            Q = df['retail_herding']
            T = df['market_temperature']
            mu = df['institutional_sync']
            J = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
            df['J'] = J.fillna(S + (mu * 1.0))

            # 6. Normalizacja i AQM Score (Tożsame z Backtestem)
            j_mean = df['J'].rolling(window=H3_CALC_WINDOW).mean()
            j_std = df['J'].rolling(window=H3_CALC_WINDOW).std(ddof=1)
            j_norm = ((df['J'] - j_mean) / j_std).fillna(0)
            
            nabla_mean = df['nabla_sq'].rolling(window=H3_CALC_WINDOW).mean()
            nabla_std = df['nabla_sq'].rolling(window=H3_CALC_WINDOW).std(ddof=1)
            nabla_norm = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
            
            m_mean = df['m_sq'].rolling(window=H3_CALC_WINDOW).mean()
            m_std = df['m_sq'].rolling(window=H3_CALC_WINDOW).std(ddof=1)
            m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)
            
            # Formuła Pola Kwantowego
            aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
            
            # Dynamiczny próg percentylowy
            threshold_series = aqm_score_series.rolling(window=H3_CALC_WINDOW).quantile(h3_percentile)

            # 7. Analiza Ostatniej Świecy (Czy mamy sygnał?)
            # Bierzemy ostatni ZAMKNIĘTY dzień (iloc[-1])
            last_candle = df.iloc[-1]
            current_aqm = aqm_score_series.iloc[-1]
            current_thresh = threshold_series.iloc[-1]
            current_m = m_norm.iloc[-1]

            # WARUNEK WEJŚCIA
            if (current_aqm > current_thresh) and \
               (current_m < h3_m_sq_threshold) and \
               (current_aqm > h3_min_score):
               
                # 8. Konstrukcja Setupu (Ceny)
                atr = last_candle['atr_14']
                ref_price = last_candle['close'] # Cena zamknięcia dnia sygnałowego
                
                # Setup teoretyczny
                take_profit = ref_price + (h3_tp_mult * atr)
                stop_loss = ref_price - (h3_sl_mult * atr)
                entry_price = ref_price # Wstępnie zakładamy wejście po cenie sygnału (lub Open następnego dnia, tu upraszczamy do ref)

                # 9. WALIDACJA LIVE (Strażnik Ważności)
                # Pobieramy AKTUALNĄ cenę live (REALTIME), aby sprawdzić czy setup nie jest spalony
                current_live_quote = api_client.get_global_quote(ticker)
                current_live_price = safe_float(current_live_quote.get('05. price')) if current_live_quote else None
                
                validation_status = True
                validation_reason = "Setup świeży (Post-Market/Pre-Market)"
                
                if current_live_price:
                    validation_status, validation_reason = _is_setup_still_valid(entry_price, stop_loss, take_profit, current_live_price)

                if not validation_status:
                    logger.info(f"H3 Setup dla {ticker} odrzucony przez Strażnika: {validation_reason}")
                    continue # Pomiń ten ticker, nie zapisuj sygnału
                
                # 10. Zapis do Bazy (Jeśli setup jest ważny)
                logger.info(f"H3 SIGNAL FOUND: {ticker} (AQM: {current_aqm:.2f}). Status: {validation_reason}")
                
                # Sprawdź czy już nie mamy aktywnego sygnału dla tego tickera
                existing = session.query(models.TradingSignal).filter(
                    models.TradingSignal.ticker == ticker,
                    models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
                ).first()
                
                if not existing:
                    new_signal = models.TradingSignal(
                        ticker=ticker,
                        status='PENDING',
                        generation_date=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc), # Kluczowe do śledzenia ważności
                        signal_candle_timestamp=last_candle.name, # Data świecy dziennej, która dała sygnał
                        
                        entry_price=float(entry_price),
                        stop_loss=float(stop_loss),
                        take_profit=float(take_profit),
                        
                        # Obliczamy strefę wejścia (np. +/- 0.5 ATR od ceny sygnału)
                        entry_zone_top=float(ref_price + (0.5 * atr)),
                        entry_zone_bottom=float(ref_price - (0.5 * atr)),
                        
                        risk_reward_ratio=float(h3_tp_mult/h3_sl_mult),
                        
                        notes=f"AQM H3 Live. Score:{current_aqm:.2f}. LivePrice:{current_live_price if current_live_price else 'N/A'}. J:{last_candle['J']:.2f}, N:{last_candle['nabla_sq']:.2f}, M:{last_candle['m_sq']:.2f}"
                    )
                    session.add(new_signal)
                    session.commit()
                    signals_generated += 1
                    
                    # 11. Alert na Telegram
                    msg = (f"⚛️ H3 QUANTUM SIGNAL: {ticker}\n"
                           f"Cena: {ref_price:.2f} (Live: {current_live_price if current_live_price else '---'})\n"
                           f"TP: {take_profit:.2f} | SL: {stop_loss:.2f}\n"
                           f"Status: {validation_reason}")
                    send_telegram_alert(msg)

        except Exception as e:
            logger.error(f"Błąd H3 Live dla {ticker}: {e}", exc_info=True)
            session.rollback()
            continue

    append_scan_log(session, f"Faza 3 (H3 Live) zakończona. Wygenerowano {signals_generated} ważnych sygnałów.")
    logger.info(f"Faza 3 zakończona. Sygnałów: {signals_generated}")
