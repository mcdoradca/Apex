import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta

# Import klienta API i Modeli
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

# Import Narzędzi (Utils)
from .utils import (
    log_decision, 
    get_raw_data_with_cache, 
    standardize_df_columns, 
    calculate_atr, 
    send_telegram_alert,
    append_scan_log,
    update_scan_progress
)

# Import Silników Matematycznych (Fundamenty z Kroku 1)
from . import aqm_v3_metrics
from . import aqm_v4_logic
from .aqm_v3_h2_loader import load_h2_data_into_cache

logger = logging.getLogger(__name__)

# ==================================================================
# GŁÓWNY STEROWNIK SKANERA LIVE (PHASE 3)
# ==================================================================

def run_h3_live_scan(session: Session, candidates: list, api_client: AlphaVantageClient, parameters: dict = None):
    """
    Uruchamia Skaner Sygnałów H3/AQM na żywo.
    Analizuje listę kandydatów, oblicza metryki wektorowe i generuje sygnały.
    """
    # 1. Konfiguracja Parametrów (Sztywne, bez adaptacji)
    params = parameters or {}
    
    # Tryb strategii: H3 (Elite Sniper) lub AQM (Adaptive Quantum)
    strategy_mode = params.get('strategy_mode', 'H3')
    
    # Parametry H3
    h3_percentile = float(params.get('h3_percentile', 0.95))
    h3_m_sq_threshold = float(params.get('h3_m_sq_threshold', -0.5))
    h3_min_score = float(params.get('h3_min_score', 0.0))
    
    # Parametry AQM
    aqm_min_score = float(params.get('aqm_min_score', 0.8)) # Domyślnie 80/100
    aqm_comp_min = float(params.get('aqm_component_min', 0.5))
    
    # Zarządzanie ryzykiem (wspólne)
    tp_mult = float(params.get('h3_tp_multiplier', 5.0))
    sl_mult = float(params.get('h3_sl_multiplier', 2.0))
    max_hold_days = int(params.get('h3_max_hold', 5))
    
    # Logowanie startu
    start_msg = f"🔭 SNIPER: Start Skanowania. Tryb: {strategy_mode}. Kandydatów: {len(candidates)}"
    append_scan_log(session, start_msg)
    logger.info(start_msg)

    processed = 0
    signals_found = 0
    total = len(candidates)

    for ticker in candidates:
        processed += 1
        # Aktualizacja paska postępu w UI
        if processed % 5 == 0:
            update_scan_progress(session, processed, total)
        
        try:
            # 2. Pobieranie Danych (Live Cache)
            # Używamy cache 24h, ale dla skanera live dane muszą być 'dzisiejsze' (po zamknięciu) lub 'wczorajsze'
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12, outputsize='full')
            
            if not daily_raw:
                log_decision(session, ticker, "DATA_FETCH", "REJECTED", "Brak danych dziennych API")
                continue

            df = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            
            # Wymagane min. 200 świec do obliczeń (EMA 200, normalizacja 100)
            if len(df) < 200:
                log_decision(session, ticker, "DATA_SIZE", "REJECTED", f"Za krótka historia ({len(df)} < 200)")
                continue

            # Konwersja indeksu
            df.index = pd.to_datetime(df.index)
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            df.sort_index(inplace=True)
            
            # Oblicz ATR (potrzebne do SL/TP i logiki)
            # WAŻNE: To oblicza kolumnę 'atr_14' w df, ale silniki mogą zwracać nowe df!
            df['atr_14'] = calculate_atr(df)

            # === ŚCIEŻKA A: STRATEGIA H3 (ELITE SNIPER) ===
            if strategy_mode == 'H3':
                # Ładowanie danych Wymiaru 2 (Insider/News)
                # Używamy loadera z cache, aby nie katować API
                h2_data = load_h2_data_into_cache(ticker, api_client, session)
                insider_df = h2_data.get('insider_df')
                news_df = h2_data.get('news_df')

                # Przygotowanie kolumn do silnika wektorowego H3
                # (Te obliczenia są szybkie, robimy je "w locie" przed wektoryzacją)
                
                # 1. Price Gravity (Grawitacja)
                df['price_gravity'] = (df['high'] + df['low'] + df['close']) / 3 / df['close'] - 1
                
                # 2. Institutional Sync (Insiderzy) - mapowanie na dni
                df['institutional_sync'] = df.apply(
                    lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1
                ).fillna(0.0)
                
                # 3. Retail Herding (Newsy) - mapowanie na dni
                df['retail_herding'] = df.apply(
                    lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1
                ).fillna(0.0)
                
                # 4. Market Temperature (Zmienność)
                df['daily_returns'] = df['close'].pct_change().fillna(0)
                df['market_temperature'] = df['daily_returns'].rolling(window=30).std().fillna(0.01) # Unikamy div/0
                
                # 5. Information Entropy (Liczba newsów)
                if not news_df.empty:
                    # Uproszczona entropia: suma newsów z 10 dni
                    if news_df.index.tz is not None: news_df.index = news_df.index.tz_localize(None)
                    nc = news_df.groupby(news_df.index.date).size()
                    nc.index = pd.to_datetime(nc.index)
                    nc = nc.reindex(df.index, fill_value=0)
                    df['information_entropy'] = nc.rolling(window=10).sum().fillna(0)
                else:
                    df['information_entropy'] = 0.0
                
                # 6. Wolumen (Surowy - silnik go znormalizuje)
                # (df['volume'] już istnieje)

                # >>> URUCHOMIENIE SILNIKA WEKTOROWEGO H3 <<<
                df_calc = aqm_v3_metrics.calculate_aqm_h3_vectorized(df)
                
                # Pobranie ostatniego wiersza (Stan na dzisiaj/wczoraj)
                last_row = df_calc.iloc[-1]
                
                # Obliczenie Ranka (Percentyla) na bieżąco
                # (Silnik zwraca Score, ale Rank musimy ocenić względem historii)
                current_score = last_row['aqm_score_h3']
                history_scores = df_calc['aqm_score_h3'].tail(100) # Ostatnie 100 dni
                current_rank = (history_scores < current_score).mean() # Percentyl
                
                current_m_sq = last_row['m_sq_norm']
                
                # DECYZJA H3
                if current_rank > h3_percentile and current_m_sq < h3_m_sq_threshold and current_score > h3_min_score:
                    # SUKCES! Generujemy sygnał.
                    reason_msg = f"H3 HIT! Rank {current_rank:.2f} > {h3_percentile}, Mass {current_m_sq:.2f} < {h3_m_sq_threshold}"
                    log_decision(session, ticker, "H3_STRATEGY", "ACCEPTED", reason_msg)
                    
                    _create_or_update_signal(
                        session=session,
                        ticker=ticker,
                        strategy="H3",
                        price=last_row['close'],
                        atr=last_row['atr_14'],
                        tp_mult=tp_mult,
                        sl_mult=sl_mult,
                        max_hold=max_hold_days,
                        score=current_score, # Zapisujemy surowy wynik AQM Score
                        details=f"Rank: {current_rank:.2f} | M2: {current_m_sq:.2f} | J: {last_row['J_norm']:.2f}"
                    )
                    signals_found += 1
                else:
                    # PORAŻKA - Logujemy dlaczego
                    fail_reasons = []
                    if current_rank <= h3_percentile: fail_reasons.append(f"Rank {current_rank:.2f}<{h3_percentile}")
                    if current_m_sq >= h3_m_sq_threshold: fail_reasons.append(f"Mass {current_m_sq:.2f}>{h3_m_sq_threshold}")
                    if current_score <= h3_min_score: fail_reasons.append(f"Score {current_score:.2f}<{h3_min_score}")
                    
                    log_decision(session, ticker, "H3_STRATEGY", "REJECTED", ", ".join(fail_reasons))

            # === ŚCIEŻKA B: STRATEGIA AQM (ADAPTIVE QUANTUM V4) ===
            elif strategy_mode == 'AQM':
                # Pobieranie danych dodatkowych (Weekly, OBV)
                # Tu upraszczamy: resamplujemy Weekly z Daily, OBV liczymy sami (żeby było szybko)
                
                # Weekly (Resample)
                weekly_df = df.resample('W').agg({
                    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
                }).dropna()
                
                # OBV (Calculate)
                df['obv'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
                obv_df = df[['obv']].rename(columns={'obv': 'OBV'})
                
                # Makro (Mockup lub pobranie z cache - tutaj weźmiemy "bezpieczne" defaulty dla Live,
                # bo Faza 0 Makro Agent już powinna zablokować skanowanie, jeśli makro jest złe)
                macro_data = {
                    'inflation': 3.0, 'yield_10y': 4.0, 'qqq_df': pd.DataFrame() # Można rozbudować o pobranie QQQ
                }

                # >>> URUCHOMIENIE SILNIKA WEKTOROWEGO AQM <<<
                df_calc = aqm_v4_logic.calculate_aqm_full_vector(
                    daily_df=df,
                    weekly_df=weekly_df,
                    intraday_60m_df=pd.DataFrame(),
                    obv_df=obv_df,
                    macro_data=macro_data
                )
                
                if df_calc.empty:
                    log_decision(session, ticker, "AQM_CALC", "ERROR", "Błąd obliczeń silnika AQM")
                    continue

                last_row = df_calc.iloc[-1]
                
                # Metryki AQM
                total_score = last_row.get('aqm_score', 0.0) # Skala 0-1
                qps = last_row.get('qps', 0.0)
                vms = last_row.get('vms', 0.0)
                tcs = last_row.get('tcs', 0.0) # Temporal Coherence (Earnings penalty)
                
                # DECYZJA AQM
                # aqm_min_score np. 0.8 (czyli 80/100)
                # aqm_comp_min np. 0.5 (składniki muszą być przyzwoite)
                if total_score > aqm_min_score and qps > aqm_comp_min and vms > aqm_comp_min and tcs > 0.1:
                    reason_msg = f"AQM HIT! Score {total_score:.2f} > {aqm_min_score}"
                    log_decision(session, ticker, "AQM_STRATEGY", "ACCEPTED", reason_msg)
                    
                    # === FIX: Bezpieczne pobranie ATR (AQM używa 'atr', H3 'atr_14') ===
                    atr_val = last_row.get('atr', last_row.get('atr_14', 0.0))
                    
                    _create_or_update_signal(
                        session=session,
                        ticker=ticker,
                        strategy="AQM",
                        price=last_row['close'],
                        atr=atr_val, # Poprawione pobieranie ATR
                        tp_mult=tp_mult,
                        sl_mult=sl_mult,
                        max_hold=max_hold_days,
                        score=total_score * 100, # Konwersja na skalę 0-100 dla UI
                        details=f"QPS: {qps:.2f} | VMS: {vms:.2f} | TCS: {tcs:.2f}"
                    )
                    signals_found += 1
                else:
                    fail_reasons = []
                    if total_score <= aqm_min_score: fail_reasons.append(f"Score {total_score:.2f}<{aqm_min_score}")
                    if qps <= aqm_comp_min: fail_reasons.append(f"QPS {qps:.2f} słabe")
                    if tcs <= 0.1: fail_reasons.append("Earnings Risk")
                    
                    log_decision(session, ticker, "AQM_STRATEGY", "REJECTED", ", ".join(fail_reasons))

        except Exception as e:
            logger.error(f"Critical error scanning {ticker}: {e}", exc_info=True)
            log_decision(session, ticker, "CRITICAL", "ERROR", str(e))
            continue

    # Podsumowanie
    end_msg = f"🔭 SNIPER: Zakończono. Znaleziono {signals_found} sygnałów."
    append_scan_log(session, end_msg)
    logger.info(end_msg)

def _create_or_update_signal(session: Session, ticker: str, strategy: str, price: float, atr: float, tp_mult: float, sl_mult: float, max_hold: int, score: float, details: str):
    """
    Tworzy lub aktualizuje sygnał w bazie danych.
    Oblicza SL/TP na podstawie sztywnych mnożników ATR.
    
    CRITICAL FIX: Rozdzielono zapis sygnału (Commit) od logowania.
    Zapewnia to, że sygnał zostanie zapisany nawet jeśli logowanie/alerting zawiedzie.
    """
    try:
        # 1. Bezpieczne rzutowanie typów (NumPy protection)
        price_val = float(price) if not np.isnan(price) else 0.0
        atr_val = float(atr) if not np.isnan(atr) else 0.0
        score_val = float(score) if not np.isnan(score) else 0.0
        
        # Obliczenia cenowe
        # Zaokrąglamy do 2 miejsc
        entry_price = round(price_val, 2)
        atr_clean = max(0.01, atr_val) # Zabezpieczenie przed 0
        
        stop_loss = round(entry_price - (sl_mult * atr_clean), 2)
        take_profit = round(entry_price + (tp_mult * atr_clean), 2)
        
        risk = entry_price - stop_loss
        reward = take_profit - entry_price
        rr_ratio = round(reward / risk, 2) if risk > 0 else 0.0
        
        # Data wygaśnięcia (Hold Time)
        expiration_date = datetime.now(timezone.utc) + timedelta(days=max_hold)
        
        # Format notatki (Zgodny z UI parserem: SCORE: XX, STRATEGIA: YY)
        note_content = (
            f"STRATEGIA: {strategy}\n"
            f"SCORE: {int(score_val)}/100\n"
            f"DETALE: {details}\n"
            f"PARAMETRY: TP {tp_mult}xATR | SL {sl_mult}xATR\n"
            f"MAX HOLD: {max_hold} dni"
        )

        # 2. Sprawdź czy sygnał już istnieje (Active/Pending)
        existing = session.query(models.TradingSignal).filter(
            models.TradingSignal.ticker == ticker,
            models.TradingSignal.status.in_(['ACTIVE', 'PENDING'])
        ).first()

        operation_type = "UPDATE"
        
        if existing:
            # Aktualizacja
            existing.updated_at = datetime.now(timezone.utc)
            existing.entry_price = entry_price
            existing.stop_loss = stop_loss
            existing.take_profit = take_profit
            existing.risk_reward_ratio = rr_ratio
            existing.notes = note_content
            existing.expiration_date = expiration_date
            # Statusu nie zmieniamy, jeśli był ACTIVE to zostaje ACTIVE
        else:
            # Nowy sygnał
            operation_type = "INSERT"
            new_signal = models.TradingSignal(
                ticker=ticker,
                status='PENDING',
                generation_date=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                entry_price=entry_price,
                stop_loss=stop_loss,
                take_profit=take_profit,
                risk_reward_ratio=rr_ratio,
                notes=note_content,
                expiration_date=expiration_date,
                
                # Wypełniamy oczekiwania dla Re-check Agenta
                expected_profit_factor=3.5, # Cel
                expected_win_rate=60.0      # Cel
            )
            session.add(new_signal)

        # 3. KRYTYCZNY PUNKT: Commit SYGNAŁU przed logowaniem
        # To gwarantuje, że sygnał trafi do bazy, nawet jeśli system logowania/alertów zawiedzie.
        session.commit()
        
        # --- Sekcja Logowania i Alertów (Oddzielna obsługa błędów) ---
        try:
            if operation_type == "UPDATE":
                append_scan_log(session, f"🔄 Sygnał zaktualizowany: {ticker} ({strategy})")
            else:
                append_scan_log(session, f"🚀 NOWY SYGNAŁ: {ticker} ({strategy}) Score: {int(score_val)}")
                send_telegram_alert(f"🚀 SNIPER {strategy}: {ticker}\nCena: {entry_price}\nScore: {int(score_val)}\nRR: {rr_ratio}")
        except Exception as log_err:
            logger.error(f"Sygnał zapisany, ale błąd logowania/alertu dla {ticker}: {log_err}")
            # Nie robimy rollbacku tutaj, bo sygnał jest już bezpieczny!

    except Exception as e:
        logger.error(f"Błąd zapisu sygnału {ticker}: {e}")
        session.rollback()
