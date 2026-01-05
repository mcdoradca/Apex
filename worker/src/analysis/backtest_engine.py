import logging
import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
import traceback

# Importy narzędziowe
from .utils import (
    get_raw_data_with_cache, 
    standardize_df_columns, 
    calculate_atr, 
    append_scan_log, 
    update_scan_progress,
    _resolve_trade,
    log_decision,
    update_system_control
)

# Importy analityczne (H2/H3)
from .aqm_v3_h2_loader import load_h2_data_into_cache
from . import aqm_v3_metrics

# Importy analityczne (AQM V4)
from . import aqm_v4_logic

# === NOWOŚĆ: Import SDAR ===
from .phase_sdar import SDARAnalyzer

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

logger = logging.getLogger(__name__)

# Słowa kluczowe BioX
BIOTECH_KEYWORDS = [
    'Biotechnology', 'Pharmaceutical', 'Health Care', 'Life Sciences', 
    'Medical', 'Therapeutics', 'Biosciences', 'Oncology', 'Genomics',
    'Drug', 'Bio'
]

# === KLASA POMOCNICZA: WEHIKUŁ CZASU DLA SDAR (NAPRAWIONA) ===
class TimeTravelSDARAnalyzer(SDARAnalyzer):
    """
    Specjalna wersja analyzera SDAR dla Backtestu.
    Nadpisuje pobieranie danych, aby 'cofnąć się w czasie'.
    """
    def __init__(self, session, api_client, target_date: datetime):
        super().__init__(session, api_client)
        self.target_date = target_date
        # Ustawiamy koniec dnia badanej daty
        self.cutoff_time = target_date.replace(hour=23, minute=59, second=59)

    def _get_news_data(self, ticker: str) -> list:
        try:
            # Okno: 48h wstecz od momentu badania
            time_from_str = (self.cutoff_time - timedelta(days=2)).strftime('%Y%m%dT%H%M')
            time_to_str = self.cutoff_time.strftime('%Y%m%dT%H%M')

            # Wywołanie klienta z parametrami historycznymi (sort='LATEST' w oknie)
            resp = self.client.get_news_sentiment(
                ticker, limit=50, time_from=time_from_str, time_to=time_to_str, sort='LATEST'
            )
            if resp and 'feed' in resp:
                return resp['feed']
        except Exception:
            pass
        return []

    # === FIX: ZMIANA NAZWY METODY NA _get_virtual_candles ABY NADPISAĆ ORYGINAŁ ===
    # Wcześniej nazywała się _get_intraday_data, przez co SDAR używał metody bazowej (Live 5min)
    def _get_virtual_candles(self, ticker: str):
        """
        Pobiera ceny historyczne, ucina je na dacie badania I AGREGUJE DO 4H.
        To kluczowe dla szybkości i poprawności SDAR w backteście.
        """
        try:
            # 1. Pobieramy dane godzinowe (Alpha Vantage ma dłuższą historię dla 60min niż 5min)
            # W backteście historycznym 60min to kompromis dla szybkości.
            raw_data = get_raw_data_with_cache(
                self.session, self.client, ticker, 
                'INTRADAY_60', 
                lambda t: self.client.get_intraday(t, interval='60min', outputsize='full'),
                expiry_hours=24 # Cache na 24h wystarczy
            )
            
            ts_key = 'Time Series (60min)'
            if not raw_data or ts_key not in raw_data: 
                return None
            
            df = pd.DataFrame.from_dict(raw_data[ts_key], orient='index')
            df = standardize_df_columns(df)
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)
            
            cols = ['open', 'high', 'low', 'close', 'volume']
            for col in cols: df[col] = pd.to_numeric(df[col], errors='coerce')

            # 2. Odcięcie przyszłości (Time Travel)
            if df.index.tz is None:
                cutoff_naive = self.cutoff_time.replace(tzinfo=None)
                df = df[df.index <= cutoff_naive]
            else:
                cutoff_aware = self.cutoff_time.replace(tzinfo=df.index.tz)
                df = df[df.index <= cutoff_aware]

            if len(df) < 20: return None # Zbyt mało danych do analizy

            # 3. Agregacja do 4H (Wymagane przez logikę SDAR)
            # Używamy '4h' (małe h) aby uniknąć błędów pandas
            df_virtual = df.resample('4h').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

            return df_virtual
        except Exception as e:
            # logger.error(f"TimeTravel Data Error: {e}")
            return None


# === NARZĘDZIA POMOCNICZE DLA MAKRO ===
def _parse_macro_to_series(raw_json: dict) -> pd.Series:
    """Konwertuje surowy JSON z Alpha Vantage (data list) na Pandas Series z indeksem czasowym."""
    try:
        if not raw_json or 'data' not in raw_json:
            return pd.Series(dtype=float)
        
        data_list = raw_json['data']
        df = pd.DataFrame(data_list)
        
        # Konwersja kolumn
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        
        # Ustawienie indeksu i sortowanie (najstarsze pierwsze, żeby .asof() działało poprawnie)
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
        
        return df['value']
    except Exception as e:
        logger.error(f"Błąd parsowania danych makro do serii: {e}")
        return pd.Series(dtype=float)

def _calculate_time_dilation_series(ticker_df: pd.DataFrame, benchmark_df: pd.DataFrame, window: int = 20) -> pd.Series:
    try:
        if not isinstance(ticker_df.index, pd.DatetimeIndex): ticker_df.index = pd.to_datetime(ticker_df.index)
        if not isinstance(benchmark_df.index, pd.DatetimeIndex): benchmark_df.index = pd.to_datetime(benchmark_df.index)
        
        bench_aligned = benchmark_df['close'].reindex(ticker_df.index, method='ffill').fillna(0)
        
        ticker_returns = ticker_df['close'].pct_change()
        bench_returns = bench_aligned.pct_change().fillna(0)

        ticker_std = ticker_returns.rolling(window=window).std()
        bench_std = bench_returns.rolling(window=window).std()
        
        time_dilation = ticker_std / bench_std.replace(0, np.nan)
        return time_dilation.fillna(0)
    except Exception:
        return pd.Series(0, index=ticker_df.index)

def run_historical_backtest(session: Session, api_client, year: str, parameters: dict = None):
    strategy_mode = 'H3' 
    if parameters:
        if parameters.get('strategy_mode') == 'AQM':
            strategy_mode = 'AQM'
        elif parameters.get('strategy_mode') == 'BIOX':
            strategy_mode = 'BIOX'
        elif parameters.get('strategy_mode') == 'SDAR': # Dodano obsługę parametru SDAR
            strategy_mode = 'SDAR'
        
    start_msg = f"[Backtest] Start analizy historycznej {year} (Strategia: {strategy_mode})..."
    logger.info(start_msg)
    append_scan_log(session, start_msg)
    
    return _run_historical_backtest_unified(session, api_client, year, parameters, strategy_mode)

def _run_historical_backtest_unified(session: Session, api_client, year: str, parameters: dict = None, strategy_mode: str = 'H3'):
    try:
        # Czyścimy stare wyniki backtestu (opcjonalnie, zależy od preferencji)
        session.execute(text("DELETE FROM virtual_trades WHERE setup_type LIKE 'BACKTEST_%'"))
        session.commit()

        # 1. SELEKCJA UNIWERSUM
        tickers = []
        # Próba pobrania z różnych źródeł (F1, FX, Portfel)
        for table in ["phase1_candidates", "phasex_candidates", "portfolio_holdings"]:
            try:
                rows = session.execute(text(f"SELECT ticker FROM {table}")).fetchall()
                tickers += [r[0] for r in rows]
            except Exception: pass

        tickers = list(set(tickers))
        
        # Fallback - jeśli pusto, bierzemy próbkę z bazy companies
        if not tickers:
            append_scan_log(session, "⚠️ Brak kandydatów w F1/FX. Pobieram próbkę z tabeli companies.")
            tickers = [r[0] for r in session.execute(text("SELECT ticker FROM companies WHERE industry != 'N/A' LIMIT 880")).fetchall()]
        
        # === FILTRACJA BENCHMARKU (QQQ i SPY) ===
        # Wykluczamy ticker QQQ z handlu, bo to nasz benchmark
        tickers = [t for t in tickers if t not in ['QQQ', 'SPY', 'IWM', 'TQQQ', 'SQQQ']]
        
        logger.info(f"[Backtest] Wybrano {len(tickers)} tickerów do analizy.")
        append_scan_log(session, f"BACKTEST: Analiza {len(tickers)} spółek...")

        # Mapa sektorów
        company_sectors = {}
        try:
            rows = session.execute(text("SELECT ticker, sector, industry FROM companies")).fetchall()
            for r in rows:
                company_sectors[r[0]] = (r[1] or '', r[2] or '')
        except Exception as e:
            logger.warning(f"Błąd pobierania sektorów: {e}")
        
        params = parameters or {}
        tp_mult = float(params.get('h3_tp_multiplier', 5.0))
        sl_mult = float(params.get('h3_sl_multiplier', 2.0))
        max_hold = int(params.get('h3_max_hold', 5))
        
        setup_name_base = f"{strategy_mode}_BACKTEST"
        setup_name_suffix = str(params.get('setup_name', ''))
        if setup_name_suffix: setup_name_base += f"_{setup_name_suffix}"

        start_date_ts = pd.Timestamp(f"{year}-01-01").tz_localize(None)
        end_date_ts = pd.Timestamp(f"{year}-12-31").tz_localize(None)

        # === A. DANE BENCHMARKOWE (NASDAQ / QQQ) ===
        append_scan_log(session, "BACKTEST: Pobieranie danych benchmarku (QQQ)...")
        qqq_raw = get_raw_data_with_cache(session, api_client, 'QQQ', 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
        qqq_df = pd.DataFrame()
        if qqq_raw:
            qqq_df = standardize_df_columns(pd.DataFrame.from_dict(qqq_raw.get('Time Series (Daily)', {}), orient='index'))
            qqq_df.index = pd.to_datetime(qqq_df.index).tz_localize(None)
            qqq_df.sort_index(inplace=True)
        else:
            append_scan_log(session, "⚠️ OSTRZEŻENIE: Nie udało się pobrać danych QQQ. Analiza relatywna może być błędna.")

        # === B. KONTEKST MAKRO (Time-Travel Fix) ===
        # Pobieramy PEŁNĄ historię, a nie tylko najnowszą wartość
        append_scan_log(session, "BACKTEST: Pobieranie historycznych danych makro (Inflacja, Yields)...")
        
        macro_data = {
            'qqq_df': qqq_df, # Używamy QQQ zamiast SPY
            'inflation_series': pd.Series(dtype=float),
            'yield_series': pd.Series(dtype=float),
            'fed_rate_series': pd.Series(dtype=float)
        }

        # Pobieranie Inflacji
        inf_raw = get_raw_data_with_cache(session, api_client, 'INFLATION', 'INFLATION', 'get_inflation_rate')
        macro_data['inflation_series'] = _parse_macro_to_series(inf_raw)
        
        # Pobieranie Rentowności 10Y
        yield_raw = get_raw_data_with_cache(session, api_client, 'TREASURY_YIELD', 'TREASURY_YIELD', 'get_treasury_yield', interval='monthly')
        macro_data['yield_series'] = _parse_macro_to_series(yield_raw)
        
        # Pobieranie Stóp Procentowych
        fed_raw = get_raw_data_with_cache(session, api_client, 'FEDERAL_FUNDS_RATE', 'FEDERAL_FUNDS_RATE', 'get_fed_funds_rate', interval='monthly')
        macro_data['fed_rate_series'] = _parse_macro_to_series(fed_raw)

        if macro_data['inflation_series'].empty:
            append_scan_log(session, "⚠️ Brak danych inflacji. AQM RAS może być niedokładny.")

        total_tickers = len(tickers)
        processed_count = 0
        trades_generated = 0
        
        # Inicjalizacja paska postępu
        update_scan_progress(session, 0, total_tickers)
        
        for ticker in tickers:
            try:
                # Logowanie postępu co 5 sztuk w bazie
                if processed_count % 5 == 0:
                    update_scan_progress(session, processed_count, total_tickers)
                    
                # Pobieramy dane dzienne (niezależnie od strategii, potrzebne do symulacji transakcji)
                daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', outputsize='full')
                daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', outputsize='full')
                
                if not daily_raw:
                    processed_count += 1
                    continue

                ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
                ohlcv.index = pd.to_datetime(ohlcv.index).tz_localize(None)
                
                adj = pd.DataFrame()
                if daily_adj_raw:
                    adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
                    adj.index = pd.to_datetime(adj.index).tz_localize(None)
                
                # Scalanie danych (Adj Close + Raw OHLC)
                if not adj.empty:
                    df = adj.join(ohlcv[['open', 'high', 'low', 'close']], rsuffix='_raw')
                    trade_open_col = 'open_raw' if 'open_raw' in df.columns else 'open'
                    trade_high_col = 'high_raw' if 'high_raw' in df.columns else 'high'
                    trade_low_col = 'low_raw' if 'low_raw' in df.columns else 'low'
                    trade_close_col = 'close_raw' if 'close_raw' in df.columns else 'close'
                else:
                    df = ohlcv
                    trade_open_col, trade_high_col, trade_low_col, trade_close_col = 'open', 'high', 'low', 'close'

                df.sort_index(inplace=True)
                
                # Filtr na rok backtestu
                if df.empty or df.index[-1] < start_date_ts or df.index[0] > end_date_ts:
                    processed_count += 1
                    continue 
                
                df['atr_14'] = calculate_atr(df).ffill().fillna(0)

                signal_df = pd.DataFrame()

                # === ŚCIEŻKA A: STRATEGIA H3 (ELITE SNIPER) ===
                if strategy_mode == 'H3':
                    if len(df) < 201: 
                        processed_count += 1; continue
                    
                    h2_data = load_h2_data_into_cache(ticker, api_client, session)
                    insider_df = h2_data.get('insider_df')
                    news_df = h2_data.get('news_df')
                    
                    # Obliczenia metryk wstepnych (pre-vectorization)
                    df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1).fillna(0.0)
                    df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1).fillna(0.0)
                    
                    df['price_gravity'] = (df['high'] + df['low'] + df['close']) / 3 / df['close'] - 1
                    df['time_dilation'] = _calculate_time_dilation_series(df, qqq_df) # QQQ jako benchmark
                    
                    df['daily_returns'] = df['close'].pct_change().fillna(0)
                    df['market_temperature'] = df['daily_returns'].rolling(window=30).std().fillna(0.01)
                    
                    if not news_df.empty:
                        if news_df.index.tz is not None: news_df.index = news_df.index.tz_localize(None)
                        nc = news_df.groupby(news_df.index.date).size()
                        nc.index = pd.to_datetime(nc.index)
                        nc = nc.reindex(df.index, fill_value=0)
                        df['information_entropy'] = nc.rolling(window=10).sum().fillna(0)
                    else:
                        df['information_entropy'] = 0.0
                    
                    # >>> URUCHOMIENIE SILNIKA WEKTOROWEGO H3 <<<
                    df = aqm_v3_metrics.calculate_aqm_h3_vectorized(df)
                    
                    # Obliczanie Rank (Percentyl) na bieżąco w oknie
                    df['aqm_rank'] = df['aqm_score_h3'].rolling(window=100, min_periods=20).rank(pct=True).fillna(0)
                    
                    h3_p = float(parameters.get('h3_percentile', 0.95))
                    h3_m = float(parameters.get('h3_m_sq_threshold', -0.5))
                    h3_min = float(parameters.get('h3_min_score', 0.0))
                    
                    df['is_signal'] = (
                        (df['aqm_rank'] > h3_p) & 
                        (df['m_sq_norm'] < h3_m) & 
                        (df['aqm_score_h3'] > h3_min)
                    )
                    signal_df = df

                # === ŚCIEŻKA B: STRATEGIA AQM (ADAPTIVE QUANTUM V4) ===
                elif strategy_mode == 'AQM':
                    if len(df) < 201: 
                        processed_count += 1; continue
                        
                    w_raw = get_raw_data_with_cache(session, api_client, ticker, 'WEEKLY_ADJUSTED', 'get_weekly_adjusted')
                    weekly_df = pd.DataFrame()
                    if w_raw: 
                        weekly_df = standardize_df_columns(pd.DataFrame.from_dict(w_raw.get('Weekly Adjusted Time Series', {}), orient='index'))
                        weekly_df.index = pd.to_datetime(weekly_df.index).tz_localize(None)
                    
                    obv_raw = get_raw_data_with_cache(session, api_client, ticker, 'OBV', 'get_obv')
                    obv_df = pd.DataFrame()
                    if obv_raw: 
                        obv_df = pd.DataFrame.from_dict(obv_raw.get('Technical Analysis: OBV', {}), orient='index')
                        obv_df.index = pd.to_datetime(obv_df.index).tz_localize(None)
                        obv_df.rename(columns={'OBV': 'OBV'}, inplace=True)

                    # Obliczamy AQM z nową obsługą danych makro
                    # >>> URUCHOMIENIE SILNIKA WEKTOROWEGO AQM <<<
                    aqm_metrics_df = aqm_v4_logic.calculate_aqm_full_vector(
                        daily_df=df,
                        weekly_df=weekly_df,
                        intraday_60m_df=pd.DataFrame(), 
                        obv_df=obv_df,
                        macro_data=macro_data, # Przekazujemy serie czasowe!
                        earnings_days_to=None
                    )
                    
                    if not aqm_metrics_df.empty:
                        df = df.join(aqm_metrics_df[['aqm_score', 'qps', 'ras', 'vms', 'tcs']], rsuffix='_dupl')
                        
                        min_score = float(parameters.get('aqm_min_score', 0.8))
                        comp_min = float(parameters.get('aqm_component_min', 0.5))
                        
                        # AQM Signal Logic
                        df['is_signal'] = (
                            (df['aqm_score'] > min_score) &
                            (df['qps'] > comp_min) &
                            (df['vms'] > comp_min) &
                            (df['tcs'] > 0.1) # TCS musi być pozytywny (brak earnings)
                        )
                        signal_df = df
                    else:
                        signal_df = pd.DataFrame()

                # === ŚCIEŻKA C: STRATEGIA BIOX (PUMP HUNTER) ===
                elif strategy_mode == 'BIOX':
                    sec, ind = company_sectors.get(ticker, ('',''))
                    is_biotech = any(k in sec or k in ind for k in BIOTECH_KEYWORDS)
                    
                    if not is_biotech:
                        processed_count += 1; continue 

                    df['prev_close'] = df['close'].shift(1)
                    df['intraday_change'] = (df['high'] - df['open']) / df['open']
                    df['session_change'] = (df['close'] - df['prev_close']) / df['prev_close']
                    
                    BIO_MIN_PRICE = 0.50
                    BIO_MAX_PRICE = 4.00 
                    BIO_PUMP_THRESHOLD = 0.20

                    df['is_signal'] = (
                        ((df['intraday_change'] >= BIO_PUMP_THRESHOLD) | (df['session_change'] >= BIO_PUMP_THRESHOLD)) &
                        (df['close'] >= BIO_MIN_PRICE) & 
                        (df['close'] <= BIO_MAX_PRICE)
                    )
                    
                    df['aqm_score_h3'] = df[['intraday_change', 'session_change']].max(axis=1) * 100
                    signal_df = df
                    
                # === ŚCIEŻKA D: STRATEGIA SDAR (NAPRAWIONA) ===
                elif strategy_mode == 'SDAR':
                    # Ograniczamy zakres analizy do wybranego roku
                    df_sdar = df[(df.index >= start_date_ts) & (df.index <= end_date_ts)].copy()
                    df_sdar['is_signal'] = False
                    df_sdar['aqm_score_h3'] = 0.0 
                    
                    # Optymalizacja: Sprawdzamy co 5 dni, żeby backtest nie trwał latami
                    check_dates = df_sdar.index[::5] 
                    
                    signals_count_local = 0
                    
                    for date_idx in check_dates:
                        # Tworzymy analyzer dla konkretnego dnia
                        # UWAGA: Teraz TimeTravelSDARAnalyzer ma poprawną metodę _get_virtual_candles
                        # więc nadpisze pobieranie danych i użyje historii godzinowej (agregowanej do 4h)
                        analyzer = TimeTravelSDARAnalyzer(session, api_client, target_date=date_idx)
                        
                        # Uruchamiamy analizę
                        result = analyzer.analyze_ticker(ticker)
                        
                        if result:
                            # Warunek sygnału: Score > 70 (zgodnie z logiką Live)
                            if result.total_anomaly_score > 70:
                                df_sdar.at[date_idx, 'aqm_score_h3'] = result.total_anomaly_score
                                df_sdar.at[date_idx, 'is_signal'] = True
                                signals_count_local += 1
                                log_decision(session, ticker, "BT_SDAR", "SIGNAL", f"Score: {result.total_anomaly_score:.1f}")
                    
                    signal_df = df_sdar

                # === SYMULACJA TRANSAKCJI (WSPÓLNA LOGIKA) ===
                if not signal_df.empty and 'is_signal' in signal_df.columns:
                    sim_start_idx = signal_df.index.searchsorted(start_date_ts)
                    i = sim_start_idx
                    
                    while i < len(signal_df) - 1:
                        current_date = signal_df.index[i]
                        if current_date > end_date_ts: break
                        
                        if signal_df['is_signal'].iloc[i]:
                            row = signal_df.iloc[i]
                            next_day_row = signal_df.iloc[i+1]
                            entry_price = next_day_row[trade_open_col]
                            
                            atr = row['atr_14']
                            if strategy_mode == 'BIOX':
                                atr = entry_price * 0.15 
                                tp_mult = 3.0 
                                sl_mult = 1.0 
                            elif atr <= 0:
                                i += 1; continue
                                
                            if entry_price <= 0:
                                i += 1; continue
                                
                            tp_price = entry_price + (tp_mult * atr)
                            sl_price = entry_price - (sl_mult * atr)
                            
                            trade_status = 'CLOSED_EXPIRED'
                            close_price = entry_price
                            close_date = next_day_row.name
                            days_held = 0
                            
                            for h in range(max_hold):
                                day_idx = i + 1 + h
                                if day_idx >= len(signal_df): 
                                    close_price = signal_df.iloc[-1][trade_close_col]
                                    close_date = signal_df.index[-1]
                                    break
                                
                                day_candle = signal_df.iloc[day_idx]
                                d_open = day_candle[trade_open_col]
                                d_high = day_candle[trade_high_col]
                                d_low = day_candle[trade_low_col]
                                d_close = day_candle[trade_close_col]
                                
                                days_held += 1
                                close_date = day_candle.name
                                
                                # Symulacja intra-day
                                if d_open <= sl_price:
                                    trade_status = 'CLOSED_SL'
                                    close_price = d_open
                                    break
                                
                                if d_low <= sl_price:
                                    trade_status = 'CLOSED_SL'
                                    close_price = sl_price
                                    break
                                
                                if d_high >= tp_price:
                                    trade_status = 'CLOSED_TP'
                                    close_price = tp_price
                                    break
                                
                                if h == max_hold - 1:
                                    trade_status = 'CLOSED_EXPIRED'
                                    close_price = d_close
                                    break
                            
                            p_l_percent = ((close_price - entry_price) / entry_price) * 100
                            
                            metric_score = 0.0
                            
                            try:
                                if strategy_mode == 'H3': 
                                    metric_score = float(row.get('aqm_score_h3', 0))
                                elif strategy_mode == 'AQM': 
                                    metric_score = float(row.get('aqm_score', 0))
                                elif strategy_mode == 'BIOX': 
                                    metric_score = float(row.get('aqm_score_h3', 0))
                                elif strategy_mode == 'SDAR': 
                                    metric_score = float(row.get('aqm_score_h3', 0))
                            except Exception:
                                metric_score = 0.0

                            trade_data = {
                                "ticker": ticker,
                                "setup_type": setup_name_base,
                                "entry_price": float(entry_price),
                                "stop_loss": float(sl_price),
                                "take_profit": float(tp_price),
                                "metric_aqm_score_h3": metric_score,
                                "metric_atr_14": float(atr),
                                "metric_J_norm": float(row.get('J_norm', 0)) if strategy_mode == 'H3' else float(row.get('qps', 0)),
                                "metric_nabla_sq_norm": float(row.get('nabla_sq_norm', 0)) if strategy_mode == 'H3' else float(row.get('ras', 0)),
                                "metric_m_sq_norm": float(row.get('m_sq_norm', 0)) if strategy_mode == 'H3' else float(row.get('vms', 0)),
                                "status": trade_status,
                                "close_price": float(close_price),
                                "final_profit_loss_percent": float(p_l_percent),
                                "open_date": next_day_row.name,
                                "close_date": close_date
                            }
                            
                            vt = models.VirtualTrade(**trade_data)
                            session.add(vt)
                            trades_generated += 1
                            
                            i += max(1, days_held)
                        else:
                            i += 1
                
                processed_count += 1
                if processed_count % 5 == 0: 
                    update_scan_progress(session, processed_count, total_tickers)
                
                if processed_count % 20 == 0:
                    session.commit()

            except Exception as e:
                logger.error(f"Błąd backtestu dla {ticker}: {e}", exc_info=True)
                # log_decision(session, ticker, "BACKTEST", "ERROR", str(e)[:100])
                session.rollback()
                continue

        session.commit()
        update_scan_progress(session, total_tickers, total_tickers)
        summary = f"BACKTEST: Zakończono dla roku {year}. Wygenerowano {trades_generated} transakcji."
        logger.info(summary)
        append_scan_log(session, summary)
        # Czyszczenie flagi
        update_system_control(session, 'backtest_request', 'NONE')

    except Exception as e:
        err_msg = f"Krytyczny błąd Backtestu Unified: {e}"
        logger.error(err_msg, exc_info=True)
        append_scan_log(session, err_msg)
        update_system_control(session, 'backtest_request', 'NONE')
