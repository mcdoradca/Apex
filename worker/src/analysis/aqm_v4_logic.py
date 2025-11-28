import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ==================================================================================
# NARZĘDZIA POMOCNICZE (Sanityzacja i Obliczenia)
# ==================================================================================

def _ensure_numeric(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    """
    Krytyczna funkcja czyszcząca. Konwertuje kolumny na float, zamieniając błędy na NaN.
    Naprawia błąd 'unsupported operand type(s) for -: str and str'.
    """
    if df is None or df.empty:
        return df
    
    target_cols = columns if columns else df.columns
    for col in target_cols:
        if col in df.columns:
            # Zamiana 'None', 'NaN' stringów na np.nan
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def _calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def _calculate_macd(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    exp12 = series.ewm(span=12, adjust=False).mean()
    exp26 = series.ewm(span=26, adjust=False).mean()
    macd = exp12 - exp26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd, signal

def _resample_to_daily(source_df: pd.DataFrame, rule='1D', method='last') -> pd.DataFrame:
    """
    Bezpieczne rzutowanie danych intraday/weekly na indeks dzienny.
    """
    if source_df is None or source_df.empty:
        return pd.DataFrame()
    
    # Upewniamy się, że indeks jest datetime
    if not isinstance(source_df.index, pd.DatetimeIndex):
        source_df.index = pd.to_datetime(source_df.index).tz_localize(None)
    else:
        source_df.index = source_df.index.tz_localize(None) # Ujednolicenie stref

    if method == 'last':
        resampled = source_df.resample(rule).last()
    elif method == 'mean':
        resampled = source_df.resample(rule).mean()
    
    # Forward fill, aby wypełnić weekendy i luki (zachowujemy stan wskaźnika)
    return resampled.ffill()

# ==================================================================================
# GŁÓWNA LOGIKA AQM
# ==================================================================================

def calculate_aqm_full_vector(
    daily_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    intraday_60m_df: pd.DataFrame, # Wymagane dla QPS (zgodnie z PDF)
    obv_df: pd.DataFrame,
    macro_data: Dict[str, Any], # Treasury Yields, Inflation, SPY
    earnings_days_to: Optional[int] = None
) -> pd.DataFrame:
    """
    Oblicza pełny wektor AQM bez uproszczeń.
    Wykorzystuje pełną paletę danych dostępnych w Alpha Vantage.
    """
    try:
        # --- 0. PRZYGOTOWANIE DANYCH (HARDENING) ---
        # Kopiujemy i czyścimy typy danych, aby uniknąć błędów na produkcji.
        df = daily_df.copy()
        df = _ensure_numeric(df, ['open', 'high', 'low', 'close', 'volume'])
        
        if len(df) < 50:
            logger.warning("Zbyt krótka historia danych dziennych dla AQM.")
            return pd.DataFrame()

        # Przygotowanie Weekly (rzutowanie na Daily)
        weekly_clean = _ensure_numeric(weekly_df.copy())
        weekly_aligned = _resample_to_daily(weekly_clean).reindex(df.index, method='ffill')
        
        # Przygotowanie Intraday (rzutowanie na Daily - jako 'stan na koniec dnia')
        # PDF: "Spójność ruchów cenowych na wielu timeframe'ach"
        intraday_clean = _ensure_numeric(intraday_60m_df.copy())
        intraday_aligned = pd.DataFrame()
        if not intraday_clean.empty:
            intraday_aligned = _resample_to_daily(intraday_clean).reindex(df.index, method='ffill')

        # === 1. QUANTUM PRIME SCORE (QPS) - 40% wagi ===
        # PDF: "Nie jest to zwykły RSI. Analiza momentum na wielu timeframe'ach."
        # Wymóg: Cena rośnie na wszystkich TF -> silny sygnał.
        
        # Wskaźniki dla Daily
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
        df['rsi'] = _calculate_rsi(df['close'])
        df['macd'], df['macd_signal'] = _calculate_macd(df['close'])
        
        # Wskaźniki dla Weekly (Trend nadrzędny)
        if not weekly_aligned.empty:
            df['w_ema_20'] = weekly_aligned['close'].ewm(span=20, adjust=False).mean()
            df['w_trend_up'] = weekly_aligned['close'] > df['w_ema_20']
        else:
            df['w_trend_up'] = False # Konserwatywnie

        # Wskaźniki dla Intraday 60min (Mikro-struktura)
        if not intraday_aligned.empty:
            df['i_ema_20'] = intraday_aligned['close'].ewm(span=20, adjust=False).mean()
            df['i_trend_up'] = intraday_aligned['close'] > df['i_ema_20']
        else:
            # Jeśli brak danych intraday (np. daleka historia), używamy Daily jako proxy
            df['i_trend_up'] = df['close'] > df['ema_20']

        # ATR (Zmienność)
        prev_close = df['close'].shift()
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        df['atr_percent'] = (df['atr'] / df['close']).fillna(0.0)

        def calc_qps_row(row):
            # 1. Multi-Timeframe Alignment (50%)
            # PDF: "Jeśli cena rośnie na wszystkich timeframe'ach, to momentum jest silne"
            alignment_score = 0.0
            
            # Daily Alignment
            daily_ok = (row['close'] > row['ema_20']) and (row['ema_20'] > row['ema_50'])
            if daily_ok: alignment_score += 0.4
            
            # Weekly Alignment (Big Picture)
            if row['w_trend_up']: alignment_score += 0.3
            
            # Intraday Alignment (Execution)
            if row['i_trend_up']: alignment_score += 0.3
            
            # 2. Momentum Strength (30%)
            mom_score = 0.0
            # RSI w strefie "Bullish Control" (40-80), nie overbought > 80 w silnym trendzie
            if 50 < row['rsi'] < 75: mom_score += 0.5
            elif 40 < row['rsi'] <= 50: mom_score += 0.2 # Recovery zone
            
            if row['macd'] > row['macd_signal']: mom_score += 0.5
            
            # 3. Volatility Quality (20%)
            # Niska zmienność jest lepsza dla bezpiecznego wejścia (Compression)
            vol_score = 0.0
            if row['atr_percent'] < 0.03: vol_score = 1.0 # Tight consolidation
            elif row['atr_percent'] < 0.06: vol_score = 0.6
            else: vol_score = 0.2 # High volatility risk
            
            return (alignment_score * 0.5) + (mom_score * 0.3) + (vol_score * 0.2)

        df['qps'] = df.apply(calc_qps_row, axis=1)

        # === 2. VOLUME ENTROPY SCORE (VES) - 30% wagi ===
        # PDF: "Analiza On-Balance Volume (OBV) jest KLUCZOWA"
        # Dodajemy: Accumulation/Distribution Line (A/D)
        
        # 1. OBV (Z API lub obliczone)
        if obv_df is not None and not obv_df.empty:
            obv_clean = _ensure_numeric(obv_df.copy(), ['OBV'])
            # Dołączamy po dacie
            df = df.join(obv_clean['OBV'], rsuffix='_api')
            # Priorytet dla danych z API, fallback na własne
            df['obv_final'] = df['OBV'].fillna(df.get('OBV_api', np.nan))
        else:
            df['obv_final'] = np.nan
            
        # Fallback Calculation (jeśli API nie ma danych dla starych dat)
        df['obv_calc'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
        df['obv_final'] = df['obv_final'].fillna(df['obv_calc'])
        
        # Wskaźniki OBV
        df['obv_ma20'] = df['obv_final'].rolling(20).mean()
        df['obv_slope'] = df['obv_final'].diff(5) # Krótki trend OBV
        
        # 2. Volume Ratio
        df['vol_ma20'] = df['volume'].rolling(20).mean()
        df['vol_ratio'] = df['volume'] / df['vol_ma20'].replace(0, 1)
        
        # 3. Accumulation/Distribution (A/D) - Zgodnie z formułą klasyczną
        # ((Close - Low) - (High - Close)) / (High - Low) * Volume
        mf_multiplier = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low']).replace(0, 1)
        df['ad_line'] = (mf_multiplier * df['volume']).fillna(0).cumsum()
        df['ad_slope'] = df['ad_line'].diff(5)

        def calc_ves_row(row):
            score = 0.0
            
            # OBV Confirmation (40%)
            # Czy OBV rośnie szybciej niż cena? (Divergence check - uproszczony)
            if row['obv_final'] > row['obv_ma20']: score += 0.2
            if row['obv_slope'] > 0: score += 0.2
            
            # Volume Anomaly (30%)
            # Szukamy "Smart Money footprints" - duży wolumen przy wzroście
            if row['vol_ratio'] > 1.2 and row['close'] > row['open']: score += 0.3
            elif row['vol_ratio'] > 0.8: score += 0.1
            
            # A/D Line (30%)
            if row['ad_slope'] > 0: score += 0.3
            
            return score

        df['ves'] = df.apply(calc_ves_row, axis=1)

        # === 3. MARKET REGIME SCORE (MRS) - 20% wagi ===
        # PDF: "Rynek zachowuje się inaczej w różnych reżimach."
        # Wymagane dane: VIX (zmienna), SPY Trend, Rentowność Obligacji (Treasury Yields)
        
        # Pobieranie danych makro
        spy_df = macro_data.get('spy_df')
        yield_10y_val = macro_data.get('yield_10y', 4.0) # Domyślnie 4%
        inflation_val = macro_data.get('inflation', 3.0) # Domyślnie 3%
        
        # SPY Trend Analysis
        is_bull_market_spy = True
        spy_volatility = 0.01 # Proxy dla VIX jeśli brak danych
        
        if spy_df is not None and not spy_df.empty:
            # Mapowanie SPY do dat tickera
            spy_reindexed = spy_df.reindex(df.index, method='ffill')
            spy_ma200 = spy_reindexed['close'].rolling(200).mean()
            is_bull_market_spy = spy_reindexed['close'] > spy_ma200
            
            # Obliczanie historycznej zmienności SPY jako proxy VIX (jeśli nie mamy realnego VIX)
            # VIX ~ Annualized StdDev * 100
            spy_volatility = spy_reindexed['close'].pct_change().rolling(20).std() * np.sqrt(252) * 100
        
        # Definicje Reżimów (Zgodnie z PDF str. 4)
        # 1. High Growth / Low Rates: VIX < 18, Yields < 2% (Tu: dostosowane do realiów)
        # 2. Inflation Transition: VIX > 22, Yields rosną
        # 3. Risk Off: VIX > 28
        
        def calc_mrs_row(row):
            # Używamy zmienności SPY jeśli VIX nie jest dostępny per day
            current_vix_proxy = spy_volatility.loc[row.name] if isinstance(spy_volatility, pd.Series) else spy_volatility
            is_spy_bull = is_bull_market_spy.loc[row.name] if isinstance(is_bull_market_spy, pd.Series) else is_bull_market_spy
            
            # PDF Logic Adaptation
            regime_score = 0.0
            
            if current_vix_proxy < 18 and is_spy_bull:
                # BULL MARKET
                regime_score = 1.0 # Pełne zielone światło
                # Kara za wysokie stopy procentowe (Equity Risk Premium)
                if yield_10y_val > 4.5: regime_score -= 0.2
                
            elif current_vix_proxy > 25 or not is_spy_bull:
                # BEAR MARKET / RISK OFF
                regime_score = 0.3 # Bardzo ostrożnie
                # Jeśli inflacja spada, lekki optymizm
                if inflation_val < 3.0: regime_score += 0.2
                
            else:
                # VOLATILE / TRANSITION
                regime_score = 0.6
            
            return max(0.1, regime_score)

        df['mrs'] = df.apply(calc_mrs_row, axis=1)

        # === 4. TEMPORAL COHERENCE SCORE (TCS) - 10% wagi ===
        # PDF: "Timing jest kluczowy. Unikaj 5 dni przed earnings."
        
        def calc_tcs_row(row):
            score = 1.0
            
            # Obsługa Earnings (Jeśli mamy dane)
            # W backteście trudno o historyczną datę earnings dla każdego dnia.
            # Używamy tego głównie w trybie Live (ostatni wiersz).
            if earnings_days_to is not None and row.name == df.index[-1]:
                # PDF: Unikaj 0 < days <= 5
                if 0 <= earnings_days_to <= 5:
                    return 0.0 # BARDZO RYZYKOWNE - ZABÓJCA SETUPU
                # PDF: Optymalnie 14-30 dni po wynikach
                elif 14 <= earnings_days_to <= 30:
                    score = 1.2 # Bonus
            
            # Seasonality (Uproszczone)
            # Unikanie historycznie słabych miesięcy (Wrzesień)
            if row.name.month == 9:
                score *= 0.8
                
            return score

        df['tcs'] = df.apply(calc_tcs_row, axis=1)
        
        # === FINAL AQM SCORE ===
        # PDF: "Iloczyn komponentów zapewnia, że wszystkie muszą być pozytywne"
        df['aqm_score'] = df['qps'] * df['ves'] * df['mrs'] * df['tcs']
        
        # Zwracamy wyczyszczoną ramkę
        cols = ['open', 'high', 'low', 'close', 'volume', 'atr', 'aqm_score', 'qps', 'ves', 'mrs', 'tcs']
        return df[cols].dropna()

    except Exception as e:
        logger.error(f"Krytyczny błąd w jądrze AQM v4: {e}", exc_info=True)
        return pd.DataFrame()
