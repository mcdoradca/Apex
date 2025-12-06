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

def _harden_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Krytyczna funkcja naprawcza indeksu. Upewnia się, że indeks to DatetimeIndex.
    Jeśli to int/object, próbuje konwersji.
    """
    if df is None or df.empty:
        return df
        
    try:
        # Jeśli indeks to nie Datetime, spróbuj skonwertować
        if not isinstance(df.index, pd.DatetimeIndex):
            # Jeśli w kolumnach jest 'date', użyj jej
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            else:
                # Ostatnia deska ratunku: próba konwersji samego indeksu
                df.index = pd.to_datetime(df.index)
        
        # Usuń strefy czasowe dla spójności
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
            
    except Exception as e:
        logger.warning(f"Nie udało się naprawić indeksu: {e}")
        # W najgorszym wypadku zwróć jak jest, błąd i tak wystąpi później
        
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
    
    # Upewniamy się, że indeks jest datetime (PONOWNA WERYFIKACJA)
    source_df = _harden_index(source_df)

    if method == 'last':
        resampled = source_df.resample(rule).last()
    elif method == 'mean':
        resampled = source_df.resample(rule).mean()
    
    # Forward fill, aby wypełnić weekendy i luki (zachowujemy stan wskaźnika)
    return resampled.ffill()

# ==================================================================================
# LOGIKA H4: KINETIC ALPHA (PULSE HUNTER)
# ==================================================================================

def analyze_intraday_kinetics(intraday_df: pd.DataFrame) -> Dict[str, Any]:
    """
    MÓZG STRATEGII H4: KINETIC ALPHA
    Analizuje dane 5-minutowe w poszukiwaniu 'Kinetycznych Strzałów' (>2% ruchu w górę).
    """
    stats = {
        'kinetic_score': 0,
        'elasticity': 0.0,
        'total_2pct_shots': 0,
        'max_daily_shots': 0,
        'avg_swing_size': 0.0,
        'hard_floor_violations': 0,
        'avg_intraday_volatility': 0.0,
        'last_shot_date': None
    }

    if intraday_df is None or intraday_df.empty:
        return stats

    try:
        df = _ensure_numeric(intraday_df.copy(), ['open', 'high', 'low', 'close', 'volume'])
        df = _harden_index(df) # Naprawa indeksu
        
        # Sortujemy chronologicznie
        df.sort_index(inplace=True)

        # Grupowanie po dniach
        daily_groups = df.groupby(df.index.date)
        
        daily_shots_list = []
        swing_sizes = []
        volatilities = []
        elasticity_scores = []
        last_shot_dt = None

        for date, day_data in daily_groups:
            if len(day_data) < 10: continue 

            day_open = day_data['open'].iloc[0]
            day_high = day_data['high'].max()
            day_low = day_data['low'].min()
            
            # 1. Hard Floor Violation (-5% od otwarcia)
            if day_open > 0 and (day_low - day_open) / day_open < -0.05:
                stats['hard_floor_violations'] += 1
            
            # 2. Average Intraday Volatility
            if day_low > 0:
                vol = (day_high - day_low) / day_low
                volatilities.append(vol)

            # 3. Elasticity (Sprężystość)
            if (day_high - day_low) > 0:
                day_close = day_data['close'].iloc[-1]
                elast = (day_close - day_low) / (day_high - day_low)
                elasticity_scores.append(elast)

            # 4. PULSE HUNTER ALGORITHM (Zliczanie Strzałów)
            shots_today = 0
            current_low = day_data['low'].iloc[0]
            
            for i in range(1, len(day_data)):
                candle = day_data.iloc[i]
                price_high = candle['high']
                price_low = candle['low']
                
                if current_low > 0:
                    potential_gain = (price_high - current_low) / current_low
                    
                    if potential_gain >= 0.02: 
                        shots_today += 1
                        swing_sizes.append(potential_gain * 100)
                        last_shot_dt = date
                        current_low = price_low 
                    
                    elif price_low < current_low:
                        current_low = price_low
            
            daily_shots_list.append(shots_today)

        # --- AGREGACJA WYNIKÓW ---
        stats['total_2pct_shots'] = sum(daily_shots_list)
        stats['max_daily_shots'] = max(daily_shots_list) if daily_shots_list else 0
        stats['avg_swing_size'] = np.mean(swing_sizes) if swing_sizes else 0.0
        stats['avg_intraday_volatility'] = np.mean(volatilities) if volatilities else 0.0
        stats['elasticity'] = np.mean(elasticity_scores) if elasticity_scores else 0.0
        stats['last_shot_date'] = last_shot_dt

        # --- SCORE KINETYCZNY (0-100) ---
        base_score = 0
        base_score += min(50, stats['total_2pct_shots'] * 1.5)
        base_score += min(20, stats['max_daily_shots'] * 4)
        if stats['avg_swing_size'] > 0:
            base_score += min(30, (stats['avg_swing_size'] / 3.0) * 30)
        penalty = stats['hard_floor_violations'] * 20
        final_score = int(max(0, min(100, base_score - penalty)))
        stats['kinetic_score'] = final_score

        return stats

    except Exception as e:
        logger.error(f"H4 Logic Error: {e}", exc_info=True)
        return stats

# ==================================================================================
# GŁÓWNA LOGIKA AQM (V4 Classic)
# ==================================================================================

def calculate_aqm_full_vector(
    daily_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    intraday_60m_df: pd.DataFrame, 
    obv_df: pd.DataFrame,
    macro_data: Dict[str, Any], 
    earnings_days_to: Optional[int] = None
) -> pd.DataFrame:
    """
    Oblicza pełny wektor AQM bez uproszczeń.
    """
    try:
        # --- 0. PRZYGOTOWANIE DANYCH (HARDENING) ---
        df = daily_df.copy()
        df = _ensure_numeric(df, ['open', 'high', 'low', 'close', 'volume'])
        df = _harden_index(df) # <--- NAPRAWA INDEKSU
        
        if len(df) < 50:
            return pd.DataFrame()

        # Przygotowanie Weekly (rzutowanie na Daily)
        weekly_clean = _ensure_numeric(weekly_df.copy())
        weekly_clean = _harden_index(weekly_clean) # <--- NAPRAWA INDEKSU
        
        # TERAZ reindex POWINIEN DZIAŁAĆ
        weekly_aligned = _resample_to_daily(weekly_clean).reindex(df.index, method='ffill')
        
        # Przygotowanie Intraday (rzutowanie na Daily)
        intraday_clean = _ensure_numeric(intraday_60m_df.copy())
        intraday_clean = _harden_index(intraday_clean) # <--- NAPRAWA INDEKSU
        
        intraday_aligned = pd.DataFrame()
        if not intraday_clean.empty:
            intraday_aligned = _resample_to_daily(intraday_clean).reindex(df.index, method='ffill')

        # === 1. QUANTUM PRIME SCORE (QPS) ===
        
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
        df['rsi'] = _calculate_rsi(df['close'])
        df['macd'], df['macd_signal'] = _calculate_macd(df['close'])
        
        # Wskaźniki dla Weekly
        if not weekly_aligned.empty:
            df['w_ema_20'] = weekly_aligned['close'].ewm(span=20, adjust=False).mean()
            df['w_trend_up'] = weekly_aligned['close'] > df['w_ema_20']
        else:
            df['w_trend_up'] = False 

        # Wskaźniki dla Intraday 60min
        if not intraday_aligned.empty:
            df['i_ema_20'] = intraday_aligned['close'].ewm(span=20, adjust=False).mean()
            df['i_trend_up'] = intraday_aligned['close'] > df['i_ema_20']
        else:
            df['i_trend_up'] = df['close'] > df['ema_20']

        # ATR
        prev_close = df['close'].shift()
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        df['atr_percent'] = (df['atr'] / df['close']).fillna(0.0)

        def calc_qps_row(row):
            alignment_score = 0.0
            daily_ok = (row['close'] > row['ema_20']) and (row['ema_20'] > row['ema_50'])
            if daily_ok: alignment_score += 0.4
            if row['w_trend_up']: alignment_score += 0.3
            if row['i_trend_up']: alignment_score += 0.3
            
            mom_score = 0.0
            if 50 < row['rsi'] < 75: mom_score += 0.5
            elif 40 < row['rsi'] <= 50: mom_score += 0.2
            
            if row['macd'] > row['macd_signal']: mom_score += 0.5
            
            vol_score = 0.0
            if row['atr_percent'] < 0.03: vol_score = 1.0
            elif row['atr_percent'] < 0.06: vol_score = 0.6
            else: vol_score = 0.2
            
            return (alignment_score * 0.5) + (mom_score * 0.3) + (vol_score * 0.2)

        df['qps'] = df.apply(calc_qps_row, axis=1)

        # === 2. VOLUME ENTROPY SCORE (VES) ===
        if obv_df is not None and not obv_df.empty:
            obv_clean = _ensure_numeric(obv_df.copy(), ['OBV'])
            obv_clean = _harden_index(obv_clean) # <--- NAPRAWA INDEKSU
            df = df.join(obv_clean['OBV'], rsuffix='_api')
            df['obv_final'] = df['OBV'].fillna(df.get('OBV_api', np.nan))
        else:
            df['obv_final'] = np.nan
            
        df['obv_calc'] = (np.sign(df['close'].diff()) * df['volume']).fillna(0).cumsum()
        df['obv_final'] = df['obv_final'].fillna(df['obv_calc'])
        
        df['obv_ma20'] = df['obv_final'].rolling(20).mean()
        df['obv_slope'] = df['obv_final'].diff(5) 
        
        df['vol_ma20'] = df['volume'].rolling(20).mean()
        df['vol_ratio'] = df['volume'] / df['vol_ma20'].replace(0, 1)
        
        mf_multiplier = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low']).replace(0, 1)
        df['ad_line'] = (mf_multiplier * df['volume']).fillna(0).cumsum()
        df['ad_slope'] = df['ad_line'].diff(5)

        def calc_ves_row(row):
            score = 0.0
            if row['obv_final'] > row['obv_ma20']: score += 0.2
            if row['obv_slope'] > 0: score += 0.2
            if row['vol_ratio'] > 1.2 and row['close'] > row['open']: score += 0.3
            elif row['vol_ratio'] > 0.8: score += 0.1
            if row['ad_slope'] > 0: score += 0.3
            return score

        df['ves'] = df.apply(calc_ves_row, axis=1)

        # === 3. MARKET REGIME SCORE (MRS) ===
        spy_df = macro_data.get('spy_df')
        yield_10y_val = macro_data.get('yield_10y', 4.0)
        inflation_val = macro_data.get('inflation', 3.0)
        
        is_bull_market_spy = True
        spy_volatility = 0.01
        
        if spy_df is not None and not spy_df.empty:
            spy_clean = _harden_index(spy_df) # <--- NAPRAWA INDEKSU
            spy_reindexed = spy_clean.reindex(df.index, method='ffill')
            spy_ma200 = spy_reindexed['close'].rolling(200).mean()
            is_bull_market_spy = spy_reindexed['close'] > spy_ma200
            spy_volatility = spy_reindexed['close'].pct_change().rolling(20).std() * np.sqrt(252) * 100
        
        def calc_mrs_row(row):
            current_vix_proxy = spy_volatility.loc[row.name] if isinstance(spy_volatility, pd.Series) else spy_volatility
            is_spy_bull = is_bull_market_spy.loc[row.name] if isinstance(is_bull_market_spy, pd.Series) else is_bull_market_spy
            
            regime_score = 0.0
            if current_vix_proxy < 18 and is_spy_bull:
                regime_score = 1.0 
                if yield_10y_val > 4.5: regime_score -= 0.2
            elif current_vix_proxy > 25 or not is_spy_bull:
                regime_score = 0.3 
                if inflation_val < 3.0: regime_score += 0.2
            else:
                regime_score = 0.6
            return max(0.1, regime_score)

        df['mrs'] = df.apply(calc_mrs_row, axis=1)

        # === 4. TEMPORAL COHERENCE SCORE (TCS) ===
        def calc_tcs_row(row):
            score = 1.0
            if earnings_days_to is not None and row.name == df.index[-1]:
                if 0 <= earnings_days_to <= 5: return 0.0 
                elif 14 <= earnings_days_to <= 30: score = 1.2 
            if row.name.month == 9: score *= 0.8
            return score

        df['tcs'] = df.apply(calc_tcs_row, axis=1)
        
        # === FINAL AQM SCORE ===
        df['aqm_score'] = df['qps'] * df['ves'] * df['mrs'] * df['tcs']
        
        cols = ['open', 'high', 'low', 'close', 'volume', 'atr', 'aqm_score', 'qps', 'ves', 'mrs', 'tcs']
        
        # === POPRAWKA: FAIL-SAFE FILL ===
        # Zamiast usuwać wiersze z brakami (co zeruje historię, jeśli np. brakuje OBV),
        # wypełniamy je zerami (neutralny/brak wpływu), aby symulacja mogła trwać.
        df[cols] = df[cols].fillna(0.0)
        
        return df[cols]

    except Exception as e:
        logger.error(f"Krytyczny błąd w jądrze AQM v4: {e}", exc_info=True)
        return pd.DataFrame()
