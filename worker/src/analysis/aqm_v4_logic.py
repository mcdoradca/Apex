import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ==================================================================================
# NARZĘDZIA POMOCNICZE (Sanityzacja i Obliczenia)
# ==================================================================================

def _ensure_numeric(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    """Konwertuje kolumny na float, zamieniając błędy na NaN."""
    if df is None or df.empty:
        return df
    
    target_cols = columns if columns else df.columns
    for col in target_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def _harden_index(df: pd.DataFrame) -> pd.DataFrame:
    """Upewnia się, że indeks to DatetimeIndex."""
    if df is None or df.empty:
        return df
    try:
        if not isinstance(df.index, pd.DatetimeIndex):
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            else:
                df.index = pd.to_datetime(df.index)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
    except Exception as e:
        logger.warning(f"Nie udało się naprawić indeksu: {e}")
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
    if source_df is None or source_df.empty:
        return pd.DataFrame()
    source_df = _harden_index(source_df)
    if method == 'last':
        resampled = source_df.resample(rule).last()
    elif method == 'mean':
        resampled = source_df.resample(rule).mean()
    return resampled.ffill()

# ==================================================================================
# LOGIKA H4: KINETIC ALPHA (PULSE HUNTER) - PRZYWRÓCONA
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
        df = _harden_index(df)
        df.sort_index(inplace=True)

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
            
            if day_open > 0 and (day_low - day_open) / day_open < -0.05:
                stats['hard_floor_violations'] += 1
            
            if day_low > 0:
                vol = (day_high - day_low) / day_low
                volatilities.append(vol)

            if (day_high - day_low) > 0:
                day_close = day_data['close'].iloc[-1]
                elast = (day_close - day_low) / (day_high - day_low)
                elasticity_scores.append(elast)

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

        stats['total_2pct_shots'] = sum(daily_shots_list)
        stats['max_daily_shots'] = max(daily_shots_list) if daily_shots_list else 0
        stats['avg_swing_size'] = np.mean(swing_sizes) if swing_sizes else 0.0
        stats['avg_intraday_volatility'] = np.mean(volatilities) if volatilities else 0.0
        stats['elasticity'] = np.mean(elasticity_scores) if elasticity_scores else 0.0
        stats['last_shot_date'] = last_shot_dt

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
# GŁÓWNA LOGIKA AQM (V4) - FIX: NEUTRAL FILLNA
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
    Oblicza pełny wektor AQM.
    FIX: Wypełnia braki wartością 0.5 (Neutral), aby nie zerować iloczynu.
    """
    try:
        df = daily_df.copy()
        df = _ensure_numeric(df, ['open', 'high', 'low', 'close', 'volume'])
        df = _harden_index(df)
        
        if len(df) < 50:
            return pd.DataFrame()

        weekly_clean = _ensure_numeric(weekly_df.copy())
        weekly_clean = _harden_index(weekly_clean)
        weekly_aligned = _resample_to_daily(weekly_clean).reindex(df.index, method='ffill')
        
        intraday_clean = _ensure_numeric(intraday_60m_df.copy())
        intraday_clean = _harden_index(intraday_clean)
        intraday_aligned = pd.DataFrame()
        if not intraday_clean.empty:
            intraday_aligned = _resample_to_daily(intraday_clean).reindex(df.index, method='ffill')

        # === 1. QUANTUM PRIME SCORE (QPS) ===
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['ema_200'] = df['close'].ewm(span=200, adjust=False).mean()
        df['rsi'] = _calculate_rsi(df['close'])
        df['macd'], df['macd_signal'] = _calculate_macd(df['close'])
        
        if not weekly_aligned.empty:
            df['w_ema_20'] = weekly_aligned['close'].ewm(span=20, adjust=False).mean()
            df['w_trend_up'] = weekly_aligned['close'] > df['w_ema_20']
        else:
            df['w_trend_up'] = False 

        if not intraday_aligned.empty:
            df['i_ema_20'] = intraday_aligned['close'].ewm(span=20, adjust=False).mean()
            df['i_trend_up'] = intraday_aligned['close'] > df['i_ema_20']
        else:
            df['i_trend_up'] = df['close'] > df['ema_20']

        prev_close = df['close'].shift()
        tr1 = df['high'] - df['low']
        tr2 = (df['high'] - prev_close).abs()
        tr3 = (df['low'] - prev_close).abs()
        df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df['atr'] = df['tr'].rolling(14).mean()
        df['atr_percent'] = (df['atr'] / df['close']).fillna(0.0)

        def calc_qps_row(row):
            # QPS bazuje na danych dziennych, więc rzadko jest NaN, o ile mamy historię
            alignment_score = 0.0
            # Używamy get, aby uniknąć błędów jeśli kolumny nie istnieją
            close = row.get('close', 0)
            ema20 = row.get('ema_20', 0)
            ema50 = row.get('ema_50', 0)
            
            if pd.isna(ema20) or pd.isna(ema50): return 0.5 # Neutralny start

            daily_ok = (close > ema20) and (ema20 > ema50)
            if daily_ok: alignment_score += 0.4
            if row.get('w_trend_up'): alignment_score += 0.3
            if row.get('i_trend_up'): alignment_score += 0.3
            
            mom_score = 0.0
            rsi = row.get('rsi', 50)
            if pd.isna(rsi): rsi = 50
            
            if 50 < rsi < 75: mom_score += 0.5
            elif 40 < rsi <= 50: mom_score += 0.2
            
            macd = row.get('macd', 0)
            macd_sig = row.get('macd_signal', 0)
            if macd > macd_sig: mom_score += 0.5
            
            vol_score = 0.0
            atr_pct = row.get('atr_percent', 0.05)
            if atr_pct < 0.03: vol_score = 1.0
            elif atr_pct < 0.06: vol_score = 0.6
            else: vol_score = 0.2
            
            return (alignment_score * 0.5) + (mom_score * 0.3) + (vol_score * 0.2)

        df['qps'] = df.apply(calc_qps_row, axis=1)

        # === 2. VOLUME ENTROPY SCORE (VES) ===
        if obv_df is not None and not obv_df.empty:
            obv_clean = _ensure_numeric(obv_df.copy(), ['OBV'])
            obv_clean = _harden_index(obv_clean)
            df = df.join(obv_clean['OBV'], rsuffix='_api')
            df['obv_final'] = df['OBV'].fillna(df.get('OBV_api', np.nan))
        else:
            df['obv_final'] = np.nan
            
        # Fallback do kalkulowanego OBV
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
            # Jeśli brakuje danych wolumenowych, zwróć neutralne 0.5
            if pd.isna(row.get('obv_final')): return 0.5
            
            if row['obv_final'] > row.get('obv_ma20', 0): score += 0.2
            if row.get('obv_slope', 0) > 0: score += 0.2
            if row.get('vol_ratio', 1) > 1.2 and row['close'] > row['open']: score += 0.3
            elif row.get('vol_ratio', 1) > 0.8: score += 0.1
            if row.get('ad_slope', 0) > 0: score += 0.3
            return score

        df['ves'] = df.apply(calc_ves_row, axis=1)

        # === 3. MARKET REGIME SCORE (MRS) ===
        # Domyślne wartości neutralne, jeśli makro brakuje
        spy_df = macro_data.get('spy_df')
        yield_10y_val = macro_data.get('yield_10y', 4.0)
        inflation_val = macro_data.get('inflation', 3.0)
        
        is_bull_market_spy = True
        spy_volatility = 0.01
        
        if spy_df is not None and not spy_df.empty:
            spy_clean = _harden_index(spy_df)
            spy_reindexed = spy_clean.reindex(df.index, method='ffill')
            spy_ma200 = spy_reindexed['close'].rolling(200).mean()
            is_bull_market_spy = spy_reindexed['close'] > spy_ma200
            spy_volatility = spy_reindexed['close'].pct_change().rolling(20).std() * np.sqrt(252) * 100
        else:
            # Fallback dla SPY - zakładamy neutral/bull żeby nie blokować testu
            is_bull_market_spy = pd.Series(True, index=df.index)
            spy_volatility = pd.Series(15.0, index=df.index) # Średnia zmienność

        def calc_mrs_row(row):
            # Bezpieczne pobieranie wartości z Series lub scalar
            idx = row.name
            try:
                curr_vix = spy_volatility.loc[idx] if isinstance(spy_volatility, pd.Series) else spy_volatility
                is_bull = is_bull_market_spy.loc[idx] if isinstance(is_bull_market_spy, pd.Series) else is_bull_market_spy
            except:
                curr_vix = 20.0
                is_bull = True

            regime_score = 0.0
            if curr_vix < 18 and is_bull:
                regime_score = 1.0 
                if yield_10y_val > 4.5: regime_score -= 0.2
            elif curr_vix > 25 or not is_bull:
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
        # Wypełniamy braki wartością 0.5 (Neutral), a nie 0.0!
        # Mnożenie przez 0.0 zabija wynik. Mnożenie przez 0.5 tylko go osłabia.
        df['qps'] = df['qps'].fillna(0.5)
        df['ves'] = df['ves'].fillna(0.5)
        df['mrs'] = df['mrs'].fillna(0.5)
        df['tcs'] = df['tcs'].fillna(1.0) # Czas domyślnie nie szkodzi

        df['aqm_score'] = df['qps'] * df['ves'] * df['mrs'] * df['tcs']
        
        cols = ['open', 'high', 'low', 'close', 'volume', 'atr', 'aqm_score', 'qps', 'ves', 'mrs', 'tcs']
        
        # Ostateczne zabezpieczenie - jeśli cokolwiek nadal jest NaN, daj 0.5 (by uniknąć dropna)
        df[cols] = df[cols].fillna(0.5)
        
        return df[cols]

    except Exception as e:
        logger.error(f"Krytyczny błąd w jądrze AQM v4: {e}", exc_info=True)
        return pd.DataFrame()
