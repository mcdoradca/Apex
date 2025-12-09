import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ==================================================================================
# NARZĘDZIA POMOCNICZE (INDICATORS)
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
    """Upewnia się, że indeks to DatetimeIndex bez strefy czasowej."""
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

def _calculate_ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

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

def _calculate_ad_line(df: pd.DataFrame) -> pd.Series:
    """
    Oblicza Chaikin A/D Line ręcznie.
    AD = CumSum(((Close - Low) - (High - Close)) / (High - Low) * Volume)
    """
    try:
        clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
        clv = clv.fillna(0.0)  # Zabezpieczenie przed dzieleniem przez zero
        ad_vol = clv * df['volume']
        return ad_vol.cumsum()
    except Exception:
        return pd.Series(0, index=df.index)

# ==================================================================================
# IMPLEMENTACJA AQM V2.0 (WERSJA UZIEMIONA - ZGODNIE Z PDF)
# ZMODYFIKOWANA POD HISTORYCZNE DANE MAKRO (Time-Travel Fix) I NASDAQ (QQQ)
# ==================================================================================

def calculate_aqm_full_vector(
    daily_df: pd.DataFrame,
    weekly_df: pd.DataFrame,
    intraday_60m_df: pd.DataFrame, # Ignorowane w V2 (zgodnie z PDF)
    obv_df: pd.DataFrame,
    macro_data: Dict[str, Any], 
    earnings_days_to: Optional[int] = None
) -> pd.DataFrame:
    """
    Oblicza wynik AQM V2.0.
    Struktura: QPS (40%) + RAS (20%) + VMS (30%) + TCS (10%)
    
    ZMIANY:
    - Obsługa historycznych szeregów czasowych dla Inflacji i Rentowności.
    - Zastąpienie SPY przez QQQ jako benchmarku reżimu rynkowego.
    """
    try:
        # Przygotowanie danych dziennych
        df = daily_df.copy()
        df = _ensure_numeric(df, ['open', 'high', 'low', 'close', 'volume'])
        df = _harden_index(df)
        
        if len(df) < 200: # Wymagane min. 200 świec dla EMA(200)
            return pd.DataFrame()

        # Przygotowanie danych tygodniowych (dla QPS Weekly)
        weekly_clean = _ensure_numeric(weekly_df.copy())
        weekly_clean = _harden_index(weekly_clean)
        
        # Jeśli brak danych tygodniowych, resampluj z dziennych
        if weekly_clean.empty and not df.empty:
            weekly_clean = df.resample('W').agg({
                'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
            })

        # Wyrównanie tygodniowych do dziennych (ffill)
        weekly_aligned = weekly_clean.reindex(df.index, method='ffill')

        # === WARSTWA 1: QUANTUM PRIME SCORE (QPS) - Waga 40% ===
        
        # 1. Analiza Daily (60% wagi QPS)
        df['ema_50'] = _calculate_ema(df['close'], 50)
        df['ema_200'] = _calculate_ema(df['close'], 200)
        df['rsi_14'] = _calculate_rsi(df['close'], 14)
        df['macd'], df['macd_signal'] = _calculate_macd(df['close'])
        
        # 2. Analiza Weekly (40% wagi QPS)
        df['w_ema_20'] = _calculate_ema(weekly_aligned['close'], 20)
        df['w_ema_50'] = _calculate_ema(weekly_aligned['close'], 50)
        
        # Obliczanie ATR
        prev_close = df['close'].shift()
        tr = pd.concat([
            df['high'] - df['low'],
            (df['high'] - prev_close).abs(),
            (df['low'] - prev_close).abs()
        ], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()

        def calc_qps(row):
            # Daily Logic
            trend_score_d = 1.0 if (row['close'] > row['ema_50'] > row['ema_200']) else 0.0
            momentum_score_d = 1.0 if (row['rsi_14'] > 55) else 0.0
            macd_score_d = 1.0 if (row['macd'] > row['macd_signal']) else 0.0
            
            # Weekly Logic
            w_close = row.get('w_close', row['close']) 
            trend_score_w = 1.0 if (w_close > row['w_ema_20'] > row['w_ema_50']) else 0.0
            
            # Formula: (Avg(Daily) * 0.6) + (Weekly * 0.4)
            daily_avg = (trend_score_d + momentum_score_d + macd_score_d) / 3.0
            return (daily_avg * 0.6) + (trend_score_w * 0.4)

        df['w_close'] = weekly_aligned['close']
        df['qps'] = df.apply(calc_qps, axis=1)

        # === WARSTWA 2: REGIME ADAPTATION SCORE (RAS) - Waga 20% ===
        # Cel: Ocena reżimu (zastępstwo VIX). 
        # UPDATE: Używamy QQQ (Nasdaq) zamiast SPY oraz historycznych danych makro.
        
        qqq_df = macro_data.get('qqq_df', pd.DataFrame()) # Zmieniono nazwę klucza ze spy_df na qqq_df
        
        # Pobieranie szeregów czasowych makro (lub pojedynczych wartości jako fallback)
        inflation_data = macro_data.get('inflation_series')
        yield_10y_data = macro_data.get('yield_series')
        
        # Obliczanie QQQ EMA 200
        qqq_ema_200 = pd.Series(dtype=float)
        if not qqq_df.empty:
            qqq_clean = _harden_index(qqq_df)
            qqq_clean = _ensure_numeric(qqq_clean, ['close'])
            qqq_reindexed = qqq_clean.reindex(df.index, method='ffill')
            qqq_ema_200 = _calculate_ema(qqq_reindexed['close'], 200)
            df['qqq_close'] = qqq_reindexed['close']
        else:
            df['qqq_close'] = np.nan

        def calc_ras(row):
            # Pobierz dane makro dla DANEGO DNIA (Time Travel Fix)
            current_date = row.name
            
            # 1. Inflacja (Jeśli seria, weź asof, jeśli float, użyj stałej)
            curr_inf = 0.0
            if isinstance(inflation_data, pd.Series):
                # asof() znajduje ostatnią dostępną wartość przed lub w dacie
                if not inflation_data.empty:
                    curr_inf = float(inflation_data.asof(current_date))
            else:
                curr_inf = float(macro_data.get('inflation', 0.0)) # Fallback

            # 2. Rentowność (Yield)
            curr_yield = 0.0
            if isinstance(yield_10y_data, pd.Series):
                if not yield_10y_data.empty:
                    curr_yield = float(yield_10y_data.asof(current_date))
            else:
                curr_yield = float(macro_data.get('yield_10y', 0.0)) # Fallback

            # Warunki RISK_OFF (Zaktualizowane)
            # 1. Inflation > 4.0
            cond_inf = curr_inf > 4.0
            # 2. Yield 10y > 4.5
            cond_yield = curr_yield > 4.5
            # 3. QQQ Price < QQQ EMA 200 (Bessa na Nasdaq)
            cond_qqq = False
            if not pd.isna(row.get('qqq_close')) and not pd.isna(qqq_ema_200.get(row.name)):
                 cond_qqq = row['qqq_close'] < qqq_ema_200[row.name]
            
            is_risk_off = cond_inf or cond_yield or cond_qqq
            
            # 0.1 (Kara za Risk-Off) lub 1.0 (Brak Kary)
            return 0.1 if is_risk_off else 1.0

        df['ras'] = df.apply(calc_ras, axis=1)

        # === WARSTWA 3: VOLUME/MICROSTRUCTURE SCORE (VMS) - Waga 30% ===
        
        if obv_df is not None and not obv_df.empty:
            obv_clean = _ensure_numeric(obv_df.copy(), ['OBV'])
            obv_clean = _harden_index(obv_clean)
            df = df.join(obv_clean['OBV'], rsuffix='_api')
            df['obv_final'] = df['OBV'].fillna(df.get('OBV_api', np.nan))
        else:
            direction = np.sign(df['close'].diff())
            df['obv_final'] = (direction * df['volume']).fillna(0).cumsum()
            
        df['obv_ema_20'] = _calculate_ema(df['obv_final'], 20)
        df['ad_line'] = _calculate_ad_line(df)
        df['ad_ema_20'] = _calculate_ema(df['ad_line'], 20)
        df['vol_avg_20'] = df['volume'].replace(0, np.nan).rolling(20).mean()

        def calc_vms(row):
            obv_score = 1.0 if (row['obv_final'] > row['obv_ema_20']) else 0.0
            ad_score = 1.0 if (row['ad_line'] > row['ad_ema_20']) else 0.0
            vol_score = 1.0 if (row['volume'] > (row['vol_avg_20'] * 1.5)) else 0.0
            return (obv_score * 0.4) + (ad_score * 0.3) + (vol_score * 0.3)

        df['vms'] = df.apply(calc_vms, axis=1)

        # === WARSTWA 4: TEMPORAL COHERENCE SCORE (TCS) - Waga 10% ===
        
        def calc_tcs(row):
            if earnings_days_to is not None and row.name == df.index[-1]:
                if abs(earnings_days_to) <= 5:
                    return 0.1 # Kara za earnings
            return 1.0

        df['tcs'] = df.apply(calc_tcs, axis=1)

        # === FINAL SCORE & ENTRY LOGIC ===
        
        df['aqm_score'] = (
            (df['qps'] * 0.40) +
            (df['ras'] * 0.20) +
            (df['vms'] * 0.30) +
            (df['tcs'] * 0.10)
        )
        
        cols_to_fill = ['aqm_score', 'qps', 'ras', 'vms', 'tcs', 'atr']
        df[cols_to_fill] = df[cols_to_fill].fillna(0.0)
        
        return df[['open', 'high', 'low', 'close', 'volume', 'atr', 'aqm_score', 'qps', 'ras', 'vms', 'tcs']]

    except Exception as e:
        logger.error(f"Krytyczny błąd w jądrze AQM V2 (Wersja Uziemiona): {e}", exc_info=True)
        return pd.DataFrame()

# ==================================================================================
# LOGIKA H4: KINETIC ALPHA (PULSE HUNTER)
# Zachowana dla kompatybilności wstecznej z Phase 4
# ==================================================================================

def analyze_intraday_kinetics(intraday_df: pd.DataFrame) -> Dict[str, Any]:
    """
    MÓZG STRATEGII H4: KINETIC ALPHA
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
