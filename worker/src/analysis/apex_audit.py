import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any
# Importy ML (wymagane do SensitivityAnalyzer)
from sklearn.ensemble import RandomForestRegressor

logger = logging.getLogger(__name__)

class ApexAudit:
    """
    Moduł diagnostyczny dla APEX AQM V3 (Basic Audit).
    Analizuje wyniki backtestu pod kątem korelacji kwantowych i wycieków Alpha.
    """

    @staticmethod
    def analyze(trades_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Przyjmuje listę słowników (transakcji) i zwraca zaawansowany raport analityczny.
        """
        if not trades_data:
            return {"error": "Brak danych do audytu"}

        df = pd.DataFrame(trades_data)
        
        # Konwersja typów danych dla bezpieczeństwa
        numeric_cols = ['profit_loss', 'inst_sync', 'retail_herding', 'price_gravity', 'aqm_score']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 1. Podstawowe metryki (KPI)
        finished_trades = df[df['profit_loss'].notnull()]
        
        if finished_trades.empty:
            return {"warning": "Brak zakończonych transakcji do analizy."}

        wins = finished_trades[finished_trades['profit_loss'] > 0]
        losses = finished_trades[finished_trades['profit_loss'] <= 0]
        
        gross_profit = wins['profit_loss'].sum()
        gross_loss = abs(losses['profit_loss'].sum())
        
        pf = gross_profit / gross_loss if gross_loss != 0 else 999.0
        win_rate = (len(wins) / len(finished_trades)) * 100 if len(finished_trades) > 0 else 0

        # 2. Analiza Korelacji (Co wpływa na wynik?)
        # Sprawdzamy korelację Pearsona między metrykami a wynikiem PnL
        correlations = {}
        if len(finished_trades) > 5: 
            for metric in ['inst_sync', 'retail_herding', 'aqm_score', 'price_gravity']:
                if metric in finished_trades.columns:
                    valid_data = finished_trades[[metric, 'profit_loss']].dropna()
                    if not valid_data.empty:
                        corr = valid_data[metric].corr(valid_data['profit_loss'])
                        correlations[metric] = round(corr, 4) if not np.isnan(corr) else 0

        # 3. Analiza Segmentowa (Institutional Sync)
        # Porównujemy wyniki, gdy Instytucje są z nami vs przeciwko nam
        inst_analysis = {}
        if 'inst_sync' in finished_trades.columns:
            pos_sync = finished_trades[finished_trades['inst_sync'] > 0]
            neg_sync = finished_trades[finished_trades['inst_sync'] <= 0]
            
            inst_analysis = {
                "positive_sync_pf": ApexAudit._calc_pf(pos_sync),
                "positive_sync_trades": len(pos_sync),
                "negative_sync_pf": ApexAudit._calc_pf(neg_sync),
                "negative_sync_trades": len(neg_sync)
            }

        return {
            "summary": {
                "profit_factor": round(pf, 2),
                "win_rate": round(win_rate, 1),
                "total_trades": len(finished_trades),
                "net_profit": round(finished_trades['profit_loss'].sum(), 2),
                "avg_trade": round(finished_trades['profit_loss'].mean(), 2)
            },
            "correlations": correlations,
            "institutional_analysis": inst_analysis
        }

    @staticmethod
    def _calc_pf(sub_df):
        """Metoda pomocnicza do obliczania PF dla podzbioru danych."""
        if sub_df.empty: return 0.0
        wins = sub_df[sub_df['profit_loss'] > 0]['profit_loss'].sum()
        losses = abs(sub_df[sub_df['profit_loss'] <= 0]['profit_loss'].sum())
        return round(wins / losses, 2) if losses != 0 else 999.0


class TemporalAudit:
    """
    Zaawansowana analiza temporalna - wykrywanie dryftu koncepcyjnego 
    i weryfikacja stabilności strategii w czasie (APEX V4).
    """
    
    @staticmethod
    def comprehensive_temporal_analysis(trades_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Kompleksowa analiza jak strategia zachowuje się w czasie.
        """
        if not trades_data:
            return {"error": "Brak danych do analizy temporalnej"}

        df = pd.DataFrame(trades_data)
        
        # Wymagane kolumny
        if 'close_date' not in df.columns or 'profit_loss' not in df.columns:
            return {"error": "Brak kolumn 'close_date' lub 'profit_loss'"}

        # Konwersja daty
        try:
            df['close_date'] = pd.to_datetime(df['close_date'])
        except Exception:
            return {"error": "Błąd parsowania dat"}

        df = df.sort_values('close_date')
        
        analysis = {
            'rolling_performance': TemporalAudit._calculate_rolling_performance(df),
            'monthly_seasonality': TemporalAudit._analyze_seasonality(df)
        }
        
        return analysis
    
    @staticmethod
    def _calculate_rolling_performance(df: pd.DataFrame, window: int = 20) -> Dict[str, Any]:
        """Analiza kroczącego Profit Factor (domyślnie okno 20 transakcji)."""
        if len(df) < window:
            return {"warning": "Za mało danych do analizy kroczącej"}

        rolling_pf = []
        dates = []
        
        for i in range(window, len(df)):
            window_data = df.iloc[i-window:i]
            wins = window_data[window_data['profit_loss'] > 0]['profit_loss'].sum()
            losses = abs(window_data[window_data['profit_loss'] <= 0]['profit_loss'].sum())
            
            pf = wins / losses if losses > 0 else (10.0 if wins > 0 else 0.0)
            rolling_pf.append(round(pf, 2))
            # Zapisujemy datę końca okna
            dates.append(df.iloc[i]['close_date'].isoformat())
        
        return {
            'dates': dates,
            'rolling_pf': rolling_pf,
            'stability_score': round(np.std(rolling_pf), 4) if rolling_pf else 0
        }

    @staticmethod
    def _analyze_seasonality(df: pd.DataFrame) -> Dict[str, float]:
        """Prosta analiza sezonowości miesięcznej."""
        df['month'] = df['close_date'].dt.month
        seasonality = {}
        
        for month in range(1, 13):
            month_data = df[df['month'] == month]
            if not month_data.empty:
                wins = month_data[month_data['profit_loss'] > 0]['profit_loss'].sum()
                losses = abs(month_data[month_data['profit_loss'] <= 0]['profit_loss'].sum())
                pf = wins / losses if losses > 0 else (999.0 if wins > 0 else 0.0)
                seasonality[str(month)] = round(pf, 2)
            else:
                seasonality[str(month)] = 0.0
                
        return seasonality


class SensitivityAnalyzer:
    """
    Analiza wrażliwości i interakcji między parametrami przy użyciu Random Forest (APEX V4).
    Określa, które parametry (np. 'h3_percentile') mają największy wpływ na Profit Factor.
    """
    
    @staticmethod
    def analyze_parameter_sensitivity(trials_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analiza feature importance na podstawie historii optymalizacji.
        trials_data: lista słowników z wynikami prób optuny.
        Oczekiwany format elementu: {'params': {'p1': 0.1, ...}, 'profit_factor': 2.5}
        """
        if not trials_data or len(trials_data) < 10:
            return {"error": "Zbyt mało prób do analizy wrażliwości (wymagane min. 10)."}

        # Przygotowanie danych
        flat_data = []
        for t in trials_data:
            row = t.get('params', {}).copy()
            # Cel analizy: Profit Factor (lub net_profit)
            # Pobieramy 'profit_factor' z głównego słownika
            target = t.get('profit_factor')
            if target is None:
                continue
            row['target_score'] = float(target)
            flat_data.append(row)
            
        df = pd.DataFrame(flat_data)
        
        # Usunięcie wierszy z brakującym targetem lub NaN
        df = df.dropna(subset=['target_score'])
        if df.empty:
            return {"error": "Brak poprawnych danych do treningu (puste df po czyszczeniu)."}

        # Podział na cechy (X) i cel (y)
        X = df.drop('target_score', axis=1)
        y = df['target_score']
        
        # One-hot encoding dla zmiennych kategorycznych (jeśli parametry takie są)
        X = pd.get_dummies(X)
        
        if X.empty:
             return {"error": "Brak zmiennych (parametrów) do analizy."}

        try:
            # Trenowanie Random Forest Regressor
            rf = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=5)
            rf.fit(X, y)
            
            # Wyciąganie ważności cech
            importances = rf.feature_importances_
            feature_names = X.columns
            
            # Sortowanie
            feature_imp_df = pd.DataFrame({
                'feature': feature_names,
                'importance': importances
            }).sort_values('importance', ascending=False)
            
            # Top 10 cech
            top_features = feature_imp_df.head(10).to_dict('records')
            
            # Prosta analiza korelacji (liniowej) dla topowych cech
            # Aby wiedzieć czy parametr wpływa pozytywnie czy negatywnie
            correlations = {}
            for feat in feature_imp_df['feature'].head(5):
                if feat in df.columns:
                    corr = df[feat].corr(df['target_score'])
                    correlations[feat] = round(corr, 4) if not np.isnan(corr) else 0

            return {
                'parameter_importance': top_features,
                'correlations': correlations
            }
            
        except Exception as e:
            logger.error(f"Błąd podczas analizy wrażliwości RF: {e}", exc_info=True)
            return {"error": f"Błąd modelu ML: {str(e)}"}
