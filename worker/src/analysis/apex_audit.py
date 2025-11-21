import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class ApexAudit:
    """
    Moduł diagnostyczny dla APEX AQM V3.
    Analizuje wyniki backtestu pod kątem korelacji kwantowych i wycieków Alpha.
    """

    @staticmethod
    def analyze(trades_data):
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
        # Filtrowanie transakcji zakończonych (z wynikiem innym niż 0 lub None)
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
                    corr = finished_trades[metric].corr(finished_trades['profit_loss'])
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
