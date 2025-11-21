import pandas as pd
import numpy as np

class ApexAudit:
    """
    Moduł diagnostyczny dla APEX AQM V3.
    Analizuje wyniki backtestu pod kątem korelacji kwantowych.
    """

    @staticmethod
    def analyze(trades_data):
        """
        Analizuje listę transakcji (słowniki) i zwraca rozszerzony raport.
        """
        if not trades_data:
            return {"error": "Brak danych do audytu"}

        df = pd.DataFrame(trades_data)
        
        # Upewnij się, że kolumny numeryczne są poprawne
        cols_to_numeric = ['profit_loss', 'inst_sync', 'retail_herding', 'price_gravity', 'aqm_score']
        for col in cols_to_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 1. Podstawowe metryki
        wins = df[df['profit_loss'] > 0]
        losses = df[df['profit_loss'] <= 0]
        
        gross_profit = wins['profit_loss'].sum()
        gross_loss = abs(losses['profit_loss'].sum())
        
        pf = gross_profit / gross_loss if gross_loss != 0 else 999.0
        win_rate = (len(wins) / len(df)) * 100 if len(df) > 0 else 0

        # 2. Analiza Korelacji (Co działa?)
        correlations = {}
        if len(df) > 5: # Wymaga minimum danych
            for metric in ['inst_sync', 'retail_herding', 'aqm_score']:
                if metric in df.columns:
                    correlations[metric] = df[metric].corr(df['profit_loss'])

        # 3. Segmentacja Instytucjonalna (Inst Sync > 0 vs < 0)
        inst_analysis = {}
        if 'inst_sync' in df.columns:
            positive_inst = df[df['inst_sync'] > 0]
            negative_inst = df[df['inst_sync'] <= 0]
            
            inst_analysis = {
                "positive_sync_pf": ApexAudit._calculate_pf(positive_inst),
                "positive_sync_count": len(positive_inst),
                "negative_sync_pf": ApexAudit._calculate_pf(negative_inst),
                "negative_sync_count": len(negative_inst)
            }

        return {
            "summary": {
                "profit_factor": round(pf, 2),
                "win_rate": round(win_rate, 1),
                "total_trades": len(df),
                "net_profit": round(df['profit_loss'].sum(), 2)
            },
            "correlations": {k: round(v, 2) for k, v in correlations.items() if not np.isnan(v)},
            "institutional_analysis": inst_analysis
        }

    @staticmethod
    def _calculate_pf(sub_df):
        if sub_df.empty: return 0.0
        wins = sub_df[sub_df['profit_loss'] > 0]['profit_loss'].sum()
        losses = abs(sub_df[sub_df['profit_loss'] <= 0]['profit_loss'].sum())
        return round(wins / losses, 2) if losses != 0 else 999.0
