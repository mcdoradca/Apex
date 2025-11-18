import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from .. import models
from .utils import update_system_control, append_scan_log

logger = logging.getLogger(__name__)

def extract_strategy_hypothesis(setup_type_str):
    """
    Wyciąga identyfikator hipotezy (np. 'H1', 'H2') z kolumny setup_type.
    """
    if 'H1' in str(setup_type_str):
        return 'H1'
    if 'H2' in str(setup_type_str):
        return 'H2'
    if 'H3' in str(setup_type_str):
        return 'H3'
    if 'H4' in str(setup_type_str):
        return 'H4'
    return 'Unknown'

def run_h3_deep_dive_analysis(session: Session, weak_year_to_analyze: int):
    """
    Głęboka analiza metryk strategii H3 bezpośrednio z bazy danych,
    aby zrozumieć różnice w wynikach rocznych.
    """
    log_msg = f"DEEP DIVE (H3): Rozpoczynanie analizy dla 'słabego roku': {weak_year_to_analyze}"
    logger.info(log_msg)
    append_scan_log(session, log_msg)
    
    # Lista do zbierania wyników zamiast print()
    report_lines = []
    
    try:
        # --- 1. Wczytanie danych H3 bezpośrednio z bazy ---
        query = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.setup_type.like('%H3%'),
            models.VirtualTrade.status != 'OPEN'
        )
        
        df_h3 = pd.read_sql(query.statement, session.bind)
        logger.info(f"Wczytano {len(df_h3)} zamkniętych transakcji H3 z bazy danych.")

        if df_h3.empty:
            logger.warning("Nie znaleziono żadnych transakcji H3 do analizy.")
            append_scan_log(session, "DEEP DIVE (H3): Nie znaleziono transakcji H3.")
            update_system_control(session, 'h3_deep_dive_report', 'BŁĄD: Nie znaleziono żadnych zamkniętych transakcji H3 do analizy.')
            return

        # --- 2. Czyszczenie i przygotowanie danych (logika ze skryptu) ---
        df_h3['final_profit_loss_percent'] = pd.to_numeric(df_h3['final_profit_loss_percent'], errors='coerce')
        df_h3['open_date'] = pd.to_datetime(df_h3['open_date'], errors='coerce', utc=True)
        df_h3['year'] = df_h3['open_date'].dt.year
        
        # (Nie potrzebujemy 'strategy', bo już filtrowaliśmy)
        
        df_h3.dropna(subset=['final_profit_loss_percent', 'year'], inplace=True)
        
        logger.info(f"Znaleziono {len(df_h3)} poprawnych transakcji H3 po czyszczeniu.")
        report_lines.append(f"Analiza {len(df_h3)} transakcji H3.\n")

        # --- 3. Krok 1: Roczny Profil Rentowności (logika ze skryptu) ---
        report_lines.append("--- Krok 1: Roczny Profil Rentowności dla H3 ---")
        
        df_h3['year'] = df_h3['year'].astype(int)
        
        annual_summary = df_h3.groupby('year')['final_profit_loss_percent'].agg(
            total_trades='count',
            mean_pnl_percent='mean',
            sum_pnl_percent='sum',
            win_rate=lambda x: (x > 0).mean() * 100
        ).sort_values(by='year', ascending=False)
        
        report_lines.append(annual_summary.to_string(float_format="%.2f"))
        
        # --- 4. Krok 2: Analiza Porażek (logika ze skryptu) ---
        report_lines.append(f"\n\n--- Krok 2: Analiza Porażek dla roku {weak_year_to_analyze} ---")
        
        df_weak_year = df_h3[df_h3['year'] == weak_year_to_analyze]
        
        if df_weak_year.empty:
            report_lines.append(f"Brak danych dla roku {weak_year_to_analyze}. Zmień rok i spróbuj ponownie.")
            logger.warning(f"Brak danych dla roku {weak_year_to_analyze}.")
        else:
            df_sl_trades = df_weak_year[df_weak_year['status'] == 'CLOSED_SL'].copy() # Dodajemy .copy(), aby jawnie stworzyć kopię
            
            if df_sl_trades.empty:
                report_lines.append(f"W roku {weak_year_to_analyze} nie znaleziono żadnych transakcji 'CLOSED_SL'.")
            else:
                report_lines.append(f"Znaleziono {len(df_sl_trades)} transakcji H3 'CLOSED_SL' w {weak_year_to_analyze}.")
                
                h3_metrics = [
                    'metric_aqm_score_h3', 'metric_J_norm', 
                    'metric_nabla_sq_norm', 'metric_m_sq_norm'
                ]
                
                valid_metrics = []
                for metric in h3_metrics:
                    if metric in df_sl_trades.columns:
                        # ==========================================================
                        # === POPRAWKA 1 (Linia 102) ===
                        # Używamy .loc, aby jawnie zmodyfikować DataFrame
                        # ==========================================================
                        df_sl_trades.loc[:, metric] = pd.to_numeric(df_sl_trades[metric], errors='coerce')
                        valid_metrics.append(metric)
                    else:
                        logger.warning(f"Ostrzeżenie: Brak kolumny '{metric}' w bazie danych.")
                
                # ==========================================================
                # === POPRAWKA 2 (Linia 107) ===
                # Unikamy 'inplace=True' na kopii i przypisujemy wynik z powrotem
                # ==========================================================
                df_sl_trades = df_sl_trades.dropna(subset=valid_metrics)

                if df_sl_trades.empty:
                    report_lines.append("Brak pełnych danych metryk dla transakcji SL.")
                else:
                    report_lines.append("\nŚrednie wartości metryk dla transakcji 'CLOSED_SL':")
                    report_lines.append(df_sl_trades[valid_metrics].mean().to_string(float_format="%.3f"))
                    
                    report_lines.append(f"\nŚrednie wartości metryk dla WSZYSTKICH transakcji w {weak_year_to_analyze}:")
                    
                    # Poprawiamy również ten blok, aby uniknąć przyszłych ostrzeżeń
                    df_all_trades_year = df_weak_year.copy()
                    for metric in valid_metrics:
                        df_all_trades_year.loc[:, metric] = pd.to_numeric(df_all_trades_year[metric], errors='coerce')
                    df_all_trades_year = df_all_trades_year.dropna(subset=valid_metrics)
                    
                    report_lines.append(df_all_trades_year[valid_metrics].mean().to_string(float_format="%.3f"))

        # --- 5. Krok 3: Analiza Parametrów Wejściowych (logika ze skryptu) ---
        report_lines.append("\n\n--- Krok 3: Analiza korelacji AQM Score z P/L ---")
        
        h3_metrics_with_pnl = ['final_profit_loss_percent', 'metric_aqm_score_h3']
        
        # Poprawiamy również ten blok
        df_h3_corr = df_h3[h3_metrics_with_pnl].copy()
        for col in h3_metrics_with_pnl:
             df_h3_corr.loc[:, col] = pd.to_numeric(df_h3_corr[col], errors='coerce')
        df_h3_corr = df_h3_corr.dropna()

        if not df_h3_corr.empty:
            correlation = df_h3_corr['metric_aqm_score_h3'].corr(df_h3_corr['final_profit_loss_percent'])
            report_lines.append(f"Ogólna korelacja P/L z 'metric_aqm_score_h3': {correlation:.4f}")
            
            report_lines.append("\nŚredni AQM Score dla transakcji ZYSKOWNYCH vs STRATNYCH:")
            avg_score_win = df_h3_corr[df_h3_corr['final_profit_loss_percent'] > 0]['metric_aqm_score_h3'].mean()
            avg_score_loss = df_h3_corr[df_h3_corr['final_profit_loss_percent'] <= 0]['metric_aqm_score_h3'].mean()
            
            report_lines.append(f"  Śr. AQM Score (Zysk):  {avg_score_win:.3f}")
            report_lines.append(f"  Śr. AQM Score (Strata): {avg_score_loss:.3f}")
            
            if avg_score_win > avg_score_loss:
                report_lines.append("\nWniosek: Wyższy AQM Score statystycznie koreluje z wyższym P/L. Próg 95% ma sens.")
            else:
                report_lines.append("\nWniosek: Wyższy AQM Score NIE koreluje z P/L. To jest problem do zbadania.")

        # --- 6. Zapisanie raportu ---
        final_report = "\n".join(report_lines)
        update_system_control(session, 'h3_deep_dive_report', final_report)
        
        log_msg_end = "DEEP DIVE (H3): Analiza zakończona. Raport zapisany w system_control."
        logger.info(log_msg_end)
        append_scan_log(session, log_msg_end)

    except Exception as e:
        log_msg_err = f"DEEP DIVE (H3) BŁĄD KRYTYCZNY: {e}"
        logger.error(log_msg_err, exc_info=True)
        append_scan_log(session, log_msg_err)
        update_system_control(session, 'h3_deep_dive_report', f"BŁĄD: {e}")
