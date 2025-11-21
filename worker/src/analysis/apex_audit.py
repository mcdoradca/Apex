import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate

# Konfiguracja
CSV_PATH = 'apex_virtual_trades_export_20251121_0621.csv'

def load_data(filepath):
    """Ładuje i wstępnie przetwarza dane z backtestu."""
    try:
        df = pd.read_csv(filepath)
        # Konwersja dat
        df['open_date'] = pd.to_datetime(df['open_date'])
        df['close_date'] = pd.to_datetime(df['close_date'])
        
        # Upewnienie się, że kolumny numeryczne są float
        numeric_cols = [col for col in df.columns if col.startswith('metric_') or col in ['final_profit_loss_percent']]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df
    except Exception as e:
        print(f"Błąd ładowania danych: {e}")
        return None

def calculate_metrics(df):
    """Oblicza kluczowe metryki tradingowe."""
    if df.empty:
        return {}
    
    wins = df[df['final_profit_loss_percent'] > 0]
    losses = df[df['final_profit_loss_percent'] <= 0]
    
    gross_profit = wins['final_profit_loss_percent'].sum()
    gross_loss = abs(losses['final_profit_loss_percent'].sum())
    
    pf = gross_profit / gross_loss if gross_loss != 0 else 0
    win_rate = len(wins) / len(df) * 100
    avg_win = wins['final_profit_loss_percent'].mean()
    avg_loss = losses['final_profit_loss_percent'].mean()
    
    # Expectancy (Oczekiwana wartość na transakcję)
    expectancy = (avg_win * (win_rate/100)) - (abs(avg_loss) * (1 - win_rate/100))
    
    return {
        "Total Trades": len(df),
        "Profit Factor": pf,
        "Win Rate": win_rate,
        "Avg Win %": avg_win,
        "Avg Loss %": avg_loss,
        "Expectancy": expectancy,
        "Net Result %": df['final_profit_loss_percent'].sum()
    }

def analyze_correlations(df):
    """Analizuje, które metryki korelują z zyskiem."""
    target = 'final_profit_loss_percent'
    metrics = [col for col in df.columns if col.startswith('metric_')]
    
    correlations = []
    for metric in metrics:
        if metric in df.columns:
            corr = df[metric].corr(df[target])
            correlations.append((metric, corr))
            
    return sorted(correlations, key=lambda x: abs(x[1]), reverse=True)

def main():
    print("=== APEX QUANTUM AUDIT - ROZPOCZĘCIE ===")
    df = load_data(CSV_PATH)
    
    if df is None:
        return

    # 1. Globalne Wyniki
    metrics = calculate_metrics(df)
    print("\n--- WYNIKI BAZOWE (OBECNA STRATEGIA) ---")
    print(tabulate(metrics.items(), headers=["Metryka", "Wartość"], tablefmt="grid"))

    # 2. Analiza wg Typu Setupu
    print("\n--- WYNIKI WG TYPU SETUPU ---")
    setup_groups = df.groupby('setup_type').apply(calculate_metrics)
    # Przekształcenie do czytelnej tabeli
    setup_table = []
    for setup, stats in setup_groups.items():
        row = [setup, stats.get('Total Trades'), f"{stats.get('Profit Factor', 0):.2f}", f"{stats.get('Win Rate', 0):.1f}%"]
        setup_table.append(row)
    print(tabulate(setup_table, headers=["Setup", "Liczba", "PF", "WR"], tablefmt="simple"))

    # 3. Analiza Korelacji (Co wpływa na wynik?)
    print("\n--- ANALIZA CZYNNIKÓW WPŁYWU (KORELACJE Z PnL) ---")
    corrs = analyze_correlations(df)
    print(tabulate(corrs[:5], headers=["Metryka AQM", "Korelacja Pearsona"], tablefmt="simple"))
    
    # 4. Segmentacja - Inst Sync
    print("\n--- HIPOTEZA: FILTR INSTYTUCJONALNY ---")
    # Sprawdźmy transakcje, gdzie inst_sync było dodatnie vs ujemne
    if 'metric_inst_sync' in df.columns:
        df_inst_pos = df[df['metric_inst_sync'] > 0]
        df_inst_neg = df[df['metric_inst_sync'] <= 0]
        
        pf_pos = calculate_metrics(df_inst_pos).get('Profit Factor', 0)
        pf_neg = calculate_metrics(df_inst_neg).get('Profit Factor', 0)
        
        print(f"PF gdy Instytucje Zgodne (>0): {pf_pos:.2f} (Liczba transakcji: {len(df_inst_pos)})")
        print(f"PF gdy Instytucje Przeciwne (<=0): {pf_neg:.2f} (Liczba transakcji: {len(df_inst_neg)})")
    
    print("\n=== KONIEC AUDYTU ===")

if __name__ == "__main__":
    main()
