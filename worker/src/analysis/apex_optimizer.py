import pandas as pd
import itertools
from tqdm import tqdm

# === KONFIGURACJA ===
CSV_PATH = 'apex_virtual_trades_export_20251121_0621.csv'
TARGET_PF = 2.0
MIN_TRADES = 20  # Minimalna liczba transakcji, aby wynik był statystycznie istotny

# === DEFINICJE ZAKRESÓW OPTYMALIZACJI (GRID SEARCH) ===
# Szukamy parametrów odcinających słabe sygnały
SEARCH_SPACE = {
    # Filtr 1: Koherencja Instytucjonalna (Czy Smart Money jest z nami?)
    'min_inst_sync': [-1.0, -0.5, 0.0, 0.2, 0.5],
    
    # Filtr 2: Herding Detaliczny (Czy ulica jest zbyt euforyczna?)
    # Jeśli herding jest zbyt wysoki, to może być pułapka. Szukamy górnego limitu.
    'max_retail_herding': [10.0, 2.0, 1.5, 1.0], # 10.0 oznacza brak filtru
    
    # Filtr 3: Siła Sygnału H3 (Czy AQM Score jest wystarczająco silny?)
    # Czy warto podnieść poprzeczkę powyżej standardowego progu?
    'min_aqm_score': [-10.0, 0.0, 0.5, 1.0], # Wartości przykładowe, zależne od skali w CSV
    
    # Filtr 4: Grawitacja Cenowa (Czy cena nie odleciała za daleko?)
    # Unikamy łapania spadających noży, jeśli grawitacja jest zbyt silna
    'max_price_gravity_abs': [10.0, 0.3, 0.1, 0.05]
}

def load_data(filepath):
    df = pd.read_csv(filepath)
    # Parsowanie liczb
    cols_to_fix = ['final_profit_loss_percent', 'metric_inst_sync', 
                   'metric_retail_herding', 'metric_aqm_score_h3', 'metric_price_gravity']
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Usuń wiersze bez wyniku (np. otwarte transakcje)
    df = df.dropna(subset=['final_profit_loss_percent'])
    
    # Dodaj kolumnę grawitacji absolutnej dla ułatwienia filtrowania
    if 'metric_price_gravity' in df.columns:
        df['abs_gravity'] = df['metric_price_gravity'].abs()
    else:
        df['abs_gravity'] = 0
        
    return df

def backtest_strategy(df, params):
    """
    Symuluje strategię z nałożonymi filtrami.
    """
    mask = pd.Series([True] * len(df))
    
    # Aplikacja filtrów
    if 'metric_inst_sync' in df.columns:
        mask &= (df['metric_inst_sync'] >= params['min_inst_sync'])
        
    if 'metric_retail_herding' in df.columns:
        mask &= (df['metric_retail_herding'] <= params['max_retail_herding'])
        
    if 'metric_aqm_score_h3' in df.columns:
        mask &= (df['metric_aqm_score_h3'] >= params['min_aqm_score'])
        
    if 'abs_gravity' in df.columns:
        mask &= (df['abs_gravity'] <= params['max_price_gravity_abs'])
        
    filtered_df = df[mask]
    
    if len(filtered_df) == 0:
        return 0.0, 0, 0.0
    
    wins = filtered_df[filtered_df['final_profit_loss_percent'] > 0]['final_profit_loss_percent'].sum()
    losses = abs(filtered_df[filtered_df['final_profit_loss_percent'] <= 0]['final_profit_loss_percent'].sum())
    
    pf = wins / losses if losses != 0 else 999.0 # Infinite PF if no losses
    total_pnl = filtered_df['final_profit_loss_percent'].sum()
    
    return pf, len(filtered_df), total_pnl

def run_optimization():
    print("=== APEX STRATEGY OPTIMIZER: ROZPOCZYNAM SZUKANIE ALPHA ===")
    df = load_data(CSV_PATH)
    
    # Generowanie wszystkich kombinacji parametrów
    keys, values = zip(*SEARCH_SPACE.items())
    param_combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
    
    print(f"Liczba kombinacji do przetestowania: {len(param_combinations)}")
    print(f"Baza transakcji: {len(df)}")
    
    results = []
    
    for params in tqdm(param_combinations):
        pf, num_trades, total_pnl = backtest_strategy(df, params)
        
        if num_trades >= MIN_TRADES:
            result_entry = params.copy()
            result_entry['PF'] = pf
            result_entry['Trades'] = num_trades
            result_entry['Total_PnL'] = total_pnl
            results.append(result_entry)
            
    # Sortowanie wyników wg PF
    results_df = pd.DataFrame(results)
    
    if results_df.empty:
        print("Nie znaleziono kombinacji spełniającej kryterium minimalnej liczby transakcji.")
        return

    best_results = results_df.sort_values(by='PF', ascending=False).head(10)
    
    print("\n=== TOP 10 ZNALEZIONYCH KONFIGURACJI ===")
    print(best_results.to_string(index=False))
    
    # Zapisz najlepszą konfigurację do pliku
    best_config = best_results.iloc[0].to_dict()
    print("\n=== REKOMENDACJA EKSPERTA ===")
    print(f"Aby osiągnąć PF {best_config['PF']:.2f}, zastosuj następujące filtry w kodzie produkcyjnym:")
    for k, v in best_config.items():
        if k not in ['PF', 'Trades', 'Total_PnL']:
            print(f" - {k}: {v}")

if __name__ == "__main__":
    run_optimization()
