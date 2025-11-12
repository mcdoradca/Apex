import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient

# Krok 18: Importujemy kalkulatory metryk z Krok 17 i 15
from . import aqm_v3_metrics
from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    get_current_NY_datetime,
    append_scan_log,
    update_scan_progress,
    # Importujemy calculate_atr, aby móc go użyć w pętli cache
    calculate_atr
)
from .. import models
from ..config import SECTOR_TO_ETF_MAP

logger = logging.getLogger(__name__)

# ==================================================================
# === Silnik Symulacji (Logika LONG) ===
# TA FUNKCJA ZOSTAJE. Jest generyczna i będzie potrzebna do testowania hipotez V3.
# ==================================================================

def _resolve_trade(historical_data: pd.DataFrame, entry_index: int, setup: Dict[str, Any], max_hold_days: int, year: str, direction: str) -> models.VirtualTrade | None:
    """
    "Spogląda w przyszłość" (w danych historycznych), aby zobaczyć, jak
    dana transakcja by się zakończyła.
    
    ZMIANA (Specyfikacja H1): Ta funkcja jest teraz zgodna z wymogami H1.
    Wejście (D+1 OPEN) i Wyjście (D+5 CLOSE) jest obsługiwane przez _simulate_trades_h1.
    _resolve_trade obsługuje tylko egzekucję SL i TP w dniach D+1 do D+5.
    """
    try:
        # Pobieramy parametry ze specyfikacji H1
        entry_price = setup['entry_price'] # OPEN(D+1)
        stop_loss = setup['stop_loss']     # OPEN(D+1) - (2 * ATR(D))
        take_profit = setup['take_profit'] # VWAP(D)
        
        close_price = entry_price # Domyślna cena zamknięcia
        status = 'CLOSED_EXPIRED' # Domyślny status
        
        # Znajdź indeks świecy D+1 (czyli 'entry_index' w pełnym DataFrame)
        # Pętla musi zacząć sprawdzać SL/TP od dnia D+1 (włącznie)
        
        # +1, ponieważ specyfikacja mówi o 5 dniach *po* wejściu (D+1 do D+5)
        # Dzień 1 = entry_index (D+1)
        # Dzień 5 = entry_index + 4 (D+5)
        for i in range(0, max_hold_days): 
            current_day_index = entry_index + i
            
            if current_day_index >= len(historical_data):
                # Transakcja doszła do końca danych historycznych
                candle = historical_data.iloc[-1]
                close_price = candle['close']
                status = 'CLOSED_EXPIRED'
                break
            
            candle = historical_data.iloc[current_day_index]
            day_low = candle['low']
            day_high = candle['high']

            if direction == 'LONG':
                # === Logika H1 (Mean Reversion) ===
                
                # Warunek 1: Czy SL został trafiony?
                # Sprawdzamy LOW dnia (nawet w dniu wejścia D+1)
                if day_low <= stop_loss:
                    close_price = stop_loss
                    status = 'CLOSED_SL'
                    break
                    
                # Warunek 2: Czy TP został trafiony?
                # Sprawdzamy HIGH dnia
                if day_high >= take_profit:
                    close_price = take_profit
                    status = 'CLOSED_TP'
                    break
            
            # (Pomijamy logikę SHORT, H1 jest tylko LONG)

        else:
            # === Warunek 3: Wyjście Czasowe (Max Hold) ===
            # Jeśli pętla zakończyła się normalnie (bez break),
            # zamykamy po cenie CLOSE dnia D+5.
            
            # Indeks D+5 to entry_index + max_hold_days - 1
            final_index = min(entry_index + max_hold_days - 1, len(historical_data) - 1)
            candle = historical_data.iloc[final_index]
            close_price = candle['close']
            status = 'CLOSED_EXPIRED'

        # Obliczanie P/L
        if entry_price == 0:
            p_l_percent = 0.0
        else:
            p_l_percent = ((close_price - entry_price) / entry_price) * 100
        
        
        trade = models.VirtualTrade(
            ticker=setup['ticker'],
            status=status,
            setup_type=f"BACKTEST_{year}_{setup['setup_type']}",
            entry_price=float(entry_price),
            stop_loss=float(stop_loss),
            take_profit=float(take_profit),
            open_date=historical_data.index[entry_index].to_pydatetime(), # Data znalezienia setupu
            close_date=candle.name.to_pydatetime(), # Data zamknięcia
            close_price=float(close_price),
            final_profit_loss_percent=float(p_l_percent)
        )
        
        return trade

    except Exception as e:
        logger.error(f"[Backtest] Błąd podczas rozwiązywania transakcji dla {setup.get('ticker')}: {e}", exc_info=True)
        return None

# ==================================================================
# === SŁOWNIK CACHE (BEZ ZMIAN) ===
# ==================================================================

_backtest_cache = {
    "vix_data": None, 
    "spy_data": None, 
    "sector_etf_data": {}, 
    "company_data": {}, 
    "tickers_by_sector": {}, 
    "sector_map": {} 
}

def _get_ticker_data_from_cache(ticker: str) -> Optional[Dict[str, Any]]:
    """Pobiera dane spółki (dzienne, tygodniowe, vwap, sektor) z cache."""
    return _backtest_cache["company_data"].get(ticker)

def _get_tickers_in_sector(sector: str) -> List[str]:
    """Pobiera listę tickerów dla danego sektora z cache."""
    return _backtest_cache["tickers_by_sector"].get(sector, [])

def _get_sector_for_ticker(session: Session, ticker: str) -> str:
    """Pobiera sektor dla tickera z bazy danych (z cache)."""
    if ticker not in _backtest_cache["sector_map"]:
        try:
            sector = session.execute(
                text("SELECT sector FROM companies WHERE ticker = :ticker"),
                {'ticker': ticker}
            ).scalar()
            _backtest_cache["sector_map"][ticker] = sector or "N/A"
        except Exception as e:
            logger.error(f"Nie udało się pobrać sektora dla {ticker}: {e}")
            _backtest_cache["sector_map"][ticker] = "N/A"
            
    return _backtest_cache["sector_map"][ticker]
# ==================================================================


# ==================================================================
# === DETEKTOR REŻIMU RYNKOWEGO (BEZ ZMIAN) ===
# ==================================================================
def _detect_market_regime(current_date_str: str) -> str:
    """
    Wykrywa reżim rynkowy na podstawie danych VXX i SPY z cache.
    """
    try:
        vix_df = _backtest_cache["vix_data"]
        spy_df = _backtest_cache["spy_data"]
        
        vix_row = vix_df[vix_df.index <= current_date_str].iloc[-1]
        spy_row = spy_df[spy_df.index <= current_date_str].iloc[-1]
        
        vix_price = vix_row['close']
        spy_price = spy_row['close']
        spy_sma_50 = spy_row['ema_50']
        spy_sma_200 = spy_row['ema_200']

        if pd.isna(vix_price) or pd.isna(spy_price) or pd.isna(spy_sma_50) or pd.isna(spy_sma_200):
            return 'volatile' 

        if vix_price < 18 and spy_price > spy_sma_200:
            return 'bull'
        elif vix_price > 25 or spy_price < spy_sma_50: 
            return 'bear'
        else:
            return 'volatile'
            
    except IndexError:
        return 'volatile'
    except Exception as e:
        logger.error(f"Błąd podczas wykrywania reżimu dla {current_date_str}: {e}")
        return 'volatile'

# ==================================================================
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (ZAKTUALIZOWANA) ===
# ==================================================================

def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny dla
    zdefiniowanego okresu i listy tickerów.
    """
    
    try:
        if not (year.isdigit() and len(year) == 4):
            raise ValueError(f"Otrzymano nieprawidłowy rok: {year}")
        
        current_year = datetime.now(timezone.utc).year
        if int(year) > current_year:
             raise ValueError(f"Nie można testować przyszłości: {year}")
        
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
    except Exception as e:
        logger.error(f"[Backtest] Błąd walidacji roku: {e}", exc_info=True)
        append_scan_log(session, f"[Backtest] BŁĄD: Nieprawidłowy format roku: {year}")
        return
        
    log_msg = f"BACKTEST HISTORYCZNY (Platforma AQM V3): Rozpoczynanie testu dla roku '{year}' ({start_date} do {end_date})"
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # === KROK 1: Czyszczenie Bazy Danych ===
    try:
        like_pattern = f"BACKTEST_{year}_AQM_V3_%"
        logger.info(f"Czyszczenie starych wyników AQM V3 dla wzorca: {like_pattern}...")
        
        delete_stmt = text("DELETE FROM virtual_trades WHERE setup_type LIKE :pattern")
        result = session.execute(delete_stmt, {'pattern': like_pattern})
        
        session.commit()
        logger.info(f"Pomyślnie usunięto {result.rowcount} starych wpisów backtestu AQM V3 dla roku {year}.")
        
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()

    # === KROK 2: Pobieranie Listy Spółek ===
    try:
        log_msg_tickers = "[Backtest V3] Pobieranie listy spółek z Fazy 1 ('Pierwsze Sito')..."
        logger.info(log_msg_tickers)
        append_scan_log(session, log_msg_tickers)
        
        tickers_p2_rows = session.execute(text(
            "SELECT DISTINCT ticker FROM phase1_candidates"
        )).fetchall()
        
        tickers_to_test = sorted([row[0] for row in tickers_p2_rows])

        if not tickers_to_test:
            log_msg = f"[Backtest] BŁĄD: Tabela 'phase1_candidates' jest pusta. Uruchom najpierw główny skan (przycisk 'Start'), aby zapełnić tę listę."
            logger.error(log_msg)
            append_scan_log(session, log_msg)
            return

        log_msg = f"[Backtest V3] Znaleziono {len(tickers_to_test)} spółek z Fazy 1 do przetestowania."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów: {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 3: Budowanie Pamięci Podręcznej (Cache) ===
    
    try:
        logger.info("[Backtest V3] Rozpoczynanie budowania pamięci podręcznej (Cache)...")
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["company_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}
        _backtest_cache["sector_map"] = {}

        # 2. Pobierz dane Makro (VXX i SPY)
        logger.info("[Backtest V3] Cache: Ładowanie VXX i SPY...")
        vix_raw = api_client.get_daily_adjusted('VXX', outputsize='full')
        vix_df = pd.DataFrame.from_dict(vix_raw['Time Series (Daily)'], orient='index')
        vix_df = standardize_df_columns(vix_df)
        vix_df.index = pd.to_datetime(vix_df.index)
        _backtest_cache["vix_data"] = vix_df
        
        spy_raw = api_client.get_daily_adjusted('SPY', outputsize='full')
        spy_df = pd.DataFrame.from_dict(spy_raw['Time Series (Daily)'], orient='index')
        spy_df = standardize_df_columns(spy_df)
        spy_df.index = pd.to_datetime(spy_df.index)
        spy_df['ema_50'] = calculate_ema(spy_df['close'], period=50)
        spy_df['ema_200'] = calculate_ema(spy_df['close'], period=200)
        _backtest_cache["spy_data"] = spy_df
        
        # 3. Pobierz dane Sektorowe (ETF-y)
        logger.info("[Backtest V3] Cache: Ładowanie 11 sektorów ETF...")
        for sector_name, etf_ticker in SECTOR_TO_ETF_MAP.items():
            try:
                sector_raw = api_client.get_daily_adjusted(etf_ticker, outputsize='full')
                sector_df = pd.DataFrame.from_dict(sector_raw['Time Series (Daily)'], orient='index')
                sector_df = standardize_df_columns(sector_df)
                sector_df.index = pd.to_datetime(sector_df.index)
                _backtest_cache["sector_etf_data"][etf_ticker] = sector_df
            except Exception as e:
                logger.error(f"  > BŁĄD ładowania danych dla sektora {etf_ticker}: {e}")
        
        # 4. Pobierz dane dla wszystkich spółek
        logger.info(f"[Backtest V3] Cache: Ładowanie danych i wstępne obliczanie metryk V3...")
        total_cache_build = len(tickers_to_test)
        for i, ticker in enumerate(tickers_to_test):
            if i % 10 == 0:
                log_msg = f"[Backtest V3] Budowanie cache... ({i}/{total_cache_build})"
                append_scan_log(session, log_msg)
                update_scan_progress(session, i, total_cache_build)
            
            try:
                # ==================================================================
                # === KROK 18: Wstępne obliczanie metryk H1 ===
                # ==================================================================
                
                # Pobierz dane dzienne (Wywołanie 1)
                price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
                # Pobierz dane tygodniowe (Wywołanie 2)
                weekly_data_raw = api_client.get_time_series_weekly(ticker)
                # Pobierz dane VWAP (Wywołanie 3)
                vwap_data_raw = api_client.get_vwap(ticker, interval='daily')
                
                if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
                   not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw or \
                   not vwap_data_raw or 'Technical Analysis: VWAP' not in vwap_data_raw:
                    logger.warning(f"Brak pełnych danych (Daily, Weekly lub VWAP) dla {ticker}, pomijanie.")
                    continue

                # Przetwórz Daily
                daily_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
                daily_df = standardize_df_columns(daily_df)
                daily_df.index = pd.to_datetime(daily_df.index)
                
                # Przetwórz Weekly
                weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
                weekly_df = standardize_df_columns(weekly_df)
                weekly_df.index = pd.to_datetime(weekly_df.index)

                # Przetwórz VWAP
                vwap_df = pd.DataFrame.from_dict(vwap_data_raw['Technical Analysis: VWAP'], orient='index')
                vwap_df.index = pd.to_datetime(vwap_df.index)
                vwap_df['VWAP'] = pd.to_numeric(vwap_df['VWAP'], errors='coerce')
                vwap_df.sort_index(inplace=True)
                
                # --- Wzbogacanie DataFrame (Krok 18) ---
                
                # 1. Dołącz dane SPY i VWAP do `daily_df`
                # Używamy `reindex` i `ffill`, aby dopasować daty i wypełnić weekendy/święta
                spy_aligned = _backtest_cache["spy_data"]['close'].reindex(daily_df.index, method='ffill').rename('spy_close')
                vwap_aligned = vwap_df['VWAP'].reindex(daily_df.index, method='ffill').rename('vwap')
                
                enriched_df = daily_df.join(spy_aligned).join(vwap_aligned)
                
                # 2. Oblicz ATR (potrzebne do SL)
                enriched_df['atr_14'] = calculate_atr(enriched_df, period=14)
                
                # 3. Oblicz Metryki H1 (Time Dilation & Price Gravity)
                # Używamy .rolling().apply() do stworzenia historycznej serii metryk
                
                # Przygotowanie danych do .apply()
                temp_spy_view = enriched_df[['spy_close']].rename(columns={'spy_close': 'close'})
                
                # Obliczanie Time Dilation (Wymiar 1.1)
                # Tworzymy 20-dniowe okno kroczące dla obu serii
                ticker_returns_rolling = enriched_df['close'].pct_change().rolling(window=20)
                spy_returns_rolling = enriched_df['spy_close'].pct_change().rolling(window=20)
                
                # Oblicz odchylenia standardowe
                std_ticker = ticker_returns_rolling.std()
                std_spy = spy_returns_rolling.std()
                
                # time_dilation = std(ticker) / std(spy)
                enriched_df['time_dilation'] = std_ticker / std_spy
                
                # Obliczanie Price Gravity (Wymiar 1.2)
                # price_gravity = (vwap - close) / close
                enriched_df['price_gravity'] = (enriched_df['vwap'] - enriched_df['close']) / enriched_df['close']
                
                # Zastąp nieskończone wartości (wynikające z dzielenia przez 0) NaN, a następnie 0
                enriched_df.replace([np.inf, -np.inf], np.nan, inplace=True)
                enriched_df['time_dilation'].fillna(0, inplace=True)
                enriched_df['price_gravity'].fillna(0, inplace=True)
                
                # --- Koniec Wzbogacania ---
                
                # Zdobądź i zapisz sektor
                sector = _get_sector_for_ticker(session, ticker)
                
                # Zapisz wzbogacone dane w cache
                _backtest_cache["company_data"][ticker] = {
                    "daily": enriched_df, # <-- ZAPISUJEMY WZBOGACONY DF
                    "weekly": weekly_df,
                    "vwap": vwap_df, 
                    "sector": sector
                }
                
                if sector not in _backtest_cache["tickers_by_sector"]:
                    _backtest_cache["tickers_by_sector"][sector] = []
                _backtest_cache["tickers_by_sector"][sector].append(ticker)

            except Exception as e:
                logger.error(f"[Backtest V3] Błąd budowania cache dla {ticker}: {e}", exc_info=True)
                
        logger.info("[Backtest V3] Budowanie pamięci podręcznej (Cache) zakończone.")
        append_scan_log(session, "[Backtest V3] Budowanie pamięci podręcznej (Cache) zakończone.")
        update_scan_progress(session, total_cache_build, total_cache_build)

    except Exception as e:
        log_msg = f"[Backtest V3] BŁĄD KRYTYCZNY podczas budowania cache: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # ==================================================================
    # === KROK 4: Uruchomienie Symulacji (TERAZ PUSTE) ===
    # ==================================================================
    
    log_msg_aqm = "[Backtest V3] Szkielet silnika gotowy. Wstępne obliczenia H1 zakończone. Oczekiwanie na implementację pętli symulacji H1."
    logger.info(log_msg_aqm)
    append_scan_log(session, log_msg_aqm)
            
    # === KONIEC SYMULACJI ===
    total_trades_found = 0 
    total_tickers = len(tickers_to_test)
    update_scan_progress(session, total_tickers, total_tickers) # Ustaw na 100%
    log_msg_final = f"BACKTEST HISTORYCZNY (AQM V3): Przebudowa platformy zakończona dla roku '{year}'. Gotowy do testowania hipotez."
    logger.info(log_msg_final)
    append_scan_log(session, log_msg_final)
