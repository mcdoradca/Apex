import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
import numpy as np
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import (
    standardize_df_columns, 
    calculate_ema, 
    get_current_NY_datetime,
    append_scan_log,
    update_scan_progress,
    # Importy wskaźników są zachowane na potrzeby _detect_market_regime i przyszłych obliczeń
    calculate_rsi, 
    calculate_macd,
    calculate_atr,
    calculate_obv,
    calculate_ad
)
from .. import models
# Importujemy SECTOR_TO_ETF_MAP (nadal potrzebne do cache)
from ..config import SECTOR_TO_ETF_MAP

logger = logging.getLogger(__name__)

# ==================================================================
# === DEKONSTRUKCJA (KROK 13) ===
# Usunięto stare, proste definicje AQM_THRESHOLDS_MINIMAL i AQM_STRATEGY_PARAMS.
# Zostaną one zastąpione przez logikę AQM V3 (Hipotezy H1-H4).
# ==================================================================


# ==================================================================
# === Silnik Symulacji (Logika LONG) ===
# TA FUNKCJA ZOSTAJE. Jest generyczna i będzie potrzebna do testowania hipotez V3.
# ==================================================================

def _resolve_trade(historical_data: pd.DataFrame, entry_index: int, setup: Dict[str, Any], max_hold_days: int, year: str, direction: str) -> models.VirtualTrade | None:
    """
    "Spogląda w przyszłość" (w danych historycznych), aby zobaczyć, jak
    dana transakcja by się zakończyła.
    
    UWAGA: Na razie obsługuje tylko transakcje 'LONG', ponieważ strategia AQM
    zgodnie z PDF jest strategią typu long-only.
    """
    try:
        entry_price = setup['entry_price']
        stop_loss = setup['stop_loss']
        take_profit = setup['take_profit']
        
        close_price = entry_price # Domyślna cena zamknięcia, jeśli nic się nie stanie
        status = 'CLOSED_EXPIRED' # Domyślny status
        candle = historical_data.iloc[entry_index] # Domyślna świeca (na wypadek błędu)

        if direction == 'LONG':
            # === LOGIKA DLA POZYCJI DŁUGIEJ (LONG) ===
            for i in range(1, max_hold_days + 1):
                if entry_index + i >= len(historical_data):
                    # Transakcja doszła do końca danych historycznych
                    candle = historical_data.iloc[-1]
                    close_price = candle['close']
                    status = 'CLOSED_EXPIRED'
                    break
                
                candle = historical_data.iloc[entry_index + i]
                day_low = candle['low']
                day_high = candle['high']

                # Sprawdź SL: Jeśli minimum dnia jest niższe niż SL, zamykamy po cenie SL
                if day_low <= stop_loss:
                    close_price = stop_loss
                    status = 'CLOSED_SL'
                    break
                    
                # Sprawdź TP: Jeśli maksimum dnia jest wyższe niż TP, zamykamy po cenie TP
                if day_high >= take_profit:
                    close_price = take_profit
                    status = 'CLOSED_TP'
                    break
            else:
                # Jeśli pętla zakończyła się normalnie (nie przez break),
                # oznacza to, że ani SL, ani TP nie zostały trafione w 'max_hold_days'.
                # Zamykamy pozycję po cenie zamknięcia ostatniego dnia.
                final_index = min(entry_index + max_hold_days, len(historical_data) - 1)
                candle = historical_data.iloc[final_index]
                close_price = candle['close']
                status = 'CLOSED_EXPIRED'

            # Unikaj dzielenia przez zero, jeśli cena wejścia była 0
            if entry_price == 0:
                p_l_percent = 0.0
            else:
                p_l_percent = ((close_price - entry_price) / entry_price) * 100
        
        elif direction == 'SHORT':
            # === LOGIKA DLA POZYCJI KRÓTKIEJ (SHORT) ===
            # Ta logika jest zachowana, ale obecnie nieużywana przez strategię AQM.
            for i in range(1, max_hold_days + 1):
                if entry_index + i >= len(historical_data):
                    candle = historical_data.iloc[-1]
                    close_price = candle['close']
                    status = 'CLOSED_EXPIRED'
                    break
                
                candle = historical_data.iloc[entry_index + i]
                day_low = candle['low']
                day_high = candle['high']

                # Dla SHORT, SL jest powyżej ceny wejścia
                if day_high >= stop_loss:
                    close_price = stop_loss
                    status = 'CLOSED_SL'
                    break
                
                # Dla SHORT, TP jest poniżej ceny wejścia
                if day_low <= take_profit:
                    close_price = take_profit
                    status = 'CLOSED_TP'
                    break
            else:
                final_index = min(entry_index + max_hold_days, len(historical_data) - 1)
                candle = historical_data.iloc[final_index]
                close_price = candle['close']
                status = 'CLOSED_EXPIRED'

            # P/L dla SHORT jest odwrotny
            if entry_price == 0:
                p_l_percent = 0.0
            else:
                p_l_percent = ((entry_price - close_price) / entry_price) * 100
        
        else:
            logger.error(f"Nieznany kierunek transakcji: {direction}")
            return None

        
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
# TA SEKCJA ZOSTAJE. Jest kluczowa dla wydajności AQM V3.
# ==================================================================

_backtest_cache = {
    "vix_data": None, # DF dla VXX
    "spy_data": None, # DF dla SPY
    "sector_etf_data": {}, # Klucz: 'XLK', Wartość: DF
    "company_data": {}, # Klucz: 'AAPL', Wartość: {'daily': DF, 'weekly': DF, 'sector': 'Technology'}
    "tickers_by_sector": {}, # Klucz: 'Technology', Wartość: ['AAPL', 'MSFT', ...]
    "sector_map": {} # Mapa Ticker -> Sektor
}

def _get_ticker_data_from_cache(ticker: str) -> Optional[Dict[str, Any]]:
    """Pobiera dane spółki (dzienne, tygodniowe, sektor) z cache."""
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
# TA SEKCJA ZOSTAJE. Dokumentacja V3 (Mapa 3, Faza 3)
# jawnie wymaga testowania w różnych reżimach rynkowych.
# ==================================================================
def _detect_market_regime(current_date_str: str) -> str:
    """
    Wykrywa reżim rynkowy na podstawie danych VXX i SPY z cache.
    Logika zgodna z PDF (str. 10 i 16).
    """
    try:
        vix_df = _backtest_cache["vix_data"]
        spy_df = _backtest_cache["spy_data"]
        
        # Pobierz ostatni wiersz do danej daty włącznie
        vix_row = vix_df[vix_df.index <= current_date_str].iloc[-1]
        spy_row = spy_df[spy_df.index <= current_date_str].iloc[-1]
        
        vix_price = vix_row['close']
        spy_price = spy_row['close']
        spy_sma_50 = spy_row['ema_50']   # Używamy EMA zamiast SMA
        spy_sma_200 = spy_row['ema_200'] # Używamy EMA zamiast SMA

        if pd.isna(vix_price) or pd.isna(spy_price) or pd.isna(spy_sma_50) or pd.isna(spy_sma_200):
            return 'volatile' # Błąd danych, przyjmij neutralny

        # Logika z PDF (str. 10 i 16)
        if vix_price < 18 and spy_price > spy_sma_200:
            return 'bull'
        elif vix_price > 25 or spy_price < spy_sma_50: # PDF str 16: spy < sma_50 (str 10: spy < sma_200) - użyjmy tej z str 16
            return 'bear'
        else:
            return 'volatile' # Wszystko pomiędzy (PDF str. 6: VIX 18-25)
            
    except IndexError:
        # Za mało danych na początku historii
        return 'volatile'
    except Exception as e:
        logger.error(f"Błąd podczas wykrywania reżimu dla {current_date_str}: {e}")
        return 'volatile'

# ==================================================================
# === DEKONSTRUKCJA (KROK 13) ===
# Całkowicie usunięto stare funkcje analityczne AQM:
# - _calculate_aqm_score (stary model)
# - _simulate_trades_aqm (stary symulator)
#
# Zostaną one zastąpione przez moduły obliczeniowe 7 Wymiarów
# i symulatory Hipotez H1-H4 z AQM V3.
# ==================================================================


# ==================================================================
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (PRZEBUDOWANA) ===
# ==================================================================

def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny dla
    zdefiniowanego okresu i listy tickerów.
    
    PRZEBUDOWANA: Platforma gotowa na implementację hipotez AQM V3.
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
        # ==================================================================
        # === DEKONSTRUKCJA (KROK 13) ===
        # Zmieniamy wzorzec, aby czyścił *nowe* wyniki AQM_V3, a nie stary
        # wzorzec "AQM_%". Nowy wzorzec będzie np. "AQM_V3_H1_...".
        # ==================================================================
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
        # ==================================================================
        # === NAPRAWA (KROK 13 - REWIZJA) ===
        # Zmieniamy źródło z `companies` na `phase1_candidates`.
        # Backtest będzie teraz działał tylko na spółkach, które przeszły
        # Twoje "Pierwsze Sito" (cena i wolumen).
        # ==================================================================
        log_msg_tickers = "[Backtest V3] Pobieranie listy spółek z Fazy 1 ('Pierwsze Sito')..."
        logger.info(log_msg_tickers)
        append_scan_log(session, log_msg_tickers)
        
        tickers_p2_rows = session.execute(text(
            "SELECT DISTINCT ticker FROM phase1_candidates" # <-- NAPRAWIONE ZAPYTANIE
        )).fetchall()
        
        tickers_to_test = sorted([row[0] for row in tickers_p2_rows])
        # ==================================================================

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
    # (Szkielet zostaje, wnętrze jest czyszczone i przygotowywane pod V3)
    
    try:
        logger.info("[Backtest V3] Rozpoczynanie budowania pamięci podręcznej (Cache)...")
        # Resetowanie cache (zostaje)
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["company_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}
        _backtest_cache["sector_map"] = {} # Zresetuj mapę sektorów

        # 2. Pobierz dane Makro (VXX i SPY) (zostaje - potrzebne dla _detect_market_regime)
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
        
        # 3. Pobierz dane Sektorowe (ETF-y) (zostaje - na razie)
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
        logger.info(f"[Backtest V3] Cache: Ładowanie danych dla {len(tickers_to_test)} spółek...")
        total_cache_build = len(tickers_to_test)
        for i, ticker in enumerate(tickers_to_test):
            if i % 10 == 0:
                log_msg = f"[Backtest V3] Budowanie cache... ({i}/{total_cache_build})"
                append_scan_log(session, log_msg)
                update_scan_progress(session, i, total_cache_build)
            
            try:
                # ==================================================================
                # === DEKONSTRUKCJA (KROK 13) ===
                # Zostawiamy pobieranie DANYCH SUROWYCH (daily i weekly),
                # ale usuwamy CAŁĄ starą logikę obliczania wskaźników
                # (RSI, MACD, OBV, AD_LINE itp.).
                #
                # W kolejnych krokach będziemy tu dodawać pobieranie
                # i obliczanie metryk AQM V3 (VWAP, Insider, 5min Intraday itp.).
                # ==================================================================
                
                # Pobierz dane dzienne i tygodniowe (2 wywołania API na ticker)
                price_data_raw = api_client.get_daily_adjusted(ticker, outputsize='full')
                weekly_data_raw = api_client.get_time_series_weekly(ticker) # outputsize='full' jest domyślny
                
                if not price_data_raw or 'Time Series (Daily)' not in price_data_raw or \
                   not weekly_data_raw or 'Weekly Adjusted Time Series' not in weekly_data_raw:
                    logger.warning(f"Brak pełnych danych dla {ticker}, pomijanie.")
                    continue

                daily_df = pd.DataFrame.from_dict(price_data_raw['Time Series (Daily)'], orient='index')
                daily_df = standardize_df_columns(daily_df)
                daily_df.index = pd.to_datetime(daily_df.index)
                
                weekly_df = pd.DataFrame.from_dict(weekly_data_raw['Weekly Adjusted Time Series'], orient='index')
                weekly_df = standardize_df_columns(weekly_df)
                weekly_df.index = pd.to_datetime(weekly_df.index)
                
                # --- (STARA LOGIKA OBLICZEŃ USUNIĘTA) ---
                
                # Zdobądź i zapisz sektor (zostaje)
                sector = _get_sector_for_ticker(session, ticker)
                
                # Zapisz wszystko w cache (zostaje)
                _backtest_cache["company_data"][ticker] = {
                    "daily": daily_df,
                    "weekly": weekly_df,
                    "sector": sector
                }
                
                # Zbuduj mapę Sektor -> Ticker (zostaje)
                if sector not in _backtest_cache["tickers_by_sector"]:
                    _backtest_cache["tickers_by_sector"][sector] = []
                _backtest_cache["tickers_by_sector"][sector].append(ticker)

            except Exception as e:
                # Nie przerywaj budowania cache z powodu jednego tickera
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
    
    # ==================================================================
    # === DEKONSTRUKCJA (KROK 13) ===
    # Cała pętla symulacji `_simulate_trades_aqm` została usunięta.
    # W kolejnych krokach zastąpimy ją pętlą uruchamiającą
    # Hipotezy H1, H2, H3, H4 z dokumentacji V3.
    # ==================================================================
    
    log_msg_aqm = "[Backtest V3] Szkielet silnika gotowy. Oczekiwanie na implementację Hipotez (H1-H4)."
    logger.info(log_msg_aqm)
    append_scan_log(session, log_msg_aqm)
            
    # === KONIEC SYMULACJI ===
    total_trades_found = 0 # Na razie 0
    total_tickers = len(tickers_to_test) # Przeniesiono definicję na dół
    update_scan_progress(session, total_tickers, total_tickers) # Ustaw na 100%
    log_msg_final = f"BACKTEST HISTORYCZNY (AQM V3): Przebudowa platformy zakończona dla roku '{year}'. Gotowy do testowania hipotez."
    logger.info(log_msg_final)
    append_scan_log(session, log_msg_final)
