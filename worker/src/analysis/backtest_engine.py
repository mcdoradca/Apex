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
    calculate_rsi,
    calculate_macd,
    calculate_atr,
    calculate_obv,
    calculate_ad
)
from .. import models
# Importujemy SECTOR_TO_ETF_MAP (usunięto nieużywany Phase3Config)
from ..config import SECTOR_TO_ETF_MAP

logger = logging.getLogger(__name__)

# ==================================================================
# === Środowisko Backtestingu (Zaktualizowane wg PDF str. 18-19) ===
# ==================================================================

# Definicje progów AQM (z PDF str. 18)
AQM_THRESHOLDS = {
    'bull': 0.65,
    'volatile': 0.75,
    'bear': 0.85
}
# Definicje strategii AQM (z PDF str. 19)
AQM_STRATEGY_PARAMS = {
    'stop_loss_percent': 0.04, # Używamy 4% (środek z 3-5%)
    'target_1_percent': 0.08,  # Target 1 (8%)
    'target_2_percent': 0.12,  # Target 2 (12%) - Na razie nieużywany
    'max_hold_days': 7         # 7 dni (z 3-7 dni)
}

# ==================================================================
# === USUNIĘTO: Parametry dla Handlu Parami ===
# Strategia Handlu Parami została całkowicie usunięta na żądanie.
# ==================================================================


# ==================================================================
# === Silnik Symulacji (Logika LONG) ===
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
# === USUNIĘTO: _calculate_sector_strength ===
# Ta funkcja była używana tylko przez Handel Parami.
# ==================================================================

# ==================================================================
# === STRATEGIA 1: AQM (Long-Only) - PRZEBUDOWANA ===
# ==================================================================

def _calculate_aqm_score(
    df_view: pd.DataFrame, 
    weekly_df_view: pd.DataFrame, 
    sector: str, 
    market_regime: str
) -> (float, dict):
    """
    Oblicza AQM Score na podstawie danych EOD (zgodnie z logiką z PDF str. 13-17).
    Zwraca: (final_score, components_dict)
    
    TA FUNKCJA ZOSTAŁA CAŁKOWICIE PRZEPISANA, ABY ODZWIERCIEDLAĆ LOGIKĘ Z PDF.
    """
    try:
        components = {}
        
        # --- 1. Quantum Prime Score (QPS) - Waga 40% (PDF str. 13-14) ---
        # Używamy tylko danych Dziennych i Tygodniowych, zgodnie z ograniczeniami backtestu.
        
        # --- QPS - Składnik Dzienny ---
        d = df_view.iloc[-1] # Ostatnia świeca dzienna
        
        # QPS - Trend Dzienny (50% wagi QPS_D)
        trend_d = 0.0
        if d['close'] > d['ema_20']: trend_d += 0.25
        if d['close'] > d['ema_50']: trend_d += 0.25
        if d['ema_20'] > d['ema_50']: trend_d += 0.25
        if d['ema_50'] > d['ema_200']: trend_d += 0.25
        
        # QPS - Momentum Dzienne (30% wagi QPS_D)
        momentum_d = 0.0
        if 50 < d['rsi_14'] < 70: momentum_d += 0.5 # PDF str 13: Optymalny zakres
        if d['macd_line'] > d['signal_line']: momentum_d += 0.5
        
        # QPS - Zmienność Dzienna (20% wagi QPS_D)
        volatility_d = 0.3 # Domyślna (zła) wartość
        if d['close'] > 0 and not pd.isna(d['atr_14']):
            atr_percent = d['atr_14'] / d['close']
            if atr_percent < 0.05: volatility_d = 1.0 # Poniżej 5% ATR
            elif atr_percent < 0.08: volatility_d = 0.7 # Poniżej 8% ATR
        
        score_daily = (trend_d * 0.5) + (momentum_d * 0.3) + (volatility_d * 0.2)
        components["QPS_Daily"] = score_daily

        # --- QPS - Składnik Tygodniowy ---
        # PDF str 13: "weekly" - używamy tylko trendu i momentum (brak ATR i EMA 200)
        w = weekly_df_view.iloc[-1] # Ostatnia świeca tygodniowa
        
        # QPS - Trend Tygodniowy (PDF str 13 - Uproszczony: 70% wagi QPS_W)
        trend_w = 0.0
        if w['close'] > w['ema_20']: trend_w += 0.25
        if w['close'] > w['ema_50']: trend_w += 0.25
        if w['ema_20'] > w['ema_50']: trend_w += 0.25
        # Ignorujemy ema_50 > ema_200, brak w danych tygodniowych
        
        # QPS - Momentum Tygodniowe (PDF str 13 - 30% wagi QPS_W)
        momentum_w = 0.0
        if 50 < w['rsi_14'] < 70: momentum_w += 0.5 # Optymalny zakres
        if w['macd_line'] > w['signal_line']: momentum_w += 0.5
        
        score_weekly = (trend_w * 0.7) + (momentum_w * 0.3)
        components["QPS_Weekly"] = score_weekly
        
        # PDF str 13: "return harmonic_mean(scores)" - Użyjemy średniej arytmetycznej
        qps_score = (score_daily + score_weekly) / 2.0
        components["QPS_Final"] = qps_score


        # --- 2. Volume Entropy Score (VES) - Waga 30% (PDF str. 14-15) ---
        # Używamy tylko danych dziennych (d), bo tam mamy OBV i A/D
        
        # VES - Analiza OBV (40% wagi VES)
        obv_score = 0.0
        if d['obv'] > d['obv_20_ma']: obv_score += 0.4
        if d['obv'] > df_view.iloc[-2]['obv']: obv_score += 0.3 # OBV rośnie (trend > 0)
        if d['obv_20_ma'] > d['obv_50_ma']: obv_score += 0.3
        
        # VES - Stosunek Wolumenu (30% wagi VES)
        volume_score = 0.1 # Domyślna (zła) wartość
        if d['volume_20_ma'] > 0:
            volume_ratio = d['volume'] / d['volume_20_ma']
            if volume_ratio > 1.2: volume_score = 1.0 # PDF str 15: > 1.2 (użyjemy 1.0)
            elif volume_ratio > 0.8: volume_score = 0.7 # PDF str 15: > 0.8 (użyjemy 0.7)
        
        # VES - Analiza A/D Line (30% wagi VES)
        ad_score = 0.0
        if d['ad_line'] > d['ad_line_20_ma']: ad_score += 0.5 # PDF str 15: Użyjemy 0.5
        if d['ad_line'] > df_view.iloc[-2]['ad_line']: ad_score += 0.5 # A/D rośnie (trend > 0)
        
        ves_score = (obv_score * 0.4) + (volume_score * 0.3) + (ad_score * 0.3)
        components["VES_Final"] = ves_score


        # --- 3. Market Regime Score (MRS) - Waga 20% (PDF str. 15-16) ---
        # Używamy `market_regime` i `sector` przekazanych do funkcji
        mrs_score = 0.0 # Domyślnie 0
        
        if market_regime == 'bull':
            # PDF str 16: Faworyzuj Technology, Communication Services, Consumer Cyclical
            if sector in ['Technology', 'Communication Services', 'Consumer Discretionary']: # Używamy nazw z naszej bazy
                mrs_score = 0.7
            # PDF nie określa "else", ale obecny kod dawał 0.3. Zostawmy 0.
            
        elif market_regime == 'bear':
            # PDF str 16: Faworyzuj Utilities, Consumer Defensive, Healthcare
            if sector in ['Utilities', 'Consumer Staples', 'Health Care']: # Używamy nazw z naszej bazy
                mrs_score = 0.7
            # PDF nie wspomina o "strong balance sheet", pomijamy
        
        elif market_regime == 'volatile':
            # PDF str 6 i 16 sugerują faworyzowanie "jakości" lub niczego
            # Obecny kod dawał 0.5. Użyjmy 0.5 jako neutralnej wartości.
            mrs_score = 0.5 

        components["MRS_Final"] = mrs_score
        
        # --- 4. Temporal Coherence Score (TCS) - Waga 10% (PDF str. 17) ---
        # Nie mamy danych o zarobkach w backteście. Używamy uproszczenia.
        tcs_score = 1.0 # Uproszczenie: Załóżmy, że timing jest zawsze dobry
        components["TCS_Final"] = tcs_score
        
        # --- FINAŁ: Mnożenie (PDF str 12 i 18) ---
        # Użyjmy mnożenia, zgodnie z PDF.
        # Musimy upewnić się, że żaden score nie jest NaN
        if pd.isna(qps_score) or pd.isna(ves_score) or pd.isna(mrs_score) or pd.isna(tcs_score):
             return 0.0, components

        final_aqm_score = (qps_score * ves_score * mrs_score * tcs_score)
        
        return final_aqm_score, components

    except Exception as e:
        logger.error(f"Błąd krytyczny w _calculate_aqm_score: {e}", exc_info=True)
        return 0.0, {}

# ==================================================================
# === Symulator AQM (Zaktualizowany) ===
# ==================================================================
def _simulate_trades_aqm(
    session: Session, 
    ticker: str, 
    historical_data: pd.DataFrame, 
    weekly_data: pd.DataFrame, 
    year: str
):
    """
    Iteruje dzień po dniu przez historyczny DataFrame DLA JEDNEJ SPÓŁKI
    i szuka setupów AQM, używając nowej logiki z PDF.
    
    (Funkcja ta zastępuje starą _simulate_trades_aqm)
    """
    trades_found = 0
    ticker_data_cache = _get_ticker_data_from_cache(ticker)
    if not ticker_data_cache:
        return 0
    ticker_sector = ticker_data_cache['sector']

    # Zaczynamy od 200, aby mieć wystarczająco danych dla EMA 200
    for i in range(200, len(historical_data)):
        
        current_date = historical_data.index[i]
        current_date_str = current_date.strftime('%Y-%m-%d')

        # Tworzymy "widok" danych, który widziałby analityk danego dnia
        # Obejmuje 200 świec wstecz + bieżącą świecę (łącznie 201)
        df_view = historical_data.iloc[i-200 : i+1].copy()
        
        # Pobierz widok danych tygodniowych (do bieżącej daty)
        weekly_df_view = weekly_data[weekly_data.index <= current_date_str].iloc[-50:]
        if len(weekly_df_view) < 50:
            continue # Za mało danych tygodniowych

        # --- TESTOWANIE STRATEGII AQM (Logika z PDF) ---
        
        # 1. Wykryj reżim rynkowy (raz na dzień symulacji)
        market_regime = _detect_market_regime(current_date_str)
        
        # 2. Oblicz NOWY AQM Score (logika z PDF str. 13-17)
        aqm_score, components = _calculate_aqm_score(df_view, weekly_df_view, ticker_sector, market_regime)
        
        # 3. Sprawdź, czy przekracza próg dla danego reżimu (logika z PDF str. 18)
        threshold = AQM_THRESHOLDS.get(market_regime, AQM_THRESHOLDS['bear']) # Domyślnie najsurowszy próg
        
        if aqm_score > threshold:
            # ZNALEZIONO SETUP!
            latest_candle = df_view.iloc[-1]
            entry_price = latest_candle['close'] # PDF str 19: "Cena wejścia (zamknięcie dnia)"
            
            # Użyj parametrów z PDF str 19
            stop_loss_val = entry_price * (1 - AQM_STRATEGY_PARAMS['stop_loss_percent'])
            take_profit_val = entry_price * (1 + AQM_STRATEGY_PARAMS['target_1_percent']) # Używamy Target 1

            setup_aqm = {
                "ticker": ticker,
                "setup_type": f"AQM_SCORE_{market_regime.upper()}", 
                "entry_price": entry_price,
                "stop_loss": stop_loss_val,
                "take_profit": take_profit_val,
            }
            
            # entry_index to 'i' w pętli historical_data
            trade = _resolve_trade(
                historical_data, i, setup_aqm, 
                AQM_STRATEGY_PARAMS['max_hold_days'], year, direction='LONG'
            )
            if trade:
                session.add(trade)
                trades_found += 1

    if trades_found > 0:
        try:
            session.commit()
        except Exception as e:
            logger.error(f"Błąd podczas commitowania transakcji AQM dla {ticker}: {e}")
            session.rollback()
        
    return trades_found

# ==================================================================
# === USUNIĘTO: Logika Strategii 2: Handel Parami ===
# Całkowicie usunięto funkcje:
# - _find_best_long_candidate
# - _find_best_short_candidate
# - _run_pairs_trading_simulation
# ==================================================================


# ==================================================================
# === GŁÓWNA FUNKCJA URUCHAMIAJĄCA (PRZEBUDOWANA) ===
# ==================================================================

def run_historical_backtest(session: Session, api_client: AlphaVantageClient, year: str):
    """
    Główna funkcja uruchamiająca backtest historyczny dla
    zdefiniowanego okresu i listy tickerów.
    
    PRZEBUDOWANA: Uruchamia TYLKO strategię AQM.
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
        
    log_msg = f"BACKTEST HISTORYCZNY (Strategia AQM): Rozpoczynanie testu dla roku '{year}' ({start_date} do {end_date})"
    logger.info(log_msg)
    append_scan_log(session, log_msg)

    # === KROK 1: Czyszczenie Bazy Danych ===
    try:
        # Usuwamy tylko wyniki pasujące do tego roku i strategii
        like_pattern = f"BACKTEST_{year}_AQM_%"
        logger.info(f"Czyszczenie starych wyników AQM dla wzorca: {like_pattern}...")
        
        delete_stmt = text("DELETE FROM virtual_trades WHERE setup_type LIKE :pattern")
        result = session.execute(delete_stmt, {'pattern': like_pattern})
        
        session.commit()
        logger.info(f"Pomyślnie usunięto {result.rowcount} starych wpisów backtestu AQM dla roku {year}.")
        
    except Exception as e:
        logger.error(f"Nie udało się wyczyścić starych wyników backtestu: {e}", exc_info=True)
        session.rollback()

    # === KROK 2: Pobieranie Listy Spółek ===
    try:
        # ==================================================================
        # === NAPRAWA BŁĘDU "1 Ticker" (Poprawiona Logika) ===
        # Pobieramy *TYLKO* tickery, które kiedykolwiek przeszły Fazę 2
        # i są oznaczone jako "is_qualified = TRUE".
        # To jest nasza ostateczna lista "APEX Elita" do testowania.
        # ==================================================================
        log_msg_tickers = "[Backtest] Pobieranie listy tickerów 'APEX Elita' (z Fazy 2)..."
        logger.info(log_msg_tickers)
        append_scan_log(session, log_msg_tickers)
        
        tickers_p2_rows = session.execute(text(
            "SELECT DISTINCT ticker FROM phase2_results WHERE is_qualified = TRUE"
        )).fetchall()
        
        tickers_to_test = sorted([row[0] for row in tickers_p2_rows])
        # ==================================================================

        if not tickers_to_test:
            log_msg = f"[Backtest] BŁĄD: Tabela 'phase2_results' (is_qualified=TRUE) jest pusta. Uruchom najpierw pełny skaner (przycisk 'Start'), aby wygenerować listę 'APEX Elita'."
            logger.error(log_msg)
            append_scan_log(session, log_msg)
            return

        log_msg = f"[Backtest] Znaleziono {len(tickers_to_test)} unikalnych tickerów 'APEX Elita' do przetestowania."
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
    except Exception as e:
        log_msg = f"[Backtest] BŁĄD: Nie można pobrać listy tickerów: {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # === KROK 3: Budowanie Pamięci Podręcznej (Cache) ===
    # (Pobiera VXX, SPY, 11 Sektorów ETF i wszystkie spółki z Fazy 2)
    
    try:
        logger.info("[Backtest] Rozpoczynanie budowania pamięci podręcznej (Cache)...")
        # Resetowanie cache
        _backtest_cache["vix_data"] = None
        _backtest_cache["spy_data"] = None
        _backtest_cache["sector_etf_data"] = {}
        _backtest_cache["company_data"] = {}
        _backtest_cache["tickers_by_sector"] = {}
        _backtest_cache["sector_map"] = {} # Zresetuj mapę sektorów

        # 2. Pobierz dane Makro (VXX i SPY)
        logger.info("[Backtest] Cache: Ładowanie VXX i SPY...")
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
        logger.info("[Backtest] Cache: Ładowanie 11 sektorów ETF...")
        for sector_name, etf_ticker in SECTOR_TO_ETF_MAP.items():
            try:
                sector_raw = api_client.get_daily_adjusted(etf_ticker, outputsize='full')
                sector_df = pd.DataFrame.from_dict(sector_raw['Time Series (Daily)'], orient='index')
                sector_df = standardize_df_columns(sector_df)
                sector_df.index = pd.to_datetime(sector_df.index)
                _backtest_cache["sector_etf_data"][etf_ticker] = sector_df
            except Exception as e:
                logger.error(f"  > BŁĄD ładowania danych dla sektora {etf_ticker}: {e}")
        
        # 4. Pobierz dane dla wszystkich spółek z listy Fazy 2
        logger.info(f"[Backtest] Cache: Ładowanie danych dla {len(tickers_to_test)} spółek...")
        total_cache_build = len(tickers_to_test)
        for i, ticker in enumerate(tickers_to_test):
            if i % 10 == 0:
                log_msg = f"[Backtest] Budowanie cache... ({i}/{total_cache_build})"
                append_scan_log(session, log_msg)
                update_scan_progress(session, i, total_cache_build)
            
            try:
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

                # Oblicz wskaźniki dla spółki (raz) - DZIENNE (potrzebne do QPS i VES)
                daily_df['rsi_14'] = calculate_rsi(daily_df['close'], period=14)
                daily_df['macd_line'], daily_df['signal_line'] = calculate_macd(daily_df['close'])
                daily_df['atr_14'] = calculate_atr(daily_df, period=14)
                daily_df['ema_20'] = calculate_ema(daily_df['close'], period=20)
                daily_df['ema_50'] = calculate_ema(daily_df['close'], period=50)
                daily_df['ema_200'] = calculate_ema(daily_df['close'], period=200)
                daily_df['obv'] = calculate_obv(daily_df)
                daily_df['obv_20_ma'] = daily_df['obv'].rolling(window=20).mean()
                daily_df['obv_50_ma'] = daily_df['obv'].rolling(window=50).mean()
                daily_df['volume_20_ma'] = daily_df['volume'].rolling(window=20).mean()
                daily_df['ad_line'] = calculate_ad(daily_df)
                daily_df['ad_line_20_ma'] = daily_df['ad_line'].rolling(window=20).mean()
                
                # Oblicz wskaźniki dla spółki (raz) - TYGODNIOWE (potrzebne do QPS)
                weekly_df['rsi_14'] = calculate_rsi(weekly_df['close'], period=14)
                weekly_df['macd_line'], weekly_df['signal_line'] = calculate_macd(weekly_df['close'])
                weekly_df['ema_20'] = calculate_ema(weekly_df['close'], period=20)
                weekly_df['ema_50'] = calculate_ema(weekly_df['close'], period=50)
                
                # Zdobądź i zapisz sektor
                sector = _get_sector_for_ticker(session, ticker)
                
                # Zapisz wszystko w cache
                _backtest_cache["company_data"][ticker] = {
                    "daily": daily_df,
                    "weekly": weekly_df,
                    "sector": sector
                }
                
                # Zbuduj mapę Sektor -> Ticker (na potrzeby MRS)
                if sector not in _backtest_cache["tickers_by_sector"]:
                    _backtest_cache["tickers_by_sector"][sector] = []
                _backtest_cache["tickers_by_sector"][sector].append(ticker)

            except Exception as e:
                # Nie przerywaj budowania cache z powodu jednego tickera
                logger.error(f"[Backtest] Błąd budowania cache dla {ticker}: {e}", exc_info=True)
                
        logger.info("[Backtest] Budowanie pamięci podręcznej (Cache) zakończone.")
        append_scan_log(session, "[Backtest] Budowanie pamięci podręcznej (Cache) zakończone.")
        update_scan_progress(session, total_cache_build, total_cache_build)

    except Exception as e:
        log_msg = f"[Backtest] BŁĄD KRYTYCZNY podczas budowania cache: {e}. Zatrzymywanie."
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        return

    # ==================================================================
    # === KROK 4: Uruchomienie Symulacji (TYLKO AQM) ===
    # ==================================================================
    
    trades_found_aqm = 0
    total_tickers = len(tickers_to_test)
    
    logger.info("[Backtest] Uruchamianie Strategii AQM (Long-Only, Bottom-Up)...")
    append_scan_log(session, "[Backtest] Uruchamianie Strategii AQM...")
    
    for i, ticker in enumerate(tickers_to_test):
        if i % 10 == 0:
            log_msg = f"[Backtest][AQM] Przetwarzanie {ticker} ({i}/{total_tickers})..."
            append_scan_log(session, log_msg)
            update_scan_progress(session, i, total_tickers)
        
        try:
            # Używamy danych z cache
            ticker_data = _get_ticker_data_from_cache(ticker)
            if not ticker_data:
                continue
            
            # Krojenie danych do roku testowego
            
            # ==================================================================
            # === NAPRAWA BŁĘDU `TypeError` ===
            # Zmieniamy `get_loc(..., method='bfill')` na `get_indexer(...)`
            # ==================================================================
            try:
                # Użyj get_indexer() dla kompatybilności ze starszymi wersjami pandas
                # Znajdź indeks pierwszej daty, która jest >= start_date
                indexer = ticker_data['daily'].index.get_indexer([start_date], method='bfill')
                
                # Sprawdź, czy data została znaleziona (indexer zwróci -1, jeśli nie)
                if indexer[0] == -1:
                    raise KeyError(f"Data {start_date} nie znaleziona w indeksie dla {ticker}")
                
                start_index = indexer[0]
                
            except KeyError:
                logger.warning(f"[Backtest] Brak danych dla {ticker} w roku {year} lub przed nim. Pomijanie.")
                continue
            # ==================================================================

            
            # Upewnij się, że mamy 200 dni historii PRZED startem roku
            if start_index < 200:
                logger.warning(f"Za mało danych historycznych dla {ticker} przed {year} (znaleziono {start_index} świec). Pomijanie.")
                continue

            # Kroimy dane dzienne: od (start_date - 200 świec) do end_date
            # To daje nam pełny bufor na obliczenia
            historical_data_slice = ticker_data['daily'].iloc[start_index-200:].loc[:end_date]
            
            # Kroimy dane tygodniowe: wszystko do end_date
            weekly_data_slice = ticker_data['weekly'].loc[:end_date] # Do końca roku
            
            if historical_data_slice.empty or len(historical_data_slice) < 200 or \
               weekly_data_slice.empty or len(weekly_data_slice) < 50:
                logger.warning(f"Pusty wycinek danych dla {ticker} w roku {year}. Pomijanie.")
                continue

            # Uruchom symulator AQM (który iteruje po dniach wewnątrz)
            trades_found_aqm += _simulate_trades_aqm(
                session, ticker, 
                historical_data_slice, 
                weekly_data_slice, 
                year
            )
        except Exception as e:
            logger.error(f"[Backtest][AQM] Błąd krytyczny dla {ticker}: {e}", exc_info=True)
            session.rollback()
            
    log_msg_aqm = f"[Backtest] Strategia AQM zakończona. Znaleziono {trades_found_aqm} transakcji."
    logger.info(log_msg_aqm)
    append_scan_log(session, log_msg_aqm)

    # ==================================================================
    # === USUNIĘTO: Uruchomienie Strategii 2 (Handel Parami) ===
    # ==================================================================

    # === KONIEC SYMULACJI ===
    total_trades_found = trades_found_aqm
    update_scan_progress(session, total_tickers, total_tickers) # Ustaw na 100%
    log_msg_final = f"BACKTEST HISTORYCZNY (AQM): Zakończono test dla roku '{year}'. Znaleziono łącznie {total_trades_found} transakcji."
    logger.info(log_msg_final)
    append_scan_log(session, log_msg_final)
