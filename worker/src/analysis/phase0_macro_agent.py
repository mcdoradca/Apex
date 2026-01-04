import logging
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy.orm import Session
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from ..analysis.utils import (
    append_scan_log, update_system_control
)

logger = logging.getLogger(__name__)

def run_macro_analysis(session: Session, api_client: AlphaVantageClient) -> str:
    """
    Agent Makro Fazy 0 (Wersja NASDAQ-FOCUSED).
    Analizuje kondycję indeksu technologicznego (QQQ vs SMA200) oraz Rentowność Obligacji.
    Zwraca: 'RISK_ON' (Handluj) lub 'RISK_OFF' (Gotówka).
    """
    logger.info("🌍 AGENT FAZY 0: Rozpoczynam analizę kondycji NASDAQ (QQQ)...")
    
    try:
        # === KROK 1: Pobieranie danych ===
        
        # A. Dane Środowiskowe (Nasdaq-100 ETF: QQQ)
        # To jest nasz benchmark. Jeśli QQQ krwawi, nasze setupy na Nasdaq też będą krwawić.
        market_ticker = "QQQ"
        market_raw = api_client.get_daily_adjusted(market_ticker, outputsize='full')
        
        # B. Dane Makro (Rentowność Obligacji 10Y)
        # Rentowność uderza w spółki technologiczne (Growth) mocniej niż w szeroki rynek.
        yield_data = api_client.get_treasury_yield(interval='monthly', maturity='10year')
        
        # === KROK 2: Przetwarzanie i Obliczenia ===
        
        # 1. Analiza Techniczna Rynku (QQQ)
        if not market_raw or 'Time Series (Daily)' not in market_raw:
            logger.warning(f"Faza 0: Brak danych dla {market_ticker}. Zakładam tryb RISK_OFF.")
            return _set_status(session, "RISK_OFF", f"Brak danych {market_ticker}")

        df = pd.DataFrame.from_dict(market_raw['Time Series (Daily)'], orient='index')
        df = df.astype(float)
        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        
        # Szukamy kolumny z ceną (adjusted close)
        close_col = [c for c in df.columns if 'adjusted close' in c][0]
        
        # Obliczenie SMA 200 (Długoterminowy Trend Technologiczny)
        df['sma200'] = df[close_col].rolling(window=200).mean()
        
        current_price = df[close_col].iloc[-1]
        current_sma200 = df['sma200'].iloc[-1]
        
        # 2. Analiza Rentowności
        current_yield = 0.0
        if yield_data and 'data' in yield_data and yield_data['data']:
            try:
                current_yield = float(yield_data['data'][0]['value'])
            except: pass

        # === KROK 3: Logika Decyzyjna (Nasdaq Logic) ===
        reasons = []
        score = 0
        
        # ZASADA 1: Trend Technologiczny (QQQ vs SMA200)
        # Dla Nasdaqa SMA200 jest kluczową linią życia.
        if current_price < current_sma200:
            score -= 100 # BESSA w TECH - Zakaz handlu
            reasons.append(f"QQQ pod SMA200 ({current_price:.2f} < {current_sma200:.2f})")
        else:
            score += 50
            reasons.append("QQQ nad SMA200 (Trend Wzrostowy)")
            
        # ZASADA 2: Rentowność Obligacji (Wrażliwość na stopy)
        # Nasdaq jest bardziej czuły na rentowność niż S&P.
        # Próg ostrzegawczy: 4.5%
        if current_yield > 4.5:
            score -= 40 
            reasons.append(f"Wysoka Rentowność 10Y ({current_yield}%) - Presja na Tech")
        elif current_yield < 3.5:
            score += 20
            
        # Werdykt
        final_status = "RISK_OFF" if score < 0 else "RISK_ON"
        reason_str = " | ".join(reasons)
        
        # Raportowanie
        log_msg = f"RAPORT NASDAQ: Decyzja={final_status}. {reason_str}"
        logger.info(log_msg)
        append_scan_log(session, log_msg)
        
        return _set_status(session, final_status, reason_str)

    except Exception as e:
        logger.error(f"AGENT FAZY 0 (MAKRO): Błąd krytyczny: {e}", exc_info=True)
        return _set_status(session, "RISK_OFF", f"Awaria Agenta Makro: {e}")

def _set_status(session, status, details):
    """Pomocnicza funkcja zapisu stanu."""
    update_system_control(session, 'market_status', status)
    update_system_control(session, 'macro_sentiment', f"{status} | {details}")
    return status
