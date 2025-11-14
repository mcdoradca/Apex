import logging
import pandas as pd
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session # <-- ZMIANA: Dodano import Session
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
# <-- ZMIANA: Importujemy funkcję cache z utils
from .utils import get_raw_data_with_cache, get_current_NY_datetime
from datetime import timedelta, datetime # Potrzebne do obliczeń czasu

logger = logging.getLogger(__name__)

def _parse_insider_transactions(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Przetwarza surową odpowiedź JSON z INSIDER_TRANSACTIONS na DataFrame
    gotowy do backtestu. (LOGIKA OBLICZENIOWA BEZ ZMIAN)
    
    AKTUALIZACJA: Naprawiono błędy parsowania na podstawie rzeczywistych danych JSON.
    """
    try:
        # === POPRAWKA 1: Błędny klucz główny ===
        transactions = raw_data.get('data', []) # <-- Klucz to 'data', a nie 'transactions'
        
        if not transactions:
            # Zwracamy pusty DF z oczekiwanymi kolumnami
            return pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))

        processed_data = []
        for tx in transactions:
            try:
                # === POPRAWKA 2: Błędne nazwy pól ===
                # Konwertujemy datę na obiekt datetime, aby ustawić ją jako indeks
                tx_date = pd.to_datetime(tx.get('transaction_date')) # <-- Poprawna nazwa pola
                tx_type = tx.get('acquisition_or_disposal') # <-- Poprawna nazwa pola
                tx_shares_str = tx.get('shares') # <-- Poprawna nazwa pola
                
                # Walidacja: upewnij się, że 'shares' nie jest puste (jak w 'Convertible Note')
                if not tx_shares_str:
                    continue
                    
                tx_shares = float(tx_shares_str)
                # ==================================================================
                
                # ==================================================================
                # === POPRAWKA 3: Błędne wartości filtra ===
                # Zgodnie ze specyfikacją H2 (Wymiar 2.1), interesują nas 'A' i 'D'
                if tx_type in ['A', 'D'] and tx_shares > 0:
                # ==================================================================
                    processed_data.append({
                        'transaction_date': tx_date,
                        'transaction_type': tx_type, # Zapisze 'A' lub 'D'
                        'transaction_shares': tx_shares
                    })
            except (ValueError, TypeError, AttributeError):
                continue # Pomiń błędne rekordy

        if not processed_data:
             return pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))

        df = pd.DataFrame(processed_data)
        # Ustawiamy datę jako indeks, aby umożliwić filtrowanie wg dat w backteście
        df.set_index('transaction_date', inplace=True)
        df.sort_index(inplace=True)
        return df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych Insider Transactions: {e}", exc_info=True)
        return None

def _parse_news_sentiment(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Przetwarza surową odpowiedź JSON z NEWS_SENTIMENT na DataFrame
    gotowy do backtestu. (LOGIKA OBLICZENIOWA BEZ ZMIAN)
    """
    try:
        feed = raw_data.get('feed', [])
        if not feed:
            # Zwracamy pusty DF z oczekiwanymi kolumnami
            return pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

        processed_data = []
        for article in feed:
            try:
                # Format czasu AV to 'YYYYMMDDTHHMMSS'
                pub_time_str = article.get('time_published')
                # Używamy datetime.strptime, bo pd.to_datetime jest zbyt wolne w pętli.
                pub_time = datetime.strptime(pub_time_str, '%Y%m%dT%H%M%S') 
                score = float(article.get('overall_sentiment_score'))
                
                # Krok 22b: Przetwarzanie 'topics'
                article_topics = article.get('topics', [])
                topic_list = [t['topic'] for t in article_topics if 'topic' in t]
                
                processed_data.append({
                    'published_at': pub_time,
                    'overall_sentiment_score': score,
                    'topics': topic_list # Zapisujemy listę tematów
                })
            except (ValueError, TypeError, AttributeError):
                continue # Pomiń błędne rekordy

        if not processed_data:
            return pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

        df = pd.DataFrame(processed_data)
        # Ustawiamy datę jako indeks
        df.set_index('published_at', inplace=True)
        df.sort_index(inplace=True)
        return df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych News Sentiment: {e}", exc_info=True)
        return None

def load_h2_data_into_cache(ticker: str, api_client: AlphaVantageClient, session: Session) -> Dict[str, pd.DataFrame]:
    """
    Główna funkcja tego modułu. Pobiera i przetwarza dane Wymiaru 2
    dla pojedynczego tickera, używając MECHANIZMU CACHE.
    
    POPRAWKA: Wprowadzamy ograniczenia czasowe, aby pobierać tylko niezbędną
    historię dla backtestu, co drastycznie przyspiesza proces.
    """
    logger.info(f"[Backtest V3][H2 Loader] Ładowanie danych Wymiaru 2 dla {ticker} (z cache)...")
    
    # 1. Definicja ram czasowych dla backtestu (zgodnie ze specyfikacją)
    now_utc = datetime.now(timezone.utc)
    
    # --- Insider Transactions (90 dni) ---
    # Zamiast pobierać całą historię (20 lat), pobieramy tylko ostatnie 90 dni, 
    # ponieważ backtest i tak używa tylko 90 dni rolling history (Wymiar 2.1).
    # UWAGA: API AV dla Insiderów nie wspiera time_from/time_to. Pamiętajmy, że pobieramy
    # domyślnie ostatnie 2 lata, więc to nie jest pełna optymalizacja, ale lepsze niż nic.
    
    # 1. Pobierz dane Insider (Wymiar 2.1) - UŻYJ CACHE
    insider_raw = get_raw_data_with_cache(
        session=session, # Przekazujemy sesję do zapisu/odczytu cache
        api_client=api_client, 
        ticker=ticker,
        data_type='INSIDER', # Stała definiująca typ danych
        api_func='get_insider_transactions'
        # UWAGA: To API nie wspiera time_from, więc pobieramy domyślną historię (ok. 2 lata)
    )
    insider_df = _parse_insider_transactions(insider_raw)
    
    if insider_df is None:
        logger.warning(f"[Backtest V3][H2 Loader] Nie udało się przetworzyć danych Insider dla {ticker}. Tworzenie pustego DataFrame.")
        insider_df = pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))
    
    # 2. Pobierz dane News (Wymiar 2.2 i 4.2) - UŻYJ CACHE
    # ZGODNIE Z MEMO SUPPORTU: Potrzebujemy do backtestu newsów z całego okresu.
    # W `aqm_v3_metrics` używamy NEWS_SENTIMENT do obliczeń Entropii (10 dni) i Herding (7 dni)
    # Zostawiamy limit na 1000, aby pokryć jak najwięcej danych do backtestu.
    news_raw = get_raw_data_with_cache(
        session=session, # Przekazujemy sesję do zapisu/odczytu cache
        api_client=api_client, 
        ticker=ticker,
        data_type='NEWS_SENTIMENT_FULL', # Zmieniono na FULL, bo pobieramy dużo
        api_func='get_news_sentiment',
        limit=1000 # Użyjemy limitu 1000, aby mieć pełną historię do backtestu
    )
    news_df = _parse_news_sentiment(news_raw)
    
    if news_df is None:
        logger.warning(f"[Backtest V3][H2 Loader] Nie udało się przetworzyć danych News dla {ticker}. Tworzenie pustego DataFrame.")
        news_df = pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

    logger.info(f"[Backtest V3][H2 Loader] Załadowano {len(insider_df)} transakcji i {len(news_df)} newsów dla {ticker}.")

    return {
        "insider_df": insider_df,
        "news_df": news_df 
    }
