import logging
import pandas as pd
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session # <-- ZMIANA: Dodano import Session
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
# <-- ZMIANA: Importujemy funkcję cache z utils
from .utils import get_raw_data_with_cache 

logger = logging.getLogger(__name__)

# Usunięto funkcję _get_raw_data_with_cache, ponieważ jest teraz w utils.py

def _parse_insider_transactions(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Przetwarza surową odpowiedź JSON z INSIDER_TRANSACTIONS na DataFrame
    gotowy do backtestu. (LOGIKA OBLICZENIOWA BEZ ZMIAN)
    """
    try:
        transactions = raw_data.get('transactions', [])
        if not transactions:
            # Zwracamy pusty DF z oczekiwanymi kolumnami
            return pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))

        processed_data = []
        for tx in transactions:
            try:
                # Konwertujemy datę na obiekt datetime, aby ustawić ją jako indeks
                tx_date = pd.to_datetime(tx.get('transactionDate'))
                tx_type = tx.get('transactionType')
                tx_shares = float(tx.get('transactionShares'))
                
                # Zgodnie ze specyfikacją H2 (Wymiar 2.1), interesują nas tylko 'P-Purchase' i 'S-Sale'
                if tx_type in ['P-Purchase', 'S-Sale'] and tx_shares > 0:
                    processed_data.append({
                        'transaction_date': tx_date,
                        'transaction_type': tx_type,
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
                pub_time = pd.to_datetime(pub_time_str, format='%Y%m%dT%H%M%S')
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
    
    ZMIANA: Dodano argument 'session' i użyto cache.
    """
    logger.info(f"[Backtest V3][H2 Loader] Ładowanie danych Wymiaru 2 dla {ticker} (z cache)...")
    
    # 1. Pobierz dane Insider (Wymiar 2.1) - UŻYJ CACHE
    insider_raw = get_raw_data_with_cache(
        session=session, # Przekazujemy sesję do zapisu/odczytu cache
        api_client=api_client, 
        ticker=ticker,
        data_type='INSIDER', # Stała definiująca typ danych
        api_func='get_insider_transactions'
    )
    insider_df = _parse_insider_transactions(insider_raw)
    
    if insider_df is None:
        logger.warning(f"[Backtest V3][H2 Loader] Nie udało się przetworzyć danych Insider dla {ticker}. Tworzenie pustego DataFrame.")
        insider_df = pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))
    
    # 2. Pobierz dane News (Wymiar 2.2 i 4.2) - UŻYJ CACHE
    news_raw = get_raw_data_with_cache(
        session=session, # Przekazujemy sesję do zapisu/odczytu cache
        api_client=api_client, 
        ticker=ticker,
        data_type='NEWS_SENTIMENT',
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
