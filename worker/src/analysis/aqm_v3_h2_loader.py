import logging
import pandas as pd
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session 
# ==================================================================
# === BŁĄD KRYTYCZNY: BRAK IMPORTU 'timezone' ===
from datetime import datetime, timezone # <--- DODANO BRAKUJĄCY IMPORT
# ==================================================================
from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import get_raw_data_with_cache 

logger = logging.getLogger(__name__)

def _parse_insider_transactions(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Przetwarza surową odpowiedź JSON z INSIDER_TRANSACTIONS na DataFrame.
    (LOGIKA OBLICZENIOWA BEZ ZMIAN)
    """
    try:
        transactions = raw_data.get('data', [])
        
        if not transactions:
            return pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))

        processed_data = []
        for tx in transactions:
            try:
                tx_date = pd.to_datetime(tx.get('transaction_date')) 
                tx_type = tx.get('acquisition_or_disposal')
                tx_shares_str = tx.get('shares') 
                
                if not tx_shares_str:
                    continue
                    
                tx_shares = float(tx_shares_str)
                
                if tx_type in ['A', 'D'] and tx_shares > 0:
                    processed_data.append({
                        'transaction_date': tx_date,
                        'transaction_type': tx_type, 
                        'transaction_shares': tx_shares
                    })
            except (ValueError, TypeError, AttributeError):
                continue 

        if not processed_data:
             return pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))

        df = pd.DataFrame(processed_data)
        df.set_index('transaction_date', inplace=True)
        df.sort_index(inplace=True)
        return df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych Insider Transactions: {e}", exc_info=True)
        return None

def _parse_news_sentiment(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Przetwarza surową odpowiedź JSON z NEWS_SENTIMENT na DataFrame
    gotowy do backtestu.
    """
    try:
        feed = raw_data.get('feed', [])
        if not feed:
            return pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

        processed_data = []
        for article in feed:
            try:
                pub_time_str = article.get('time_published')
                # ZMIANA: Upewnij się, że parsujesz z uwzględnieniem strefy czasowej
                pub_time = pd.to_datetime(pub_time_str, format='%Y%m%dT%H%M%S', utc=True)
                score = float(article.get('overall_sentiment_score'))
                
                article_topics = article.get('topics', [])
                topic_list = [t['topic'] for t in article_topics if 'topic' in t]
                
                processed_data.append({
                    'published_at': pub_time,
                    'overall_sentiment_score': score,
                    'topics': topic_list
                })
            except (ValueError, TypeError, AttributeError):
                continue

        if not processed_data:
            return pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

        df = pd.DataFrame(processed_data)
        df.set_index('published_at', inplace=True)
        df.sort_index(inplace=True)
        return df
        
    except Exception as e:
        logger.error(f"Błąd podczas parsowania danych News Sentiment: {e}", exc_info=True)
        return None

def load_h2_data_into_cache(ticker: str, api_client: AlphaVantageClient, session: Session) -> Dict[str, pd.DataFrame]:
    """
    Pobiera i przetwarza dane Wymiaru 2 dla pojedynczego tickera, używając MECHANIZMU CACHE.
    """
    logger.info(f"[Backtest V3][H2 Loader] Ładowanie danych Wymiaru 2 dla {ticker} (z cache)...")
    
    # Używamy datetime i timezone poprawnie zaimportowanych na górze
    now_utc = datetime.now(timezone.utc)
    
    # 1. Pobierz dane Insider (Wymiar 2.1) - UŻYJ CACHE
    # Używamy cache, ale data aktualizacji jest używana do walidacji w utils.py (CACHE_EXPIRY_DAYS)
    insider_raw = get_raw_data_with_cache(
        session=session,
        api_client=api_client, 
        ticker=ticker,
        data_type='INSIDER',
        api_func='get_insider_transactions'
    )
    insider_df = _parse_insider_transactions(insider_raw)
    
    if insider_df is None:
        logger.warning(f"[Backtest V3][H2 Loader] Nie udało się przetworzyć danych Insider dla {ticker}. Tworzenie pustego DataFrame.")
        insider_df = pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))
    
    # 2. Pobierz dane News (Wymiar 2.2 i 4.2) - UŻYJ CACHE
    # Zmieniliśmy logikę na pobieranie pełnej historii do backtestu (limit=1000)
    news_raw = get_raw_data_with_cache(
        session=session,
        api_client=api_client, 
        ticker=ticker,
        data_type='NEWS_SENTIMENT_FULL_HISTORY', # Zmieniono na unikalny klucz
        api_func='get_news_sentiment',
        limit=1000 # Pełna historia
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
