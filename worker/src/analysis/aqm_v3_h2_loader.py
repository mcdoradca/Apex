import logging
import pandas as pd
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session 
from datetime import datetime, timezone
from functools import lru_cache # <--- KLUCZ DO SZYBKOŚCI

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .utils import get_raw_data_with_cache 

logger = logging.getLogger(__name__)

# Prosty cache w pamięci dla przetworzonych DataFrame'ów
# Zapobiega wielokrotnemu parsowaniu tych samych danych JSON przy każdej próbie Optuny
_H2_DATA_MEMORY_CACHE = {}

def clear_h2_memory_cache():
    """Czyści cache w pamięci (np. przed nowym dużym zadaniem)"""
    global _H2_DATA_MEMORY_CACHE
    _H2_DATA_MEMORY_CACHE.clear()
    logger.info("H2 Memory Cache cleared.")

def _parse_insider_transactions(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
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
                
                if not tx_shares_str: continue
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
    except Exception:
        return None

def _parse_news_sentiment(raw_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    try:
        feed = raw_data.get('feed', [])
        if not feed:
            return pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

        processed_data = []
        for article in feed:
            try:
                pub_time_str = article.get('time_published')
                pub_time = pd.to_datetime(pub_time_str, format='%Y%m%dT%H%M%S', utc=True)
                score = float(article.get('overall_sentiment_score'))
                topics = [t['topic'] for t in article.get('topics', [])]
                
                processed_data.append({
                    'published_at': pub_time,
                    'overall_sentiment_score': score,
                    'topics': topics
                })
            except (ValueError, TypeError, AttributeError):
                continue

        if not processed_data:
            return pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

        df = pd.DataFrame(processed_data)
        df.set_index('published_at', inplace=True)
        df.sort_index(inplace=True)
        return df
    except Exception:
        return None

def load_h2_data_into_cache(ticker: str, api_client: AlphaVantageClient, session: Session) -> Dict[str, pd.DataFrame]:
    """
    Pobiera i przetwarza dane Wymiaru 2.
    Używa CACHE W PAMIĘCI RAM, aby drastycznie przyspieszyć Optimizera.
    """
    # Sprawdź Memory Cache (najszybsze)
    if ticker in _H2_DATA_MEMORY_CACHE:
        return _H2_DATA_MEMORY_CACHE[ticker]

    # logger.info(f"[H2 Loader] Loading data for {ticker} (DB/API)...")
    
    # 1. Pobierz dane Insider (DB Cache lub API)
    insider_raw = get_raw_data_with_cache(
        session=session,
        api_client=api_client, 
        ticker=ticker,
        data_type='INSIDER',
        api_func='get_insider_transactions'
    )
    insider_df = _parse_insider_transactions(insider_raw)
    
    if insider_df is None:
        insider_df = pd.DataFrame(columns=['transaction_type', 'transaction_shares']).set_index(pd.to_datetime([]))
    
    # 2. Pobierz dane News (DB Cache lub API)
    news_raw = get_raw_data_with_cache(
        session=session,
        api_client=api_client, 
        ticker=ticker,
        data_type='NEWS_SENTIMENT_FULL_HISTORY', 
        api_func='get_news_sentiment',
        limit=1000 
    )
    news_df = _parse_news_sentiment(news_raw)
    
    if news_df is None:
        news_df = pd.DataFrame(columns=['overall_sentiment_score', 'topics']).set_index(pd.to_datetime([]))

    result = {
        "insider_df": insider_df,
        "news_df": news_df 
    }
    
    # Zapisz do Memory Cache
    _H2_DATA_MEMORY_CACHE[ticker] = result
    
    return result
