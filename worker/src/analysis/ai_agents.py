import logging
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime

from ..config import Phase2Config
# Zmieniono import, aby używać tylko potrzebnych funkcji
from .utils import safe_float # Usunięto get_performance, get_market_status_and_time

logger = logging.getLogger(__name__)

# --- POPRAWKA BŁĘDU #2: Agent Momentum musi używać danych intraday i live ---
def _run_momentum_agent(ticker: str, api_client: object, quote_data: dict) -> dict:
    """
    Zaktualizowany agent momentum, który używa danych intraday (dla RSI)
    i danych live (dla bieżącej zmiany procentowej).
    """
    score = 0
    max_score = 4 # Utrzymujemy max_score na 4 dla spójności
    details = {}

    try:
        # 1. Analiza RSI na danych INTRADAY (60min), aby lepiej odzwierciedlać bieżące momentum
        # Używamy _make_request, aby obsłużyć błędy API
        rsi_data = api_client.get_rsi(ticker, time_period=9, interval='60min') # Zmieniono na 60min

        if rsi_data and 'Technical Analysis: RSI' in rsi_data:
            # Pobieramy najnowszy dostępny wpis RSI
            rsi_series = rsi_data['Technical Analysis: RSI']
            if rsi_series:
                latest_rsi_key = sorted(rsi_series.keys())[-1] # Znajdź najnowszą datę/godzinę
                latest_rsi = safe_float(rsi_series[latest_rsi_key]['RSI'])
                if latest_rsi:
                    details["9-okresowy RSI (H1)"] = f"{latest_rsi:.2f}"
                    # Dostosowanie punktacji dla RSI H1 (mniej punktów niż dla daily)
                    if latest_rsi > 70: # Wykupienie na H1
                        score += 1
                        details["Wniosek RSI H1"] = "Bardzo silne momentum intraday (RSI > 70)"
                    elif latest_rsi > 55: # Zdecydowanie powyżej neutralnego
                        score += 0 # Neutralne, ale pozytywne
                        details["Wniosek RSI H1"] = "Pozytywne momentum intraday (RSI > 55)"
                    else:
                        details["Wniosek RSI H1"] = "Neutralne lub słabe momentum intraday"
                else:
                     details["RSI (H1)"] = "Brak sparsowanej wartości"
            else:
                 details["RSI (H1)"] = "Pusta seria danych"
        else:
            details["RSI (H1)"] = "Brak danych z API"

        # 2. Analiza Siły Względnej na podstawie danych LIVE (z quote_data)
        # Używamy zmiany procentowej z sesji regularnej jako głównego wskaźnika
        regular_change_percent = quote_data.get("regular_session", {}).get("change_percent")
        live_price = quote_data.get("live_price")

        # Sprawdzamy, czy mamy dane na żywo
        if regular_change_percent is not None and live_price is not None:
            # Używamy safe_float dla pewności
            change_percent_float = safe_float(regular_change_percent)
            details["Zmiana dzienna (live)"] = f"{change_percent_float:.2f}%" if change_percent_float is not None else "N/A"

            if change_percent_float is not None:
                # Podstawowa ocena - czy spółka jest na plusie
                if change_percent_float > 0.5: # Lekki plus
                    score += 1
                    details["Wniosek (Dzienny)"] = "Pozytywny zwrot w trakcie sesji"

                    # Sprawdzamy, czy jest liderem (np. > 3%)
                    if change_percent_float > 3.0:
                        score += 1
                        details["Wniosek (Siła)"] = "Spółka jest silna dzisiaj (> 3%)"

                    # Sprawdzamy, czy jest absolutnym liderem (np. > 8%)
                    if change_percent_float > 8.0:
                        score += 1 # Dodatkowy punkt za wyjątkową siłę
                        details["Wniosek (Siła)"] = "Spółka jest liderem rynku (> 8%)"

                elif change_percent_float < -0.5: # Lekki minus
                    details["Wniosek (Dzienny)"] = "Negatywny zwrot w trakcie sesji"
                    # Można dodać karę, jeśli mocno spada
                    if change_percent_float < -5.0:
                        score -= 1 # Kara za duży spadek
                        details["Wniosek (Słabość)"] = "Spółka jest słaba dzisiaj (< -5%)"
                else: # Neutralnie
                    details["Wniosek (Dzienny)"] = "Neutralny zwrot w trakcie sesji"
            else:
                 details["Zmiana dzienna (live)"] = "Błąd konwersji"

        else:
            details["Zmiana dzienna (live)"] = "Brak danych"
            # Jeśli nie ma danych live, spróbujmy użyć danych EOD do oceny siły historycznej
            logger.warning(f"Brak danych live dla {ticker} w Agencie Momentum. Używam danych daily.")
            try:
                ticker_data_raw = api_client.get_daily_adjusted(ticker, 'compact')
                # get_performance używa danych daily, więc jest OK jako fallback
                # Użyjemy funkcji get_performance zaimportowanej globalnie na początku pliku, jeśli istnieje
                # LUB zdefiniujemy ją lokalnie, jeśli została usunięta z utils
                # Zakładając, że get_performance nadal istnieje:
                # from .utils import get_performance # Jeśli import byłby usunięty z góry
                ticker_perf_5d = get_performance(ticker_data_raw, 5) # Funkcja get_performance musi być dostępna
                if ticker_perf_5d is not None:
                    details["Zwrot (5 dni EOD)"] = f"{ticker_perf_5d:.2f}%"
                    if ticker_perf_5d > 5.0: # Jeśli wzrosła o >5% w 5 dni
                        score += 1
                        details["Wniosek (Siła EOD)"] = "Pozytywny zwrot w ostatnich 5 dniach."
            except NameError:
                 logger.error("Funkcja get_performance nie jest dostępna dla fallbacku Agenta Momentum.")
            except Exception as e_fallback:
                 logger.error(f"Błąd podczas fallbacku Agenta Momentum dla {ticker}: {e_fallback}")

    except Exception as e:
        logger.error(f"Błąd w Agencie Momentum dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Momentum", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "details": {}}

    # Dostosowanie podsumowania do nowej logiki punktacji
    if score >= 3:
        summary = "Spółka wykazuje bardzo silne momentum (wysoki zwrot dzienny)."
    elif score == 2:
        summary = "Spółka wykazuje solidne momentum."
    elif score == 1:
        summary = "Spółka ma lekko pozytywne momentum."
    elif score <= 0:
        summary = "Brak wyraźnych sygnałów siły lub widoczna słabość."

    # Upewniamy się, że score nie jest < 0 ani > max_score
    final_score = max(0, min(score, max_score))

    return {"name": "Agent Momentum", "score": final_score, "max_score": max_score, "summary": summary, "details": details}


# --- AGENT 2: ANALIZA KOMPRESJI ENERGII ---
def _run_volatility_agent(ticker: str, api_client: object) -> dict:
    """Agent analizujący zmienność (Bollinger Band Width Squeeze)."""
    score = 0
    max_score = 3
    details = {}

    try:
        # Pobieramy dane Bollinger Bands (domyślnie daily)
        bbands_data = api_client.get_bollinger_bands(ticker, time_period=20)
        tech_analysis = bbands_data.get('Technical Analysis: BBANDS')
        # Potrzebujemy wystarczająco długiej historii (np. 100 dni)
        if not tech_analysis or len(tech_analysis) < 100:
             return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Niewystarczająca historia danych do analizy BBW.", "details": {}}

        # Obliczanie Bollinger Band Width (BBW)
        bbw_values = []
        for date_str, values in tech_analysis.items():
            upper = safe_float(values.get('Real Upper Band'))
            lower = safe_float(values.get('Real Lower Band'))
            middle = safe_float(values.get('Real Middle Band'))
            # BBW = (Górna Wstęga - Dolna Wstęga) / Środkowa Wstęga
            if upper and lower and middle and middle > 0:
                bbw = (upper - lower) / middle
                bbw_values.append(bbw)

        if not bbw_values:
            return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Błąd obliczeń BBW.", "details": {}}

        # Używamy Pandas do obliczenia rangi procentowej
        bbw_series = pd.Series(bbw_values)
        # Pobieramy najnowszą wartość BBW (pierwszy element, bo dane są od najnowszych)
        current_bbw = bbw_series.iloc[0]
        # Obliczamy rangę procentową najnowszej wartości w stosunku do historii
        percentile_rank = bbw_series.rank(pct=True).iloc[0] * 100

        details["Ranga % BBW (100 dni)"] = f"{percentile_rank:.1f}%"

        # Punktacja na podstawie rangi procentowej BBW
        if percentile_rank < 10: # Bardzo niska zmienność (top 10%)
            score = 3
            summary = "Ekstremalna kompresja zmienności (BBW < 10%). Wysoki potencjał na gwałtowny ruch ceny."
        elif percentile_rank < 25: # Niska zmienność
            score = 2
            summary = "Zmienność jest niska (BBW < 25%). Potencjał na ruch ceny rośnie."
        elif percentile_rank < 40: # Zmienność poniżej średniej
            score = 1
            summary = "Zmienność poniżej średniej (BBW < 40%). Spółka w fazie konsolidacji."
        else: # Standardowa lub wysoka zmienność
            score = 0
            summary = "Standardowa lub wysoka zmienność. Brak oznak kompresji energii."

        details["Wniosek"] = summary

    except Exception as e:
        logger.error(f"Błąd w Agencie Zmienności dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Zmienności", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "details": {}}

    return {"name": "Agent Zmienności", "score": score, "max_score": max_score, "summary": summary, "details": details}


# --- AGENT 3: ANALIZA SENTYMENTU ---
def _run_sentiment_agent(ticker: str, api_client: object) -> dict:
    """Agent analizujący sentyment na podstawie wiadomości."""
    score = 0
    max_score = 3
    details = {}

    try:
        # Pobieramy dane o sentymencie
        news_data = api_client.get_news_sentiment(ticker)
        if not news_data or not news_data.get('feed'):
            return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Brak dostępnych wiadomości do analizy sentymentu.", "details": {}}

        # Filtrujemy i obliczamy średni sentyment
        relevant_scores = [item['overall_sentiment_score'] for item in news_data['feed'] if item.get('overall_sentiment_score') is not None]
        if not relevant_scores:
            return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Brak wiarygodnych ocen sentymentu w dostępnych wiadomościach.", "details": {}}

        avg_sentiment = sum(relevant_scores) / len(relevant_scores)
        details["Średni Sentyment"] = f"{avg_sentiment:.3f}"

        # Punktacja na podstawie średniego sentymentu
        if avg_sentiment >= 0.35: # Bardzo pozytywny
            score = 3
            summary = "Bardzo silny, jednoznacznie pozytywny sentyment w mediach."
        elif avg_sentiment >= 0.15: # Pozytywny
            score = 2
            summary = "Wyraźnie pozytywny sentyment w mediach."
        elif avg_sentiment > 0.0: # Lekko pozytywny
            score = 1
            summary = "Lekko pozytywny sentyment, przewaga byków."
        else: # Neutralny lub negatywny
            score = 0
            summary = "Neutralny lub negatywny sentyment."

        details["Wniosek"] = summary

    except Exception as e:
        logger.error(f"Błąd w Agencie Sentymentu dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Sentymentu", "score": 0, "max_score": max_score, "summary": "Błąd analizy.", "details": {}}

    return {"name": "Agent Sentymentu", "score": score, "max_score": max_score, "summary": summary, "details": details}


# --- AGENT 4: WYSZUKIWANIE SETUPU TAKTYCZNEGO (FAZA 3) ---
def _run_tactical_agent(ticker: str, api_client: object) -> dict:
    """Agent sprawdzający, czy istnieje aktualny setup EOD (End-of-Day)."""
    # Import lokalny, aby uniknąć problemów z cyklicznymi zależnościami
    from . import phase3_sniper

    details = {}
    max_score = 5 # Setup taktyczny ma dużą wagę

    try:
        # Wywołujemy funkcję szukającą setupów EOD
        trade_setup = phase3_sniper.find_end_of_day_setup(ticker, api_client)

        # Sprawdzamy wynik
        if trade_setup.get("signal"):
            score = 5 # Maksymalna liczba punktów, jeśli jest setup
            summary = "Znaleziono prawidłowy setup taktyczny EOD! Spółka jest w strukturze do potencjalnego wejścia."
            details["Status Setupu"] = trade_setup.get('status', 'NIEZNANY') # ACTIVE lub PENDING
            details["Typ Setupu"] = trade_setup.get('notes', 'Brak opisu').split(':')[0] # Wyciągamy typ z notatek

            # Wyświetlanie ceny lub strefy wejścia
            if trade_setup.get('entry_price'):
                details["Cena Wejścia (EOD)"] = f"${trade_setup['entry_price']:.2f}"
            elif trade_setup.get('entry_zone_bottom'):
                details["Strefa Wejścia (EOD)"] = f"${trade_setup['entry_zone_bottom']:.2f} - ${trade_setup['entry_zone_top']:.2f}"

            if trade_setup.get('take_profit'):
                details["Potencjalny Cel TP (EOD)"] = f"${trade_setup['take_profit']:.2f}"
            if trade_setup.get('stop_loss'):
                 details["Stop Loss (EOD)"] = f"${trade_setup['stop_loss']:.2f}"
        else:
            score = 0 # Brak punktów, jeśli nie ma setupu
            summary = "Brak setupu taktycznego EOD. Spółka nie jest obecnie w optymalnej strukturze do wejścia na podstawie analizy EOD."
            details["Status Setupu"] = "Brak Setupu EOD"
            details["Powód Braku Setupu"] = trade_setup.get('reason', 'Nieznany.')

    except Exception as e:
        logger.error(f"Błąd w Agencie Taktycznym dla {ticker}: {e}", exc_info=True)
        return {"name": "Agent Taktyczny", "score": 0, "max_score": max_score, "summary": "Błąd analizy setupu.", "details": {}}

    return {"name": "Agent Taktyczny", "score": score, "max_score": max_score, "summary": summary, "details": details}


# --- GŁÓWNA FUNKCJA ORKIESTRUJĄCA ---
def run_ai_analysis(ticker: str, api_client: object) -> dict:
    """
    Uruchamia wszystkich agentów AI i agreguje ich wyniki.
    POPRAWKA BŁĘDU #2: Teraz pobiera `quote_data` jako pierwszy krok i przekazuje je do agentów.
    POPRAWKA BŁĘDU #3: Używa `market_status_internal` z `quote_data`.
    """
    logger.info(f"Running full AI analysis for {ticker}...")

    # KROK 1: Pobierz dane na żywo i status rynku
    quote_data = api_client.get_live_quote_details(ticker) # Ta funkcja teraz sama określa status

    # KROK 2: Uruchom agentów, przekazując im dane na żywo
    momentum_results = _run_momentum_agent(ticker, api_client, quote_data)
    volatility_results = _run_volatility_agent(ticker, api_client)
    sentiment_results = _run_sentiment_agent(ticker, api_client)
    tactical_results = _run_tactical_agent(ticker, api_client)

    agents_list = [momentum_results, volatility_results, sentiment_results, tactical_results]

    # Obliczanie sumarycznego wyniku
    total_score = sum(agent['score'] for agent in agents_list)
    total_max_score = sum(agent['max_score'] for agent in agents_list)

    # Obliczanie wyniku procentowego
    final_score_percent = (total_score / total_max_score) * 100 if total_max_score > 0 else 0

    # Generowanie rekomendacji na podstawie wyniku i obecności setupu taktycznego
    if final_score_percent >= 75 and tactical_results['score'] > 0:
        recommendation = "BARDZO SILNY KANDDAT DO KUPNA"
        recommendation_details = "Spółka wykazuje wyjątkową siłę na wielu płaszczyznach i posiada prawidłowy setup taktyczny EOD."
    elif final_score_percent >= 60:
        recommendation = "SILNY KANDYDAT DO OBSERWACJI"
        recommendation_details = "Spółka ma wiele pozytywnych cech. Warto dodać do obserwowanych i czekać na setup lub potwierdzenie."
    elif final_score_percent >= 40:
        recommendation = "INTERESUJĄCY KANDYDAT"
        recommendation_details = "Spółka wykazuje pewne pozytywne sygnały, ale wymaga dalszej obserwacji i potwierdzenia siły."
    else:
        recommendation = "NEUTRALNY / ZALECA SIĘ OSTROŻNOĆ"
        recommendation_details = "Obecnie spółka nie wykazuje wystarczająco silnych sygnałów do podjęcia działań."

    # Przygotowanie informacji o rynku z obiektu quote_data
    market_info_from_quote = {
        "status": quote_data.get("market_status_internal", "UNKNOWN"), # Używamy naszego poprawnego statusu
        "time_ny": quote_data.get("time_ny", "N/A"),
        "date_ny": quote_data.get("date_ny", "N/A")
    }

    # Zwracany obiekt z wynikami
    return {
        "status": "DONE", # Status analizy
        "ticker": ticker,
        "quote_data": quote_data, # Pełne dane live
        "market_info": market_info_from_quote, # Poprawne informacje o rynku
        "overall_score": total_score,
        "max_score": total_max_score,
        "final_score_percent": round(final_score_percent),
        "recommendation": recommendation,
        "recommendation_details": recommendation_details,
        "agents": { # Wyniki poszczególnych agentów
            "momentum": momentum_results,
            "volatility": volatility_results,
            "sentiment": sentiment_results,
            "tactical_setup": tactical_results
        },
        "analysis_timestamp_utc": datetime.utcnow().isoformat() # Czas zakończenia analizy
    }

# Dodatkowa funkcja pomocnicza 'get_performance' (jeśli została usunięta z utils)
# Ta funkcja operuje na danych 'daily', więc używamy jej tylko w fallbacku Agenta Momentum
def get_performance(data: dict, days: int) -> float | None:
    """Oblicza zwrot procentowy w danym okresie na podstawie słownika danych daily."""
    try:
        time_series = data.get('Time Series (Daily)')
        if not time_series or len(time_series) < days + 1:
            return None

        # Sortujemy daty malejąco (od najnowszej)
        dates = sorted(time_series.keys(), reverse=True)

        # Pobieramy cenę zamknięcia z najnowszego dnia i 'days' dni wstecz
        end_price = safe_float(time_series[dates[0]]['4. close'])
        start_price = safe_float(time_series[dates[days]]['4. close'])

        # Sprawdzamy, czy mamy poprawne ceny i unikamy dzielenia przez zero
        if start_price is None or end_price is None or start_price == 0:
            return None

        # Obliczamy zwrot procentowy
        return ((end_price - start_price) / start_price) * 100
    except (IndexError, KeyError, TypeError) as e:
        logger.warning(f"Could not calculate EOD performance: {e}")
        return None

