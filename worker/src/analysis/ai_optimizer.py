import logging
import os
import json
import requests # Potrzebne do wywołania API Gemini
import time
import random
from sqlalchemy.orm import Session
from sqlalchemy import text, func, cast, String
from decimal import Decimal
from collections import defaultdict

# Używamy modeli zdefiniowanych w głównym module
from .. import models
from .utils import append_scan_log, update_system_control

logger = logging.getLogger(__name__)

# ==================================================================
# === Konfiguracja API Gemini ===
# ==================================================================
# Ten klucz MUSI być ustawiony w zmiennych środowiskowych na Render.com
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY nie został znaleziony! Mega Agent Analityczny nie będzie działać.")
    # Nie wychodzimy z błędem, po prostu funkcja nie zadziała

# Używamy najnowszego modelu Flash do szybkiej i taniej analizy
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
API_HEADERS = {'Content-Type': 'application/json'}

# ==================================================================
# === Silnik Lokalnej Analizy Statystycznej ===
# (Ta część działa na serwerze Render, nie kosztuje nic API)
# ==================================================================

def _calculate_statistics(trades: list[models.VirtualTrade]) -> (dict, str):
    """
    Przetwarza tysiące transakcji w zwięzłe statystyki i raport tekstowy.
    Zwraca (dane_statystyczne, raport_tekstowy_dla_AI).
    """
    if not trades:
        return {}, "Brak transakcji do analizy."

    # defaultdict ułatwia grupowanie
    # Klucz: setup_type (np. "BACKTEST_TRUMP_2019_EMA_BOUNCE")
    # Wartość: lista P/L (procentów)
    stats_by_setup = defaultdict(list)
    
    for trade in trades:
        if trade.final_profit_loss_percent is not None:
            stats_by_setup[trade.setup_type].append(float(trade.final_profit_loss_percent))

    if not stats_by_setup:
        return {}, "Brak zakończonych transakcji do analizy."

    text_report_lines = ["PODSUMOWANIE WYNIKÓW HISTORYCZNYCH:\n"]
    full_stats = {}

    for setup_name, results in stats_by_setup.items():
        total_trades = len(results)
        wins = [r for r in results if r > 0]
        losses = [r for r in results if r < 0]
        
        num_wins = len(wins)
        num_losses = len(losses)
        
        win_rate = (num_wins / total_trades) * 100 if total_trades > 0 else 0
        
        total_p_l = sum(results)
        total_win_p_l = sum(wins)
        total_loss_p_l = sum(losses) # Ta wartość jest ujemna
        
        avg_win = total_win_p_l / num_wins if num_wins > 0 else 0
        avg_loss = total_loss_p_l / num_losses if num_losses > 0 else 0
        
        # Profit Factor = (Całkowity zysk) / (Całkowita strata jako wartość absolutna)
        profit_factor = abs(total_win_p_l / total_loss_p_l) if total_loss_p_l != 0 else float('inf')

        # Zapisz pełne statystyki (na przyszłość)
        full_stats[setup_name] = {
            "total_trades": total_trades,
            "win_rate_percent": win_rate,
            "total_p_l_percent": total_p_l,
            "profit_factor": profit_factor
        }
        
        # Dodaj do raportu tekstowego dla AI
        text_report_lines.append(f"--- Strategia: {setup_name} ---")
        text_report_lines.append(f"  Liczba transakcji: {total_trades}")
        text_report_lines.append(f"  Skuteczność (Win Rate): {win_rate:.1f}%")
        text_report_lines.append(f"  Całkowity P/L (suma %): {total_p_l:.1f}%")
        text_report_lines.append(f"  Współczynnik zyskowności (Profit Factor): {profit_factor:.2f}")
        text_report_lines.append(f"  Śr. Zysk / Śr. Strata: {avg_win:.1f}% / {avg_loss:.1f}%\n")

    return full_stats, "\n".join(text_report_lines)

# ==================================================================
# === Komunikacja z API Gemini (Tylko 1 zapytanie) ===
# ==================================================================

def _call_gemini_api_for_analysis(stats_report: str) -> str:
    """
    Wysyła JEDNO zapytanie do API Gemini z prośbą o analizę
    statystyk tekstowych wygenerowanych lokalnie.
    """
    if not GEMINI_API_KEY:
        logger.error("Mega Agent: Brak klucza GEMINI_API_KEY. Analiza AI niemożliwa.")
        return "BŁĄD: Brak klucza GEMINI_API_KEY. Skonfiguruj go w Render.com."

    # Czas na odpowiedź AI
    time.sleep(1.0 + random.uniform(0, 0.5)) 
    
    prompt = f"""
    Jesteś "Mega Agentem Analitycznym" dla systemu APEX Predator. Twoim zadaniem jest analiza wyników backtestingu i zaproponowanie konkretnych, możliwych do wdrożenia optymalizacji w kodzie, aby poprawić rentowność.

    Otrzymałeś następujący raport statystyczny z bazy danych:

    {stats_report}

    Bazując *wyłącznie* na tych danych, wykonaj następujące zadania:
    1.  Dokonaj zwięzłej analizy (1-2 akapity): Które strategie (np. EMA_BOUNCE) są zyskowne? Jak reżim rynkowy (np. ROK_2019 vs ROK_2022) wpływa na ich skuteczność?
    2.  Zaproponuj 3 KONKRETNE, możliwe do wdrożenia sugestie optymalizacyjne. Skup się na parametrach, które możemy zmienić:
        * `TARGET_RR_RATIO` (Obecnie 1.5): Czy powinniśmy go zwiększyć, czy zmniejszyć dla konkretnej strategii lub reżimu?
        * `MAX_HOLD_DAYS` (Obecnie 7): Czy powinniśmy trzymać pozycje dłużej, czy krócej?
        * `FILTRY FAZY 1` (np. `MAX_VOLATILITY_ATR_PERCENT` = 0.12): Czy dane sugerują, że powinniśmy filtrować inaczej?

    Odpowiedz zwięźle, w punktach, po polsku. Formatuj odpowiedź używając Markdown.
    """
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        # Nie potrzebujemy schematu JSON, oczekujemy odpowiedzi tekstowej (raportu)
    }

    max_retries = 3
    initial_backoff = 3

    for attempt in range(max_retries):
        try:
            response = requests.post(GEMINI_API_URL, headers=API_HEADERS, data=json.dumps(payload), timeout=60) # Dłuższy timeout na analizę
            response.raise_for_status()
            data = response.json()
            
            text_content = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', 'BŁĄD: Otrzymano pustą odpowiedź od API Gemini.')
            
            logger.info("Mega Agent: Pomyślnie otrzymano analizę AI.")
            return text_content # Zwracamy raport tekstowy

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = (initial_backoff * (2 ** attempt)) + random.uniform(0, 1)
                logger.warning(f"Mega Agent: Rate limit (429) (Próba {attempt + 1}/{max_retries}). Ponawiam za {wait:.2f}s...")
                time.sleep(wait)
                continue
            else:
                logger.error(f"Mega Agent: Błąd HTTP (inny niż 429) podczas wywołania Gemini: {e}", exc_info=True)
                return f"BŁĄD: Wystąpił błąd HTTP {e.response.status_code} podczas kontaktu z API Gemini."
        except requests.exceptions.RequestException as e:
            logger.error(f"Mega Agent: Błąd sieciowy podczas wywołania Gemini: {e}", exc_info=True)
            return f"BŁĄD: Błąd sieciowy podczas kontaktu z API Gemini."
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.error(f"Mega Agent: Błąd przetwarzania odpowiedzi JSON z Gemini: {e}", exc_info=True)
            return f"BŁĄD: Nie można przetworzyć odpowiedzi z API Gemini."
    
    logger.error(f"Mega Agent: Nie udało się uzyskać analizy AI po {max_retries} próbach.")
    return "BŁĄD: Nie udało się uzyskać analizy AI po wielu próbach."

# ==================================================================
# === Główna Funkcja Uruchamiająca ===
# ==================================================================

def run_ai_optimization_analysis(session: Session):
    """
    Główna funkcja uruchamiająca "Mega Agenta".
    Uruchamiana na żądanie przez workera.
    """
    log_msg = "🤖 MEGA AGENT: Rozpoczynanie analizy wydajności..."
    logger.info(log_msg)
    append_scan_log(session, log_msg)
    
    try:
        # Krok 1: Pobierz wszystkie dane z bazy
        logger.info("🤖 MEGA AGENT: Pobieranie wszystkich zamkniętych transakcji z bazy danych...")
        all_trades = session.query(models.VirtualTrade).filter(
            models.VirtualTrade.status != 'OPEN'
        ).all()
        
        if not all_trades:
            log_msg = "🤖 MEGA AGENT: Brak zamkniętych transakcji w bazie danych. Analiza niemożliwa."
            logger.warning(log_msg)
            append_scan_log(session, log_msg)
            # Zapisz pusty raport (na potrzeby UI)
            update_system_control(session, 'ai_optimizer_report', log_msg)
            return

        # Krok 2: Przetwórz dane lokalnie
        logger.info(f"🤖 MEGA AGENT: Przetwarzanie {len(all_trades)} transakcji lokalnie...")
        stats_data, text_report = _calculate_statistics(all_trades)
        
        log_msg = f"🤖 MEGA AGENT: Statystyki lokalne wygenerowane. Wysyłanie raportu do analizy API Gemini..."
        logger.info(log_msg)
        append_scan_log(session, log_msg)

        # Krok 3: Wyślij JEDNO zapytanie do API Gemini
        ai_analysis_report = _call_gemini_api_for_analysis(text_report)
        
        # Krok 4: Zapisz pełny raport (lokalne statystyki + analiza AI) w bazie
        
        # Tworzymy ostateczny raport do zapisania (dla UI)
        final_report_text = f"ANALIZA MEGA AGENTA (z dnia {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})\n\n"
        final_report_text += "=== SUGESTIE OPTYMALIZACYJNE (od AI) ===\n\n"
        final_report_text += ai_analysis_report
        final_report_text += "\n\n\n=== SUROWE DANE STATYSTYCZNE (lokalne) ===\n\n"
        final_report_text += text_report
        
        # Zapisujemy w system_control (wystarczy, nie potrzebujemy nowej tabeli)
        update_system_control(session, 'ai_optimizer_report', final_report_text)
        
        log_msg = "🤖 MEGA AGENT: Analiza zakończona. Raport zapisany w system_control."
        logger.info(log_msg)
        append_scan_log(session, log_msg)

    except Exception as e:
        log_msg = f"BŁĄD KRYTYCZNY MEGA AGENTA: {e}"
        logger.error(log_msg, exc_info=True)
        append_scan_log(session, log_msg)
        update_system_control(session, 'ai_optimizer_report', log_msg)
