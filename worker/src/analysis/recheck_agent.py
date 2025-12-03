import logging
import os
import json
import time
import random
import requests
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone

from .. import models
from .utils import append_scan_log, update_system_control

logger = logging.getLogger(__name__)

# Konfiguracja API Gemini (Mózg Agenta)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
API_HEADERS = {'Content-Type': 'application/json'}

def _call_gemini_auditor(trade_context: dict) -> dict:
    """
    Wysyła dane o zakończonym setupie do Gemini w celu przeprowadzenia audytu 'Re-check'.
    Zwraca słownik z raportem tekstowym i sugestiami optymalizacyjnymi.
    """
    if not GEMINI_API_KEY:
        return {"report": "Brak klucza API Gemini. Audyt niemożliwy.", "suggestion": None}

    # Opóźnienie dla rate-limitingu
    time.sleep(1.5 + random.uniform(0, 0.5))

    prompt = f"""
    Jesteś Agentem Re-check (Audytorem Strategii) w systemie tradingowym APEX. 
    Twoim zadaniem jest bezlitosna weryfikacja skuteczności setupów wygenerowanych przez algorytmy.
    
    Analizujemy zakończony setup na spółce: {trade_context['ticker']}
    
    === DANE WEJŚCIOWE (OBIETNICA) ===
    Strategia/Setup: {trade_context['setup_type']}
    Oczekiwany Profit Factor (z backtestu): {trade_context['expected_pf']}
    Oczekiwany Win Rate (z backtestu): {trade_context['expected_wr']}%
    
    === RZECZYWISTOŚĆ (WYNIK) ===
    Status wyjścia: {trade_context['status']}
    Cena wejścia: {trade_context['entry_price']}
    Cena wyjścia: {trade_context['close_price']}
    Wynik P/L: {trade_context['p_l_percent']}%
    Czas trwania: {trade_context['duration_days']} dni
    
    === KONTEKST METRYK (W MOMENCIE WEJŚCIA) ===
    AQM Score: {trade_context.get('metric_aqm_score_h3')}
    Retail Herding (Tłum): {trade_context.get('metric_retail_herding')}
    Institutional Sync (Instytucje): {trade_context.get('metric_inst_sync')}
    
    ZADANIE:
    1. Porównaj Oczekiwania z Rzeczywistością. Czy strategia "dowiezła" wynik?
    2. Zidentyfikuj przyczynę porażki (lub sukcesu). Czy wina leży w złym timingu (zbyt wczesne wejście), fałszywym sygnale (pułapka na byki), czy może czynnikach zewnętrznych?
    3. Podaj KONKRETNĄ rekomendację dla Optymalizatora (np. "Podnieś próg AQM", "Skróć czas trwania", "Zwiększ wymóg Institutional Sync").
    
    Format odpowiedzi: JSON z polami:
    - "audit_summary": (String) Zwięzły raport tekstowy dla tradera (Wnioski).
    - "optimization_tweak": (Object) Sugerowane zmiany parametrów (np. {{"h3_min_score": "+0.1", "h3_max_hold": "-1"}}).
    """

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    }

    try:
        response = requests.post(GEMINI_API_URL, headers=API_HEADERS, data=json.dumps(payload), timeout=30)
        response.raise_for_status()
        data = response.json()
        
        text_content = data.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')
        return json.loads(text_content)
        
    except Exception as e:
        logger.error(f"Re-check Agent: Błąd komunikacji z AI: {e}")
        return {"report": f"Błąd audytu AI: {str(e)}", "suggestion": None}

def run_recheck_audit_cycle(session: Session):
    """
    Główna pętla Agenta Re-check.
    Skanuje zakończone transakcje, które posiadają 'Oczekiwania' (expected_pf)
    ale nie mają jeszcze raportu (ai_audit_report).
    """
    logger.info("🕵️ Re-check Agent: Rozpoczynanie cyklu audytowego...")
    
    # 1. Znajdź transakcje do audytu
    # Warunki:
    # - Status zakończony (TP, SL, EXPIRED)
    # - Posiadają dane z Optymalizatora (expected_profit_factor IS NOT NULL) - to odróżnia nowe setupy od starych
    # - Nie były jeszcze audytowane (ai_audit_report IS NULL)
    
    trades_to_audit = session.query(models.VirtualTrade).filter(
        models.VirtualTrade.status.in_(['CLOSED_TP', 'CLOSED_SL', 'CLOSED_EXPIRED']),
        models.VirtualTrade.expected_profit_factor.isnot(None),
        models.VirtualTrade.ai_audit_report.is_(None)
    ).limit(5).all() # Limit 5 na cykl, żeby nie zabić API
    
    if not trades_to_audit:
        # Cicha praca w tle - nie logujemy jeśli nie ma pracy, żeby nie śmiecić
        return

    append_scan_log(session, f"🕵️ Re-check Agent: Znaleziono {len(trades_to_audit)} setupów do weryfikacji.")
    
    for trade in trades_to_audit:
        try:
            # 2. Przygotuj kontekst dla AI
            duration = 0
            if trade.close_date and trade.open_date:
                duration = (trade.close_date - trade.open_date).days
            
            context = {
                "ticker": trade.ticker,
                "setup_type": trade.setup_type,
                "expected_pf": float(trade.expected_profit_factor),
                "expected_wr": float(trade.expected_win_rate),
                "status": trade.status,
                "entry_price": float(trade.entry_price),
                "close_price": float(trade.close_price) if trade.close_price else 0.0,
                "p_l_percent": float(trade.final_profit_loss_percent) if trade.final_profit_loss_percent else 0.0,
                "duration_days": duration,
                # Metryki
                "metric_aqm_score_h3": float(trade.metric_aqm_score_h3) if trade.metric_aqm_score_h3 else None,
                "metric_retail_herding": float(trade.metric_retail_herding) if trade.metric_retail_herding else None,
                "metric_inst_sync": float(trade.metric_inst_sync) if trade.metric_inst_sync else None
            }
            
            logger.info(f"Re-check: Audytowanie {trade.ticker} (P/L: {context['p_l_percent']}%)")
            
            # 3. Wywołaj Mózg (Gemini)
            audit_result = _call_gemini_auditor(context)
            
            report_text = audit_result.get("audit_summary", "Brak raportu.")
            suggestions = audit_result.get("optimization_tweak", {})
            
            # 4. Zapisz wyniki w bazie
            trade.ai_audit_report = report_text
            trade.ai_optimization_suggestion = suggestions
            trade.ai_audit_date = datetime.now(timezone.utc)
            
            session.commit()
            
            # Loguj ciekawsze wnioski
            if "FAIL" in report_text or context['p_l_percent'] < 0:
                append_scan_log(session, f"📉 Re-check Wniosek ({trade.ticker}): {report_text[:100]}...")
            else:
                append_scan_log(session, f"✅ Re-check Potwierdzenie ({trade.ticker}): Strategia skuteczna.")
                
        except Exception as e:
            logger.error(f"Błąd audytu dla {trade.ticker}: {e}")
            session.rollback()
            continue

    logger.info("Re-check Agent: Cykl zakończony.")
