import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def update_system_control(session: Session, key: str, value: str):
    """Aktualizuje lub wstawia wartość w tabeli system_control (UPSERT)."""
    try:
        stmt = text("""
            INSERT INTO system_control (key, value, updated_at)
            VALUES (:key, :value, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = NOW();
        """)
        session.execute(stmt, {'key': key, 'value': str(value)})
        session.commit()
    except Exception as e:
        logger.error(f"Error updating system_control for key {key}: {e}")
        session.rollback()

def get_system_control_value(session: Session, key: str) -> str | None:
    """Odczytuje pojedynczą wartość z tabeli system_control."""
    try:
        result = session.execute(text("SELECT value FROM system_control WHERE key = :key"), {'key': key}).fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting system_control value for key {key}: {e}")
        return None

def update_scan_progress(session: Session, processed: int, total: int):
    """Aktualizuje postęp skanowania w bazie danych."""
    update_system_control(session, 'scan_progress_processed', str(processed))
    update_system_control(session, 'scan_progress_total', str(total))

def append_scan_log(session: Session, message: str):
    """Dodaje nową linię do logu skanowania w bazie danych."""
    try:
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] {message}\n"
        
        # Użycie `||` do konkatenacji stringów w PostgreSQL
        stmt = text("""
            UPDATE system_control
            SET value = value || :message
            WHERE key = 'scan_log';
        """)
        session.execute(stmt, {'message': log_message})
        session.commit()
    except Exception as e:
        logger.error(f"Error appending to scan_log: {e}")
        session.rollback()

def check_for_commands(session: Session, current_state: str) -> tuple[bool, str]:
    """Sprawdza i reaguje na polecenia z bazy danych."""
    command = get_system_control_value(session, 'worker_command')
    should_run_now = False
    new_state = current_state

    if command == "START_REQUESTED":
        logger.info("Start command received, triggering immediate analysis cycle.")
        update_system_control(session, 'worker_command', 'NONE')
        should_run_now = True
    elif command == "PAUSE_REQUESTED" and current_state == "RUNNING":
        new_state = "PAUSED"
        update_system_control(session, 'worker_status', 'PAUSED')
        update_system_control(session, 'worker_command', 'NONE')
        logger.info("Worker paused by command.")
    elif command == "RESUME_REQUESTED" and current_state == "PAUSED":
        new_state = "RUNNING"
        update_system_control(session, 'worker_status', 'RUNNING')
        update_system_control(session, 'worker_command', 'NONE')
        logger.info("Worker resumed by command.")
        
    return should_run_now, new_state

def report_heartbeat(session: Session):
    """Raportuje 'życie' workera do bazy danych."""
    update_system_control(session, 'last_heartbeat', datetime.now(timezone.utc).isoformat())

def safe_float(value) -> float | None:
    """Bezpiecznie konwertuje wartość na float."""
    if value is None: return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def get_performance(data: dict, days: int) -> float | None:
    """Oblicza zwrot procentowy w danym okresie."""
    try:
        time_series = data.get('Time Series (Daily)')
        if not time_series or len(time_series) < days: return None
        
        dates = sorted(time_series.keys())
        end_price = safe_float(time_series[dates[-1]]['4. close'])
        start_price = safe_float(time_series[dates[-days]]['4. close'])
        
        if start_price is None or end_price is None or start_price == 0: return None
        
        return ((end_price - start_price) / start_price) * 100
    except (IndexError, KeyError, TypeError) as e:
        logger.warning(f"Could not calculate performance: {e}")
        return None

