# scripts_outreach/send/shared/send_utils.py
# helpers: batch, throttling, dry-run, etc
# kopplar upp mot outreach.db
# läser settings (limits, dry-run, etc.)
# tolkar emails (JSON, CSV eller single)
# väljer primär mottagar-email
# skapar rader i email_messages (loggar varje utskick/försök)
# loggar händelser i events
# hanterar timestamps och output-mappar

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

OUTREACH_DB_PATH = Path("data/db/outreach.db.sqlite")


# Kommentar (svenska): ISO-tid i UTC för DB-loggning
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path: Path = OUTREACH_DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def get_setting(con: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    cur = con.cursor()
    cur.execute("SELECT value FROM settings WHERE key = ? LIMIT 1", (key,))
    row = cur.fetchone()
    return str(row["value"]) if row else default


def get_int_setting(con: sqlite3.Connection, key: str, default: int) -> int:
    v = get_setting(con, key, None)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except ValueError:
        return default


def get_float_setting(con: sqlite3.Connection, key: str, default: float) -> float:
    v = get_setting(con, key, None)
    if v is None:
        return default
    try:
        return float(str(v).strip())
    except ValueError:
        return default


def is_dry_run(con: sqlite3.Connection) -> bool:
    # Kommentar (svenska): 1/true => dry-run
    v = (get_setting(con, "dry_run", "1") or "1").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def parse_emails(value: Optional[str]) -> List[str]:
    """
    Kommentar (svenska):
    leads.emails / companies.emails är tänkt som JSON-text, men kan vara tomt.
    Vi stödjer:
      - JSON array: ["a@x.se","b@x.se"]
      - CSV: "a@x.se,b@x.se"
      - single: "a@x.se"
    """
    if not value:
        return []

    s = value.strip()
    if not s:
        return []

    # JSON array?
    if s.startswith("["):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass

    # CSV / single
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s]


def choose_primary_email(emails_value: Optional[str]) -> Optional[str]:
    emails = parse_emails(emails_value)
    return emails[0] if emails else None


def ensure_out_dir() -> Path:
    out_dir = Path("data/out")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def upsert_email_message(
    con: sqlite3.Connection,
    *,
    lead_id: int,
    campaign_id: int,
    template_id: Optional[int],
    step: int,
    variant: Optional[str],
    to_email: str,
    from_email: str,
    subject_rendered: str,
    body_rendered: str,
    status: str,
    scheduled_at: Optional[str] = None,
    sent_at: Optional[str] = None,
    error: Optional[str] = None,
) -> int:
    """
    Kommentar (svenska):
    Skapar en rad i email_messages. Returnerar message_id.
    Vi gör INSERT (inte upsert) för att logga varje försök/utskick som egen rad.
    """
    ts = now_iso()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO email_messages
        (lead_id, campaign_id, template_id, step, variant, to_email, from_email,
         subject_rendered, body_rendered, status, scheduled_at, sent_at, error, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            campaign_id,
            template_id,
            step,
            variant,
            to_email,
            from_email,
            subject_rendered,
            body_rendered,
            status,
            scheduled_at,
            sent_at,
            error,
            ts,
            ts,
        ),
    )
    return int(cur.lastrowid)


def insert_event(
    con: sqlite3.Connection,
    *,
    lead_id: int,
    campaign_id: int,
    message_id: Optional[int],
    event_type: str,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Kommentar (svenska):
    events.type har CHECK constraint (sent/reply/bounce/booked/won/lost/unsubscribe)
    I MVP använder vi främst 'sent' och ev 'bounce'/'failed' senare.
    """
    ts = now_iso()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            campaign_id,
            message_id,
            event_type,
            json.dumps(meta or {}, ensure_ascii=False),
            ts,
        ),
    )
    return int(cur.lastrowid)
