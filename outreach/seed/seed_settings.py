# seed/seed_settings.py
# Skapar grundinställningar (default template, delays, from-name).
# Central konfig i DB.
# Skapar tabellen settings i outreach.db om den inte finns
# Lägger in globala outreach-inställningar (from-email, limits, delays, dry-run m.m.)
# Kör upsert → samma script kan köras flera gånger utan dubletter
# Läser värden från env om de finns, annars använder defaults
# Sparar inga hemligheter (lösenord ska ligga i .env, inte DB)
# Resultat:
# send-scripten har en gemensam plats att läsa regler från innan mejl skickas.

import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(os.getenv("OUTREACH_DB_PATH", "data/db/outreach.db.sqlite"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_table(cur: sqlite3.Cursor):
    # Kommentar (svenska): Enkel key/value-tabell för globala outreach-inställningar
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
          key        TEXT PRIMARY KEY,
          value      TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS ix_settings_key ON settings(key)")


def upsert_setting(cur: sqlite3.Cursor, key: str, value: str):
    ts = now_iso()
    cur.execute(
        """
        INSERT INTO settings (key, value, created_at, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (key, value, ts, ts),
    )


def seed():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    ensure_table(cur)

    # Kommentar (svenska): Defaults + möjlighet att override:a via env.
    # OBS: lägg INTE lösenord/API-nycklar i DB.
    defaults = {
        # Identitet
        "from_email": os.getenv("OUTREACH_FROM_EMAIL", "no-reply@example.com"),
        "from_name": os.getenv("OUTREACH_FROM_NAME", "Marcus"),
        "reply_to": os.getenv("OUTREACH_REPLY_TO", ""),

        # Sändning / throttling
        "daily_send_limit": os.getenv("OUTREACH_DAILY_SEND_LIMIT", "200"),
        "per_minute_limit": os.getenv("OUTREACH_PER_MINUTE_LIMIT", "30"),
        "sleep_between_sends_seconds": os.getenv("OUTREACH_SLEEP_BETWEEN_SENDS_SECONDS", "1.0"),

        # Sekvens-timing (för lead_campaigns / next_send_at logik senare)
        "min_delay_between_steps_hours": os.getenv("OUTREACH_MIN_DELAY_BETWEEN_STEPS_HOURS", "24"),
        "max_delay_between_steps_hours": os.getenv("OUTREACH_MAX_DELAY_BETWEEN_STEPS_HOURS", "72"),

        # Driftläge
        "dry_run": os.getenv("OUTREACH_DRY_RUN", "1"),  # 1 = logga men skicka inte

        # Tidzon (för schemaläggning)
        "timezone": os.getenv("OUTREACH_TIMEZONE", "Europe/Stockholm"),

        # Prioritering (nytt, säkra defaults)
        # Kommentar (svenska): 1 = sortera så tier 1 går före, NULL/utan tier hamnar sist
        "prioritize_tier": os.getenv("OUTREACH_PRIORITIZE_TIER", "1"),
        # Kommentar (svenska): 1 = inom samma tier, välj högst score först
        "prioritize_score": os.getenv("OUTREACH_PRIORITIZE_SCORE", "1"),

        # Batch (valfritt men ofta praktiskt i send)
        # Kommentar (svenska): hur många "due" lead_campaigns send får plocka per körning
        "max_due_batch": os.getenv("OUTREACH_MAX_DUE_BATCH", "500"),
    }

    for k, v in defaults.items():
        upsert_setting(cur, k, str(v))

    conn.commit()
    conn.close()
    print("✓ Settings seeded")
    print(f"db={DB_PATH}")
    print(f"keys={len(defaults)}")


if __name__ == "__main__":
    seed()
