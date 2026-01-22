#Läser signaturer från disk.
#Sparar dem i outreach-DB.
#säkerställer att tabellen settings finns i outreach.db
#Kontrollerar att neutral signature (neutral.html + neutral.txt) finns i tabellen templates
#Sätter dessa som aktiva signatures i settings
#Kör upsert → kan köras flera gånger utan dubletter
#MVP: samma neutral signature används för alla mejl


import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/db/outreach.db.sqlite")

# Kommentar (svenska): Matchar seed_templates.py:
# name = f"signature_{rel.as_posix()}"
NEUTRAL_HTML = "signature_neutral/neutral.html"
NEUTRAL_TXT = "signature_neutral/neutral.txt"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_settings_table(cur: sqlite3.Cursor):
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


def template_exists(cur: sqlite3.Cursor, name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM templates WHERE name = ? AND channel = 'signature' LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def seed():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    ensure_settings_table(cur)

    missing = []
    if not template_exists(cur, NEUTRAL_HTML):
        missing.append(NEUTRAL_HTML)
    if not template_exists(cur, NEUTRAL_TXT):
        missing.append(NEUTRAL_TXT)

    if missing:
        conn.close()
        raise SystemExit(
            "Saknar signatures i DB. Kör seed_templates.py först eller fixa paths:\n- "
            + "\n- ".join(missing)
        )

    # Kommentar (svenska): MVP = en neutral signature för alla
    upsert_setting(cur, "active_signature_html", NEUTRAL_HTML)
    upsert_setting(cur, "active_signature_txt", NEUTRAL_TXT)

    conn.commit()
    conn.close()

    print("✓ Signatures seeded")
    print(f"active_signature_html={NEUTRAL_HTML}")
    print(f"active_signature_txt={NEUTRAL_TXT}")


if __name__ == "__main__":
    seed()
