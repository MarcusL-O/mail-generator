# migrate_add_scb_discover_columns.py
# Kommentar: Lägger till SCB "discover"-kolumner i companies + state-tabell.
# Kommentar: Safe migration: ADD COLUMN bara om kolumnen saknas.

import os
import sqlite3
from dotenv import load_dotenv

DB_PATH_DEFAULT = "data/companies.db.sqlite"
TABLE = "companies"

DISCOVER_COLS = {
    "scb_registration_date": "TEXT",
    "scb_legal_form": "TEXT",
    "scb_company_status": "TEXT",
    "scb_sector": "TEXT",
    "scb_private_public": "TEXT",
    "scb_discovered_at": "TEXT",
}

def get_db_path() -> str:
    return os.getenv("DB_PATH", DB_PATH_DEFAULT).strip()

def existing_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cur.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}

def main():
    load_dotenv()
    db_path = get_db_path()

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Kommentar: WAL för robusthet vid avbrott
    con.execute("PRAGMA journal_mode=WAL;")

    cols = existing_columns(cur, TABLE)

    added = 0
    for col, coltype in DISCOVER_COLS.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col} {coltype};")
            added += 1

    # Kommentar: State-tabell för "ingen delta" => vi sparar var vi var
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scb_discover_state (
            id INTEGER PRIMARY KEY CHECK (id=1),
            last_registration_date TEXT,
            last_page INTEGER,
            updated_at TEXT
        );
    """)
    cur.execute("""
        INSERT OR IGNORE INTO scb_discover_state (id, last_registration_date, last_page, updated_at)
        VALUES (1, '1970-01-01', 1, datetime('now'));
    """)

    con.commit()
    con.close()

    print("✅ Migration klar")
    print(f"DB: {db_path}")
    print(f"Added columns: {added}")
    print("State table: scb_discover_state (ok)")

if __name__ == "__main__":
    main()
