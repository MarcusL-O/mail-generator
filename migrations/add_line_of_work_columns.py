# companies/control/migrations/add_line_of_work_columns.py
# Kommentar: lägger till kolumner i companies (idempotent)

import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/companies.db.sqlite")

NEW_COLUMNS = [
    ("line_of_work", "TEXT"),
    ("line_of_work_raw", "TEXT"),
    ("line_of_work_conf", "REAL"),
    ("line_of_work_bucket", "TEXT"),
    ("line_of_work_source", "TEXT"),
    ("line_of_work_updated_at", "TEXT"),
]

def get_existing_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # row[1] = name

def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH.as_posix())
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        existing = get_existing_cols(conn, "companies")

        added = 0
        for col, coltype in NEW_COLUMNS:
            if col in existing:
                continue
            # Kommentar: SQLite stödjer ALTER TABLE ADD COLUMN
            conn.execute(f"ALTER TABLE companies ADD COLUMN {col} {coltype}")
            added += 1
            print(f"Added: {col} {coltype}")

        conn.commit()
        print(f"KLART ✅ Added {added} column(s).")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
