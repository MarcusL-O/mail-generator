# add_columns_tech.py
import sqlite3
from pathlib import Path

# Kommentar: ÄNDRA INTE om du inte måste
DB_PATH = Path("data/companies.db.sqlite")

COLUMNS_TO_ADD = [
    # Kommentar: Microsoft-signal
    ("microsoft_status", "TEXT"),        # "yes" | "no" | "unknown"
    ("microsoft_strength", "TEXT"),      # "weak" | "strong" | NULL
    ("microsoft_confidence", "TEXT"),    # "high" | "medium" | "low"

    # Kommentar: IT-support-signal
    ("it_support_signal", "TEXT"),       # "yes" | "no" | "unknown"
    ("it_support_confidence", "TEXT"),   # "high" | "medium" | "low"

    # Kommentar: meta
    ("tech_checked_at", "TEXT"),         # ISO UTC
    ("tech_err_reason", "TEXT"),         # "" | "403" | "429" | "other" | "not_html"
]

def column_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]  # r[1] = name
    return col in cols

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH.as_posix())
    try:
        cur = conn.cursor()
        table = "companies"

        added = 0
        skipped = 0

        for col, coltype in COLUMNS_TO_ADD:
            if column_exists(conn, table, col):
                skipped += 1
                continue
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")
            added += 1

        conn.commit()

        print("KLART ✅")
        print(f"DB: {DB_PATH}")
        print(f"Added columns: {added}")
        print(f"Already existed: {skipped}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
