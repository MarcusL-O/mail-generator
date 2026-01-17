# add_columns_hiring.py
import sqlite3
from pathlib import Path

# Kommentar: ÄNDRA INTE om du inte måste
DB_PATH = Path("data/companies.db.sqlite")

COLUMNS_TO_ADD = [
    ("hiring_status", "TEXT"),          # "yes" | "no" | "unknown"
    ("hiring_what_text", "TEXT"),       # kort sammanfattning: "DevOps; HR; Marketing"
    ("hiring_count", "INTEGER"),        # antal roller (0 om no, NULL om unknown)
    ("hiring_checked_at", "TEXT"),      # ISO UTC
    ("hiring_err_reason", "TEXT"),      # "" | "403" | "429" | "other" | "not_html"
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
