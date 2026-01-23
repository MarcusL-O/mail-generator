# companies/control/migrate_add_segment_groups.py
# Kommentar:
# Lägger till segment_groups + segment_groups_checked_at i companies om de saknas.
# Kör:
#   python companies/control/migrate_add_segment_groups.py

import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/companies.db.sqlite")

def has_column(con: sqlite3.Connection, table: str, col: str) -> bool:
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {str(r[1]) for r in cur.fetchall()}
    return col in cols

def add_column(con: sqlite3.Connection, table: str, col: str, decl: str) -> None:
    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    con = sqlite3.connect(DB_PATH.as_posix())
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("BEGIN;")

        if not has_column(con, "companies", "segment_groups"):
            # Kommentar: CSV utan mellanslag, samma stil som sni_groups
            add_column(con, "companies", "segment_groups", "TEXT")
            print("ADD: companies.segment_groups")

        if not has_column(con, "companies", "segment_groups_checked_at"):
            # Kommentar: ISO timestamp, används för refresh
            add_column(con, "companies", "segment_groups_checked_at", "TEXT")
            print("ADD: companies.segment_groups_checked_at")

        con.commit()
        print("KLART ✅")

    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()
