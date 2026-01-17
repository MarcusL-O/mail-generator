# add_columns_employees.py
import sqlite3
from pathlib import Path

DB_PATH = Path("data/companies.db.sqlite")

COLUMNS_TO_ADD = [
    ("employees_class", "TEXT"),          # ex "20-49", "500+"
    ("employees_checked_at", "TEXT"),     # ISO UTC
    ("employees_status", "TEXT"),         # ok | unknown | not_found | err
    ("employees_next_check_at", "TEXT"),  # ISO UTC (checked_at + 90d)
]

def column_exists(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return col in [r[1] for r in cur.fetchall()]

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    added = skipped = 0
    for col, typ in COLUMNS_TO_ADD:
        if column_exists(conn, "companies", col):
            skipped += 1
            continue
        cur.execute(f"ALTER TABLE companies ADD COLUMN {col} {typ};")
        added += 1

    conn.commit()
    conn.close()

    print("KLART ✅")
    print(f"Added: {added}, Skipped: {skipped}")

if __name__ == "__main__":
    main()
