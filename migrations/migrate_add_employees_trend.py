# migrate_add_employees_trend.py
import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/companies.db.sqlite")

COLUMNS = [
    ("employees_trend", "TEXT"),
    ("employees_trend_at", "TEXT"),
]

def column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table});")
    return col in [r[1] for r in cur.fetchall()]

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    added = skipped = 0
    for col, typ in COLUMNS:
        if column_exists(cur, "companies", col):
            skipped += 1
            continue
        cur.execute(f"ALTER TABLE companies ADD COLUMN {col} {typ};")
        added += 1

    con.commit()
    con.close()
    print(f"KLART ✅ Added={added}, Skipped={skipped}")

if __name__ == "__main__":
    main()
