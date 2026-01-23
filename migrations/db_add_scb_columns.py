# db_add_scb_columns.py
# Kommentar: Lägger till SCB-kolumner i companies om de saknas (SQLite-safe, idempotent).

import sqlite3

DB_PATH = "data/db/companies.db.sqlite"
TABLE = "companies"

COLUMNS = [
    ("scb_employees_class", "TEXT"),
    ("scb_workplaces_count", "INTEGER"),
    ("scb_postort", "TEXT"),
    ("scb_municipality", "TEXT"),
    ("scb_region", "TEXT"),
    ("scb_status", "TEXT"),
    ("scb_checked_at", "TEXT"),
    ("scb_next_check_at", "TEXT"),
    ("scb_err_reason", "TEXT"),
]

def get_existing_columns(cur) -> set[str]:
    cur.execute(f"PRAGMA table_info({TABLE});")
    return {row[1] for row in cur.fetchall()}

def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    existing = get_existing_columns(cur)

    added = 0
    for name, coltype in COLUMNS:
        if name in existing:
            continue
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {name} {coltype};")
        print(f"Added column: {name} {coltype}")
        added += 1

    # Kommentar: index för refresh-kön
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_scb_next_check_at ON {TABLE}(scb_next_check_at);")

    con.commit()
    con.close()
    print(f"DONE ✅ columns_added={added}")

if __name__ == "__main__":
    main()
