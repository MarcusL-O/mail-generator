# companies/migrations/2026_01_add_sni_groups.py
# Kommentar (svenska):
# - Lägger till companies.sni_groups (TEXT) om den saknas
# - Körs säkert flera gånger

import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/companies.db.sqlite")

def column_exists(con, table: str, column: str) -> bool:
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    try:
        if column_exists(con, "companies", "sni_groups"):
            print("OK: companies.sni_groups finns redan")
            return

        con.execute("ALTER TABLE companies ADD COLUMN sni_groups TEXT")
        con.commit()
        print("DONE ✅ Lade till kolumn: companies.sni_groups")
    finally:
        con.close()

if __name__ == "__main__":
    main()
