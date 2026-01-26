# =========================================
# 2) MIGRATION: nolla felaktiga min/max
# =========================================
# Skapa fil: companies/open_data/scb/migrate_reset_employee_span.py

# Kommentar (svenska):
# - Nollar scb_employees_min/max så enrich kan fylla korrekt efter fixen.

from __future__ import annotations

import os
import sqlite3
from dotenv import load_dotenv

DB_PATH_DEFAULT = "data/db/companies.db.sqlite"
TABLE = "companies"

COL_MIN = "scb_employees_min"
COL_MAX = "scb_employees_max"

def db_path() -> str:
    return os.getenv("DB_PATH", DB_PATH_DEFAULT).strip()

def main() -> None:
    load_dotenv()
    con = sqlite3.connect(db_path())
    cur = con.cursor()

    cur.execute(f"UPDATE {TABLE} SET {COL_MIN}=NULL, {COL_MAX}=NULL")
    con.commit()
    con.close()

    print("DONE ✅ migrate_reset_employee_span")

if __name__ == "__main__":
    main()
