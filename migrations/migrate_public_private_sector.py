# Kommentar (svenska):
# - Lägger till kolumner för Privat/publikt + Sektor (kod + text) om de saknas.

from __future__ import annotations

import os
import sqlite3
from dotenv import load_dotenv

DB_PATH_DEFAULT = "data/db/companies.db.sqlite"
TABLE = "companies"

COL_PRIVATE_PUBLIC = "scb_private_public"
COL_PRIVATE_PUBLIC_CODE = "scb_private_public_code"
COL_SECTOR = "scb_sector"
COL_SECTOR_CODE = "scb_sector_code"


def db_path() -> str:
    return os.getenv("DB_PATH", DB_PATH_DEFAULT).strip()


def ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cols = {row[1] for row in cur.execute(f"PRAGMA table_info({TABLE})").fetchall()}

    if COL_PRIVATE_PUBLIC_CODE not in cols:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COL_PRIVATE_PUBLIC_CODE} TEXT")
    if COL_PRIVATE_PUBLIC not in cols:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COL_PRIVATE_PUBLIC} TEXT")

    if COL_SECTOR_CODE not in cols:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COL_SECTOR_CODE} TEXT")
    if COL_SECTOR not in cols:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COL_SECTOR} TEXT")

    con.commit()


def main() -> None:
    load_dotenv()
    con = sqlite3.connect(db_path())
    ensure_columns(con)
    con.close()
    print("DONE ✅ migrate_public_private_sector")


if __name__ == "__main__":
    main()
