# Kommentar (svenska):
# - Lägger till kolumner för spann (min/max) om de saknas.
# - Backfill: försöker räkna ut min/max från scb_employees_class_code eller scb_employees_class.

from __future__ import annotations

import os
import re
import sqlite3
from typing import Optional, Tuple

from dotenv import load_dotenv

DB_PATH_DEFAULT = "data/db/companies.db.sqlite"
TABLE = "companies"

COL_CLASS = "scb_employees_class"
COL_CODE = "scb_employees_class_code"
COL_MIN = "scb_employees_min"
COL_MAX = "scb_employees_max"


def db_path() -> str:
    return os.getenv("DB_PATH", DB_PATH_DEFAULT).strip()


def ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cols = {row[1] for row in cur.execute(f"PRAGMA table_info({TABLE})").fetchall()}

    if COL_MIN not in cols:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COL_MIN} INTEGER")
    if COL_MAX not in cols:
        cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {COL_MAX} INTEGER")

    con.commit()


def code_to_span(code: str) -> Tuple[Optional[int], Optional[int]]:
    # Kommentar (svenska): typisk SCB/JE storleksklass-mappning
    c = (code or "").strip()
    if not c:
        return None, None

    # vissa kommer som "4 " etc
    m = re.search(r"\d+", c)
    if not m:
        return None, None
    n = int(m.group(0))

    mapping = {
        0: (0, 0),
        1: (1, 4),
        2: (5, 9),
        3: (10, 19),
        4: (20, 49),
        5: (50, 99),
        6: (100, 199),
        7: (200, 499),
        8: (500, None),  # 500+
    }
    return mapping.get(n, (None, None))


def text_to_span(text: str) -> Tuple[Optional[int], Optional[int]]:
    t = (text or "").lower().strip()
    if not t:
        return None, None

    # "0 anställda"
    if t.startswith("0"):
        return 0, 0

    # "10-19 anställda" eller "10–19 anställda"
    t = t.replace("–", "-")
    m = re.search(r"(\d+)\s*-\s*(\d+)", t)
    if m:
        return int(m.group(1)), int(m.group(2))

    # "500+"
    m = re.search(r"(\d+)\s*\+", t)
    if m:
        return int(m.group(1)), None

    return None, None


def main() -> None:
    load_dotenv()

    con = sqlite3.connect(db_path())
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ensure_columns(con)

    rows = cur.execute(
        f"""
        SELECT rowid, {COL_CODE}, {COL_CLASS}, {COL_MIN}, {COL_MAX}
        FROM {TABLE}
        """
    ).fetchall()

    updated = 0
    for r in rows:
        rowid = r["rowid"]
        if r[COL_MIN] is not None or r[COL_MAX] is not None:
            continue

        code = r[COL_CODE] or ""
        cls = r[COL_CLASS] or ""

        mn, mx = code_to_span(str(code))
        if mn is None and mx is None:
            mn, mx = text_to_span(str(cls))

        if mn is None and mx is None:
            continue

        cur.execute(
            f"UPDATE {TABLE} SET {COL_MIN}=?, {COL_MAX}=? WHERE rowid=?",
            (mn, mx, rowid),
        )
        updated += 1

    con.commit()
    con.close()

    print("DONE ✅ migrate_employees_span")
    print(f"updated={updated}")


if __name__ == "__main__":
    main()
