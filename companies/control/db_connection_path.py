# i detta script är sökvägen till databasen definierad och en funktion för att ansluta till databasen finns.
# denna kod används i alla control scripts

# scripts_control/_db_utils.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional

DEFAULT_DB_PATH = Path("data/db/companies.db.sqlite")


def connect(db_path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def one(cur: sqlite3.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    row = cur.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]


def nonempty_sql(col: str) -> str:
    # TRIM(COALESCE(col,'')) != ''
    return f"TRIM(COALESCE({col},'')) != ''"


def empty_sql(col: str) -> str:
    # TRIM(COALESCE(col,'')) = ''
    return f"TRIM(COALESCE({col},'')) = ''"


def print_kv(key: str, value: Any) -> None:
    print(f"{key:<24} {value}")
