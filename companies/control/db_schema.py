# retunerar alla fäl, PK, osv
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Iterable, List

# =========================
# KONFIG (samma stil som db_overview.py)
# =========================
DB_PATH = Path("data/db/companies.db.sqlite")
# =========================


def resolve_db_path() -> Path:
    if DB_PATH.exists():
        return DB_PATH
    raise FileNotFoundError(f"DB saknas: {DB_PATH}")


def print_kv(key: str, value: Any) -> None:
    print(f"{key:<24} {value}")


def print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def one(cur: sqlite3.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    row = cur.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]


def get_tables(cur: sqlite3.Cursor) -> List[str]:
    rows = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [r[0] for r in rows]


def print_table_schema(cur: sqlite3.Cursor, table: str) -> None:
    print_section(f"TABLE: {table}")

    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    if not rows:
        print("(no columns)")
        return

    for r in rows:
        name = r[1]
        col_type = r[2] or "(no type)"
        not_null = "NOT NULL" if r[3] else ""
        default = f"DEFAULT {r[4]}" if r[4] is not None else ""
        pk = "PK" if r[5] else ""

        parts = " ".join(p for p in [col_type, not_null, default, pk] if p)
        print_kv(f"- {name}", parts)


def main() -> None:
    db_path = resolve_db_path()

    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()

        print("DATABASE SCHEMA")
        print_kv("Database", str(db_path))

        tables = get_tables(cur)
        if not tables:
            print("\n(no tables found)")
            return

        print_section("TABLES")
        for t in tables:
            print(f"- {t}")

        for t in tables:
            print_table_schema(cur, t)

    finally:
        con.close()


if __name__ == "__main__":
    main()
