# companies/control/db_overview.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import sqlite3

from db_connection_path import (
    DEFAULT_DB_PATH,
    connect,
    one,
    print_kv,
    nonempty_sql,
)


def utc_now_str() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    return (
        one(
            cur,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        is not None
    )


def get_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cols: set[str] = set()
    for row in cur.execute(f"PRAGMA table_info({table})").fetchall():
        cols.add(row[1])
    return cols


def count_rows(cur: sqlite3.Cursor, table: str) -> int:
    return int(one(cur, f"SELECT COUNT(*) FROM {table}") or 0)


def pct(part: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(part * 100.0 / total):.1f}%"


def print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def print_table_counts(cur: sqlite3.Cursor) -> None:
    print_section("TABLES")
    rows = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    for (name,) in rows:
        if name == "leads":
            continue  # Kommentar: användaren vill inte se leads
        print_kv(name, f"{count_rows(cur, name):,} rows")


def print_companies_core(cur: sqlite3.Cursor, total: int, cols: set[str]) -> None:
    print_section("COMPANIES – CORE COVERAGE")

    def nonempty_count(col: str) -> int:
        return int(one(cur, f"SELECT COUNT(*) FROM companies WHERE {nonempty_sql(col)}") or 0)

    key_cols = ["website", "emails", "employees", "sni_codes", "city"]
    for c in key_cols:
        if c not in cols:
            continue
        n = nonempty_count(c)
        print_kv(c, f"{n:,} ({pct(n, total)})")


def print_status_counts(cur: sqlite3.Cursor, cols: set[str], col: str) -> None:
    if col not in cols:
        return
    print_section(col)
    rows = cur.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(CAST({col} AS TEXT)), ''), '(empty)') AS v, COUNT(*) AS n
        FROM companies
        GROUP BY v
        ORDER BY n DESC
        LIMIT 30
        """
    ).fetchall()
    for r in rows:
        print_kv(f"- {r['v']}", f"{int(r['n']):,}")


def print_tech_signals(cur: sqlite3.Cursor, cols: set[str]) -> None:
    print_section("TECH / IT SIGNALS")
    # Kommentar: medvetet INTE tech_err_reason (användaren vill inte se det)
    for c in [
        "microsoft_status",
        "microsoft_strength",
        "microsoft_confidence",
        "it_support_signal",
        "it_support_confidence",
    ]:
        print_status_counts(cur, cols, c)


def print_freshness(cur: sqlite3.Cursor, cols: set[str], total: int) -> None:
    if "tech_checked_at" not in cols:
        return

    print_section("DATA FRESHNESS (tech_checked_at)")
    # Kommentar: SQLite ISO-strängar fungerar bra med datetime('now', ...)
    last_24h = int(one(cur, "SELECT COUNT(*) FROM companies WHERE tech_checked_at >= datetime('now','-1 day')") or 0)
    last_7d = int(one(cur, "SELECT COUNT(*) FROM companies WHERE tech_checked_at >= datetime('now','-7 day')") or 0)
    last_30d = int(one(cur, "SELECT COUNT(*) FROM companies WHERE tech_checked_at >= datetime('now','-30 day')") or 0)
    never = int(one(cur, "SELECT COUNT(*) FROM companies WHERE tech_checked_at IS NULL OR TRIM(tech_checked_at)=''") or 0)

    print_kv("- last 24h", f"{last_24h:,} ({pct(last_24h, total)})")
    print_kv("- last 7 days", f"{last_7d:,} ({pct(last_7d, total)})")
    print_kv("- last 30 days", f"{last_30d:,} ({pct(last_30d, total)})")
    print_kv("- never", f"{never:,} ({pct(never, total)})")

    newest = one(cur, "SELECT MAX(tech_checked_at) FROM companies")
    print_kv("newest tech_checked_at", newest or "(none)")


def print_top_cities(cur: sqlite3.Cursor, cols: set[str]) -> None:
    if "city" not in cols:
        return
    print_section("TOP CITIES (companies.city)")
    rows = cur.execute(
        """
        SELECT TRIM(city) AS city, COUNT(*) AS n
        FROM companies
        WHERE TRIM(COALESCE(city,'')) != ''
        GROUP BY TRIM(city)
        ORDER BY n DESC
        LIMIT 10
        """
    ).fetchall()
    for i, r in enumerate(rows, start=1):
        print_kv(f"{i}. {r['city']}", f"{int(r['n']):,}")


def main() -> None:
    con = connect(DEFAULT_DB_PATH)
    try:
        cur = con.cursor()

        print("DATABASE OVERVIEW")
        print_kv("Database", str(DEFAULT_DB_PATH))
        print_kv("Generated", utc_now_str())

        print_table_counts(cur)

        if not table_exists(cur, "companies"):
            print("\n(ingen tabell: companies)")
            return

        cols = get_columns(cur, "companies")
        total = count_rows(cur, "companies")

        print_section("COMPANIES")
        print_kv("total companies", f"{total:,}")

        print_companies_core(cur, total, cols)

        # Kommentar: statusfält som brukar vara nyttiga (utan leads + utan tech_err_reason)
        for c in ["website_status", "email_status", "hiring_status"]:
            print_status_counts(cur, cols, c)

        print_tech_signals(cur, cols)
        print_freshness(cur, cols, total)
        print_top_cities(cur, cols)

        print("\nSUMMARY")
        # Kommentar: håll summary kort och “faktabaserad”
        if "website" in cols:
            w = int(one(cur, f"SELECT COUNT(*) FROM companies WHERE {nonempty_sql('website')}") or 0)
            print_kv("website coverage", f"{pct(w, total)}")
        if "emails" in cols:
            e = int(one(cur, f"SELECT COUNT(*) FROM companies WHERE {nonempty_sql('emails')}") or 0)
            print_kv("emails coverage", f"{pct(e, total)}")
        if "microsoft_status" in cols:
            ms_yes = int(one(cur, "SELECT COUNT(*) FROM companies WHERE microsoft_status='yes'") or 0)
            print_kv("microsoft yes", f"{ms_yes:,} ({pct(ms_yes, total)})")

    finally:
        con.close()


if __name__ == "__main__":
    main()
