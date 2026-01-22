# outreach/control/db_overview.py
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# =========================
# KONFIG (samma stil som companies/control/db_overview.py)
# =========================
DB_PATH = Path("data/db/outreach.db.sqlite")
# =========================


def utc_now_str() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def print_kv(key: str, value: Any) -> None:
    print(f"{key:<24} {value}")


def one(cur: sqlite3.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    row = cur.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]


def nonempty_sql(col: str) -> str:
    return f"TRIM(COALESCE({col},'')) != ''"


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    return one(cur, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)) is not None


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


def print_table_counts(cur: sqlite3.Cursor) -> None:
    print_section("TABLES")
    rows = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    for (name,) in rows:
        print_kv(name, f"{count_rows(cur, name):,} rows")


def print_leads_core(cur: sqlite3.Cursor, total: int, cols: set[str]) -> None:
    print_section("LEADS – CORE COVERAGE")

    def nonempty_count(col: str) -> int:
        return int(one(cur, f"SELECT COUNT(*) FROM leads WHERE {nonempty_sql(col)}") or 0)

    for c in ["emails", "website", "company_name", "city", "owner"]:
        if c not in cols:
            continue
        n = nonempty_count(c)
        print_kv(c, f"{n:,} ({pct(n, total)})")


def print_status_counts(cur: sqlite3.Cursor, table: str, col: str) -> None:
    cols = get_columns(cur, table)
    if col not in cols:
        return

    print_section(f"{table}.{col}")
    rows = cur.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(CAST({col} AS TEXT)), ''), '(empty)') AS v, COUNT(*) AS n
        FROM {table}
        GROUP BY v
        ORDER BY n DESC
        LIMIT 30
        """
    ).fetchall()
    for r in rows:
        print_kv(f"- {r['v']}", f"{int(r['n']):,}")


def print_freshness_email_messages(cur: sqlite3.Cursor) -> None:
    if not table_exists(cur, "email_messages"):
        return
    cols = get_columns(cur, "email_messages")
    if "sent_at" not in cols:
        return

    total = count_rows(cur, "email_messages")
    print_section("DATA FRESHNESS (email_messages.sent_at)")

    last_24h = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE sent_at >= datetime('now','-1 day')") or 0)
    last_7d = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE sent_at >= datetime('now','-7 day')") or 0)
    last_30d = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE sent_at >= datetime('now','-30 day')") or 0)
    never = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE sent_at IS NULL OR TRIM(sent_at)=''") or 0)

    print_kv("- last 24h", f"{last_24h:,} ({pct(last_24h, total)})")
    print_kv("- last 7 days", f"{last_7d:,} ({pct(last_7d, total)})")
    print_kv("- last 30 days", f"{last_30d:,} ({pct(last_30d, total)})")
    print_kv("- never", f"{never:,} ({pct(never, total)})")

    newest = one(cur, "SELECT MAX(sent_at) FROM email_messages")
    print_kv("newest sent_at", newest or "(none)")


def print_top_cities(cur: sqlite3.Cursor) -> None:
    if not table_exists(cur, "leads"):
        return
    cols = get_columns(cur, "leads")
    if "city" not in cols:
        return

    print_section("TOP CITIES (leads.city)")
    rows = cur.execute(
        """
        SELECT TRIM(city) AS city, COUNT(*) AS n
        FROM leads
        WHERE TRIM(COALESCE(city,'')) != ''
        GROUP BY TRIM(city)
        ORDER BY n DESC
        LIMIT 10
        """
    ).fetchall()
    for i, r in enumerate(rows, start=1):
        print_kv(f"{i}. {r['city']}", f"{int(r['n']):,}")


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    try:
        cur = con.cursor()

        print("DATABASE OVERVIEW")
        print_kv("Database", str(DB_PATH))
        print_kv("Generated", utc_now_str())

        print_table_counts(cur)

        if not table_exists(cur, "leads"):
            print("\n(ingen tabell: leads)")
            return

        leads_cols = get_columns(cur, "leads")
        total_leads = count_rows(cur, "leads")

        print_section("LEADS")
        print_kv("total leads", f"{total_leads:,}")

        print_leads_core(cur, total_leads, leads_cols)

        # Status / categorical breakdowns
        for c in ["lead_type", "status"]:
            print_status_counts(cur, "leads", c)

        if table_exists(cur, "campaigns"):
            print_status_counts(cur, "campaigns", "status")

        if table_exists(cur, "email_messages"):
            print_status_counts(cur, "email_messages", "status")

        if table_exists(cur, "suppliers"):
            print_status_counts(cur, "suppliers", "status")

        # Freshness + cities
        print_freshness_email_messages(cur)
        print_top_cities(cur)

        # Summary (snabbt, samma vibe som companies)
        print("\nSUMMARY")
        if "emails" in leads_cols:
            e = int(one(cur, f"SELECT COUNT(*) FROM leads WHERE {nonempty_sql('emails')}") or 0)
            print_kv("emails coverage", f"{pct(e, total_leads)}")
        if "website" in leads_cols:
            w = int(one(cur, f"SELECT COUNT(*) FROM leads WHERE {nonempty_sql('website')}") or 0)
            print_kv("website coverage", f"{pct(w, total_leads)}")
        if table_exists(cur, "email_messages"):
            sent = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE status='sent'") or 0)
            print_kv("messages sent", f"{sent:,}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
