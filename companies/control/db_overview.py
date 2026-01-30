# companies/control/db_overview.py
# Kommentar (svenska):
# - Översikt för nya schema (companies + checks + ekonomi)
# - Inga antaganden om gamla fält (ingen private_public, ingen employees_trend_at)

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

DB_PATH = Path("data/db/companies.db.sqlite")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def one(cur: sqlite3.Cursor, sql: str, params: Tuple[Any, ...] = ()) -> Any:
    r = cur.execute(sql, params).fetchone()
    return None if r is None else r[0]


def pct(n: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(n * 100.0 / total):.1f}%"


def filled_count(cur: sqlite3.Cursor, table: str, col: str) -> int:
    sql = f"SELECT COUNT(*) FROM {table} WHERE TRIM(COALESCE({col},'')) != ''"
    return int(one(cur, sql) or 0)


def nonnull_count(cur: sqlite3.Cursor, table: str, col: str) -> int:
    sql = f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL"
    return int(one(cur, sql) or 0)


def print_dist(cur: sqlite3.Cursor, table: str, col: str, title: str, limit: int = 10) -> None:
    print(title)
    rows = cur.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM({col}),''), '(empty)') AS v, COUNT(*) AS n
        FROM {table}
        GROUP BY v
        ORDER BY n DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for v, n in rows:
        print(f"- {v:<22} {int(n):,}")
    print()


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    con = sqlite3.connect(DB_PATH.as_posix())
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]

    total_companies = int(one(cur, "SELECT COUNT(*) FROM companies") or 0)
    total_checks = int(one(cur, "SELECT COUNT(*) FROM company_checks") or 0) if "company_checks" in tables else 0
    total_hist = int(one(cur, "SELECT COUNT(*) FROM company_employee_class_history") or 0) if "company_employee_class_history" in tables else 0
    total_fin = int(one(cur, "SELECT COUNT(*) FROM company_financials") or 0) if "company_financials" in tables else 0
    total_scores = int(one(cur, "SELECT COUNT(*) FROM company_financial_scores") or 0) if "company_financial_scores" in tables else 0

    print("DATABASE OVERVIEW")
    print(f"Database                 {DB_PATH}")
    print(f"Generated                {utc_now_iso()}\n")

    print("TABLES")
    print("------")
    for t in ["companies", "company_checks", "company_employee_class_history", "company_financial_scores", "company_financials"]:
        if t in tables:
            n = int(one(cur, f"SELECT COUNT(*) FROM {t}") or 0)
            print(f"{t:<24} {n:,} rows")
    print()

    print("COMPANIES")
    print("---------")
    print(f"total companies          {total_companies:,}\n")

    print("COMPANIES – CORE COVERAGE")
    print("-------------------------")
    for col in ["website", "emails", "sni_codes"]:
        n = filled_count(cur, "companies", col)
        print(f"{col:<23} {n:,} ({pct(n, total_companies)})")
    print()

    print("COMPANIES – GEO/LEGAL COVERAGE")
    print("------------------------------")
    for col in ["kommun", "region", "postort", "registration_date", "legal_form", "company_status", "sector"]:
        n = filled_count(cur, "companies", col)
        print(f"{col:<23} {n:,} ({pct(n, total_companies)})")
    print()

    print("COMPANIES – EMPLOYEES COVERAGE")
    print("------------------------------")
    for col in ["employees_class", "workplaces_count", "employees_trend"]:
        if col == "workplaces_count":
            n = nonnull_count(cur, "companies", col)
        else:
            n = filled_count(cur, "companies", col)
        print(f"{col:<23} {n:,} ({pct(n, total_companies)})")
    print()

    print("HIRING")
    print("------")
    print_dist(cur, "companies", "hiring_status", "hiring_status", limit=10)

    print("TECH / IT SIGNALS")
    print("-----------------")
    for col in ["microsoft_status", "microsoft_strength", "microsoft_confidence", "it_support_signal", "it_support_confidence"]:
        n = filled_count(cur, "companies", col)
        print(f"{col:<23} {n:,} ({pct(n, total_companies)})")
    print()

    print("ECONOMY SNAPSHOT (companies)")
    print("----------------------------")
    for col in ["financial_score_total", "financial_latest_year_end", "financial_net_revenue_latest", "financial_revenue_trend_pct", "financial_revenue_trend"]:
        if col in ("financial_score_total", "financial_net_revenue_latest"):
            n = nonnull_count(cur, "companies", col)
        else:
            n = filled_count(cur, "companies", col)
        print(f"{col:<23} {n:,} ({pct(n, total_companies)})")
    print()

    print("SUMMARY")
    print("-------")
    website_cov = filled_count(cur, "companies", "website")
    emails_cov = filled_count(cur, "companies", "emails")
    sni_cov = filled_count(cur, "companies", "sni_codes")
    print(f"website coverage         {pct(website_cov, total_companies)}")
    print(f"emails coverage          {pct(emails_cov, total_companies)}")
    print(f"sni_codes coverage       {pct(sni_cov, total_companies)}")
    print(f"company_checks rows      {total_checks:,}")
    print(f"employee_history rows    {total_hist:,}")
    print(f"financials rows          {total_fin:,}")
    print(f"financial_scores rows    {total_scores:,}")

    con.close()


if __name__ == "__main__":
    main()
