# scripts_control/check_emails_stats.py
from __future__ import annotations

import argparse
from pathlib import Path
from db_connection_path import DEFAULT_DB_PATH, connect, one, print_kv


def main() -> None:
    ap = argparse.ArgumentParser(description="Email stats (simplified)")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to sqlite db")
    args = ap.parse_args()

    db_path = Path(args.db)

    con = connect(db_path)
    cur = con.cursor()

    total = one(cur, "SELECT COUNT(*) FROM companies")

    found = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(email_status,'')) = 'found'
        """,
    )

    not_found_yet = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(email_status,'')) = 'not_found'
        """,
    )

    fetch_failed = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(email_status,'')) = 'fetch_failed'
        """,
    )

    print("=== EMAIL STATS ===")
    print_kv("DB:", db_path)
    print_kv("TOTAL_COMPANIES:", total)
    print("")
    print("=== RESULTS ===")
    print_kv("FOUND:", found)
    print_kv("NOT_FOUND_YET:", not_found_yet)
    print_kv("FETCH_FAILED:", fetch_failed)

    con.close()


if __name__ == "__main__":
    main()
