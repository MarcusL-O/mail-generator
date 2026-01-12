# scripts_control/check_db_overview.py
from __future__ import annotations

import argparse
from pathlib import Path
from db_connection_path import DEFAULT_DB_PATH, connect, one, print_kv

NO_SNI_MARK = "__NO_SNI__"
BAD_SNI = "00000"


def main() -> None:
    ap = argparse.ArgumentParser(description="DB overview stats (simple)")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to sqlite db")
    args = ap.parse_args()

    db_path = Path(args.db)

    con = connect(db_path)
    cur = con.cursor()

    # ==========
    # COMPANIES
    # ==========
    total_companies = one(cur, "SELECT COUNT(*) FROM companies")

    print("=== DB OVERVIEW ===")
    print_kv("DB:", db_path)
    print("")
    print("=== COMPANIES ===")
    print_kv("TOTAL_COMPANIES:", total_companies)

    # ==========
    # CITIES
    # ==========
    total_cities = one(
        cur,
        """
        SELECT COUNT(*)
        FROM (
            SELECT TRIM(COALESCE(city,'')) AS c
            FROM companies
            WHERE TRIM(COALESCE(city,'')) != ''
            GROUP BY c
        )
        """,
    )

    companies_with_city = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) != ''
        """,
    )

    companies_without_city = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ''
        """,
    )

    print("")
    print("=== CITIES ===")
    print_kv("TOTAL_CITIES:", total_cities)
    print_kv("WITH_CITY:", companies_with_city)
    print_kv("WITHOUT_CITY:", companies_without_city)

    # ==========
    # SNI
    # ==========
    sni_valid = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(sni_codes,'')) != ''
          AND TRIM(sni_codes) != ?
          AND TRIM(sni_codes) != ?
        """,
        (NO_SNI_MARK, BAD_SNI),
    )

    sni_no_sni = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(sni_codes,'')) = ?
        """,
        (NO_SNI_MARK,),
    )

    sni_not_checked = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(sni_codes,'')) = ''
           OR TRIM(COALESCE(sni_codes,'')) = ?
        """,
        (BAD_SNI,),
    )

    print("")
    print("=== SNI ===")
    print_kv("VALID_SNI:", sni_valid)
    print_kv("NO_SNI:", sni_no_sni)
    print_kv("NOT_CHECKED_YET:", sni_not_checked)

    # ==========
    # WEBSITES (status-based like your final script)
    # ==========
    web_found = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(website_status,'')) = 'found'
        """,
    )

    web_not_found_yet = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(website_status,'')) = 'not_found'
        """,
    )

    web_parked = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(website_status,'')) = 'parked'
        """,
    )

    print("")
    print("=== WEBSITES ===")
    print_kv("FOUND:", web_found)
    print_kv("NOT_FOUND_YET:", web_not_found_yet)
    print_kv("PARKED:", web_parked)

    # ==========
    # EMAILS (status-based like your final script)
    # ==========
    email_found = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(email_status,'')) = 'found'
        """,
    )

    email_not_found_yet = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(email_status,'')) = 'not_found'
        """,
    )

    email_fetch_failed = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(email_status,'')) = 'fetch_failed'
        """,
    )

    print("")
    print("=== EMAILS ===")
    print_kv("FOUND:", email_found)
    print_kv("NOT_FOUND_YET:", email_not_found_yet)
    print_kv("FETCH_FAILED:", email_fetch_failed)

    con.close()


if __name__ == "__main__":
    main()
