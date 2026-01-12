# Skriver ut antal företag, mejl, webplatser, antal med sni koder, antal utan sni koder för en STAD
from __future__ import annotations

import argparse
from pathlib import Path
from db_connection_path import DEFAULT_DB_PATH, connect, one, nonempty_sql, empty_sql, print_kv

NO_SNI_MARK = "__NO_SNI__"
BAD_SNI = "00000"

# =========================
# ÄNDRA HÄR
# =========================
CITY = "Göteborg"


def main() -> None:
    ap = argparse.ArgumentParser(description="City-level stats")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to sqlite db")
    args = ap.parse_args()

    db_path = Path(args.db)
    city = CITY.strip()

    con = connect(db_path)
    cur = con.cursor()

    total = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
        """,
        (city,),
    )

    # ---------- WEBSITES ----------
    websites_valid = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND {nonempty_sql("website")}
        """,
        (city,),
    )

    websites_checked = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND {nonempty_sql("website_checked_at")}
        """,
        (city,),
    )

    websites_no_but_checked = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND {empty_sql("website")}
          AND {nonempty_sql("website_checked_at")}
        """,
        (city,),
    )

    # ---------- EMAILS ----------
    emails_valid = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND {nonempty_sql("emails")}
        """,
        (city,),
    )

    emails_checked = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND {nonempty_sql("emails_checked_at")}
        """,
        (city,),
    )

    emails_no_but_checked = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND {empty_sql("emails")}
          AND {nonempty_sql("emails_checked_at")}
        """,
        (city,),
    )

    # ---------- SNI ----------
    valid_sni = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND {nonempty_sql("sni_codes")}
          AND TRIM(sni_codes) != ?
          AND TRIM(sni_codes) != ?
        """,
        (city, NO_SNI_MARK, BAD_SNI),
    )

    no_sni = one(
        cur,
        """
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND TRIM(COALESCE(sni_codes,'')) = ?
        """,
        (city, NO_SNI_MARK),
    )

    sni_not_checked = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(city,'')) = ?
          AND ({empty_sql("sni_codes")} OR TRIM(COALESCE(sni_codes,'')) = ?)
        """,
        (city, BAD_SNI),
    )

    # ---------- PRINT ----------
    print("=== CITY STATS ===")
    print_kv("DB:", db_path)
    print_kv("CITY:", city)
    print_kv("TOTAL_COMPANIES:", total)

    print("")
    print("=== WEBSITES ===")
    print_kv("WEBSITES_VALID:", websites_valid)
    print_kv("WEBSITES_CHECKED:", websites_checked)
    print_kv("WEBSITES_NO_BUT_CHECKED:", websites_no_but_checked)

    print("")
    print("=== EMAILS ===")
    print_kv("EMAILS_VALID:", emails_valid)
    print_kv("EMAILS_CHECKED:", emails_checked)
    print_kv("EMAILS_NO_BUT_CHECKED:", emails_no_but_checked)

    print("")
    print("=== SNI ===")
    print_kv("VALID_SNI:", valid_sni)
    print_kv("NO_SNI (__NO_SNI__):", no_sni)
    print_kv("SNI_NOT_CHECKED:", sni_not_checked)

    con.close()


if __name__ == "__main__":
    main()
