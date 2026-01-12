# Kollar antal sni i hel db 


from __future__ import annotations

import argparse
from pathlib import Path
from db_connection_path import DEFAULT_DB_PATH, connect, one, nonempty_sql, empty_sql, print_kv

NO_SNI_MARK = "__NO_SNI__"
BAD_SNI = "00000"  # räknas som "inte kollad" / skräp


def main() -> None:
    ap = argparse.ArgumentParser(description="SNI stats for companies table")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to sqlite db")
    args = ap.parse_args()

    db_path = Path(args.db)

    con = connect(db_path)
    cur = con.cursor()

    total = one(cur, "SELECT COUNT(*) FROM companies")

    valid_sni = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE {nonempty_sql("sni_codes")}
          AND TRIM(sni_codes) != ?
          AND TRIM(sni_codes) != ?
        """,
        (NO_SNI_MARK, BAD_SNI),
    )

    no_sni = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(sni_codes,'')) = ?
        """,
        (NO_SNI_MARK,),
    )

    bad_sni = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE TRIM(COALESCE(sni_codes,'')) = ?
        """,
        (BAD_SNI,),
    )

    not_checked = one(
        cur,
        f"""
        SELECT COUNT(*)
        FROM companies
        WHERE {empty_sql("sni_codes")}
           OR TRIM(COALESCE(sni_codes,'')) = ?
        """,
        (BAD_SNI,),
    )

    checked_total = (valid_sni or 0) + (no_sni or 0)

    print("=== SNI STATS ===")
    print_kv("DB:", db_path)
    print_kv("TOTAL_COMPANIES:", total)
    print_kv("CHECKED_TOTAL:", checked_total)
    print_kv("VALID_SNI:", valid_sni)
    print_kv("NO_SNI (__NO_SNI__):", no_sni)
    print_kv("BAD_SNI (00000):", bad_sni)
    print_kv("NOT_CHECKED_YET:", not_checked)

    con.close()


if __name__ == "__main__":
    main()
