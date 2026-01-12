# skriver ut antal SNI-koder för en stad + SNI-text
from __future__ import annotations

import argparse
from pathlib import Path
from db_connection_path import DEFAULT_DB_PATH, connect, one, print_kv

NO_SNI_MARK = "__NO_SNI__"
BAD_SNI = "00000"

# =========================
# ÄNDRA HÄR
# =========================
CITY = "Göteborg"
SNI_LIST = ["43320", "41200"]  # en eller flera


def main() -> None:
    ap = argparse.ArgumentParser(description="Count companies in a city by one or more SNI codes")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to sqlite db")
    args = ap.parse_args()

    db_path = Path(args.db)
    city = CITY.strip()
    sni_list = [s.strip() for s in SNI_LIST if str(s).strip()]

    con = connect(db_path)
    cur = con.cursor()

    print("=== CITY + SNI COUNTS ===")
    print_kv("DB:", db_path)
    print_kv("CITY:", city)
    print_kv("SNI_LIST:", ", ".join(sni_list))
    print("")

    for code in sni_list:
        cnt = one(
            cur,
            """
            SELECT COUNT(*)
            FROM companies
            WHERE TRIM(COALESCE(city,'')) = ?
              AND TRIM(COALESCE(sni_codes,'')) != ''
              AND TRIM(sni_codes) != ?
              AND TRIM(sni_codes) != ?
              AND (
                    TRIM(sni_codes) = ?
                 OR  sni_codes LIKE ?
                 OR  sni_codes LIKE ?
                 OR  sni_codes LIKE ?
              )
            """,
            (
                city,
                NO_SNI_MARK,
                BAD_SNI,
                code,
                f"{code},%",
                f"%,{code},%",
                f"%,{code}",
            ),
        )

        sni_text = one(
            cur,
            """
            SELECT MIN(sni_text)
            FROM companies
            WHERE sni_text IS NOT NULL
              AND TRIM(sni_text) != ''
              AND (
                    TRIM(sni_codes) = ?
                 OR  sni_codes LIKE ?
                 OR  sni_codes LIKE ?
                 OR  sni_codes LIKE ?
              )
            """,
            (
                code,
                f"{code},%",
                f"%,{code},%",
                f"%,{code}",
            ),
        )

        print_kv(f"SNI_{code}:", cnt)
        print_kv("  SNI_TEXT:", sni_text or "(saknas)")
        print("")

    con.close()


if __name__ == "__main__":
    main()
