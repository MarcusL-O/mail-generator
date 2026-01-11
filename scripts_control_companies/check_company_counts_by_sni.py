# skriver ut antal företag som matchar SNI-koder (endast de du anger)
from __future__ import annotations

import argparse
from pathlib import Path
from db_connection_path import DEFAULT_DB_PATH, connect, one, print_kv

NO_SNI_MARK = "__NO_SNI__"
BAD_SNI = "00000"

# =========================
# ÄNDRA HÄR
# =========================
SNI_LIST = ["43320", "41200"]  # en eller flera


def main() -> None:
    ap = argparse.ArgumentParser(description="Count companies by one or more SNI codes")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to sqlite db")
    args = ap.parse_args()

    db_path = Path(args.db)
    sni_list = [s.strip() for s in SNI_LIST if str(s).strip()]

    con = connect(db_path)
    cur = con.cursor()

    print("=== SNI COUNTS (SELECTED) ===")
    print_kv("DB:", db_path)
    print_kv("SNI_LIST:", ", ".join(sni_list) if sni_list else "(empty)")
    print("")

    for code in sni_list:
        cnt = one(
            cur,
            """
            SELECT COUNT(*)
            FROM companies
            WHERE TRIM(COALESCE(sni_codes,'')) != ''
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
                NO_SNI_MARK,
                BAD_SNI,
                code,
                f"{code},%",
                f"%,{code},%",
                f"%,{code}",
            ),
        )
        print_kv(f"SNI_{code}:", cnt)

    con.close()


if __name__ == "__main__":
    main()