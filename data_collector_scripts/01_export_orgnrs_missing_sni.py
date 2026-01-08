import os
import argparse
import sqlite3
from pathlib import Path

DB_PATH = os.getenv("DB_PATH", "data/mail_generator_db.sqlite")
TABLE = os.getenv("DB_TABLE", "companies")
COL_ORGNR = os.getenv("DB_COL_ORGNR", "orgnr")
COL_CITY = os.getenv("DB_COL_CITY", "city")
COL_SNI_CODES = os.getenv("DB_COL_SNI_CODES", "sni_codes")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", required=True, help="t.ex. Göteborg")
    ap.add_argument("--out", required=True, help="t.ex. data/out/orgnrs_missing_sni_gbg.txt")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    sql = f"""
    SELECT {COL_ORGNR}
    FROM {TABLE}
    WHERE LOWER({COL_CITY}) = LOWER(?)
      AND ({COL_SNI_CODES} IS NULL OR {COL_SNI_CODES} = '')
    """
    params = [args.city]

    if args.limit and args.limit > 0:
        sql += " LIMIT ?"
        params.append(args.limit)

    cur.execute(sql, params)
    rows = cur.fetchall()
    con.close()

    with out_path.open("w", encoding="utf-8") as f:
        for (orgnr,) in rows:
            if orgnr:
                f.write(str(orgnr).strip() + "\n")

    print(f"DONE ✅ exported={len(rows)} -> {out_path}")

if __name__ == "__main__":
    main()
#python data_collector_scripts/01_export_orgnrs_missing_sni.py --city Göteborg --out data/out/orgnrs_missing_sni_gbg.txt
