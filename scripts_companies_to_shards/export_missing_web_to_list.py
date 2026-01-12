# export_missing_websites_guess_enriched.py
import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB = Path("data/companies.db.sqlite")
DEFAULT_OUT = Path("data/out/missing_websites_guess_enriched.txt")

SQL = """
SELECT orgnr, name, city, sni_codes, sni_text
FROM companies
WHERE website_checked_at IS NOT NULL
  AND (website IS NULL OR TRIM(website) = '')
  AND website_status IN ('not_found', 'parked')
ORDER BY website_checked_at ASC
"""

def norm(v):
    if v is None:
        return ""
    return str(v).replace("\t", " ").replace("\n", " ").strip()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--limit", type=int, default=0, help="0 = ingen limit")
    args = ap.parse_args()

    db_path = Path(args.db)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not db_path.exists():
        raise SystemExit(f"DB saknas: {db_path}")

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    sql = SQL + (f"\nLIMIT {args.limit}\n" if args.limit and args.limit > 0 else "")
    cur.execute(sql)
    rows = cur.fetchall()

    with out_path.open("w", encoding="utf-8") as f:
        # header
        f.write("orgnr\tname\tcity\tsni_codes\tsni_text\n")

        for orgnr, name, city, sni_codes, sni_text in rows:
            if not orgnr or not name:
                continue
            f.write(
                f"{norm(orgnr)}\t{norm(name)}\t{norm(city)}\t{norm(sni_codes)}\t{norm(sni_text)}\n"
            )

    conn.close()

    print("=== EXPORT KLAR ===")
    print(f"Rader: {len(rows)}")
    print(f"Utfil: {out_path}")

if __name__ == "__main__":
    main()
