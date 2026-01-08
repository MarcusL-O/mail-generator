import os
import json
import sqlite3
import argparse

DB_PATH = os.getenv("DB_PATH", "data/mail_generator_db.sqlite")
TABLE = os.getenv("DB_TABLE", "companies")
NO_SNI_MARK = os.getenv("NO_SNI_MARK", "__NO_SNI__")
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "2000"))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="ndjson från BV enrich")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    cur = con.cursor()

    applied = skipped = missing_in_db = errors = 0

    with open(args.in_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                errors += 1
                continue

            status = (obj.get("status") or "").strip().lower()
            orgnr = (obj.get("orgnr") or "").strip()

            if not orgnr:
                skipped += 1
                continue

            # bara rader som är "klara"
            if status not in ("ok", "ok_no_sni", "not_found"):
                skipped += 1
                continue

            sni_codes = (obj.get("sni_codes") or "").strip()
            sni_text = (obj.get("sni_text") or "").strip()

            # Om inte hittad men kollad: markera __NO_SNI__
            if not sni_codes and status == "ok_no_sni":
                sni_codes = NO_SNI_MARK
                sni_text = ""

            # Om vi fortfarande inte har något att skriva: skippa
            if not sni_codes:
                skipped += 1
                continue

            # SÄKERHET: uppdatera ENDAST om DB saknar sni_codes (NULL eller '')
            cur.execute(f"""
                UPDATE {TABLE}
                SET
                    sni_codes = ?,
                    sni_text  = ?
                WHERE orgnr = ?
                  AND (sni_codes IS NULL OR TRIM(sni_codes) = '')
            """, (sni_codes, sni_text, orgnr))

            if cur.rowcount == 0:
                # antingen saknas orgnr i DB, eller så hade den redan SNI (så vi rörde inget)
                cur.execute(f"SELECT 1 FROM {TABLE} WHERE orgnr = ? LIMIT 1", (orgnr,))
                if cur.fetchone() is None:
                    missing_in_db += 1
                else:
                    skipped += 1
            else:
                applied += 1

            if i % COMMIT_EVERY == 0:
                con.commit()
                print(f"[{i}] applied={applied} skipped={skipped} missing_in_db={missing_in_db} errors={errors}")

    con.commit()
    con.close()
    print(f"DONE ✅ applied={applied} skipped={skipped} missing_in_db={missing_in_db} errors={errors}")

if __name__ == "__main__":
    main()