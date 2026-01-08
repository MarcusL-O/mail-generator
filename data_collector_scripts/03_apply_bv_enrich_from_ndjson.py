import os
import json
import sqlite3
import argparse

DB_PATH = os.getenv("DB_PATH", "data/mail_generator_db.sqlite")
TABLE = os.getenv("DB_TABLE", "companies")
NO_SNI_MARK = os.getenv("NO_SNI_MARK", "__NO_SNI__")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    cur = con.cursor()

    applied = skipped = missing_in_db = 0

    with open(args.in_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)
            status = (obj.get("status") or "").strip().lower()
            orgnr = (obj.get("orgnr") or "").strip()

            if not orgnr:
                skipped += 1
                continue

            # Vi applicerar bara “klara”
            if status not in ("ok", "ok_no_sni"):
                skipped += 1
                continue

            name = (obj.get("name") or "").strip()
            city = (obj.get("city") or "").strip()
            sni_codes = (obj.get("sni_codes") or "").strip()
            sni_text = (obj.get("sni_text") or "").strip()

            # normalisera no_sni
            if not sni_codes:
                sni_codes = NO_SNI_MARK
                sni_text = ""

            # Uppdatera bara om inkommande har värde (överskriv inte bra data)
            cur.execute(f"""
                UPDATE {TABLE}
                SET
                  name = CASE WHEN ? != '' THEN ? ELSE name END,
                  city = CASE WHEN ? != '' THEN ? ELSE city END,
                  sni_codes = CASE WHEN ? != '' THEN ? ELSE sni_codes END,
                  sni_text  = CASE WHEN ? != '' THEN ? ELSE sni_text END,
                  updated_at = COALESCE(updated_at, datetime('now'))
                WHERE orgnr = ?
            """, (name, name, city, city, sni_codes, sni_codes, sni_text, sni_text, orgnr))

            if cur.rowcount == 0:
                missing_in_db += 1
            else:
                applied += 1

            if i % 2000 == 0:
                con.commit()
                print(f"[{i}] applied={applied} skipped={skipped} missing_in_db={missing_in_db}")

    con.commit()
    con.close()
    print(f"DONE ✅ applied={applied} skipped={skipped} missing_in_db={missing_in_db}")

if __name__ == "__main__":
    main()

#python data_collector_scripts/03_apply_bv_enrich_from_ndjson.py --in data/out/bv_enrich_gbg.ndjson
