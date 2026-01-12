# denna fil tar allt från dem shards som finns nedan + ndjson filerna och för in i db 

import json
import sqlite3
from pathlib import Path
import glob

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/companies.db.sqlite")

# Tar både shards och ev. "single" filer
WEBSITES_PATTERNS = [
    "data/out/websites_guess_shard*.ndjson",
    "data/out/websites_guess.ndjson",
]
EMAILS_PATTERNS = [
    "data/out/emails_found_shard*.ndjson",
    "data/out/emails_found.ndjson",
]

COMMIT_EVERY = 2000
BUSY_TIMEOUT_MS = 10000

TABLE = "companies"

COL_ORGNR = "orgnr"

COL_WEBSITE = "website"
COL_WEBSITE_STATUS = "website_status"
COL_WEBSITE_CHECKED_AT = "website_checked_at"

COL_EMAILS = "emails"
COL_EMAIL_STATUS = "email_status"
COL_EMAILS_CHECKED_AT = "emails_checked_at"

COL_UPDATED_AT = "updated_at"
# =========================


def _safe_loads(line: str):
    try:
        return json.loads(line)
    except Exception:
        return None


def _list_files(patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    seen = set()
    for pat in patterns:
        for p in sorted(glob.glob(pat)):
            pp = Path(p)
            if pp.exists() and pp.resolve() not in seen:
                files.append(pp)
                seen.add(pp.resolve())
    return files


def apply_websites_file(conn: sqlite3.Connection, ndjson_path: Path) -> dict:
    """
    NDJSON-format:
    {
      "orgnr": "...",
      "found_website": "https://...",
      "status": "found" | "not_found" | "parked",
      "checked_at": "2026-01-08T...+00:00"
    }
    """
    applied_value = 0
    applied_marker = 0
    skipped = 0
    errors = 0

    with ndjson_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = _safe_loads(line)
            if not obj:
                errors += 1
                continue

            orgnr = (obj.get("orgnr") or "").strip()
            status = (obj.get("status") or "").strip().lower() or "checked"
            checked_at = (obj.get("checked_at") or "").strip()
            found_website = (obj.get("found_website") or "").strip()

            if not orgnr:
                skipped += 1
                continue

            # 1) Sätt website ENDAST om DB.website är tom
            if found_website:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_WEBSITE} = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND ({COL_WEBSITE} IS NULL OR TRIM({COL_WEBSITE}) = '')
                    """,
                    (found_website, orgnr),
                )
                if cur.rowcount > 0:
                    applied_value += 1

            # 2) Uppdatera status + checked_at om NDJSON är NYARE (eller DB saknar datum)
            #    (julianday hanterar både "YYYY-MM-DD HH:MM:SS" och ISO8601 med T/+00:00)
            if checked_at:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_WEBSITE_STATUS} = ?,
                        {COL_WEBSITE_CHECKED_AT} = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND (
                        {COL_WEBSITE_CHECKED_AT} IS NULL OR TRIM({COL_WEBSITE_CHECKED_AT}) = ''
                        OR julianday(?) > julianday({COL_WEBSITE_CHECKED_AT})
                      )
                    """,
                    (status, checked_at, orgnr, checked_at),
                )
                if cur.rowcount > 0:
                    applied_marker += 1

            if i % COMMIT_EVERY == 0:
                conn.commit()

    return {
        "applied_value": applied_value,
        "applied_marker": applied_marker,
        "skipped": skipped,
        "errors": errors,
    }


def apply_emails_file(conn: sqlite3.Connection, ndjson_path: Path) -> dict:
    """
    NDJSON-format:
    {
      "orgnr": "...",
      "status": "found" | "not_found" | "fetch_failed",
      "emails": "a@b.com,c@d.com",
      "checked_at": "2026-01-08T...+00:00"
    }
    """
    applied_value = 0
    applied_marker = 0
    skipped = 0
    errors = 0

    with ndjson_path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = _safe_loads(line)
            if not obj:
                errors += 1
                continue

            orgnr = (obj.get("orgnr") or "").strip()
            status = (obj.get("status") or "").strip().lower() or "checked"
            checked_at = (obj.get("checked_at") or "").strip()
            emails_csv = (obj.get("emails") or "").strip()

            if not orgnr:
                skipped += 1
                continue

            # 1) Sätt emails ENDAST om DB.emails är tom
            if emails_csv:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_EMAILS} = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND ({COL_EMAILS} IS NULL OR TRIM({COL_EMAILS}) = '')
                    """,
                    (emails_csv, orgnr),
                )
                if cur.rowcount > 0:
                    applied_value += 1

            # 2) Uppdatera status + checked_at om NDJSON är NYARE (eller DB saknar datum)
            if checked_at:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_EMAIL_STATUS} = ?,
                        {COL_EMAILS_CHECKED_AT} = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND (
                        {COL_EMAILS_CHECKED_AT} IS NULL OR TRIM({COL_EMAILS_CHECKED_AT}) = ''
                        OR julianday(?) > julianday({COL_EMAILS_CHECKED_AT})
                      )
                    """,
                    (status, checked_at, orgnr, checked_at),
                )
                if cur.rowcount > 0:
                    applied_marker += 1

            if i % COMMIT_EVERY == 0:
                conn.commit()

    return {
        "applied_value": applied_value,
        "applied_marker": applied_marker,
        "skipped": skipped,
        "errors": errors,
    }


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    website_files = _list_files(WEBSITES_PATTERNS)
    email_files = _list_files(EMAILS_PATTERNS)

    print("=== FILES ===")
    print(f"Websites files: {len(website_files)}")
    for p in website_files:
        print(" -", p)
    print(f"Emails files: {len(email_files)}")
    for p in email_files:
        print(" -", p)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS};")

    try:
        total_wv = total_wm = total_we = 0
        if website_files:
            print("\n=== APPLY WEBSITES (ALL FILES) ===")
            for idx, fp in enumerate(website_files, start=1):
                r = apply_websites_file(conn, fp)
                conn.commit()
                total_wv += r["applied_value"]
                total_wm += r["applied_marker"]
                total_we += r["errors"]
                print(f"[web {idx}/{len(website_files)}] {fp.name} value={r['applied_value']} marker={r['applied_marker']} errors={r['errors']}")
        else:
            print("\n[websites] inga filer hittades.")

        total_ev = total_em = total_ee = 0
        if email_files:
            print("\n=== APPLY EMAILS (ALL FILES) ===")
            for idx, fp in enumerate(email_files, start=1):
                r = apply_emails_file(conn, fp)
                conn.commit()
                total_ev += r["applied_value"]
                total_em += r["applied_marker"]
                total_ee += r["errors"]
                print(f"[email {idx}/{len(email_files)}] {fp.name} value={r['applied_value']} marker={r['applied_marker']} errors={r['errors']}")
        else:
            print("\n[emails] inga filer hittades.")

        print("\n=== SUMMARY ===")
        print(f"WEBSITES total: value={total_wv} marker={total_wm} errors={total_we}")
        print(f"EMAILS   total: value={total_ev} marker={total_em} errors={total_ee}")
        print("DONE ✅")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
