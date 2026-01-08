import json
import sqlite3
from pathlib import Path

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/mail_generator_db.sqlite")

WEBSITES_NDJSON = Path("data/out/websites_guess.ndjson")
EMAILS_NDJSON = Path("data/out/emails_found.ndjson")

COMMIT_EVERY = 2000
BUSY_TIMEOUT_MS = 10000  # vänta om DB är låst

TABLE = "companies"

# kolumner i DB
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


def apply_websites(conn: sqlite3.Connection) -> dict:
    """
    NDJSON-format (från website-scriptet):
    {
      "orgnr": "...",
      "found_website": "https://...",
      "status": "found" | "not_found" | "parked",
      "checked_at": "2026-01-08T..."
    }
    """
    applied_value = 0
    applied_marker = 0
    skipped = 0
    missing_in_db = 0
    errors = 0

    if not WEBSITES_NDJSON.exists():
        print(f"[websites] saknas: {WEBSITES_NDJSON}")
        return {
            "applied_value": 0, "applied_marker": 0, "skipped": 0,
            "missing_in_db": 0, "errors": 0
        }

    with WEBSITES_NDJSON.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = _safe_loads(line)
            if not obj:
                errors += 1
                continue

            orgnr = (obj.get("orgnr") or "").strip()
            status = (obj.get("status") or "").strip().lower()
            checked_at = (obj.get("checked_at") or "").strip()
            found_website = (obj.get("found_website") or "").strip()

            if not orgnr:
                skipped += 1
                continue

            # 1) Sätt website-värde ENDAST om DB.website är tom och vi har hittat en URL
            if found_website:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_WEBSITE} = ?,
                        {COL_UPDATED_AT} = COALESCE({COL_UPDATED_AT}, datetime('now'))
                    WHERE {COL_ORGNR} = ?
                      AND ({COL_WEBSITE} IS NULL OR TRIM({COL_WEBSITE}) = '')
                    """,
                    (found_website, orgnr),
                )
                if cur.rowcount > 0:
                    applied_value += 1

            # 2) Markera att den är kollad (status + checked_at)
            #    ENDAST om checked_at i DB är tomt (så vi inte "skriver över")
            if checked_at:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_WEBSITE_STATUS} = COALESCE(NULLIF(TRIM({COL_WEBSITE_STATUS}), ''), ?),
                        {COL_WEBSITE_CHECKED_AT} = COALESCE(NULLIF(TRIM({COL_WEBSITE_CHECKED_AT}), ''), ?),
                        {COL_UPDATED_AT} = COALESCE({COL_UPDATED_AT}, datetime('now'))
                    WHERE {COL_ORGNR} = ?
                      AND ({COL_WEBSITE_CHECKED_AT} IS NULL OR TRIM({COL_WEBSITE_CHECKED_AT}) = '')
                    """,
                    (status or "checked", checked_at, orgnr),
                )
                if cur.rowcount > 0:
                    applied_marker += 1

            # om inget uppdaterades alls: antingen saknas orgnr i DB eller den var redan ifylld/markerad
            if (not found_website) and (not checked_at):
                skipped += 1
            else:
                # kolla om orgnr finns i DB om vi inte lyckades uppdatera något alls
                # (billigt: bara när både value+marker misslyckades)
                pass

            if i % COMMIT_EVERY == 0:
                conn.commit()
                print(f"[websites {i}] value={applied_value} marker={applied_marker} skipped={skipped} errors={errors}")

    # missing_in_db: vi räknar enkelt genom att testa orgnr i DB för de rader som inte kunde markeras alls
    # (håller det kort: skippar extra queries här för speed)
    return {
        "applied_value": applied_value,
        "applied_marker": applied_marker,
        "skipped": skipped,
        "missing_in_db": missing_in_db,
        "errors": errors,
    }


def apply_emails(conn: sqlite3.Connection) -> dict:
    """
    NDJSON-format (från email-scriptet):
    {
      "orgnr": "...",
      "status": "found" | "not_found" | "fetch_failed",
      "emails": "a@b.com,c@d.com",
      "checked_at": "2026-01-08T..."
    }
    """
    applied_value = 0
    applied_marker = 0
    skipped = 0
    missing_in_db = 0
    errors = 0

    if not EMAILS_NDJSON.exists():
        print(f"[emails] saknas: {EMAILS_NDJSON}")
        return {
            "applied_value": 0, "applied_marker": 0, "skipped": 0,
            "missing_in_db": 0, "errors": 0
        }

    with EMAILS_NDJSON.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = _safe_loads(line)
            if not obj:
                errors += 1
                continue

            orgnr = (obj.get("orgnr") or "").strip()
            status = (obj.get("status") or "").strip().lower()
            checked_at = (obj.get("checked_at") or "").strip()
            emails_csv = (obj.get("emails") or "").strip()

            if not orgnr:
                skipped += 1
                continue

            # 1) Sätt emails ENDAST om DB.emails är tom och vi har hittat emails
            if emails_csv:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_EMAILS} = ?,
                        {COL_UPDATED_AT} = COALESCE({COL_UPDATED_AT}, datetime('now'))
                    WHERE {COL_ORGNR} = ?
                      AND ({COL_EMAILS} IS NULL OR TRIM({COL_EMAILS}) = '')
                    """,
                    (emails_csv, orgnr),
                )
                if cur.rowcount > 0:
                    applied_value += 1

            # 2) Markera att den är kollad (status + checked_at) utan overwrite:
            #    bara om emails_checked_at är tomt
            if checked_at:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_EMAIL_STATUS} = COALESCE(NULLIF(TRIM({COL_EMAIL_STATUS}), ''), ?),
                        {COL_EMAILS_CHECKED_AT} = COALESCE(NULLIF(TRIM({COL_EMAILS_CHECKED_AT}), ''), ?),
                        {COL_UPDATED_AT} = COALESCE({COL_UPDATED_AT}, datetime('now'))
                    WHERE {COL_ORGNR} = ?
                      AND ({COL_EMAILS_CHECKED_AT} IS NULL OR TRIM({COL_EMAILS_CHECKED_AT}) = '')
                    """,
                    (status or "checked", checked_at, orgnr),
                )
                if cur.rowcount > 0:
                    applied_marker += 1

            if (not emails_csv) and (not checked_at):
                skipped += 1

            if i % COMMIT_EVERY == 0:
                conn.commit()
                print(f"[emails {i}] value={applied_value} marker={applied_marker} skipped={skipped} errors={errors}")

    return {
        "applied_value": applied_value,
        "applied_marker": applied_marker,
        "skipped": skipped,
        "missing_in_db": missing_in_db,
        "errors": errors,
    }


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS};")

    try:
        print("=== APPLY WEBSITES ===")
        w = apply_websites(conn)
        conn.commit()
        print(f"WEBSITES ✅ value={w['applied_value']} marker={w['applied_marker']} skipped={w['skipped']} errors={w['errors']}")

        print("=== APPLY EMAILS ===")
        e = apply_emails(conn)
        conn.commit()
        print(f"EMAILS ✅ value={e['applied_value']} marker={e['applied_marker']} skipped={e['skipped']} errors={e['errors']}")

    except KeyboardInterrupt:
        conn.commit()
        print("\nCtrl+C — commit gjord ✅")

    finally:
        conn.close()

    print("DONE ✅")


if __name__ == "__main__":
    main()
