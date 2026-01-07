import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

DB_PATH = Path("data/mail_generator_db.sqlite")

# ====== Peka på dina filer här ======
WEBSITE_HITS_PATH = Path("data/out/gbg_sites_hits.ndjson")      # orgnr + website/url
WEBSITE_MISSES_PATH = Path("data/out/gbg_sites_misses.ndjson")  # orgnr (och ev reason)

EMAIL_HITS_PATH = Path("data/out/email_hits.ndjson")           # orgnr + emails/email
EMAIL_MISSES_PATH = Path("data/out/email_misses.ndjson")       # orgnr (och ev reason)

# Om allt i DB är Göteborg, sätt True för att fylla city på ALLA där den är NULL/blank.
FILL_CITY_FOR_ALL_NULLS = True
CITY_VALUE = "Göteborg"

MAX_EMAILS_PER_COMPANY = 5
COMMIT_EVERY = 200


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_lines(path: Path) -> Iterable[str]:
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


def try_parse_json(line: str) -> Optional[Dict[str, Any]]:
    try:
        obj = json.loads(line)
        if isinstance(obj, dict):
            return obj
    except Exception:
        return None
    return None


def extract_orgnr(obj_or_line: Any) -> Optional[str]:
    # obj = dict eller line = str
    if isinstance(obj_or_line, dict):
        for k in ("orgnr", "org_nr", "organizationNumber", "organisation_number", "organization_number"):
            v = obj_or_line.get(k)
            if v:
                s = re.sub(r"\D", "", str(v))
                return s if len(s) >= 8 else None
        return None

    if isinstance(obj_or_line, str):
        s = re.sub(r"\D", "", obj_or_line)
        return s if len(s) >= 8 else None

    return None


def extract_website(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("website", "url", "domain", "homepage", "site"):
        v = obj.get(k)
        if v:
            s = str(v).strip()
            return s if s else None
    return None


def normalize_emails(value: Optional[str], max_count: int = 5) -> Optional[str]:
    if not value:
        return None

    # split på komma/semikolon/whitespace
    parts = re.split(r"[,\s;]+", value.strip())
    parts = [p.strip().lower() for p in parts if p.strip()]

    # enkel filter: måste ha @ och en punkt efter
    filtered = []
    for p in parts:
        if "@" not in p:
            continue
        domain = p.split("@")[-1]
        if "." not in domain:
            continue
        filtered.append(p)

    # dedupe men behåll ordning
    seen = set()
    unique = []
    for p in filtered:
        if p not in seen:
            seen.add(p)
            unique.append(p)

    unique = unique[:max_count]
    return ",".join(unique) if unique else None


def extract_emails(obj: Dict[str, Any]) -> Optional[str]:
    v = None
    for k in ("emails", "email", "mail"):
        if k in obj:
            v = obj.get(k)
            break

    if not v:
        return None

    if isinstance(v, list):
        raw = ",".join([str(x).strip() for x in v if str(x).strip()])
        return normalize_emails(raw, max_count=MAX_EMAILS_PER_COMPANY)

    return normalize_emails(str(v).strip(), max_count=MAX_EMAILS_PER_COMPANY)


def ensure_city_for_orgnr(conn: sqlite3.Connection, orgnr: str) -> None:
    conn.execute(
        """
        UPDATE companies
        SET city = COALESCE(NULLIF(TRIM(city), ''), ?),
            updated_at = COALESCE(updated_at, ?)
        WHERE orgnr = ?
        """,
        (CITY_VALUE, utcnow_iso(), orgnr),
    )


def upsert_website_result(conn: sqlite3.Connection, orgnr: str, website: Optional[str], status: str) -> None:
    now = utcnow_iso()
    website = (website or "").strip()

    conn.execute(
        """
        UPDATE companies
        SET website = CASE
                WHEN ? IS NOT NULL AND TRIM(?) <> '' THEN ?
                ELSE website
            END,
            website_status = ?,
            website_checked_at = ?,
            updated_at = ?
        WHERE orgnr = ?
        """,
        (website, website, website, status, now, now, orgnr),
    )
    ensure_city_for_orgnr(conn, orgnr)


def upsert_email_result(conn: sqlite3.Connection, orgnr: str, emails: Optional[str], status: str) -> None:
    now = utcnow_iso()
    normalized = normalize_emails((emails or "").strip(), max_count=MAX_EMAILS_PER_COMPANY) or ""

    conn.execute(
        """
        UPDATE companies
        SET emails = CASE
                WHEN ? IS NOT NULL AND TRIM(?) <> '' THEN ?
                ELSE emails
            END,
            email_status = ?,
            emails_checked_at = ?,
            updated_at = ?
        WHERE orgnr = ?
        """,
        (normalized, normalized, normalized, status, now, now, orgnr),
    )
    ensure_city_for_orgnr(conn, orgnr)


def import_file(
    conn: sqlite3.Connection,
    path: Path,
    kind: str,  # "website_hits" | "website_misses" | "email_hits" | "email_misses"
    commit_every: int = COMMIT_EVERY,
) -> Tuple[int, int]:
    if not path or not path.exists():
        print(f"SKIP (saknas): {path}")
        return 0, 0

    processed = 0
    updated = 0

    for line in read_lines(path):
        obj = try_parse_json(line)

        # stöd för plain-text listor med en orgnr per rad
        if obj is None:
            orgnr = extract_orgnr(line)
            if not orgnr:
                continue
            obj = {"orgnr": orgnr}

        orgnr = extract_orgnr(obj)
        if not orgnr:
            continue

        if kind == "website_hits":
            website = extract_website(obj)
            if website:
                upsert_website_result(conn, orgnr, website, "found")
            else:
                upsert_website_result(conn, orgnr, None, "tried")
            updated += 1

        elif kind == "website_misses":
            upsert_website_result(conn, orgnr, None, "not_found")
            updated += 1

        elif kind == "email_hits":
            emails = extract_emails(obj)
            if emails:
                upsert_email_result(conn, orgnr, emails, "found")
            else:
                upsert_email_result(conn, orgnr, None, "tried")
            updated += 1

        elif kind == "email_misses":
            upsert_email_result(conn, orgnr, None, "not_found")
            updated += 1

        processed += 1

        if processed % commit_every == 0:
            conn.commit()
            print(f"[{kind}] processed={processed} updated={updated} (committed)")

    conn.commit()
    print(f"[{kind}] DONE processed={processed} updated={updated}")
    return processed, updated


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    try:
        import_file(conn, WEBSITE_HITS_PATH, "website_hits")
        import_file(conn, WEBSITE_MISSES_PATH, "website_misses")

        import_file(conn, EMAIL_HITS_PATH, "email_hits")
        import_file(conn, EMAIL_MISSES_PATH, "email_misses")

        if FILL_CITY_FOR_ALL_NULLS:
            now = utcnow_iso()
            conn.execute(
                """
                UPDATE companies
                SET city = ?,
                    updated_at = COALESCE(updated_at, ?)
                WHERE city IS NULL OR TRIM(city) = ''
                """,
                (CITY_VALUE, now),
            )
            conn.commit()
            print("City mass-fill DONE ✅")

        print("IMPORT KLART ✅")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
