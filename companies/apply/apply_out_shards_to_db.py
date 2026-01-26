# Läser shard-ndjson i data/out och applicerar resultatet in i companies-tabellen.

from __future__ import annotations

import json
import glob
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from pathlib import Path

# =========================
# KONFIG
# =========================
DB_PATH = Path("data/db/companies.db.sqlite")
OUT_DIR = Path("data/out/shards")

WEBSITES_PATTERNS = [
    "data/out/shards/websites_guess_shard*.ndjson",
    "data/out/shards/websites_guess.ndjson",
]
EMAILS_PATTERNS = [
    "data/out/shards/emails_found_shard*.ndjson",
    "data/out/shards/emails_found.ndjson",
]
TECH_PATTERNS = [
    "data/out/shards/tech_footprint_shard*.ndjson",
]
SITE_REVIEW_PATTERNS = [
    "data/out/shards/site_review_shard*.ndjson",
]
HIRING_PATTERNS = [
    "data/out/shards/hiring_review_shard*.ndjson",
]

COMMIT_EVERY = 2000
BUSY_TIMEOUT_MS = 10_000

TABLE = "companies"
COL_ORGNR = "orgnr"
COL_UPDATED_AT = "updated_at"
# =========================


def _safe_loads(line: str) -> dict[str, Any] | None:
    try:
        return json.loads(line)
    except Exception:
        return None


def _json_dumps_compact(value: Any) -> str:
    #DB lagrar listor/objekt som JSON-text
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _list_files(patterns: list[str]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for pat in patterns:
        for p in sorted(glob.glob(pat)):
            pp = Path(p)
            if pp.exists():
                rp = pp.resolve()
                if rp not in seen:
                    files.append(pp)
                    seen.add(rp)
    return files


def _commit_maybe(conn: sqlite3.Connection, i: int) -> None:
    if i % COMMIT_EVERY == 0:
        conn.commit()


# -------------------------
# WEBSITES
# -------------------------
def apply_websites_file(conn: sqlite3.Connection, ndjson_path: Path) -> dict[str, int]:
    """
    {
      "orgnr": "...",
      "found_website": "https://...",
      "status": "found" | "not_found" | "parked",
      "err_reason": "...",   (kan finnas)
      "checked_at": "2026-01-08T...+00:00"
    }
    """
    applied_value = applied_marker = skipped = errors = 0

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
            if not orgnr:
                skipped += 1
                continue

            found_website = (obj.get("found_website") or "").strip()
            status = (obj.get("status") or "").strip().lower() or "checked"
            checked_at = (obj.get("checked_at") or "").strip()

            # 1) Sätt website ENDAST om tomt i DB
            if found_website:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET website = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND (website IS NULL OR TRIM(website) = '')
                    """,
                    (found_website, orgnr),
                )
                if cur.rowcount > 0:
                    applied_value += 1

            # 2) Uppdatera status + checked_at om NDJSON är nyare (eller DB saknar datum)
            if checked_at:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET website_status = ?,
                        website_checked_at = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND (
                        website_checked_at IS NULL OR TRIM(website_checked_at) = ''
                        OR julianday(?) > julianday(website_checked_at)
                      )
                    """,
                    (status, checked_at, orgnr, checked_at),
                )
                if cur.rowcount > 0:
                    applied_marker += 1

            _commit_maybe(conn, i)

    return {
        "applied_value": applied_value,
        "applied_marker": applied_marker,
        "skipped": skipped,
        "errors": errors,
    }


# -------------------------
# EMAILS
# -------------------------
def apply_emails_file(conn: sqlite3.Connection, ndjson_path: Path) -> dict[str, int]:
    """
    {
      "orgnr": "...",
      "status": "found" | "not_found" | "fetch_failed",
      "emails": "a@b.com,c@d.com",
      "checked_at": "2026-01-08T...+00:00"
    }
    """
    applied_value = applied_marker = skipped = errors = 0

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
            if not orgnr:
                skipped += 1
                continue

            status = (obj.get("status") or "").strip().lower() or "checked"
            checked_at = (obj.get("checked_at") or "").strip()
            emails_csv = (obj.get("emails") or "").strip()

            # 1) Sätt emails ENDAST om tomt i DB
            if emails_csv:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET emails = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND (emails IS NULL OR TRIM(emails) = '')
                    """,
                    (emails_csv, orgnr),
                )
                if cur.rowcount > 0:
                    applied_value += 1

            # 2) Uppdatera status + checked_at om NDJSON är nyare (eller DB saknar datum)
            if checked_at:
                cur = conn.execute(
                    f"""
                    UPDATE {TABLE}
                    SET email_status = ?,
                        emails_checked_at = ?,
                        {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                      AND (
                        emails_checked_at IS NULL OR TRIM(emails_checked_at) = ''
                        OR julianday(?) > julianday(emails_checked_at)
                      )
                    """,
                    (status, checked_at, orgnr, checked_at),
                )
                if cur.rowcount > 0:
                    applied_marker += 1

            _commit_maybe(conn, i)

    return {
        "applied_value": applied_value,
        "applied_marker": applied_marker,
        "skipped": skipped,
        "errors": errors,
    }


# -------------------------
# TECH FOOTPRINT
# -------------------------
def apply_tech_file(conn: sqlite3.Connection, ndjson_path: Path) -> dict[str, int]:
    """
    {
      "orgnr": "...",
      "checked_at": "...",
      "err_reason": "",
      "microsoft_status": "yes"|"no"|"unknown",
      "microsoft_strength": "weak"|"strong"|null,
      "microsoft_confidence": "low"|"medium"|"high",
      "it_support_signal": "yes"|"no"|"unknown",
      "it_support_confidence": "low"|"medium"|"high"
    }
    """
    applied_marker = skipped = errors = 0

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
            checked_at = (obj.get("checked_at") or "").strip()
            if not orgnr or not checked_at:
                skipped += 1
                continue

            tech_err_reason = (obj.get("err_reason") or "").strip()

            ms_status = (obj.get("microsoft_status") or "").strip() or None
            ms_strength = obj.get("microsoft_strength")
            ms_conf = (obj.get("microsoft_confidence") or "").strip() or None

            it_signal = (obj.get("it_support_signal") or "").strip() or None
            it_conf = (obj.get("it_support_confidence") or "").strip() or None

            cur = conn.execute(
                f"""
                UPDATE {TABLE}
                SET microsoft_status = ?,
                    microsoft_strength = ?,
                    microsoft_confidence = ?,
                    it_support_signal = ?,
                    it_support_confidence = ?,
                    tech_checked_at = ?,
                    tech_err_reason = ?,
                    {COL_UPDATED_AT} = datetime('now')
                WHERE {COL_ORGNR} = ?
                  AND (
                    tech_checked_at IS NULL OR TRIM(tech_checked_at) = ''
                    OR julianday(?) > julianday(tech_checked_at)
                  )
                """,
                (
                    ms_status,
                    ms_strength,
                    ms_conf,
                    it_signal,
                    it_conf,
                    checked_at,
                    tech_err_reason,
                    orgnr,
                    checked_at,
                ),
            )
            if cur.rowcount > 0:
                applied_marker += 1

            _commit_maybe(conn, i)

    return {
        "applied_marker": applied_marker,
        "skipped": skipped,
        "errors": errors,
    }


# -------------------------
# SITE REVIEW
# -------------------------
def apply_site_review_file(conn: sqlite3.Connection, ndjson_path: Path) -> dict[str, int]:
    """
    {
      "orgnr": "...",
      "checked_at": "...",
      "err_reason": "",
      "site_score": 0..10,
      "site_flags": [...]
    }
    """
    applied_marker = skipped = errors = 0

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
            checked_at = (obj.get("checked_at") or "").strip()
            if not orgnr or not checked_at:
                skipped += 1
                continue

            err_reason = (obj.get("err_reason") or "").strip()
            site_score = obj.get("site_score", None)
            site_flags = obj.get("site_flags", None)

            # Kommentar: site_flags lagras som JSON-text (om listan finns)
            site_flags_txt = None
            if site_flags is not None:
                site_flags_txt = _json_dumps_compact(site_flags)

            cur = conn.execute(
                f"""
                UPDATE {TABLE}
                SET site_score = ?,
                    site_flags = ?,
                    site_review_checked_at = ?,
                    site_review_err_reason = ?,
                    {COL_UPDATED_AT} = datetime('now')
                WHERE {COL_ORGNR} = ?
                  AND (
                    site_review_checked_at IS NULL OR TRIM(site_review_checked_at) = ''
                    OR julianday(?) > julianday(site_review_checked_at)
                  )
                """,
                (
                    site_score,
                    site_flags_txt,
                    checked_at,
                    err_reason,
                    orgnr,
                    checked_at,
                ),
            )
            if cur.rowcount > 0:
                applied_marker += 1

            _commit_maybe(conn, i)

    return {
        "applied_marker": applied_marker,
        "skipped": skipped,
        "errors": errors,
    }


# -------------------------
# HIRING REVIEW
# -------------------------
def apply_hiring_file(conn: sqlite3.Connection, ndjson_path: Path) -> dict[str, int]:
    """
    {
      "orgnr": "...",
      "checked_at": "...",
      "err_reason": "",
      "hiring_status": "yes"|"no"|"unknown",
      "hiring_what_text": "...",
      "hiring_count": 0..,
      "evidence_url": "...",
      "external_job_urls": [...]
    }
    """
    applied_marker = skipped = errors = 0

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
            checked_at = (obj.get("checked_at") or "").strip()
            if not orgnr or not checked_at:
                skipped += 1
                continue

            err_reason = (obj.get("err_reason") or "").strip()
            hiring_status = (obj.get("hiring_status") or "").strip() or None
            hiring_what_text = (obj.get("hiring_what_text") or "").strip()
            hiring_count = obj.get("hiring_count", None)

            # evidence_url mappar till hiring_external_urls / hiring_external_urls är kolumnen du har i DB
            evidence_url = (obj.get("evidence_url") or "").strip()
            external_job_urls = obj.get("external_job_urls", [])
            merged_urls: list[str] = []
            if isinstance(external_job_urls, list):
                merged_urls.extend([str(u).strip() for u in external_job_urls if str(u).strip()])
            if evidence_url:
                merged_urls.append(evidence_url)

            urls_txt = _json_dumps_compact(merged_urls) if merged_urls else None

            cur = conn.execute(
                f"""
                UPDATE {TABLE}
                SET hiring_status = ?,
                    hiring_what_text = ?,
                    hiring_count = ?,
                    hiring_checked_at = ?,
                    hiring_err_reason = ?,
                    hiring_external_urls = ?,
                    {COL_UPDATED_AT} = datetime('now')
                WHERE {COL_ORGNR} = ?
                  AND (
                    hiring_checked_at IS NULL OR TRIM(hiring_checked_at) = ''
                    OR julianday(?) > julianday(hiring_checked_at)
                  )
                """,
                (
                    hiring_status,
                    hiring_what_text,
                    hiring_count,
                    checked_at,
                    err_reason,
                    urls_txt,
                    orgnr,
                    checked_at,
                ),
            )
            if cur.rowcount > 0:
                applied_marker += 1

            _commit_maybe(conn, i)

    return {
        "applied_marker": applied_marker,
        "skipped": skipped,
        "errors": errors,
    }


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    # samla alla filer
    website_files = _list_files(WEBSITES_PATTERNS)
    email_files = _list_files(EMAILS_PATTERNS)
    tech_files = _list_files(TECH_PATTERNS)
    site_review_files = _list_files(SITE_REVIEW_PATTERNS)
    hiring_files = _list_files(HIRING_PATTERNS)

    print("=== FILES ===")
    print(f"Websites files: {len(website_files)}")
    for p in website_files:
        print(" -", p)
    print(f"Emails files: {len(email_files)}")
    for p in email_files:
        print(" -", p)
    print(f"Tech files: {len(tech_files)}")
    for p in tech_files:
        print(" -", p)
    print(f"Site review files: {len(site_review_files)}")
    for p in site_review_files:
        print(" -", p)
    print(f"Hiring files: {len(hiring_files)}")
    for p in hiring_files:
        print(" -", p)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS};")

    try:
        # Websites
        total_wv = total_wm = total_we = 0
        if website_files:
            print("\n=== APPLY WEBSITES (ALL FILES) ===")
            for idx, fp in enumerate(website_files, start=1):
                r = apply_websites_file(conn, fp)
                conn.commit()
                total_wv += r["applied_value"]
                total_wm += r["applied_marker"]
                total_we += r["errors"]
                print(
                    f"[web {idx}/{len(website_files)}] {fp.name} "
                    f"value={r['applied_value']} marker={r['applied_marker']} errors={r['errors']}"
                )
        else:
            print("\n[websites] inga filer hittades.")

        # Emails
        total_ev = total_em = total_ee = 0
        if email_files:
            print("\n=== APPLY EMAILS (ALL FILES) ===")
            for idx, fp in enumerate(email_files, start=1):
                r = apply_emails_file(conn, fp)
                conn.commit()
                total_ev += r["applied_value"]
                total_em += r["applied_marker"]
                total_ee += r["errors"]
                print(
                    f"[email {idx}/{len(email_files)}] {fp.name} "
                    f"value={r['applied_value']} marker={r['applied_marker']} errors={r['errors']}"
                )
        else:
            print("\n[emails] inga filer hittades.")

        # Tech
        total_tm = total_te = 0
        if tech_files:
            print("\n=== APPLY TECH (ALL FILES) ===")
            for idx, fp in enumerate(tech_files, start=1):
                r = apply_tech_file(conn, fp)
                conn.commit()
                total_tm += r["applied_marker"]
                total_te += r["errors"]
                print(f"[tech {idx}/{len(tech_files)}] {fp.name} marker={r['applied_marker']} errors={r['errors']}")
        else:
            print("\n[tech] inga filer hittades.")

        # Site review
        total_sm = total_se = 0
        if site_review_files:
            print("\n=== APPLY SITE REVIEW (ALL FILES) ===")
            for idx, fp in enumerate(site_review_files, start=1):
                r = apply_site_review_file(conn, fp)
                conn.commit()
                total_sm += r["applied_marker"]
                total_se += r["errors"]
                print(
                    f"[site {idx}/{len(site_review_files)}] {fp.name} marker={r['applied_marker']} errors={r['errors']}"
                )
        else:
            print("\n[site_review] inga filer hittades.")

        # Hiring
        total_hm = total_he = 0
        if hiring_files:
            print("\n=== APPLY HIRING (ALL FILES) ===")
            for idx, fp in enumerate(hiring_files, start=1):
                r = apply_hiring_file(conn, fp)
                conn.commit()
                total_hm += r["applied_marker"]
                total_he += r["errors"]
                print(
                    f"[hiring {idx}/{len(hiring_files)}] {fp.name} marker={r['applied_marker']} errors={r['errors']}"
                )
        else:
            print("\n[hiring] inga filer hittades.")

        print("\n=== SUMMARY ===")
        print(f"WEBSITES   total: value={total_wv} marker={total_wm} errors={total_we}")
        print(f"EMAILS     total: value={total_ev} marker={total_em} errors={total_ee}")
        print(f"TECH       total: marker={total_tm} errors={total_te}")
        print(f"SITE_REVIEW total: marker={total_sm} errors={total_se}")
        print(f"HIRING     total: marker={total_hm} errors={total_he}")
        print("DONE ✅")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
