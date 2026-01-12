import re
import time
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

DB_PATH = Path("data/companies.db.sqlite")

BATCH_SIZE = None

TIMEOUT_SECONDS = 5
SLEEP_BETWEEN_REQUESTS = 0.05

TLDS = ["se", "com"]

# refresh: kör om website-check om äldre än X dagar
REFRESH_DAYS = 90

PARKED_KEYWORDS = [
    "domain for sale",
    "buy this domain",
    "köp domän",
    "köp domänen",
    "this domain",
    "parked",
    "sedo",
    "afternic",
    "dan.com",
    "one.com",
    "namecheap",
    "godaddy",
    "domain is for sale",
]

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Didup-Site-Guesser/1.0)"})


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def needs_refresh(website: Optional[str], checked_at: Optional[str]) -> bool:
    # saknar website -> ja
    if not website or not str(website).strip():
        return True

    dt = parse_iso(checked_at)
    if not dt:
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
    return dt < cutoff


def _normalize_swedish(s: str) -> str:
    return (
        s.replace("å", "a")
        .replace("ä", "a")
        .replace("ö", "o")
        .replace("é", "e")
    )


def clean_company_name(name: str) -> str:
    s = name.lower().strip()

    suffixes = [
        " aktiebolag", " ab",
        " handelsbolag", " hb",
        " kommanditbolag", " kb",
        " ekonomisk förening", " ekonomisk forening",
        " ideell förening", " ideell forening",
    ]
    for suf in suffixes:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()

    s = s.replace("&", " och ")
    s = _normalize_swedish(s)

    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def slug_compact(clean_name: str) -> str:
    s = clean_name.replace(" ", "")
    return s if len(s) >= 4 else ""


def slug_hyphen(clean_name: str) -> str:
    s = clean_name.replace(" ", "-")
    s = re.sub(r"-+", "-", s).strip("-")
    return s if len(s) >= 4 else ""


def domain_candidates(slugs: list[str]) -> list[str]:
    domains = []
    for slug in slugs:
        if not slug:
            continue
        for tld in TLDS:
            domains.append(f"{slug}.{tld}")
            domains.append(f"www.{slug}.{tld}")

    seen = set()
    uniq = []
    for d in domains:
        if d not in seen:
            seen.add(d)
            uniq.append(d)
    return uniq


def url_variants(domain: str) -> list[str]:
    return [f"https://{domain}", f"http://{domain}"]


def looks_like_html(headers: dict) -> bool:
    ct = (headers.get("Content-Type") or "").lower()
    return ("text/html" in ct) or ("application/xhtml" in ct) or ct.startswith("text/")


def is_parked_html(html_lower: str) -> bool:
    return any(k in html_lower for k in PARKED_KEYWORDS)


def fetch_probe(url: str) -> tuple[bool, bool]:
    """
    Return (ok, parked)
    ok=True om sidan finns (200-399)
    parked=True om den ser ut som parkerad/sälj-domän
    """
    try:
        r = session.get(
            url,
            timeout=(3, TIMEOUT_SECONDS),
            allow_redirects=True,
            stream=True,
        )

        if not (200 <= r.status_code < 400):
            r.close()
            return (False, False)

        if not looks_like_html(r.headers):
            r.close()
            return (True, False)

        chunk = r.raw.read(20_000, decode_content=True)
        r.close()

        snippet = ""
        try:
            snippet = (chunk.decode("utf-8", errors="ignore") or "").lower()
        except Exception:
            snippet = ""

        if snippet and is_parked_html(snippet):
            return (True, True)

        return (True, False)

    except requests.RequestException:
        return (False, False)


def pick_targets(conn: sqlite3.Connection, limit: int) -> list[tuple[str, str, Optional[str], Optional[str]]]:
    """
    Return (orgnr, name, website, website_checked_at)
    Vi hämtar *kandidater* och filtrerar TTL i Python (enkelt + robust).
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT orgnr, name, website, website_checked_at
        FROM companies
        ORDER BY website_checked_at IS NOT NULL, website_checked_at ASC
        LIMIT ?
        """,
        (limit * 5,),  # plocka lite fler och filtrera
    )
    rows = cur.fetchall()
    out = []
    for orgnr, name, website, checked_at in rows:
        if not orgnr or not name:
            continue
        if needs_refresh(website, checked_at):
            out.append((orgnr, name, website, checked_at))
            if len(out) >= limit:
                break
    return out


def update_company_website(conn: sqlite3.Connection, orgnr: str, website: Optional[str], status: str) -> None:
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


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    targets = pick_targets(conn, BATCH_SIZE)
    print(f"Targets: {len(targets)} (batch={BATCH_SIZE})")

    processed = 0
    hits = 0
    misses = 0
    parked_skips = 0

    try:
        for orgnr, name, _, _ in targets:
            processed += 1

            cleaned = clean_company_name(name)
            slugs = [slug_compact(cleaned), slug_hyphen(cleaned)]
            domains = domain_candidates(slugs)

            found_url = None
            status = "not_found"

            for domain in domains:
                for url in url_variants(domain):
                    ok, parked = fetch_probe(url)
                    time.sleep(SLEEP_BETWEEN_REQUESTS)

                    if not ok:
                        continue

                    if parked:
                        parked_skips += 1
                        status = "parked"
                        continue

                    found_url = url
                    status = "found"
                    break

                if found_url:
                    break

            if found_url:
                hits += 1
                update_company_website(conn, orgnr, found_url, "found")
            else:
                misses += 1
                # Sätt status + checked_at, men skriv inte över website-fältet med tomt
                update_company_website(conn, orgnr, None, status)

            if processed % 200 == 0:
                conn.commit()
                print(f"[{processed}] committed | hits={hits} misses={misses} parked={parked_skips}")

        conn.commit()

    except KeyboardInterrupt:
        conn.commit()
        print("\nAvbruten (Ctrl+C) — commit gjord ✅")

    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed}")
    print(f"HITS: {hits}")
    print(f"MISSES: {misses}")
    print(f"Parked-skips: {parked_skips}")


if __name__ == "__main__":
    main()
