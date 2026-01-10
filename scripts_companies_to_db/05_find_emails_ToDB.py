import re
import time
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

DB_PATH = Path("data/companies.db.sqlite")

BATCH_SIZE = 500

MAX_EMAILS_PER_COMPANY = 3  # ändra till 5 om du vill

# Nätverk
TIMEOUT_SEC = 7
RETRY_COUNT = 1
SLEEP_BETWEEN_REQUESTS_SEC = 0.10

# Speed: läs bara första N bytes
MAX_READ_BYTES = 80_000

USER_AGENT = "Mozilla/5.0 (compatible; Didup-Mail-Finder/1.1)"

# TTL/refresh
REFRESH_DAYS = 180

# Kontakt-sidor att leta efter (sv + eng)
CONTACT_KEYWORDS = [
    "kontakt", "kontakta", "kontakt oss",
    "contact", "contact us",
    "support", "help", "kundservice",
    "om oss", "about", "about us",
]

CONTACT_PATH_HINTS = [
    "/kontakt", "/kontakta-oss", "/kontakt-oss",
    "/contact", "/contact-us",
    "/support", "/help",
    "/om-oss", "/about", "/about-us",
]

BLOCKLIST_PREFIXES = {"noreply", "no-reply", "donotreply", "do-not-reply", "example", "test"}
BLOCKLIST_CONTAINS = {"@example.", "@test.", "@email.com"}

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.7",
})


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def needs_email_refresh(emails: Optional[str], checked_at: Optional[str]) -> bool:
    # saknar emails -> ja
    if not emails or not str(emails).strip():
        return True

    dt = parse_iso(checked_at)
    if not dt:
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
    return dt < cutoff


def normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def same_domain(a: str, b: str) -> bool:
    try:
        ha = urlparse(a).netloc.lower()
        hb = urlparse(b).netloc.lower()
        return ha == hb
    except Exception:
        return False


def fetch_html_snippet(url: str) -> Optional[str]:
    url = normalize_url(url)
    if not url:
        return None

    for attempt in range(RETRY_COUNT + 1):
        try:
            with session.get(url, timeout=TIMEOUT_SEC, allow_redirects=True, stream=True) as r:
                if r.status_code >= 400:
                    continue

                buf = bytearray()
                for chunk in r.iter_content(chunk_size=8192):
                    if not chunk:
                        break
                    buf.extend(chunk)
                    if len(buf) >= MAX_READ_BYTES:
                        break

                encoding = r.encoding or "utf-8"
                return bytes(buf).decode(encoding, errors="ignore")

        except Exception:
            time.sleep(0.35 + attempt * 0.35)

    return None


def extract_emails_from_text(text: str) -> list[str]:
    if not text:
        return []

    found = EMAIL_RE.findall(text)

    out: list[str] = []
    seen: set[str] = set()

    for e in found:
        em = e.strip().strip(".,;:()[]{}<>\"'").lower()
        if not em:
            continue

        prefix = em.split("@", 1)[0]
        if prefix in BLOCKLIST_PREFIXES:
            continue
        if any(x in em for x in BLOCKLIST_CONTAINS):
            continue

        BAD_TLDS = (".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif", ".css", ".js")

        if em.endswith(BAD_TLDS):
            continue

        if em not in seen:
            out.append(em)
            seen.add(em)

    return out


def extract_emails_from_html(html: str) -> list[str]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    emails: list[str] = []

    # 1) mailto:
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href.lower().startswith("mailto:"):
            mail = href.split(":", 1)[1].split("?", 1)[0].strip().lower()
            if mail:
                emails.append(mail)

    # 2) regex i HTML/text
    emails.extend(extract_emails_from_text(html))

    # dedupe preserving order
    out: list[str] = []
    seen: set[str] = set()
    for e in emails:
        e = (e or "").strip().lower()
        if e and e not in seen:
            out.append(e)
            seen.add(e)

    return out


def prioritize_emails(emails: list[str]) -> list[str]:
    def score(email: str) -> int:
        local = email.split("@", 1)[0]
        if local.startswith(("it", "support", "helpdesk", "servicedesk")):
            return 100
        if local.startswith(("info", "kontakt", "contact", "hello", "sales")):
            return 80
        if local.startswith(("admin", "office", "hr", "ekonomi", "finance")):
            return 60
        return 10

    return sorted(emails, key=lambda e: score(e), reverse=True)


def cap_emails(emails: list[str], max_n: int) -> list[str]:
    if not emails:
        return []
    # unique first
    seen = set()
    uniq = []
    for e in emails:
        if e not in seen:
            uniq.append(e)
            seen.add(e)
    return prioritize_emails(uniq)[:max_n]


def find_contact_links(base_url: str, html: str) -> list[str]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []
    base_url = normalize_url(base_url)

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        text = (a.get_text(" ", strip=True) or "").strip().lower()
        href_l = href.lower()

        if not href or href.startswith(("javascript:", "mailto:", "tel:")):
            continue

        if any(k in text for k in CONTACT_KEYWORDS) or any(k in href_l for k in CONTACT_KEYWORDS):
            full = urljoin(base_url, href)
            if same_domain(base_url, full):
                candidates.append(full)

    for path in CONTACT_PATH_HINTS:
        candidates.append(urljoin(base_url, path))

    # dedupe preserving order
    out: list[str] = []
    seen: set[str] = set()
    for u in candidates:
        if u not in seen:
            out.append(u)
            seen.add(u)

    return out[:3]


def pick_targets(conn: sqlite3.Connection, limit: int) -> list[tuple[str, str, str, Optional[str], Optional[str]]]:
    """
    Return (orgnr, name, website, emails, emails_checked_at)
    Hämtar bara de med website och som behöver refresh.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT orgnr, name, website, emails, emails_checked_at
        FROM companies
        WHERE website IS NOT NULL AND TRIM(website) <> ''
        ORDER BY emails_checked_at IS NOT NULL, emails_checked_at ASC
        LIMIT ?
        """,
        (limit * 5,),
    )
    rows = cur.fetchall()

    out = []
    for orgnr, name, website, emails, checked_at in rows:
        if not orgnr or not name or not website:
            continue
        if needs_email_refresh(emails, checked_at):
            out.append((orgnr, name, website, emails, checked_at))
            if len(out) >= limit:
                break
    return out


def update_company_emails(conn: sqlite3.Connection, orgnr: str, emails_csv: Optional[str], status: str) -> None:
    now = utcnow_iso()
    emails_csv = (emails_csv or "").strip()

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
        (emails_csv, emails_csv, emails_csv, status, now, now, orgnr),
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
    fetch_fail = 0

    try:
        for orgnr, name, website, _, _ in targets:
            processed += 1
            website = normalize_url(website)

            emails: list[str] = []

            # 1) startsida
            html_home = fetch_html_snippet(website)
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            if html_home:
                emails = extract_emails_from_html(html_home)
                emails = cap_emails(emails, MAX_EMAILS_PER_COMPANY)

                # 2) kontakt-länkar om inga emails på startsidan
                if not emails:
                    contact_links = find_contact_links(website, html_home)
                    for link in contact_links:
                        html_contact = fetch_html_snippet(link)
                        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)
                        if not html_contact:
                            continue

                        found = extract_emails_from_html(html_contact)
                        found = cap_emails(found, MAX_EMAILS_PER_COMPANY)
                        if found:
                            emails = found
                            break
            else:
                fetch_fail += 1

            if emails:
                hits += 1
                emails_csv = ",".join(emails)
                update_company_emails(conn, orgnr, emails_csv, "found")
            else:
                misses += 1
                # markera att vi försökte; skriv inte över emails med tomt
                status = "fetch_failed" if not html_home else "not_found"
                update_company_emails(conn, orgnr, None, status)

            if processed % 100 == 0:
                conn.commit()
                print(f"[{processed}] committed | hits={hits} misses={misses} fetch_fail={fetch_fail}")

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
    print(f"Fetch-fail: {fetch_fail}")


if __name__ == "__main__":
    main()
