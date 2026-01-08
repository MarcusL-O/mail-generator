import re
import time
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/mail_generator_db.sqlite")
OUT_PATH = Path("data/out/emails_found.ndjson")  # outputfil (NDJSON)
LIMIT = 0 # antal att processa (0 = ALLA)
RESUME = True  # hoppa över orgnr som redan finns i OUT_PATH
PRINT_EVERY = 100
# =========================

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

    out: list[str] = []
    seen: set[str] = set()
    for u in candidates:
        if u not in seen:
            out.append(u)
            seen.add(u)

    return out[:3]


def pick_targets(conn: sqlite3.Connection, limit: Optional[int]) -> list[tuple[str, str, str, Optional[str], Optional[str]]]:
    """
    Return (orgnr, name, website, emails, emails_checked_at)
    Hämtar bara de med website och som behöver refresh.
    """
    cur = conn.cursor()

    if limit is None:
        cur.execute(
            """
            SELECT orgnr, name, website, emails, emails_checked_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) <> ''
            ORDER BY emails_checked_at IS NOT NULL, emails_checked_at ASC
            """
        )
        rows = cur.fetchall()
    else:
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
            if limit is not None and len(out) >= limit:
                break
    return out


def load_done_set(path: Path) -> set[str]:
    done = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as rf:
        for line in rf:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                o = (obj.get("orgnr") or "").strip()
                if o:
                    done.add(o)
            except Exception:
                pass
    return done


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    done = load_done_set(OUT_PATH) if RESUME else set()
    limit = None if LIMIT == 0 else LIMIT

    # Read-only connect => inga DB-locks mot andra scripts
    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA journal_mode=WAL;")

    targets = pick_targets(conn, limit)

    if RESUME and done:
        targets = [(o, n, w, e, c) for (o, n, w, e, c) in targets if o not in done]

    print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME})")

    processed = hits = misses = fetch_fail = 0
    start = time.time()

    try:
        with OUT_PATH.open("a", encoding="utf-8") as out_f:
            for orgnr, name, website, emails_before, checked_before in targets:
                processed += 1
                website = normalize_url(website)

                found_emails: list[str] = []
                status = "not_found"

                # 1) startsida
                html_home = fetch_html_snippet(website)
                time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

                if html_home:
                    found_emails = extract_emails_from_html(html_home)
                    found_emails = cap_emails(found_emails, MAX_EMAILS_PER_COMPANY)

                    # 2) kontakt-länkar om inga emails på startsidan
                    if not found_emails:
                        contact_links = find_contact_links(website, html_home)
                        for link in contact_links:
                            html_contact = fetch_html_snippet(link)
                            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)
                            if not html_contact:
                                continue

                            cand = extract_emails_from_html(html_contact)
                            cand = cap_emails(cand, MAX_EMAILS_PER_COMPANY)
                            if cand:
                                found_emails = cand
                                break
                else:
                    fetch_fail += 1
                    status = "fetch_failed"

                if found_emails:
                    hits += 1
                    status = "found"
                else:
                    misses += 1
                    if status != "fetch_failed":
                        status = "not_found"

                row = {
                    "orgnr": orgnr,
                    "name": name,
                    "website": website,
                    "status": status,
                    "emails": ",".join(found_emails),
                    "checked_at": utcnow_iso(),
                    "db_emails_before": (emails_before or ""),
                    "db_emails_checked_at_before": (checked_before or ""),
                }
                out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(f"[{processed}] hits={hits} misses={misses} fetch_fail={fetch_fail} | {rate:.1f}/s")

    except KeyboardInterrupt:
        print("\nAvbruten (Ctrl+C) — filen är sparad ✅")

    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed}")
    print(f"HITS: {hits}")
    print(f"MISSES: {misses}")
    print(f"Fetch-fail: {fetch_fail}")
    print(f"OUT: {OUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
