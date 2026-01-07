import json
import re
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


IN_PATH = Path("data/out/gbg_sites_hits.ndjson")
HITS_PATH = Path("data/out/email_hits.ndjson")
MISSES_PATH = Path("data/out/email_misses.ndjson")

MAX_EMAILS_PER_COMPANY = 3

# Nätverk
TIMEOUT_SEC = 7
RETRY_COUNT = 1
SLEEP_BETWEEN_REQUESTS_SEC = 0.10

# Speed: läs bara första N bytes (räcker ofta för header/footer + kontaktinfo)
MAX_READ_BYTES = 80_000  # 80KB

USER_AGENT = "Mozilla/5.0 (compatible; Didup-Mail-Finder/1.1)"

LIMIT = 10

# Kontakt-sidor att leta efter (sv + eng)
CONTACT_KEYWORDS = [
    "kontakt", "kontakta", "kontakt oss",
    "contact", "contact us",
    "support", "help", "kundservice",
    "om oss", "about", "about us"
]

# Vanliga URL-paths (om sidan inte har tydliga länkar)
CONTACT_PATH_HINTS = [
    "/kontakt", "/kontakta-oss", "/kontakt-oss",
    "/contact", "/contact-us",
    "/support", "/help",
    "/om-oss", "/about", "/about-us"
]

# Oönskade mail (skräp)
BLOCKLIST_PREFIXES = {"noreply", "no-reply", "donotreply", "do-not-reply", "example", "test"}
BLOCKLIST_CONTAINS = {"@example.", "@test.", "@email.com"}

# Strict email regex (vanliga mail på sidor)
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.7",
})


# -------------------------
# HELPERS
# -------------------------
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


def fetch_html_snippet(url: str) -> str | None:
    """
    Snabb fetch: GET stream=True, läs bara första MAX_READ_BYTES.
    Returnerar en (oftast tillräcklig) HTML-snutt för email-detektion.
    """
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
                text = bytes(buf).decode(encoding, errors="ignore")
                return text

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

    # dedupe
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
    return prioritize_emails(emails)[:max_n]


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

    # fallback: vanliga paths
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


def pick_fields(obj: dict) -> tuple[str, str, str]:
    orgnr = str(obj.get("orgnr") or obj.get("orgNr") or obj.get("organizationNumber") or "").strip()
    name = str(obj.get("name") or obj.get("company_name") or obj.get("bolagsnamn") or "").strip()
    website = str(obj.get("website") or obj.get("site") or obj.get("url") or "").strip()
    return orgnr, name, website


# -------------------------
# MAIN
# -------------------------
def main():
    if not IN_PATH.exists():
        raise SystemExit(f"Input not found: {IN_PATH}")

    HITS_PATH.parent.mkdir(parents=True, exist_ok=True)

    # skriv om filer varje körning (MVP)
    if HITS_PATH.exists():
        HITS_PATH.unlink()
    if MISSES_PATH.exists():
        MISSES_PATH.unlink()

    processed = 0
    hits = 0
    misses = 0

    with IN_PATH.open("r", encoding="utf-8") as f_in, \
         HITS_PATH.open("w", encoding="utf-8") as f_hits, \
         MISSES_PATH.open("w", encoding="utf-8") as f_miss:

        for line in f_in:
            if LIMIT is not None and processed >= LIMIT:
                break

            processed += 1
            obj = json.loads(line)

            orgnr, name, website = pick_fields(obj)
            website = normalize_url(website)

            if not website:
                misses += 1
                f_miss.write(json.dumps({
                    "orgnr": orgnr,
                    "name": name,
                    "website": "",
                    "emails": [],
                    "reason": "no_website"
                }, ensure_ascii=False) + "\n")
                continue

            emails: list[str] = []
            source: str | None = None

            # 1) Startsida (snabb snippet-läsning)
            html_home = fetch_html_snippet(website)
            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

            if html_home:
                emails = extract_emails_from_html(html_home)
                if emails:
                    emails = cap_emails(emails, MAX_EMAILS_PER_COMPANY)
                    source = "home"
                else:
                    # 2) Kontakt-länkar
                    contact_links = find_contact_links(website, html_home)
                    for link in contact_links:
                        html_contact = fetch_html_snippet(link)
                        time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)
                        if not html_contact:
                            continue

                        found = extract_emails_from_html(html_contact)
                        if found:
                            emails = cap_emails(found, MAX_EMAILS_PER_COMPANY)
                            source = f"contact:{link}"
                            break

            if emails:
                hits += 1
                f_hits.write(json.dumps({
                    "orgnr": orgnr,
                    "name": name,
                    "website": website,
                    "emails": emails,
                    "source": source,
                }, ensure_ascii=False) + "\n")
            else:
                misses += 1
                f_miss.write(json.dumps({
                    "orgnr": orgnr,
                    "name": name,
                    "website": website,
                    "emails": [],
                    "reason": "no_email_found_or_fetch_failed"
                }, ensure_ascii=False) + "\n")

            if processed % 50 == 0:
                print(f"[{processed}] hits={hits} misses={misses}")

    print(f"Done. processed={processed} hits={hits} misses={misses}")
    print(f"Hits:   {HITS_PATH}")
    print(f"Misses: {MISSES_PATH}")


if __name__ == "__main__":
    main()
