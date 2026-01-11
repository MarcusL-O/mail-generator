# 30% hitrate men då körd 4 shards websidor och 4 shards mejl. kan påverka, det ser jag imorgon

import re
import time
import json
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import argparse
from urllib.parse import urljoin, urlparse, urlsplit

import requests
from bs4 import BeautifulSoup
from urllib3.exceptions import LocationParseError
from requests.exceptions import InvalidURL

ap = argparse.ArgumentParser()
ap.add_argument("--shard-id", type=int, required=True)
ap.add_argument("--shard-total", type=int, default=4)
args = ap.parse_args()

SHARD_ID = args.shard_id
SHARD_TOTAL = args.shard_total

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/companies.db.sqlite")
OUT_PATH = Path(f"data/out/emails_found_shard{SHARD_ID}.ndjson")  # <-- unik output per shard
LIMIT = 0  # antal att processa (0 = ALLA)
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

# fler vanliga paths (lågrisk, ofta payoff)
CONTACT_PATH_HINTS = [
    "/kontakt", "/kontakt/", "/kontakta", "/kontakta-oss", "/kontakt-oss",
    "/contact", "/contact/", "/contact-us",
    "/support", "/help", "/kundservice",
    "/om-oss", "/about", "/about-us",
    "/om", "/om/", "/about/", "/company/contact",
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


def _valid_hostname(host: str) -> bool:
    if not host:
        return False

    host = host.strip().lower().rstrip(".")
    if len(host) > 253:
        return False

    if any(c.isspace() for c in host):
        return False
    if ".." in host:
        return False

    labels = host.split(".")
    if len(labels) < 2:
        return False

    for lab in labels:
        if not lab or len(lab) > 63:
            return False
        if lab.startswith("-") or lab.endswith("-"):
            return False
        if not re.fullmatch(r"[a-z0-9-]+", lab):
            return False

    return True


def _safe_url(url: str) -> bool:
    try:
        u = (url or "").strip()
        if not u:
            return False
        parts = urlsplit(u)
        if parts.scheme not in ("http", "https"):
            return False
        host = parts.hostname or ""
        return _valid_hostname(host)
    except Exception:
        return False


def _is_retryable_status(code: int) -> bool:
    return code in (403, 429, 500, 502, 503, 504)


def fetch_html_snippet(url: str) -> tuple[Optional[str], str]:
    """
    Returns (html, err_code)
    err_code: "" | "403" | "429" | "timeout" | "other"
    """
    url = normalize_url(url)
    if not url:
        return (None, "other")

    if not _safe_url(url):
        return (None, "other")

    for attempt in range(RETRY_COUNT + 1):
        try:
            with session.get(url, timeout=TIMEOUT_SEC, allow_redirects=True, stream=True) as r:
                if _is_retryable_status(r.status_code):
                    return (None, str(r.status_code))

                if r.status_code >= 400:
                    # riktiga 404/410/etc => ej retry
                    return (None, "")

                buf = bytearray()
                for chunk in r.iter_content(chunk_size=8192):
                    if not chunk:
                        break
                    buf.extend(chunk)
                    if len(buf) >= MAX_READ_BYTES:
                        break

                encoding = r.encoding or "utf-8"
                return (bytes(buf).decode(encoding, errors="ignore"), "")

        except (LocationParseError, InvalidURL):
            return (None, "other")
        except requests.Timeout:
            return (None, "timeout")
        except Exception:
            time.sleep(0.35 + attempt * 0.35)

    return (None, "other")


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

    emails: list[str] = []

    # 1) Försök parse:a HTML (kan krascha på trasiga charrefs)
    try:
        soup = BeautifulSoup(html, "html.parser")

        # mailto:
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if href.lower().startswith("mailto:"):
                mail = href.split(":", 1)[1].split("?", 1)[0].strip().lower()
                if mail:
                    emails.append(mail)

        # extra: regex på synlig text också (bra vid obfuscation)
        try:
            text = soup.get_text(" ", strip=True)
            emails.extend(extract_emails_from_text(text))
        except Exception:
            pass

    except Exception:
        # Fallback: om parsern failar, kör regex direkt på rå HTML
        pass

    # 2) Regex i rå HTML (alltid, även om soup funkade)
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

    base_url = normalize_url(base_url)

    # Försök parse:a HTML, men låt aldrig detta krascha hela pipen
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        # Om parsing failar: vi kan fortfarande testa de vanliga path-hints
        soup = None

    candidates: list[str] = []

    if soup is not None:
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

    # Lägg alltid till path-hints som fallback
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


def in_shard(orgnr: str) -> bool:
    h = hashlib.md5(orgnr.encode("utf-8")).hexdigest()
    return (int(h, 16) % SHARD_TOTAL) == SHARD_ID


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    done = load_done_set(OUT_PATH) if RESUME else set()
    limit = None if LIMIT == 0 else LIMIT

    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA journal_mode=WAL;")

    targets = pick_targets(conn, limit)

    targets = [(o, n, w, e, c) for (o, n, w, e, c) in targets if in_shard(o)]

    if RESUME and done:
        targets = [(o, n, w, e, c) for (o, n, w, e, c) in targets if o not in done]

    print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={SHARD_ID}/{SHARD_TOTAL})")

    processed = hits = misses = fetch_fail = 0
    err_403 = err_429 = err_timeout = err_other = 0

    start = time.time()

    try:
        with OUT_PATH.open("a", encoding="utf-8") as out_f:
            for orgnr, name, website, emails_before, checked_before in targets:
                processed += 1
                website = normalize_url(website)

                found_emails: list[str] = []
                status = "not_found"
                had_retry_error = False

                # 1) startsida
                html_home, err = fetch_html_snippet(website)
                time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

                if err:
                    had_retry_error = True
                    if err == "403":
                        err_403 += 1
                    elif err == "429":
                        err_429 += 1
                    elif err == "timeout":
                        err_timeout += 1
                    else:
                        err_other += 1

                if html_home:
                    found_emails = extract_emails_from_html(html_home)
                    found_emails = cap_emails(found_emails, MAX_EMAILS_PER_COMPANY)

                    # även om vi hittat något, försök förbättra via kontakt-länkar
                    if len(found_emails) < MAX_EMAILS_PER_COMPANY:
                        contact_links = find_contact_links(website, html_home)
                        for link in contact_links:
                            html_contact, err2 = fetch_html_snippet(link)
                            time.sleep(SLEEP_BETWEEN_REQUESTS_SEC)

                            if err2:
                                had_retry_error = True
                                if err2 == "403":
                                    err_403 += 1
                                elif err2 == "429":
                                    err_429 += 1
                                elif err2 == "timeout":
                                    err_timeout += 1
                                else:
                                    err_other += 1

                            if not html_contact:
                                continue

                            cand = extract_emails_from_html(html_contact)
                            if not cand:
                                continue

                            merged = found_emails + cand
                            found_emails = cap_emails(merged, MAX_EMAILS_PER_COMPANY)

                            if len(found_emails) >= MAX_EMAILS_PER_COMPANY:
                                break
                else:
                    # endast counta som fetch_fail om vi inte har en retrybar felkod
                    # (annars kör vi retry-logiken nedan)
                    if not err:
                        fetch_fail += 1
                    status = "fetch_failed"

                if found_emails:
                    hits += 1
                    status = "found"
                else:
                    misses += 1
                    if status != "fetch_failed":
                        status = "not_found"

                    # retry om vi haft nätfel någonstans
                    if had_retry_error:
                        status = "retry"

                # Om retry: skriv INTE till OUT => den kommer igen nästa körning
                if status != "retry":
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
                    print(
                        f"[{processed}] hits={hits} misses={misses} fetch_fail={fetch_fail} "
                        f"403={err_403} 429={err_429} timeout={err_timeout} other={err_other} | {rate:.1f}/s"
                    )

    except KeyboardInterrupt:
        print("\nAvbruten (Ctrl+C) — filen är sparad ✅")

    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed}")
    print(f"HITS: {hits}")
    print(f"MISSES: {misses}")
    print(f"Fetch-fail: {fetch_fail}")
    print(f"Errors: 403={err_403} 429={err_429} timeout={err_timeout} other={err_other}")
    print(f"OUT: {OUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
