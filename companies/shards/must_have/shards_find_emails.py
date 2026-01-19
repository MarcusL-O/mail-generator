# 30% hitrate men då körd 4 shards websidor och 4 shards mejl. kan påverka.
# Denna version är "safe": DNS/resolve-fel räknas som vanlig miss (inte retry-loop)
# Retry sker ENDAST på timeout (inte 403/429/other) för att inte fastna på WAF.
#
# UPPGRADERINGAR (24/7-safe):
# - Fångar mail i mailto: även om URL-encodat + HTML-escapat
# - Dekodar Cloudflare email-protection (data-cfemail + /cdn-cgi/l/email-protection)
# - Fångar enkla obfuskeringar i text (info [at] x [dot] y) utan browser
# - Skannar fler attribut (data-email/aria/...) utan extra requests
# - Smartare kontaktcrawl: eskalerar bara vid behov + hård cap per domän
# - Stoppar direkt på 403/429 per domän (ingen extra crawl på den domänen)

import re
import time
import json
import sqlite3
import hashlib
import random
import html as html_lib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import argparse
from urllib.parse import urljoin, urlparse, urlsplit, unquote

import requests
from bs4 import BeautifulSoup
from urllib3.exceptions import LocationParseError
from requests.exceptions import InvalidURL, ConnectionError

ap = argparse.ArgumentParser()
ap.add_argument("--shard-id", type=int, required=True)
ap.add_argument("--shard-total", type=int, default=4)
args = ap.parse_args()

SHARD_ID = args.shard_id
SHARD_TOTAL = args.shard_total

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/db/companies.db.sqlite")
OUT_PATH = Path(f"data/out/shards/emails_found_shard{SHARD_ID}.ndjson")
LIMIT = 0
RESUME = True
PRINT_EVERY = 100
# =========================

MAX_EMAILS_PER_COMPANY = 3

# Nätverk
TIMEOUT_SEC = 7
RETRY_COUNT = 1

# Bas-sleep (vi lägger jitter runt detta)
SLEEP_BETWEEN_REQUESTS_SEC = 0.20

# 24/7-safe kontaktcrawl cap (max antal fetch per domän i detta script)
# Notera: startsida räknas också in
MAX_FETCHES_PER_DOMAIN = 8

# Kontaktcrawl eskalering:
# - Försök alltid startsida + upp till 2 kontaktlänkar först
# - Om inga mail hittas -> eskalera och prova fler kontaktlänkar, men aldrig över cap
INITIAL_CONTACT_TRIES = 2
MAX_CONTACT_LINKS_TO_CONSIDER = 12  # hur många vi kan samla, men vi fetch:ar max enligt cap ovan

MAX_READ_BYTES = 80_000

USER_AGENT = "Mozilla/5.0 (compatible; Didup-Mail-Finder/1.2)"

REFRESH_DAYS = 180

CONTACT_KEYWORDS = [
    "kontakt", "kontakta", "kontakt oss",
    "contact", "contact us",
    "support", "help", "kundservice",
    "om oss", "about", "about us",
]

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

# Enkel obfuskering: "info [at] didup [dot] com" / "info(at)didup.com" / "info @ didup . com"
OBFUSCATED_RE = re.compile(
    r"([a-zA-Z0-9._%+\-]{1,64})\s*(?:\(|\[)?\s*(?:at|@)\s*(?:\)|\])?\s*"
    r"([a-zA-Z0-9.\-]{1,190})\s*(?:\(|\[)?\s*(?:dot|\.)\s*(?:\)|\])?\s*"
    r"([a-zA-Z]{2,24})",
    re.IGNORECASE
)

# Cloudflare email protection (data-cfemail)
CFEMAIL_RE = re.compile(r"data-cfemail=['\"]([0-9a-fA-F]+)['\"]")
# Cloudflare l/email-protection#hex i href
CFPROTECT_HREF_RE = re.compile(r"/cdn-cgi/l/email-protection#([0-9a-fA-F]+)")

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


def _is_dns_miss_error(e: Exception) -> bool:
    msg = str(e).lower()
    return (
        "failed to resolve" in msg
        or "name or service not known" in msg
        or "nodename nor servname" in msg
        or "getaddrinfo failed" in msg
        or "temporary failure in name resolution" in msg
    )


def _sleep_jitter(base: float) -> None:
    # Liten jitter så vi inte blir "maskinella" över tid (24/7-safe)
    lo = max(0.0, base * 0.9)
    hi = base * 1.75
    time.sleep(random.uniform(lo, hi))


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
        except (ConnectionError, requests.RequestException) as e:
            if _is_dns_miss_error(e):
                return (None, "")
            return (None, "other")
        except Exception:
            time.sleep(0.35 + attempt * 0.35)

    return (None, "other")


def _clean_email_candidate(em: str) -> str:
    em = (em or "").strip()
    em = html_lib.unescape(em)
    em = unquote(em)
    em = em.strip().strip(".,;:()[]{}<>\"'").lower()
    return em


def _is_blocklisted(em: str) -> bool:
    if not em or "@" not in em:
        return True

    prefix = em.split("@", 1)[0]
    if prefix in BLOCKLIST_PREFIXES:
        return True
    if any(x in em for x in BLOCKLIST_CONTAINS):
        return True

    BAD_TLDS = (".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif", ".css", ".js")
    if em.endswith(BAD_TLDS):
        return True

    return False


def extract_emails_from_text(text: str) -> list[str]:
    if not text:
        return []

    # Normal regex
    found = EMAIL_RE.findall(text)

    out: list[str] = []
    seen: set[str] = set()

    for e in found:
        em = _clean_email_candidate(e)
        if not em or _is_blocklisted(em):
            continue
        if em not in seen:
            out.append(em)
            seen.add(em)

    # Obfuskering (mänskligt synliga)
    # Ex: info [at] didup [dot] com
    try:
        for m in OBFUSCATED_RE.finditer(text):
            local = (m.group(1) or "").strip()
            dom = (m.group(2) or "").strip().strip(".")
            tld = (m.group(3) or "").strip()
            cand = f"{local}@{dom}.{tld}"
            em = _clean_email_candidate(cand)
            if not em or _is_blocklisted(em):
                continue
            if em not in seen:
                out.append(em)
                seen.add(em)
    except Exception:
        pass

    return out


def _cf_decode_hex(cfhex: str) -> Optional[str]:
    # Cloudflare email obfuscation decode
    try:
        cfhex = (cfhex or "").strip()
        if len(cfhex) < 4 or len(cfhex) % 2 != 0:
            return None
        key = int(cfhex[0:2], 16)
        chars = []
        for i in range(2, len(cfhex), 2):
            b = int(cfhex[i:i+2], 16) ^ key
            chars.append(chr(b))
        s = "".join(chars)
        s = _clean_email_candidate(s)
        if s and "@" in s and not _is_blocklisted(s):
            return s
        return None
    except Exception:
        return None


def _extract_cf_protected_emails(html: str) -> list[str]:
    if not html:
        return []
    out: list[str] = []
    seen: set[str] = set()

    # data-cfemail="..."
    for m in CFEMAIL_RE.finditer(html):
        dec = _cf_decode_hex(m.group(1))
        if dec and dec not in seen:
            out.append(dec)
            seen.add(dec)

    # href="/cdn-cgi/l/email-protection#HEX"
    for m in CFPROTECT_HREF_RE.finditer(html):
        dec = _cf_decode_hex(m.group(1))
        if dec and dec not in seen:
            out.append(dec)
            seen.add(dec)

    return out


def extract_emails_from_html(html: str) -> list[str]:
    if not html:
        return []

    emails: list[str] = []

    # 1) Cloudflare skyddade mail
    emails.extend(_extract_cf_protected_emails(html))

    try:
        soup = BeautifulSoup(html, "html.parser")

        # 2) mailto: (URL-decoding + HTML unescape)
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            href_l = href.lower()

            if href_l.startswith("mailto:"):
                raw = href.split(":", 1)[1].split("?", 1)[0]
                raw = _clean_email_candidate(raw)
                if raw and not _is_blocklisted(raw):
                    emails.append(raw)

            # 3) Cloudflare /cdn-cgi l/email-protection länkar kan ligga i href
            if "/cdn-cgi/l/email-protection#" in href_l:
                mm = CFPROTECT_HREF_RE.search(href_l)
                if mm:
                    dec = _cf_decode_hex(mm.group(1))
                    if dec:
                        emails.append(dec)

        # 4) Attribut-scan (utan extra requests)
        # Vanliga ställen där mail gömmer sig: data-email, aria-label, title, content, value
        ATTR_KEYS = ("data-email", "data-mail", "data-contact", "aria-label", "title", "content", "value")
        try:
            for tag in soup.find_all(True):
                for k in ATTR_KEYS:
                    v = tag.get(k)
                    if not v:
                        continue
                    v = html_lib.unescape(str(v))
                    v = unquote(v)
                    emails.extend(extract_emails_from_text(v))
        except Exception:
            pass

        # 5) Textinnehåll
        try:
            text = soup.get_text(" ", strip=True)
            emails.extend(extract_emails_from_text(text))
        except Exception:
            pass

    except Exception:
        pass

    # 6) Regex på rå HTML (som backup)
    # (Vi unescape+unquote lite först för att fånga encodat innehåll)
    try:
        html_norm = html_lib.unescape(html)
        html_norm = unquote(html_norm)
        emails.extend(extract_emails_from_text(html_norm))
    except Exception:
        emails.extend(extract_emails_from_text(html))

    # Dedupe
    out: list[str] = []
    seen: set[str] = set()
    for e in emails:
        e = _clean_email_candidate(e)
        if not e or _is_blocklisted(e):
            continue
        if e not in seen:
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

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        soup = None

    candidates: list[str] = []

    if soup is not None:
        # Försök hitta kontaktlänkar och prioritera "footer/header" liknande
        # (enkelt: vi kollar alla länkar, men låter keyword match styra urvalet)
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

    # Path hints (gissningar)
    for path in CONTACT_PATH_HINTS:
        candidates.append(urljoin(base_url, path))

    # Dedupe
    out: list[str] = []
    seen: set[str] = set()
    for u in candidates:
        if u not in seen:
            out.append(u)
            seen.add(u)

    # Vi returnerar fler kandidater nu, men fetch begränsas senare av cap+eskalering
    return out[:MAX_CONTACT_LINKS_TO_CONSIDER]


def pick_targets(conn: sqlite3.Connection, limit: Optional[int]) -> list[tuple[str, str, str, Optional[str], Optional[str]]]:
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
                err_reason = ""

                # Retry ENDAST på timeout
                had_timeout = False

                # 24/7-safe cap per domän
                fetches_used = 0
                blocked_by_waf = False  # sätts om 403/429 så vi inte fortsätter crawla

                # 1) startsida
                html_home, err = fetch_html_snippet(website)
                fetches_used += 1
                _sleep_jitter(SLEEP_BETWEEN_REQUESTS_SEC)

                if err:
                    if err == "403":
                        err_403 += 1
                        err_reason = "403"
                        blocked_by_waf = True
                    elif err == "429":
                        err_429 += 1
                        err_reason = "429"
                        blocked_by_waf = True
                    elif err == "timeout":
                        err_timeout += 1
                        had_timeout = True
                        err_reason = "timeout"
                    else:
                        err_other += 1
                        err_reason = "other"

                contact_links: list[str] = []

                if html_home:
                    found_emails = extract_emails_from_html(html_home)
                    found_emails = cap_emails(found_emails, MAX_EMAILS_PER_COMPANY)

                    # Hämta kontaktlänkar (utan att nödvändigtvis besöka alla)
                    contact_links = find_contact_links(website, html_home)

                    # Plan:
                    # - Kör initialt bara 1–2 kontaktlänkar om vi inte redan är fulla
                    # - Om fortfarande inga mail -> eskalera och prova fler, men aldrig över cap
                    def can_fetch_more() -> bool:
                        return (not blocked_by_waf) and (fetches_used < MAX_FETCHES_PER_DOMAIN)

                    def handle_err(e: str) -> None:
                        nonlocal err_403, err_429, err_timeout, err_other, had_timeout, err_reason, blocked_by_waf
                        if not e:
                            return
                        if e == "403":
                            err_403 += 1
                            if not err_reason:
                                err_reason = "403"
                            blocked_by_waf = True
                        elif e == "429":
                            err_429 += 1
                            if not err_reason:
                                err_reason = "429"
                            blocked_by_waf = True
                        elif e == "timeout":
                            err_timeout += 1
                            had_timeout = True
                            if not err_reason:
                                err_reason = "timeout"
                        else:
                            err_other += 1
                            if not err_reason:
                                err_reason = "other"

                    # 2) Kontaktlänkar (initial)
                    if len(found_emails) < MAX_EMAILS_PER_COMPANY and contact_links and can_fetch_more():
                        tries = 0
                        for link in contact_links:
                            if tries >= INITIAL_CONTACT_TRIES:
                                break
                            if not can_fetch_more():
                                break

                            html_contact, err2 = fetch_html_snippet(link)
                            fetches_used += 1
                            _sleep_jitter(SLEEP_BETWEEN_REQUESTS_SEC)

                            handle_err(err2)
                            if blocked_by_waf:
                                break
                            if not html_contact:
                                tries += 1
                                continue

                            cand = extract_emails_from_html(html_contact)
                            if cand:
                                merged = found_emails + cand
                                found_emails = cap_emails(merged, MAX_EMAILS_PER_COMPANY)
                                if len(found_emails) >= MAX_EMAILS_PER_COMPANY:
                                    break

                            tries += 1

                    # 3) Eskalering: prova fler kontaktlänkar vid behov (utan att ändra grundlogiken)
                    if (not found_emails) and contact_links and can_fetch_more():
                        for link in contact_links[INITIAL_CONTACT_TRIES:]:
                            if not can_fetch_more():
                                break

                            html_contact, err2 = fetch_html_snippet(link)
                            fetches_used += 1
                            _sleep_jitter(SLEEP_BETWEEN_REQUESTS_SEC)

                            handle_err(err2)
                            if blocked_by_waf:
                                break
                            if not html_contact:
                                continue

                            cand = extract_emails_from_html(html_contact)
                            if not cand:
                                continue

                            merged = found_emails + cand
                            found_emails = cap_emails(merged, MAX_EMAILS_PER_COMPANY)

                            if found_emails:
                                break

                else:
                    if not err:
                        fetch_fail += 1
                    status = "fetch_failed"

                if found_emails:
                    hits += 1
                    status = "found"
                    err_reason = ""
                else:
                    misses += 1
                    if status != "fetch_failed":
                        status = "not_found"

                    # retry bara timeout
                    if had_timeout:
                        status = "retry"

                # Om retry -> skriv INTE till OUT, så den kan köras om senare
                if status != "retry":
                    row = {
                        "orgnr": orgnr,
                        "name": name,
                        "website": website,
                        "status": status,          # found / not_found / fetch_failed
                        "err_reason": err_reason,  # 403/429/timeout/other/""
                        "emails": ",".join(found_emails),
                        "checked_at": utcnow_iso(),
                        "db_emails_before": (emails_before or ""),
                        "db_emails_checked_at_before": (checked_before or ""),
                        "fetches_used": fetches_used,  # extra debug (hjälper dig mäta slowdown vs hitrate)
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
