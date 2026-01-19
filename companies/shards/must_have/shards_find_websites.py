import re
import time
import json
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import argparse

import requests
from urllib.parse import urlsplit
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

OUT_PATH = Path(f"data/out/websites_guess_shard{SHARD_ID}.ndjson")
LIMIT = 0  # 0 = ALLA
RESUME = True
PRINT_EVERY = 50
# =========================

TIMEOUT_SECONDS = 10
SLEEP_BETWEEN_REQUESTS = 0.2

TLDS = ["se", "com"]
REFRESH_DAYS = 90

PARKED_STRONG = [
    "domain for sale",
    "buy this domain",
    "domain is for sale",
    "köp domän",
    "köp domänen",
]
PARKED_WEAK = [
    "parked",
    "sedo",
    "afternic",
    "dan.com",
    "one.com",
    "namecheap",
    "godaddy",
    "this domain",
]

session = requests.Session()
session.headers.update({
    # Kommentar: håll UA enkel/normal. Det här är “botigt” men ok.
    "User-Agent": f"Mozilla/5.0 (Didup-Site-Guesser/1.0; shard={SHARD_ID})"
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


def needs_refresh(website: Optional[str], checked_at: Optional[str]) -> bool:
    if not website or not str(website).strip():
        return True
    dt = parse_iso(checked_at)
    if not dt:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
    return dt < cutoff


def _normalize_swedish(s: str) -> str:
    return s.replace("å", "a").replace("ä", "a").replace("ö", "o").replace("é", "e")


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


STOP_WORDS = {
    "holding", "fastighet", "fastigheter", "konsult", "konsulting", "consulting",
    "group", "gruppen", "förvaltning", "forvaltning", "management",
    "service", "services", "solutions", "solution",
    "invest", "investments", "investerare", "investeringar",
    "trading", "transport", "bygg", "byggnad", "entreprenad",
    "ab", "aktiebolag", "hb", "kb"
}


def _words(s: str) -> list[str]:
    return [w for w in (s or "").split() if w]


def _make_slug_from_words(words: list[str], hyphen: bool) -> str:
    if not words:
        return ""
    txt = "-".join(words) if hyphen else "".join(words)
    txt = re.sub(r"-+", "-", txt).strip("-")
    return txt if len(txt) >= 3 else ""


def slug_variants(clean_name: str) -> list[str]:
    ws = _words(clean_name)
    ws_f = [w for w in ws if w not in STOP_WORDS]

    variants: list[str] = []

    variants.append(slug_compact(clean_name))
    variants.append(slug_hyphen(clean_name))

    if ws_f:
        variants.append(_make_slug_from_words(ws_f, hyphen=False))
        variants.append(_make_slug_from_words(ws_f, hyphen=True))

        variants.append(_make_slug_from_words(ws_f[:1], hyphen=False))
        variants.append(_make_slug_from_words(ws_f[:1], hyphen=True))
        variants.append(_make_slug_from_words(ws_f[:2], hyphen=False))
        variants.append(_make_slug_from_words(ws_f[:2], hyphen=True))
        variants.append(_make_slug_from_words(ws_f[:3], hyphen=False))
        variants.append(_make_slug_from_words(ws_f[:3], hyphen=True))

    initials = "".join([w[0] for w in ws_f if len(w) >= 2]) if ws_f else ""
    if len(initials) >= 3:
        variants.append(initials)
        variants.append("-".join(list(initials)))

    seen = set()
    out = []
    for v in variants:
        v = (v or "").strip()
        if not v:
            continue
        if len(v) > 40:
            continue
        if v not in seen:
            seen.add(v)
            out.append(v)

    return out[:12]


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
    # Kommentar: bara https för färre requests
    return [f"https://{domain}"]


def looks_like_html(headers: dict) -> bool:
    ct = (headers.get("Content-Type") or "").lower()
    return ("text/html" in ct) or ("application/xhtml" in ct) or ct.startswith("text/")


def is_parked_html(html_lower: str) -> bool:
    if any(k in html_lower for k in PARKED_STRONG):
        return True
    weak_hits = sum(1 for k in PARKED_WEAK if k in html_lower)
    return weak_hits >= 2


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
        u = url.strip()
        parts = urlsplit(u)
        if parts.scheme not in ("http", "https"):
            return False
        host = parts.hostname or ""
        return _valid_hostname(host)
    except Exception:
        return False


def _is_retryable_status(code: int) -> bool:
    # Kommentar: detta är “problem status”, men vi kommer INTE retrya dem längre
    return code in (403, 429, 500, 502, 503, 504)


def _is_dns_miss_error(e: Exception) -> bool:
    msg = str(e).lower()
    return (
        "name or service not known" in msg
        or "failed to resolve" in msg
        or "nodename nor servname" in msg
        or "temporary failure in name resolution" in msg
        or "getaddrinfo failed" in msg
    )


def _head_ok(url: str) -> tuple[bool, bool, str]:
    """
    Returns (ok, is_html-ish, err_code)
    err_code: "" | "403" | "429" | "timeout" | "other"
    """
    if not _safe_url(url):
        return (False, False, "other")

    try:
        r = session.head(
            url,
            timeout=(3, TIMEOUT_SECONDS),
            allow_redirects=True,
        )

        if r.status_code == 405:
            try:
                rg = session.get(
                    url,
                    timeout=(3, TIMEOUT_SECONDS),
                    allow_redirects=True,
                    stream=True,
                )
                rg.close()

                if _is_retryable_status(rg.status_code):
                    return (False, False, str(rg.status_code))

                if not (200 <= rg.status_code < 400):
                    return (False, False, "")

                return (True, True, "")
            except requests.Timeout:
                return (False, False, "timeout")
            except (ConnectionError, requests.RequestException) as e:
                if _is_dns_miss_error(e):
                    return (False, False, "")
                return (False, False, "other")

        if _is_retryable_status(r.status_code):
            return (False, False, str(r.status_code))

        if not (200 <= r.status_code < 400):
            return (False, False, "")

        return (True, looks_like_html(r.headers), "")

    except (LocationParseError, InvalidURL):
        return (False, False, "other")
    except requests.Timeout:
        return (False, False, "timeout")
    except (ConnectionError, requests.RequestException) as e:
        if _is_dns_miss_error(e):
            return (False, False, "")
        return (False, False, "other")
    except Exception as e:
        if _is_dns_miss_error(e):
            return (False, False, "")
        return (False, False, "other")


def _get_snippet_lower(url: str) -> tuple[str, str]:
    """
    Returns (snippet_lower, err_code)
    err_code: "" | "403" | "429" | "timeout" | "other"
    """
    if not _safe_url(url):
        return ("", "other")

    try:
        r = session.get(
            url,
            timeout=(3, TIMEOUT_SECONDS),
            allow_redirects=True,
            stream=True,
        )

        if _is_retryable_status(r.status_code):
            r.close()
            return ("", str(r.status_code))

        if not (200 <= r.status_code < 400):
            r.close()
            return ("", "")

        if not looks_like_html(r.headers):
            r.close()
            return ("", "")

        chunk = r.raw.read(20_000, decode_content=True)
        r.close()

        try:
            return ((chunk.decode("utf-8", errors="ignore") or "").lower(), "")
        except Exception:
            return ("", "other")

    except (LocationParseError, InvalidURL):
        return ("", "other")
    except requests.Timeout:
        return ("", "timeout")
    except (ConnectionError, requests.RequestException) as e:
        if _is_dns_miss_error(e):
            return ("", "")
        return ("", "other")
    except Exception as e:
        if _is_dns_miss_error(e):
            return ("", "")
        return ("", "other")


def fetch_probe(url: str) -> tuple[bool, bool, str]:
    """
    Returns (ok, parked, err_code)
    err_code: "" | "403" | "429" | "timeout" | "other"
    """
    ok, is_html, err = _head_ok(url)
    if not ok:
        return (False, False, err)

    if not is_html:
        return (True, False, "")

    snippet, err2 = _get_snippet_lower(url)
    if err2:
        return (False, False, err2)

    if snippet and is_parked_html(snippet):
        return (True, True, "")

    return (True, False, "")


def pick_targets(conn: sqlite3.Connection, limit: Optional[int]) -> list[tuple[str, str, Optional[str], Optional[str]]]:
    cur = conn.cursor()

    if limit is None:
        cur.execute(
            """
            SELECT orgnr, name, website, website_checked_at
            FROM companies
            ORDER BY website_checked_at IS NOT NULL, website_checked_at ASC
            """
        )
        rows = cur.fetchall()
    else:
        cur.execute(
            """
            SELECT orgnr, name, website, website_checked_at
            FROM companies
            ORDER BY website_checked_at IS NOT NULL, website_checked_at ASC
            LIMIT ?
            """,
            (limit * 5,),
        )
        rows = cur.fetchall()

    out = []
    for orgnr, name, website, checked_at in rows:
        if not orgnr or not name:
            continue
        if needs_refresh(website, checked_at):
            out.append((orgnr, name, website, checked_at))
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
    targets = [(o, n, w, c) for (o, n, w, c) in targets if in_shard(o)]

    if RESUME and done:
        targets = [(o, n, w, c) for (o, n, w, c) in targets if o not in done]

    print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={SHARD_ID}/{SHARD_TOTAL})")

    processed = hits = misses = parked_skips = 0
    err_403 = err_429 = err_timeout = err_other = 0

    start = time.time()

    try:
        with OUT_PATH.open("a", encoding="utf-8") as out_f:
            for orgnr, name, existing_website, existing_checked_at in targets:
                processed += 1

                cleaned = clean_company_name(name)
                slugs = slug_variants(cleaned)
                domains = domain_candidates(slugs)

                found_url = None
                status = "not_found"
                err_reason = ""  # Kommentar: spara varför vi missade

                # Kommentar: NU retryar vi ENDAST timeout (för att inte fastna på WAF)
                had_timeout = False

                for domain in domains:
                    for url in url_variants(domain):
                        ok, parked, err = fetch_probe(url)
                        time.sleep(SLEEP_BETWEEN_REQUESTS)

                        if not ok:
                            if err == "403":
                                err_403 += 1
                                err_reason = "403"
                            elif err == "429":
                                err_429 += 1
                                err_reason = "429"
                            elif err == "timeout":
                                err_timeout += 1
                                had_timeout = True
                                err_reason = "timeout"
                            elif err:
                                err_other += 1
                                err_reason = "other"
                            continue

                        if parked:
                            parked_skips += 1
                            status = "parked"
                            continue

                        found_url = url
                        status = "found"
                        err_reason = ""
                        break

                    if found_url:
                        break

                if found_url:
                    hits += 1
                else:
                    misses += 1
                    # Kommentar: retry bara om vi hade timeout (tillfälligt)
                    if had_timeout:
                        status = "retry"
                    else:
                        status = "not_found"

                # Kommentar: om retry -> skriv inte, så den kan köras om senare
                if status != "retry":
                    row = {
                        "orgnr": orgnr,
                        "name": name,
                        "found_website": found_url or "",
                        "status": status,              # found / parked / not_found
                        "err_reason": err_reason,      # 403 / 429 / timeout / other / ""
                        "checked_at": utcnow_iso(),
                        "db_website_before": (existing_website or ""),
                        "db_checked_at_before": (existing_checked_at or ""),
                    }
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(
                        f"[{processed}] hits={hits} misses={misses} parked={parked_skips} "
                        f"403={err_403} 429={err_429} timeout={err_timeout} other={err_other} | {rate:.1f}/s"
                    )

    except KeyboardInterrupt:
        print("\nAvbruten (Ctrl+C) — filen är sparad ✅")
    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed} | HITS: {hits} | MISSES: {misses} | Parked: {parked_skips}")
    print(f"Errors: 403={err_403} 429={err_429} timeout={err_timeout} other={err_other}")
    print(f"OUT: {OUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
