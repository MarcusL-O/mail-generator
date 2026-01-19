# klar fungerar, bra data 19/01


import re
import time
import json
import sqlite3
import hashlib
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

import requests
from requests.exceptions import ConnectionError, InvalidURL
from urllib3.exceptions import LocationParseError

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
OUT_PATH = Path(f"data/out/site_review_shard{SHARD_ID}.ndjson")
LIMIT = 0              # 0 = ALLA
RESUME = True
PRINT_EVERY = 50
REFRESH_DAYS = 30      # Kommentar: rerun efter 30 dagar
# =========================

TIMEOUT_SECONDS = 12
SLEEP_BETWEEN_REQUESTS = 0.15
MAX_BYTES = 500_000    # Kommentar: vi läser max ~500KB HTML

session = requests.Session()
session.headers.update({
    "User-Agent": f"Mozilla/5.0 (Didup-WebReview/1.0; shard={SHARD_ID})"
})

CTA_WORDS = [
    "kontakta", "kontakt", "boka", "offert", "kostnadsfri", "prisförslag", "priser",
    "ring", "maila", "skicka", "förfrågan"
]
SERVICE_HINTS = [
    "/tjanst", "/tjanster", "/service", "/services", "/vara-tjanster", "/vad-vi-gor",
    "tjänst", "tjänster", "service", "services"
]

EMAIL_RE = re.compile(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", re.I)
TEL_RE = re.compile(r"(?:\+46|0)\s?\d[\d\s\-]{6,}", re.I)
ORGNR_RE = re.compile(r"\b\d{6}\-\d{4}\b")

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def needs_refresh(checked_at: Optional[str]) -> bool:
    dt = parse_iso(checked_at)
    if not dt:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
    return dt < cutoff

def in_shard(orgnr: str) -> bool:
    h = hashlib.md5(orgnr.encode("utf-8")).hexdigest()
    return (int(h, 16) % SHARD_TOTAL) == SHARD_ID

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
    # Kommentar: vi retryar INTE 403/429, bara timeout (men vi vill kunna särskilja dessa koder)
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

def fetch_html(url: str) -> tuple[Optional[str], str]:
    """
    Returns (html_text_or_none, err_reason)
    err_reason: "" | "403" | "429" | "timeout" | "other" | "not_html"
    """
    if not _safe_url(url):
        return (None, "other")

    try:
        r = session.get(
            url,
            timeout=(3, TIMEOUT_SECONDS),
            allow_redirects=True,
            stream=True,
        )

        if _is_retryable_status(r.status_code):
            r.close()
            return (None, str(r.status_code))

        if not (200 <= r.status_code < 400):
            r.close()
            return (None, "other")

        ct = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ct and "application/xhtml" not in ct and not ct.startswith("text/"):
            r.close()
            return (None, "not_html")

        # Kommentar: läs max MAX_BYTES
        chunks = []
        read = 0
        for chunk in r.iter_content(chunk_size=32_768):
            if not chunk:
                break
            chunks.append(chunk)
            read += len(chunk)
            if read >= MAX_BYTES:
                break
        r.close()

        raw = b"".join(chunks)
        html = raw.decode("utf-8", errors="ignore")
        return (html, "")

    except requests.Timeout:
        return (None, "timeout")
    except (LocationParseError, InvalidURL):
        return (None, "other")
    except (ConnectionError, requests.RequestException) as e:
        if _is_dns_miss_error(e):
            return (None, "other")
        return (None, "other")
    except Exception:
        return (None, "other")

def fingerprint_tech(html_lower: str) -> str:
    # Kommentar: enkel, billig fingerprint
    if "wp-content" in html_lower or "wp-includes" in html_lower:
        return "wordpress"
    if "cdn.shopify.com" in html_lower or "x-shopify" in html_lower:
        return "shopify"
    if "wix.com" in html_lower or "wixsite" in html_lower:
        return "wix"
    if "__next" in html_lower or "nextjs" in html_lower:
        return "nextjs"
    return ""

def extract_internal_links(html_lower: str) -> list[str]:
    # Kommentar: enkel href-extraktion (inte perfekt men snabb)
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html_lower)
    out = []
    for h in hrefs:
        h = (h or "").strip()
        if not h:
            continue
        if h.startswith("mailto:") or h.startswith("tel:"):
            continue
        if h.startswith("#"):
            continue
        out.append(h)
    return out[:2000]

def compute_score(url: str, html: str) -> tuple[int, list[str]]:
    flags: list[str] = []
    html_lower = html.lower()

    points = 0

    # 1) HTTPS
    if url.startswith("https://"):
        points += 2
    else:
        flags.append("no_https")

    # 2) Kontaktinfo
    has_email = EMAIL_RE.search(html_lower) is not None
    has_tel = TEL_RE.search(html_lower) is not None
    has_form = "<form" in html_lower
    if has_email or has_tel or has_form or ("kontakt" in html_lower):
        points += 2
    else:
        flags.append("no_contact")

    # 3) CTA-ord
    cta_hits = sum(1 for w in CTA_WORDS if w in html_lower)
    if cta_hits >= 2:
        points += 1
    else:
        flags.append("low_cta")

    # 4) Tjänsteindikatorer / interna länkar
    links = extract_internal_links(html_lower)
    service_link_hits = 0
    for h in links:
        if any(s in h for s in SERVICE_HINTS):
            service_link_hits += 1
    if service_link_hits >= 2:
        points += 1
    else:
        flags.append("few_service_links")

    # 5) Orgnr/adress (seriöshet)
    has_orgnr = ORGNR_RE.search(html_lower) is not None
    has_addressish = ("besöksadress" in html_lower) or ("adress" in html_lower) or ("postadress" in html_lower)
    if has_orgnr or has_addressish:
        points += 1
    else:
        flags.append("no_orgnr_or_address")

    # 6) Bilder (proxy, vi laddar inte ner)
    img_count = html_lower.count("<img")
    has_og_image = 'property="og:image"' in html_lower or "og:image" in html_lower
    if img_count >= 3 or has_og_image:
        points += 1
    else:
        flags.append("few_images")

    # 7) Innehållsmängd (thin content proxy)
    text_rough = re.sub(r"<script[\s\S]*?</script>", " ", html_lower)
    text_rough = re.sub(r"<style[\s\S]*?</style>", " ", text_rough)
    text_rough = re.sub(r"<[^>]+>", " ", text_rough)
    text_rough = re.sub(r"\s+", " ", text_rough).strip()
    word_count = len(text_rough.split(" ")) if text_rough else 0
    if word_count >= 250:
        points += 1
    else:
        flags.append("thin_content")

    # 8) Tech fingerprint (info + liten poäng för “modern CMS”)
    tech = fingerprint_tech(html_lower)
    if tech:
        points += 1
    else:
        flags.append("unknown_tech")

    # Clamp till 1–10
    score = max(1, min(10, points))
    return score, flags

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

def pick_targets(conn: sqlite3.Connection, limit: Optional[int]) -> list[tuple[str, str, str, Optional[str]]]:
    cur = conn.cursor()

    # Kommentar: vi tar bara bolag med website, och bara de som behöver refresh
    if limit is None:
        cur.execute(
            """
            SELECT orgnr, name, website, site_review_checked_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) != ''
            """
        )
        rows = cur.fetchall()
    else:
        cur.execute(
            """
            SELECT orgnr, name, website, site_review_checked_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) != ''
            LIMIT ?
            """,
            (limit * 5,),
        )
        rows = cur.fetchall()

    out = []
    for orgnr, name, website, checked_at in rows:
        if not orgnr or not website:
            continue
        if needs_refresh(checked_at):
            out.append((orgnr, name or "", website, checked_at))
            if limit is not None and len(out) >= limit:
                break
    return out

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u
    return u

def _format_score_counts(score_counts: dict[int, int]) -> str:
    # Kommentar: alltid 1..10 i output
    parts = []
    for s in range(1, 11):
        parts.append(f"{s}={score_counts.get(s, 0)}")
    return " ".join(parts)

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

    print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={SHARD_ID}/{SHARD_TOTAL}, REFRESH_DAYS={REFRESH_DAYS})")

    processed = ok = skipped_not_html = 0
    err_403 = err_429 = err_timeout = err_other = 0

    # Kommentar: score-fördelning (bara för OK-rader)
    score_counts: dict[int, int] = {s: 0 for s in range(1, 11)}

    start = time.time()

    try:
        with OUT_PATH.open("a", encoding="utf-8") as out_f:
            for orgnr, name, website, prev_checked_at in targets:
                processed += 1

                url = normalize_url(website)

                html, err = fetch_html(url)
                time.sleep(SLEEP_BETWEEN_REQUESTS)

                # Kommentar: timeout = temporärt => skriv INTE rad (så den kan köras om)
                if err == "timeout":
                    err_timeout += 1
                    if processed % PRINT_EVERY == 0:
                        rate = processed / max(1e-9, time.time() - start)
                        print(
                            f"[{processed}] ok={ok} 403={err_403} 429={err_429} timeout={err_timeout} other={err_other} | {rate:.1f}/s\n"
                            f"Scores: {_format_score_counts(score_counts)}"
                        )
                    continue

                row = {
                    "orgnr": orgnr,
                    "name": name,
                    "website": url,
                    "checked_at": utcnow_iso(),
                    "err_reason": "",
                    "site_score": None,
                    "site_flags": [],
                }

                if err in ("403", "429"):
                    if err == "403":
                        err_403 += 1
                    else:
                        err_429 += 1
                    row["err_reason"] = err
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                elif err:
                    if err == "not_html":
                        skipped_not_html += 1
                        row["err_reason"] = "not_html"
                    else:
                        err_other += 1
                        row["err_reason"] = "other"
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                else:
                    score, flags = compute_score(url, html or "")
                    row["site_score"] = score
                    row["site_flags"] = flags
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    ok += 1
                    score_counts[score] = score_counts.get(score, 0) + 1

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(
                        f"[{processed}] ok={ok} not_html={skipped_not_html} "
                        f"403={err_403} 429={err_429} timeout={err_timeout} other={err_other} | {rate:.1f}/s\n"
                        f"Scores: {_format_score_counts(score_counts)}"
                    )

    except KeyboardInterrupt:
        print("\nAvbruten (Ctrl+C) — filen är sparad ✅")
    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed} | OK: {ok} | not_html: {skipped_not_html}")
    print(f"Errors: 403={err_403} 429={err_429} timeout={err_timeout} other={err_other}")
    print(f"Scores: {_format_score_counts(score_counts)}")
    print(f"OUT: {OUT_PATH.resolve()}")

if __name__ == "__main__":
    main()
