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
DB_PATH = Path("data/mail_generator_db.sqlite")

OUT_PATH = Path(f"data/out/websites_guess_shard{SHARD_ID}.ndjson")  # <-- unik output per shard
LIMIT = 0  # <-- antal att processa (0 = ALLA)
RESUME = True  # <-- hoppa över orgnr som redan finns i OUT_PATH
PRINT_EVERY = 100
# =========================

TIMEOUT_SECONDS = 5
SLEEP_BETWEEN_REQUESTS = 0.05

TLDS = ["se", "com"]

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


# --- NYTT (minsta möjliga) ---
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
# --- /NYTT ---


def fetch_probe(url: str) -> tuple[bool, bool]:
    # --- NYTT (minsta möjliga) ---
    if not _safe_url(url):
        return (False, False)
    # --- /NYTT ---

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

    # --- NYTT (minsta möjliga) ---
    except (LocationParseError, InvalidURL):
        return (False, False)
    except Exception:
        # säkerhetsnät så scriptet aldrig dör
        return (False, False)
    # --- /NYTT ---
    except requests.RequestException:
        return (False, False)


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

    # shard-filter
    targets = [(o, n, w, c) for (o, n, w, c) in targets if in_shard(o)]

    # resume-filter
    if RESUME and done:
        targets = [(o, n, w, c) for (o, n, w, c) in targets if o not in done]

    print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={SHARD_ID}/{SHARD_TOTAL})")

    processed = hits = misses = parked_skips = 0
    start = time.time()

    try:
        with OUT_PATH.open("a", encoding="utf-8") as out_f:
            for orgnr, name, existing_website, existing_checked_at in targets:
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
                else:
                    misses += 1

                row = {
                    "orgnr": orgnr,
                    "name": name,
                    "found_website": found_url or "",
                    "status": status,
                    "checked_at": utcnow_iso(),
                    "db_website_before": (existing_website or ""),
                    "db_checked_at_before": (existing_checked_at or ""),
                }
                out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(f"[{processed}] hits={hits} misses={misses} parked={parked_skips} | {rate:.1f}/s")

    except KeyboardInterrupt:
        print("\nAvbruten (Ctrl+C) — filen är sparad ✅")

    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed} | HITS: {hits} | MISSES: {misses} | Parked: {parked_skips}")
    print(f"OUT: {OUT_PATH.resolve()}")


if __name__ == "__main__":
    main()
