import json
import re
import time
from pathlib import Path

import requests

IN_PATH = Path("data/out/goteborg_companies_filtered.ndjson")

HITS_PATH = Path("data/out/gbg_sites_hits.ndjson")
MISSES_PATH = Path("data/out/gbg_sites_misses.ndjson")

BATCH_SIZE = 1000

TIMEOUT_SECONDS = 5
SLEEP_BETWEEN_REQUESTS = 0.05

TLDS = ["se", "com"]

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


def load_done_orgnrs() -> set[str]:
    done = set()

    for path in (HITS_PATH, MISSES_PATH):
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    orgnr = (obj.get("orgnr") or "").strip()
                    if orgnr:
                        done.add(orgnr)
                except Exception:
                    continue

    return done


def main():
    HITS_PATH.parent.mkdir(parents=True, exist_ok=True)

    done = load_done_orgnrs()
    if done:
        print(f"Resume: {len(done):,} bolag redan processade (hits+misses) -> skippar dem.")

    scanned = 0
    processed = 0
    hits = 0
    misses = 0
    parked_skips = 0

    with IN_PATH.open("r", encoding="utf-8") as fin, \
         HITS_PATH.open("a", encoding="utf-8") as fh, \
         MISSES_PATH.open("a", encoding="utf-8") as fm:

        for line in fin:
            if not line.strip():
                continue

            scanned += 1
            if processed >= BATCH_SIZE:
                break

            obj = json.loads(line)
            orgnr = (obj.get("orgnr") or "").strip()
            name = (obj.get("name") or "").strip()
            if not orgnr or not name:
                continue

            if orgnr in done:
                continue

            processed += 1

            cleaned = clean_company_name(name)
            slugs = [slug_compact(cleaned), slug_hyphen(cleaned)]
            domains = domain_candidates(slugs)

            website = None

            for domain in domains:
                for url in url_variants(domain):
                    ok, parked = fetch_probe(url)
                    time.sleep(SLEEP_BETWEEN_REQUESTS)

                    if not ok:
                        continue
                    if parked:
                        parked_skips += 1
                        continue

                    website = url
                    break

                if website:
                    break

            if website:
                hits += 1
                out = {"orgnr": orgnr, "name": name, "website": website}
                fh.write(json.dumps(out, ensure_ascii=False) + "\n")
            else:
                misses += 1
                out = {"orgnr": orgnr, "name": name, "website": None, "source": None}
                fm.write(json.dumps(out, ensure_ascii=False) + "\n")

            done.add(orgnr)

    print("KLART ✅")
    print(f"Scannat (tills batchen fylldes): {scanned:,}")
    print(f"Processade i denna körning (batch): {processed:,}")
    print(f"HITS: {hits:,}")
    print(f"MISSES: {misses:,}")
    print(f"Parked-skips: {parked_skips:,}")
    print(f"Output HITS: {HITS_PATH}")
    print(f"Output MISSES: {MISSES_PATH}")


if __name__ == "__main__":
    main()
