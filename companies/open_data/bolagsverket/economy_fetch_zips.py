# companies/open_data/bolagsverket/economy_fetch_zips.py
# Hämtar och sparar årsredovisnings-zippar för ett år (bulkfiler).
# - Försöker lista .zip-länkar från år-sida (om index finns)
# - Om index saknas (404): bruteforce "NN_M.zip" där NN=01..52 och M=1..max
# - Resume via .part + Range
# - Skriver manifest: data/economy/manifests/{year}.txt
# - Sparar zippar i: data/economy/{year}/

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

ZIP_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+\.zip)["\']', re.IGNORECASE)

DEFAULT_BASE = "https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler/arsredovisningar/"
DEFAULT_TIMEOUT = 60


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    name = Path(path).name
    if not name.lower().endswith(".zip"):
        raise ValueError(f"URL ser inte ut som en zip: {url}")
    return name


def fetch_html(url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "economy_fetch/1.0"})
    r.raise_for_status()
    return r.text


def extract_zip_urls(html: str, base_url: str) -> list[str]:
    urls: list[str] = []
    for m in ZIP_HREF_RE.finditer(html):
        href = m.group(1).strip()
        urls.append(urljoin(base_url, href))

    # dedupe (ingen dublett)
    seen = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def head_exists(url: str, timeout: int = DEFAULT_TIMEOUT) -> bool:
    try:
        r = requests.head(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers={"User-Agent": "economy_fetch/1.0"},
        )
        return r.status_code < 400
    except Exception:
        return False


def download_with_resume(url: str, dest: Path, timeout: int = DEFAULT_TIMEOUT) -> tuple[bool, str]:
    _ensure_dir(dest.parent)
    tmp = dest.with_suffix(dest.suffix + ".part")
    existing = tmp.stat().st_size if tmp.exists() else 0

    headers = {"User-Agent": "economy_fetch/1.0"}
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        if r.status_code == 416:
            # Range Not Satisfiable => anta att filen är komplett
            if not dest.exists() and tmp.exists():
                tmp.rename(dest)
            return True, "already_complete(416)"

        r.raise_for_status()

        mode = "ab" if existing > 0 else "wb"
        with open(tmp, mode) as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    if tmp.exists():
        tmp.rename(dest)

    return True, "downloaded"


def brute_force_zip_urls(year_url: str, max_part: int, miss_streak_stop: int) -> list[str]:
    """
    Gissar exakt mönster:
      NN_M.zip där NN=01..52 och M=1..max_part
    Stoppar för en given NN när vi fått miss_streak_stop missar i rad.
    """
    found: list[str] = []
    for nn in range(1, 53):
        prefix = f"{nn:02d}"
        miss_streak = 0

        for m in range(1, max_part + 1):
            url = urljoin(year_url, f"{prefix}_{m}.zip")
            if head_exists(url):
                found.append(url)
                miss_streak = 0
            else:
                miss_streak += 1
                if miss_streak >= miss_streak_stop:
                    break

    # dedupe (ingen dublett)
    seen = set()
    out: list[str] = []
    for u in found:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--year-url", type=str, default=None)
    ap.add_argument("--out-dir", type=str, default=None)
    ap.add_argument("--manifest-dir", type=str, default="data/economy/manifests")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)

    # För bruteforce
    ap.add_argument("--max-part", type=int, default=60, help="Max _N att testa per NN (default 60)")
    ap.add_argument("--miss-stop", type=int, default=5, help="Stoppa efter X missar i rad per NN (default 5)")

    # Begränsar *antal kandidater* vi testar (för snabbtest)
    ap.add_argument("--limit", type=int, default=0, help="0 = ingen limit (debug)")

    args = ap.parse_args()

    year = args.year
    year_url = args.year_url or urljoin(DEFAULT_BASE, f"{year}/")

    out_dir = Path(args.out_dir or f"data/economy/{year}")
    manifest_dir = Path(args.manifest_dir)
    _ensure_dir(out_dir)
    _ensure_dir(manifest_dir)

    print(f"YEAR: {year}")
    print(f"YEAR_URL: {year_url}")
    print(f"OUT_DIR: {out_dir}")
    print(f"MANIFEST_DIR: {manifest_dir}")
    print("-" * 60)

    # 1) Försök HTML-index (om det finns)
    zip_urls: list[str] = []
    try:
        html = fetch_html(year_url, timeout=args.timeout)
        zip_urls = extract_zip_urls(html, year_url)
    except requests.exceptions.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        if status != 404:
            raise

    # 2) Fallback: bruteforce (det som gäller för din källa)
    if not zip_urls:
        print("År-URL saknar index (404) eller gav 0 länkar. Fallback: bruteforce NN_M.zip (01..52)...")
        zip_urls = brute_force_zip_urls(year_url, max_part=args.max_part, miss_streak_stop=args.miss_stop)

    if not zip_urls:
        print("Hittade inga zip-filer.")
        sys.exit(2)

    # Debug-limit: begränsa listan (kan göra att du missar filer – bara för snabbtest)
    if args.limit and args.limit > 0:
        zip_urls = zip_urls[: args.limit]

    # Manifest
    manifest_path = manifest_dir / f"{year}.txt"
    manifest_path.write_text("\n".join(zip_urls) + "\n", encoding="utf-8")
    print(f"manifest_written: {manifest_path} (count={len(zip_urls)})")

    # Download
    ok = 0
    skipped = 0
    for i, url in enumerate(zip_urls, 1):
        fn = _safe_filename_from_url(url)
        dest = out_dir / fn

        if dest.exists():
            skipped += 1
            if i % 25 == 0:
                print(f"[{i}/{len(zip_urls)}] skipped_exists={skipped} ok={ok}")
            continue

        success, status = download_with_resume(url, dest, timeout=args.timeout)
        if success:
            ok += 1
        print(f"[{i}/{len(zip_urls)}] {status} -> {dest.name}")

    print("-" * 60)
    print("DONE ✅")
    print(f"downloaded={ok} skipped_already_exists={skipped} total={len(zip_urls)}")


if __name__ == "__main__":
    main()
