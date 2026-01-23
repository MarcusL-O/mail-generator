# scripts_economy/economy_fetch_zips.py
# Hämtar och sparar årsredovisnings-zippar för ett år.
# - Listar alla .zip-länkar från en år-sida (HTML med <a href="...zip">)
# - Laddar ner med resume (Range) om filen redan finns delvis
# - Skriver manifest: data/economy/manifests/{year}.txt
# - Sparar zippar i: data/bolagsverket/annual_reports/{year}/
#
# Kör:
#   python scripts_economy/economy_fetch_zips.py --year 2024 --year-url "https://.../arsredovisningar/2024/"
#
# OBS:
# - Om sidan är JS-renderad och HTML saknar länkar kan du behöva peka på en "rå" index-sida.
#   Testa först: curl -L <year-url> och se om .zip syns i HTML.

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

ZIP_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+\.zip)["\']', re.IGNORECASE)

DEFAULT_BASE = "https://vardefulla-datamangder.bolagsverket.se/arsredovisningar/"
DEFAULT_TIMEOUT = 60


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _safe_filename_from_url(url: str) -> str:
    # Tar sista path-segmentet som filnamn
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
        full = urljoin(base_url, href)
        urls.append(full)
    # Dedupe men behåll ordning
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def head_content_length(url: str, timeout: int = DEFAULT_TIMEOUT) -> int | None:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True, headers={"User-Agent": "economy_fetch/1.0"})
        if r.status_code >= 400:
            return None
        cl = r.headers.get("Content-Length")
        return int(cl) if cl and cl.isdigit() else None
    except Exception:
        return None


def download_with_resume(url: str, dest: Path, timeout: int = DEFAULT_TIMEOUT) -> tuple[bool, str]:
    """
    Returnerar (downloaded_or_completed, status_text)
    """
    _ensure_dir(dest.parent)
    tmp = dest.with_suffix(dest.suffix + ".part")

    existing = tmp.stat().st_size if tmp.exists() else 0

    headers = {"User-Agent": "economy_fetch/1.0"}
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        if r.status_code == 416:
            # "Range Not Satisfiable" => vi antar att filen redan är komplett
            if not dest.exists() and tmp.exists():
                tmp.rename(dest)
            return True, "already_complete(416)"
        r.raise_for_status()

        mode = "ab" if existing > 0 else "wb"
        with open(tmp, mode) as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    # Flytta till slutfil
    if tmp.exists():
        tmp.rename(dest)

    return True, "downloaded"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--year-url", type=str, default=None, help="Exakt URL till år-mappen/sidan (slutar oftast med /)")
    ap.add_argument("--out-dir", type=str, default=None)
    ap.add_argument("--manifest-dir", type=str, default="data/economy/manifests")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("--limit", type=int, default=0, help="0 = ingen limit")
    args = ap.parse_args()

    year = args.year
    year_url = args.year_url or urljoin(DEFAULT_BASE, f"{year}/")

    out_dir = Path(args.out_dir or f"data/bolagsverket/annual_reports/{year}")
    manifest_dir = Path(args.manifest_dir)
    _ensure_dir(out_dir)
    _ensure_dir(manifest_dir)

    print(f"YEAR: {year}")
    print(f"YEAR_URL: {year_url}")
    print(f"OUT_DIR: {out_dir}")
    print(f"MANIFEST_DIR: {manifest_dir}")
    print("-" * 60)

    html = fetch_html(year_url, timeout=args.timeout)
    zip_urls = extract_zip_urls(html, year_url)

    if not zip_urls:
        print("Hittade 0 zip-länkar i HTML.")
        print("Tips: kör `curl -L <year-url>` och kontrollera att .zip faktiskt finns i HTML.")
        sys.exit(2)

    if args.limit and args.limit > 0:
        zip_urls = zip_urls[: args.limit]

    manifest_path = manifest_dir / f"{year}.txt"
    manifest_path.write_text("\n".join(zip_urls) + "\n", encoding="utf-8")
    print(f"manifest_written: {manifest_path} (count={len(zip_urls)})")

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
