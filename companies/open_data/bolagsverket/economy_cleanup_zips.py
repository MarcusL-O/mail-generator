# scripts_economy/economy_cleanup_zips.py
# Tar bort årsredovisnings-zippar efter lyckad parse (NDJSON finns).
# - Kräver att data/economy/annual_{year}.ndjson finns och är > 0 bytes
# - Tar bort: data/economy/{year}/*.zip och *.zip.part
# - Raderar permanent (ingen papperskorg)

from __future__ import annotations

import argparse
from pathlib import Path

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--econ-dir", type=str, default="data/economy")
    args = ap.parse_args()

    year = args.year
    econ_dir = Path(args.econ_dir)
    year_dir = econ_dir / str(year)
    ndjson_path = econ_dir / f"annual_{year}.ndjson"

    if not ndjson_path.exists() or ndjson_path.stat().st_size <= 0:
        raise SystemExit(f"STOP: NDJSON saknas eller är tom: {ndjson_path}")

    if not year_dir.exists():
        print(f"Inget att radera. År-mapp saknas: {year_dir}")
        return

    zips = list(year_dir.glob("*.zip"))
    parts = list(year_dir.glob("*.zip.part"))

    if not zips and not parts:
        print("Inget att radera.")
        return

    removed = 0
    removed_bytes = 0

    # Radera zip
    for p in zips + parts:
        try:
            sz = p.stat().st_size if p.exists() else 0
            p.unlink()
            removed += 1
            removed_bytes += sz
        except Exception as e:
            print(f"MISS: {p} ({e})")

    print("DONE ✅")
    print(f"removed_files={removed}")
    print(f"removed_bytes={removed_bytes}")
    print(f"kept_ndjson={ndjson_path}")

if __name__ == "__main__":
    main()
