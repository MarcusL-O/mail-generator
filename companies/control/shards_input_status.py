# companies/control/shards_input_status.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import sqlite3

from .db_connection_path import DEFAULT_DB_PATH, connect, one, print_kv


SHARDS_OUT_DIR = Path("data/out")
TECH_PREFIX = "tech_footprint_shard"
NDJSON_EXT = ".ndjson"


def utc_now_str() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def list_ndjson_files() -> list[Path]:
    if not SHARDS_OUT_DIR.exists():
        return []
    return sorted(
        p for p in SHARDS_OUT_DIR.iterdir()
        if p.is_file() and p.name.startswith(TECH_PREFIX) and p.suffix == NDJSON_EXT
    )


def count_ndjson_rows(path: Path) -> int:
    n = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                n += 1
    return n


def main() -> None:
    print("SHARDS INPUT STATUS")
    print_kv("Generated", utc_now_str())
    print_kv("NDJSON dir", str(SHARDS_OUT_DIR))

    files = list_ndjson_files()
    print_section("NDJSON FILES")

    if not files:
        print("(no shard output files found)")
    else:
        total_rows = 0
        for p in files:
            rows = count_ndjson_rows(p)
            total_rows += rows
            print_kv(p.name, f"{rows:,} rows")
        print_kv("TOTAL ndjson rows", f"{total_rows:,}")

    # DB side
    con = connect(DEFAULT_DB_PATH)
    try:
        cur = con.cursor()

        print_section("DATABASE (companies)")

        total_companies = int(one(cur, "SELECT COUNT(*) FROM companies") or 0)
        with_tech = int(
            one(
                cur,
                "SELECT COUNT(*) FROM companies WHERE tech_checked_at IS NOT NULL AND TRIM(tech_checked_at) != ''",
            )
            or 0
        )

        updated_24h = int(
            one(
                cur,
                "SELECT COUNT(*) FROM companies WHERE tech_checked_at >= datetime('now','-1 day')",
            )
            or 0
        )

        updated_7d = int(
            one(
                cur,
                "SELECT COUNT(*) FROM companies WHERE tech_checked_at >= datetime('now','-7 day')",
            )
            or 0
        )

        print_kv("total companies", f"{total_companies:,}")
        print_kv("companies with tech", f"{with_tech:,}")
        print_kv("tech updated last 24h", f"{updated_24h:,}")
        print_kv("tech updated last 7 days", f"{updated_7d:,}")

        newest = one(cur, "SELECT MAX(tech_checked_at) FROM companies")
        print_kv("newest tech_checked_at", newest or "(none)")

    finally:
        con.close()


if __name__ == "__main__":
    main()
