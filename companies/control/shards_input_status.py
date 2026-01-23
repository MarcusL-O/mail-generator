# Kollar om alla shards data kommit in i db
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# =========================
# KONFIG (samma stil som db_overview.py)
# =========================
DB_PATH = Path("data/db/companies.db.sqlite")

SHARDS_OUT_DIR = Path("data/out")
NDJSON_EXT = ".ndjson"
# =========================

PREFIXES = {
    "websites": "websites_guess_shard",
    "emails": "emails_found_shard",
    "tech": "tech_footprint_shard",
    "site_review": "site_review_shard",
    "hiring": "hiring_review_shard",
}


def resolve_db_path() -> Path:
    if DB_PATH.exists():
        return DB_PATH
    raise FileNotFoundError(f"DB saknas: {DB_PATH}")


def utc_now_str() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def print_kv(key: str, value: Any) -> None:
    print(f"{key:<24} {value}")


def one(cur: sqlite3.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    row = cur.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]


def list_ndjson_files(prefix: str) -> list[Path]:
    if not SHARDS_OUT_DIR.exists():
        return []
    files = [
        p for p in SHARDS_OUT_DIR.iterdir()
        if p.is_file() and p.name.startswith(prefix) and p.suffix == NDJSON_EXT
    ]
    return sorted(files)


def count_ndjson_rows(path: Path) -> int:
    n = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                n += 1
    return n


def print_file_group(label: str, prefix: str) -> int:
    files = list_ndjson_files(prefix)
    print_section(f"NDJSON FILES – {label}")
    if not files:
        print("(none found)")
        return 0

    total = 0
    for p in files:
        rows = count_ndjson_rows(p)
        total += rows
        print_kv(p.name, f"{rows:,} rows")
    print_kv(f"TOTAL {label} rows", f"{total:,}")
    return total


def main() -> None:
    print("SHARDS INPUT STATUS")
    print_kv("Generated", utc_now_str())
    print_kv("NDJSON dir", str(SHARDS_OUT_DIR))

    # NDJSON side
    totals: dict[str, int] = {}
    for label, prefix in PREFIXES.items():
        totals[label] = print_file_group(label, prefix)

    # DB side
    db_path = resolve_db_path()
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()

        print_section("DATABASE (companies)")
        total_companies = int(one(cur, "SELECT COUNT(*) FROM companies") or 0)
        print_kv("Database", str(db_path))
        print_kv("total companies", f"{total_companies:,}")

        # Tech
        tech_with = int(
            one(cur, "SELECT COUNT(*) FROM companies WHERE tech_checked_at IS NOT NULL AND TRIM(tech_checked_at) != ''")
            or 0
        )
        print_kv("companies with tech", f"{tech_with:,}")
        print_kv("newest tech_checked_at", one(cur, "SELECT MAX(tech_checked_at) FROM companies") or "(none)")

        # Site review
        site_with = int(
            one(
                cur,
                "SELECT COUNT(*) FROM companies WHERE site_review_checked_at IS NOT NULL AND TRIM(site_review_checked_at) != ''",
            )
            or 0
        )
        print_kv("companies with site_review", f"{site_with:,}")
        print_kv(
            "newest site_review_checked_at",
            one(cur, "SELECT MAX(site_review_checked_at) FROM companies") or "(none)",
        )

        # Hiring
        hiring_with = int(
            one(
                cur,
                "SELECT COUNT(*) FROM companies WHERE hiring_checked_at IS NOT NULL AND TRIM(hiring_checked_at) != ''",
            )
            or 0
        )
        print_kv("companies with hiring", f"{hiring_with:,}")
        print_kv(
            "newest hiring_checked_at",
            one(cur, "SELECT MAX(hiring_checked_at) FROM companies") or "(none)",
        )

        # Websites
        web_with = int(
            one(
                cur,
                "SELECT COUNT(*) FROM companies WHERE website_checked_at IS NOT NULL AND TRIM(website_checked_at) != ''",
            )
            or 0
        )
        print_kv("companies with website_check", f"{web_with:,}")
        print_kv(
            "newest website_checked_at",
            one(cur, "SELECT MAX(website_checked_at) FROM companies") or "(none)",
        )

        # Emails
        email_with = int(
            one(
                cur,
                "SELECT COUNT(*) FROM companies WHERE emails_checked_at IS NOT NULL AND TRIM(emails_checked_at) != ''",
            )
            or 0
        )
        print_kv("companies with email_check", f"{email_with:,}")
        print_kv(
            "newest emails_checked_at",
            one(cur, "SELECT MAX(emails_checked_at) FROM companies") or "(none)",
        )

        # Freshness quickies (tech som proxy)
        print_section("FRESHNESS (tech_checked_at)")
        updated_24h = int(one(cur, "SELECT COUNT(*) FROM companies WHERE tech_checked_at >= datetime('now','-1 day')") or 0)
        updated_7d = int(one(cur, "SELECT COUNT(*) FROM companies WHERE tech_checked_at >= datetime('now','-7 day')") or 0)
        print_kv("tech updated last 24h", f"{updated_24h:,}")
        print_kv("tech updated last 7 days", f"{updated_7d:,}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
