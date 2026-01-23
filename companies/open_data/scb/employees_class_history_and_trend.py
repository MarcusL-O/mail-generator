# - SNABBT post-process-script (inga HTTP-anrop)
# - Läser companies.scb_employees_class (klass/spann) och scb_status (om finns)
# - Loggar ALLTID en rad per bolag per körning i en egen historiktabell
# - Uppdaterar companies med:
#   - employees_trend: up | down | same | unknown
#   - employees_trend_at: timestamp
#
# Kör t.ex:
#   python employees_class_history_and_trend.py --db data/companies.db.sqlite
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
import re
from typing import Optional, Tuple

UNKNOWN_MARK = "unknown"
DB_PATH = "data/db/companies.db.sqlite"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def table_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    return {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}


def ensure_history_table(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_employee_class_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            orgnr TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            employees_class TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_emp_hist_orgnr ON company_employee_class_history(orgnr);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_emp_hist_observed_at ON company_employee_class_history(observed_at);")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_emp_hist_orgnr_observed "
        "ON company_employee_class_history(orgnr, observed_at);"
    )


def normalize_class(v: Optional[str]) -> str:
    s = (v or "").strip()
    return s if s else UNKNOWN_MARK


def class_rank(v: str) -> Optional[int]:
    s = (v or "").strip().lower()
    if not s or s == UNKNOWN_MARK:
        return None

    m = re.search(r"(\d+)\s*\+", s)
    if m:
        return int(m.group(1))

    m = re.search(r"(\d+)\s*[-–]\s*(\d+)", s)
    if m:
        return int(m.group(1))

    if s.isdigit():
        return int(s)

    m = re.search(r"(\d+)", s)
    if m:
        return int(m.group(1))

    return None


def compute_trend(prev_class: str, new_class: str) -> str:
    prev_n = class_rank(prev_class)
    new_n = class_rank(new_class)

    if prev_n is None or new_n is None:
        return "unknown"
    if new_n > prev_n:
        return "up"
    if new_n < prev_n:
        return "down"
    return "same"


def latest_history_row(cur: sqlite3.Cursor, orgnr: str) -> Optional[Tuple[str, str]]:
    r = cur.execute(
        """
        SELECT employees_class, status
        FROM company_employee_class_history
        WHERE orgnr = ?
        ORDER BY observed_at DESC, id DESC
        LIMIT 1
        """,
        (orgnr,),
    ).fetchone()
    return (str(r[0]), str(r[1])) if r else None


def derive_status(scb_status: Optional[str], employees_class: str) -> str:
    s = (scb_status or "").strip()
    if s:
        return s
    return "ok" if employees_class != UNKNOWN_MARK else "unknown"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--print-every", type=int, default=5000)
    ap.add_argument("--commit-every", type=int, default=5000)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ensure_history_table(cur)
    con.commit()

    cols = table_columns(cur, "companies")
    if "scb_employees_class" not in cols:
        con.close()
        raise SystemExit("companies saknar scb_employees_class")

    has_scb_status = "scb_status" in cols

    total = cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    limit = args.limit if args.limit > 0 else None

    print("EMPLOYEES CLASS HISTORY + TREND")
    print(f"- db: {DB_PATH}")
    print(f"- total companies: {total:,}")

    q = "SELECT orgnr, scb_employees_class" + (", scb_status" if has_scb_status else "") + " FROM companies"
    rows = cur.execute(q + (" LIMIT ?" if limit else ""), ((limit,) if limit else ()))

    observed_at = utc_now_iso()
    processed = up = down = same = unknown = 0

    try:
        for r in rows:
            orgnr = (r["orgnr"] or "").strip()
            if not orgnr:
                continue

            new_class = normalize_class(r["scb_employees_class"])
            scb_status = r["scb_status"] if has_scb_status else None
            status = derive_status(scb_status, new_class)

            prev = latest_history_row(cur, orgnr)
            trend = (
                "unknown" if prev is None and class_rank(new_class) is None
                else "same" if prev is None
                else compute_trend(prev[0], new_class)
            )

            cur.execute(
                """
                INSERT INTO company_employee_class_history
                (orgnr, observed_at, employees_class, status, source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (orgnr, observed_at, new_class, status, "scb_postprocess"),
            )

            cur.execute(
                """
                UPDATE companies
                SET employees_trend = ?, employees_trend_at = ?
                WHERE orgnr = ?
                """,
                (trend, observed_at, orgnr),
            )

            processed += 1
            up += trend == "up"
            down += trend == "down"
            same += trend == "same"
            unknown += trend == "unknown"

            if processed % args.commit_every == 0:
                con.commit()

            if processed % args.print_every == 0:
                print(f"[{processed:,}] up={up:,} down={down:,} same={same:,} unknown={unknown:,}")

    except KeyboardInterrupt:
        print("\n⛔ Avbruten – committar...")

    finally:
        con.commit()
        con.close()

    print("DONE ✅")
    print(f"- processed: {processed:,}")
    print(f"- up={up:,} down={down:,} same={same:,} unknown={unknown:,}")


if __name__ == "__main__":
    main()
