#kollar om majel skickats.
# skickar även till min gmail och outlook för att kunna verifiera

# outreach/control/delivery_audit.py
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

# =========================
# KONFIG (samma stil som db_overview.py)
# =========================
DB_PATH = Path("data/db/outreach.db.sqlite")
# =========================

# Kommentar (svenska): “stale” = accepterad/sent men ingen delivered efter X timmar
STALE_HOURS_6 = 6
STALE_HOURS_24 = 24


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


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    return one(cur, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)) is not None


def get_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    cols: set[str] = set()
    for row in cur.execute(f"PRAGMA table_info({table})").fetchall():
        cols.add(row[1])
    return cols


def count_rows(cur: sqlite3.Cursor, table: str) -> int:
    return int(one(cur, f"SELECT COUNT(*) FROM {table}") or 0)


def pct(part: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(part * 100.0 / total):.1f}%"


def print_table_counts(cur: sqlite3.Cursor) -> None:
    print_section("TABLES")
    rows = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    for (name,) in rows:
        print_kv(name, f"{count_rows(cur, name):,} rows")


def print_status_counts(cur: sqlite3.Cursor, table: str, col: str) -> None:
    cols = get_columns(cur, table)
    if col not in cols:
        return

    print_section(f"{table}.{col}")
    rows = cur.execute(
        f"""
        SELECT COALESCE(NULLIF(TRIM(CAST({col} AS TEXT)), ''), '(empty)') AS v, COUNT(*) AS n
        FROM {table}
        GROUP BY v
        ORDER BY n DESC
        LIMIT 30
        """
    ).fetchall()
    for r in rows:
        print_kv(f"- {r['v']}", f"{int(r['n']):,}")


def print_email_messages_core(cur: sqlite3.Cursor, total: int, cols: set[str]) -> None:
    print_section("EMAIL_MESSAGES – CORE COVERAGE")

    def nonempty_count(col: str) -> int:
        return int(one(cur, f"SELECT COUNT(*) FROM email_messages WHERE TRIM(COALESCE({col},'')) != ''") or 0)

    for c in ["to_email", "from_email", "scheduled_at", "sent_at", "error"]:
        if c not in cols:
            continue
        n = nonempty_count(c)
        print_kv(c, f"{n:,} ({pct(n, total)})")

    if "sent_at" in cols:
        newest = one(cur, "SELECT MAX(sent_at) FROM email_messages")
        print_kv("newest sent_at", newest or "(none)")


def _events_table_has_message_id(cur: sqlite3.Cursor) -> bool:
    if not table_exists(cur, "events"):
        return False
    return "message_id" in get_columns(cur, "events")


def print_pipeline_summary(cur: sqlite3.Cursor) -> None:
    """
    Kommentar (svenska):
    Read-only “audit” som klassar varje email_message via events + status.
    Prioritet:
      1) FAILED: event type i ('bounced','complaint','bounce','failed') eller email_messages.status i ('failed','error')
      2) DELIVERED: event type='delivered'
      3) ACCEPTED: event type i ('accepted','sent') eller email_messages.status i ('sent','accepted')
      4) QUEUED: email_messages.status i ('queued','scheduled')
      5) UNKNOWN: resten
    """
    if not table_exists(cur, "email_messages"):
        return

    has_events = table_exists(cur, "events") and _events_table_has_message_id(cur)

    # Kommentar (svenska): events-typer vi letar efter (tål gamla namn också)
    fail_types = ("bounced", "complaint", "bounce", "failed")
    accept_types = ("accepted", "sent")
    delivered_types = ("delivered",)

    total = count_rows(cur, "email_messages")
    print_section("PIPELINE (best effort)")
    print_kv("total messages", f"{total:,}")
    print_kv("events linked by message_id", "yes" if has_events else "no")

    if total == 0:
        return

    # Kommentar (svenska): utan events kan vi bara använda email_messages.status/error
    if not has_events:
        failed = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE status IN ('failed','error') OR TRIM(COALESCE(error,'')) != ''") or 0)
        accepted = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE status IN ('sent','accepted')") or 0)
        queued = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE status IN ('queued','scheduled')") or 0)
        unknown = max(0, total - failed - accepted - queued)

        print_kv("FAILED", f"{failed:,} ({pct(failed, total)})")
        print_kv("ACCEPTED", f"{accepted:,} ({pct(accepted, total)})")
        print_kv("QUEUED", f"{queued:,} ({pct(queued, total)})")
        print_kv("UNKNOWN", f"{unknown:,} ({pct(unknown, total)})")
        return

    # Kommentar (svenska): med events kan vi göra riktig klassning per message_id
    failed = int(
        one(
            cur,
            f"""
            SELECT COUNT(*)
            FROM email_messages em
            WHERE
              em.id IN (SELECT DISTINCT message_id FROM events WHERE type IN {fail_types})
              OR em.status IN ('failed','error')
              OR TRIM(COALESCE(em.error,'')) != ''
            """,
        )
        or 0
    )

    delivered = int(
        one(
            cur,
            f"""
            SELECT COUNT(*)
            FROM email_messages em
            WHERE em.id IN (SELECT DISTINCT message_id FROM events WHERE type IN {delivered_types})
              AND em.id NOT IN (SELECT DISTINCT message_id FROM events WHERE type IN {fail_types})
            """,
        )
        or 0
    )

    accepted = int(
        one(
            cur,
            f"""
            SELECT COUNT(*)
            FROM email_messages em
            WHERE (
                em.id IN (SELECT DISTINCT message_id FROM events WHERE type IN {accept_types})
                OR em.status IN ('sent','accepted')
            )
              AND em.id NOT IN (SELECT DISTINCT message_id FROM events WHERE type IN {fail_types})
              AND em.id NOT IN (SELECT DISTINCT message_id FROM events WHERE type IN {delivered_types})
            """,
        )
        or 0
    )

    queued = int(
        one(
            cur,
            """
            SELECT COUNT(*)
            FROM email_messages em
            WHERE em.status IN ('queued','scheduled')
              AND em.id NOT IN (SELECT DISTINCT message_id FROM events)
            """,
        )
        or 0
    )

    classified = failed + delivered + accepted + queued
    unknown = max(0, total - classified)

    print_kv("FAILED", f"{failed:,} ({pct(failed, total)})")
    print_kv("DELIVERED", f"{delivered:,} ({pct(delivered, total)})")
    print_kv("ACCEPTED", f"{accepted:,} ({pct(accepted, total)})")
    print_kv("QUEUED", f"{queued:,} ({pct(queued, total)})")
    print_kv("UNKNOWN", f"{unknown:,} ({pct(unknown, total)})")


def print_stale_accepteds(cur: sqlite3.Cursor) -> None:
    if not table_exists(cur, "email_messages"):
        return
    if not (table_exists(cur, "events") and _events_table_has_message_id(cur)):
        return

    # Kommentar (svenska): “stale” = accepted/sent men ingen delivered/bounced/complaint efter X timmar
    print_section("STALE ACCEPTED (no delivered yet)")

    for hours in (STALE_HOURS_6, STALE_HOURS_24):
        n = int(
            one(
                cur,
                f"""
                WITH accepted AS (
                  SELECT DISTINCT message_id
                  FROM events
                  WHERE type IN ('accepted','sent')
                ),
                delivered AS (
                  SELECT DISTINCT message_id
                  FROM events
                  WHERE type='delivered'
                ),
                failed AS (
                  SELECT DISTINCT message_id
                  FROM events
                  WHERE type IN ('bounced','complaint','bounce','failed')
                )
                SELECT COUNT(*)
                FROM email_messages em
                WHERE em.id IN (SELECT message_id FROM accepted)
                  AND em.id NOT IN (SELECT message_id FROM delivered)
                  AND em.id NOT IN (SELECT message_id FROM failed)
                  AND (
                    COALESCE(em.sent_at, em.created_at) < datetime('now', ?)
                  )
                """,
                (f"-{hours} hours",),
            )
            or 0
        )
        print_kv(f"- older than {hours}h", f"{n:,}")


def print_top_errors(cur: sqlite3.Cursor) -> None:
    if not table_exists(cur, "email_messages"):
        return
    cols = get_columns(cur, "email_messages")
    if "error" not in cols:
        return

    print_section("TOP ERRORS (email_messages.error)")
    rows = cur.execute(
        """
        SELECT TRIM(error) AS err, COUNT(*) AS n
        FROM email_messages
        WHERE TRIM(COALESCE(error,'')) != ''
        GROUP BY TRIM(error)
        ORDER BY n DESC
        LIMIT 15
        """
    ).fetchall()
    if not rows:
        print("(none)")
        return
    for r in rows:
        print_kv(f"- {r['err']}", f"{int(r['n']):,}")


def print_recent_activity(cur: sqlite3.Cursor) -> None:
    print_section("RECENT ACTIVITY")

    if table_exists(cur, "email_messages"):
        last_24h = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE created_at >= datetime('now','-1 day')") or 0)
        last_7d = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE created_at >= datetime('now','-7 day')") or 0)
        newest = one(cur, "SELECT MAX(created_at) FROM email_messages")
        print_kv("messages last 24h", f"{last_24h:,}")
        print_kv("messages last 7d", f"{last_7d:,}")
        print_kv("newest message", newest or "(none)")

    if table_exists(cur, "events"):
        last_24h = int(one(cur, "SELECT COUNT(*) FROM events WHERE created_at >= datetime('now','-1 day')") or 0)
        last_7d = int(one(cur, "SELECT COUNT(*) FROM events WHERE created_at >= datetime('now','-7 day')") or 0)
        newest = one(cur, "SELECT MAX(created_at) FROM events")
        print_kv("events last 24h", f"{last_24h:,}")
        print_kv("events last 7d", f"{last_7d:,}")
        print_kv("newest event", newest or "(none)")


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()

        print("DELIVERY AUDIT")
        print_kv("Database", str(DB_PATH))
        print_kv("Generated", utc_now_str())

        print_table_counts(cur)

        if not table_exists(cur, "email_messages"):
            print("\n(ingen tabell: email_messages)")
            return

        total_messages = count_rows(cur, "email_messages")
        cols = get_columns(cur, "email_messages")

        print_section("EMAIL_MESSAGES")
        print_kv("total messages", f"{total_messages:,}")
        print_email_messages_core(cur, total_messages, cols)

        # Status breakdowns
        print_status_counts(cur, "email_messages", "status")
        if table_exists(cur, "events"):
            print_status_counts(cur, "events", "type")

        # Pipeline + stale + errors + recent
        print_pipeline_summary(cur)
        print_stale_accepteds(cur)
        print_top_errors(cur)
        print_recent_activity(cur)

        print("\nSUMMARY")
        sent = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE status IN ('sent','accepted')") or 0)
        queued = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE status IN ('queued','scheduled')") or 0)
        failed = int(one(cur, "SELECT COUNT(*) FROM email_messages WHERE status IN ('failed','error') OR TRIM(COALESCE(error,'')) != ''") or 0)
        print_kv("sent/accepted", f"{sent:,} ({pct(sent, total_messages)})")
        print_kv("queued/scheduled", f"{queued:,} ({pct(queued, total_messages)})")
        print_kv("failed/error", f"{failed:,} ({pct(failed, total_messages)})")

    finally:
        con.close()


if __name__ == "__main__":
    main()
