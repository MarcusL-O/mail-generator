# scripts_outreach/migrations/2026_01_19_add_tiering_and_sni_groups.py
# Migrerar outreach.db.sqlite:
# - lägger till tier/match_flags/score i lead_campaigns
# - skapar targeting_sni_groups + targeting_sni_group_items

import sqlite3
from pathlib import Path
from typing import Set

OUTREACH_DB = Path("data/db/outreach.db.sqlite")


def _cols(con: sqlite3.Connection, table: str) -> Set[str]:
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {str(r[1]) for r in cur.fetchall()}


def add_column_if_missing(con: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    if col in _cols(con, table):
        return
    cur = con.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


def main() -> None:
    if not OUTREACH_DB.exists():
        raise SystemExit(f"Hittar inte {OUTREACH_DB}")

    con = sqlite3.connect(str(OUTREACH_DB))
    try:
        con.execute("PRAGMA foreign_keys=OFF;")

        # lead_campaigns: tier/match_flags/score
        add_column_if_missing(con, "lead_campaigns", "tier", "INTEGER")
        add_column_if_missing(con, "lead_campaigns", "match_flags", "TEXT")
        add_column_if_missing(con, "lead_campaigns", "score", "INTEGER")

        # SNI-grupper (targeting config)
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS targeting_sni_groups (
              id         INTEGER PRIMARY KEY AUTOINCREMENT,
              group_key  TEXT    NOT NULL UNIQUE,
              label      TEXT,
              match_mode TEXT    NOT NULL CHECK (match_mode IN ('prefix','exact')),
              created_at TEXT    NOT NULL,
              updated_at TEXT    NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS targeting_sni_group_items (
              id        INTEGER PRIMARY KEY AUTOINCREMENT,
              group_id  INTEGER NOT NULL,
              pattern   TEXT    NOT NULL,
              created_at TEXT   NOT NULL,
              FOREIGN KEY (group_id) REFERENCES targeting_sni_groups(id) ON DELETE CASCADE
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_targeting_sni_group_items_group_pattern
            ON targeting_sni_group_items(group_id, pattern)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_targeting_sni_groups_key
            ON targeting_sni_groups(group_key)
            """
        )

        con.commit()
        print("MIGRATION DONE ✅")
        print("lead_campaigns: added tier, match_flags, score (om de saknades)")
        print("created: targeting_sni_groups, targeting_sni_group_items")

    finally:
        con.close()


if __name__ == "__main__":
    main()
