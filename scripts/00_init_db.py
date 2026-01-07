# scripts/00_init_db.py
import sqlite3
from pathlib import Path

DB_PATH = Path("data/mail_generator_db.sqlite")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")         # bättre tålighet/prestanda
        conn.execute("PRAGMA synchronous=NORMAL;")       # bra balans
        conn.execute("PRAGMA foreign_keys=ON;")

        conn.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            orgnr TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            city TEXT,
            employees INTEGER,
            sni_codes TEXT,                -- JSON/text, ex: '["62010","62020"]'
            sni_text TEXT,                 -- ev. branschbeskrivning
            website TEXT,
            emails TEXT,                   -- JSON/text, ex: '["info@x.se","it@x.se"]'
            website_status TEXT,           -- 'hit' | 'miss' | NULL
            email_status TEXT,             -- 'hit' | 'miss' | NULL
            website_checked_at TEXT,       -- ISO datetime string
            emails_checked_at TEXT,        -- ISO datetime string
            last_seen_at TEXT,             -- senaste sync från masterlista
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_last_seen
        ON companies(last_seen_at);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_website_checked
        ON companies(website_checked_at);
        """)

        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_emails_checked
        ON companies(emails_checked_at);
        """)

    print(f"OK: init DB at {DB_PATH}")

if __name__ == "__main__":
    main()
