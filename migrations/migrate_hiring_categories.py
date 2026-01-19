# scripts_startup/migrate_hiring_categories.py
# Kommentar: kör detta EN gång på rätt DB: data/db/companies.db.sqlite

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/db/companies.db.sqlite")

# =========================
# EDITERA HÄR: KATEGORIER + KEYWORDS
# =========================
CATEGORIES = [
    # key, label, active
    ("it_devops", "IT – DevOps / Platform", 1),
    ("it_security", "IT – Security", 1),
    ("it_software", "IT – Software Dev", 1),
    ("it_support", "IT – Support / Helpdesk", 1),
    ("finance_accounting", "Finance / Accounting", 1),
    ("sales_marketing", "Sales / Marketing", 1),
    ("hr_recruiting", "HR / Recruiting", 1),
    ("legal_compliance", "Legal / Compliance", 1),
    ("operations_production", "Operations / Production", 1),
    ("other_non_it", "Other (Non-IT)", 1),
]

# category_key -> list of keywords (lowercase)
CATEGORY_KEYWORDS = {
    "it_devops": [
        "devops", "platform engineer", "site reliability", "sre",
        "kubernetes", "docker", "terraform", "ci/cd", "cicd",
        "azure devops", "gitlab", "jenkins",
    ],
    "it_security": [
        "it-säkerhet", "informationssäkerhet", "cyber security", "security",
        "soc", "siem", "iam", "iso27001", "nis2", "compliance", "risk",
        "defender", "sentinel", "azure ad", "entra", "intune",
    ],
    "it_software": [
        "utvecklare", "developer", "software engineer", "backend", "frontend", "fullstack",
        "java", "c#", ".net", "python", "node", "react", "angular", "sql",
    ],
    "it_support": [
        "it-support", "itsupport", "helpdesk", "servicedesk", "service desk",
        "support", "1st line", "2nd line", "drift", "it-drift",
    ],
    "finance_accounting": [
        "ekonomi", "redovisning", "accounting", "controller", "bokföring", "payroll", "lön",
    ],
    "sales_marketing": [
        "sälj", "sales", "account manager", "marketing", "marknad", "growth", "seo", "sem",
    ],
    "hr_recruiting": [
        "hr", "human resources", "rekryter", "recruit", "talent acquisition",
    ],
    "legal_compliance": [
        "jurist", "legal", "compliance", "dataskydd", "gdpr", "privacy", "dpo",
    ],
    "operations_production": [
        "produktion", "operatör", "operator", "warehouse", "lager", "logistik",
        "truck", "mekaniker", "tekniker",
    ],
    "other_non_it": [
        "målare", "målare", "electrician", "snickare", "bygg", "chaufför", "driver",
    ],
}
# =========================

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH.as_posix())
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode=WAL;")

    # 1) Lägg till kolumner i companies
    if not col_exists(conn, "companies", "hiring_category"):
        conn.execute("ALTER TABLE companies ADD COLUMN hiring_category TEXT;")
    if not col_exists(conn, "companies", "hiring_external_urls"):
        conn.execute("ALTER TABLE companies ADD COLUMN hiring_external_urls TEXT;")

    # 2) Skapa tabeller
    if not table_exists(conn, "categories"):
        conn.execute("""
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
            created_at TEXT NOT NULL
        );
        """)

    if not table_exists(conn, "category_keywords"):
        conn.execute("""
        CREATE TABLE category_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            keyword TEXT NOT NULL,
            weight INTEGER NOT NULL DEFAULT 1,
            match_type TEXT NOT NULL DEFAULT 'contains' CHECK (match_type IN ('contains','exact')),
            is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
            created_at TEXT NOT NULL,
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_category_keywords_category_id ON category_keywords(category_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_category_keywords_keyword ON category_keywords(keyword);")

    # 3) Seed categories
    now = utcnow_iso()
    for key, label, active in CATEGORIES:
        conn.execute(
            """
            INSERT INTO categories (key, label, is_active, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              label=excluded.label,
              is_active=excluded.is_active
            """,
            (key, label, int(active), now),
        )

    # 4) Seed keywords (idempotent-ish: vi lägger bara in nya, rör inte gamla)
    cat_id = {row[0]: row[1] for row in conn.execute("SELECT key, id FROM categories").fetchall()}
    for ckey, kws in CATEGORY_KEYWORDS.items():
        if ckey not in cat_id:
            continue
        cid = cat_id[ckey]
        for kw in kws:
            kw = (kw or "").strip().lower()
            if not kw:
                continue
            conn.execute(
                """
                INSERT INTO category_keywords (category_id, keyword, weight, match_type, is_active, created_at)
                SELECT ?, ?, 1, 'contains', 1, ?
                WHERE NOT EXISTS (
                  SELECT 1 FROM category_keywords WHERE category_id=? AND keyword=? AND match_type='contains'
                )
                """,
                (cid, kw, now, cid, kw),
            )

    conn.commit()
    conn.close()
    print("KLART ✅ migration + seed av categories/category_keywords")
    print(f"DB: {DB_PATH.resolve()}")

if __name__ == "__main__":
    main()
