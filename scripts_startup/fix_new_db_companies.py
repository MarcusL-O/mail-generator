# migrate_companies_db_clean.py
# Kommentar (VIKTIGT):
# - Du sa att du kommer byta namn på din nuvarande companies-db temporärt (backup).
# - Den nya DB:n ska till slut heta EXAKT: data/db/companies.db.sqlite
#   för att inte förstöra andra scripts.
#
# Kommentar (VIKTIGT):
# - Dubbelkolla att alla migrations/kolumner du “borde ha” faktiskt finns i gamla DB:n
#   innan du kör detta. Annars kan data/kolumner saknas och du tappar info i flytten.
#
# Kommentar:
# - Du har beslutat att city ska bli KOMMUN. Därför heter kolumnen "kommun" i nya schema.
#   Alla scripts som idag läser/skriv "city" måste uppdateras till "kommun"
#   (inkl: companies\bulkfil\01_bulk_city_import_db.py).
#
# Kommentar:
# - Tech-signaler (microsoft_status/strength/confidence, it_support_signal/confidence)
#   ligger kvar i companies som fakta-värden. "när kollades" hamnar i company_checks.
#
# Kommentar:
# - SCB “fakta” flyttas in i companies utan scb_-prefix:
#   registration_date, legal_form, company_status, sector, private_public,
#   employees_class, workplaces_count, postort, region, kommun.
#   scb_municipality vinner alltid för kommun.
#
# Kommentar:
# - company_changes skapas och fylls under migration för relevanta fält när old != new
#   (source='migration'). Framåt kan dina enrich-scripts också skriva hit när de uppdaterar
#   värden (inte bara migrationen).

from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_IN = Path("data/db/companies.db.sqlite")
DEFAULT_OUT = Path("data/db/companies.db.sqlite.NEW")  # Kommentar: genereras, du byter in den när du är nöjd


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def file_exists(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"DB saknas: {p}")


def dict_row(cur: sqlite3.Cursor, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
    row = cur.execute(sql, tuple(params)).fetchone()
    return row


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    r = dict_row(cur, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return r is not None


def get_columns(cur: sqlite3.Cursor, table: str) -> List[str]:
    cols: List[str] = []
    for r in cur.execute(f"PRAGMA table_info({table})").fetchall():
        cols.append(r[1])
    return cols


def safe_get(row: sqlite3.Row, col: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[col]
    except Exception:
        return default


def nonempty_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return int(s)
    return None


def normalize_null(v: Any) -> Any:
    # Kommentar: Du vill behålla exakta markörer som "unknown", "no_sni_" etc.
    # Vi konverterar INTE "unknown" till NULL här.
    # Vi gör bara tom-sträng -> NULL för att slippa skräp.
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s == "" else s
    return v


@dataclass
class CompanyNew:
    orgnr: str
    name: str
    kommun: Optional[str]
    region: Optional[str]
    postort: Optional[str]

    employees: Optional[int]
    employees_class: Optional[str]
    workplaces_count: Optional[int]

    sni_codes: Optional[str]
    sni_text: Optional[str]
    sni_groups: Optional[str]

    website: Optional[str]
    emails: Optional[str]

    # "site review" (värden)
    site_score: Optional[int]
    site_flags: Optional[str]

    # hiring (värden)
    hiring_status: Optional[str]
    hiring_what_text: Optional[str]
    hiring_count: Optional[int]
    hiring_category: Optional[str]
    hiring_external_urls: Optional[str]

    # tech-signaler (värden)
    microsoft_status: Optional[str]
    microsoft_strength: Optional[str]
    microsoft_confidence: Optional[str]
    it_support_signal: Optional[str]
    it_support_confidence: Optional[str]

    # SCB "fakta" utan prefix
    registration_date: Optional[str]
    legal_form: Optional[str]
    company_status: Optional[str]
    sector: Optional[str]
    private_public: Optional[str]

    # line_of_work (värden)
    line_of_work: Optional[str]
    line_of_work_raw: Optional[str]
    line_of_work_conf: Optional[float]
    line_of_work_bucket: Optional[str]
    line_of_work_source: Optional[str]
    line_of_work_updated_at: Optional[str]

    # basic timestamps (housekeeping)
    last_seen_at: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]


def create_schema(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA foreign_keys=ON;")

    # Kommentar: taxonomi-tabeller (behåll)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY,
            key TEXT NOT NULL,
            label TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS category_keywords (
            id INTEGER PRIMARY KEY,
            category_id INTEGER NOT NULL,
            keyword TEXT NOT NULL,
            weight INTEGER NOT NULL DEFAULT 1,
            match_type TEXT NOT NULL DEFAULT 'contains',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_category_keywords_category_id ON category_keywords(category_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_categories_key ON categories(key);")

    # Kommentar: sanningstabell (fakta)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS companies (
            orgnr TEXT PRIMARY KEY,
            name TEXT NOT NULL,

            kommun TEXT,
            region TEXT,
            postort TEXT,

            employees INTEGER,
            employees_class TEXT,
            workplaces_count INTEGER,

            sni_codes TEXT,
            sni_text TEXT,
            sni_groups TEXT,

            website TEXT,
            emails TEXT,

            site_score INTEGER,
            site_flags TEXT,

            hiring_status TEXT,
            hiring_what_text TEXT,
            hiring_count INTEGER,
            hiring_category TEXT,
            hiring_external_urls TEXT,

            microsoft_status TEXT,
            microsoft_strength TEXT,
            microsoft_confidence TEXT,
            it_support_signal TEXT,
            it_support_confidence TEXT,

            registration_date TEXT,
            legal_form TEXT,
            company_status TEXT,
            sector TEXT,
            private_public TEXT,

            line_of_work TEXT,
            line_of_work_raw TEXT,
            line_of_work_conf REAL,
            line_of_work_bucket TEXT,
            line_of_work_source TEXT,
            line_of_work_updated_at TEXT,

            last_seen_at TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_companies_kommun ON companies(kommun);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_companies_region ON companies(region);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_companies_sni_codes ON companies(sni_codes);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_companies_website ON companies(website);")

    # Kommentar: checks/tabell (status + checked_at osv)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            orgnr TEXT NOT NULL,
            check_key TEXT NOT NULL,          -- ex: 'website', 'email', 'site_review', 'hiring', 'tech', 'scb', 'scb_discover'
            status TEXT,                      -- behåll exakta statusvärden (unknown/no_sni_/ok/etc)
            checked_at TEXT,
            next_check_at TEXT,
            err_reason TEXT,
            meta TEXT,                        -- valfritt JSON/text
            created_at TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_company_checks_orgnr ON company_checks(orgnr);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_company_checks_key ON company_checks(check_key);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_company_checks_next ON company_checks(next_check_at);")

    # Kommentar: historik vid ändring (för triggers/insyn)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            orgnr TEXT NOT NULL,
            field TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            changed_at TEXT NOT NULL,
            source TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_company_changes_orgnr ON company_changes(orgnr);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_company_changes_field ON company_changes(field);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_company_changes_changed_at ON company_changes(changed_at);")

    # Kommentar: behåll state-tabellen för SCB discover (så dina discover-scripts kan fortsätta resume:a)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scb_discover_state (
            id INTEGER PRIMARY KEY,
            last_registration_date TEXT,
            last_page INTEGER,
            updated_at TEXT
        );
        """
    )

    con.commit()


def insert_change(cur: sqlite3.Cursor, orgnr: str, field: str, old_v: Any, new_v: Any, at: str) -> None:
    if old_v is None and new_v is None:
        return
    if str(old_v) == str(new_v):
        return
    cur.execute(
        """
        INSERT INTO company_changes (orgnr, field, old_value, new_value, changed_at, source)
        VALUES (?, ?, ?, ?, ?, 'migration')
        """,
        (
            orgnr,
            field,
            None if old_v is None else str(old_v),
            None if new_v is None else str(new_v),
            at,
        ),
    )


def add_check(
    cur: sqlite3.Cursor,
    orgnr: str,
    check_key: str,
    status: Any = None,
    checked_at: Any = None,
    next_check_at: Any = None,
    err_reason: Any = None,
    meta: Any = None,
) -> None:
    cur.execute(
        """
        INSERT INTO company_checks (orgnr, check_key, status, checked_at, next_check_at, err_reason, meta, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            orgnr,
            check_key,
            normalize_null(status),
            normalize_null(checked_at),
            normalize_null(next_check_at),
            normalize_null(err_reason),
            normalize_null(meta),
            utc_now_iso(),
        ),
    )


def map_company_row(old: sqlite3.Row, cols: set[str]) -> CompanyNew:
    orgnr = nonempty_str(safe_get(old, "orgnr", ""))
    name = nonempty_str(safe_get(old, "name", ""))

    # Kommentar: kommun ska komma från SCB och vinna alltid
    scb_municipality = nonempty_str(safe_get(old, "scb_municipality", "")) if "scb_municipality" in cols else ""
    old_city = nonempty_str(safe_get(old, "city", "")) if "city" in cols else ""
    kommun = scb_municipality or old_city or None

    region = nonempty_str(safe_get(old, "scb_region", "")) if "scb_region" in cols else ""
    postort = nonempty_str(safe_get(old, "scb_postort", "")) if "scb_postort" in cols else ""

    employees = as_int(safe_get(old, "employees")) if "employees" in cols else None
    employees_class = nonempty_str(safe_get(old, "scb_employees_class", "")) if "scb_employees_class" in cols else ""
    workplaces_count = as_int(safe_get(old, "scb_workplaces_count")) if "scb_workplaces_count" in cols else None

    sni_codes = nonempty_str(safe_get(old, "sni_codes", "")) if "sni_codes" in cols else ""
    sni_text = nonempty_str(safe_get(old, "sni_text", "")) if "sni_text" in cols else ""
    sni_groups = nonempty_str(safe_get(old, "sni_groups", "")) if "sni_groups" in cols else ""

    website = nonempty_str(safe_get(old, "website", "")) if "website" in cols else ""
    emails = nonempty_str(safe_get(old, "emails", "")) if "emails" in cols else ""

    site_score = as_int(safe_get(old, "site_score")) if "site_score" in cols else None
    site_flags = nonempty_str(safe_get(old, "site_flags", "")) if "site_flags" in cols else ""

    hiring_status = nonempty_str(safe_get(old, "hiring_status", "")) if "hiring_status" in cols else ""
    hiring_what_text = nonempty_str(safe_get(old, "hiring_what_text", "")) if "hiring_what_text" in cols else ""
    hiring_count = as_int(safe_get(old, "hiring_count")) if "hiring_count" in cols else None
    hiring_category = nonempty_str(safe_get(old, "hiring_category", "")) if "hiring_category" in cols else ""
    hiring_external_urls = nonempty_str(safe_get(old, "hiring_external_urls", "")) if "hiring_external_urls" in cols else ""

    microsoft_status = nonempty_str(safe_get(old, "microsoft_status", "")) if "microsoft_status" in cols else ""
    microsoft_strength = nonempty_str(safe_get(old, "microsoft_strength", "")) if "microsoft_strength" in cols else ""
    microsoft_confidence = nonempty_str(safe_get(old, "microsoft_confidence", "")) if "microsoft_confidence" in cols else ""
    it_support_signal = nonempty_str(safe_get(old, "it_support_signal", "")) if "it_support_signal" in cols else ""
    it_support_confidence = nonempty_str(safe_get(old, "it_support_confidence", "")) if "it_support_confidence" in cols else ""

    registration_date = nonempty_str(safe_get(old, "scb_registration_date", "")) if "scb_registration_date" in cols else ""
    legal_form = nonempty_str(safe_get(old, "scb_legal_form", "")) if "scb_legal_form" in cols else ""
    company_status = nonempty_str(safe_get(old, "scb_company_status", "")) if "scb_company_status" in cols else ""
    sector = nonempty_str(safe_get(old, "scb_sector", "")) if "scb_sector" in cols else ""
    private_public = nonempty_str(safe_get(old, "scb_private_public", "")) if "scb_private_public" in cols else ""

    line_of_work = nonempty_str(safe_get(old, "line_of_work", "")) if "line_of_work" in cols else ""
    line_of_work_raw = nonempty_str(safe_get(old, "line_of_work_raw", "")) if "line_of_work_raw" in cols else ""
    line_of_work_conf = safe_get(old, "line_of_work_conf") if "line_of_work_conf" in cols else None
    try:
        line_of_work_conf = float(line_of_work_conf) if line_of_work_conf is not None else None
    except Exception:
        line_of_work_conf = None
    line_of_work_bucket = nonempty_str(safe_get(old, "line_of_work_bucket", "")) if "line_of_work_bucket" in cols else ""
    line_of_work_source = nonempty_str(safe_get(old, "line_of_work_source", "")) if "line_of_work_source" in cols else ""
    line_of_work_updated_at = nonempty_str(safe_get(old, "line_of_work_updated_at", "")) if "line_of_work_updated_at" in cols else ""

    last_seen_at = nonempty_str(safe_get(old, "last_seen_at", "")) if "last_seen_at" in cols else ""
    created_at = nonempty_str(safe_get(old, "created_at", "")) if "created_at" in cols else ""
    updated_at = nonempty_str(safe_get(old, "updated_at", "")) if "updated_at" in cols else ""

    return CompanyNew(
        orgnr=orgnr,
        name=name,
        kommun=normalize_null(kommun),
        region=normalize_null(region),
        postort=normalize_null(postort),
        employees=employees,
        employees_class=normalize_null(employees_class),
        workplaces_count=workplaces_count,
        sni_codes=normalize_null(sni_codes),
        sni_text=normalize_null(sni_text),
        sni_groups=normalize_null(sni_groups),
        website=normalize_null(website),
        emails=normalize_null(emails),
        site_score=site_score,
        site_flags=normalize_null(site_flags),
        hiring_status=normalize_null(hiring_status),
        hiring_what_text=normalize_null(hiring_what_text),
        hiring_count=hiring_count,
        hiring_category=normalize_null(hiring_category),
        hiring_external_urls=normalize_null(hiring_external_urls),
        microsoft_status=normalize_null(microsoft_status),
        microsoft_strength=normalize_null(microsoft_strength),
        microsoft_confidence=normalize_null(microsoft_confidence),
        it_support_signal=normalize_null(it_support_signal),
        it_support_confidence=normalize_null(it_support_confidence),
        registration_date=normalize_null(registration_date),
        legal_form=normalize_null(legal_form),
        company_status=normalize_null(company_status),
        sector=normalize_null(sector),
        private_public=normalize_null(private_public),
        line_of_work=normalize_null(line_of_work),
        line_of_work_raw=normalize_null(line_of_work_raw),
        line_of_work_conf=line_of_work_conf,
        line_of_work_bucket=normalize_null(line_of_work_bucket),
        line_of_work_source=normalize_null(line_of_work_source),
        line_of_work_updated_at=normalize_null(line_of_work_updated_at),
        last_seen_at=normalize_null(last_seen_at),
        created_at=normalize_null(created_at),
        updated_at=normalize_null(updated_at),
    )


def migrate_taxonomy(old_con: sqlite3.Connection, new_con: sqlite3.Connection) -> None:
    old_cur = old_con.cursor()
    new_cur = new_con.cursor()

    if table_exists(old_cur, "categories"):
        rows = old_cur.execute("SELECT * FROM categories").fetchall()
        for r in rows:
            new_cur.execute(
                """
                INSERT INTO categories (id, key, label, is_active, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (r["id"], r["key"], r["label"], r["is_active"], r["created_at"]),
            )

    if table_exists(old_cur, "category_keywords"):
        rows = old_cur.execute("SELECT * FROM category_keywords").fetchall()
        for r in rows:
            new_cur.execute(
                """
                INSERT INTO category_keywords (id, category_id, keyword, weight, match_type, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (r["id"], r["category_id"], r["keyword"], r["weight"], r["match_type"], r["is_active"], r["created_at"]),
            )

    new_con.commit()


def migrate_scb_state(old_con: sqlite3.Connection, new_con: sqlite3.Connection) -> None:
    old_cur = old_con.cursor()
    new_cur = new_con.cursor()

    if not table_exists(old_cur, "scb_discover_state"):
        return

    rows = old_cur.execute("SELECT * FROM scb_discover_state").fetchall()
    for r in rows:
        new_cur.execute(
            """
            INSERT INTO scb_discover_state (id, last_registration_date, last_page, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (r["id"], r["last_registration_date"], r["last_page"], r["updated_at"]),
        )

    new_con.commit()


def migrate_companies_and_checks(old_con: sqlite3.Connection, new_con: sqlite3.Connection, print_every: int = 5000) -> None:
    old_cur = old_con.cursor()
    new_cur = new_con.cursor()

    if not table_exists(old_cur, "companies"):
        raise SystemExit("Gamla DB saknar tabell: companies")

    cols = set(get_columns(old_cur, "companies"))

    total = old_cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    print(f"Companies to migrate: {total:,}")

    q = old_cur.execute("SELECT * FROM companies")
    i = 0
    changes_at = utc_now_iso()

    for old_row in q:
        i += 1
        c = map_company_row(old_row, cols)

        # Insert companies (clean)
        new_cur.execute(
            """
            INSERT INTO companies (
                orgnr, name,
                kommun, region, postort,
                employees, employees_class, workplaces_count,
                sni_codes, sni_text, sni_groups,
                website, emails,
                site_score, site_flags,
                hiring_status, hiring_what_text, hiring_count, hiring_category, hiring_external_urls,
                microsoft_status, microsoft_strength, microsoft_confidence, it_support_signal, it_support_confidence,
                registration_date, legal_form, company_status, sector, private_public,
                line_of_work, line_of_work_raw, line_of_work_conf, line_of_work_bucket, line_of_work_source, line_of_work_updated_at,
                last_seen_at, created_at, updated_at
            ) VALUES (
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?
            )
            """,
            (
                c.orgnr, c.name,
                c.kommun, c.region, c.postort,
                c.employees, c.employees_class, c.workplaces_count,
                c.sni_codes, c.sni_text, c.sni_groups,
                c.website, c.emails,
                c.site_score, c.site_flags,
                c.hiring_status, c.hiring_what_text, c.hiring_count, c.hiring_category, c.hiring_external_urls,
                c.microsoft_status, c.microsoft_strength, c.microsoft_confidence, c.it_support_signal, c.it_support_confidence,
                c.registration_date, c.legal_form, c.company_status, c.sector, c.private_public,
                c.line_of_work, c.line_of_work_raw, c.line_of_work_conf, c.line_of_work_bucket, c.line_of_work_source, c.line_of_work_updated_at,
                c.last_seen_at, c.created_at, c.updated_at
            ),
        )

        # ---------- company_changes (logga diffar old->new där vi aktivt "byter källa/namn") ----------
        # Kommentar: Kommun: scb_municipality vinner alltid (kan skilja från gamla city)
        old_city = safe_get(old_row, "city", None) if "city" in cols else None
        old_scb_mun = safe_get(old_row, "scb_municipality", None) if "scb_municipality" in cols else None
        # old “kommun-kandidat” = scb_municipality om finns annars city
        old_kommun_candidate = (nonempty_str(old_scb_mun) or nonempty_str(old_city) or None)
        insert_change(new_cur, c.orgnr, "kommun", old_kommun_candidate, c.kommun, changes_at)

        # Kommentar: De här är “scb_*” i gamla men blir clean fält i nya
        insert_change(new_cur, c.orgnr, "region", safe_get(old_row, "scb_region", None), c.region, changes_at)
        insert_change(new_cur, c.orgnr, "postort", safe_get(old_row, "scb_postort", None), c.postort, changes_at)
        insert_change(new_cur, c.orgnr, "registration_date", safe_get(old_row, "scb_registration_date", None), c.registration_date, changes_at)
        insert_change(new_cur, c.orgnr, "legal_form", safe_get(old_row, "scb_legal_form", None), c.legal_form, changes_at)
        insert_change(new_cur, c.orgnr, "company_status", safe_get(old_row, "scb_company_status", None), c.company_status, changes_at)
        insert_change(new_cur, c.orgnr, "sector", safe_get(old_row, "scb_sector", None), c.sector, changes_at)
        insert_change(new_cur, c.orgnr, "private_public", safe_get(old_row, "scb_private_public", None), c.private_public, changes_at)
        insert_change(new_cur, c.orgnr, "employees_class", safe_get(old_row, "scb_employees_class", None), c.employees_class, changes_at)
        insert_change(new_cur, c.orgnr, "workplaces_count", safe_get(old_row, "scb_workplaces_count", None), c.workplaces_count, changes_at)

        # (Valfritt) logga website/emails om du vill:
        # insert_change(new_cur, c.orgnr, "website", safe_get(old_row, "website", None), c.website, changes_at)
        # insert_change(new_cur, c.orgnr, "emails", safe_get(old_row, "emails", None), c.emails, changes_at)

        # ---------- company_checks ----------
        # website
        if "website_status" in cols or "website_checked_at" in cols:
            add_check(
                new_cur,
                c.orgnr,
                check_key="website",
                status=safe_get(old_row, "website_status", None),
                checked_at=safe_get(old_row, "website_checked_at", None),
                err_reason=None,
            )

        # email
        if "email_status" in cols or "emails_checked_at" in cols:
            add_check(
                new_cur,
                c.orgnr,
                check_key="email",
                status=safe_get(old_row, "email_status", None),
                checked_at=safe_get(old_row, "emails_checked_at", None),
                err_reason=None,
            )

        # site_review
        if "site_review_checked_at" in cols or "site_review_err_reason" in cols:
            add_check(
                new_cur,
                c.orgnr,
                check_key="site_review",
                status="checked" if normalize_null(safe_get(old_row, "site_review_checked_at", None)) else None,
                checked_at=safe_get(old_row, "site_review_checked_at", None),
                err_reason=safe_get(old_row, "site_review_err_reason", None),
            )

        # hiring
        if "hiring_status" in cols or "hiring_checked_at" in cols or "hiring_err_reason" in cols:
            add_check(
                new_cur,
                c.orgnr,
                check_key="hiring",
                status=safe_get(old_row, "hiring_status", None),
                checked_at=safe_get(old_row, "hiring_checked_at", None),
                err_reason=safe_get(old_row, "hiring_err_reason", None),
            )

        # tech (checked_at + err_reason flyttas hit)
        if "tech_checked_at" in cols or "tech_err_reason" in cols:
            add_check(
                new_cur,
                c.orgnr,
                check_key="tech",
                status="checked" if normalize_null(safe_get(old_row, "tech_checked_at", None)) else None,
                checked_at=safe_get(old_row, "tech_checked_at", None),
                err_reason=safe_get(old_row, "tech_err_reason", None),
            )

        # scb enrich (status + checked + next + err)
        if "scb_status" in cols or "scb_checked_at" in cols or "scb_next_check_at" in cols or "scb_err_reason" in cols:
            add_check(
                new_cur,
                c.orgnr,
                check_key="scb",
                status=safe_get(old_row, "scb_status", None),
                checked_at=safe_get(old_row, "scb_checked_at", None),
                next_check_at=safe_get(old_row, "scb_next_check_at", None),
                err_reason=safe_get(old_row, "scb_err_reason", None),
            )

        # scb discover
        if "scb_discovered_at" in cols:
            add_check(
                new_cur,
                c.orgnr,
                check_key="scb_discover",
                status="discovered" if normalize_null(safe_get(old_row, "scb_discovered_at", None)) else None,
                checked_at=safe_get(old_row, "scb_discovered_at", None),
                err_reason=None,
            )

        if i % print_every == 0:
            new_con.commit()
            print(f"[{i:,}/{total:,}] migrated...")

    new_con.commit()
    print("Companies + checks migrated ✅")


def print_overview(con: sqlite3.Connection) -> None:
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    def one(sql: str, params: Tuple[Any, ...] = ()) -> Any:
        r = cur.execute(sql, params).fetchone()
        return None if r is None else r[0]

    def pct(part: int, total: int) -> str:
        if total <= 0:
            return "0.0%"
        return f"{(part * 100.0 / total):.1f}%"

    total = int(one("SELECT COUNT(*) FROM companies") or 0)
    print("\nNEW DB OVERVIEW")
    print(f"- generated_at: {utc_now_iso()}")
    print(f"- total companies: {total:,}")

    for col in ["website", "emails", "sni_codes", "kommun", "region", "postort"]:
        n = int(one(f"SELECT COUNT(*) FROM companies WHERE TRIM(COALESCE({col},''))!=''") or 0)
        print(f"- {col:<10} {n:,} ({pct(n, total)})")

    checks = int(one("SELECT COUNT(*) FROM company_checks") or 0)
    changes = int(one("SELECT COUNT(*) FROM company_changes") or 0)
    print(f"- company_checks rows: {checks:,}")
    print(f"- company_changes rows: {changes:,}")

    # Top kommun
    rows = cur.execute(
        """
        SELECT kommun, COUNT(*) AS n
        FROM companies
        WHERE TRIM(COALESCE(kommun,''))!=''
        GROUP BY kommun
        ORDER BY n DESC
        LIMIT 10
        """
    ).fetchall()
    print("\nTOP KOMMUN")
    for i, r in enumerate(rows, start=1):
        print(f"{i:>2}. {r['kommun']:<30} {int(r['n']):,}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default=str(DEFAULT_IN), help="Input DB (old)")
    ap.add_argument("--out", dest="out_path", default=str(DEFAULT_OUT), help="Output DB (new)")
    ap.add_argument(
        "--replace",
        action="store_true",
        help="Om satt: backar upp input DB till .bak_TIMESTAMP och ersätter med output DB (riskfyllt).",
    )
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    file_exists(in_path)

    if out_path.exists():
        raise SystemExit(f"Output finns redan: {out_path} (ta bort/byt namn innan körning)")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Open old
    old_con = sqlite3.connect(str(in_path))
    old_con.row_factory = sqlite3.Row

    # Create new
    new_con = sqlite3.connect(str(out_path))
    new_con.row_factory = sqlite3.Row

    try:
        create_schema(new_con)

        migrate_taxonomy(old_con, new_con)
        migrate_scb_state(old_con, new_con)
        migrate_companies_and_checks(old_con, new_con, print_every=5000)

        print_overview(new_con)

    finally:
        old_con.close()
        new_con.close()

    print("\nDONE ✅")
    print(f"- Old DB: {in_path}")
    print(f"- New DB: {out_path}")

    print("\nNÄSTA STEG (MANUELLT) ✅")
    print("1) Byt namn på gamla DB (backup), t.ex. companies.db.sqlite.OLD")
    print("2) Byt namn på nya DB till: data/db/companies.db.sqlite")
    print("3) Uppdatera scripts som använder 'city' -> 'kommun' (inkl bulk city import).")

    if args.replace:
        # Kommentar: valfritt, men riskfyllt. Kör bara om du vet vad du gör.
        # Vi gör en backup och ersätter.
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bak = in_path.with_suffix(in_path.suffix + f".bak_{ts}")
        print(f"\nREPLACE MODE ⚠️")
        print(f"- Backup old -> {bak}")
        print(f"- Replace {in_path} with {out_path}")
        shutil.move(str(in_path), str(bak))
        shutil.move(str(out_path), str(in_path))
        print("Replace klart ✅")


if __name__ == "__main__":
    main()
