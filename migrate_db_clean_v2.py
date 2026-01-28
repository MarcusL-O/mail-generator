# companies/control/create_clean_db.py
# Skapar en NY TOM DB med slutligt schema.
# Antar att DB inte finns. Gör inget med ev. befintlig fil.

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/companies.db.sqlite")


def main() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(DB_PATH.as_posix())
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    cur = con.cursor()

    # =========================
    # companies
    # =========================
    cur.execute("""
    CREATE TABLE companies (
        orgnr TEXT PRIMARY KEY,
        name TEXT,

        kommun TEXT,
        region TEXT,
        postort TEXT,

        employees_class TEXT,
        workplaces_count INTEGER,
        employees_trend TEXT,
        employees_trend_at TEXT,

        sni_codes TEXT,
        sni_text TEXT,

        website TEXT,
        emails TEXT,
        site_score INTEGER,

        hiring_status TEXT,
        hiring_what_text TEXT,
        hiring_count INTEGER,
        hiring_category TEXT,

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
        line_of_work_conf REAL,
        segment_groups TEXT,

        financial_score_total REAL,
        financial_latest_year_end TEXT,
        financial_net_revenue_latest INTEGER,
        financial_revenue_trend_pct REAL,
        financial_revenue_trend TEXT
    );
    """)

    # =========================
    # company_checks
    # =========================
    cur.execute("""
    CREATE TABLE company_checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        orgnr TEXT NOT NULL,
        check_key TEXT NOT NULL,
        status TEXT,
        checked_at TEXT,
        next_check_at TEXT,
        err_reason TEXT,
        FOREIGN KEY (orgnr) REFERENCES companies(orgnr)
    );
    """)

    # =========================
    # employee history
    # =========================
    cur.execute("""
    CREATE TABLE company_employee_class_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        orgnr TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        employees_class TEXT NOT NULL,
        status TEXT NOT NULL,
        source TEXT NOT NULL,
        FOREIGN KEY (orgnr) REFERENCES companies(orgnr)
    );
    """)

    # =========================
    # financials
    # =========================
    cur.execute("""
    CREATE TABLE company_financials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        orgnr TEXT NOT NULL,
        fiscal_year_end_date TEXT NOT NULL,
        fiscal_year_end_year INTEGER NOT NULL,

        revenue_sek INTEGER,
        profit_sek INTEGER,
        result_after_fin_sek INTEGER,
        assets_total_sek INTEGER,
        equity_total_sek INTEGER,
        solidity_pct REAL,

        cash_sek INTEGER,
        liabilities_short_sek INTEGER,
        liabilities_long_sek INTEGER,

        source_file TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (orgnr) REFERENCES companies(orgnr)
    );
    """)

    # =========================
    # financial scores (per år)
    # =========================
    cur.execute("""
    CREATE TABLE company_financial_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        orgnr TEXT NOT NULL,
        fiscal_year_end_date TEXT NOT NULL,
        fiscal_year_end_year INTEGER NOT NULL,

        score_current REAL NOT NULL,
        score_growth REAL,
        score_total REAL NOT NULL,

        model_version TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (orgnr) REFERENCES companies(orgnr)
    );
    """)

    con.commit()
    con.close()

    print("DONE ✅ DB skapad:", DB_PATH.resolve())


if __name__ == "__main__":
    main()
