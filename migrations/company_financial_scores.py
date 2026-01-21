# migrate_company_financial_scores.py
# ==========================================
# Skapar tabellen company_financial_scores.
#
# Syfte:
# - Lagra ekonomiskt betyg per företag och räkenskapsår (årsraden i company_financials).
# - Ett betyg = 3 värden:
#   - score_current (0–100): senaste årets “hälsa” baserat på nyckeltal
#   - score_growth  (0–100): tillväxtsignal mellan två år (kräver föregående år)
#   - score_total   (0–100): 70% current + 30% growth (om growth saknas -> total=current)
#
# Nyckel (unik):
# - (orgnr, fiscal_year_end_date)
#
# OBS:
# - orgnr måste finnas i companies-tabellen (enrich-only).
# ==========================================

import os
import sqlite3

DB_PATH = os.getenv("DB_PATH", "data/companies.db.sqlite")

def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_financial_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            orgnr TEXT NOT NULL,                     -- 10 siffror, matchar companies.orgnr
            fiscal_year_end_date TEXT NOT NULL,      -- YYYY-MM-DD (matchar company_financials)
            fiscal_year_end_year INTEGER NOT NULL,   -- härledd från end_date (t.ex. 2026)

            score_current REAL NOT NULL,             -- 0–100
            score_growth REAL,                       -- 0–100 (NULL om saknar föregående år)
            score_total REAL NOT NULL,               -- 0–100

            model_version TEXT NOT NULL,             -- så du kan uppgradera modellen senare
            updated_at TEXT NOT NULL                 -- ISO-timestamp
        );
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_company_financial_scores_key
        ON company_financial_scores(orgnr, fiscal_year_end_date);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_company_financial_scores_orgnr
        ON company_financial_scores(orgnr);
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_company_financial_scores_year
        ON company_financial_scores(fiscal_year_end_year);
        """
    )

    con.commit()
    con.close()
    print(f"✅ Migration klar: {DB_PATH}")

if __name__ == "__main__":
    main()
