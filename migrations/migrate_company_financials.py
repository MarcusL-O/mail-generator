# migrate_company_financials.py
# ==========================================
# Skapar tabellen company_financials + index.
# Den här tabellen lagrar ekonominyckeltal per företag och räkenskapsår.
#
# Nyckelidé:
# - En rad per (orgnr, fiscal_year_end_date)
# - Alla belopp normaliseras till SEK (inte tkr)
# - solidity_pct lagras som 0–100 (t.ex. 42.0)
#
# Varje kolumn betyder:
# - orgnr: 10 siffror (utan bindestreck), matchar companies.orgnr
# - fiscal_year_end_date: räkenskapsårets sista dag (YYYY-MM-DD)
# - fiscal_year_end_year: härledd från fiscal_year_end_date (t.ex. 2023)
# - revenue_sek: Nettoomsättning (SEK)
# - profit_sek: Årets resultat (SEK)
# - result_after_fin_sek: Resultat efter finansiella poster (SEK)
# - assets_total_sek: Summa tillgångar / balansomslutning (SEK)
# - equity_total_sek: Summa eget kapital (SEK)
# - solidity_pct: Soliditet i procent (0–100)
# - cash_sek: Kassa och bank (SEK) (om finns i årsredovisningen)
# - liabilities_short_sek: Kortfristiga skulder (SEK) (om finns)
# - liabilities_long_sek: Långfristiga skulder (SEK) (om finns)
# - source_file: vilken fil vi läste (för spårbarhet)
# - updated_at: ISO-timestamp när raden skapades/uppdaterades
# ==========================================

import os
import sqlite3
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "data/companies.db.sqlite")

def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    cur = con.cursor()

    # Tabell för ekonomi per räkenskapsår
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_financials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            orgnr TEXT NOT NULL,                     -- 10 siffror
            fiscal_year_end_date TEXT NOT NULL,      -- YYYY-MM-DD
            fiscal_year_end_year INTEGER NOT NULL,   -- t.ex. 2023

            revenue_sek INTEGER,                     -- Nettoomsättning (SEK)
            profit_sek INTEGER,                      -- Årets resultat (SEK)
            result_after_fin_sek INTEGER,            -- Resultat efter finansiella poster (SEK)

            assets_total_sek INTEGER,                -- Summa tillgångar / balansomslutning (SEK)
            equity_total_sek INTEGER,                -- Summa eget kapital (SEK)

            solidity_pct REAL,                       -- 0–100 (procent)

            cash_sek INTEGER,                        -- Kassa och bank (SEK)
            liabilities_short_sek INTEGER,           -- Kortfristiga skulder (SEK)
            liabilities_long_sek INTEGER,            -- Långfristiga skulder (SEK)

            source_file TEXT,                        -- spårbarhet (vilken zip/xhtml)
            updated_at TEXT NOT NULL                 -- ISO-timestamp
        );
        """
    )

    # Unik nyckel: ett företag kan bara ha en post per räkenskapsårsslut
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_company_financials_orgnr_enddate
        ON company_financials(orgnr, fiscal_year_end_date);
        """
    )

    # Index för vanliga queries
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_company_financials_orgnr
        ON company_financials(orgnr);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_company_financials_year
        ON company_financials(fiscal_year_end_year);
        """
    )

    con.commit()
    con.close()
    print(f"✅ Migration klar: {DB_PATH}")

if __name__ == "__main__":
    main()
