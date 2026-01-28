# Räknar ekonomiska betyg (0–100) från company_financials och sparar i company_financial_scores.
#
# Design (robust & resume):
# - En rad score per (orgnr, fiscal_year_end_date)
# - Only enrich: orgnr måste finnas i companies
# - Skip om score är "fresh" (dvs financials.updated_at <= scores.updated_at)
# - Batch commit + commit på Ctrl+C => ingen data loss
# - Rerun safe: UPSERT på (orgnr, fiscal_year_end_date)
#
# Modell:
# - score_current byggs av percentil-baserade delmått (per år) + winsorize (1%).
# - score_growth (om föregående år finns): 50/50 revenue + profit log-change, percentiler per år.
# - score_total = 0.7*current + 0.3*growth (om growth saknas => total=current)
#
# Körning:
# - Om YEAR är satt (t.ex. YEAR=2026) -> processa bara det året.
# - Annars -> processa alla år som finns i company_financials.


import os
import math
import time
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

DB_PATH = os.getenv("DB_PATH", "data/db/companies.db.sqlite")
COMPANIES_TABLE = os.getenv("COMPANIES_TABLE", "companies")
COMPANIES_COL_ORGNR = os.getenv("COMPANIES_COL_ORGNR", "orgnr")

PRINT_EVERY = int(os.getenv("PRINT_EVERY", "2000"))
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "1000"))

YEAR_FILTER = os.getenv("YEAR", "").strip()
MODEL_VERSION = os.getenv("MODEL_VERSION", "econ_v1")

WINSOR_P = float(os.getenv("WINSOR_P", "0.01"))  # 1% (0.01)

WEIGHT_CURRENT = float(os.getenv("WEIGHT_CURRENT", "0.70"))
WEIGHT_GROWTH = float(os.getenv("WEIGHT_GROWTH", "0.30"))

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

def signed_log1p(x: float) -> float:
    # Hanterar 0 och negativa värden stabilt
    if x == 0:
        return 0.0
    return math.copysign(math.log1p(abs(x)), x)

def percentile(values_sorted: List[float], p: float) -> float:
    # p i [0,1]
    if not values_sorted:
        return 0.0
    n = len(values_sorted)
    if n == 1:
        return values_sorted[0]
    idx = (n - 1) * p
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return values_sorted[lo]
    w = idx - lo
    return values_sorted[lo] * (1 - w) + values_sorted[hi] * w

def rank_percentile(values_sorted: List[float], x: float) -> float:
    # Returnerar percentil-rank 0..100
    # Enkel binärsök via bisect
    import bisect
    n = len(values_sorted)
    if n == 0:
        return 0.0
    i = bisect.bisect_left(values_sorted, x)
    return (i / n) * 100.0

def winsorize(values: List[float], p: float) -> Tuple[List[float], float, float]:
    s = sorted(values)
    lo = percentile(s, p)
    hi = percentile(s, 1.0 - p)
    out = [clamp(v, lo, hi) for v in values]
    return out, lo, hi

def load_companies_set(cur: sqlite3.Cursor) -> set:
    rows = cur.execute(f"SELECT {COMPANIES_COL_ORGNR} FROM {COMPANIES_TABLE}").fetchall()
    return set(str(r[0]).strip() for r in rows if r and r[0])

def ensure_scores_table(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_financial_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            orgnr TEXT NOT NULL,
            fiscal_year_end_date TEXT NOT NULL,
            fiscal_year_end_year INTEGER NOT NULL,
            score_current REAL NOT NULL,
            score_growth REAL,
            score_total REAL NOT NULL,
            model_version TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_company_financial_scores_key
        ON company_financial_scores(orgnr, fiscal_year_end_date);
        """
    )

def upsert_score(cur: sqlite3.Cursor, row: dict) -> None:
    cur.execute(
        """
        INSERT INTO company_financial_scores(
            orgnr, fiscal_year_end_date, fiscal_year_end_year,
            score_current, score_growth, score_total,
            model_version, updated_at
        )
        VALUES(
            :orgnr, :fiscal_year_end_date, :fiscal_year_end_year,
            :score_current, :score_growth, :score_total,
            :model_version, :updated_at
        )
        ON CONFLICT(orgnr, fiscal_year_end_date) DO UPDATE SET
            fiscal_year_end_year=excluded.fiscal_year_end_year,
            score_current=excluded.score_current,
            score_growth=excluded.score_growth,
            score_total=excluded.score_total,
            model_version=excluded.model_version,
            updated_at=excluded.updated_at
        ;
        """,
        row,
    )

def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ensure_scores_table(cur)

    companies_set = load_companies_set(cur)

    years = []
    if YEAR_FILTER:
        years = [int(YEAR_FILTER)]
    else:
        years = [r[0] for r in cur.execute("SELECT DISTINCT fiscal_year_end_year FROM company_financials").fetchall()]
        years = sorted(int(y) for y in years if y is not None)

    if not years:
        print("Inga år hittades i company_financials.")
        con.close()
        return

    print(f"DB: {DB_PATH}")
    print(f"År: {years}")
    print(f"MODEL_VERSION: {MODEL_VERSION}")
    print("-" * 60)

    scanned = 0
    updated = 0
    skipped_fresh = 0
    skipped_missing_prev = 0
    start = time.time()

    try:
        for year in years:
            # Hämta alla rows för året (end_year == year)
            rows = cur.execute(
                """
                SELECT
                    orgnr, fiscal_year_end_date, fiscal_year_end_year,
                    revenue_sek, profit_sek, result_after_fin_sek,
                    assets_total_sek, equity_total_sek, solidity_pct,
                    cash_sek, liabilities_short_sek, liabilities_long_sek,
                    updated_at
                FROM company_financials
                WHERE fiscal_year_end_year = ?
                """,
                (year,),
            ).fetchall()

            # Filtrera till endast orgnr som finns i companies
            rows = [r for r in rows if r["orgnr"] in companies_set]
            if not rows:
                continue

            # Bygg map för growth (föregående år)
            prev_rows = cur.execute(
                """
                SELECT orgnr, revenue_sek, profit_sek
                FROM company_financials
                WHERE fiscal_year_end_year = ?
                """,
                (year - 1,),
            ).fetchall()
            prev_map = {r["orgnr"]: (r["revenue_sek"], r["profit_sek"]) for r in prev_rows if r["orgnr"] in companies_set}

            # Bygg “fresh-check”: om score finns och är nyare än financials.updated_at -> skippa
            score_rows = cur.execute(
                """
                SELECT orgnr, fiscal_year_end_date, updated_at
                FROM company_financial_scores
                WHERE fiscal_year_end_year = ?
                """,
                (year,),
            ).fetchall()
            score_updated_map = {(r["orgnr"], r["fiscal_year_end_date"]): (r["updated_at"] or "") for r in score_rows}

            # === Current delmått per rad (raw) ===
            # Profitability
            profit_margin = []
            profit_level = []
            # Solvency
            solidity = []
            debt_ratio = []  # (assets - equity) / assets (lägre är bättre)
            # Liquidity
            cash_ratio = []  # cash / liabilities_short (om möjligt)
            # Stability (skillnad mellan result_after_fin och profit)
            stability_gap = []  # abs(result_after_fin - profit) / max(1, abs(result_after_fin))

            # Hjälp-listor i samma ordning som rows, så vi kan mappa tillbaka
            idx_ok = []  # indexer som har data för respektive mått

            # Vi samlar per-mått värden för percentiler (bara där det går att räkna)
            for i, r in enumerate(rows):
                rev = r["revenue_sek"]
                prof = r["profit_sek"]
                raf = r["result_after_fin_sek"]
                assets = r["assets_total_sek"]
                equity = r["equity_total_sek"]
                sol = r["solidity_pct"]
                cash = r["cash_sek"]
                liab_s = r["liabilities_short_sek"]

                # profit_margin
                if rev is not None and rev != 0 and prof is not None:
                    profit_margin.append(prof / rev)
                else:
                    profit_margin.append(None)

                # profit_level (signed log)
                if prof is not None:
                    profit_level.append(signed_log1p(float(prof)))
                else:
                    profit_level.append(None)

                # solidity_pct
                if sol is not None:
                    solidity.append(float(sol))
                else:
                    solidity.append(None)

                # debt_ratio
                if assets is not None and assets != 0 and equity is not None:
                    dr = (assets - equity) / assets
                    debt_ratio.append(float(dr))
                else:
                    debt_ratio.append(None)

                # cash_ratio
                if cash is not None and liab_s is not None and liab_s > 0:
                    cash_ratio.append(float(cash) / float(liab_s))
                else:
                    cash_ratio.append(None)

                # stability_gap
                if raf is not None and prof is not None:
                    denom = max(1.0, abs(float(raf)))
                    stability_gap.append(abs(float(raf) - float(prof)) / denom)
                else:
                    stability_gap.append(None)

            # Funktion: gör percentil-score (0..100) för ett mått med winsorize
            def build_scores(values: List[Optional[float]], higher_is_better: bool) -> List[Optional[float]]:
                idx = [i for i, v in enumerate(values) if v is not None and math.isfinite(float(v))]
                if not idx:
                    return [None] * len(values)

                raw = [float(values[i]) for i in idx]
                wz, lo, hi = winsorize(raw, WINSOR_P)
                s = sorted(wz)

                out = [None] * len(values)
                for j, i in enumerate(idx):
                    v = wz[j]
                    p = rank_percentile(s, v)  # 0..100
                    out[i] = p if higher_is_better else (100.0 - p)
                return out

            pm_score = build_scores(profit_margin, higher_is_better=True)
            pl_score = build_scores(profit_level, higher_is_better=True)
            sol_score = build_scores(solidity, higher_is_better=True)
            dr_score = build_scores(debt_ratio, higher_is_better=False)       # lägre debt_ratio bättre
            cr_score = build_scores(cash_ratio, higher_is_better=True)
            sg_score = build_scores(stability_gap, higher_is_better=False)    # lägre gap bättre

            # Growth: log-change mellan år (50/50 rev + profit)
            growth_vals = [None] * len(rows)
            for i, r in enumerate(rows):
                org = r["orgnr"]
                if org not in prev_map:
                    continue
                prev_rev, prev_prof = prev_map[org]
                cur_rev = r["revenue_sek"]
                cur_prof = r["profit_sek"]
                if cur_rev is None or prev_rev is None or cur_prof is None or prev_prof is None:
                    continue

                # signed log-change för stabilitet
                g_rev = signed_log1p(float(cur_rev)) - signed_log1p(float(prev_rev))
                g_prof = signed_log1p(float(cur_prof)) - signed_log1p(float(prev_prof))
                growth_vals[i] = 0.5 * g_rev + 0.5 * g_prof

            growth_score = build_scores(growth_vals, higher_is_better=True)

            # Kombinera till score_current
            def avg_available(vals: List[Optional[float]]) -> Optional[float]:
                xs = [v for v in vals if v is not None]
                if not xs:
                    return None
                return sum(xs) / len(xs)

            # Pelare (viktas jämnt inom current)
            for r_i, r in enumerate(rows):
                scanned += 1

                orgnr = r["orgnr"]
                end_date = r["fiscal_year_end_date"]
                fin_updated_at = r["updated_at"] or ""

                # Fresh check
                prev_score_updated = score_updated_map.get((orgnr, end_date), "")
                if prev_score_updated and fin_updated_at and fin_updated_at <= prev_score_updated:
                    skipped_fresh += 1
                    continue

                # current components
                profitability = avg_available([pm_score[r_i], pl_score[r_i]])
                solvency = avg_available([sol_score[r_i], dr_score[r_i]])
                liquidity = avg_available([cr_score[r_i]])
                stability = avg_available([sg_score[r_i]])

                current = avg_available([profitability, solvency, liquidity, stability])
                if current is None:
                    # Om vi saknar allt (ska vara ovanligt) -> sätt 0
                    current = 0.0

                g = growth_score[r_i]
                total = current if g is None else (WEIGHT_CURRENT * current + WEIGHT_GROWTH * g)

                row = {
                    "orgnr": orgnr,
                    "fiscal_year_end_date": end_date,
                    "fiscal_year_end_year": year,
                    "score_current": float(clamp(current, 0.0, 100.0)),
                    "score_growth": None if g is None else float(clamp(g, 0.0, 100.0)),
                    "score_total": float(clamp(total, 0.0, 100.0)),
                    "model_version": MODEL_VERSION,
                    "updated_at": now_iso(),
                }

                upsert_score(cur, row)
                updated += 1

                if updated % COMMIT_EVERY == 0:
                    con.commit()

                if scanned % PRINT_EVERY == 0:
                    rate = scanned / max(1e-9, time.time() - start)
                    print(
                        f"[{year}] scanned={scanned} updated={updated} skipped_fresh={skipped_fresh} "
                        f"| {rate:.1f}/s"
                    )

    except KeyboardInterrupt:
        print("\n⛔ Avbruten av användare – committar data...")

    finally:
        con.commit()
        con.close()

    rate = scanned / max(1e-9, time.time() - start)
    print("DONE ✅")
    print(f"scanned={scanned} updated={updated} skipped_fresh={skipped_fresh} | {rate:.2f}/s")

if __name__ == "__main__":
    main()
