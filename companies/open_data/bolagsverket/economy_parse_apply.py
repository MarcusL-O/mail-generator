# scripts_economy/economy_parse_apply.py
# Parsear nedladdade årsredovisnings-zippar för ett år -> NDJSON -> UPSERT till DB.
#
# Flöde:
# - Läser zippar i: data/bolagsverket/annual_reports/{year}/
# - (Container-zip + inner-zip + xhtml) hanteras rekursivt
# - Validerar orgnr + räkenskapsårslut mot dokumentets nonNumeric (om finns)
# - Skipp:
#   - orgnr som inte finns i companies (du har ~300k)
#   - (orgnr, end_date) som redan finns i company_financials
# - Skriver NDJSON till: data/economy/annual_{year}.ndjson
# - UPSERT till table: company_financials
#
# Kör:
#   python scripts_economy/economy_parse_apply.py --year 2024
#
# Miljö:
#   DB_PATH=... (default: data/companies.db.sqlite)

from __future__ import annotations

import argparse
import io
import json
import os
import re
import time
import zipfile
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple

from lxml import etree

DB_PATH = os.getenv("DB_PATH", "data/companies.db.sqlite")
COMPANIES_TABLE = os.getenv("COMPANIES_TABLE", "companies")
COMPANIES_COL_ORGNR = os.getenv("COMPANIES_COL_ORGNR", "orgnr")

PRINT_EVERY = int(os.getenv("PRINT_EVERY", "500"))
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "500"))

BASE_DIR_DEFAULT = "data/bolagsverket/annual_reports"
ECON_DIR_DEFAULT = "data/economy"

# Filnamn-format (inner-zip i container): 5560172933_2023-12-31.zip
RE_INNER_ZIP = re.compile(r"(?P<orgnr>\d{10})_(?P<end>\d{4}-\d{2}-\d{2})\.zip$", re.IGNORECASE)

# iXBRL-koncept vi vill hämta (döpta så de är tydliga i DB)
FACTS_MAP = {
    "revenue_sek": "se-gen-base:Nettoomsattning",
    "profit_sek": "se-gen-base:AretsResultat",
    "result_after_fin_sek": "se-gen-base:ResultatEfterFinansiellaPoster",
    "assets_total_sek": "se-gen-base:Tillgangar",
    "equity_total_sek": "se-gen-base:EgetKapital",
    "solidity_pct": "se-gen-base:Soliditet",
    "cash_sek": "se-gen-base:KassaBank",
    "liabilities_short_sek": "se-gen-base:KortfristigaSkulder",
    "liabilities_long_sek": "se-gen-base:LangfristigaSkulder",
}

# NonNumeric för validering
NN_ORGNR = "se-cd-base:Organisationsnummer"
NN_END = "se-cd-base:RakenskapsarSistaDag"

PARSER = etree.XMLParser(recover=True, huge_tree=True)


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def norm_orgnr(s: str) -> str:
    return re.sub(r"\D", "", (s or ""))


def parse_number_text(raw: str) -> Optional[float]:
    if raw is None:
        return None
    cleaned = raw.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", "", cleaned)
    if cleaned == "":
        return None
    cleaned = cleaned.replace(",", ".")
    if not re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def apply_scale(value: float, scale: Optional[str]) -> float:
    if not scale:
        return value
    try:
        p = int(scale)
    except ValueError:
        return value
    return value * (10 ** p)


def get_nsmap(root: etree._Element) -> Dict[str, str]:
    return {k: v for k, v in (root.nsmap or {}).items() if k}


def first_text(el: etree._Element) -> str:
    return "".join(el.itertext()).strip()


def extract_nonnumeric(root: etree._Element, ns: Dict[str, str], name: str) -> Optional[str]:
    els = root.xpath(f'.//ix:nonNumeric[@name="{name}"]', namespaces=ns)
    if not els:
        return None
    return first_text(els[0])


def extract_fact(root: etree._Element, ns: Dict[str, str], name: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    els = root.xpath(f'.//ix:nonFraction[@name="{name}"]', namespaces=ns)
    if not els:
        return None, None, None
    el = els[0]
    raw = first_text(el)
    v = parse_number_text(raw)
    unit = el.get("unitRef")
    scale = el.get("scale")
    if v is None:
        return None, unit, scale
    return apply_scale(v, scale), unit, scale


def soliditet_to_pct(value_scaled: float, unit: Optional[str]) -> float:
    # I praktiken brukar unitRef="procent" + scale=-2 => 0.915 => vill ha 91.5
    if (unit or "").lower() == "procent":
        return value_scaled * 100.0
    return value_scaled * 100.0


def iter_zip_inputs(year_dir: Path) -> Iterator[Path]:
    # Endast zippar direkt i året (du laddar ner 01_1.zip, 01_2.zip osv)
    for p in sorted(year_dir.glob("*.zip")):
        yield p


def iter_documents_from_zip(zip_path: Path) -> Iterator[Tuple[str, str, bytes, str]]:
    """
    Yields: (orgnr_from_filename, end_date_from_filename, xhtml_bytes, source_file_label)
    source_file_label används för spårbarhet i DB + NDJSON.
    """
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()

        # Typ B: zipen innehåller direkt .xhtml
        xhtmls = [n for n in names if n.lower().endswith(".xhtml")]
        if xhtmls:
            base = zip_path.name
            m = RE_INNER_ZIP.search(base)
            if not m:
                return
            orgnr = m.group("orgnr")
            end_date = m.group("end")
            xbytes = z.read(xhtmls[0])
            yield orgnr, end_date, xbytes, f"{zip_path}::{xhtmls[0]}"
            return

        # Typ A: container zip med många inner-zipar
        inner_zips = [n for n in names if n.lower().endswith(".zip")]
        for inner_name in inner_zips:
            base = Path(inner_name).name
            m = RE_INNER_ZIP.search(base)
            if not m:
                continue
            orgnr = m.group("orgnr")
            end_date = m.group("end")

            inner_bytes = z.read(inner_name)
            with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
                xfiles = [n for n in inner.namelist() if n.lower().endswith(".xhtml")]
                if not xfiles:
                    continue
                xbytes = inner.read(xfiles[0])
                yield orgnr, end_date, xbytes, f"{zip_path}::{inner_name}::{xfiles[0]}"


def ensure_table_exists(cur: sqlite3.Cursor) -> None:
    # Säkerhetsnät: om migration inte körts
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_financials (
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
            updated_at TEXT NOT NULL
        );
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_company_financials_orgnr_enddate
        ON company_financials(orgnr, fiscal_year_end_date);
        """
    )


def upsert_financial(cur: sqlite3.Cursor, row: dict) -> None:
    cur.execute(
        """
        INSERT INTO company_financials(
            orgnr, fiscal_year_end_date, fiscal_year_end_year,
            revenue_sek, profit_sek, result_after_fin_sek,
            assets_total_sek, equity_total_sek, solidity_pct,
            cash_sek, liabilities_short_sek, liabilities_long_sek,
            source_file, updated_at
        )
        VALUES(
            :orgnr, :fiscal_year_end_date, :fiscal_year_end_year,
            :revenue_sek, :profit_sek, :result_after_fin_sek,
            :assets_total_sek, :equity_total_sek, :solidity_pct,
            :cash_sek, :liabilities_short_sek, :liabilities_long_sek,
            :source_file, :updated_at
        )
        ON CONFLICT(orgnr, fiscal_year_end_date) DO UPDATE SET
            fiscal_year_end_year=excluded.fiscal_year_end_year,
            revenue_sek=excluded.revenue_sek,
            profit_sek=excluded.profit_sek,
            result_after_fin_sek=excluded.result_after_fin_sek,
            assets_total_sek=excluded.assets_total_sek,
            equity_total_sek=excluded.equity_total_sek,
            solidity_pct=excluded.solidity_pct,
            cash_sek=excluded.cash_sek,
            liabilities_short_sek=excluded.liabilities_short_sek,
            liabilities_long_sek=excluded.liabilities_long_sek,
            source_file=excluded.source_file,
            updated_at=excluded.updated_at
        ;
        """,
        row,
    )


def build_companies_set(cur: sqlite3.Cursor) -> set[str]:
    rows = cur.execute(f"SELECT {COMPANIES_COL_ORGNR} FROM {COMPANIES_TABLE}").fetchall()
    return set(str(r[0]).strip() for r in rows if r and r[0])


def build_existing_keys(cur: sqlite3.Cursor) -> set[tuple[str, str]]:
    rows = cur.execute("SELECT orgnr, fiscal_year_end_date FROM company_financials").fetchall()
    return set((str(o), str(d)) for o, d in rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--base-dir", type=str, default=BASE_DIR_DEFAULT)
    ap.add_argument("--econ-dir", type=str, default=ECON_DIR_DEFAULT)
    args = ap.parse_args()

    year = args.year
    year_dir = Path(args.base_dir) / str(year)
    econ_dir = Path(args.econ_dir)
    econ_dir.mkdir(parents=True, exist_ok=True)

    ndjson_path = econ_dir / f"annual_{year}.ndjson"
    missing_org_path = econ_dir / f"missing_not_in_companies_{year}.txt"
    log_path = econ_dir / f"parse_apply_{year}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"

    if not year_dir.exists():
        raise SystemExit(f"År-mapp saknas: {year_dir}")

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ensure_table_exists(cur)

    companies_set = build_companies_set(cur)
    existing_keys = build_existing_keys(cur)

    zip_inputs = list(iter_zip_inputs(year_dir))
    if not zip_inputs:
        raise SystemExit(f"Inga zip-filer hittades i: {year_dir}")

    # NDJSON: append-läge så du kan köra om (men vi skriver bara rader som vi faktiskt upsertar)
    ndjson_f = open(ndjson_path, "a", encoding="utf-8")
    missing_orgs: set[str] = set()

    scanned_docs = 0
    inserted_or_updated = 0
    skipped_not_in_companies = 0
    skipped_already_exists = 0
    skipped_validation_mismatch = 0
    skipped_parse_error = 0

    start = time.time()

    def log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")

    def to_int_sek(v: Optional[float]) -> Optional[int]:
        if v is None:
            return None
        return int(round(v))

    print(f"DB: {DB_PATH}")
    print(f"YEAR_DIR: {year_dir}")
    print(f"ZIP_INPUTS: {len(zip_inputs)}")
    print(f"NDJSON: {ndjson_path}")
    print(f"LOG: {log_path}")
    print("-" * 60)

    try:
        for zpath in zip_inputs:
            for orgnr_a, end_a, xbytes, source_label in iter_documents_from_zip(zpath):
                scanned_docs += 1

                # Årfilter baserat på end_date
                if not end_a.startswith(str(year)):
                    continue

                orgnr = norm_orgnr(orgnr_a)
                if len(orgnr) != 10:
                    skipped_parse_error += 1
                    log(f"parse_error bad_orgnr_from_filename orgnr='{orgnr_a}' source='{source_label}'")
                    continue

                # Bara bolag som finns i din companies-tabell
                if orgnr not in companies_set:
                    skipped_not_in_companies += 1
                    missing_orgs.add(orgnr)
                    continue

                key = (orgnr, end_a)
                if key in existing_keys:
                    skipped_already_exists += 1
                    continue

                # Parse XHTML
                try:
                    root = etree.fromstring(xbytes, parser=PARSER)
                except Exception:
                    skipped_parse_error += 1
                    log(f"parse_error xml_parse_failed orgnr={orgnr} end={end_a} source='{source_label}'")
                    continue

                ns = get_nsmap(root)
                if "ix" not in ns:
                    skipped_parse_error += 1
                    log(f"parse_error missing_ix_namespace orgnr={orgnr} end={end_a} source='{source_label}'")
                    continue

                # Validering från dokumentet (om finns)
                orgnr_b_raw = extract_nonnumeric(root, ns, NN_ORGNR)
                end_b_raw = extract_nonnumeric(root, ns, NN_END)

                if orgnr_b_raw:
                    orgnr_b = norm_orgnr(orgnr_b_raw)
                    if orgnr_b and orgnr_b != orgnr:
                        skipped_validation_mismatch += 1
                        log(
                            f"skipped validation_mismatch orgnr_file={orgnr} orgnr_doc={orgnr_b} "
                            f"end_file={end_a} end_doc={end_b_raw or ''} source='{source_label}'"
                        )
                        continue

                if end_b_raw:
                    end_b = (end_b_raw or "").strip()
                    if end_b and end_b != end_a:
                        skipped_validation_mismatch += 1
                        log(
                            f"skipped validation_mismatch_end orgnr={orgnr} end_file={end_a} end_doc={end_b} "
                            f"source='{source_label}'"
                        )
                        continue

                # Extrahera fakta
                out: Dict[str, Optional[float]] = {}
                units: Dict[str, Optional[str]] = {}

                for col, concept in FACTS_MAP.items():
                    v, unit, _scale = extract_fact(root, ns, concept)
                    out[col] = v
                    units[col] = unit

                fiscal_year_end_year = int(end_a[:4])

                row = {
                    "orgnr": orgnr,
                    "fiscal_year_end_date": end_a,
                    "fiscal_year_end_year": fiscal_year_end_year,
                    "revenue_sek": to_int_sek(out["revenue_sek"]),
                    "profit_sek": to_int_sek(out["profit_sek"]),
                    "result_after_fin_sek": to_int_sek(out["result_after_fin_sek"]),
                    "assets_total_sek": to_int_sek(out["assets_total_sek"]),
                    "equity_total_sek": to_int_sek(out["equity_total_sek"]),
                    "solidity_pct": None,
                    "cash_sek": to_int_sek(out["cash_sek"]),
                    "liabilities_short_sek": to_int_sek(out["liabilities_short_sek"]),
                    "liabilities_long_sek": to_int_sek(out["liabilities_long_sek"]),
                    "source_file": source_label,
                    "updated_at": now_iso(),
                }

                if out["solidity_pct"] is not None:
                    row["solidity_pct"] = float(soliditet_to_pct(out["solidity_pct"], units["solidity_pct"]))

                # NDJSON-rad (samma fält som DB)
                ndjson_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                # UPSERT
                try:
                    upsert_financial(cur, row)
                except Exception:
                    skipped_parse_error += 1
                    log(f"db_error upsert_failed orgnr={orgnr} end={end_a} source='{source_label}'")
                    continue

                inserted_or_updated += 1
                existing_keys.add(key)

                if inserted_or_updated % COMMIT_EVERY == 0:
                    con.commit()

                if scanned_docs % PRINT_EVERY == 0:
                    rate = scanned_docs / max(1e-9, time.time() - start)
                    print(
                        f"[docs={scanned_docs}] upserted={inserted_or_updated} "
                        f"skip_exists={skipped_already_exists} skip_not_in_companies={skipped_not_in_companies} "
                        f"skip_mismatch={skipped_validation_mismatch} err={skipped_parse_error} | {rate:.1f}/s"
                    )

    except KeyboardInterrupt:
        print("\n⛔ Avbruten av användare – committar data...")

    finally:
        con.commit()
        ndjson_f.close()
        con.close()

    # Spara missing-orgs så du kan se vad som inte gick att lägga in pga du saknar org i DB
    if missing_orgs:
        with open(missing_org_path, "w", encoding="utf-8") as f:
            for o in sorted(missing_orgs):
                f.write(o + "\n")

    rate = scanned_docs / max(1e-9, time.time() - start)
    print("DONE ✅")
    print(f"docs_scanned={scanned_docs} upserted={inserted_or_updated} | {rate:.2f}/s")
    print(f"skipped_already_exists={skipped_already_exists}")
    print(f"skipped_not_in_companies={skipped_not_in_companies}")
    print(f"skipped_validation_mismatch={skipped_validation_mismatch}")
    print(f"errors={skipped_parse_error}")
    print(f"ndjson={ndjson_path}")
    if missing_orgs:
        print(f"missing_orgs_file={missing_org_path}")
    print(f"log_file={log_path}")


if __name__ == "__main__":
    main()
