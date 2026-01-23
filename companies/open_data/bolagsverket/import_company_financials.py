# import_company_financials.py
# ==========================================
# Läser årsredovisningar (iXBRL/XHTML) från zip-filer och skriver ekonomi in i DB.
#
# Krav du bad om:
# - Snabb körning (ingen shards)
# - Ingen data loss vid Ctrl+C (batch-commit + commit i finally)
# - Ska inte köra om samma bolag/år i onödan (skip om redan finns i DB)
# - Får INTE skriva in ekonomi på fel företag:
#   -> Vi tar orgnr + räkenskapsårsslut från filnamn
#   -> Vi försöker även läsa orgnr + räkenskapsårsslut ur dokumentet och jämföra
#      (om mismatch: skippa + logga)
# - Bolag som inte finns i companies ska skippas
#
# Förväntad filstruktur:
# - Du lägger års-filer på VPS, t.ex:
#   data/bolagsverket/annual_reports/2024/*.zip
# - Scriptet hanterar två typer:
#   A) "container zip" som innehåller många inner-zipar (som din 01_1.zip)
#   B) "vanliga zips" där zipen direkt innehåller .xhtml
# ==========================================

import io
import os
import re
import sys
import time
import zipfile
import sqlite3
from datetime import datetime
from typing import Dict, Iterator, Optional, Tuple

from lxml import etree

DB_PATH = os.getenv("DB_PATH", "data/companies.db.sqlite")
COMPANIES_TABLE = os.getenv("COMPANIES_TABLE", "companies")
COMPANIES_COL_ORGNR = os.getenv("COMPANIES_COL_ORGNR", "orgnr")

BASE_DIR = os.getenv("BV_AR_BASE_DIR", "data/bolagsverket/annual_reports")
PRINT_EVERY = int(os.getenv("PRINT_EVERY", "500"))
COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "500"))

# Om True: vi laddar alla orgnr från companies i minne för snabb "exists"-check
LOAD_COMPANIES_SET = os.getenv("LOAD_COMPANIES_SET", "1").strip() not in ("0", "false", "no")

# Om True: vi laddar alla redan importerade (orgnr,end_date) för snabb skip
LOAD_EXISTING_KEYS = os.getenv("LOAD_EXISTING_KEYS", "1").strip() not in ("0", "false", "no")

# Filnamn-format (ditt exempel): 5560172933_2023-12-31.zip
RE_INNER_ZIP = re.compile(r"(?P<orgnr>\d{10})_(?P<end>\d{4}-\d{2}-\d{2})\.zip$", re.IGNORECASE)

# iXBRL-koncept vi vill hämta
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
    # Ta bort bindestreck och allt som inte är siffra
    return re.sub(r"\D", "", (s or ""))

def parse_number_text(raw: str) -> Optional[float]:
    if raw is None:
        return None
    # Ta bort whitespace/newlines, behåll siffror, minus, komma, punkt
    cleaned = raw.replace("\xa0", " ")
    cleaned = re.sub(r"\s+", "", cleaned)
    if cleaned == "":
        return None
    # Svensk decimal-komma -> punkt
    cleaned = cleaned.replace(",", ".")
    # Tillåt minus
    if not re.fullmatch(r"-?\d+(\.\d+)?", cleaned):
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None

def apply_scale(value: float, scale: Optional[str]) -> float:
    if scale is None or scale == "":
        return value
    try:
        p = int(scale)
    except ValueError:
        return value
    return value * (10 ** p)

def get_nsmap(root: etree._Element) -> Dict[str, str]:
    # lxml xpath gillar inte None-prefix, så vi tar bort den
    return {k: v for k, v in (root.nsmap or {}).items() if k}

def first_text(el: etree._Element) -> str:
    return "".join(el.itertext()).strip()

def extract_nonnumeric(root: etree._Element, ns: Dict[str, str], name: str) -> Optional[str]:
    els = root.xpath(f'.//ix:nonNumeric[@name="{name}"]', namespaces=ns)
    if not els:
        return None
    return first_text(els[0])

def extract_fact(root: etree._Element, ns: Dict[str, str], name: str) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """
    Returnerar: (värde, unitRef, scale)
    """
    els = root.xpath(f'.//ix:nonFraction[@name="{name}"]', namespaces=ns)
    if not els:
        return None, None, None

    el = els[0]
    raw = first_text(el)
    v = parse_number_text(raw)
    if v is None:
        return None, el.get("unitRef"), el.get("scale")

    unit = el.get("unitRef")
    scale = el.get("scale")
    v = apply_scale(v, scale)
    return v, unit, scale

def soliditet_to_pct(value_scaled: float, unit: Optional[str]) -> float:
    # I dina filer är unitRef="procent" och scale=-2, vilket ger 0.915 etc.
    # Du vill lagra 0–100, alltså multiplicera med 100.
    if (unit or "").lower() == "procent":
        return value_scaled * 100.0
    # Om unit inte är "procent" kör vi ändå som "procentvärde" (för säkerhets skull)
    return value_scaled

def iter_zip_inputs(base_dir: str) -> Iterator[str]:
    # Hitta alla .zip rekursivt
    for root, _, files in os.walk(base_dir):
        for fn in files:
            if fn.lower().endswith(".zip"):
                yield os.path.join(root, fn)

def iter_documents_from_zip(zip_path: str) -> Iterator[Tuple[str, str, bytes, str]]:
    """
    Yields: (orgnr_from_filename, end_date_from_filename, xhtml_bytes, source_file_label)
    source_file_label används för spårbarhet/logg.
    """
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()

        # Typ B: zipen innehåller direkt .xhtml
        xhtmls = [n for n in names if n.lower().endswith(".xhtml")]
        if xhtmls:
            # För den här typen måste vi försöka få orgnr/end_date från zip_path-namnet
            base = os.path.basename(zip_path)
            m = RE_INNER_ZIP.search(base)
            if not m:
                # Kan inte mappa säkert -> skippa
                return
            orgnr = m.group("orgnr")
            end_date = m.group("end")
            xbytes = z.read(xhtmls[0])
            yield orgnr, end_date, xbytes, f"{zip_path}::{xhtmls[0]}"
            return

        # Typ A: container zip med många inner-zipar
        inner_zips = [n for n in names if n.lower().endswith(".zip")]
        for inner_name in inner_zips:
            base = os.path.basename(inner_name)
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

def build_companies_set(cur: sqlite3.Cursor) -> Optional[set]:
    if not LOAD_COMPANIES_SET:
        return None
    rows = cur.execute(f"SELECT {COMPANIES_COL_ORGNR} FROM {COMPANIES_TABLE}").fetchall()
    s = set()
    for (orgnr,) in rows:
        if orgnr:
            s.add(str(orgnr).strip())
    return s

def build_existing_keys(cur: sqlite3.Cursor) -> Optional[set]:
    if not LOAD_EXISTING_KEYS:
        return None
    rows = cur.execute("SELECT orgnr, fiscal_year_end_date FROM company_financials").fetchall()
    return set((str(o), str(d)) for o, d in rows)

def ensure_table_exists(cur: sqlite3.Cursor) -> None:
    # Säkerhetsnät: ifall du glömde köra migrationen
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
    # SQLite UPSERT via ON CONFLICT
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

def main() -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ensure_table_exists(cur)

    companies_set = build_companies_set(cur)
    existing_keys = build_existing_keys(cur)

    inputs = list(iter_zip_inputs(BASE_DIR))
    if not inputs:
        print(f"Inga zip hittades i: {BASE_DIR}")
        return

    log_path = os.path.join("logs", f"financial_import_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    scanned_docs = 0
    inserted = 0
    skipped_not_in_companies = 0
    skipped_already_exists = 0
    skipped_validation_mismatch = 0
    skipped_parse_error = 0

    start = time.time()

    def log(line: str) -> None:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line.rstrip() + "\n")

    print(f"DB: {DB_PATH}")
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"Zip inputs: {len(inputs)}")
    print(f"Log: {log_path}")
    print(f"LOAD_COMPANIES_SET={LOAD_COMPANIES_SET} | LOAD_EXISTING_KEYS={LOAD_EXISTING_KEYS}")
    print("-" * 60)

    try:
        for zip_path in inputs:
            for orgnr_a, end_a, xbytes, source_label in iter_documents_from_zip(zip_path):
                scanned_docs += 1

                # Normalisera orgnr från filnamn (ska redan vara 10 siffror)
                orgnr = norm_orgnr(orgnr_a)
                if len(orgnr) != 10:
                    skipped_parse_error += 1
                    log(f"parse_error bad_orgnr_from_filename orgnr='{orgnr_a}' source='{source_label}'")
                    continue

                # Skippa om bolaget inte finns i companies
                if companies_set is not None and orgnr not in companies_set:
                    skipped_not_in_companies += 1
                    log(f"skipped not_in_companies orgnr={orgnr} end={end_a} source='{source_label}'")
                    continue

                # Skippa om vi redan har (orgnr, end_date)
                key = (orgnr, end_a)
                if existing_keys is not None and key in existing_keys:
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

                # Lätt validering från dokumentet (om finns)
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

                # Räkenskapsårslut-år
                fiscal_year_end_year = int(end_a[:4])

                # Konvertera till DB-typer
                def to_int_sek(v: Optional[float]) -> Optional[int]:
                    if v is None:
                        return None
                    # Värden kan vara float pga scale/decimal-komma. Vi rundar till närmaste heltal SEK.
                    return int(round(v))

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

                # Soliditet: lagra 0–100
                if out["solidity_pct"] is not None:
                    row["solidity_pct"] = float(soliditet_to_pct(out["solidity_pct"], units["solidity_pct"]))

                # Upsert
                try:
                    upsert_financial(cur, row)
                except Exception:
                    skipped_parse_error += 1
                    log(f"db_error upsert_failed orgnr={orgnr} end={end_a} source='{source_label}'")
                    continue

                inserted += 1
                if existing_keys is not None:
                    existing_keys.add(key)

                # Batch commit
                if inserted % COMMIT_EVERY == 0:
                    con.commit()

                # Progress
                if scanned_docs % PRINT_EVERY == 0:
                    rate = scanned_docs / max(1e-9, time.time() - start)
                    print(
                        f"[docs={scanned_docs}] inserted={inserted} "
                        f"skip_exists={skipped_already_exists} skip_not_in_companies={skipped_not_in_companies} "
                        f"skip_mismatch={skipped_validation_mismatch} err={skipped_parse_error} "
                        f"| {rate:.1f}/s"
                    )

    except KeyboardInterrupt:
        print("\n⛔ Avbruten av användare – committar data...")

    finally:
        con.commit()
        con.close()

    rate = scanned_docs / max(1e-9, time.time() - start)
    print("DONE ✅")
    print(f"docs_scanned={scanned_docs} inserted_or_updated={inserted} | {rate:.2f}/s")
    print(f"skipped_already_exists={skipped_already_exists}")
    print(f"skipped_not_in_companies={skipped_not_in_companies}")
    print(f"skipped_validation_mismatch={skipped_validation_mismatch}")
    print(f"errors={skipped_parse_error}")
    print(f"log_file={log_path}")

if __name__ == "__main__":
    main()
