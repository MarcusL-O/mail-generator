# apply_new_companies.py
# Läser Bolagsverkets bulk-zip och:
# - INSERT: nya orgnr (companies)
# - UPDATE: befintliga rader (fill-if-null)
# - company_status sätts alltid från senaste filen

from __future__ import annotations

import argparse
import csv
import sqlite3
import zipfile
from pathlib import Path
from typing import Optional, Iterable, Tuple

# =========================
# Config
# =========================
DB_PATH_DEFAULT = "data/db/companies.db.sqlite"
TABLE = "companies"

RAW_DIR = Path("data/raw/bolagsverket")
ZIP_GLOB = "bolagsverket_bulk_*.zip"

# Kolumner i companies
COL_ORGNR = "orgnr"
COL_NAME = "name"
COL_KOMUN = "kommun"
COL_POSTORT = "postort"
COL_REGISTRATION_DATE = "registration_date"
COL_LEGAL_FORM = "legal_form"
COL_COMPANY_STATUS = "company_status"

# =========================
# Helpers
# =========================
def digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def db_path() -> str:
    return DB_PATH_DEFAULT

def latest_zip(path: Path) -> Path:
    files = sorted(path.glob(ZIP_GLOB))
    if not files:
        raise SystemExit(f"Ingen zip hittad i {path}")
    return files[-1]

def _iter_text_lines_from_zip(zip_path: Path) -> Iterable[str]:
    with zipfile.ZipFile(zip_path, "r") as z:
        name = next(n for n in z.namelist() if not n.endswith("/"))
        with z.open(name, "r") as f:
            for raw in f:
                line = raw.decode("utf-8", errors="replace")
                if "\x00" not in line:
                    yield line

def _iter_rows_from_zip(zip_path: Path):
    lines = _iter_text_lines_from_zip(zip_path)
    first = next(lines, "")
    if not first:
        return

    def all_lines():
        yield first
        for l in lines:
            yield l

    reader = csv.DictReader(all_lines(), delimiter=";")
    for row in reader:
        if row:
            yield row

def clean_bulk_value_keep_first(s: str) -> Optional[str]:
    s = (s or "").strip().strip('"')
    if not s:
        return None
    return s.split("$", 1)[0].strip() or None

def extract_orgnr_only(orgident: str) -> Optional[str]:
    s = (orgident or "").strip().strip('"')
    if not s:
        return None
    parts = s.split("$")
    if len(parts) < 2 or parts[1] != "ORGNR-IDORG":
        return None
    org10 = digits_only(parts[0])
    return org10 if len(org10) == 10 else None

def parse_postadress(postadress: str) -> Tuple[Optional[str], Optional[str]]:
    s = (postadress or "").strip().strip('"')
    if not s:
        return None, None
    parts = s.split("$")
    postnr = parts[2].strip() if len(parts) > 2 else None
    postort = parts[3].strip() if len(parts) > 3 else None
    return postnr or None, postort or None

def pick_orgname(orgnam_field: str) -> Optional[str]:
    s = (orgnam_field or "").strip().strip('"')
    if not s:
        return None
    return s.split("|", 1)[0].split("$", 1)[0].strip() or None

def map_orgform_to_legal_form(orgform: Optional[str]) -> Optional[str]:
    if not orgform:
        return None

    s = orgform.upper()

    # Aktiebolag (alla varianter)
    if "AB" in s:
        return "AB"

    # Handelsbolag / Kommanditbolag
    if "KOMMANDIT" in s or s.startswith("KB"):
        return "KB"
    if "HANDELSBOLAG" in s or s.startswith("HB"):
        return "HB"

    # Enskild firma
    if "ENSKILD" in s or s.startswith("EF"):
        return "EF"

    # Bostadsrättsförening
    if "BOSTADSRÄTT" in s or "BRF" in s:
        return "BRF"

    # Ekonomisk förening
    if "EKONOMISK" in s or s.startswith("EK"):
        return "EK"

    # Stiftelse
    if "STIFTELSE" in s or s.startswith("ST"):
        return "ST"

    # Ideell förening
    if "IDEELL" in s:
        return "IDEELL"

    # Fallback (ska i praktiken nästan aldrig hända)
    return "OTHER"


# =========================
# Main
# =========================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=db_path())
    ap.add_argument("--commit-every", type=int, default=2000)
    ap.add_argument("--print-every", type=int, default=50000)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--zip", default="")
    args = ap.parse_args()

    zip_path = Path(args.zip).resolve() if args.zip else latest_zip(RAW_DIR)
    print(f"→ Using zip: {zip_path}")

    con = sqlite3.connect(args.db)
    con.execute("PRAGMA journal_mode=WAL;")
    cur = con.cursor()

    existing = {
        r[0] for r in cur.execute(f"SELECT {COL_ORGNR} FROM {TABLE}").fetchall()
    }

    scanned = inserted = updated = 0
    first_printed = False

    try:
        for row in _iter_rows_from_zip(zip_path):
            scanned += 1
            row_l = {k.lower(): v for k, v in row.items()}

            orgnr = extract_orgnr_only(row_l.get("organisationsidentitet", ""))
            if not orgnr:
                continue

            orgname = pick_orgname(row_l.get("organisationsnamn", ""))
            regdate = clean_bulk_value_keep_first(row_l.get("registreringsdatum", ""))
            orgform_raw = clean_bulk_value_keep_first(row_l.get("organisationsform", ""))
            legal_form = map_orgform_to_legal_form(orgform_raw)
            deregdate = clean_bulk_value_keep_first(row_l.get("avregistreringsdatum", ""))

            _, postort = parse_postadress(row_l.get("postadress", ""))

            company_status = "inactive" if deregdate else "active"
            is_new = orgnr not in existing

            if not first_printed:
                first_printed = True
                print("FIRST ROW ✅")
                print(f"orgnr={orgnr} new={is_new}")
                print(f"orgname={orgname}")
                print(f"regdate={regdate} legal_form={legal_form}")
                print(f"company_status={company_status}")
                print(f"postort={postort}")
                print("-" * 40)

            if is_new:
                if not args.dry_run:
                    cur.execute(
                        f"""
                        INSERT INTO {TABLE}
                        ({COL_ORGNR}, {COL_NAME}, {COL_KOMUN}, {COL_POSTORT},
                         {COL_REGISTRATION_DATE}, {COL_LEGAL_FORM}, {COL_COMPANY_STATUS})
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (orgnr, orgname or f"ORG {orgnr}", None, postort,
                         regdate, legal_form, company_status),
                    )
                existing.add(orgnr)
                inserted += 1

            if not args.dry_run:
                cur.execute(
                    f"""
                    UPDATE {TABLE}
                    SET
                      {COL_NAME} = COALESCE({COL_NAME}, ?),
                      {COL_POSTORT} = COALESCE({COL_POSTORT}, ?),
                      {COL_REGISTRATION_DATE} = COALESCE({COL_REGISTRATION_DATE}, ?),
                      {COL_LEGAL_FORM} = COALESCE({COL_LEGAL_FORM}, ?),
                      {COL_COMPANY_STATUS} = ?
                    WHERE {COL_ORGNR} = ?
                    """,
                    (orgname, postort, regdate, legal_form, company_status, orgnr),
                )

            updated += 1

            if scanned % args.commit_every == 0:
                con.commit()
            if scanned % args.print_every == 0:
                print(f"[{scanned:,}] inserted={inserted:,} updated={updated:,}")

        con.commit()

    finally:
        con.close()

    print("DONE ✅")
    print(f"scanned={scanned:,} inserted={inserted:,} updated={updated:,}")
    print(f"zip={zip_path.name}")

if __name__ == "__main__":
    main()
