# apply_new_companies.py
# Läser Bolagsverkets bulk-zip (manuellt nedladdad + döpt: bolagsverket_bulk_YYYY-MM-DD.zip)
# och:
# - INSERT: nya orgnr (companies)
# - UPDATE: befintliga rader om de saknar värden (fill-if-null)
# - Markerar inactive om avregistreringsdatum finns

from __future__ import annotations

import argparse
import csv
import sqlite3
import zipfile
from pathlib import Path
from typing import Dict, Optional, Iterable, Tuple

# =========================
# Config
# =========================
DB_PATH_DEFAULT = "data/db/companies.db.sqlite"
TABLE = "companies"

RAW_DIR = Path("data/raw/bolagsverket")
ZIP_GLOB = "bolagsverket_bulk_*.zip"

# Bas-kolumner (finns redan i din DB)
COL_ORGNR = "orgnr"
COL_NAME = "name"   # NOT NULL i din schema
COL_CITY = "city"
COL_CREATED_AT = "created_at"
COL_UPDATED_AT = "updated_at"

# BV-kolumner (läggs till)
BV_REGDATE = "bv_registration_date"
BV_ORGFORM = "bv_org_form"
BV_ORGNAME = "bv_org_name"
BV_DEREGDATE = "bv_deregistration_date"
BV_DEREGREASON = "bv_deregistration_reason"
BV_DESC = "bv_business_description"
BV_POSTADDR = "bv_post_address"
BV_POSTNR = "bv_postnr"
BV_POSTORT = "bv_postort"
BV_ACTIVE = "bv_active"  # 1=aktiv, 0=inaktiv

WANTED_COLS: Dict[str, str] = {
    BV_REGDATE: "TEXT",
    BV_ORGFORM: "TEXT",
    BV_ORGNAME: "TEXT",
    BV_DEREGDATE: "TEXT",
    BV_DEREGREASON: "TEXT",
    BV_DESC: "TEXT",
    BV_POSTADDR: "TEXT",
    BV_POSTNR: "TEXT",
    BV_POSTORT: "TEXT",
    BV_ACTIVE: "INTEGER",
}

# =========================
# Helpers
# =========================
def digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())

def is_blank(v: Optional[str]) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")

def db_path() -> str:
    return DB_PATH_DEFAULT

def ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cols = {row[1] for row in cur.execute(f"PRAGMA table_info({TABLE})").fetchall()}
    for col, typ in WANTED_COLS.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col} {typ}")
    con.commit()

def latest_zip(path: Path) -> Path:
    files = sorted(path.glob(ZIP_GLOB))
    if not files:
        raise SystemExit(f"Ingen zip hittad i {path} som matchar {ZIP_GLOB}")
    return files[-1]

def _iter_text_lines_from_zip(zip_path: Path) -> Iterable[str]:
    with zipfile.ZipFile(zip_path, "r") as z:
        names = [n for n in z.namelist() if not n.endswith("/")]
        if not names:
            raise SystemExit("ZIP contains no files.")
        with z.open(names[0], "r") as f:
            for raw in f:
                line = raw.decode("utf-8", errors="replace")
                if "\x00" in line:
                    continue
                yield line

def _iter_rows_from_zip(zip_path: Path):
    # Kommentar (svenska): BV-bulk verkar vara CSV med semikolon-delimiter.
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

def clean_bulk_value_keep_first(s: str) -> str:
    # Kommentar (svenska): För de flesta fält vill vi ta första delen före $.
    s = (s or "").strip().strip('"')
    if not s:
        return ""
    if "$" in s:
        s = s.split("$", 1)[0].strip()
    return s

def extract_orgnr_only(orgident: str) -> Optional[str]:
    # organisationsidentitet: "<id>$ORGNR-IDORG"
    s = (orgident or "").strip().strip('"')
    if not s:
        return None
    parts = s.split("$")
    ident = parts[0].strip() if len(parts) > 0 else ""
    ident_typ = parts[1].strip() if len(parts) > 1 else ""
    if ident_typ != "ORGNR-IDORG":
        return None
    org10 = digits_only(ident)
    return org10 if len(org10) == 10 else None

def parse_postadress(postadress: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # postadress = "utdelningsadress$CO$postnr$postort$land"
    s = (postadress or "").strip().strip('"')
    if not s:
        return None, None, None
    parts = s.split("$")
    utd = parts[0].strip() if len(parts) > 0 else ""
    co = parts[1].strip() if len(parts) > 1 else ""
    postnr = parts[2].strip() if len(parts) > 2 else ""
    postort = parts[3].strip() if len(parts) > 3 else ""
    addr = utd
    if co:
        addr = f"{utd}, {co}" if utd else co
    return (addr or None), (postnr or None), (postort or None)

def pick_orgname(orgnam_field: str) -> Optional[str]:
    # organisationsnamn kan vara: "Namn$FORETAGSNAMN-ORGNAM$YYYY-MM-DD|..."
    s = (orgnam_field or "").strip().strip('"')
    if not s:
        return None
    first = s.split("|", 1)[0]
    # Ta bara namndelen (före första $)
    return clean_bulk_value_keep_first(first) or None

# =========================
# Main
# =========================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=db_path())
    ap.add_argument("--commit-every", type=int, default=2000)
    ap.add_argument("--print-every", type=int, default=50000)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--zip", default="")  # valfritt: peka på exakt zip
    args = ap.parse_args()

    zip_path = Path(args.zip).resolve() if args.zip else latest_zip(RAW_DIR)
    print(f"→ Using zip: {zip_path}")

    con = sqlite3.connect(args.db)
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ensure_columns(con)

    existing = {
        row[0] for row in cur.execute(f"SELECT {COL_ORGNR} FROM {TABLE}").fetchall()
        if row and row[0]
    }

    scanned = inserted = updated = 0
    first_printed = False

    try:
        for row in _iter_rows_from_zip(zip_path):
            scanned += 1

            row_l = {str(k).strip().lower(): v for k, v in (row or {}).items()}

            orgident_raw = str(row_l.get("organisationsidentitet", "") or "")
            orgnr = extract_orgnr_only(orgident_raw)
            if not orgnr:
                continue

            orgname_raw = str(row_l.get("organisationsnamn", "") or "")
            orgname = pick_orgname(orgname_raw)

            regdate = clean_bulk_value_keep_first(str(row_l.get("registreringsdatum", "") or "")) or None
            orgform = clean_bulk_value_keep_first(str(row_l.get("organisationsform", "") or "")) or None
            deregdate = clean_bulk_value_keep_first(str(row_l.get("avregistreringsdatum", "") or "")) or None
            deregreason = clean_bulk_value_keep_first(str(row_l.get("avregistreringsorsak", "") or "")) or None
            desc = str(row_l.get("verksamhetsbeskrivning", "") or "").strip().strip('"') or None

            postadress_raw = str(row_l.get("postadress", "") or "")
            addr, postnr, postort = parse_postadress(postadress_raw)

            is_new = orgnr not in existing

            if not first_printed:
                first_printed = True
                print("FIRST ROW ✅")
                print(f"orgnr={orgnr} new={is_new}")
                print(f"orgname={orgname}")
                print(f"regdate={regdate} orgform={orgform}")
                print(f"deregdate={deregdate} reason={deregreason}")
                print(f"postort={postort} postnr={postnr} addr={addr}")
                print("-" * 40)

            # INSERT nya (name är NOT NULL)
            if is_new:
                name_for_db = orgname or f"ORG {orgnr}"
                if not args.dry_run:
                    cur.execute(
                        f"""
                        INSERT INTO {TABLE} ({COL_ORGNR}, {COL_NAME}, {COL_CITY}, {COL_CREATED_AT}, {COL_UPDATED_AT})
                        VALUES (?, ?, ?, datetime('now'), datetime('now'))
                        """,
                        (orgnr, name_for_db, postort),
                    )
                existing.add(orgnr)
                inserted += 1

            # UPDATE fill-if-null + alltid sätta avregistrering om värde finns
            if not args.dry_run:
                cur.execute(
                    f"""
                    UPDATE {TABLE}
                    SET
                      {COL_NAME} = CASE WHEN {COL_NAME} IS NULL OR {COL_NAME}='' THEN ? ELSE {COL_NAME} END,
                      {COL_CITY} = CASE WHEN {COL_CITY} IS NULL OR {COL_CITY}='' THEN ? ELSE {COL_CITY} END,

                      {BV_ORGNAME} = CASE WHEN {BV_ORGNAME} IS NULL OR {BV_ORGNAME}='' THEN ? ELSE {BV_ORGNAME} END,
                      {BV_REGDATE} = CASE WHEN {BV_REGDATE} IS NULL OR {BV_REGDATE}='' THEN ? ELSE {BV_REGDATE} END,
                      {BV_ORGFORM} = CASE WHEN {BV_ORGFORM} IS NULL OR {BV_ORGFORM}='' THEN ? ELSE {BV_ORGFORM} END,
                      {BV_DESC} = CASE WHEN {BV_DESC} IS NULL OR {BV_DESC}='' THEN ? ELSE {BV_DESC} END,
                      {BV_POSTADDR} = CASE WHEN {BV_POSTADDR} IS NULL OR {BV_POSTADDR}='' THEN ? ELSE {BV_POSTADDR} END,
                      {BV_POSTNR} = CASE WHEN {BV_POSTNR} IS NULL OR {BV_POSTNR}='' THEN ? ELSE {BV_POSTNR} END,
                      {BV_POSTORT} = CASE WHEN {BV_POSTORT} IS NULL OR {BV_POSTORT}='' THEN ? ELSE {BV_POSTORT} END,

                      {BV_DEREGDATE} = CASE WHEN ? IS NOT NULL AND ? != '' THEN ? ELSE {BV_DEREGDATE} END,
                      {BV_DEREGREASON} = CASE WHEN ? IS NOT NULL AND ? != '' THEN ? ELSE {BV_DEREGREASON} END,

                      {BV_ACTIVE} = CASE
                          WHEN ? IS NOT NULL AND ? != '' THEN 0
                          ELSE COALESCE({BV_ACTIVE}, 1)
                      END,

                      {COL_UPDATED_AT} = datetime('now')
                    WHERE {COL_ORGNR} = ?
                    """,
                    (
                        orgname,
                        postort,

                        orgname,
                        regdate,
                        orgform,
                        desc,
                        addr,
                        postnr,
                        postort,

                        deregdate, deregdate, deregdate,
                        deregreason, deregreason, deregreason,

                        deregdate, deregdate,

                        orgnr,
                    ),
                )

            updated += 1

            if scanned % args.commit_every == 0:
                con.commit()

            if scanned % args.print_every == 0:
                print(f"[{scanned:,}] inserted_new={inserted:,} updated={updated:,} dry_run={args.dry_run}")

        con.commit()

    except KeyboardInterrupt:
        print("\n⛔ Avbruten – committar...")
        con.commit()

    finally:
        con.close()

    print("DONE ✅")
    print(f"scanned={scanned:,}")
    print(f"inserted_new={inserted:,}")
    print(f"updated={updated:,}")
    print(f"zip={zip_path.name}")

if __name__ == "__main__":
    main()
