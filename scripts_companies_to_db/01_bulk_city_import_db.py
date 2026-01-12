# Hämtar från Bulkfil, tar data som orgnr + namn + registreringsdatum
# Skickar datan till DB (filtrerar på CITY) + skriver progress i terminal
# VIKTIGT: INSERT ONLY (skriver ALDRIG över befintliga rader)

import os
import csv
import json
import zipfile
import sqlite3
from pathlib import Path
from datetime import datetime
import re

# =========================
# ÄNDRA HÄR
# =========================
BULK_FILE = "data/raw/bolagsverket_bulkfil.zip"
CITY = "Stockholm"         # ex: "Stockholm", "Malmö"
BATCH_LIMIT = None         # None = ingen limit, annars t.ex. 10
PRINT_EVERY = 10000         
# =========================

DB_PATH = os.getenv("DB_PATH", "data/companies.db.sqlite")
TABLE = os.getenv("DB_TABLE", "companies")

COL_ORGNR = os.getenv("DB_COL_ORGNR", "orgnr")
COL_NAME = os.getenv("DB_COL_NAME", "name")
COL_CITY = os.getenv("DB_COL_CITY", "city")
COL_STARTED_AT = os.getenv("DB_COL_STARTED_AT", "started_at")

COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "2000"))

BULK_ORGNR_KEYS = ["organisationsidentitet"]
BULK_NAME_KEYS = ["organisationsnamn"]
BULK_POSTADRESS_KEYS = ["postadress"]
BULK_STARTED_AT_KEYS = ["registreringsdatum"]


def clean_bulk_value(s: str) -> str:
    s = (s or "").strip().strip('"')
    if not s:
        return ""
    # Ta bort metadata efter första $
    if "$" in s:
        s = s.split("$", 1)[0].strip()
    return s


def _pick(row: dict, keys: list[str]) -> str:
    for k in keys:
        if k in row and row[k] is not None:
            v = clean_bulk_value(str(row[k]))
            if v:
                return v
    return ""


def _to_iso_date(s: str) -> str:
    s = clean_bulk_value(s)
    if not s:
        return ""
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y.%m.%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date().isoformat()
        except ValueError:
            pass
    return s


def normalize_city(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def extract_city_from_postadress(postadress: str) -> str:
    """
    I din bulk ligger ofta postadress som:
      "Box 7435$$STOCKHOLM"
      "Järnvågsgatan 3$c/o X$GÖTEBORG$$SE-LAND"
    Vi tar texten efter sista '$$' om den finns, annars jobbar vi med hela.
    Sen rensar vi metadata efter '$', tar bort postnummer och städar whitespace.
    """
    s = (postadress or "").strip().strip('"')
    if not s:
        return ""

    if "$$" in s:
        tail = s.split("$$")[-1]
    else:
        tail = s

    if "$" in tail:
        tail = tail.split("$", 1)[0]

    tail = tail.replace("\r", "\n").strip()
    tail = re.sub(r"\b(SE-?)?\d{3}\s?\d{2}\b", "", tail, flags=re.IGNORECASE).strip()
    tail = re.sub(r"\bsverige\b", "", tail, flags=re.IGNORECASE).strip()
    tail = tail.replace(",", " ")
    tail = re.sub(r"\s+", " ", tail).strip()
    return tail


def city_matches(postadress: str, wanted_city: str) -> bool:
    city = normalize_city(extract_city_from_postadress(postadress))
    wanted = normalize_city(wanted_city)
    if not city or not wanted:
        return False
    if city == wanted:
        return True
    if wanted in city:
        return re.search(rf"\b{re.escape(wanted)}\b", city) is not None
    return False


def _iter_text_lines(path: Path):
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as z:
            names = [n for n in z.namelist() if not n.endswith("/")]
            if not names:
                raise SystemExit("ZIP contains no files.")
            with z.open(names[0], "r") as f:
                for raw in f:
                    yield raw.decode("utf-8", errors="replace")
        return

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            yield line


def _detect_format(path: Path) -> str:
    suf = path.suffix.lower()
    if suf in (".ndjson", ".jsonl"):
        return "ndjson"
    if suf == ".tsv":
        return "tsv"
    if suf == ".csv":
        return "csv"
    if suf == ".zip":
        first = next(_iter_text_lines(path), "")
        if first.lstrip().startswith("{"):
            return "ndjson"
        return "tsv" if "\t" in first else "csv"
    first = next(_iter_text_lines(path), "")
    if first.lstrip().startswith("{"):
        return "ndjson"
    return "tsv" if "\t" in first else "csv"


def _iter_rows(path: Path, fmt: str):
    lines = _iter_text_lines(path)

    if fmt == "ndjson":
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue
        return

    first = next(lines, "")
    delim = "\t" if fmt == "tsv" else ";"
    if fmt == "csv" and first.count(",") > first.count(";"):
        delim = ","

    def all_lines():
        yield first
        for l in lines:
            yield l

    reader = csv.DictReader(all_lines(), delimiter=delim)
    for row in reader:
        if row:
            yield row


def main():
    in_path = Path(BULK_FILE)
    if not in_path.exists():
        raise SystemExit(f"Input file not found: {in_path}")

    fmt = _detect_format(in_path)
    print(f"Starting import: file={in_path} format={fmt} city='{CITY}' batch_limit={BATCH_LIMIT}")

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    cur = con.cursor()

    # index för snabb exists-check
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_ORGNR} ON {TABLE}({COL_ORGNR});")

    def exists_orgnr(orgnr: str) -> bool:
        cur.execute(f"SELECT 1 FROM {TABLE} WHERE {COL_ORGNR}=? LIMIT 1", (orgnr,))
        return cur.fetchone() is not None

    processed = matched_city = inserted = skipped_exists = bad = 0
    printed_first = False

    try:
        for row in _iter_rows(in_path, fmt):
            processed += 1
            try:
                orgnr = _pick(row, BULK_ORGNR_KEYS)
                if not orgnr:
                    bad += 1
                    continue

                postadress_raw = str(row.get(BULK_POSTADRESS_KEYS[0], "") or "")
                if CITY and not city_matches(postadress_raw, CITY):
                    continue

                matched_city += 1

                # INSERT ONLY: skriv ALDRIG över
                if exists_orgnr(orgnr):
                    skipped_exists += 1
                    continue

                city = extract_city_from_postadress(postadress_raw)
                name = _pick(row, BULK_NAME_KEYS)
                started_at = _to_iso_date(_pick(row, BULK_STARTED_AT_KEYS))

                if not printed_first:
                    printed_first = True
                    print("FIRST INSERT ✅")
                    print(f"orgnr={orgnr}")
                    print(f"name={name}")
                    print(f"city_extracted={city}")
                    print(f"started_at={started_at}")
                    print("-" * 40)

                cur.execute(
                    f"""
                    INSERT INTO {TABLE}
                      ({COL_ORGNR}, {COL_NAME}, {COL_CITY}, {COL_STARTED_AT})
                    VALUES (?, ?, ?, ?)
                    """,
                    (orgnr, name, city, started_at),
                )
                inserted += 1

                if PRINT_EVERY and inserted % PRINT_EVERY == 0:
                    print(
                        f"[inserted={inserted}] processed={processed} matched_city={matched_city} "
                        f"skipped_exists={skipped_exists} bad={bad}"
                    )

                if COMMIT_EVERY and inserted % COMMIT_EVERY == 0:
                    con.commit()

                if BATCH_LIMIT is not None and inserted >= int(BATCH_LIMIT):
                    break

            except Exception:
                bad += 1
                continue

    except KeyboardInterrupt:
        print("\n⛔ Avbruten av användare – committar data...")

    finally:
        con.commit()
        con.close()
        print("✅ Data committad till DB")

    print("DONE ✅")
    print(
        f"processed={processed} matched_city={matched_city} inserted={inserted} "
        f"skipped_exists={skipped_exists} bad={bad}"
    )


if __name__ == "__main__":
    main()

# python get_data/01_bulk_city_import_db.py