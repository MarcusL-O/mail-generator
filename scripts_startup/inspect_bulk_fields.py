import json
import csv
import zipfile
from pathlib import Path

BULK_FILE = "data/raw/bolagsverket_bulkfil.zip"  # ÄNDRA VID BEHOV


def inspect_text(lines):
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # NDJSON
        if line.startswith("{"):
            obj = json.loads(line)
            print("FIELDS FOUND:")
            for k in obj.keys():
                print("-", k)
            return

        # CSV/TSV
        delimiter = "\t" if "\t" in line else ";"
        reader = csv.DictReader([line], delimiter=delimiter)
        print("FIELDS FOUND:")
        for k in reader.fieldnames:
            print("-", k)
        return


def main():
    path = Path(BULK_FILE)
    if not path.exists():
        raise SystemExit("File not found")

    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                lines = (l.decode("utf-8", errors="replace") for l in f)
                inspect_text(lines)
    else:
        with path.open(encoding="utf-8", errors="replace") as f:
            inspect_text(f)


if __name__ == "__main__":
    main()

#python scripts_startup/inspect_bulk_fields.py