import zipfile
import csv
import json
import io
from pathlib import Path

ZIP_PATH = Path("data/raw/bolagsverket_bulkfil.zip")
OUT_PATH = Path("data/out/goteborg_companies2.ndjson")          # ny outputfil
TMP_PATH = Path("data/out/goteborg_companies2.ndjson.tmp")

DATA_FILE_NAME = "bolagsverket_bulkfil.txt"
DELIMITER = ";"
TARGET_CITY = "GÖTEBORG"

def extract_before_dollar(value: str) -> str:
    if not value:
        return ""
    return value.split("$", 1)[0].strip().strip('"')

def parse_postadress(value: str) -> dict:
    if not value:
        return {"street": "", "co": "", "city": "", "postalCode": "", "countryCode": ""}

    v = value.strip().strip('"')
    parts = v.split("$")

    street = parts[0].strip() if len(parts) > 0 else ""
    co = parts[1].strip() if len(parts) > 1 else ""
    city = parts[2].strip().upper() if len(parts) > 2 else ""
    postal = parts[3].strip() if len(parts) > 3 else ""
    country = parts[4].strip() if len(parts) > 4 else ""

    return {
        "street": street,
        "co": co,
        "city": city,
        "postalCode": postal,
        "countryCode": country
    }

def main():
    if not ZIP_PATH.exists():
        raise SystemExit(f"ZIP not found: {ZIP_PATH}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if TMP_PATH.exists():
        TMP_PATH.unlink()

    seen = set()
    scanned = 0
    kept = 0

    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        if DATA_FILE_NAME not in z.namelist():
            raise SystemExit(f"Hittar inte '{DATA_FILE_NAME}' i zip. Innehåll: {z.namelist()}")

        with z.open(DATA_FILE_NAME) as f_in, TMP_PATH.open("w", encoding="utf-8") as f_out:
            # EXAKT som du körde när det funkade:
            text_stream = io.TextIOWrapper(f_in, encoding="utf-8", errors="replace", newline="")
            reader = csv.DictReader(text_stream, delimiter=DELIMITER)

            for row in reader:
                scanned += 1

                org_raw = row.get("organisationsidentitet", "")
                name_raw = row.get("organisationsnamn", "")
                post_raw = row.get("postadress", "")

                orgnr = extract_before_dollar(org_raw)
                if not orgnr or orgnr in seen:
                    continue

                name = extract_before_dollar(name_raw)
                if not name:
                    continue

                addr = parse_postadress(post_raw)
                if addr["city"] != TARGET_CITY:
                    continue

                # ✅ ENDA ÄNDRINGEN: minimal output
                f_out.write(json.dumps({"orgnr": orgnr, "name": name}, ensure_ascii=False) + "\n")
                kept += 1
                seen.add(orgnr)

                if scanned % 1_000_000 == 0:
                    print(f"Scannat {scanned:,} rader – sparat {kept:,} Göteborg")

    if OUT_PATH.exists():
        OUT_PATH.unlink()
    TMP_PATH.rename(OUT_PATH)

    print("\nKLART ✅")
    print(f"Scannat totalt: {scanned:,}")
    print(f"Göteborg sparade: {kept:,}")
    print(f"Output: {OUT_PATH} (NDJSON)")

if __name__ == "__main__":
    main()
