#Öppna zip fil
#lista filerna i zip
#läs bara 80 Kb i taget
#gissa delimiter
#skriv ut första 5 raderna
#syfte: kunna se hur csv filen är formaterad innan vi kör script 02 ( datan klarade int av att öppna den)


import zipfile
import csv
from pathlib import Path

ZIP_PATH = Path("data/raw/bolagsverket_bulkfil.zip")

def sniff_delimiter(sample: str) -> str:
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"]).delimiter
    except Exception:
        return ";"

def main():
    if not ZIP_PATH.exists():
        raise SystemExit(f"File not found: {ZIP_PATH}")

    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        names = z.namelist()
        print("Files in the ZIP:")
        for n in names:
            print(" -", n)

        data_file = next((n for n in names if n.lower().endswith((".csv", ".txt"))), None)
        if not data_file:
            raise SystemExit("No CSV or TXT file found in the ZIP archive.")

        print("\nInspecting file:", data_file)

        with z.open(data_file) as f:
            raw = f.read(80_000)  # bara första 80 KB
            text = raw.decode("utf-8", errors="replace")
            delimiter = sniff_delimiter(text[:5000])

            print("\nGuessed delimiter:", repr(delimiter))

            lines = text.splitlines()
            print("\nFirst rows (sample):")
            for l in lines[:5]:
                print(l)

            header = lines[0].split(delimiter)
            print("\nHeader columns:")
            for c in header[:80]:
                print(" -", c.strip())
            if len(header) > 80:
                print(f"... ({len(header)} columns total)")

if __name__ == "__main__":
    main()
