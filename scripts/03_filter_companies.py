import json
from pathlib import Path

IN_PATH = Path("data/out/goteborg_companies2.ndjson")
OUT_PATH = Path("data/out/goteborg_companies_filtered.ndjson")

# Ord/fraser som ofta betyder "inte IT-kund"
NEGATIVE_KEYWORDS = [
    "pizzeria", "restaurang", "café", "cafe", "frisör", "frisor",
    "livs", "livsmedel", "kiosk", "tobak",
    "taxi", "städ", "stad", "måleri", "maleri",
    "bygg", "snickeri", "rör", "ror", "el", "plåt", "plat",
    "bilverkstad", "verkstad", "däck", "dack",
    "salong", "massage", "hudvård", "hudvard",
    "bageri", "konditori", "hotell", "vandrarhem",
]

def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    scanned = 0
    saved = 0
    removed = 0

    with IN_PATH.open("r", encoding="utf-8") as fin, \
         OUT_PATH.open("w", encoding="utf-8") as fout:

        for line in fin:
            if not line.strip():
                continue

            scanned += 1
            obj = json.loads(line)

            name = (obj.get("name") or "").lower()

            # innehåller företagsnamnet något negativt keyword?
            if any(k in name for k in NEGATIVE_KEYWORDS):
                removed += 1
                continue

            fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
            saved += 1

    print("KLART ✅")
    print(f"Scannat totalt: {scanned:,}")
    print(f"Sparade: {saved:,}")
    print(f"Bortfiltrerade: {removed:,}")
    print(f"Output: {OUT_PATH}")

if __name__ == "__main__":
    main()
