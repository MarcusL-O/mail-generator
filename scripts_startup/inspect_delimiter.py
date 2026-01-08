import zipfile
from pathlib import Path
import json
import collections

BULK_FILE = "data/raw/bolagsverket_bulkfil.zip"  # ändra vid behov
CANDIDATE_DELIMITERS = [";", ",", "\t", "|", "$"]


def get_lines(path: Path, max_lines=3):
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path, "r") as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                for i, raw in enumerate(f):
                    if i >= max_lines:
                        break
                    yield raw.decode("utf-8", errors="replace").strip()
    else:
        with path.open(encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                yield line.strip()


def main():
    path = Path(BULK_FILE)
    if not path.exists():
        raise SystemExit("File not found")

    lines = list(get_lines(path))
    print("=== RAW LINES ===")
    for i, l in enumerate(lines, 1):
        print(f"[{i}] {l[:200]}")

    # JSON-check
    try:
        json.loads(lines[0])
        print("\nFORMAT: NDJSON (JSON per rad)")
        return
    except Exception:
        pass

    # delimiter check
    scores = {}
    for d in CANDIDATE_DELIMITERS:
        scores[d] = sum(l.count(d) for l in lines)

    print("\nDELIMITER COUNTS:")
    for d, c in scores.items():
        print(f"'{d}': {c}")

    best = max(scores.items(), key=lambda x: x[1])
    print(f"\nBEST GUESS DELIMITER: '{best[0]}'")


if __name__ == "__main__":
    main()
