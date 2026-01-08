import zipfile
from pathlib import Path
import re

ZIP_PATH = Path("data/raw/bolagsverket_bulkfil.zip")

MAX_HITS = 20
CHUNK_SIZE = 256 * 1024

# Strukturerade varianter vi vill hitta
PATS = [
    re.compile(r"(?i)\$sni"),             # $SNI (samma stil som $ORGNR-IDORG)
    re.compile(r"(?i)sni[_-]"),           # SNI_ eller SNI-
    re.compile(r"(?i)\"sni\""),           # "sni" (JSON-liknande)
    re.compile(r"(?i)naringsgren"),       # fältnamn
    re.compile(r"(?i)näringsgren"),
    re.compile(r"(?i)sni-kod"),           # SNI-kod
    re.compile(r"(?i)sni kod"),
]

def iter_lines_from_zip(zip_path: Path, inner_name: str):
    with zipfile.ZipFile(zip_path, "r") as z:
        with z.open(inner_name) as f:
            buf = b""
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    yield line.decode("utf-8", errors="replace")
            if buf:
                yield buf.decode("utf-8", errors="replace")

def main():
    if not ZIP_PATH.exists():
        raise SystemExit(f"Missing: {ZIP_PATH}")

    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        data_file = next((n for n in z.namelist() if n.lower().endswith((".txt", ".csv"))), None)
        if not data_file:
            raise SystemExit("No .txt/.csv found in zip")
        print("Using:", data_file)

    hits = 0
    for line in iter_lines_from_zip(ZIP_PATH, data_file):
        for pat in PATS:
            m = pat.search(line)
            if m:
                print("\n--- HIT ---")
                print("Pattern:", pat.pattern)
                print(line[:2000])
                hits += 1
                if hits >= MAX_HITS:
                    print(f"\nDone. hits={hits}")
                    return

    print(f"\nDone. hits={hits} (no structured SNI patterns found)")

if __name__ == "__main__":
    main()
