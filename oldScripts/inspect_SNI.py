import zipfile
import csv
import io
from pathlib import Path

ZIP_PATH = Path("data/raw/bolagsverket_bulkfil.zip")
SAMPLE_ROWS = 10

DELIMS = [",", ";", "\t", "|"]
ENCODINGS = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

def decode_bytes(raw: bytes) -> tuple[str, str]:
    """Returnerar (text, encoding)"""
    for enc in ENCODINGS:
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            pass
    return raw.decode("utf-8", errors="replace"), "utf-8(replace)"

def sniff_delimiter(sample_text: str) -> str:
    try:
        return csv.Sniffer().sniff(sample_text, delimiters=DELIMS).delimiter
    except Exception:
        return ";"  # vanligast i svenska exportfiler

def norm(s: str) -> str:
    return (s or "").strip().lower().replace("\ufeff", "")

def pick_column(headers: list[str], candidates: list[str]) -> str | None:
    """
    Välj första header som matchar någon kandidat (substring).
    """
    h_norm = [norm(h) for h in headers]
    for cand in candidates:
        c = norm(cand)
        for i, h in enumerate(h_norm):
            if c in h:
                return headers[i]
    return None

def find_sni_columns(headers: list[str]) -> tuple[str | None, str | None]:
    """
    Försök hitta:
    - en "kod"-kolumn (sni/bransch + kod)
    - en text/benämning-kolumn (sni/bransch + text/benämning/beskrivning)
    Funkar för både 'SNI-koder organisation' och SCB-aktiga 'Bransch_1' etc.
    """
    h_norm = [norm(h) for h in headers]

    code_idx = None
    text_idx = None

    # 1) tydliga sni + kod / bransch + kod
    for i, h in enumerate(h_norm):
        if ("sni" in h or "bransch" in h or "näringsgren" in h or "naringsgren" in h) and "kod" in h:
            code_idx = i
            break

    # 2) text/benämning/beskrivning
    for i, h in enumerate(h_norm):
        if ("sni" in h or "bransch" in h or "näringsgren" in h or "naringsgren" in h) and (
            "text" in h or "benäm" in h or "benam" in h or "beskriv" in h or "namn" in h
        ):
            text_idx = i
            break

    # 3) fallback: om det heter typ "SNI-koder organisation" utan kod/text
    if code_idx is None:
        for i, h in enumerate(h_norm):
            if "sni" in h and ("koder" in h or "kod" in h):
                code_idx = i
                break

    return (headers[code_idx] if code_idx is not None else None,
            headers[text_idx] if text_idx is not None else None)

def main():
    if not ZIP_PATH.exists():
        raise SystemExit(f"File not found: {ZIP_PATH}")

    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        names = z.namelist()
        print("Files in ZIP:")
        for n in names:
            print(" -", n)

        data_file = next((n for n in names if n.lower().endswith((".csv", ".txt"))), None)
        if not data_file:
            raise SystemExit("No .csv or .txt found in the ZIP.")

        print("\nUsing file:", data_file)

        # Läs lite bytes för att gissa encoding + delimiter
        with z.open(data_file) as f:
            raw = f.read(120_000)
        sample_text, enc = decode_bytes(raw)
        delim = sniff_delimiter(sample_text[:10_000])

        print("\nGuessed encoding:", enc)
        print("Guessed delimiter:", repr(delim))

        # Öppna igen och streama med DictReader
        with z.open(data_file) as fbin:
            ftxt = io.TextIOWrapper(fbin, encoding=enc.replace("(replace)", ""), errors="replace", newline="")
            reader = csv.reader(ftxt, delimiter=delim)
            try:
                headers = next(reader)
            except StopIteration:
                raise SystemExit("File appears to be empty.")

        headers = [h.strip() for h in headers]
        print(f"\nHeader columns ({len(headers)}):")
        for h in headers[:80]:
            print(" -", h)
        if len(headers) > 80:
            print(f"... ({len(headers)} columns total)")

        # Försök hitta orgnr + namn
        org_col = pick_column(headers, ["organisationsnummer", "orgnr", "identitetsbeteckning"])
        name_col = pick_column(headers, ["organisationsnamn", "företagsnamn", "foretagsnamn", "namn"])

        sni_code_col, sni_text_col = find_sni_columns(headers)

        print("\nDetected columns:")
        print(" - ORGNR:", org_col)
        print(" - NAME :", name_col)
        print(" - SNI_CODE:", sni_code_col)
        print(" - SNI_TEXT:", sni_text_col)

        if not (org_col and name_col and sni_code_col):
            print("\n⚠️  Hittade inte allt automatiskt.")
            print("Tips: kopiera headern härifrån så pekar jag exakt på rätt kolumner.")
            # ändå fortsätt och prova med det som finns

        # Läs och printa sample-rader
        with z.open(data_file) as fbin:
            ftxt = io.TextIOWrapper(fbin, encoding=enc.replace("(replace)", ""), errors="replace", newline="")
            dict_reader = csv.DictReader(ftxt, delimiter=delim)
            print(f"\nSample rows (first {SAMPLE_ROWS}):\n")

            shown = 0
            for row in dict_reader:
                org = (row.get(org_col) if org_col else None) or ""
                name = (row.get(name_col) if name_col else None) or ""
                sni_code = (row.get(sni_code_col) if sni_code_col else None) or ""
                sni_text = (row.get(sni_text_col) if sni_text_col else None) or ""

                # hoppa tomma rader
                if not (org.strip() or name.strip() or sni_code.strip()):
                    continue

                print("ORGNR:", org.strip())
                print("NAME :", name.strip())
                print("SNI  :", sni_code.strip())
                if sni_text_col:
                    print("SNI_TEXT:", sni_text.strip())
                print("-" * 60)

                shown += 1
                if shown >= SAMPLE_ROWS:
                    break

            if shown == 0:
                print("No data rows printed (maybe different delimiter/encoding).")

if __name__ == "__main__":
    main()
