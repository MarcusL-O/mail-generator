# Copilot / Agent Instructions for mail-generator

Short, actionable guidance to help an AI coding agent be productive in this repository.

**Repo Summary:**
- Language: Python (scripts under `scripts/`).
- Purpose: Pipeline that extracts company data from a bulk ZIP, filters Gothenburg entries, finds websites/emails and exports JSON (MVP described in `README.md`).
- Layout: `data/raw/` (input ZIPs), `scripts/` (numbered pipeline scripts), project root includes `requirments.txt` (note: filename is misspelled).

**Big picture / data flow**
- User places a Bolagsverket bulk ZIP at `data/raw/bolagsverket_bulkfil.zip`.
- `scripts/01_inspect.py` reads the ZIP, locates the CSV/TXT, decodes bytes as UTF-8 and sniff-detects delimiter, then prints header information.
- `scripts/02_filter_gbg_to_json.py` (next step) reads/filter rows for Gothenburg and writes JSON for downstream processing (web scraping / email extraction / mail-sending are separate concerns).

**Key files to inspect first**
- `README.md` — high-level plan (written in Swedish) describing the intended pipeline and MVP.
- `scripts/01_inspect.py` — shows ZIP handling, delimiter sniffing (`sniff_delimiter()`), and encoding/decoding choices.
- `scripts/02_filter_gbg_to_json.py` — next-stage filter (use to learn data model / output schema).
- `requirments.txt` (root) — install dependencies; verify filename `requirments.txt` (typo) before running installs.

**Project-specific conventions & gotchas**
- Pipeline order is expressed by numeric prefixes in `scripts/` (e.g., `01_`, `02_`). Preserve order when adding or renaming scripts.
- CSV/sniffing behavior: `sniff_delimiter(sample)` uses `csv.Sniffer` with preferred delimiters `[",",";","\t","|"]` and defaults to `';'` on failure.
- Encoding: scripts decode bytes using `utf-8` with `errors='replace'`. Expect some malformed bytes and tolerant behavior.
- Data reading: `01_inspect.py` reads up to ~80_000 bytes for sampling — don't rely on reading entire file for sniffing.
- Variable bug to watch: `01_inspect.py` uses a `text` variable for decoded content but later references `txt` (`lines = txt.splitlines()`), which will raise NameError. Use `text.splitlines()` instead.

**How to run locally (Windows PowerShell)**
1. Create venv and activate (PowerShell):

```
python -m venv .venv
.venv\Scripts\Activate.ps1
```

2. Install dependencies (take note of the filename typo):

```
pip install -r requirments.txt
```

3. Run the inspect script to preview headers and delimiter:

```
python scripts/01_inspect.py
```

4. Run the filter step (after ensuring input data exists):

```
python scripts/02_filter_gbg_to_json.py
```

**When editing code**
- Make minimal, surgical changes: preserve `scripts/` filenames and numeric ordering.
- Prefer small PRs that update one pipeline step at a time and include a short note about data expectations (sample bytes read, delimiter default).
- If adding tests, add them under a `tests/` dir and provide sample fixture data under `data/raw/sample/` to avoid committing large binaries.

**Integration points / external dependencies**
- No CI or tests detected — run scripts locally.
- Downstream steps (web lookups, email scraping, mail sending) are described in `README.md` but not implemented; confirm expected JSON schema in `scripts/02_filter_gbg_to_json.py` before integrating scrapers or mailers.

**Notes for agents**
- Prefer explicit references to files when suggesting changes (e.g., "update `scripts/01_inspect.py` to use `text.splitlines()`").
- When proposing dependency changes, update `requirments.txt` and include exact `pip` install strings.
- Translate or preserve Swedish comments/README content only when asked; don't overwrite the README plan without confirmation.

If anything here is unclear or you want more detail (examples for JSON schema, expected columns, or a sample fixture), tell me which area to expand.
