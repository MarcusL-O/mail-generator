#!/usr/bin/env python3
"""
Repo-audit dump för AI-granskning.
- Skriver INTE hemligheter (försök ändå undvika att ha .env med secrets i repo).
- Samlar struktur + nyckelfiler + enkla risk-checks.
"""

import os
import sys
import json
import platform
import subprocess
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

EXCLUDE_DIRS = {".git", ".venv", "venv", "__pycache__", ".pytest_cache", ".mypy_cache", "node_modules"}
EXCLUDE_FILES = {".DS_Store"}

# Begränsa hur mycket innehåll vi skriver ut från varje fil
MAX_FILE_CHARS = 12000

# Välj vilka "nyckelfiler" vi alltid vill dumpa om de finns
KEY_FILES = [
    "README.md",
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    ".env.example",
    ".gitignore",
    "data/README.md",
]

# Välj patterns att leta efter (enkla riskindikatorer)
GREP_PATTERNS = [
    "TODO", "FIXME",
    "sqlite3.connect", "outreach.db.sqlite", "companies.db.sqlite",
    "data/out", "NDJSON", "tmux",
    "SMTP", "smtplib", "sendgrid", "mailgun",
    "dotenv", ".env",
    "root@", "77.42.82.210", "mail-vps",
]

def run(cmd: list[str]) -> tuple[int, str]:
    """Kör kommando och returnerar (exit_code, output)."""
    try:
        p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        out = (p.stdout or "") + (("\n" + p.stderr) if p.stderr else "")
        return p.returncode, out.strip()
    except Exception as e:
        return 999, f"ERROR running {cmd}: {e}"

def safe_read(path: Path) -> str:
    try:
        txt = path.read_text(encoding="utf-8", errors="replace")
        if len(txt) > MAX_FILE_CHARS:
            return txt[:MAX_FILE_CHARS] + "\n\n...TRUNCATED...\n"
        return txt
    except Exception as e:
        return f"ERROR reading file: {e}"

def iter_files(max_depth: int = 5):
    for p in ROOT.rglob("*"):
        if p.is_dir():
            continue
        rel = p.relative_to(ROOT)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        if p.name in EXCLUDE_FILES:
            continue
        if len(rel.parts) > max_depth:
            continue
        yield p

def print_header(title: str):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)

def main():
    now = datetime.utcnow().isoformat() + "Z"

    print_header("AI REPO AUDIT DUMP")
    print(f"timestamp_utc: {now}")
    print(f"root: {ROOT}")
    print(f"python: {sys.version.replace(os.linesep, ' ')}")
    print(f"platform: {platform.platform()}")

    # Git info
    print_header("GIT INFO")
    code, out = run(["git", "rev-parse", "--is-inside-work-tree"])
    print(out)
    if code == 0 and out.strip() == "true":
        _, branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
        _, commit = run(["git", "rev-parse", "HEAD"])
        _, status = run(["git", "status", "--porcelain"])
        print(f"branch: {branch}")
        print(f"commit: {commit}")
        print("dirty:", "yes" if status.strip() else "no")
    else:
        print("Not a git repo (or git not available).")

    # File tree (begränsad)
    print_header("FILE TREE (maxdepth=5)")
    files = sorted([str(p.relative_to(ROOT)) for p in iter_files(max_depth=5)])
    print(f"total_files_listed: {len(files)}")
    for f in files[:1200]:
        print(f)
    if len(files) > 1200:
        print("\n...TRUNCATED FILE LIST...\n")

    # Key files contents
    print_header("KEY FILES (content)")
    for rel in KEY_FILES:
        p = ROOT / rel
        if p.exists() and p.is_file():
            print("\n" + "-" * 40)
            print(f"[{rel}]")
            print("-" * 40)
            print(safe_read(p))
        else:
            print(f"\n[{rel}] NOT FOUND")

    # Quick grep signals (utan externa grep)
    print_header("PATTERN SCAN (lightweight)")
    hits = {pat: [] for pat in GREP_PATTERNS}
    for p in iter_files(max_depth=6):
        # hoppa över stora binärer/DB
        if p.suffix.lower() in {".sqlite", ".db", ".png", ".jpg", ".jpeg", ".pdf", ".zip"}:
            continue
        txt = safe_read(p)
        for pat in GREP_PATTERNS:
            if pat in txt:
                hits[pat].append(str(p.relative_to(ROOT)))
    for pat, files_hit in hits.items():
        if files_hit:
            print(f"\nPATTERN: {pat}")
            for f in sorted(set(files_hit))[:80]:
                print(f" - {f}")
            if len(set(files_hit)) > 80:
                print(" ...TRUNCATED...")

    # Python sanity checks
    print_header("PYTHON SANITY")
    code, out = run([sys.executable, "-m", "compileall", "-q", "."])
    print(f"compileall_exit_code: {code}")
    print(out if out else "(no output)")

    code, out = run([sys.executable, "-m", "pip", "check"])
    print("\npip_check_exit_code:", code)
    print(out if out else "(no output)")

    # Requirements snapshot (om pip finns)
    print_header("PIP FREEZE (optional)")
    code, out = run([sys.executable, "-m", "pip", "freeze"])
    if code == 0:
        lines = out.splitlines()
        print(f"pip_freeze_lines: {len(lines)}")
        print("\n".join(lines[:300]))
        if len(lines) > 300:
            print("\n...TRUNCATED...\n")
    else:
        print(out)

    print_header("DONE")
    print("Klistra in hela outputen i chatten (eller spara till fil och ladda upp).")

if __name__ == "__main__":
    main()
