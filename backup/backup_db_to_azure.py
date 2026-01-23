# - Tar konsistent backup av SQLite via sqlite3 .backup
# - Gzipp:ar backupen
# - Laddar upp till Azure Blob (RBAC) med --auth-mode login
# - Endast DB-filer: companies + outreach

import argparse
import gzip
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class DbItem:
    key: str
    src_path: Path
    blob_prefix: str  # "companies" / "outreach"


def utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed ({p.returncode}): {' '.join(cmd)}\n{p.stdout}")
    if p.stdout.strip():
        print(p.stdout.strip())


def require_file(p: Path) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing file: {p}")


def sqlite_backup(src: Path, dst: Path) -> None:
    # .backup ger konsistent snapshot även om DB skrivs samtidigt.
    run(["sqlite3", str(src), f".backup '{dst.as_posix()}'"])


def gzip_file(src: Path, dst: Path) -> None:
    with src.open("rb") as rf, gzip.open(dst, "wb", compresslevel=6) as wf:
        shutil.copyfileobj(rf, wf)


def upload_blob(*, account: str, container: str, blob_name: str, file_path: Path) -> None:
    run(
        [
            "az", "storage", "blob", "upload",
            "--account-name", account,
            "--container-name", container,
            "--name", blob_name,
            "--file", str(file_path),
            "--auth-mode", "login",
            "--overwrite", "true",
        ]
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--storage-account", required=True)
    ap.add_argument("--container", default="backups")
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default = idag UTC)")
    args = ap.parse_args()

    date_str = (args.date or "").strip() or utc_date()

    root = Path(".")
    companies_db = root / "data/db/companies.db.sqlite"
    outreach_db = root / "data/db/outreach.db.sqlite"

    items = [
        DbItem(key="companies", src_path=companies_db, blob_prefix="companies"),
        DbItem(key="outreach", src_path=outreach_db, blob_prefix="outreach"),
    ]

    for it in items:
        require_file(it.src_path)

    print(f"Backup date: {date_str}")
    print(f"Storage: {args.storage_account} / {args.container}")

    with tempfile.TemporaryDirectory(prefix="dbbak_") as td:
        td_path = Path(td)

        for it in items:
            tmp_sqlite = td_path / f"{it.key}_{date_str}.sqlite"
            tmp_gz = td_path / f"{it.key}_{date_str}.sqlite.gz"

            print(f"\n== {it.key} ==")
            print(f"source: {it.src_path}")

            sqlite_backup(it.src_path, tmp_sqlite)
            gzip_file(tmp_sqlite, tmp_gz)

            blob_name = f"{it.blob_prefix}/{it.key}_{date_str}.sqlite.gz"
            upload_blob(
                account=args.storage_account,
                container=args.container,
                blob_name=blob_name,
                file_path=tmp_gz,
            )

            size_mb = tmp_gz.stat().st_size / (1024 * 1024)
            print(f"uploaded: {blob_name} ({size_mb:.2f} MB)")

    print("\nDONE ✅")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR ❌ {e}")
        sys.exit(1)
