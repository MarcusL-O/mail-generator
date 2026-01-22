# companies/control/restore_db_from_azure.py
# Kommentar (svenska):
# - Laddar ner backup från Azure Blob för valt datum
# - Packar upp .gz
# - Ersätter DB-filerna i data/db/
# - Säkrast: stoppa din service innan restore (gör det manuellt)

import argparse
import gzip
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DbItem:
    key: str
    dst_path: Path
    blob_prefix: str


def run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed ({p.returncode}): {' '.join(cmd)}\n{p.stdout}")
    if p.stdout.strip():
        print(p.stdout.strip())


def download_blob(*, account: str, container: str, blob_name: str, out_path: Path) -> None:
    run(
        [
            "az", "storage", "blob", "download",
            "--account-name", account,
            "--container-name", container,
            "--name", blob_name,
            "--file", str(out_path),
            "--auth-mode", "login",
        ]
    )


def gunzip_file(src_gz: Path, dst: Path) -> None:
    with gzip.open(src_gz, "rb") as rf, dst.open("wb") as wf:
        shutil.copyfileobj(rf, wf)


def backup_existing(dst: Path) -> None:
    if not dst.exists():
        return
    bak = dst.with_suffix(dst.suffix + ".bak")
    shutil.copy2(dst, bak)
    print(f"saved existing backup: {bak}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--storage-account", required=True)
    ap.add_argument("--container", default="backups")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    date_str = args.date.strip()

    root = Path(".")
    companies_dst = root / "data/db/companies.db.sqlite"
    outreach_dst = root / "data/db/outreach.db.sqlite"

    items = [
        DbItem(key="companies", dst_path=companies_dst, blob_prefix="companies"),
        DbItem(key="outreach", dst_path=outreach_dst, blob_prefix="outreach"),
    ]

    with tempfile.TemporaryDirectory(prefix="dbrestore_") as td:
        td_path = Path(td)

        for it in items:
            print(f"\n== {it.key} ==")
            blob_name = f"{it.blob_prefix}/{it.key}_{date_str}.sqlite.gz"
            tmp_gz = td_path / f"{it.key}_{date_str}.sqlite.gz"
            tmp_sqlite = td_path / f"{it.key}_{date_str}.sqlite"

            download_blob(
                account=args.storage_account,
                container=args.container,
                blob_name=blob_name,
                out_path=tmp_gz,
            )

            gunzip_file(tmp_gz, tmp_sqlite)

            it.dst_path.parent.mkdir(parents=True, exist_ok=True)
            backup_existing(it.dst_path)
            shutil.copy2(tmp_sqlite, it.dst_path)

            print(f"restored -> {it.dst_path}")

    print("\nRESTORE DONE ✅")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR ❌ {e}")
        sys.exit(1)
