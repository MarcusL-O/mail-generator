import json
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/mail_generator_db.sqlite")
IN_PATH = Path("data/out/goteborg_companies_filtered.ndjson")

LIMIT = None
COMMIT_EVERY = 5000

def pick_fields(obj: dict) -> tuple[str, str]:
    orgnr = str(obj.get("orgnr") or "").strip()
    name = str(obj.get("name") or "").strip()
    return orgnr, name

def main():
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    scanned = 0
    upserted = 0

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        cur = conn.cursor()

        with IN_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue

                scanned += 1
                if LIMIT is not None and scanned > LIMIT:
                    break

                obj = json.loads(line)
                orgnr, name = pick_fields(obj)

                if not orgnr or not name:
                    continue

                cur.execute(
                    """
                    INSERT INTO companies (orgnr, name, last_seen_at, updated_at)
                    VALUES (?, ?, ?, datetime('now'))
                    ON CONFLICT(orgnr) DO UPDATE SET
                        name = excluded.name,
                        last_seen_at = excluded.last_seen_at,
                        updated_at = datetime('now')
                    """,
                    (orgnr, name, now),
                )
                upserted += 1

                if upserted % COMMIT_EVERY == 0:
                    conn.commit()
                    print(f"[{scanned:,}] upserted={upserted:,}")

        conn.commit()

    print("KLART ✅")
    print(f"Scannat:  {scanned:,}")
    print(f"Upserted: {upserted:,}")

if __name__ == "__main__":
    main()
