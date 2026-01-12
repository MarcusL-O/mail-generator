#Kollar iflal innehåll i ALLA shards ocj njson flyttas till db 

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/companies.db.sqlite")
TABLE = "companies"

def _parse_sqlite_dt(s: str):
    # Förväntat format från sqlite datetime('now'): "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)  # sqlite 'now' = UTC
    except Exception:
        return None

def _minutes_ago(dt_utc: datetime):
    now_utc = datetime.now(timezone.utc)
    delta = now_utc - dt_utc
    return int(delta.total_seconds() // 60)

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    total = conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]

    websites = conn.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE website IS NOT NULL AND TRIM(website) != ''
    """).fetchone()[0]

    emails = conn.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE emails IS NOT NULL AND TRIM(emails) != ''
    """).fetchone()[0]

    latest_updated_at = conn.execute(f"""
        SELECT MAX(updated_at) FROM {TABLE}
        WHERE updated_at IS NOT NULL AND TRIM(updated_at) != ''
    """).fetchone()[0]

    print("=== DB STATUS ===")
    print(f"Totalt bolag: {total}")
    print(f"Hemsidor (website): {websites}")
    print(f"Mejladresser (emails): {emails}")

    print("\n=== SENASTE UPPDATERING ===")
    if not latest_updated_at:
        print("updated_at: (saknas / inga uppdateringar)")
    else:
        dt = _parse_sqlite_dt(latest_updated_at)
        if not dt:
            print(f"updated_at: {latest_updated_at} (kunde inte tolka format)")
        else:
            mins = _minutes_ago(dt)
            print(f"updated_at (UTC): {latest_updated_at}")
            print(f"Senast uppdaterad: {mins} min sedan")

    conn.close()

if __name__ == "__main__":
    main()
