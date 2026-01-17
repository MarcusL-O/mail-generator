# normalize_dates.py
# Kommentar: Normaliserar datumformat i companies till ISO-UTC med "Z".
# Kommentar: Rör inte NULL. Om ett värde inte kan parse:as lämnas det orört och loggas som "skipped".

import sqlite3
from datetime import datetime, timezone

DB_PATH = "data/companies.db.sqlite"
TABLE = "companies"

# Kommentar: Lägg alla datumkolumner här som du vill normalisera
DATE_COLS = [
    "website_checked_at",
    "emails_checked_at",
    "last_seen_at",
    "created_at",
    "updated_at",
    "started_at",
    "site_review_checked_at",
    "hiring_checked_at",
    "tech_checked_at",
    # nya SCB när de finns:
    "scb_checked_at",
    "scb_next_check_at",
]

BATCH_SIZE = 1000

def to_iso_z(value: str) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # 1) Standard: ...Z
    # datetime.fromisoformat stödjer inte "Z" direkt, så vi byter till +00:00
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s[:-1] + "+00:00")
            return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    # 2) ISO med offset, ex 2026-01-10T15:52:58+00:00
    try:
        if "T" in s and ("+" in s[-6:] or "-" in s[-6:]):
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    # 3) "YYYY-MM-DD HH:MM:SS" (SQLite-klassiker)
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    # 4) "YYYY-MM-DDTHH:MM:SS" utan tz
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    return "SKIP"

def main():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Hämta rowid + alla date cols
    cols_sql = ", ".join([f'"{c}"' for c in DATE_COLS])
    cur.execute(f'SELECT rowid, {cols_sql} FROM {TABLE}')
    rows = cur.fetchall()

    changed = 0
    skipped = 0
    total_vals = 0

    for i, row in enumerate(rows, start=1):
        rowid = row["rowid"]
        updates = {}
        for c in DATE_COLS:
            v = row[c]
            if v is None:
                continue
            total_vals += 1
            new_v = to_iso_z(v)
            if new_v == "SKIP":
                skipped += 1
                continue
            if new_v is None:
                continue
            if str(v).strip() != new_v:
                updates[c] = new_v

        if updates:
            set_sql = ", ".join([f'"{k}"=?' for k in updates.keys()])
            params = list(updates.values()) + [rowid]
            cur.execute(f'UPDATE {TABLE} SET {set_sql} WHERE rowid=?', params)
            changed += len(updates)

        if i % BATCH_SIZE == 0:
            con.commit()
            print(f"progress: rows={i} changed_values={changed} skipped_values={skipped}")

    con.commit()
    con.close()

    print("DONE ✅")
    print(f"rows={len(rows)} total_nonnull_values={total_vals} changed_values={changed} skipped_values={skipped}")
    print("Kommentar: 'skipped' betyder att värdet inte gick att parse:a och lämnades orört.")

if __name__ == "__main__":
    main()
