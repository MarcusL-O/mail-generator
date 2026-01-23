#Sätter lead-status = contacted + contacted_at.
#Används efter första utskicket för att inte dubbelkontakta.

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import json

DB_PATH = Path("data/db/outreach.db.sqlite")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lead-id", type=int, required=True)
    ap.add_argument("--campaign-id", type=int, required=True)
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ts = now_iso()

    # Uppdatera lead-status
    cur.execute(
        """
        UPDATE leads
        SET status = 'contacted', updated_at = ?
        WHERE id = ?
        """,
        (ts, args.lead_id),
    )

    # Logga event
    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, NULL, 'sent', ?, ?)
        """,
        (args.lead_id, args.campaign_id, json.dumps({"manual": True}), ts),
    )

    con.commit()
    con.close()

    print("✓ mark_contacted")
    print(f"lead_id={args.lead_id} campaign_id={args.campaign_id}")


if __name__ == "__main__":
    main()
