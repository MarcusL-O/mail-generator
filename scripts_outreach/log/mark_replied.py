#Körs manuellt när du får ett faktiskt svar i mejl eller via kontakt.
#Scriptet markerar leadet som replied i outreach-DB, vilket gör att uppföljningar stoppas och
#att svaret räknas med i statistiken (+1 svar).

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import json

DB_PATH = Path("data/outreach.db.sqlite")


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

    # 1) Uppdatera lead-status
    cur.execute(
        """
        UPDATE leads
        SET status = 'replied', updated_at = ?
        WHERE id = ?
        """,
        (ts, args.lead_id),
    )

    # 2) Stoppa vidare utskick i kampanjen
    cur.execute(
        """
        UPDATE lead_campaigns
        SET stopped_reason = 'replied', updated_at = ?
        WHERE lead_id = ? AND campaign_id = ?
        """,
        (ts, args.lead_id, args.campaign_id),
    )

    # 3) Logga event
    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, NULL, 'reply', ?, ?)
        """,
        (
            args.lead_id,
            args.campaign_id,
            json.dumps({"manual": True}, ensure_ascii=False),
            ts,
        ),
    )

    con.commit()
    con.close()

    print("✓ mark_replied")
    print(f"lead_id={args.lead_id} campaign_id={args.campaign_id}")


if __name__ == "__main__":
    main()
