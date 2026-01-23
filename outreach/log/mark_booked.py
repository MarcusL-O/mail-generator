#Körs manuellt när ett möte har bokats mellan kund och leverantör.
#Markerar leadet som booked med timestamp i DB, vilket ger +1 bokat möte i statistiken
#och används som huvud-KPI för outreach.

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
    ap.add_argument("--note", default="", help="Valfri anteckning om mötet")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ts = now_iso()

    # 1) Uppdatera lead-status
    cur.execute(
        """
        UPDATE leads
        SET status = 'booked', updated_at = ?
        WHERE id = ?
        """,
        (ts, args.lead_id),
    )

    # 2) Stoppa vidare utskick
    cur.execute(
        """
        UPDATE lead_campaigns
        SET stopped_reason = 'booked', updated_at = ?
        WHERE lead_id = ? AND campaign_id = ?
        """,
        (ts, args.lead_id, args.campaign_id),
    )

    # 3) Logga event (detta är +1 bokning i statistiken)
    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, NULL, 'booked', ?, ?)
        """,
        (
            args.lead_id,
            args.campaign_id,
            json.dumps(
                {
                    "manual": True,
                    "note": args.note,
                },
                ensure_ascii=False,
            ),
            ts,
        ),
    )

    con.commit()
    con.close()

    print("✓ mark_booked")
    print(f"lead_id={args.lead_id} campaign_id={args.campaign_id}")
    if args.note:
        print(f"note={args.note}")


if __name__ == "__main__":
    main()
