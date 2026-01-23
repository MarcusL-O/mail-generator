#Körs manuellt när en faktisk affär blivit av efter mötet.
#Markerar leadet som deal i DB och räknas som +1 affär i statistiken
#(kan även användas för intäktsuppföljning senare).

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
    ap.add_argument("--amount", type=float, default=None, help="Affärsvärde (valfritt)")
    ap.add_argument("--note", default="", help="Valfri anteckning")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ts = now_iso()

    # 1) Uppdatera lead-status
    cur.execute(
        """
        UPDATE leads
        SET status = 'won', updated_at = ?
        WHERE id = ?
        """,
        (ts, args.lead_id),
    )

    # 2) Stoppa vidare utskick
    cur.execute(
        """
        UPDATE lead_campaigns
        SET stopped_reason = 'won', updated_at = ?
        WHERE lead_id = ? AND campaign_id = ?
        """,
        (ts, args.lead_id, args.campaign_id),
    )

    # 3) Logga event (detta är +1 affär)
    meta = {"manual": True}
    if args.amount is not None:
        meta["amount"] = args.amount
    if args.note:
        meta["note"] = args.note

    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, NULL, 'won', ?, ?)
        """,
        (
            args.lead_id,
            args.campaign_id,
            json.dumps(meta, ensure_ascii=False),
            ts,
        ),
    )

    con.commit()
    con.close()

    print("✓ mark_deal")
    print(f"lead_id={args.lead_id} campaign_id={args.campaign_id}")
    if args.amount is not None:
        print(f"amount={args.amount}")
    if args.note:
        print(f"note={args.note}")


if __name__ == "__main__":
    main()
