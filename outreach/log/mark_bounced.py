#Sätter lead-status = bounced/invalid + bounced_at + ev reason.
#Stoppar framtida utskick till den adressen.

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
    ap.add_argument("--message-id", type=int, required=True)
    ap.add_argument("--reason", default="bounce", help="Valfri orsak (t.ex. hard_bounce)")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Hämta message + lead/campaign
    cur.execute(
        """
        SELECT id, lead_id, campaign_id
        FROM email_messages
        WHERE id = ?
        LIMIT 1
        """,
        (args.message_id,),
    )
    msg = cur.fetchone()
    if not msg:
        con.close()
        raise SystemExit(f"Hittar inget email_messages.id={args.message_id}")

    ts = now_iso()

    # 1) Markera mejlet som bounced
    cur.execute(
        """
        UPDATE email_messages
        SET status = 'bounced', error = ?, updated_at = ?
        WHERE id = ?
        """,
        (args.reason, ts, args.message_id),
    )

    # 2) Stoppa vidare utskick (och skydda deliverability)
    cur.execute(
        """
        UPDATE lead_campaigns
        SET stopped_reason = 'bounced', updated_at = ?
        WHERE lead_id = ? AND campaign_id = ?
        """,
        (ts, msg["lead_id"], msg["campaign_id"]),
    )

    # 3) Sätt lead som do_not_contact
    cur.execute(
        """
        UPDATE leads
        SET status = 'do_not_contact', updated_at = ?
        WHERE id = ?
        """,
        (ts, msg["lead_id"]),
    )

    # 4) Logga event
    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, ?, 'bounce', ?, ?)
        """,
        (
            msg["lead_id"],
            msg["campaign_id"],
            msg["id"],
            json.dumps({"reason": args.reason}, ensure_ascii=False),
            ts,
        ),
    )

    con.commit()
    con.close()

    print("✓ mark_bounced")
    print(f"message_id={args.message_id} reason={args.reason}")


if __name__ == "__main__":
    main()
