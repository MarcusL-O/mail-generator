#Sparar att ett mejl skickats (lead_id, template, timestamp, ev message_id).
#Används direkt efter lyckat send.

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/db/outreach.db.sqlite")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--message-id", type=int, required=True)
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Hämta message för att kunna skriva event med lead_id/campaign_id
    cur.execute(
        """
        SELECT id, lead_id, campaign_id, status
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

    sent_at = now_iso()

    # Markera som sent
    cur.execute(
        """
        UPDATE email_messages
        SET status = 'sent', sent_at = ?, updated_at = ?
        WHERE id = ?
        """,
        (sent_at, sent_at, args.message_id),
    )

    # Logga event "sent"
    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, ?, 'sent', ?, ?)
        """,
        (msg["lead_id"], msg["campaign_id"], msg["id"], "{}", sent_at),
    )

    con.commit()
    con.close()

    print("✓ log_email_sent")
    print(f"message_id={args.message_id}")
    print(f"sent_at={sent_at}")


if __name__ == "__main__":
    main()
