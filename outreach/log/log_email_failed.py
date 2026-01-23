#Sparar misslyckat skick (lead_id, error, timestamp, retry_flag).
#Används när send kraschar eller SMTP/API nekar.

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
    ap.add_argument("--error", required=True, help="Felmeddelande (kort)")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

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

    # Markera som failed
    cur.execute(
        """
        UPDATE email_messages
        SET status = 'failed', error = ?, updated_at = ?
        WHERE id = ?
        """,
        (args.error, ts, args.message_id),
    )

    # Logga event "failed"
    cur.execute(
        """
        INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
        VALUES (?, ?, ?, 'failed', ?, ?)
        """,
        (
            msg["lead_id"],
            msg["campaign_id"],
            msg["id"],
            json.dumps({"error": args.error}, ensure_ascii=False),
            ts,
        ),
    )

    con.commit()
    con.close()

    print("✓ log_email_failed")
    print(f"message_id={args.message_id}")
    print(f"error={args.error}")


if __name__ == "__main__":
    main()
