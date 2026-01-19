# Vad: Manuell kill-switch via orgnr.
# Gör: Stoppar alla aktiva lead_campaigns (stopped_reason om NULL)
#      och sätter lead.status='do_not_contact'.
# När: Kör när någon ber oss sluta eller leaden är fel målgrupp.

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
    ap.add_argument("--orgnr", required=True, help="Bolagets orgnr")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    cur.execute("SELECT id FROM leads WHERE orgnr = ? LIMIT 1", (args.orgnr,))
    lead = cur.fetchone()
    if not lead:
        con.close()
        raise SystemExit(f"Hittar ingen lead med orgnr={args.orgnr}")

    lead_id = int(lead["id"])
    ts = now_iso()

    cur.execute(
        "UPDATE leads SET status = 'do_not_contact', updated_at = ? WHERE id = ?",
        (ts, lead_id),
    )

    cur.execute(
        """
        UPDATE lead_campaigns
        SET stopped_reason = 'manual_stop', updated_at = ?
        WHERE lead_id = ?
          AND stopped_reason IS NULL
        """,
        (ts, lead_id),
    )
    stopped_rows = cur.rowcount

    # Logga event "manual_stop" (om din events CHECK tillåter det)
    try:
        cur.execute(
            """
            INSERT INTO events (lead_id, campaign_id, message_id, type, meta, created_at)
            VALUES (?, NULL, NULL, 'manual_stop', ?, ?)
            """,
            (lead_id, json.dumps({"manual": True}, ensure_ascii=False), ts),
        )
    except Exception:
        pass

    con.commit()
    con.close()

    print("OK ✅ manual_stop")
    print(f"orgnr={args.orgnr} lead_id={lead_id} stopped_rows={stopped_rows}")


if __name__ == "__main__":
    main()
