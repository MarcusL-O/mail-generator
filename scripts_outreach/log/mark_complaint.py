"""
Vad: Markerar complaint via orgnr.
Gör: Stoppar alla aktiva lead_campaigns och sätter lead.status='do_not_contact'.
När: Kör när någon klagar/spam-markerar och du vill blocka för alltid.
"""

import argparse
import sqlite3

from scripts_outreach.send.shared.send_utils import connect_db, now_iso


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orgnr", required=True, help="Bolagets orgnr")
    args = ap.parse_args()

    con = connect_db()
    try:
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute("SELECT id FROM leads WHERE orgnr = ? LIMIT 1", (args.orgnr,))
        lead = cur.fetchone()
        if not lead:
            raise SystemExit(f"Hittar ingen lead med orgnr={args.orgnr}")

        lead_id = int(lead["id"])
        now = now_iso()

        cur.execute(
            "UPDATE leads SET status = 'do_not_contact', updated_at = ? WHERE id = ?",
            (now, lead_id),
        )

        cur.execute(
            """
            UPDATE lead_campaigns
            SET stopped_reason = 'complaint', updated_at = ?
            WHERE lead_id = ?
              AND stopped_reason IS NULL
            """,
            (now, lead_id),
        )

        con.commit()
        print("OK ✅ complaint")
        print(f"orgnr={args.orgnr} lead_id={lead_id} stopped_rows={cur.rowcount}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
