# scripts_outreach/maintenance/reset_campaign_test.py
# Rensar testdata för en kampanj utan att påverka seeds eller andra kampanjer.
# Tar bort:
# - lead_campaigns för vald campaign
# - email_messages för vald campaign
# - leads som inte längre är kopplade till någon campaign
#
# Lämnar:
# - campaigns
# - sequences
# - templates
# - settings
# helt orörda.

import argparse
import sqlite3
from pathlib import Path

DB_PATH = Path("data/outreach.db.sqlite")


def main() -> None:
    ap = argparse.ArgumentParser(description="Reset testdata för en campaign")
    ap.add_argument("--campaign-name", required=True, help="ex: supplier_intro")
    ap.add_argument("--dry-run", action="store_true", help="Visa vad som tas bort utan att radera")
    args = ap.parse_args()

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT id FROM campaigns WHERE name = ? LIMIT 1", (args.campaign_name,))
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"Kampanj hittas inte: {args.campaign_name}")

    campaign_id = int(row[0])

    # Räkna först
    lc_count = cur.execute(
        "SELECT COUNT(*) FROM lead_campaigns WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()[0]

    em_count = cur.execute(
        "SELECT COUNT(*) FROM email_messages WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()[0]

    orphan_leads = cur.execute(
        """
        SELECT COUNT(*)
        FROM leads
        WHERE id NOT IN (SELECT DISTINCT lead_id FROM lead_campaigns)
        """
    ).fetchone()[0]

    print("=== RESET PREVIEW ===")
    print(f"campaign_name: {args.campaign_name}")
    print(f"lead_campaigns to delete: {lc_count}")
    print(f"email_messages to delete: {em_count}")
    print(f"orphan leads to delete: {orphan_leads}")
    print("=====================")

    if args.dry_run:
        print("DRY-RUN: inget raderat")
        con.close()
        return

    # Radera i rätt ordning
    cur.execute("DELETE FROM lead_campaigns WHERE campaign_id = ?", (campaign_id,))
    cur.execute("DELETE FROM email_messages WHERE campaign_id = ?", (campaign_id,))
    cur.execute(
        """
        DELETE FROM leads
        WHERE id NOT IN (SELECT DISTINCT lead_id FROM lead_campaigns)
        """
    )

    con.commit()
    con.close()

    print("✓ Reset klar – testdata borttagen säkert")


if __name__ == "__main__":
    main()
