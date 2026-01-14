import sqlite3
from pathlib import Path

DB_PATH = Path("data/outreach.db.sqlite")


def main():
    if not DB_PATH.exists():
        print(f"❌ Hittar inte DB: {DB_PATH}")
        return

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    print("== CHECK: sequences (campaign_templates) ==")

    cur.execute(
        """
        SELECT c.name AS campaign, ct.step, ct.variant, t.name AS template_name
        FROM campaign_templates ct
        JOIN campaigns c ON c.id = ct.campaign_id
        JOIN templates t ON t.id = ct.template_id
        ORDER BY c.name, ct.step, ct.variant
        """
    )
    rows = cur.fetchall()

    if not rows:
        print("❌ Inga campaign_templates hittades (seed_sequences saknas eller fel).")
    else:
        # summera per campaign
        per_campaign = {}
        for r in rows:
            camp = r["campaign"]
            per_campaign.setdefault(camp, set()).add(int(r["step"]))

        for camp, steps in sorted(per_campaign.items()):
            steps_sorted = sorted(list(steps))
            print(f"- {camp}: steps={steps_sorted} (count_templates={sum(1 for x in rows if x['campaign']==camp)})")

        print("\nExempelrader (max 25):")
        for r in rows[:25]:
            print(f"  {r['campaign']:<18} step={r['step']} var={r['variant']} -> {r['template_name']}")

    print("\n== CHECK: lead_campaigns (queue) ==")
    try:
        cur.execute(
            """
            SELECT c.name AS campaign, COUNT(*) AS n
            FROM lead_campaigns lc
            JOIN campaigns c ON c.id = lc.campaign_id
            GROUP BY c.name
            ORDER BY n DESC
            """
        )
        q = cur.fetchall()
        if not q:
            print("⚠️ Inga lead_campaigns ännu (du har inte seedat leads/queue).")
        else:
            for r in q:
                print(f"- {r['campaign']}: {r['n']} leads i kö")

        # visa några exempel
        cur.execute(
            """
            SELECT lc.id, lc.lead_id, c.name AS campaign, lc.current_step, lc.current_variant, lc.next_send_at, lc.stopped_reason
            FROM lead_campaigns lc
            JOIN campaigns c ON c.id = lc.campaign_id
            ORDER BY lc.id DESC
            LIMIT 10
            """
        )
        ex = cur.fetchall()
        if ex:
            print("\nSenaste 10 lead_campaigns:")
            for r in ex:
                print(
                    f"  id={r['id']} lead_id={r['lead_id']} camp={r['campaign']} "
                    f"step={r['current_step']} var={r['current_variant']} next={r['next_send_at']} stop={r['stopped_reason']}"
                )
    except sqlite3.OperationalError as e:
        print(f"❌ Kunde inte läsa lead_campaigns: {e}")

    con.close()
    print("\n✅ Klar")


if __name__ == "__main__":
    main()
