# scripts_outreach/send/engine.py
# gemensam logik: select → render → send → log
# - välja vilka leads som är “due” för en kampanj
# - hitta rätt template för step+variant
# - kalla render_email()
# - i dry-run: bara logga email_messages (ingen SMTP)

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple

from scripts_outreach.render.render_email import render_email

DB_PATH = Path("data/db/outreach.db.sqlite")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_setting(con: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    cur = con.cursor()
    cur.execute("SELECT value FROM settings WHERE key = ? LIMIT 1", (key,))
    row = cur.fetchone()
    return str(row["value"]) if row else default


def _get_int_setting(con: sqlite3.Connection, key: str, default: int) -> int:
    v = _get_setting(con, key, None)
    if v is None:
        return default
    try:
        return int(str(v).strip())
    except ValueError:
        return default


def _is_dry_run(con: sqlite3.Connection) -> bool:
    v = (_get_setting(con, "dry_run", "1") or "1").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _parse_emails(value: Optional[str]) -> list[str]:
    if not value:
        return []
    s = value.strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            import json
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s]


def _choose_primary_email(emails_value: Optional[str]) -> Optional[str]:
    emails = _parse_emails(emails_value)
    return emails[0] if emails else None


def _upsert_email_message(
    con: sqlite3.Connection,
    *,
    lead_id: int,
    campaign_id: int,
    template_id: Optional[int],
    step: int,
    variant: Optional[str],
    to_email: str,
    from_email: str,
    subject_rendered: str,
    body_rendered: str,
    status: str,
    scheduled_at: Optional[str] = None,
    sent_at: Optional[str] = None,
    error: Optional[str] = None,
) -> int:
    ts = now_iso()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO email_messages
        (lead_id, campaign_id, template_id, step, variant, to_email, from_email,
         subject_rendered, body_rendered, status, scheduled_at, sent_at, error, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            campaign_id,
            template_id,
            step,
            variant,
            to_email,
            from_email,
            subject_rendered,
            body_rendered,
            status,
            scheduled_at,
            sent_at,
            error,
            ts,
            ts,
        ),
    )
    return int(cur.lastrowid)


def _pick_template_for_step(
    con: sqlite3.Connection, campaign_id: int, step: int, preferred_variant: Optional[str]
) -> Tuple[int, str]:
    cur = con.cursor()

    if preferred_variant:
        cur.execute(
            """
            SELECT template_id, variant
            FROM campaign_templates
            WHERE campaign_id = ? AND step = ? AND variant = ?
            LIMIT 1
            """,
            (campaign_id, step, preferred_variant),
        )
        row = cur.fetchone()
        if row:
            return int(row[0]), str(row[1])

    cur.execute(
        """
        SELECT template_id, variant
        FROM campaign_templates
        WHERE campaign_id = ? AND step = ?
        ORDER BY variant ASC
        LIMIT 1
        """,
        (campaign_id, step),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Ingen template kopplad för campaign_id={campaign_id} step={step}")
    return int(row[0]), str(row[1])


def _get_campaign_id(con: sqlite3.Connection, campaign_name: str) -> int:
    cur = con.cursor()
    cur.execute("SELECT id FROM campaigns WHERE name = ? LIMIT 1", (campaign_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Kampanj saknas i DB: {campaign_name} (har du kört seed_sequences.py?)")
    return int(row[0])


def _get_template_name(con: sqlite3.Connection, template_id: int) -> str:
    cur = con.cursor()
    cur.execute("SELECT name FROM templates WHERE id = ? LIMIT 1", (template_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Template saknas i DB: id={template_id}")
    return str(row[0])


def _build_context_from_lead(row: sqlite3.Row, con: sqlite3.Connection) -> dict:
    from_name = _get_setting(con, "from_name", "") or ""
    from_email = _get_setting(con, "from_email", "") or ""
    reply_to = _get_setting(con, "reply_to", "") or ""

    return {
        "orgnr": row["orgnr"],
        "company_name": row["company_name"] or "",
        "city": row["city"] or "",
        "sni_codes": row["sni_codes"] or "",
        "website": row["website"] or "",
        "emails": row["emails"] or "",
        "contact_name": "",
        "your_company": from_name,
        "your_contact_info": reply_to or from_email,
    }


def _setting_bool(con: sqlite3.Connection, key: str, default: str = "0") -> bool:
    v = (_get_setting(con, key, default) or default).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _build_order_by(con: sqlite3.Connection) -> str:
    prioritize_tier = _setting_bool(con, "prioritize_tier", "1")
    prioritize_score = _setting_bool(con, "prioritize_score", "1")

    parts = []
    if prioritize_tier:
        parts.append("(lc.tier IS NULL) ASC")
        parts.append("lc.tier ASC")
    if prioritize_score:
        parts.append("lc.score DESC")

    parts.append("lc.next_send_at ASC")
    parts.append("lc.id ASC")
    return " ORDER BY " + ", ".join(parts)


def run_engine(*, campaign_name: str, limit: int, advance_state: bool):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        dry = _is_dry_run(con)
        if not dry:
            raise SystemExit("dry_run=0 men SMTP/send är inte implementerat här ännu. Sätt dry_run=1.")

        campaign_id = _get_campaign_id(con, campaign_name)

        now = now_iso()
        cur = con.cursor()

        order_by_sql = _build_order_by(con)

        cur.execute(
            f"""
            SELECT
              lc.id               AS lead_campaign_id,
              lc.lead_id          AS lead_id,
              lc.current_step     AS current_step,
              lc.current_variant  AS current_variant,
              lc.next_send_at     AS next_send_at,
              lc.tier             AS tier,
              lc.score            AS score,

              l.orgnr             AS orgnr,
              l.company_name      AS company_name,
              l.city              AS city,
              l.sni_codes         AS sni_codes,
              l.website           AS website,
              l.emails            AS emails,
              l.status            AS lead_status
            FROM lead_campaigns lc
            JOIN leads l ON l.id = lc.lead_id
            WHERE lc.campaign_id = ?
              AND lc.stopped_reason IS NULL
              AND (lc.next_send_at IS NULL OR lc.next_send_at <= ?)
              AND l.status NOT IN ('do_not_contact')
            {order_by_sql}
            LIMIT ?
            """,
            (campaign_id, now, limit),
        )
        rows = cur.fetchall()

        if not rows:
            print("Inga leads är due ✅")
            return

        from_email = _get_setting(con, "from_email", None)
        if not from_email:
            raise ValueError("settings.from_email saknas (kör seed_settings.py och sätt OUTREACH_FROM_EMAIL).")

        created = 0
        skipped = 0

        for r in rows:
            to_email = _choose_primary_email(r["emails"])
            if not to_email:
                skipped += 1
                continue

            step = int(r["current_step"] or 1)
            preferred_variant = r["current_variant"]

            template_id, variant = _pick_template_for_step(con, campaign_id, step, preferred_variant)
            template_name = _get_template_name(con, template_id)

            context = _build_context_from_lead(r, con)
            subject, html, _txt = render_email(template_name=template_name, context=context)

            _upsert_email_message(
                con,
                lead_id=int(r["lead_id"]),
                campaign_id=campaign_id,
                template_id=template_id,
                step=step,
                variant=variant,
                to_email=to_email,
                from_email=from_email,
                subject_rendered=subject,
                body_rendered=html,
                status="queued",
                scheduled_at=now,
                sent_at=None,
                error=None,
            )

            created += 1

            if advance_state:
                min_h = _get_int_setting(con, "min_delay_between_steps_hours", 24)
                max_h = _get_int_setting(con, "max_delay_between_steps_hours", 72)
                delay_h = min_h if max_h <= min_h else min_h

                next_send = (_utc_now() + timedelta(hours=delay_h)).isoformat()

                cur2 = con.cursor()
                cur2.execute(
                    """
                    UPDATE lead_campaigns
                    SET current_step = ?, current_variant = ?, next_send_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (step + 1, variant, next_send, now, int(r["lead_campaign_id"])),
                )

            con.commit()

        print("DONE ✅")
        print(f"campaign={campaign_name}")
        print(f"due={len(rows)} created_email_messages={created} skipped_no_email={skipped}")
        print(f"advance_state={'yes' if advance_state else 'no'}")

    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign", required=True, help="ex: supplier_intro eller customer_intro")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--advance-state", action="store_true", help="Uppdatera lead_campaigns step/next_send_at")
    args = ap.parse_args()

    run_engine(campaign_name=args.campaign, limit=args.limit, advance_state=args.advance_state)


if __name__ == "__main__":
    main()
