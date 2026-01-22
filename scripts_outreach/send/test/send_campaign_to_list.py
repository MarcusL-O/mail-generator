"""
Skickar en hel kampanj (eller valda steps) till manuella email-adresser.

ANVÄNDNING:
python send_campaign_to_list.py \
  --campaign customer_intro \
  --orgnr 556677-8899 \
  --steps 1,2

eller:
python send_campaign_to_list.py \
  --campaign customer_intro \
  --orgnr 556677-8899 \
  --all
"""

import argparse
import sqlite3
from pathlib import Path
from typing import Dict, Any, List

from scripts_outreach.render.render_email import render_email

COMPANIES_DB = Path("data/db/companies.db.sqlite")
OUTREACH_DB = Path("data/db/outreach.db.sqlite")
EMAILS_FILE = Path(__file__).parent / "emails.txt"


def load_emails() -> list[str]:
    if not EMAILS_FILE.exists():
        raise SystemExit(f"Hittar inte {EMAILS_FILE}")
    emails = [l.strip() for l in EMAILS_FILE.read_text(encoding="utf-8").splitlines() if l.strip()]
    if not emails:
        raise SystemExit("emails.txt är tom")
    return emails


def load_company(orgnr: str) -> Dict[str, Any]:
    con = sqlite3.connect(COMPANIES_DB)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute("SELECT * FROM companies WHERE orgnr = ? LIMIT 1", (orgnr,))
        row = cur.fetchone()
        if not row:
            raise SystemExit(f"orgnr {orgnr} finns inte i companies.db")
        return dict(row)
    finally:
        con.close()


def load_settings_and_templates(
    campaign: str, steps: List[int] | None
) -> List[str]:
    con = sqlite3.connect(OUTREACH_DB)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute("SELECT id FROM campaigns WHERE name = ? LIMIT 1", (campaign,))
        row = cur.fetchone()
        if not row:
            raise SystemExit(f"Kampanj saknas: {campaign}")
        campaign_id = row["id"]

        sql = """
        SELECT t.name, ct.step
        FROM campaign_templates ct
        JOIN templates t ON t.id = ct.template_id
        WHERE ct.campaign_id = ?
        """
        params = [campaign_id]

        if steps:
            placeholders = ",".join(["?"] * len(steps))
            sql += f" AND ct.step IN ({placeholders})"
            params.extend(steps)

        sql += " ORDER BY ct.step ASC, ct.variant ASC"

        cur.execute(sql, params)
        rows = cur.fetchall()
        if not rows:
            raise SystemExit("Inga templates hittades för urvalet")

        return [r["name"] for r in rows]
    finally:
        con.close()


def load_settings() -> Dict[str, str]:
    con = sqlite3.connect(OUTREACH_DB)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute("SELECT key, value FROM settings")
        return {r["key"]: r["value"] for r in cur.fetchall()}
    finally:
        con.close()


def build_context(company: Dict[str, Any], settings: Dict[str, str]) -> Dict[str, Any]:
    return {
        "company_name": company.get("name", ""),
        "city": company.get("city", ""),
        "sni_codes": company.get("sni_codes", ""),
        "website": company.get("website", ""),
        "emails": company.get("emails", ""),
        "your_company": settings.get("from_name", ""),
        "your_contact_info": settings.get("reply_to") or settings.get("from_email", ""),
    }


def send_email(to_email: str, subject: str, html: str):
    # TODO: riktig SMTP senare
    print("=" * 60)
    print(f"TO: {to_email}")
    print(f"SUBJECT: {subject}")
    print(html[:500], "...")
    print("=" * 60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--orgnr", required=True)
    ap.add_argument("--steps", default="")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    if not args.all and not args.steps:
        raise SystemExit("Välj --steps eller --all")

    steps = None
    if not args.all:
        steps = [int(s.strip()) for s in args.steps.split(",") if s.strip()]

    emails = load_emails()
    company = load_company(args.orgnr)
    settings = load_settings()
    context = build_context(company, settings)

    templates = load_settings_and_templates(args.campaign, steps)

    for tpl in templates:
        subject, html, _txt = render_email(template_name=tpl, context=context)
        for email in emails:
            send_email(email, subject, html)


if __name__ == "__main__":
    main()
