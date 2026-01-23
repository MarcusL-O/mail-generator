"""
Skickar valda templates till manuellt angivna email-adresser (testläge).

ANVÄNDNING:
python send_templates_to_list.py \
  --orgnr 556677-8899 \
  --templates email_customer_intro/A.html,email_customer_intro/B.html

KRAV:
- emails.txt ska ligga i samma mapp som detta script
- orgnr MÅSTE finnas i companies.db
- Använder outreach.db för templates + signature
- Skriver INGENTING till DB (endast render + send)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import argparse
import sqlite3
from pathlib import Path
from typing import Dict, Any

from outreach.render.render_email import render_email

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
    # TODO: ersätt med riktig SMTP senare
    print("=" * 60)
    print(f"TO: {to_email}")
    print(f"SUBJECT: {subject}")
    print(html[:500], "...")
    print("=" * 60)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orgnr", required=True)
    ap.add_argument("--templates", required=True, help="Komma-separerad lista av template-namn")
    args = ap.parse_args()

    emails = load_emails()
    company = load_company(args.orgnr)
    settings = load_settings()
    context = build_context(company, settings)

    templates = [t.strip() for t in args.templates.split(",") if t.strip()]
    if not templates:
        raise SystemExit("Inga templates angivna")

    for tpl in templates:
        subject, html, _txt = render_email(template_name=tpl, context=context)
        for email in emails:
            send_email(email, subject, html)


if __name__ == "__main__":
    main()
