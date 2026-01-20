# scripts_outreach/send/supplier/send_contract.py
# Kommentar (svenska):
# - Hämtar leverantör från outreach.db (suppliers-tabellen)
# - Renderar kontrakt (md/text) + bygger PDF
# - Renderar mejl via render_email (template från DB + signatur)
# - I dry_run: loggar email_messages (queued) + skriver preview-filer i data/out/
# - I dry_run=0: försöker skicka via SMTP med PDF som attachment (kräver SMTP-settings)

import argparse
import sqlite3
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

from scripts_outreach.send.shared.send_utils import (
    connect_db,
    get_setting,
    is_dry_run,
    upsert_email_message,
    ensure_out_dir,
)
from scripts_outreach.render.render_email import render_email
from scripts_outreach.render.render_contract import render_contract
from scripts_outreach.render.render_contract_to_pdf import contract_text_to_pdf


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_campaign_id(con: sqlite3.Connection, campaign_name: str) -> int:
    cur = con.cursor()
    cur.execute("SELECT id FROM campaigns WHERE name = ? LIMIT 1", (campaign_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Kampanj saknas i DB: {campaign_name} (kör seed_sequences.py eller skapa campaign manuellt)")
    return int(row["id"])


def _get_supplier(con: sqlite3.Connection, supplier_id: int) -> sqlite3.Row:
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, name, sector, contact_name, contact_email, status, notes, created_at, updated_at
        FROM suppliers
        WHERE id = ?
        LIMIT 1
        """,
        (supplier_id,),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Leverantör saknas: suppliers.id={supplier_id}")
    return row


def _smtp_send_with_attachment(
    *,
    smtp_host: str,
    smtp_port: int,
    smtp_user: Optional[str],
    smtp_pass: Optional[str],
    smtp_tls: bool,
    from_email: str,
    to_email: str,
    subject: str,
    html_body: str,
    attachment_path: Path,
) -> None:
    # Kommentar (svenska): Minimal SMTP-sändning med PDF-attachment.
    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject

    # HTML-body
    msg.set_content("Din mailklient stödjer inte HTML.")
    msg.add_alternative(html_body, subtype="html")

    # Attachment
    data = attachment_path.read_bytes()
    msg.add_attachment(data, maintype="application", subtype="pdf", filename=attachment_path.name)

    if smtp_tls:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
            s.starttls()
            if smtp_user and smtp_pass:
                s.login(smtp_user, smtp_pass)
            s.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
            if smtp_user and smtp_pass:
                s.login(smtp_user, smtp_pass)
            s.send_message(msg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--supplier-id", type=int, required=True, help="suppliers.id i outreach.db")
    ap.add_argument("--campaign", default="supplier_intro", help="Vilken campaign_id som ska loggas på (default supplier_intro)")

    # Kommentar (svenska): Pris/villkor (förhandlingsbart)
    ap.add_argument("--price-per-meeting", required=True, help='Ex: "2500 kr"')
    ap.add_argument("--success-fee", required=True, help='Ex: "5%" eller "0 kr"')
    ap.add_argument("--payment-terms", default="10", help='Ex: "10" (dagar)')

    # Kommentar (svenska): Vilken email-template i DB som används för själva “skicka avtal”-mejlet
    ap.add_argument(
        "--email-template-name",
        default="email_supplier/supplier_contract/supplier_email_contract.html",
        help="Template.name i DB (seed_templates skapar detta från fil-path)",
    )

    # Kommentar (svenska): Vart preview ska skrivas vid dry-run
    ap.add_argument("--out-prefix", default="contract_send", help="prefix för preview-filer i data/out/")

    args = ap.parse_args()

    con = connect_db()
    try:
        dry = is_dry_run(con)

        campaign_id = _get_campaign_id(con, args.campaign)
        supplier = _get_supplier(con, args.supplier_id)

        # Kommentar (svenska): Vi använder contact_email om den finns, annars stoppar vi.
        to_email = (supplier["contact_email"] or "").strip()
        if not to_email:
            raise ValueError("Leverantören saknar contact_email i suppliers-tabellen (behövs för att skicka avtal).")

        from_email = (get_setting(con, "from_email", "") or "").strip()
        if not from_email:
            raise ValueError("settings.from_email saknas (kör seed_settings.py).")

        from_name = (get_setting(con, "from_name", "") or "").strip()

        # Kommentar (svenska): Juridiska uppgifter för ditt bolag (lägg i settings om du vill)
        your_company_legal = (get_setting(con, "your_company_legal_name", from_name) or from_name).strip()
        your_orgnr = (get_setting(con, "your_company_orgnr", "") or "").strip()

        # 1) Rendera kontrakt-text
        contract_context = {
            "YOUR_COMPANY_NAME": your_company_legal,
            "YOUR_ORGNR": your_orgnr,
            "SUPPLIER_COMPANY_NAME": supplier["name"],
            "SUPPLIER_ORGNR": "",  # Kommentar (svenska): du sa att du vill ha orgnr – lägg det här när du har fältet i DB
            "PRICE_PER_MEETING": args.price_per_meeting,
            "SUCCESS_FEE": args.success_fee,
            "PAYMENT_TERMS": args.payment_terms,
        }
        contract_text = render_contract(context=contract_context)

        # 2) Bygg PDF
        pdf_path = contract_text_to_pdf(
            contract_text=contract_text,
            supplier_orgnr=(contract_context["SUPPLIER_ORGNR"] or f"supplier_{supplier['id']}"),
        )

        # 3) Rendera mejl från DB-template + signatur
        email_context = {
            "supplier_company_name": supplier["name"],
            "your_company": your_company_legal,
            "your_contact_info": (get_setting(con, "reply_to", from_email) or from_email).strip(),
        }
        subject, html, txt = render_email(template_name=args.email_template_name, context=email_context)

        # 4) Logga email_messages (queued i dry-run, sent om vi skickar på riktigt)
        status = "queued" if dry else "sent"
        scheduled_at = now_iso()

        # Kommentar (svenska): template_id är ok att lämna NULL om du inte vill slå upp den här
        message_id = upsert_email_message(
            con,
            lead_id=0,  # Kommentar (svenska): detta är inte lead-driven. Du kan lägga egen tabell senare.
            campaign_id=campaign_id,
            template_id=None,
            step=0,
            variant=None,
            to_email=to_email,
            from_email=from_email,
            subject_rendered=subject,
            body_rendered=html,
            status=status,
            scheduled_at=scheduled_at,
            sent_at=None if dry else scheduled_at,
            error=None,
        )
        con.commit()

        out_dir = ensure_out_dir()

        # 5) Dry-run: skriv preview så du kan öppna i browser
        if dry:
            html_path = out_dir / f"{args.out_prefix}_supplier{supplier['id']}.html"
            txt_path = out_dir / f"{args.out_prefix}_supplier{supplier['id']}.txt"
            html_path.write_text(html, encoding="utf-8")
            txt_path.write_text(txt, encoding="utf-8")

            print("DRY RUN ✅")
            print(f"message_id={message_id}")
            print(f"to={to_email}")
            print(f"pdf={pdf_path}")
            print(f"preview_html={html_path}")
            print(f"preview_txt={txt_path}")
            return

        # 6) Real send (dry_run=0): kräver SMTP-settings
        smtp_host = (get_setting(con, "smtp_host", "") or "").strip()
        smtp_port = int((get_setting(con, "smtp_port", "587") or "587").strip())
        smtp_user = (get_setting(con, "smtp_user", None) or None)
        smtp_pass = (get_setting(con, "smtp_pass", None) or None)
        smtp_tls = ((get_setting(con, "smtp_tls", "1") or "1").strip().lower() in ("1", "true", "yes", "y", "on"))

        if not smtp_host:
            raise ValueError("dry_run=0 men smtp_host saknas i settings. Lägg SMTP i settings först.")

        _smtp_send_with_attachment(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_pass=smtp_pass,
            smtp_tls=smtp_tls,
            from_email=from_email,
            to_email=to_email,
            subject=subject,
            html_body=html,
            attachment_path=pdf_path,
        )

        print("SENT ✅")
        print(f"message_id={message_id}")
        print(f"to={to_email}")
        print(f"pdf={pdf_path}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
