#Bygger färdigt mejl (subject + html + txt).
#Sätter ihop template + data + signatur + snippets.import re
#Hämtar email-template från outreach.db (templates-tabellen)
#Ersätter alla {{ placeholders }} med värden från context
#Hämtar aktiv signature (HTML + TXT) från settings
#Appendar signaturen längst ner i mejlet
#Returnerar 3 färdiga strängar:
#subject (renderad)
#html-body
#txt-body

import sqlite3
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

DB_PATH = Path("data/outreach.db.sqlite")

# Kommentar (svenska): Stödjer {{key}} och {{key | default:""}} (som dina templates)
_PLACEHOLDER_RE = re.compile(
    r"\{\{\s*(?P<key>[a-zA-Z0-9_]+)\s*(\|\s*default\s*:\s*\"(?P<default>[^\"]*)\")?\s*\}\}"
)


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _get_setting(con: sqlite3.Connection, key: str) -> Optional[str]:
    cur = con.cursor()
    cur.execute("SELECT value FROM settings WHERE key = ? LIMIT 1", (key,))
    row = cur.fetchone()
    return str(row["value"]) if row else None


def _get_template(con: sqlite3.Connection, name: str, channel: str) -> sqlite3.Row:
    cur = con.cursor()
    cur.execute(
        """
        SELECT name, channel, subject, body
        FROM templates
        WHERE name = ? AND channel = ?
        LIMIT 1
        """,
        (name, channel),
    )
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Template not found: name='{name}', channel='{channel}'")
    return row


def _render_placeholders(text: str, context: Dict[str, Any]) -> str:
    def repl(m: re.Match) -> str:
        key = m.group("key")
        default = m.group("default")
        if key in context and context[key] is not None:
            return str(context[key])
        return default if default is not None else ""

    return _PLACEHOLDER_RE.sub(repl, text)


def _append_signature(body: str, signature: str, channel: str) -> str:
    # Kommentar (svenska): Enkel append. HTML får <br><br>, text får \n\n.
    if not signature:
        return body

    if channel == "html":
        sep = "<br><br>"
        return f"{body}{sep}{signature}"
    else:
        sep = "\n\n"
        return f"{body}{sep}{signature}"


def render_email(
    *,
    template_name: str,
    context: Dict[str, Any],
    signature_html_setting_key: str = "active_signature_html",
    signature_txt_setting_key: str = "active_signature_txt",
) -> Tuple[str, str, str]:
    """
    Renderar en template från DB + injicerar placeholders + appendar aktiv signature.

    Input:
      - template_name: ex "email_customer_intro/A.html"
      - context: dict med keys som matchar {{key}} i template

    Output:
      (subject_rendered, html_rendered, txt_rendered)
    """
    con = _connect()
    try:
        # Kommentar (svenska): Hämta email-template (subject + body)
        t = _get_template(con, template_name, "email")
        subject_raw = t["subject"] or ""
        body_raw = t["body"] or ""

        # Kommentar (svenska): Render placeholders
        subject_rendered = _render_placeholders(subject_raw, context)
        body_rendered = _render_placeholders(body_raw, context)

        # Kommentar (svenska): Hämta aktiv signature (html/txt) från settings
        sig_html_name = _get_setting(con, signature_html_setting_key)
        sig_txt_name = _get_setting(con, signature_txt_setting_key)

        sig_html = ""
        sig_txt = ""

        if sig_html_name:
            sig_html_row = _get_template(con, sig_html_name, "signature")
            sig_html = sig_html_row["body"] or ""

        if sig_txt_name:
            sig_txt_row = _get_template(con, sig_txt_name, "signature")
            sig_txt = sig_txt_row["body"] or ""

        # Kommentar (svenska): Bygg HTML + TXT separat.
        # Om din template är .html eller .txt spelar ingen roll här – vi returnerar alltid båda.
        html_rendered = _append_signature(body_rendered, sig_html, "html")
        txt_rendered = _append_signature(body_rendered, sig_txt, "txt")

        return subject_rendered, html_rendered, txt_rendered
    finally:
        con.close()


if __name__ == "__main__":
    # Kommentar (svenska): Minimal manuell testkörning
    # Byt template_name till en som finns i din DB.
    demo_template = "email_customer_intro/A.html"
    demo_context = {
        "company_name": "Demo AB",
        "your_company": "Din Firma",
        "city": "Göteborg",
        "industry_or_service": "IT-konsult",
        "your_contact_info": "marcus@example.com",
    }

    try:
        subj, html, txt = render_email(template_name=demo_template, context=demo_context)
        print("SUBJECT:\n", subj)
        print("\nHTML:\n", html[:500], "...\n")
        print("\nTXT:\n", txt[:500], "...\n")
    except Exception as e:
        print("Render failed:", e)
