#Definierar utskicksflöden (intro → followup).
#Används av send-scripts.
#Läser alla email-templates från DB
#Skapar kampanjer (supplier_intro, customer_intro)
#Avgör:
#vilket steg (intro, FU1, FU2, FU3)
#vilken variant (A/B/C)
#Kopplar template → kampanj + steg + variant i DB
#Är idempotent (kan köras flera gånger)
#Detta gör att send-scriptet senare vet exakt vilket mejl som ska skickas när.


import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import re

DB_PATH = Path("data/outreach.db.sqlite")

# Vilka kampanjer vi vill ha i MVP
# Kommentar (svenska): Vi kör en kampanj per audience som innehåller intro + followups i steg.
AUDIENCES = ("supplier", "customer")

VARIANTS = ("A", "B", "C")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_campaign(cur, name: str, audience: str) -> int:
    """
    Skapar eller uppdaterar campaign och returnerar campaign_id
    """
    ts = now_iso()
    cur.execute("SELECT id FROM campaigns WHERE name = ?", (name,))
    row = cur.fetchone()
    if row:
        campaign_id = row[0]
        cur.execute(
            """
            UPDATE campaigns
            SET audience = ?, updated_at = ?
            WHERE id = ?
            """,
            (audience, ts, campaign_id),
        )
        return campaign_id

    cur.execute(
        """
        INSERT INTO campaigns (name, audience, status, notes, created_at, updated_at)
        VALUES (?, ?, 'draft', NULL, ?, ?)
        """,
        (name, audience, ts, ts),
    )
    return cur.lastrowid


def get_template_id(cur, template_name: str) -> int | None:
    cur.execute(
        "SELECT id FROM templates WHERE name = ? AND channel = 'email' LIMIT 1",
        (template_name,),
    )
    row = cur.fetchone()
    return row[0] if row else None


def upsert_campaign_template(cur, campaign_id: int, step: int, variant: str, template_id: int):
    """
    Idempotent upsert mot unique(campaign_id, step, variant)
    """
    cur.execute(
        """
        INSERT INTO campaign_templates (campaign_id, step, variant, template_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(campaign_id, step, variant)
        DO UPDATE SET template_id = excluded.template_id
        """,
        (campaign_id, step, variant, template_id),
    )


def detect_variant_from_filename(filename: str) -> str:
    """
    Försöker hitta variant A/B/C från filnamn.
    Ex: A.html, B.txt, SUP_Intro_Whatever_A.html → A
    """
    base = Path(filename).stem.upper()

    # Exakt A/B/C
    if base in VARIANTS:
        return base

    # Sök _A / -A / space A etc
    m = re.search(r"(^|[^A-Z])(A|B|C)($|[^A-Z])", base)
    if m:
        return m.group(2)

    # Fallback
    return "A"


def detect_step_from_path(relpath: str) -> int | None:
    """
    Relpath är t.ex: customer_intro/A.html eller supplier_followup/FU2.html
    """
    p = relpath.lower()

    # Intro = step 1
    if "_intro/" in p:
        return 1

    # Followups: FU1/FU2/FU3 i filnamn eller path
    if "_followup/" in p:
        if "fu1" in p:
            return 2
        if "fu2" in p:
            return 3
        if "fu3" in p:
            return 4
        # Om followup saknar FU-nummer: lägg den som step 2
        return 2

    # Review ingår inte i MVP (skippa)
    if "_review/" in p:
        return None

    # Okänt: skippa
    return None


def seed():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Kommentar (svenska): läs alla email-templates som seed_templates.py har lagt in.
    cur.execute(
        """
        SELECT id, name, audience
        FROM templates
        WHERE channel = 'email'
        """
    )
    rows = cur.fetchall()

    # Skapa kampanjer upfront
    campaign_ids = {}
    for audience in AUDIENCES:
        campaign_name = f"{audience}_intro"
        campaign_ids[audience] = upsert_campaign(cur, campaign_name, audience)

    linked = 0
    skipped = 0

    for template_id, template_name, audience in rows:
        # Vi stödjer bara supplier/customer i MVP
        if audience not in AUDIENCES:
            skipped += 1
            continue

        # template_name = "email_<relpath>"
        if not template_name.startswith("email_"):
            skipped += 1
            continue

        relpath = template_name[len("email_") :]  # ex: "customer_intro/A.html"
        step = detect_step_from_path(relpath)
        if step is None:
            skipped += 1
            continue

        variant = detect_variant_from_filename(relpath)

        # Kampanj: supplier_intro / customer_intro (MVP)
        campaign_id = campaign_ids[audience]

        # Kommentar (svenska): koppla template till campaign+step+variant
        upsert_campaign_template(cur, campaign_id, step, variant, template_id)
        linked += 1

    conn.commit()
    conn.close()

    print("✓ Sequences seeded")
    print(f"linked={linked} skipped={skipped}")


if __name__ == "__main__":
    seed()
