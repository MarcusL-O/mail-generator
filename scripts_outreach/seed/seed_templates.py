import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path("data/outreach.db.sqlite")
TEMPLATES_ROOT = Path("templates")

def read_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")

def detect_audience_from_path(rel_parts):
    # customer_* → customer, supplier_* → supplier
    first = rel_parts[0]
    if first.startswith("customer"):
        return "customer"
    if first.startswith("supplier"):
        return "supplier"
    # fallback (ska i praktiken inte hända)
    return "customer"

def upsert_template(cur, name, audience, channel, body):
    now = datetime.now(timezone.utc).isoformat()

    cur.execute("SELECT id FROM templates WHERE name = ?", (name,))
    row = cur.fetchone()

    if row:
        cur.execute("""
            UPDATE templates
            SET body = ?, audience = ?, updated_at = ?
            WHERE name = ?
        """, (body, audience, now, name))
    else:
        cur.execute("""
            INSERT INTO templates
            (name, audience, channel, subject, body, version, is_active, created_at, updated_at)
            VALUES (?, ?, ?, '', ?, 1, 1, ?, ?)
        """, (name, audience, channel, body, now, now))

def seed():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # EMAIL TEMPLATES
    email_root = TEMPLATES_ROOT / "email"
    for path in email_root.rglob("*"):
        if path.suffix not in (".html", ".txt"):
            continue

        rel = path.relative_to(email_root)
        audience = detect_audience_from_path(rel.parts)
        name = f"email_{rel.as_posix()}"
        body = read_file(path)

        upsert_template(
            cur,
            name=name,
            audience=audience,
            channel="email",
            body=body
        )

    # SIGNATURES (neutral → sätts som customer i DB, används för båda)
    sig_root = TEMPLATES_ROOT / "signatures"
    for path in sig_root.rglob("*"):
        if path.suffix not in (".html", ".txt"):
            continue

        rel = path.relative_to(sig_root)
        name = f"signature_{rel.as_posix()}"
        body = read_file(path)

        upsert_template(
            cur,
            name=name,
            audience="customer",
            channel="signature",
            body=body
        )

    conn.commit()
    conn.close()
    print("✓ Templates & signatures seeded")

if __name__ == "__main__":
    seed()
