# Skapar outreach.db och alla tabeller (templates, campaigns, leads, mail_events, stats, blacklist).
# Körs en gång eller vid schema-ändring.

from __future__ import annotations

import sqlite3
from pathlib import Path
from datetime import datetime, timezone


DB_PATH = Path("data/outreach.db.sqlite")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

-- =========================
-- templates
-- =========================
CREATE TABLE IF NOT EXISTS templates (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  name        TEXT    NOT NULL,
  audience    TEXT    NOT NULL CHECK (audience IN ('supplier','customer')),
  channel     TEXT    NOT NULL DEFAULT 'email',
  subject     TEXT    NOT NULL,
  body        TEXT    NOT NULL,
  version     INTEGER NOT NULL DEFAULT 1,
  is_active   INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0,1)),
  created_at  TEXT    NOT NULL,
  updated_at  TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_templates_name_version
  ON templates(name, version);

CREATE INDEX IF NOT EXISTS ix_templates_audience_active
  ON templates(audience, is_active);

-- =========================
-- campaigns
-- =========================
CREATE TABLE IF NOT EXISTS campaigns (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  name        TEXT    NOT NULL,
  audience    TEXT    NOT NULL CHECK (audience IN ('supplier','customer')),
  status      TEXT    NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','running','paused','done')),
  notes       TEXT,
  created_at  TEXT    NOT NULL,
  updated_at  TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_campaigns_name
  ON campaigns(name);

CREATE INDEX IF NOT EXISTS ix_campaigns_status
  ON campaigns(status);

-- =========================
-- campaign_templates
-- =========================
CREATE TABLE IF NOT EXISTS campaign_templates (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  campaign_id INTEGER NOT NULL,
  step        INTEGER NOT NULL CHECK (step >= 1),
  variant     TEXT    NOT NULL CHECK (variant IN ('A','B','C')),
  template_id INTEGER NOT NULL,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
  FOREIGN KEY (template_id) REFERENCES templates(id) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_campaign_templates_campaign_step_variant
  ON campaign_templates(campaign_id, step, variant);

CREATE INDEX IF NOT EXISTS ix_campaign_templates_campaign
  ON campaign_templates(campaign_id);

CREATE INDEX IF NOT EXISTS ix_campaign_templates_template
  ON campaign_templates(template_id);

-- =========================
-- leads (snapshot från companies)
-- =========================
CREATE TABLE IF NOT EXISTS leads (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  orgnr        TEXT    NOT NULL,
  company_name TEXT,
  city         TEXT,
  sni_codes    TEXT,
  website      TEXT,
  emails       TEXT,
  lead_type    TEXT    NOT NULL CHECK (lead_type IN ('supplier','customer')),
  status       TEXT    NOT NULL DEFAULT 'new'
               CHECK (status IN ('new','contacted','replied','interested','booked','won','lost','do_not_contact')),
  owner        TEXT,
  created_at   TEXT    NOT NULL,
  updated_at   TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_leads_orgnr
  ON leads(orgnr);

CREATE INDEX IF NOT EXISTS ix_leads_status
  ON leads(status);

CREATE INDEX IF NOT EXISTS ix_leads_type
  ON leads(lead_type);

CREATE INDEX IF NOT EXISTS ix_leads_city
  ON leads(city);

-- =========================
-- lead_campaigns (state per lead i kampanj)
-- =========================
CREATE TABLE IF NOT EXISTS lead_campaigns (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  lead_id         INTEGER NOT NULL,
  campaign_id     INTEGER NOT NULL,
  current_step    INTEGER NOT NULL DEFAULT 1,
  current_variant TEXT,
  next_send_at    TEXT,
  stopped_reason  TEXT,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_lead_campaigns_lead_campaign
  ON lead_campaigns(lead_id, campaign_id);

CREATE INDEX IF NOT EXISTS ix_lead_campaigns_campaign
  ON lead_campaigns(campaign_id);

CREATE INDEX IF NOT EXISTS ix_lead_campaigns_next_send
  ON lead_campaigns(next_send_at);

-- =========================
-- email_messages (queue + historik)
-- =========================
CREATE TABLE IF NOT EXISTS email_messages (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  lead_id           INTEGER NOT NULL,
  campaign_id       INTEGER NOT NULL,
  template_id       INTEGER,
  step              INTEGER NOT NULL DEFAULT 1,
  variant           TEXT,
  to_email          TEXT NOT NULL,
  from_email        TEXT NOT NULL,
  subject_rendered  TEXT NOT NULL,
  body_rendered     TEXT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued','sent','bounced','failed','replied')),
  scheduled_at      TEXT,
  sent_at           TEXT,
  error             TEXT,
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL,
  FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
  FOREIGN KEY (template_id) REFERENCES templates(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_email_messages_status
  ON email_messages(status);

CREATE INDEX IF NOT EXISTS ix_email_messages_campaign
  ON email_messages(campaign_id);

CREATE INDEX IF NOT EXISTS ix_email_messages_lead
  ON email_messages(lead_id);

CREATE INDEX IF NOT EXISTS ix_email_messages_scheduled
  ON email_messages(scheduled_at);

CREATE INDEX IF NOT EXISTS ix_email_messages_sent
  ON email_messages(sent_at);

-- =========================
-- events (logg/statistik)
-- =========================
CREATE TABLE IF NOT EXISTS events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  lead_id      INTEGER NOT NULL,
  campaign_id  INTEGER NOT NULL,
  message_id   INTEGER,
  type         TEXT    NOT NULL
              CHECK (type IN ('sent','reply','bounce','booked','won','lost','unsubscribe')),
  meta         TEXT,
  created_at   TEXT    NOT NULL,
  FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE,
  FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
  FOREIGN KEY (message_id) REFERENCES email_messages(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_events_campaign_type_time
  ON events(campaign_id, type, created_at);

CREATE INDEX IF NOT EXISTS ix_events_lead_time
  ON events(lead_id, created_at);

-- =========================
-- do_not_contact (global spärrlista)
-- =========================
CREATE TABLE IF NOT EXISTS do_not_contact (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  email      TEXT    NOT NULL,
  orgnr      TEXT,
  reason     TEXT,
  created_at TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dnc_email
  ON do_not_contact(email);

CREATE INDEX IF NOT EXISTS ix_dnc_orgnr
  ON do_not_contact(orgnr);

-- =========================
-- suppliers (valfri MVP+ men vi tar med den)
-- =========================
CREATE TABLE IF NOT EXISTS suppliers (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  name          TEXT NOT NULL,
  sector        TEXT,
  contact_name  TEXT,
  contact_email TEXT,
  status        TEXT NOT NULL DEFAULT 'new'
               CHECK (status IN ('new','onboarded','active','paused','dropped')),
  notes         TEXT,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_suppliers_status
  ON suppliers(status);

CREATE INDEX IF NOT EXISTS ix_suppliers_sector
  ON suppliers(sector);

CREATE INDEX IF NOT EXISTS ix_suppliers_contact_email
  ON suppliers(contact_email);
"""


def ensure_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(SCHEMA_SQL)

        # Quick sanity info (optional)
        # conn.execute("VACUUM;")  # kör bara om du vill, annars låt vara


def main() -> None:
    ensure_db(DB_PATH)
    print(f"OK: initialized {DB_PATH} at {utc_now_iso()} (UTC)")


if __name__ == "__main__":
    main()
