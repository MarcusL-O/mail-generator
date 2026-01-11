# MAIL-GENERATOR
### Outreach & Lead Matching System (MVP)

## Overview
MAIL-GENERATOR is a script-based system for **B2B outreach and lead matching**.

It:
- collects and enriches company data
- selects and segments relevant leads
- runs controlled outreach pipelines
- logs real-world outcomes (replies, meetings, deals)
- provides transparent statistics and control

This is a **technical MVP**: fully functional, production-oriented, and intentionally simple.
It is **not a UI product** — it is a **data + pipeline system**.

---

## High-level Flow
Companies DB
→ Data enrichment (websites, emails, SNI)
→ Outreach DB
→ Render email (templates + signatures + snippets)
→ Send email
→ Log events (sent / replied / booked / deal)
→ Control & statistics scripts


Two databases are used:
- `companies.db.sqlite` – company data & enrichment
- `outreach.db.sqlite` – outreach state, events, and KPIs

---

## Project Structure

### `/data`
Databases and generated artifacts.
- companies.db.sqlite
- outreach.db.sqlite
- raw/ – original input data
- out/ – shard & ndjson outputs
- backup_db/ – DB backups

---

### `/templates`
Content only (no logic).

- `email/` – email bodies grouped by intent and audience  
  (supplier_intro, supplier_followup, customer_intro, etc.)
- `signatures/` – reusable signatures (neutral / supplier / customer)
- `snippets/` – reusable blocks (ratings, reviews)

Templates can have multiple variants (A/B/C).  
The pipeline decides which one is active.

---

### `/scripts_companies_to_db`
Builds and enriches the companies database:
- imports
- SNI enrichment
- website & email discovery
- deduplication

---

### `/scripts_companies_to_shards`
Shard-based processing for heavy discovery tasks.
Designed for parallel execution.

---

### `/scripts_outreach`
The outreach engine.

#### `seed/`
Seeds content into the outreach DB  
(templates, signatures, settings, reviews).

#### `render/`
Builds final emails:
- combines template + signature + snippets + lead data
- outputs subject + HTML + TXT
- no sending, no DB writes

#### `send/`
Runs outreach pipelines:
- selects leads
- renders email
- sends email
- logs results

Shared logic lives in `send/shared/`.

#### `log/`
Writes truth into the outreach DB:
- sent / failed
- contacted / bounced
- replied / booked / deal (manual)

---

### `/scripts_control_companies`
Read-only inspection scripts for companies DB.

---

### `/scripts_control_outreach`
Read-only control & statistics for outreach DB:
- status overview
- recent activity
- delivery health
- duplicates
- sequence queues
- segment performance
- CSV exports

These scripts never modify data.

---

## How the System Is Used (Short Manual)

1. Build and enrich `companies.db`
2. Select leads into `outreach.db`
3. Seed templates and settings when content changes
4. Run send pipelines (supplier or customer)
5. Monitor system via control scripts
6. Manually mark outcomes:
   - reply → `mark_replied.py`
   - meeting → `mark_booked.py`
   - deal → `mark_deal.py`
7. Review statistics and conversion rates

---

## Status
This project represents a **strong technical MVP**:
- operational
- measurable
- auditable
- ready for real-world testing and pitching