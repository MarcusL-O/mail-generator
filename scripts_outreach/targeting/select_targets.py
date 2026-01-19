# scripts_outreach/targeting/select_targets.py
# Väljer bolag från companies.db.sqlite och registrerar dem i outreach.db.sqlite för en kampanj.
# Filter: city (en eller flera), SNI (prefix eller exakt kod), samt krav på website_status/email_status.
# Dedupe: ett bolag (orgnr) finns bara en gång i leads, men kan kopplas till flera kampanjer via lead_campaigns.
# Kopplingen (lead_id, campaign_id) skapas med INSERT OR IGNORE så samma bolag inte läggs in två gånger i samma kampanj.
# next_send_at sätts till nu + stagger (minuter) så utskick sprids över tid och blir “due” i rätt ordning.

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional, Sequence, Tuple


COMPANIES_DB = Path("data/db/companies.db.sqlite")
OUTREACH_DB = Path("data/db/outreach.db.sqlite")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_csv_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


@dataclass(frozen=True)
class CompanyRow:
    orgnr: str
    name: str
    city: str
    sni_codes_raw: Optional[str]
    website: Optional[str]
    emails_raw: Optional[str]
    website_status: Optional[str]
    email_status: Optional[str]


def get_campaign(con: sqlite3.Connection, campaign_name: str) -> Tuple[int, str]:
    cur = con.cursor()
    cur.execute("SELECT id, audience FROM campaigns WHERE name = ? LIMIT 1", (campaign_name,))
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"Kampanj saknas i outreach.db: {campaign_name} (har du kört seed_sequences.py?)")
    return int(row[0]), str(row[1])


def _build_sni_where(*, wanted_snis: Sequence[str], mode: str) -> Tuple[str, List[object]]:
    """
    Kommentar (svenska):
    Vi lagrar sni_codes som text, ofta kommaseparerat: "69201,70200".
    Vi kan därför matcha med LIKE mot olika positioner (början, mitten, slut).
    För prefix-match vill vi hitta token som börjar på prefixet, oavsett position.
    """
    if not wanted_snis:
        return "", []

    clauses: List[str] = []
    params: List[object] = []

    for s in wanted_snis:
        if mode == "exact":
            # Exakt tokenmatch i en kommaseparerad sträng
            clauses.append(
                "("
                "TRIM(COALESCE(sni_codes,'')) = ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ?"
                ")"
            )
            params.extend(
                [
                    s,
                    f"{s},%",
                    f"%,{s},%",
                    f"%,{s}",
                ]
            )
        else:
            # Prefixmatch: token som börjar med prefixet, oavsett position
            # Ex: prefix=69 matchar "69102" eller "69201" både i början och efter komma.
            clauses.append(
                "("
                "TRIM(COALESCE(sni_codes,'')) LIKE ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ?"
                ")"
            )
            params.extend(
                [
                    f"{s}%",
                    f"{s}%,%",
                    f"%,{s}%",
                ]
            )

    return f" AND ({' OR '.join(clauses)})", params


def fetch_candidates(
    con: sqlite3.Connection,
    *,
    cities: Sequence[str],
    wanted_snis: Sequence[str],
    sni_match: str,
    require_website_status: str,
    require_email_status: str,
    limit: int,
) -> List[CompanyRow]:
    cur = con.cursor()

    where = [
        "website_status = ?",
        "email_status = ?",
        "emails IS NOT NULL",
        "trim(emails) != ''",
        "trim(coalesce(sni_codes,'')) != ''",
        "trim(sni_codes) != '__NO_SNI__'",
        "trim(sni_codes) != '00000'",
    ]
    params: List[object] = [require_website_status, require_email_status]

    if cities:
        placeholders = ",".join(["?"] * len(cities))
        where.append(f"city IN ({placeholders})")
        params.extend(list(cities))

    sni_where, sni_params = _build_sni_where(wanted_snis=wanted_snis, mode=sni_match)
    # sni_where börjar med " AND (...)" om den finns
    sql = f"""
    SELECT orgnr, name, city, sni_codes, website, emails, website_status, email_status
    FROM companies
    WHERE {" AND ".join(where)}{sni_where}
    ORDER BY orgnr ASC
    LIMIT ?
    """
    params.extend(sni_params)
    params.append(limit)

    rows: List[CompanyRow] = []
    for r in cur.execute(sql, params).fetchall():
        rows.append(
            CompanyRow(
                orgnr=str(r[0]),
                name=str(r[1]),
                city=str(r[2] or ""),
                sni_codes_raw=r[3],
                website=r[4],
                emails_raw=r[5],
                website_status=r[6],
                email_status=r[7],
            )
        )
    return rows


def upsert_lead(
    con: sqlite3.Connection,
    *,
    orgnr: str,
    company_name: str,
    city: str,
    sni_codes_raw: Optional[str],
    website: Optional[str],
    emails_raw: Optional[str],
    lead_type: str,
    ts: str,
) -> int:
    cur = con.cursor()

    cur.execute(
        """
        INSERT INTO leads (orgnr, company_name, city, sni_codes, website, emails, lead_type, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)
        ON CONFLICT(orgnr) DO UPDATE SET
          company_name=excluded.company_name,
          city=excluded.city,
          sni_codes=excluded.sni_codes,
          website=excluded.website,
          emails=excluded.emails,
          lead_type=excluded.lead_type,
          updated_at=excluded.updated_at
        """,
        (orgnr, company_name, city, sni_codes_raw, website, emails_raw, lead_type, ts, ts),
    )

    cur.execute("SELECT id FROM leads WHERE orgnr = ? LIMIT 1", (orgnr,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Kunde inte läsa tillbaka lead id för orgnr={orgnr}")
    return int(row[0])


def ensure_lead_campaign(
    con: sqlite3.Connection,
    *,
    lead_id: int,
    campaign_id: int,
    next_send_at: str,
    ts: str,
) -> bool:
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO lead_campaigns
          (lead_id, campaign_id, current_step, current_variant, next_send_at, stopped_reason, created_at, updated_at)
        VALUES (?, ?, 1, NULL, ?, NULL, ?, ?)
        """,
        (lead_id, campaign_id, next_send_at, ts, ts),
    )
    return cur.rowcount > 0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign-name", required=True, help="Kampanjnamn i outreach.db, ex: supplier_intro")
    ap.add_argument("--cities", default="", help="Komma-separerad lista, ex: Göteborg,Stockholm")
    ap.add_argument("--sni", default="", help="Komma-separerad lista, ex: 62,63 eller 71110")
    ap.add_argument("--sni-match", choices=["prefix", "exact"], default="prefix")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--stagger-minutes", type=int, default=2)
    ap.add_argument("--require-website-status", default="found")
    ap.add_argument("--require-email-status", default="found")
    args = ap.parse_args()

    cities = parse_csv_list(args.cities)
    wanted_snis = parse_csv_list(args.sni)

    if not COMPANIES_DB.exists():
        raise SystemExit(f"Hittar inte {COMPANIES_DB}")
    if not OUTREACH_DB.exists():
        raise SystemExit(f"Hittar inte {OUTREACH_DB}")

    c_con = sqlite3.connect(str(COMPANIES_DB))
    o_con = sqlite3.connect(str(OUTREACH_DB))
    try:
        campaign_id, audience = get_campaign(o_con, args.campaign_name)
        lead_type = audience  # 'supplier' | 'customer'

        candidates = fetch_candidates(
            c_con,
            cities=cities,
            wanted_snis=wanted_snis,
            sni_match=args.sni_match,
            require_website_status=args.require_website_status,
            require_email_status=args.require_email_status,
            limit=args.limit,
        )

        ts = now_iso()
        added_links = 0
        upserted_leads = 0

        base = datetime.now(timezone.utc)

        for idx, comp in enumerate(candidates):
            lead_id = upsert_lead(
                o_con,
                orgnr=comp.orgnr,
                company_name=comp.name,
                city=comp.city,
                sni_codes_raw=comp.sni_codes_raw,
                website=comp.website,
                emails_raw=comp.emails_raw,
                lead_type=lead_type,
                ts=ts,
            )
            upserted_leads += 1

            next_send = (base + timedelta(minutes=idx * max(args.stagger_minutes, 0))).isoformat()
            if ensure_lead_campaign(
                o_con,
                lead_id=lead_id,
                campaign_id=campaign_id,
                next_send_at=next_send,
                ts=ts,
            ):
                added_links += 1

            o_con.commit()

        print("DONE ✅")
        print(f"campaign={args.campaign_name} (id={campaign_id}, audience={audience})")
        print(f"candidates_matched_all_filters={len(candidates)}")
        print(f"upserted_leads={upserted_leads} new_campaign_links={added_links}")
        print(f"stagger_minutes={args.stagger_minutes}")

    finally:
        c_con.close()
        o_con.close()


if __name__ == "__main__":
    main()
