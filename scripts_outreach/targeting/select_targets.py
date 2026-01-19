# scripts_outreach/targeting/select_targets.py
# Väljer bolag från companies.db.sqlite och registrerar dem i outreach.db.sqlite för en kampanj.
#
# Lager 1 (alltid på):
# - require website_status/email_status (defaults)
# - kräver emails (annars kan vi inte kontakta)
# - filtrerar bort "no sni" (kan slås av via flagga)
# - exkluderar do_not_contact + unsubscribe/bounce i outreach (kan slås av via flagga)
#
# Lager 2 (valbara filter):
# - city (en/flera/alla)
# - SNI via grupp(er) eller manuell lista (prefix/exact)
# - employees-kategorier (en/flera)
# - founded/created_at intervall (min/max)
# - “extra flags” (tech/review) stödjer bara om kolumner finns i companies (annars räknas som saknad data)
#
# Tiers (1..5):
# - Tier 1 = matchar alla aktiva filter
# - Tier 2 = missar 1 aktivt filter
# - ...
# - Tier 5 = missar >=4 aktiva filter (cap)
#
# Sparar i lead_campaigns:
# - tier, match_flags (JSON), score
#
# OBS: Wizard bygger du senare – detta är CLI-motorn.

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


COMPANIES_DB = Path("data/db/companies.db.sqlite")
OUTREACH_DB = Path("data/db/outreach.db.sqlite")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_csv_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def parse_bool_choice(value: str) -> str:
    """
    Kommentar (svenska):
    Vi använder samma enkla logik överallt:
    - "yes" = kräv
    - "no"  = ignorera helt
    """
    v = (value or "").strip().lower()
    if v not in ("yes", "no"):
        raise SystemExit("Ogiltigt val, använd 'yes' eller 'no'.")
    return v


@dataclass(frozen=True)
class CompanyRow:
    orgnr: str
    name: str
    city: str
    employees: Optional[int]
    sni_codes_raw: Optional[str]
    website: Optional[str]
    emails_raw: Optional[str]
    website_status: Optional[str]
    email_status: Optional[str]
    created_at: Optional[str]
    started_at: Optional[str]
    # “extra” (om de finns i DB)
    tech_flag: Optional[str]
    review_flag: Optional[str]


def get_campaign(con: sqlite3.Connection, campaign_name: str) -> Tuple[int, str]:
    cur = con.cursor()
    cur.execute("SELECT id, audience FROM campaigns WHERE name = ? LIMIT 1", (campaign_name,))
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"Kampanj saknas i outreach.db: {campaign_name}")
    return int(row[0]), str(row[1])


def _companies_has_column(con: sqlite3.Connection, col: str) -> bool:
    cur = con.cursor()
    cur.execute("PRAGMA table_info(companies)")
    cols = {str(r[1]) for r in cur.fetchall()}
    return col in cols


def _build_sni_where(*, wanted_snis: Sequence[str], mode: str) -> Tuple[str, List[object]]:
    """
    Kommentar (svenska):
    I companies.db är sni_codes text, ibland JSON eller kommaseparerat.
    Vi matchar med LIKE för att undvika att bygga JSON-parser i SQL.
    """
    if not wanted_snis:
        return "", []

    clauses: List[str] = []
    params: List[object] = []

    for s in wanted_snis:
        s = s.strip()
        if not s:
            continue

        if mode == "exact":
            clauses.append(
                "("
                "TRIM(COALESCE(sni_codes,'')) = ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ? "
                ")"
            )
            # Kommentar: stöder både CSV ("...,69201,...") och JSON-liknande ('["69201", ...]')
            params.extend(
                [
                    s,
                    f"{s},%",
                    f"%,{s},%",
                    f"%,{s}",
                    f'%"{s}"%',  # JSON-token
                ]
            )
        else:
            # Prefixmatch: token som börjar med prefixet
            clauses.append(
                "("
                "TRIM(COALESCE(sni_codes,'')) LIKE ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ? "
                "OR sni_codes LIKE ? "
                ")"
            )
            params.extend(
                [
                    f"{s}%",
                    f"{s}%,%",
                    f"%,{s}%",
                    f'%"{s}%',  # JSON token prefix
                ]
            )

    if not clauses:
        return "", []

    return f" AND ({' OR '.join(clauses)})", params


def _build_city_where(cities: Sequence[str]) -> Tuple[str, List[object]]:
    if not cities:
        return "", []
    placeholders = ",".join(["?"] * len(cities))
    return f" AND city IN ({placeholders})", list(cities)


def _build_employees_where(ranges: Sequence[str]) -> Tuple[str, List[object]]:
    """
    Kommentar (svenska):
    ranges ex: ["0-4","5-9","10-19","20-49","50-99","100-249","250+"]
    """
    if not ranges:
        return "", []

    clauses: List[str] = []
    params: List[object] = []

    for r in ranges:
        rr = r.strip()
        if not rr:
            continue
        if rr.endswith("+"):
            lo = int(rr[:-1])
            clauses.append("(employees IS NOT NULL AND employees >= ?)")
            params.append(lo)
        else:
            a, b = rr.split("-", 1)
            lo = int(a.strip())
            hi = int(b.strip())
            clauses.append("(employees IS NOT NULL AND employees BETWEEN ? AND ?)")
            params.extend([lo, hi])

    if not clauses:
        return "", []
    return f" AND ({' OR '.join(clauses)})", params


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Kommentar: SQLite datetime('now') kan vara "YYYY-MM-DD HH:MM:SS"
        if "T" not in s and " " in s:
            return datetime.fromisoformat(s.replace(" ", "T"))
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def fetch_candidates(
    con: sqlite3.Connection,
    *,
    cities: Sequence[str],
    wanted_snis: Sequence[str],
    sni_match: str,
    require_website_status: str,
    require_email_status: str,
    require_sni_present: bool,
    employees_ranges: Sequence[str],
    limit: int,
    tech_filter: str,
    review_filter: str,
    founded_min: Optional[str],
    founded_max: Optional[str],
) -> List[CompanyRow]:
    cur = con.cursor()

    has_tech = _companies_has_column(con, "tech_footprint_status") or _companies_has_column(con, "tech_footprint")
    has_review = _companies_has_column(con, "website_review_status") or _companies_has_column(con, "site_review_status")

    tech_col = "tech_footprint_status" if _companies_has_column(con, "tech_footprint_status") else ("tech_footprint" if _companies_has_column(con, "tech_footprint") else None)
    review_col = "website_review_status" if _companies_has_column(con, "website_review_status") else ("site_review_status" if _companies_has_column(con, "site_review_status") else None)

    select_cols = [
        "orgnr", "name", "city", "employees", "sni_codes", "website", "emails",
        "website_status", "email_status", "created_at", "started_at",
    ]
    if tech_col:
        select_cols.append(f"{tech_col} AS tech_flag")
    else:
        select_cols.append("NULL AS tech_flag")
    if review_col:
        select_cols.append(f"{review_col} AS review_flag")
    else:
        select_cols.append("NULL AS review_flag")

    where = [
        "website_status = ?",
        "email_status = ?",
        "emails IS NOT NULL",
        "trim(emails) != ''",
    ]
    params: List[object] = [require_website_status, require_email_status]

    if require_sni_present:
        where.extend(
            [
                "trim(coalesce(sni_codes,'')) != ''",
                "trim(sni_codes) != '__NO_SNI__'",
                "trim(sni_codes) != '00000'",
            ]
        )

    city_where, city_params = _build_city_where(cities)
    emp_where, emp_params = _build_employees_where(employees_ranges)
    sni_where, sni_params = _build_sni_where(wanted_snis=wanted_snis, mode=sni_match)

    sql = f"""
    SELECT {", ".join(select_cols)}
    FROM companies
    WHERE {" AND ".join(where)}
    {city_where}
    {emp_where}
    {sni_where}
    ORDER BY orgnr ASC
    LIMIT ?
    """
    params.extend(city_params)
    params.extend(emp_params)
    params.extend(sni_params)
    params.append(limit)

    rows: List[CompanyRow] = []
    for r in cur.execute(sql, params).fetchall():
        rows.append(
            CompanyRow(
                orgnr=str(r[0]),
                name=str(r[1]),
                city=str(r[2] or ""),
                employees=r[3],
                sni_codes_raw=r[4],
                website=r[5],
                emails_raw=r[6],
                website_status=r[7],
                email_status=r[8],
                created_at=r[9],
                started_at=r[10],
                tech_flag=r[11],
                review_flag=r[12],
            )
        )

    # Kommentar (svenska): founded/created_at intervall (om användaren vill)
    if founded_min or founded_max:
        dt_min = _parse_iso(founded_min) if founded_min else None
        dt_max = _parse_iso(founded_max) if founded_max else None

        def _ok(dt: Optional[datetime]) -> bool:
            if not dt:
                return False
            if dt_min and dt < dt_min:
                return False
            if dt_max and dt > dt_max:
                return False
            return True

        filtered: List[CompanyRow] = []
        for row in rows:
            # Kommentar: primärt started_at, fallback created_at
            dt = _parse_iso(row.started_at) or _parse_iso(row.created_at)
            if _ok(dt):
                filtered.append(row)
        rows = filtered

    # Kommentar (svenska): “tech/review” filterval om kolumner finns
    def _has_data(val: Optional[str]) -> bool:
        return bool(val and str(val).strip())

    if tech_filter == "yes" and has_tech:
        rows = [x for x in rows if _has_data(x.tech_flag)]
    if review_filter == "yes" and has_review:
        rows = [x for x in rows if _has_data(x.review_flag)]

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
    tier: Optional[int],
    match_flags_json: str,
    score: Optional[int],
) -> bool:
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO lead_campaigns
          (lead_id, campaign_id, current_step, current_variant, next_send_at, stopped_reason, created_at, updated_at, tier, match_flags, score)
        VALUES (?, ?, 1, NULL, ?, NULL, ?, ?, ?, ?, ?)
        """,
        (lead_id, campaign_id, next_send_at, ts, ts, tier, match_flags_json, score),
    )
    return cur.rowcount > 0


def load_sni_groups(o_con: sqlite3.Connection, group_keys: Sequence[str]) -> List[str]:
    """
    Kommentar (svenska):
    Läser grupp->patterns från outreach-db (targeting_sni_groups + targeting_sni_group_items).
    Returnerar lista av pattern-strängar (prefix eller exact beroende på group.mode).
    """
    if not group_keys:
        return []

    cur = o_con.cursor()
    placeholders = ",".join(["?"] * len(group_keys))
    cur.execute(
        f"""
        SELECT g.group_key, g.match_mode, i.pattern
        FROM targeting_sni_groups g
        JOIN targeting_sni_group_items i ON i.group_id = g.id
        WHERE g.group_key IN ({placeholders})
        ORDER BY g.group_key, i.pattern
        """,
        list(group_keys),
    )
    rows = cur.fetchall()
    if not rows:
        raise SystemExit("Inga SNI-grupper hittades (har du kört migrationsscriptet + lagt in grupper?)")

    patterns: List[str] = []
    for _gk, _mode, pat in rows:
        if pat:
            patterns.append(str(pat))
    return patterns


def get_blocked_orgnrs(o_con: sqlite3.Connection, orgnrs: Sequence[str], *, exclude_dnc: bool) -> set:
    """
    Kommentar (svenska):
    Exkluderar do_not_contact + unsubscribe/bounce events för befintliga leads.
    """
    if not exclude_dnc or not orgnrs:
        return set()

    cur = o_con.cursor()
    placeholders = ",".join(["?"] * len(orgnrs))

    blocked: set = set()

    # do_not_contact via orgnr
    cur.execute(f"SELECT orgnr FROM do_not_contact WHERE orgnr IN ({placeholders})", list(orgnrs))
    blocked.update({str(r[0]) for r in cur.fetchall() if r and r[0]})

    # leads som redan är do_not_contact
    cur.execute(f"SELECT orgnr FROM leads WHERE orgnr IN ({placeholders}) AND status = 'do_not_contact'", list(orgnrs))
    blocked.update({str(r[0]) for r in cur.fetchall() if r and r[0]})

    # events: unsubscribe/bounce
    cur.execute(
        f"""
        SELECT l.orgnr
        FROM leads l
        JOIN events e ON e.lead_id = l.id
        WHERE l.orgnr IN ({placeholders})
          AND e.type IN ('unsubscribe','bounce')
        """,
        list(orgnrs),
    )
    blocked.update({str(r[0]) for r in cur.fetchall() if r and r[0]})

    return blocked


def compute_tier_and_flags(
    comp: CompanyRow,
    *,
    active_filters: Dict[str, bool],
    wanted_snis_any: bool,
    cities_any: bool,
    employees_any: bool,
    founded_any: bool,
    tech_any: bool,
    review_any: bool,
) -> Tuple[int, Dict[str, object], int]:
    """
    Kommentar (svenska):
    active_filters: map filter_name -> om den är aktiv (räknas i tier)
    Vi räknar missar på aktiva filter och cappar tier till 5.
    score är enkel just nu (kan förbättras senare).
    """
    flags: Dict[str, object] = {
        "city_ok": None,
        "sni_ok": None,
        "employees_ok": None,
        "founded_ok": None,
        "tech_ok": None,
        "review_ok": None,
    }

    misses = 0
    active_count = 0

    def _count(name: str, ok: bool) -> None:
        nonlocal misses, active_count
        if not active_filters.get(name, False):
            return
        active_count += 1
        if not ok:
            misses += 1

    # Kommentar: de här “ok”-värdena sätts redan av SQL-filter när du valt "yes"
    # men vi vill fortfarande kunna tier:a om du väljer "no" på vissa och senare vill prioritera.
    # Här blir därför default ok=True om filter inte är aktivt.
    flags["city_ok"] = True if not cities_any else bool(comp.city.strip())
    _count("city", bool(comp.city.strip()))

    flags["sni_ok"] = True if not wanted_snis_any else bool((comp.sni_codes_raw or "").strip())
    _count("sni", bool((comp.sni_codes_raw or "").strip()))

    flags["employees_ok"] = True if not employees_any else (comp.employees is not None)
    _count("employees", comp.employees is not None)

    dt = _parse_iso(comp.started_at) or _parse_iso(comp.created_at)
    flags["founded_ok"] = True if not founded_any else bool(dt)
    _count("founded", bool(dt))

    flags["tech_ok"] = True if not tech_any else bool((comp.tech_flag or "").strip())
    _count("tech", bool((comp.tech_flag or "").strip()))

    flags["review_ok"] = True if not review_any else bool((comp.review_flag or "").strip())
    _count("review", bool((comp.review_flag or "").strip()))

    tier = min(1 + misses, 5)

    # Kommentar: enkel score: tier driver mest
    score = (6 - tier) * 100  # tier1=500 ... tier5=100
    score += max(0, (10 - misses))  # liten bonus
    score += max(0, active_count)   # liten bonus

    return tier, flags, score


def main() -> None:
    ap = argparse.ArgumentParser()

    # Bas
    ap.add_argument("--campaign-name", required=True)
    ap.add_argument("--limit", type=int, default=10_000)
    ap.add_argument("--stagger-minutes", type=int, default=2)

    # Hårda krav (lager 1)
    ap.add_argument("--require-website-status", default="found")
    ap.add_argument("--require-email-status", default="found")
    ap.add_argument("--require-sni-present", choices=["yes", "no"], default="yes")
    ap.add_argument("--exclude-do-not-contact", choices=["yes", "no"], default="yes")

    # Geo (lager 2)
    ap.add_argument("--cities", default="", help="Komma-separerad, tomt = inget city-filter")

    # SNI (lager 2)
    ap.add_argument("--sni", default="", help="Komma-separerad lista ex: 62,63 eller 71110")
    ap.add_argument("--sni-groups", default="", help="Komma-separerad lista ex: kontor_fastighet,it_tech")
    ap.add_argument("--sni-match", choices=["prefix", "exact"], default="prefix")

    # Employees (lager 2)
    ap.add_argument(
        "--employees",
        default="",
        help='Komma-separerade intervall ex: "0-4,5-9,10-19" eller "250+"',
    )

    # Founded/created_at (lager 2)
    ap.add_argument("--founded-min", default="", help="ISO eller 'YYYY-MM-DD' (matchas mot started_at/created_at)")
    ap.add_argument("--founded-max", default="", help="ISO eller 'YYYY-MM-DD' (matchas mot started_at/created_at)")

    # Extra flags (lager 2)
    ap.add_argument("--tech", choices=["yes", "no"], default="no", help="yes = kräver att tech-kolumn finns + är satt")
    ap.add_argument("--review", choices=["yes", "no"], default="no", help="yes = kräver att review-kolumn finns + är satt")

    args = ap.parse_args()

    if not COMPANIES_DB.exists():
        raise SystemExit(f"Hittar inte {COMPANIES_DB}")
    if not OUTREACH_DB.exists():
        raise SystemExit(f"Hittar inte {OUTREACH_DB}")

    require_sni_present = parse_bool_choice(args.require_sni_present) == "yes"
    exclude_dnc = parse_bool_choice(args.exclude_do_not_contact) == "yes"
    tech_filter = parse_bool_choice(args.tech)
    review_filter = parse_bool_choice(args.review)

    cities = parse_csv_list(args.cities)
    wanted_snis_manual = parse_csv_list(args.sni)
    group_keys = parse_csv_list(args.sni_groups)
    employees_ranges = parse_csv_list(args.employees)

    founded_min = (args.founded_min or "").strip() or None
    founded_max = (args.founded_max or "").strip() or None

    c_con = sqlite3.connect(str(COMPANIES_DB))
    o_con = sqlite3.connect(str(OUTREACH_DB))
    try:
        campaign_id, audience = get_campaign(o_con, args.campaign_name)
        lead_type = audience  # 'supplier' | 'customer'

        # Kommentar: expandera SNI-grupper (om angivet)
        wanted_snis_from_groups: List[str] = []
        if group_keys:
            wanted_snis_from_groups = load_sni_groups(o_con, group_keys)

        wanted_snis = wanted_snis_manual + wanted_snis_from_groups

        candidates = fetch_candidates(
            c_con,
            cities=cities,
            wanted_snis=wanted_snis,
            sni_match=args.sni_match,
            require_website_status=args.require_website_status,
            require_email_status=args.require_email_status,
            require_sni_present=require_sni_present,
            employees_ranges=employees_ranges,
            limit=args.limit,
            tech_filter=tech_filter,
            review_filter=review_filter,
            founded_min=founded_min,
            founded_max=founded_max,
        )

        # Kommentar: exkludera DNC / unsubscribe / bounce
        blocked = get_blocked_orgnrs(o_con, [c.orgnr for c in candidates], exclude_dnc=exclude_dnc)
        candidates = [c for c in candidates if c.orgnr not in blocked]

        ts = now_iso()
        base = datetime.now(timezone.utc)

        # Kommentar: vilka filter är “aktiva” för tier-beräkning?
        active_filters = {
            "city": bool(cities),
            "sni": bool(wanted_snis) or require_sni_present,
            "employees": bool(employees_ranges),
            "founded": bool(founded_min or founded_max),
            "tech": (tech_filter == "yes"),
            "review": (review_filter == "yes"),
        }

        added_links = 0
        upserted_leads = 0
        tiers_count = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}

        for idx, comp in enumerate(candidates):
            tier, flags, score = compute_tier_and_flags(
                comp,
                active_filters=active_filters,
                wanted_snis_any=active_filters["sni"],
                cities_any=active_filters["city"],
                employees_any=active_filters["employees"],
                founded_any=active_filters["founded"],
                tech_any=active_filters["tech"],
                review_any=active_filters["review"],
            )
            tiers_count[tier] += 1

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
            match_flags_json = json.dumps(
                {
                    **flags,
                    "active_filters": active_filters,
                    "sni_groups": group_keys,
                    "sni_manual": wanted_snis_manual,
                    "employees_ranges": employees_ranges,
                    "founded_min": founded_min or "",
                    "founded_max": founded_max or "",
                },
                ensure_ascii=False,
            )

            if ensure_lead_campaign(
                o_con,
                lead_id=lead_id,
                campaign_id=campaign_id,
                next_send_at=next_send,
                ts=ts,
                tier=tier,
                match_flags_json=match_flags_json,
                score=score,
            ):
                added_links += 1

            # Kommentar: commit per rad (enklast/robust), optimera senare om du vill
            o_con.commit()

        print("DONE ✅")
        print(f"campaign={args.campaign_name} (id={campaign_id}, audience={audience})")
        print(f"matched_after_filters={len(candidates)}")
        print(f"upserted_leads={upserted_leads} new_campaign_links={added_links}")
        print(f"tiers: t1={tiers_count[1]} t2={tiers_count[2]} t3={tiers_count[3]} t4={tiers_count[4]} t5={tiers_count[5]}")
        print(f"stagger_minutes={args.stagger_minutes}")

    finally:
        c_con.close()
        o_con.close()


if __name__ == "__main__":
    main()
