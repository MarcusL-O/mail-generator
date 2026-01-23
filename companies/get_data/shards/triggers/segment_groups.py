# segment_grouping_shards.py
# - Läser companies.db.sqlite (read-only)
# - Bygger segment_groups (CSV utan mellanslag) från "sanningen" line_of_work (+ fallback på sni_text)
# - Union + dedupe: tar aldrig bort grupper, bara lägger till
# - Skriver NDJSON per shard till data/out/shards/
#
# Kör:
#   python companies/control/segment_grouping_shards.py --shard-id 0 --shard-total 4
#
# Obs:
# - Kräver att companies.segment_groups finns (migration om saknas).
# - Om du vill “refresh”-logik: använd companies.segment_groups_checked_at (TEXT ISO).

import argparse
import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/db/companies.db.sqlite")
OUT_DIR = Path("data/out/shards")
LIMIT = 0
RESUME = True
PRINT_EVERY = 250
REFRESH_DAYS = 180  # används om segment_groups_checked_at finns
# =========================


# =========================
# Segment (8–15) – breda outreach-grupper
# =========================
@dataclass(frozen=True)
class SegmentGroup:
    key: str
    note: str

SEGMENTS: List[SegmentGroup] = [
    SegmentGroup("office_b2b", "Kontorsbaserade B2B-tjänster (redovisning/juridik/rekry/marknad/IT-tjänster m.m.)."),
    SegmentGroup("it_sector", "IT/tech (drift, IT-tjänster, mjukvara, SaaS)."),
    SegmentGroup("construction_sector", "Bygg/hantverk (entreprenad, el, VVS, måleri, snickeri)."),
    SegmentGroup("finance_sector", "Finans/försäkring/redovisning/fastighetstjänster."),
    SegmentGroup("transport_sector", "Transport/logistik/åkeri/frakt."),
    SegmentGroup("industry_sector", "Tillverkning/industri/produktion."),
    SegmentGroup("health_sector", "Vård/tandvård/kliniker."),
    SegmentGroup("education_sector", "Utbildning/training."),
    SegmentGroup("hospitality_sector", "Restaurang/hotell/resa/konferens."),
    SegmentGroup("retail_sector", "E-handel/handel/butik."),
    SegmentGroup("agri_sector", "Lantbruk/skog/jordbruk."),
    SegmentGroup("events_sector", "Event/mässor/arrangemang."),
]
# =========================


# =========================
# Mappning: line_of_work -> segment
# (line_of_work kommer från din klassning: final_label / line_of_work)
# =========================
LINE_OF_WORK_TO_SEGMENTS: Dict[str, List[str]] = {
    # IT
    "it_services": ["it_sector", "office_b2b"],
    "software": ["it_sector", "office_b2b"],

    # Bygg/hantverk
    "architecture": ["construction_sector"],
    "construction": ["construction_sector"],
    "electrical": ["construction_sector"],
    "plumbing_hvac": ["construction_sector"],
    "carpentry": ["construction_sector"],
    "painting": ["construction_sector"],

    # Business services / kontor
    "marketing": ["office_b2b"],
    "recruitment": ["office_b2b"],
    "legal": ["office_b2b"],
    "cleaning": ["office_b2b"],
    "security": ["office_b2b"],
    "industrial_services": ["office_b2b"],

    # Finance / fastighet
    "accounting": ["finance_sector", "office_b2b"],
    "finance_insurance": ["finance_sector", "office_b2b"],
    "real_estate": ["finance_sector", "office_b2b"],

    # Övrigt
    "transport_logistics": ["transport_sector"],
    "manufacturing": ["industry_sector"],
    "automotive": ["retail_sector"],  # valfritt – byt till consumer_services om du vill ha en egen
    "healthcare": ["health_sector"],
    "dental": ["health_sector"],
    "education": ["education_sector"],
    "restaurant_cafe": ["hospitality_sector"],
    "hotel_travel": ["hospitality_sector"],
    "ecommerce_retail": ["retail_sector"],
    "events": ["events_sector"],
    "agriculture": ["agri_sector"],
}

# Fallback: sni_text -> segment (lätt/robust)
SNI_TEXT_KEYWORDS: List[Tuple[str, str]] = [
    ("construction_sector", "bygg"),
    ("construction_sector", "entreprenad"),
    ("construction_sector", "elinstallation"),
    ("construction_sector", "vvs"),
    ("it_sector", "it"),
    ("it_sector", "systemutveck"),
    ("it_sector", "programvar"),
    ("finance_sector", "redovis"),
    ("finance_sector", "revision"),
    ("finance_sector", "försäkring"),
    ("finance_sector", "fastighet"),
    ("transport_sector", "transport"),
    ("transport_sector", "logistik"),
    ("hospitality_sector", "restaurang"),
    ("hospitality_sector", "hotell"),
    ("health_sector", "vård"),
    ("health_sector", "tand"),
    ("education_sector", "utbild"),
    ("retail_sector", "handel"),
    ("retail_sector", "butik"),
    ("retail_sector", "e-handel"),
]
# =========================


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        ss = str(s).replace("Z", "+00:00")
        if "T" not in ss and " " in ss:
            ss = ss.replace(" ", "T")
        return datetime.fromisoformat(ss)
    except Exception:
        return None


def _has_column(con: sqlite3.Connection, table: str, col: str) -> bool:
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {str(r[1]) for r in cur.fetchall()}
    return col in cols


def _require_columns(con: sqlite3.Connection) -> None:
    for col in ("orgnr", "name", "line_of_work"):
        if not _has_column(con, "companies", col):
            raise SystemExit(f"Saknar kolumn companies.{col}")
    if not _has_column(con, "companies", "segment_groups"):
        raise SystemExit("Saknar kolumn companies.segment_groups (kör migration).")


def _in_shard(orgnr: str, shard_id: int, shard_total: int) -> bool:
    h = hashlib.md5(orgnr.encode("utf-8")).hexdigest()
    return (int(h, 16) % shard_total) == shard_id


def _load_done_set(path: Path) -> Set[str]:
    done: Set[str] = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as rf:
        for line in rf:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                o = (obj.get("orgnr") or "").strip()
                if o:
                    done.add(o)
            except Exception:
                pass
    return done


def _parse_groups_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    s = str(raw).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _to_groups_csv(groups: Iterable[str]) -> str:
    order = {g.key: i for i, g in enumerate(SEGMENTS)}
    uniq = list(dict.fromkeys([x for x in groups if x]))
    uniq.sort(key=lambda k: (order.get(k, 10_000), k))
    return ",".join(uniq)


def _should_refresh(existing_groups: Optional[str], checked_at: Optional[str], *, has_checked_col: bool) -> bool:
    # Kommentar: samma tänk som sni_groups-scriptet
    if not has_checked_col:
        return True
    if not existing_groups or not str(existing_groups).strip():
        return True
    dt = _parse_iso(checked_at)
    if not dt:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
    return dt < cutoff


def _segment_from_line_of_work(line_of_work: str) -> List[str]:
    k = (line_of_work or "").strip().lower()
    if not k or k in ("unknown", "okänd", '""', "__no__"):
        return []
    return LINE_OF_WORK_TO_SEGMENTS.get(k, [])


def _segment_from_sni_text(sni_text: str) -> List[str]:
    t = (sni_text or "").lower().strip()
    if not t or t in ("__no_sni__", "okänd", '""'):
        return []
    out: List[str] = []
    for seg, kw in SNI_TEXT_KEYWORDS:
        if kw in t:
            out.append(seg)
    # Kommentar: om den ser ut som kontor (finance/it) -> office_b2b också
    if any(s in out for s in ("it_sector", "finance_sector")) and "office_b2b" not in out:
        out.append("office_b2b")
    # dedupe
    return list(dict.fromkeys(out))


def _pick_targets(
    con: sqlite3.Connection,
    *,
    limit: Optional[int],
    has_checked_col: bool,
) -> List[Tuple[str, str, str, str, Optional[str], Optional[str]]]:
    """
    Returns: (orgnr, name, line_of_work, sni_text, segment_groups_before, segment_groups_checked_at)
    """
    cur = con.cursor()

    select_cols = ["orgnr", "name", "line_of_work", "COALESCE(sni_text,'') AS sni_text", "segment_groups"]
    if has_checked_col:
        select_cols.append("segment_groups_checked_at")
    else:
        select_cols.append("NULL AS segment_groups_checked_at")

    base_sql = f"""
      SELECT {", ".join(select_cols)}
      FROM companies
      WHERE line_of_work IS NOT NULL AND TRIM(line_of_work) <> ''
      ORDER BY orgnr ASC
    """

    if limit is None:
        cur.execute(base_sql)
        rows = cur.fetchall()
    else:
        cur.execute(base_sql + " LIMIT ?", (limit * 5,))
        rows = cur.fetchall()

    out: List[Tuple[str, str, str, str, Optional[str], Optional[str]]] = []
    for orgnr, name, line_of_work, sni_text, seg_before, checked_at in rows:
        if not orgnr or not name:
            continue
        if _should_refresh(seg_before, checked_at, has_checked_col=has_checked_col):
            out.append((str(orgnr), str(name), str(line_of_work or ""), str(sni_text or ""), seg_before, checked_at))
            if limit is not None and len(out) >= limit:
                break
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard-id", type=int, required=True)
    ap.add_argument("--shard-total", type=int, default=4)
    args = ap.parse_args()

    shard_id = int(args.shard_id)
    shard_total = int(args.shard_total)

    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"segment_groups_shard{shard_id}.ndjson"

    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA journal_mode=WAL;")

    try:
        _require_columns(conn)
        has_checked_col = _has_column(conn, "companies", "segment_groups_checked_at")

        done = _load_done_set(out_path) if RESUME else set()
        limit = None if LIMIT == 0 else LIMIT

        targets = _pick_targets(conn, limit=limit, has_checked_col=has_checked_col)
        targets = [t for t in targets if _in_shard(t[0], shard_id, shard_total)]
        if RESUME and done:
            targets = [t for t in targets if t[0] not in done]

        print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={shard_id}/{shard_total})")
        if not has_checked_col:
            print("Info: companies.segment_groups_checked_at saknas -> refresh=ALLA (lägg migration om du vill).")

        processed = 0
        hits = 0
        misses = 0
        err_other = 0

        seg_counts: Dict[str, int] = {s.key: 0 for s in SEGMENTS}

        start = time.time()
        ts = utcnow_iso()

        with out_path.open("a", encoding="utf-8") as out_f:
            for orgnr, name, line_of_work, sni_text, seg_before_raw, checked_before in targets:
                processed += 1
                try:
                    matched = _segment_from_line_of_work(line_of_work)
                    if not matched:
                        # Kommentar: fallback om line_of_work är "okänd"/tomt/omappad
                        matched = _segment_from_sni_text(sni_text)

                    before = _parse_groups_csv(seg_before_raw)
                    after_set = set(before)
                    for k in matched:
                        after_set.add(k)

                    after_csv = _to_groups_csv(after_set)
                    status = "found" if after_csv else "not_found"
                    if status == "found":
                        hits += 1
                    else:
                        misses += 1

                    for k in matched:
                        seg_counts[k] = seg_counts.get(k, 0) + 1

                    row = {
                        "orgnr": orgnr,
                        "name": name,
                        "line_of_work": line_of_work,
                        "sni_text": sni_text,
                        "status": status,
                        "err_reason": "",
                        "segment_groups_before": (seg_before_raw or ""),
                        "segment_groups_after": after_csv,
                        "checked_at": ts,
                        "segment_groups_checked_at_before": (checked_before or ""),
                        "shard_id": shard_id,
                        "shard_total": shard_total,
                    }
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                except Exception:
                    err_other += 1
                    row = {
                        "orgnr": orgnr,
                        "name": name,
                        "line_of_work": line_of_work,
                        "sni_text": sni_text,
                        "status": "error",
                        "err_reason": "other",
                        "segment_groups_before": (seg_before_raw or ""),
                        "segment_groups_after": (seg_before_raw or ""),
                        "checked_at": ts,
                        "segment_groups_checked_at_before": (checked_before or ""),
                        "shard_id": shard_id,
                        "shard_total": shard_total,
                    }
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(f"[{processed}] hits={hits} misses={misses} other={err_other} | {rate:.1f}/s")

        print("KLART ✅")
        print(f"Processade: {processed}")
        print(f"HITS: {hits}")
        print(f"MISSES: {misses}")
        print(f"Errors: other={err_other}")
        print("Per segment (matchade företag):")
        for s in SEGMENTS:
            print(f"- {s.key}: {seg_counts.get(s.key, 0)}")
        print(f"OUT: {out_path.resolve()}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
