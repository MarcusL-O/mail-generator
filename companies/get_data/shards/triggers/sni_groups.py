# companies/control/sni_grouping_shards.py
# - Läser companies.db.sqlite (read-only)
# - Räknar ut sni_groups (CSV utan mellanslag) baserat på SNI-regler
# - Union + dedupe: tar aldrig bort grupper, bara lägger till
# - Skriver NDJSON per shard till data/out/shards/
#
# Kör:
#   python companies/control/sni_grouping_shards.py --shard-id 0 --shard-total 4
#
# Obs:
# - Kräver att kolumnen companies.sni_groups finns (migration om saknas).
# - Om du vill “refresh”-logik: lägg till companies.sni_groups_checked_at (TEXT ISO) via migration.

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
REFRESH_DAYS = 180  # används om sni_groups_checked_at finns
# =========================


# =========================
# GRUPPDEFINITIONER (lätt att ändra manuellt)
# =========================
@dataclass(frozen=True)
class SniGroup:
    key: str
    patterns: Sequence[str]
    match_mode: str  # "prefix" | "exact"
    # Kommentar (svenska): Beskriv varför gruppen finns (inte uppenbart av namnet).
    note: str


SNI_GROUPS: List[SniGroup] = [
    SniGroup(
        key="kontor_foretag",
        match_mode="prefix",
        patterns=["69", "70", "71", "73", "74", "82"],
        note="Kontorsbaserade B2B-tjänstebolag (ekonomi/juridik/management/teknikkonsult/marknad/övriga tjänster/administration).",
    ),
    SniGroup(
        key="hantverkare",
        match_mode="prefix",
        patterns=["43"],
        note="Specialiserad byggverksamhet (typiskt hantverk: el, VVS, måleri, golv, etc).",
    ),
    SniGroup(
        key="konsultbolag",
        match_mode="prefix",
        patterns=["70", "71", "73", "74"],
        note="Konsultverksamhet (management, teknikkonsult/arkitekt, reklam/marknad, övriga företagstjänster).",
    ),
    SniGroup(
        key="itbolag",
        match_mode="prefix",
        patterns=["62", "63"],
        note="IT/tech (programvara, konsult, drift, informationstjänster).",
    ),
    SniGroup(
        key="ehandel",
        match_mode="prefix",
        patterns=["4791"],
        note="Ren e-handel (postorder- och internethandel).",
    ),
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
    if not _has_column(con, "companies", "sni_codes"):
        raise SystemExit("Saknar kolumn companies.sni_codes")
    if not _has_column(con, "companies", "sni_groups"):
        raise SystemExit("Saknar kolumn companies.sni_groups (lägg migration).")


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


def _parse_sni_codes(raw: Optional[str]) -> List[str]:
    """
    Kommentar (svenska):
    sni_codes kan vara CSV eller JSON-text. Vi normaliserar till lista av tokens (str).
    """
    if not raw:
        return []
    s = str(raw).strip()
    if not s:
        return []

    # JSON-lista?
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            out = []
            for x in arr if isinstance(arr, list) else []:
                t = str(x).strip().strip('"').strip("'")
                t = "".join(ch for ch in t if ch.isdigit())
                if t:
                    out.append(t)
            return out
        except Exception:
            pass

    # CSV/fallback
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out2 = []
    for p in parts:
        t = "".join(ch for ch in p if ch.isdigit())
        if t:
            out2.append(t)
    return out2


def _parse_groups_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    s = str(raw).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def _to_groups_csv(groups: Iterable[str]) -> str:
    # Stabil sort: enligt ordningen i SNI_GROUPS, därefter alfabetiskt för okända
    order = {g.key: i for i, g in enumerate(SNI_GROUPS)}
    uniq = list(dict.fromkeys([x for x in groups if x]))  # dedupe, behåll ordning initialt
    uniq.sort(key=lambda k: (order.get(k, 10_000), k))
    return ",".join(uniq)


def _match_group(sni_tokens: Sequence[str], group: SniGroup) -> bool:
    if not sni_tokens:
        return False
    pats = [p.strip() for p in group.patterns if str(p).strip()]
    if not pats:
        return False

    if group.match_mode == "exact":
        token_set = set(sni_tokens)
        return any(p in token_set for p in pats)

    # prefix (default)
    for t in sni_tokens:
        for p in pats:
            if t.startswith(p):
                return True
    return False


def _should_refresh(existing_groups: Optional[str], checked_at: Optional[str], *, has_checked_col: bool) -> bool:
    """
    Kommentar (svenska):
    Om sni_groups_checked_at finns: refresh om saknas, eller äldre än REFRESH_DAYS.
    Om inte finns: refresh alltid (då är urvalet "allt").
    """
    if not has_checked_col:
        return True

    if not existing_groups or not str(existing_groups).strip():
        return True

    dt = _parse_iso(checked_at)
    if not dt:
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
    return dt < cutoff


def _pick_targets(
    con: sqlite3.Connection,
    *,
    limit: Optional[int],
    has_checked_col: bool,
) -> List[Tuple[str, str, str, Optional[str], Optional[str]]]:
    """
    Returns: (orgnr, name, sni_codes, sni_groups_before, sni_groups_checked_at)
    """
    cur = con.cursor()

    select_cols = ["orgnr", "name", "sni_codes", "sni_groups"]
    if has_checked_col:
        select_cols.append("sni_groups_checked_at")
    else:
        select_cols.append("NULL AS sni_groups_checked_at")

    base_sql = f"""
      SELECT {", ".join(select_cols)}
      FROM companies
      WHERE sni_codes IS NOT NULL AND TRIM(sni_codes) <> ''
      ORDER BY orgnr ASC
    """

    if limit is None:
        cur.execute(base_sql)
        rows = cur.fetchall()
    else:
        # lite buffert, samma stil som dina andra scripts
        cur.execute(base_sql + " LIMIT ?", (limit * 5,))
        rows = cur.fetchall()

    out: List[Tuple[str, str, str, Optional[str], Optional[str]]] = []
    for orgnr, name, sni_codes, sni_groups_before, checked_at in rows:
        if not orgnr or not name or not sni_codes:
            continue
        if _should_refresh(sni_groups_before, checked_at, has_checked_col=has_checked_col):
            out.append((str(orgnr), str(name), str(sni_codes), sni_groups_before, checked_at))
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
    out_path = OUT_DIR / f"sni_groups_shard{shard_id}.ndjson"

    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA journal_mode=WAL;")

    try:
        _require_columns(conn)
        has_checked_col = _has_column(conn, "companies", "sni_groups_checked_at")

        done = _load_done_set(out_path) if RESUME else set()
        limit = None if LIMIT == 0 else LIMIT

        targets = _pick_targets(conn, limit=limit, has_checked_col=has_checked_col)
        targets = [t for t in targets if _in_shard(t[0], shard_id, shard_total)]
        if RESUME and done:
            targets = [t for t in targets if t[0] not in done]

        print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={shard_id}/{shard_total})")
        if not has_checked_col:
            print("Info: companies.sni_groups_checked_at saknas -> refresh=ALLA (lägg migration om du vill).")

        processed = 0
        skipped_no_sni = 0
        hits = 0
        misses = 0

        # “Err”-räknare (för samma output-stil)
        err_other = 0

        # Gruppstatistik
        group_counts: Dict[str, int] = {g.key: 0 for g in SNI_GROUPS}

        start = time.time()
        ts = utcnow_iso()

        with out_path.open("a", encoding="utf-8") as out_f:
            for orgnr, name, sni_codes_raw, groups_before_raw, checked_before in targets:
                processed += 1

                sni_tokens = _parse_sni_codes(sni_codes_raw)
                if not sni_tokens:
                    skipped_no_sni += 1
                    continue

                try:
                    matched: List[str] = []
                    for g in SNI_GROUPS:
                        if _match_group(sni_tokens, g):
                            matched.append(g.key)

                    before = _parse_groups_csv(groups_before_raw)
                    after_set = set(before)
                    for k in matched:
                        after_set.add(k)

                    # Räkna “hit” = vi har minst 1 grupp efter
                    after_csv = _to_groups_csv(after_set)
                    status = "found" if after_csv else "not_found"

                    if status == "found":
                        hits += 1
                    else:
                        misses += 1

                    # Gruppcounts: räkna bara ny “matchning” från SNI (inte “redan fanns”)
                    for k in matched:
                        group_counts[k] = group_counts.get(k, 0) + 1

                    row = {
                        "orgnr": orgnr,
                        "name": name,
                        "sni_codes": sni_codes_raw,
                        "status": status,  # found / not_found
                        "err_reason": "",  # samma fält som dina andra scripts
                        "sni_groups_before": (groups_before_raw or ""),
                        "sni_groups_after": after_csv,  # CSV utan mellanslag
                        "checked_at": ts,
                        "sni_groups_checked_at_before": (checked_before or ""),
                        "shard_id": shard_id,
                        "shard_total": shard_total,
                    }
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                except Exception:
                    err_other += 1
                    row = {
                        "orgnr": orgnr,
                        "name": name,
                        "sni_codes": sni_codes_raw,
                        "status": "error",
                        "err_reason": "other",
                        "sni_groups_before": (groups_before_raw or ""),
                        "sni_groups_after": (groups_before_raw or ""),
                        "checked_at": ts,
                        "sni_groups_checked_at_before": (checked_before or ""),
                        "shard_id": shard_id,
                        "shard_total": shard_total,
                    }
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(
                        f"[{processed}] hits={hits} misses={misses} no_sni={skipped_no_sni} other={err_other} | {rate:.1f}/s"
                    )

        print("KLART ✅")
        print(f"Processade: {processed}")
        print(f"HITS: {hits}")
        print(f"MISSES: {misses}")
        print(f"NO_SNI: {skipped_no_sni}")
        print(f"Errors: other={err_other}")
        print("Per grupp (matchade företag):")
        for g in SNI_GROUPS:
            print(f"- {g.key}: {group_counts.get(g.key, 0)}")
        print(f"OUT: {out_path.resolve()}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
