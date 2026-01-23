#  En enda SCB-enricher (batch/resume) som uppdaterar companies per orgnr (1 request/orgnr):
# - scb_employees_class
# - scb_workplaces_count
# - scb_postort
# - scb_municipality
# - scb_region
# + scb_status, scb_err_reason, scb_checked_at, scb_next_check_at
#
# Robust drift:
# - WAL + commit i batch
# - Resume via scb_next_check_at
# - Retry/backoff på 429/503/5xx
# - 403 loggas (kan betyda “maxvärde överskridet”), och vi skjuter nästa försök längre fram
#
# ENV (krav):
# - SCB_CERT_PATH=/path/to/cert.pfx|.p12
# - SCB_CERT_PASSWORD=...
# - SCB_BASE_URL=https://...
# - SCB_DETAIL_ENDPOINT=/foretag/{orgnr}   (exempel)
#
# ENV (valfritt):
# - DB_PATH=data/companies.db.sqlite
# - SCB_CITY_FILTER=all | * | "Göteborg,Stockholm"  (tillfälligt, kan droppas senare)
# - SCB_REQUIRE_SNI=1 (default) / 0
# - SCB_ENABLE_HISTORY=1 (default) / 0

import os
import time
import uuid
import sqlite3
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

# =========================
# Default-config (kan styras via ENV)
# =========================
DB_PATH_DEFAULT = "data/companies.db.sqlite"
TABLE = "companies"

BATCH_LIMIT = 200
SLEEP_SECONDS = 1.05
PRINT_EVERY = 250
COMMIT_EVERY = 250
REFRESH_DAYS = 90

# Vid "hard errors" (t.ex 403) kan vi vänta längre innan nästa försök
HARD_ERROR_WAIT_DAYS = 14
SOFT_ERROR_WAIT_DAYS = 2

HTTP_TIMEOUT = 30
MAX_RETRIES = 4
BASE_BACKOFF = 0.6

UNKNOWN_MARK = "unknown"

# Companies columns
COL_ORGNR = "orgnr"
COL_CITY = "city"
COL_SNI_CODES = "sni_codes"

# SCB columns
COL_EMP_CLASS = "scb_employees_class"
COL_WORKPLACES = "scb_workplaces_count"
COL_POSTORT = "scb_postort"
COL_MUNICIPALITY = "scb_municipality"
COL_REGION = "scb_region"
COL_STATUS = "scb_status"
COL_CHECKED_AT = "scb_checked_at"
COL_NEXT_CHECK_AT = "scb_next_check_at"
COL_ERR_REASON = "scb_err_reason"


def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def iso_plus_days(days: int) -> str:
    return (datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=days)).isoformat()


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def db_path() -> str:
    return os.getenv("DB_PATH", DB_PATH_DEFAULT).strip()


def normalize_region(county: str) -> str:
    #"Västra Götalands län" -> "Västra Götaland"
    v = (county or "").strip()
    if not v:
        return ""
    v = v.replace(" län", "").replace(" Län", "")
    v = v.replace("Götalands", "Götaland")
    return v.strip()


def deep_find_value(obj, target_keys: set[str]):
    #Letar efter första matchande nyckel rekursivt (dict/list)
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k in target_keys:
                return v
            found = deep_find_value(v, target_keys)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = deep_find_value(item, target_keys)
            if found is not None:
                return found
    return None


def to_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


def to_int(v):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return int(s)
    return None


def parse_scb(payload: dict):
    #Vi matchar både “snälla” och exakta nycklar (SCB varierar ibland i wrappers)
    emp_keys = {
        "storleksklassAnstallda", "antalAnstalldaStorleksklass",
        "StklKod", "stklKod", "Stkl, kod", "Stkl, kod ",
        "employeesClass", "employees_class",
    }
    workplaces_keys = {
        "antalArbetsstallen", "antalArbetsställen",
        "Antal arbetsställen", "workplaces", "workplacesCount",
    }
    postort_keys = {"PostOrt", "postOrt", "postort"}
    municipality_keys = {"Säteskommun", "säteskommun", "sateskommun", "kommun", "municipality"}
    county_keys = {"Säteslän", "säteslän", "sateslan", "län", "lan", "county"}

    emp_raw = deep_find_value(payload, emp_keys)
    wp_raw = deep_find_value(payload, workplaces_keys)
    post_raw = deep_find_value(payload, postort_keys)
    mun_raw = deep_find_value(payload, municipality_keys)
    county_raw = deep_find_value(payload, county_keys)

    emp_class = to_str(emp_raw)
    workplaces = to_int(wp_raw)
    postort = to_str(post_raw)
    municipality = to_str(mun_raw)
    region = normalize_region(to_str(county_raw))

    return emp_class, workplaces, postort, municipality, region


def make_scb_session():
    # Kräver PFX/P12 -> requests-pkcs12
    cert_path = must_env("SCB_CERT_PATH")
    cert_password = must_env("SCB_CERT_PASSWORD")

    try:
        from requests_pkcs12 import Pkcs12Adapter  # type: ignore
    except Exception:
        raise SystemExit("Installera: pip install requests-pkcs12")

    s = requests.Session()
    s.mount("https://", Pkcs12Adapter(pkcs12_filename=cert_path, pkcs12_password=cert_password))
    return s


def fetch_one(session: requests.Session, orgnr: str):
    """
    returns: (emp_class, workplaces, postort, municipality, region, status, err_reason, hard_error)
    status: ok | unknown | not_found | err
    hard_error: True vid t.ex. 403 (ofta bättre att vänta längre)
    """
    base_url = must_env("SCB_BASE_URL").rstrip("/")
    detail_endpoint = must_env("SCB_DETAIL_ENDPOINT")  # ex: /foretag/{orgnr}
    url = f"{base_url}{detail_endpoint}".replace("{orgnr}", orgnr)

    last_err = ""
    last_code = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(
                url,
                headers={"Accept": "application/json", "X-Request-Id": str(uuid.uuid4())},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException:
            last_err = "request_exception"
            time.sleep(BASE_BACKOFF * attempt)
            continue

        last_code = r.status_code

        if r.status_code == 200:
            payload = r.json() if r.content else {}
            emp_class, workplaces, postort, municipality, region = parse_scb(payload)

            got_any = any([bool(emp_class), workplaces is not None, bool(postort), bool(municipality), bool(region)])
            if got_any:
                return (
                    emp_class or UNKNOWN_MARK,
                    workplaces,
                    postort or UNKNOWN_MARK,
                    municipality or UNKNOWN_MARK,
                    region or UNKNOWN_MARK,
                    "ok",
                    "",
                    False,
                )

            return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "unknown", "", False)

        if r.status_code == 404:
            return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "not_found", "404", False)

        # SCB-limit/drift -> retry
        if r.status_code in (429, 500, 502, 503, 504):
            last_err = str(r.status_code)
            time.sleep(BASE_BACKOFF * attempt)
            continue

        # 403 kan indikera “maxvärde överskridet” enligt SCB-info
        if r.status_code == 403:
            return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "err", "403", True)

        return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "err", str(r.status_code), False)

    return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "err", last_err or str(last_code or "err"), False)


def ensure_history_table(cur: sqlite3.Cursor):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS scb_company_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            orgnr TEXT NOT NULL,
            field TEXT NOT NULL,
            old_value TEXT,
            new_value TEXT,
            checked_at TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scb_changes_orgnr ON scb_company_changes(orgnr);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scb_changes_checked_at ON scb_company_changes(checked_at);")


def log_change(cur: sqlite3.Cursor, orgnr: str, field: str, old_v, new_v, checked_at: str):
    if old_v is None and new_v is None:
        return
    if str(old_v) == str(new_v):
        return
    cur.execute(
        """
        INSERT INTO scb_company_changes (orgnr, field, old_value, new_value, checked_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            orgnr,
            field,
            None if old_v is None else str(old_v),
            None if new_v is None else str(new_v),
            checked_at,
        ),
    )


def parse_city_filter() -> list[str] | None:
    raw = os.getenv("SCB_CITY_FILTER", "all").strip()
    if raw.lower() in ("all", "none", "*", ""):
        return None
    return [c.strip().lower() for c in raw.split(",") if c.strip()]


def main():
    load_dotenv()

    require_sni = env_bool("SCB_REQUIRE_SNI", True)
    enable_history = env_bool("SCB_ENABLE_HISTORY", True)
    cities = parse_city_filter()

    con = sqlite3.connect(db_path())
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Index för snabbare filter/resume
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_NEXT_CHECK_AT} ON {TABLE}({COL_NEXT_CHECK_AT});")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_CITY} ON {TABLE}({COL_CITY});")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_SNI_CODES} ON {TABLE}({COL_SNI_CODES});")

    if enable_history:
        ensure_history_table(cur)

    session = make_scb_session()

    total_scanned = ok = unknown = not_found = err = 0
    first_printed = False
    start = time.time()

    try:
        while True:
            now = iso_now()

            where_parts = [
                f"{COL_ORGNR} IS NOT NULL AND {COL_ORGNR} != ''",
                f"({COL_NEXT_CHECK_AT} IS NULL OR {COL_NEXT_CHECK_AT} <= ?)",
            ]
            params: list = [now]

            if require_sni:
                where_parts.append(f"{COL_SNI_CODES} IS NOT NULL AND {COL_SNI_CODES} != ''")

            if cities:
                placeholders = ",".join(["?"] * len(cities))
                where_parts.append(f"LOWER({COL_CITY}) IN ({placeholders})")
                params.extend(cities)

            where_sql = " AND ".join(where_parts)

            cur.execute(
                f"""
                SELECT {COL_ORGNR}
                FROM {TABLE}
                WHERE {where_sql}
                ORDER BY COALESCE({COL_NEXT_CHECK_AT}, '1970-01-01') ASC
                LIMIT {BATCH_LIMIT}
                """,
                params,
            )
            batch = [row[COL_ORGNR] for row in cur.fetchall()]
            if not batch:
                break

            for orgnr in batch:
                #Hämta gamla värden (endast om historik är på)
                old = None
                if enable_history:
                    cur.execute(
                        f"""
                        SELECT {COL_EMP_CLASS}, {COL_WORKPLACES}, {COL_POSTORT}, {COL_MUNICIPALITY}, {COL_REGION}
                        FROM {TABLE}
                        WHERE {COL_ORGNR}=?
                        """,
                        (orgnr,),
                    )
                    old = cur.fetchone()

                emp_class, workplaces, postort, municipality, region, status, err_reason, hard_error = fetch_one(session, str(orgnr).strip())

                checked_at = iso_now()
                if status in ("ok", "unknown", "not_found"):
                    next_check = iso_plus_days(REFRESH_DAYS)
                else:
                    #Vid fel väntar vi kortare (soft) eller längre (hard) innan nytt försök
                    next_check = iso_plus_days(HARD_ERROR_WAIT_DAYS if hard_error else SOFT_ERROR_WAIT_DAYS)

                if enable_history and old is not None:
                    log_change(cur, orgnr, COL_EMP_CLASS, old.get(COL_EMP_CLASS), emp_class, checked_at)
                    log_change(cur, orgnr, COL_WORKPLACES, old.get(COL_WORKPLACES), workplaces, checked_at)
                    log_change(cur, orgnr, COL_POSTORT, old.get(COL_POSTORT), postort, checked_at)
                    log_change(cur, orgnr, COL_MUNICIPALITY, old.get(COL_MUNICIPALITY), municipality, checked_at)
                    log_change(cur, orgnr, COL_REGION, old.get(COL_REGION), region, checked_at)

                cur.execute(
                    f"""
                    UPDATE {TABLE}
                    SET {COL_EMP_CLASS}=?,
                        {COL_WORKPLACES}=?,
                        {COL_POSTORT}=?,
                        {COL_MUNICIPALITY}=?,
                        {COL_REGION}=?,
                        {COL_STATUS}=?,
                        {COL_ERR_REASON}=?,
                        {COL_CHECKED_AT}=?,
                        {COL_NEXT_CHECK_AT}=?
                    WHERE {COL_ORGNR}=?
                    """,
                    (emp_class, workplaces, postort, municipality, region, status, err_reason, checked_at, next_check, orgnr),
                )

                total_scanned += 1
                if status == "ok":
                    ok += 1
                elif status == "unknown":
                    unknown += 1
                elif status == "not_found":
                    not_found += 1
                else:
                    err += 1

                if not first_printed:
                    first_printed = True
                    print("FIRST SCB ✅")
                    print(f"orgnr={orgnr}")
                    print(f"employees_class={emp_class}")
                    print(f"workplaces_count={workplaces}")
                    print(f"postort={postort}")
                    print(f"municipality={municipality}")
                    print(f"region={region}")
                    print(f"status={status} err_reason={err_reason} next_check={next_check}")
                    print("-" * 40)

                if total_scanned % COMMIT_EVERY == 0:
                    con.commit()

                if total_scanned % PRINT_EVERY == 0:
                    rate = total_scanned / max(1e-9, time.time() - start)
                    city_info = os.getenv("SCB_CITY_FILTER", "all")
                    print(
                        f"[{total_scanned}] ok={ok} unknown={unknown} not_found={not_found} err={err} "
                        f"| {rate:.2f}/s | city_filter={city_info} require_sni={int(require_sni)} history={int(enable_history)}"
                    )

                time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\n⛔ Avbruten – committar data...")

    finally:
        con.commit()
        con.close()

    rate = total_scanned / max(1e-9, time.time() - start)
    print("DONE ✅ SCB Enrich")
    print(f"scanned={total_scanned} ok={ok} unknown={unknown} not_found={not_found} err={err} | {rate:.2f}/s")


if __name__ == "__main__":
    main()
