# Enrichar companies via SCB (1 request per orgnr):
# - scb_employees_class (storleksklass anställda)
# - scb_workplaces_count (antal arbetsställen)
# - scb_postort (postort/stad)
# - scb_municipality (kommun)
# - scb_region (län normaliserat till regionnamn utan "län")
#
# Refresh efter REFRESH_DAYS även om status blev unknown.
# Sparar ändringar i en historiktabell så du ser växt/krymp över tid.

import os
import time
import uuid
import sqlite3
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

# =========================
# ÄNDRA HÄR
# =========================
CITY = "all"              # ex: "Göteborg" | "Stockholm" | "all" | "*" | "Stockholm,Göteborg"
PRINT_EVERY = 250
SLEEP_SECONDS = 1.05
REFRESH_DAYS = 90
# =========================

# Kommentar: All config här i scriptet (inte i .env)
DB_PATH = "data/companies.db.sqlite"
TABLE = "companies"

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

HTTP_TIMEOUT = 30
MAX_RETRIES = 4
BASE_BACKOFF = 0.6

UNKNOWN_MARK = "unknown"

def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v

def iso_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def iso_plus_days(days: int) -> str:
    return (datetime.utcnow().replace(microsecond=0) + timedelta(days=days)).isoformat() + "Z"

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
    #Nycklar från SCB-spec (vi matchar både “snälla” och exakt-nycklar)
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
    # P12/PFX kräver requests_pkcs12 eller konvertering till PEM.
    # Vi använder din .env:
    # SCB_CERT_PATH=/path/to/scb_cert.p12
    # SCB_CERT_PASSWORD=...
    cert_path = must_env("SCB_CERT_PATH")
    cert_password = must_env("SCB_CERT_PASSWORD")

    try:
        from requests_pkcs12 import Pkcs12Adapter  # type: ignore
    except Exception:
        raise SystemExit(
            "Du har SCB_CERT_PATH som .p12/.pfx men requests saknar pkcs12-stöd.\n"
            "Installera: pip install requests-pkcs12\n"
            "ELLER konvertera certet till PEM (cert+key) och ändra scriptet därefter."
        )

    s = requests.Session()
    s.mount("https://", Pkcs12Adapter(pkcs12_filename=cert_path, pkcs12_password=cert_password))
    return s

def fetch_one(session: requests.Session, orgnr: str):
    """
    returns: (emp_class, workplaces, postort, municipality, region, status, err_reason)
    status: ok | unknown | not_found | err
    """
    base_url = must_env("SCB_BASE_URL").rstrip("/")
    url = f"{base_url}/foretag/{orgnr}"  #byt path om din SCB-gateway kräver annat

    last_err = ""

    for attempt in range(1, MAX_RETRIES + 1):
        headers = {
            "Accept": "application/json",
            "X-Request-Id": str(uuid.uuid4()),
        }

        try:
            r = session.get(url, headers=headers, timeout=HTTP_TIMEOUT)
        except requests.RequestException:
            last_err = "request_exception"
            time.sleep(BASE_BACKOFF * attempt)
            continue

        if r.status_code == 200:
            payload = r.json() if r.content else {}
            emp_class, workplaces, postort, municipality, region = parse_scb(payload)

            got_any = any([
                bool(emp_class),
                workplaces is not None,
                bool(postort),
                bool(municipality),
                bool(region),
            ])

            if got_any:
                return (
                    emp_class or UNKNOWN_MARK,
                    workplaces,
                    postort or UNKNOWN_MARK,
                    municipality or UNKNOWN_MARK,
                    region or UNKNOWN_MARK,
                    "ok",
                    "",
                )

            return (
                UNKNOWN_MARK,
                None,
                UNKNOWN_MARK,
                UNKNOWN_MARK,
                UNKNOWN_MARK,
                "unknown",
                "",
            )

        if r.status_code == 404:
            return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "not_found", "404")

        if r.status_code in (429, 500, 502, 503, 504):
            last_err = str(r.status_code)
            time.sleep(BASE_BACKOFF * attempt)
            continue

        return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "err", str(r.status_code))

    return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "err", last_err or "err")

def ensure_history_table(cur: sqlite3.Cursor):
    #Historik för triggers (växt/krymp) utan att röra companies-schema mer.
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
        (orgnr, field, None if old_v is None else str(old_v), None if new_v is None else str(new_v), checked_at),
    )

def main():
    load_dotenv()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    #index (valfritt men bra)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_CITY} ON {TABLE}({COL_CITY});")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_SNI_CODES} ON {TABLE}({COL_SNI_CODES});")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_NEXT_CHECK_AT} ON {TABLE}({COL_NEXT_CHECK_AT});")

    ensure_history_table(cur)

    # =========================
    # CITY-filter
    # =========================
    city_raw = (CITY or "").strip()
    cities = None

    if city_raw.lower() in ("all", "none", "*", ""):
        cities = None
    else:
        cities = [c.strip().lower() for c in city_raw.split(",") if c.strip()]

    where_city = ""
    params = [iso_now()]

    if cities:
        placeholders = ",".join(["?"] * len(cities))
        where_city = f"AND LOWER({COL_CITY}) IN ({placeholders})"
        params.extend(cities)

    # Bara bolag med riktig SNI + refresh efter 90 dagar
    cur.execute(
        f"""
        SELECT {COL_ORGNR}
        FROM {TABLE}
        WHERE {COL_SNI_CODES} IS NOT NULL
          AND {COL_SNI_CODES} != ''
          AND (
              {COL_NEXT_CHECK_AT} IS NULL
              OR {COL_NEXT_CHECK_AT} <= ?
          )
        {where_city}
        """,
        params,
    )

    orgnrs = [str(r[COL_ORGNR]).strip() for r in cur.fetchall() if r[COL_ORGNR]]
    total = len(orgnrs)
    print(f"Loaded from DB: {total} orgnrs to enrich for city='{CITY}' (SCB bundle)")

    if total == 0:
        con.close()
        print("Nothing to do ✅")
        return

    session = make_scb_session()

    ok = unknown = not_found = err = 0
    scanned = 0
    start = time.time()
    printed_first = False

    try:
        for orgnr in orgnrs:
            # hämta gamla värden för historik
            cur.execute(
                f"""
                SELECT {COL_EMP_CLASS}, {COL_WORKPLACES}, {COL_POSTORT}, {COL_MUNICIPALITY}, {COL_REGION}
                FROM {TABLE}
                WHERE {COL_ORGNR}=?
                """,
                (orgnr,),
            )
            old = cur.fetchone() or {}

            emp_class, workplaces, postort, municipality, region, status, err_reason = fetch_one(session, orgnr)

            checked_at = iso_now()
            next_check = iso_plus_days(REFRESH_DAYS)

            #logga förändringar (för triggers)
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

            if not printed_first:
                printed_first = True
                print("FIRST SCB ✅")
                print(f"orgnr={orgnr}")
                print(f"employees_class={emp_class}")
                print(f"workplaces_count={workplaces}")
                print(f"postort={postort}")
                print(f"municipality={municipality}")
                print(f"region={region}")
                print(f"status={status} err_reason={err_reason}")
                print("-" * 40)

            scanned += 1
            if status == "ok":
                ok += 1
            elif status == "unknown":
                unknown += 1
            elif status == "not_found":
                not_found += 1
            else:
                err += 1

            if scanned % PRINT_EVERY == 0:
                con.commit()
                rate = scanned / max(1e-9, time.time() - start)
                print(
                    f"[{scanned}/{total}] ok={ok} unknown={unknown} not_found={not_found} err={err} "
                    f"| {rate:.1f}/s | last={status}"
                )

            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\n⛔ Avbruten av användare – committar data...")

    finally:
        con.commit()
        con.close()
        print("✅ Data committad till DB")

    rate = scanned / max(1e-9, time.time() - start)
    print("DONE ✅")
    print(f"city={CITY}")
    print(f"scanned={scanned} ok={ok} unknown={unknown} not_found={not_found} err={err} | {rate:.2f}/s")

if __name__ == "__main__":
    main()
