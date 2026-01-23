#employees
#workplaces
#kommun / län / postort
#endast orgnr som redan finns

# scb_enrich_company_geo_work.py
# Kommentar: Enrichar befintliga orgnr via SCB (1 request per orgnr).
# Kommentar: Ansvar: employees_class + workplaces_count + postort + municipality + region.
# Kommentar: Robust: WAL, commit ofta, ingen “lista i minnet”, resume via scb_next_check_at.

import os
import time
import uuid
import sqlite3
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

DB_PATH_DEFAULT = "data/companies.db.sqlite"
TABLE = "companies"

SLEEP_SECONDS = 1.05
PRINT_EVERY = 250
COMMIT_EVERY = 250
REFRESH_DAYS = 90

HTTP_TIMEOUT = 30
MAX_RETRIES = 4
BASE_BACKOFF = 0.6

UNKNOWN_MARK = "unknown"

COL_ORGNR = "orgnr"
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

def db_path() -> str:
    return os.getenv("DB_PATH", DB_PATH_DEFAULT).strip()

def normalize_region(county: str) -> str:
    v = (county or "").strip()
    if not v:
        return ""
    v = v.replace(" län", "").replace(" Län", "")
    return v.strip()

def deep_find_value(obj, target_keys: set[str]):
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
    emp_keys = {"storleksklassAnstallda", "antalAnstalldaStorleksklass", "StklKod", "stklKod"}
    workplaces_keys = {"antalArbetsstallen", "antalArbetsställen", "Antal arbetsställen"}
    postort_keys = {"PostOrt", "postOrt", "postort"}
    municipality_keys = {"Säteskommun", "säteskommun", "sateskommun", "kommun"}
    county_keys = {"Säteslän", "säteslän", "sateslan", "län", "lan"}

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
    base_url = must_env("SCB_BASE_URL").rstrip("/")
    detail_endpoint = must_env("SCB_DETAIL_ENDPOINT")  # ex: /foretag/{orgnr}
    url = f"{base_url}{detail_endpoint}".replace("{orgnr}", orgnr)

    last_err = ""
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

        if r.status_code == 200:
            payload = r.json() if r.content else {}
            emp_class, workplaces, postort, municipality, region = parse_scb(payload)

            got_any = any([bool(emp_class), workplaces is not None, bool(postort), bool(municipality), bool(region)])
            if got_any:
                return (emp_class or UNKNOWN_MARK, workplaces, postort or UNKNOWN_MARK,
                        municipality or UNKNOWN_MARK, region or UNKNOWN_MARK, "ok", "")
            return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "unknown", "")

        if r.status_code == 404:
            return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "not_found", "404")

        if r.status_code in (429, 500, 502, 503, 504):
            last_err = str(r.status_code)
            time.sleep(BASE_BACKOFF * attempt)
            continue

        return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "err", str(r.status_code))

    return (UNKNOWN_MARK, None, UNKNOWN_MARK, UNKNOWN_MARK, UNKNOWN_MARK, "err", last_err or "err")

def main():
    load_dotenv()

    con = sqlite3.connect(db_path())
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    session = make_scb_session()

    total_scanned = ok = unknown = not_found = err = 0
    first_printed = False
    start = time.time()

    try:
        while True:
            now = iso_now()

            # Kommentar: Hämta en batch som behöver enrich/refresh (resume-safe)
            cur.execute(
                f"""
                SELECT {COL_ORGNR}
                FROM {TABLE}
                WHERE {COL_ORGNR} IS NOT NULL AND {COL_ORGNR} != ''
                  AND (
                        {COL_NEXT_CHECK_AT} IS NULL
                        OR {COL_NEXT_CHECK_AT} <= ?
                      )
                ORDER BY COALESCE({COL_NEXT_CHECK_AT}, '1970-01-01') ASC
                LIMIT 200
                """,
                (now,),
            )
            batch = [row[COL_ORGNR] for row in cur.fetchall()]
            if not batch:
                break

            for orgnr in batch:
                emp_class, workplaces, postort, municipality, region, status, err_reason = fetch_one(session, orgnr)

                checked_at = iso_now()
                next_check = iso_plus_days(REFRESH_DAYS)

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
                    print("FIRST ENRICH ✅")
                    print(f"orgnr={orgnr}")
                    print(f"employees_class={emp_class}")
                    print(f"workplaces_count={workplaces}")
                    print(f"postort={postort}")
                    print(f"municipality={municipality}")
                    print(f"region={region}")
                    print(f"status={status} err_reason={err_reason}")
                    print("-" * 40)

                if total_scanned % COMMIT_EVERY == 0:
                    con.commit()

                if total_scanned % PRINT_EVERY == 0:
                    rate = total_scanned / max(1e-9, time.time() - start)
                    print(f"[{total_scanned}] ok={ok} unknown={unknown} not_found={not_found} err={err} | {rate:.2f}/s")

                time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\n⛔ Avbruten – committar data...")

    finally:
        con.commit()
        con.close()

    rate = total_scanned / max(1e-9, time.time() - start)
    print("DONE ✅ Enrich")
    print(f"scanned={total_scanned} ok={ok} unknown={unknown} not_found={not_found} err={err} | {rate:.2f}/s")

if __name__ == "__main__":
    main()
