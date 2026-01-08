#Kompleterar datan i DB med SNI-koder. 
#Skickar till DB 

import os
import time
import uuid
import sqlite3
import requests
from dotenv import load_dotenv

# =========================
# ÄNDRA HÄR
# =========================
CITY = "Stockholm"          # ex: "Göteborg", "Stockholm", "Malmö"
PRINT_EVERY = 250           # progress
SLEEP_SECONDS = float(os.getenv("SNI_SLEEP_SECONDS", "1.05"))  # ~60/min
# =========================

DB_PATH = os.getenv("DB_PATH", "data/mail_generator_db.sqlite")
TABLE = os.getenv("DB_TABLE", "companies")
COL_ORGNR = os.getenv("DB_COL_ORGNR", "orgnr")
COL_CITY = os.getenv("DB_COL_CITY", "city")
COL_SNI_CODES = os.getenv("DB_COL_SNI_CODES", "sni_codes")
COL_SNI_TEXT = os.getenv("DB_COL_SNI_TEXT", "sni_text")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "0.6"))

NO_SNI_MARK = os.getenv("NO_SNI_MARK", "__NO_SNI__")
NOT_FOUND_MARK = os.getenv("NOT_FOUND_MARK", "__NOT_FOUND__")


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


def get_access_token(session: requests.Session) -> str:
    resp = session.post(
        must_env("BOLAGSVERKET_TOKEN_URL"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": must_env("BOLAGSVERKET_CLIENT_ID"),
            "client_secret": must_env("BOLAGSVERKET_CLIENT_SECRET"),
            "scope": os.getenv("BOLAGSVERKET_SCOPE", "vardefulla-datamangder:read"),
        },
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def extract_sni(org_obj: dict) -> tuple[str, str]:
    candidates = []
    candidates.append((((org_obj.get("naringsgrenOrganisation") or {}).get("sni")) or []))
    candidates.append(((org_obj.get("naringsgrenar") or {}).get("sni")) or [])
    candidates.append((org_obj.get("sni") or []))

    ng = org_obj.get("naringsgren") or {}
    if isinstance(ng, dict):
        candidates.append(ng.get("sni") or [])

    sni_list = []
    for c in candidates:
        if isinstance(c, list) and c:
            sni_list = c
            break

    pairs = []
    for i in sni_list:
        if not isinstance(i, dict):
            continue
        kod = (i.get("kod") or i.get("code") or "").strip()
        txt = (i.get("klartext") or i.get("text") or i.get("beskrivning") or "").strip()
        if kod:
            pairs.append((kod, txt))

    return (
        ",".join(k for k, _ in pairs),
        " | ".join(t for _, t in pairs if t),
    )


def fetch_one(session: requests.Session, token: str, orgnr: str) -> tuple[str, str, str, str, str]:
    """
    returns: (new_token, codes, texts, status, raw_status)
    status: ok | ok_no_sni | not_found | error_XXX | error_retry_exhausted
    """
    url = f"{must_env('BOLAGSVERKET_BASE_URL')}/organisationer"

    for attempt in range(1, MAX_RETRIES + 1):
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Request-Id": str(uuid.uuid4()),
        }

        try:
            r = session.post(url, headers=headers, json={"identitetsbeteckning": orgnr}, timeout=HTTP_TIMEOUT)
        except requests.RequestException:
            time.sleep(BASE_BACKOFF * attempt)
            continue

        if r.status_code == 200:
            orgs = (r.json().get("organisationer") or [])
            if not orgs:
                return token, "", "", "not_found", "200_no_orgs"

            codes, texts = extract_sni(orgs[0])
            if not codes:
                return token, NO_SNI_MARK, "", "ok_no_sni", "200_no_sni"
            return token, codes, texts, "ok", "200_ok"

        if r.status_code == 401 and attempt < MAX_RETRIES:
            token = get_access_token(session)
            time.sleep(BASE_BACKOFF * attempt)
            continue

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(BASE_BACKOFF * attempt)
            continue

        return token, "", "", f"error_{r.status_code}", str(r.status_code)

    return token, "", "", "error_retry_exhausted", "retry_exhausted"


def main():
    load_dotenv()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # (valfritt men bra) index för snabb city + sni-filter
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_CITY} ON {TABLE}({COL_CITY});")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{COL_SNI_CODES} ON {TABLE}({COL_SNI_CODES});")

    # Bara okollade / tomma / '00000' och INTE redan markerade
    cur.execute(
        f"""
        SELECT {COL_ORGNR}
        FROM {TABLE}
        WHERE LOWER({COL_CITY}) = LOWER(?)
          AND (
            {COL_SNI_CODES} IS NULL
            OR {COL_SNI_CODES} = ''
            OR {COL_SNI_CODES} = '00000'
          )
          AND COALESCE({COL_SNI_CODES}, '') NOT IN (?, ?)
        """,
        (CITY, NO_SNI_MARK, NOT_FOUND_MARK),
    )

    orgnrs = [str(r[COL_ORGNR]).strip() for r in cur.fetchall() if r[COL_ORGNR]]
    total = len(orgnrs)
    print(f"Loaded from DB: {total} orgnrs to enrich for city='{CITY}'")

    if total == 0:
        con.close()
        print("Nothing to do ✅")
        return

    session = requests.Session()
    token = get_access_token(session)

    ok = ok_no_sni = not_found = err = 0
    scanned = 0
    start = time.time()
    printed_first = False

    try:
        for orgnr in orgnrs:
            token, codes, texts, status, raw_status = fetch_one(session, token, orgnr)

            if status == "ok":
                ok += 1
                cur.execute(
                    f"UPDATE {TABLE} SET {COL_SNI_CODES}=?, {COL_SNI_TEXT}=? WHERE {COL_ORGNR}=?",
                    (codes, texts, orgnr),
                )
                if not printed_first:
                    printed_first = True
                    print("FIRST SNI ✅")
                    print(f"orgnr={orgnr}")
                    print(f"sni_codes={codes}")
                    print(f"sni_text={texts}")
                    print("-" * 40)

            elif status == "ok_no_sni":
                ok_no_sni += 1
                cur.execute(
                    f"UPDATE {TABLE} SET {COL_SNI_CODES}=?, {COL_SNI_TEXT}=? WHERE {COL_ORGNR}=?",
                    (NO_SNI_MARK, "", orgnr),
                )
                if not printed_first:
                    printed_first = True
                    print("FIRST NO_SNI ✅")
                    print(f"orgnr={orgnr}")
                    print(f"sni_codes={NO_SNI_MARK}")
                    print("-" * 40)

            elif status == "not_found":
                not_found += 1
                cur.execute(
                    f"UPDATE {TABLE} SET {COL_SNI_CODES}=?, {COL_SNI_TEXT}=? WHERE {COL_ORGNR}=?",
                    (NOT_FOUND_MARK, "", orgnr),
                )
                if not printed_first:
                    printed_first = True
                    print("FIRST NOT_FOUND ✅")
                    print(f"orgnr={orgnr}")
                    print(f"sni_codes={NOT_FOUND_MARK}")
                    print("-" * 40)

            else:
                err += 1
                # lämna tomt för att kunna retry senare

            scanned += 1

            if scanned % PRINT_EVERY == 0:
                con.commit()
                rate = scanned / max(1e-9, time.time() - start)
                print(
                    f"[{scanned}/{total}] ok={ok} no_sni={ok_no_sni} not_found={not_found} err={err} "
                    f"| {rate:.1f}/s | last={status}:{raw_status}"
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
    print(f"scanned={scanned} ok={ok} no_sni={ok_no_sni} not_found={not_found} err={err} | {rate:.2f}/s")


if __name__ == "__main__":
    main()

# python get_data/02_enrich_sni_import_db.py