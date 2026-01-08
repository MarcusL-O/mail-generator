import os
import time
import uuid
import sqlite3
import requests
from dotenv import load_dotenv

# ====== DB CONFIG ======
DB_PATH = os.getenv("DB_PATH", "data/mail_generator_db.sqlite")
TABLE = os.getenv("DB_TABLE", "companies")
COL_ORGNR = os.getenv("DB_COL_ORGNR", "orgnr")
COL_SNI_CODES = os.getenv("DB_COL_SNI_CODES", "sni_codes")
COL_SNI_TEXT = os.getenv("DB_COL_SNI_TEXT", "sni_text")

# ====== RUNTIME CONFIG ======
BATCH_COMMIT_EVERY = int(os.getenv("COMMIT_EVERY", "25"))
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.08"))  # lite snabbare
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

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

    # 1) vanligast
    candidates.append((((org_obj.get("naringsgrenOrganisation") or {}).get("sni")) or []))

    # 2) alternativa paths som API:t ibland använder
    candidates.append(((org_obj.get("naringsgrenar") or {}).get("sni")) or [])
    candidates.append((org_obj.get("sni") or []))

    # 3) fallback
    ng = org_obj.get("naringsgren")
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
def fetch_org_data(
    session: requests.Session,
    access_token: str,
    orgnr: str,
) -> tuple[dict | None, str]:

    url = f"{must_env('BOLAGSVERKET_BASE_URL')}/organisationer"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Request-Id": str(uuid.uuid4()),
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(url, headers=headers, json={"identitetsbeteckning": orgnr}, timeout=HTTP_TIMEOUT)
        except requests.RequestException:
            time.sleep(0.5 * attempt)
            continue

        if r.status_code == 200:
            orgs = r.json().get("organisationer") or []
            return (orgs[0] if orgs else None), access_token

        if r.status_code == 401 and attempt < MAX_RETRIES:
            access_token = get_access_token(session)
            headers["Authorization"] = f"Bearer {access_token}"
            continue

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(0.7 * attempt)
            continue

        return None, access_token

    return None, access_token

def main():
    load_dotenv()

    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    cur = con.cursor()

    orgnrs = [
        r[0] for r in cur.execute(f"""
            SELECT {COL_ORGNR}
            FROM {TABLE}
            WHERE ({COL_SNI_CODES} IS NULL OR TRIM({COL_SNI_CODES}) = '')
              AND TRIM({COL_ORGNR}) != ''
        """)
    ]

    print("To enrich:", len(orgnrs))

    session = requests.Session()
    access_token = get_access_token(session)

    scanned = updated = 0
    start = time.time()

    try:
        for orgnr in orgnrs:
            scanned += 1
            org_obj, access_token = fetch_org_data(session, access_token, orgnr)
            import json

            codes, texts = extract_sni(org_obj) if org_obj else ("", "")
            if codes:
                print(f"SNI OK {orgnr}: {codes} | {texts}")
            else:
                print(f"SNI TOM {orgnr}")
            cur.execute(
                f"UPDATE {TABLE} SET {COL_SNI_CODES}=?, {COL_SNI_TEXT}=? WHERE {COL_ORGNR}=?",
                (codes, texts, orgnr),
            )
            updated += 1

            if scanned % BATCH_COMMIT_EVERY == 0:
                con.commit()
                rate = scanned / (time.time() - start)
                print(f"[{scanned}] committed | {rate:.1f}/s")

            time.sleep(SLEEP_BETWEEN)

    except KeyboardInterrupt:
        print("\nCTRL+C – committing…")
        con.commit()

    finally:
        con.commit()
        con.close()
        print(f"Done. scanned={scanned} updated={updated}")

if __name__ == "__main__":
    main()
