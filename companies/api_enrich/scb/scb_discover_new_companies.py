#nya orgnr
#registreringsdatum
#juridisk_form
#foretagsstatus
#sektor / privat_offentlig

# scb_discover_new_orgnrs.py
# Kommentar: Hämtar nya bolag från SCB (ingen delta => filtrera på registreringsdatum + state).
# Kommentar: Ansvar: orgnr + registreringsdatum + juridisk_form + foretagsstatus + sektor + privat/offentlig.
# Kommentar: Robust: WAL, commit ofta, resume från scb_discover_state, Ctrl+C safe.

import os
import time
import uuid
import sqlite3
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

SLEEP_SECONDS = 1.05
PRINT_EVERY = 250
COMMIT_EVERY = 250
HTTP_TIMEOUT = 30
MAX_RETRIES = 4
BASE_BACKOFF = 0.6

DB_PATH_DEFAULT = "data/db/companies.db.sqlite"

def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v

def db_path() -> str:
    return os.getenv("DB_PATH", DB_PATH_DEFAULT).strip()

def make_scb_session():
    # Kommentar: SCB cert (p12/pfx) via requests-pkcs12 som i ditt enrich-script
    cert_path = must_env("SCB_CERT_PATH")
    cert_password = must_env("SCB_CERT_PASSWORD")

    try:
        from requests_pkcs12 import Pkcs12Adapter  # type: ignore
    except Exception:
        raise SystemExit(
            "Installera: pip install requests-pkcs12\n"
            "Eller konvertera certet till PEM."
        )

    s = requests.Session()
    s.mount("https://", Pkcs12Adapter(pkcs12_filename=cert_path, pkcs12_password=cert_password))
    return s

def request_with_retry(session: requests.Session, url: str, params: dict):
    last_err = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(
                url,
                params=params,
                headers={"Accept": "application/json", "X-Request-Id": str(uuid.uuid4())},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException:
            last_err = "request_exception"
            time.sleep(BASE_BACKOFF * attempt)
            continue

        if r.status_code == 200:
            return r

        if r.status_code in (429, 500, 502, 503, 504):
            last_err = str(r.status_code)
            time.sleep(BASE_BACKOFF * attempt)
            continue

        # Kommentar: andra fel => fail fast
        raise SystemExit(f"SCB list error: {r.status_code} body={r.text[:200]}")

    raise SystemExit(f"SCB list failed after retries: {last_err}")

def normalize_row(row: dict) -> dict:
    # Kommentar: Mappa fält från SCB svar. Anpassa nycklar via ENV om din gateway skiljer sig.
    # Default keys:
    # orgnr, registreringsdatum, juridiskForm, foretagsstatus, sektor, privatPublikt
    return {
        "orgnr": (row.get(os.getenv("SCB_KEY_ORGNR", "orgnr")) or "").strip(),
        "registration_date": (row.get(os.getenv("SCB_KEY_REGDATE", "registreringsdatum")) or "").strip(),
        "legal_form": (row.get(os.getenv("SCB_KEY_LEGALFORM", "juridiskForm")) or "").strip(),
        "company_status": (row.get(os.getenv("SCB_KEY_STATUS", "foretagsstatus")) or "").strip(),
        "sector": (row.get(os.getenv("SCB_KEY_SECTOR", "sektor")) or "").strip(),
        "private_public": (row.get(os.getenv("SCB_KEY_PRIVATE_PUBLIC", "privatPublikt")) or "").strip(),
    }

def main():
    load_dotenv()

    con = sqlite3.connect(db_path())
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Kommentar: state
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scb_discover_state (
            id INTEGER PRIMARY KEY CHECK (id=1),
            last_registration_date TEXT,
            last_page INTEGER,
            updated_at TEXT
        );
    """)
    cur.execute("""
        INSERT OR IGNORE INTO scb_discover_state (id, last_registration_date, last_page, updated_at)
        VALUES (1, '1970-01-01', 1, datetime('now'));
    """)
    con.commit()

    state = cur.execute("SELECT last_registration_date, last_page FROM scb_discover_state WHERE id=1").fetchone()
    last_regdate = (state["last_registration_date"] or "1970-01-01").strip()
    page = int(state["last_page"] or 1)

    base_url = must_env("SCB_BASE_URL").rstrip("/")
    list_endpoint = must_env("SCB_LIST_ENDPOINT")  # ex: /foretag
    list_url = f"{base_url}{list_endpoint}"

    p_from = os.getenv("SCB_LIST_PARAM_FROM", "registreringsdatumFrom")
    p_page = os.getenv("SCB_LIST_PARAM_PAGE", "page")
    p_pagesize = os.getenv("SCB_LIST_PARAM_PAGESIZE", "pageSize")
    results_key = os.getenv("SCB_LIST_RESULTS_KEY", "results")

    session = make_scb_session()

    print(f"Discover start ✅ from_regdate={last_regdate} page={page}")
    inserted = 0
    scanned = 0
    first_printed = False

    try:
        while True:
            params = {
                p_from: last_regdate,
                p_page: page,
                p_pagesize: 200,
            }

            r = request_with_retry(session, list_url, params=params)
            payload = r.json() if r.content else {}
            rows = payload.get(results_key, []) or []

            if not isinstance(rows, list) or len(rows) == 0:
                # Kommentar: ingen mer data
                break

            for raw in rows:
                row = normalize_row(raw)
                orgnr = row["orgnr"]
                if not orgnr:
                    continue

                now = iso_now()

                # Kommentar: insert om saknas, annars uppdatera discover-fält
                cur.execute("""
                    INSERT INTO companies (orgnr, created_at, updated_at)
                    VALUES (?, datetime('now'), datetime('now'))
                    ON CONFLICT(orgnr) DO NOTHING
                """, (orgnr,))

                cur.execute("""
                    UPDATE companies
                    SET scb_registration_date=?,
                        scb_legal_form=?,
                        scb_company_status=?,
                        scb_sector=?,
                        scb_private_public=?,
                        scb_discovered_at=?,
                        updated_at=datetime('now')
                    WHERE orgnr=?
                """, (
                    row["registration_date"] or None,
                    row["legal_form"] or None,
                    row["company_status"] or None,
                    row["sector"] or None,
                    row["private_public"] or None,
                    now,
                    orgnr
                ))

                inserted += 1
                scanned += 1

                if not first_printed:
                    first_printed = True
                    print("FIRST DISCOVER ✅")
                    print(f"orgnr={orgnr}")
                    print(f"registreringsdatum={row['registration_date']}")
                    print(f"juridisk_form={row['legal_form']}")
                    print(f"foretagsstatus={row['company_status']}")
                    print(f"sektor={row['sector']}")
                    print(f"privat_offentlig={row['private_public']}")
                    print("-" * 40)

                if scanned % COMMIT_EVERY == 0:
                    # Kommentar: spara progress (state) så resume blir exakt
                    cur.execute("""
                        UPDATE scb_discover_state
                        SET last_registration_date=?,
                            last_page=?,
                            updated_at=datetime('now')
                        WHERE id=1
                    """, (last_regdate, page))
                    con.commit()

                if scanned % PRINT_EVERY == 0:
                    print(f"[page={page}] scanned={scanned} updated={inserted}")

                time.sleep(SLEEP_SECONDS)

            # Kommentar: sida klar -> bump page, spara state
            page += 1
            cur.execute("""
                UPDATE scb_discover_state
                SET last_registration_date=?,
                    last_page=?,
                    updated_at=datetime('now')
                WHERE id=1
            """, (last_regdate, page))
            con.commit()

    except KeyboardInterrupt:
        print("\n⛔ Avbruten – committar & sparar state...")

    finally:
        # Kommentar: vid “ren” avslut: bumpa last_regdate till nu och reset page=1
        # Detta gör att nästa körning bara tittar framåt.
        cur.execute("""
            UPDATE scb_discover_state
            SET last_registration_date=?,
                last_page=1,
                updated_at=datetime('now')
            WHERE id=1
        """, (iso_now(),))
        con.commit()
        con.close()

    print("DONE ✅ Discover")
    print(f"scanned={scanned} updated={inserted}")

if __name__ == "__main__":
    main()
