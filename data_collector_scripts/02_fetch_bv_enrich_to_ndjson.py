import os
import time
import uuid
import json
import argparse
import threading
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
SLEEP_BETWEEN_SUBMITS = float(os.getenv("SLEEP_BETWEEN", "0.08"))
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "0.6"))
NO_SNI_MARK = os.getenv("NO_SNI_MARK", "__NO_SNI__")

_tls = threading.local()


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


def get_thread_session_and_token() -> tuple[requests.Session, str]:
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
        _tls.token = get_access_token(_tls.session)
    return _tls.session, _tls.token


def set_thread_token(token: str) -> None:
    _tls.token = token


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


def extract_name_city(org_obj: dict) -> tuple[str, str]:
    name = (org_obj.get("namn") or org_obj.get("firma") or org_obj.get("foretagsnamn") or "").strip()

    city = ""
    adr = org_obj.get("postadress") or org_obj.get("adress") or org_obj.get("besoksadress") or {}
    if isinstance(adr, dict):
        city = (adr.get("postort") or adr.get("ort") or adr.get("stad") or "").strip()

    return name, city


def fetch_one(orgnr: str) -> dict:
    session, token = get_thread_session_and_token()
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
                return {"orgnr": orgnr, "status": "not_found"}

            org0 = orgs[0]
            codes, texts = extract_sni(org0)
            name, city = extract_name_city(org0)

            if not codes:
                codes = NO_SNI_MARK
                texts = ""
                status = "ok_no_sni"
            else:
                status = "ok"

            return {
                "orgnr": orgnr,
                "name": name,
                "city": city,
                "employees": None,
                "sni_codes": codes,
                "sni_text": texts,
                "website": "",
                "emails": "",
                "website_status": "",
                "email_status": "",
                "website_checked_at": "",
                "emails_checked_at": "",
                "last_seen_at": "",
                "status": status,
            }

        if r.status_code == 401 and attempt < MAX_RETRIES:
            token = get_access_token(session)
            set_thread_token(token)
            time.sleep(BASE_BACKOFF * attempt)
            continue

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(BASE_BACKOFF * attempt)
            continue

        return {"orgnr": orgnr, "status": f"error_{r.status_code}"}

    return {"orgnr": orgnr, "status": "error_retry_exhausted"}


def main():
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="txt med orgnr, en per rad")
    ap.add_argument("--out", dest="out_path", required=True, help="ndjson output")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--concurrency", type=int, default=6)
    ap.add_argument("--print-every", type=int, default=200)
    args = ap.parse_args()

    with open(args.in_path, "r", encoding="utf-8") as f:
        orgnrs = [line.strip() for line in f if line.strip()]

    os.makedirs(os.path.dirname(args.out_path) or ".", exist_ok=True)

    done_set = set()
    if args.resume and os.path.exists(args.out_path):
        with open(args.out_path, "r", encoding="utf-8") as rf:
            for line in rf:
                try:
                    row = json.loads(line)
                    orgnr = (row.get("orgnr") or "").strip()
                    status = (row.get("status") or "").strip()
                    if orgnr and status in ("ok", "ok_no_sni", "not_found"):
                        done_set.add(orgnr)
                except Exception:
                    pass

    orgnrs = [o for o in orgnrs if o not in done_set]
    total = len(orgnrs)

    scanned = ok = ok_no_sni = nf = err = 0
    start = time.time()

    print("=== START ===")
    print("IN:", os.path.abspath(args.in_path))
    print("OUT:", os.path.abspath(args.out_path))
    print(f"to_process={total} concurrency={args.concurrency} resume={args.resume}")

    in_flight = set()

    with open(args.out_path, "a", encoding="utf-8") as out_f, ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        idx = 0

        while idx < total or in_flight:
            # fyll upp poolen i lugn takt
            while idx < total and len(in_flight) < args.concurrency:
                fut = ex.submit(fetch_one, orgnrs[idx])
                in_flight.add(fut)
                idx += 1
                time.sleep(SLEEP_BETWEEN_SUBMITS)

            # vänta tills minst en är klar
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)

            for fut in done:
                in_flight.remove(fut)

                row = fut.result()
                status = row.get("status", "")

                scanned += 1
                if status == "ok":
                    ok += 1
                elif status == "ok_no_sni":
                    ok_no_sni += 1
                elif status == "not_found":
                    nf += 1
                else:
                    err += 1

                out_f.write(json.dumps(row, ensure_ascii=False) + "\n")

                if scanned % args.print_every == 0:
                    rate = scanned / max(1e-9, time.time() - start)
                    print(f"[{scanned}/{total}] ok={ok} ok_no_sni={ok_no_sni} nf={nf} err={err} | {rate:.1f}/s")

    rate = scanned / max(1e-9, time.time() - start)
    print("=== DONE ===")
    print(f"scanned={scanned} ok={ok} ok_no_sni={ok_no_sni} nf={nf} err={err} | {rate:.1f}/s")
    print(f"NO_SNI_MARK used: {NO_SNI_MARK}")


if __name__ == "__main__":
    main()
