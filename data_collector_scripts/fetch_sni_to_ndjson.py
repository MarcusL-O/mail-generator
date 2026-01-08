import os
import time
import uuid
import json
import argparse
import threading
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========= CONFIG =========
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))
SLEEP_BETWEEN = float(os.getenv("SLEEP_BETWEEN", "0.08"))  # snäll mot API
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "0.6"))

# Markera "kollad men ingen SNI", så du inte kör samma orgnr om och om igen
NO_SNI_MARK = os.getenv("NO_SNI_MARK", "__NO_SNI__")

# Thread-local för session + token (viktigt: requests.Session är inte thread-safe)
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
    """
    Skapar en egen requests.Session + token per tråd.
    """
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
        _tls.token = get_access_token(_tls.session)
    return _tls.session, _tls.token


def set_thread_token(token: str) -> None:
    _tls.token = token


def extract_sni(org_obj: dict) -> tuple[str, str]:
    """
    Stöd för flera möjliga paths i API-svaret.
    """
    candidates = []

    # 1) vanligaste (det vi såg i ditt riktiga svar)
    candidates.append((((org_obj.get("naringsgrenOrganisation") or {}).get("sni")) or []))

    # 2) alternativa nycklar
    candidates.append(((org_obj.get("naringsgrenar") or {}).get("sni")) or [])
    candidates.append((org_obj.get("sni") or []))

    # 3) fallback
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


def fetch_one(orgnr: str) -> tuple[str, str, str, str]:
    """
    Returns: (orgnr, codes, texts, status)
    status:
      - ok (har svar; kan ha SNI eller inte)
      - not_found
      - error_XXX
      - error_retry_exhausted
    """
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
            r = session.post(
                url,
                headers=headers,
                json={"identitetsbeteckning": orgnr},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException:
            time.sleep(BASE_BACKOFF * attempt)
            continue

        if r.status_code == 200:
            orgs = (r.json().get("organisationer") or [])
            if not orgs:
                return orgnr, "", "", "not_found"

            codes, texts = extract_sni(orgs[0])
            # Markera "kollad men ingen SNI" med sentinel så du slipper loopa samma
            if not codes:
                return orgnr, NO_SNI_MARK, "", "ok_no_sni"

            return orgnr, codes, texts, "ok"

        if r.status_code == 401 and attempt < MAX_RETRIES:
            # token expired -> refresh per tråd
            token = get_access_token(session)
            set_thread_token(token)
            time.sleep(BASE_BACKOFF * attempt)
            continue

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(BASE_BACKOFF * attempt)
            continue

        return orgnr, "", "", f"error_{r.status_code}"

    return orgnr, "", "", "error_retry_exhausted"


def main():
    load_dotenv()

    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--reverse", action="store_true", help="Kör från längst ner i listan (reversera input)")
    ap.add_argument("--print-every", type=int, default=200, help="Skriv en liten statusrad var N:e resultat")
    args = ap.parse_args()

    # Läs orgnr
    with open(args.in_path, "r", encoding="utf-8") as f:
        orgnrs = [line.strip() for line in f if line.strip()]

    # Kör från "längst ner"
    if args.reverse:
        orgnrs = list(reversed(orgnrs))

    os.makedirs(os.path.dirname(args.out_path) or ".", exist_ok=True)

    # Resume: hoppa över redan skrivna orgnr i out-filen
    done = set()
    if args.resume and os.path.exists(args.out_path):
        with open(args.out_path, "r", encoding="utf-8") as rf:
            for line in rf:
                try:
                    row = json.loads(line)
                    status = (row.get("status") or "").strip()
                    orgnr = (row.get("orgnr") or "").strip()
                    # Bara dessa räknas som "klara" och ska INTE köras igen
                    if orgnr and status in ("ok", "ok_no_sni", "not_found"):
                        done.add(orgnr)
                except Exception:
                    # trasig rad -> ignorera, kör om senare
                    pass

    orgnrs = [o for o in orgnrs if o not in done]

    total = len(orgnrs)
    scanned = ok = ok_no_sni = nf = err = 0
    start = time.time()

    print("=== START ===")
    print("IN:", os.path.abspath(args.in_path))
    print("OUT:", os.path.abspath(args.out_path))
    print(f"Loaded orgnrs: {total} | concurrency={args.concurrency} | reverse={args.reverse} | resume={args.resume}")
    if total > 0:
        print("Sample (first 5 to process):", orgnrs[:5])

    # Viktigt: skriver append så du kan avbryta och köra igen med --resume
    with open(args.out_path, "a", encoding="utf-8") as out_f:
        with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            futures = [ex.submit(fetch_one, o) for o in orgnrs]

            for fut in as_completed(futures):
                orgnr, codes, texts, status = fut.result()
                scanned += 1

                if status == "ok":
                    ok += 1
                elif status == "ok_no_sni":
                    ok_no_sni += 1
                elif status == "not_found":
                    nf += 1
                else:
                    err += 1

                # Kontroll: skriv rad i output
                out_f.write(
                    json.dumps(
                        {
                            "orgnr": orgnr,
                            "sni_codes": codes,
                            "sni_text": texts,
                            "status": status,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

                # Kontroll: visa att saker händer
                if scanned % args.print_every == 0:
                    rate = scanned / max(1e-9, time.time() - start)
                    print(
                        f"[{scanned}/{total}] ok={ok} ok_no_sni={ok_no_sni} nf={nf} err={err} | {rate:.1f}/s"
                    )

                # var snäll mot API (lite)
                time.sleep(SLEEP_BETWEEN)

    rate = scanned / max(1e-9, time.time() - start)
    print("=== DONE ===")
    print(f"scanned={scanned} ok={ok} ok_no_sni={ok_no_sni} nf={nf} err={err} | {rate:.1f}/s")
    print(f"NO_SNI_MARK used: {NO_SNI_MARK}")


if __name__ == "__main__":
    main()

#den rrätta för att starta
#python data_collector_scripts/fetch_sni_to_ndjson.py --in data/out/orgnrs_missing_sni.txt --out data/out/sni_results.ndjson --resume --concurrency 8
