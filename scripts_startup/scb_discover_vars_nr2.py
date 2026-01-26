# Kommentar (svenska):
# - Kollar snabbt om du har access till JE och AE.
# - Testar:
#   1) GET /api/Ae/KoptaVariabler (ska INTE vara tom om du har AE)
#   2) POST /api/Ae/HamtaFirmor med orgnr (ska ge 200 + data eller tydlig 403/400)
#   3) POST /api/Je/HamtaForetag med orgnr (baseline)
#
# Kör: python companies/open_data/scb/scb_check_access.py 5560707555

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict

import requests
from dotenv import load_dotenv


HTTP_TIMEOUT = 30


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


def digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def make_scb_session() -> requests.Session:
    try:
        from requests_pkcs12 import Pkcs12Adapter  # type: ignore
    except Exception:
        raise SystemExit("Installera: pip install requests-pkcs12")

    s = requests.Session()
    s.mount(
        "https://",
        Pkcs12Adapter(
            pkcs12_filename=must_env("SCB_CERT_PATH"),
            pkcs12_password=must_env("SCB_CERT_PASSWORD"),
        ),
    )
    return s


def p(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def show_resp(r: requests.Response):
    print("status:", r.status_code)
    ct = (r.headers.get("content-type") or "").lower()
    print("content-type:", ct)
    txt = (r.text or "").strip()
    if txt:
        print("body:", txt[:800])
    else:
        print("body: <empty>")


def main() -> None:
    load_dotenv()

    if len(sys.argv) < 2:
        raise SystemExit("Usage: python companies/open_data/scb/scb_check_access.py <orgnr10>")

    org10 = digits_only(sys.argv[1])
    if len(org10) != 10:
        raise SystemExit(f"Bad orgnr (need 10 digits): {org10}")

    base = must_env("SCB_BASE_URL").rstrip("/")
    je_ep = os.getenv("SCB_JE_ENDPOINT", "/api/Je/HamtaForetag").strip()
    ae_ep = os.getenv("SCB_AE_ENDPOINT", "/api/Ae/HamtaFirmor").strip()

    s = make_scb_session()

    # 1) AE KöptaVariabler
    p("AE: GET /api/Ae/KoptaVariabler")
    url = f"{base}/api/Ae/KoptaVariabler"
    r = s.get(url, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
    show_resp(r)
    if r.status_code == 200:
        try:
            data = r.json()
            if isinstance(data, list):
                print("count:", len(data))
                for it in data[:20]:
                    var_id = it.get("Id_Variabel_AE") or it.get("Id") or it.get("Variabel")
                    ops = it.get("Operatorer") or []
                    print("-", var_id, "| ops=", ops)
            else:
                print("json type:", type(data).__name__)
        except Exception:
            pass

    # 2) JE HamtaForetag (baseline)
    p(f"JE: POST {je_ep} (OrgNr (10 siffror) = {org10})")
    je_url = f"{base}/{je_ep.lstrip('/')}"
    je_payload: Dict[str, Any] = {
        "Företagsstatus": "1",
        "Registreringsstatus": "1",
        "variabler": [
            {"Varde1": org10, "Varde2": "", "Operator": "ArLikaMed", "Variabel": "OrgNr (10 siffror)"}
        ],
    }
    r = s.post(je_url, json=je_payload, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
    show_resp(r)

    # 3) AE HamtaFirmor (facts)
    p(f"AE: POST {ae_ep} (OrgNr (10 siffror) = {org10})")
    ae_url = f"{base}/{ae_ep.lstrip('/')}"
    ae_payload: Dict[str, Any] = {
        # Kommentar: samma modell-stil som JE brukar funka i dessa endpoints
        "variabler": [
            {"Varde1": org10, "Varde2": "", "Operator": "ArLikaMed", "Variabel": "OrgNr (10 siffror)"}
        ],
    }
    r = s.post(ae_url, json=ae_payload, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
    show_resp(r)


if __name__ == "__main__":
    main()
