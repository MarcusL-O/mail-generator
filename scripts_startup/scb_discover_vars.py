# Kommentar (svenska):
# - Skriver ut vilka variabler/kategorier du HAR tillgång till (köpta) + operatorer.
# - Sparar även JSON till data/out/scb_discover/ så du kan läsa i efterhand.

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv


OUT_DIR = Path("data/out/scb_discover")
OUT_DIR.mkdir(parents=True, exist_ok=True)

HTTP_TIMEOUT = 30


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


def make_scb_session() -> requests.Session:
    # Kommentar: använder samma cert som du redan har
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


def fetch_json(session: requests.Session, url: str) -> Any:
    r = session.get(url, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
    ct = (r.headers.get("content-type") or "").lower()
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {url} -> {r.text[:300]}")
    if "json" not in ct:
        # Kommentar: vissa endpoints kan svara text/html vid fel, men här är det 200
        pass
    return r.json()


def dump_vars(name: str, items: List[Dict[str, Any]]) -> None:
    # Kommentar: skriver kort, men tillräckligt för att bygga payloads
    print(f"\n=== {name} ===")
    print(f"count={len(items)}")

    # Vanliga fält i swagger: Id_Variabel_* och Operatorer
    for i, it in enumerate(items[:80], start=1):
        var_id = it.get("Id_Variabel_JE") or it.get("Id_Variabel_AE") or it.get("Id") or it.get("Variabel")
        ops = it.get("Operatorer") or []
        dt = it.get("Datatyp") or ""
        ln = it.get("Langd") or ""
        print(f"{i:03d}. {var_id} | type={dt} len={ln} | ops={ops}")

    if len(items) > 80:
        print(f"... (visar första 80 av {len(items)})")


def dump_simple(name: str, items: Any) -> None:
    print(f"\n=== {name} ===")
    if isinstance(items, list):
        print(f"count={len(items)}")
        for i, it in enumerate(items[:40], start=1):
            print(f"{i:03d}. {it}")
        if len(items) > 40:
            print(f"... (visar första 40 av {len(items)})")
    else:
        print(type(items), str(items)[:300])


def main() -> None:
    load_dotenv()

    base = must_env("SCB_BASE_URL").rstrip("/")
    session = make_scb_session()

    targets = [
        ("JE Variabler", f"{base}/api/Je/Variabler"),
        ("JE KöptaVariabler", f"{base}/api/Je/KoptaVariabler"),
        ("JE KategorierMedKodtabeller", f"{base}/api/Je/KategorierMedKodtabeller"),
        ("JE KöptaKategorier", f"{base}/api/Je/KoptaKategorier"),

        ("AE Variabler", f"{base}/api/Ae/Variabler"),
        ("AE KöptaVariabler", f"{base}/api/Ae/KoptaVariabler"),
        ("AE KategorierMedKodtabeller", f"{base}/api/Ae/KategorierMedKodtabeller"),
        ("AE KöptaKategorier", f"{base}/api/Ae/KoptaKategorier"),
    ]

    results: Dict[str, Any] = {}

    for label, url in targets:
        try:
            data = fetch_json(session, url)
            results[label] = data

            # Spara fil
            safe = label.lower().replace(" ", "_").replace("å", "a").replace("ä", "a").replace("ö", "o")
            (OUT_DIR / f"{safe}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

            # Print
            if "Variabler" in label:
                if isinstance(data, list):
                    dump_vars(label, data)
                else:
                    dump_simple(label, data)
            else:
                dump_simple(label, data)

        except Exception as e:
            print(f"\n=== {label} ===")
            print(f"ERROR: {e}")

    print(f"\nDONE ✅ sparade json i: {OUT_DIR}")


if __name__ == "__main__":
    main()
