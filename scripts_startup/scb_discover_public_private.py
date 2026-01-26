# Kommentar (svenska):
# - Läser samma endpoints som innan
# - SÖKER i ALLA strängar i varje objekt (oavsett nycklar)
# - Printar:
#   1) alla kategorinamn den lyckas hitta (för översikt)
#   2) träffar som innehåller privat/offentlig/sektor/kommun/stat/region
# - Sparar raw JSON till data/out/scb_discover_public_private (som innan)

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv

HTTP_TIMEOUT = 30
OUT_DIR = Path("data/out/scb_discover_public_private")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEYWORDS = ["privat", "publikt", "offentlig", "sektor", "stat", "kommun", "region"]


def must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


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


def contains_kw(s: str) -> bool:
    t = (s or "").lower()
    return any(k in t for k in KEYWORDS)


def walk_strings(obj: Any, path: str = "") -> List[Tuple[str, str]]:
    # Kommentar (svenska): samlar alla strängar med path
    out: List[Tuple[str, str]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else str(k)
            out.extend(walk_strings(v, p))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            p = f"{path}[{i}]"
            out.extend(walk_strings(v, p))
    elif isinstance(obj, str):
        out.append((path, obj))
    return out


def extract_categories(data: Any) -> List[Dict[str, Any]]:
    # Kommentar (svenska): försök hitta listan av kategorier oavsett wrapper
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        # vanliga wrappers
        for k in ["Kategorier", "kategorier", "Items", "items", "Data", "data", "Resultat", "resultat"]:
            if k in data and isinstance(data[k], list):
                return [x for x in data[k] if isinstance(x, dict)]
        # annars: om dicten i sig verkar innehålla listor av dictar
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return [x for x in v if isinstance(x, dict)]
    return []


def try_guess_name(cat: Dict[str, Any]) -> str:
    # Kommentar (svenska): försök hitta “namnet” på kategorin
    for key in ["Kategori", "kategori", "Namn", "namn", "Titel", "titel", "Name", "name", "Beskrivning", "beskrivning"]:
        v = cat.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # fallback: första strängen som ser ut som titel
    strs = walk_strings(cat)
    for _, s in strs:
        if isinstance(s, str) and len(s.strip()) <= 80 and "kod" not in s.lower():
            return s.strip()
    return "<okänt>"


def main() -> None:
    load_dotenv()

    base = must_env("SCB_BASE_URL").rstrip("/")
    s = make_scb_session()

    endpoints = [
        ("JE_KategorierMedKodtabeller", f"{base}/api/Je/KategorierMedKodtabeller"),
        ("JE_KoptaKategorier", f"{base}/api/Je/KoptaKategorier"),
    ]

    for label, url in endpoints:
        print("\n" + "=" * 90)
        print(label, url)
        print("=" * 90)

        r = s.get(url, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
        print("status:", r.status_code)
        if r.status_code != 200:
            print("body:", (r.text or "")[:800])
            continue

        data = r.json()
        (OUT_DIR / f"{label}.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        cats = extract_categories(data)
        print("kategorier_count:", len(cats))

        # 1) lista alla kategorinamnen (för översikt)
        print("\nALLA kategorier (namn-gissning):")
        for i, c in enumerate(cats, start=1):
            print(f"{i:02d}. {try_guess_name(c)}")

        # 2) hitta matchar genom att söka i ALLA strängar i objektet
        matches: List[Tuple[str, str]] = []
        for c in cats:
            name = try_guess_name(c)
            for p, s2 in walk_strings(c):
                if contains_kw(s2):
                    matches.append((name, f"{p}={s2}"))
                    break  # räcker med första träffen per kategori

        print("\nMATCHES:", len(matches))
        for name, hit in matches:
            print(f"\n--- MATCH: {name}\n{hit}")

    print(f"\nDONE ✅ sparade json i: {OUT_DIR}")


if __name__ == "__main__":
    main()
