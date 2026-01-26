from __future__ import annotations

import json
import os
import re
import time
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

# =========================
# Config
# =========================
DB_PATH_DEFAULT = "data/db/companies.db.sqlite"
TABLE = "companies"

SLEEP_SECONDS = 1.05
PRINT_EVERY = 250
COMMIT_EVERY = 250
SAMPLE_EVERY_OK = 250
REFRESH_DAYS = 90

HTTP_TIMEOUT = 30
MAX_RETRIES = 4
BASE_BACKOFF = 0.6

# Kodtabellfil (från ditt discover-script)
CODETABLE_PATH = Path("data/out/scb_discover_public_private/JE_KategorierMedKodtabeller.json")

# =========================
# Columns
# =========================
COL_ORGNR = "orgnr"

# Drift/status
COL_SCB_STATUS = "scb_status"
COL_SCB_ERR_REASON = "scb_err_reason"
COL_SCB_CHECKED_AT = "scb_checked_at"
COL_SCB_NEXT_CHECK_AT = "scb_next_check_at"

# Company facts
COL_NAME = "scb_company_name"

COL_EMP_CLASS = "scb_employees_class"
COL_EMP_CLASS_CODE = "scb_employees_class_code"
COL_EMP_MIN = "scb_employees_min"
COL_EMP_MAX = "scb_employees_max"

COL_WORKPLACES = "scb_workplaces_count"

COL_POST_ADR = "scb_postadress"
COL_POSTNR = "scb_postnr"
COL_POSTORT = "scb_postort"

COL_MUNICIPALITY = "scb_municipality"
COL_MUNICIPALITY_CODE = "scb_municipality_code"

COL_REGION = "scb_region"
COL_REGION_CODE = "scb_region_code"

COL_LEGAL_FORM = "scb_legal_form"
COL_LEGAL_FORM_CODE = "scb_legal_form_code"

COL_COMPANY_STATUS = "scb_company_status"
COL_COMPANY_STATUS_CODE = "scb_company_status_code"

COL_REG_SKV = "scb_registered_skv"
COL_REG_SKV_CODE = "scb_registered_skv_code"

COL_SNI5 = "scb_sni_5"
COL_SNI5P = "scb_sni_5p"

# Public/private + sector
COL_PRIVATE_PUBLIC = "scb_private_public"
COL_PRIVATE_PUBLIC_CODE = "scb_private_public_code"
COL_SECTOR = "scb_sector"
COL_SECTOR_CODE = "scb_sector_code"

# =========================
# Helpers
# =========================
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


def digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def _snip(t: str, n: int = 250) -> str:
    return (t or "").replace("\r", " ").replace("\n", " ").strip()[:n]


def to_int_maybe(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        s2 = "".join(ch for ch in s if ch.isdigit())
        if s2.isdigit():
            return int(s2)
    return None


def to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return str(v).strip()


# =========================================
# 1) FIX i enrich: emp_code_to_span()
# =========================================
# Byt ut din emp_code_to_span() mot denna.

import re
from typing import Optional, Tuple

def emp_code_to_span(code: str) -> Tuple[Optional[int], Optional[int]]:
    # Kommentar (svenska): Rätt mapping för SCB Stkl-koder:
    # 0=0, 1=1-4, 2=5-9, 3=10-19, 4=20-49, 5=50-99, 6=100-199, 7=200-499, 8=500+
    c = (code or "").strip()
    m = re.search(r"\d+", c)
    if not m:
        return None, None
    n = int(m.group(0))
    mapping = {
        0: (0, 0),
        1: (1, 4),
        2: (5, 9),
        3: (10, 19),
        4: (20, 49),
        5: (50, 99),
        6: (100, 199),
        7: (200, 499),
        8: (500, None),
    }
    return mapping.get(n, (None, None))



def public_bucket(pp_text: str, sector_text: str) -> str:
    s = (pp_text or "").strip().lower()
    t = (sector_text or "").strip().lower()
    blob = f"{s} {t}".strip()
    if not blob:
        return "unknown"
    if "stat" in blob or "kommun" in blob or "region" in blob or "offentlig" in blob:
        return "offentlig"
    if "privat" in blob:
        return "privat"
    return "unknown"


# =========================
# Codetable loader
# =========================
def load_category_maps() -> Dict[str, Dict[str, str]]:
    if not CODETABLE_PATH.exists():
        return {}

    data = json.loads(CODETABLE_PATH.read_text(encoding="utf-8"))
    cats = data if isinstance(data, list) else data.get("Kategorier", [])

    out: Dict[str, Dict[str, str]] = {}
    for c in cats:
        if not isinstance(c, dict):
            continue
        name = (c.get("Kategori") or c.get("Namn") or c.get("Id_Kategori_JE") or "").strip()
        if name not in ("Privat/publikt", "Sektor"):
            continue

        m: Dict[str, str] = {}
        # Koder brukar ligga i VardeLista
        for row in c.get("VardeLista", []) or []:
            if not isinstance(row, dict):
                continue
            kod = to_str(row.get("Kod"))
            txt = to_str(row.get("Text"))
            if kod:
                m[kod] = txt
        out[name] = m

    return out


# =========================
# DB migrate (auto-add)
# =========================
WANTED_COLS: Dict[str, str] = {
    COL_SCB_STATUS: "TEXT",
    COL_SCB_ERR_REASON: "TEXT",
    COL_SCB_CHECKED_AT: "TEXT",
    COL_SCB_NEXT_CHECK_AT: "TEXT",

    COL_NAME: "TEXT",

    COL_EMP_CLASS: "TEXT",
    COL_EMP_CLASS_CODE: "TEXT",
    COL_EMP_MIN: "INTEGER",
    COL_EMP_MAX: "INTEGER",

    COL_WORKPLACES: "INTEGER",

    COL_POST_ADR: "TEXT",
    COL_POSTNR: "TEXT",
    COL_POSTORT: "TEXT",

    COL_MUNICIPALITY: "TEXT",
    COL_MUNICIPALITY_CODE: "TEXT",
    COL_REGION: "TEXT",
    COL_REGION_CODE: "TEXT",

    COL_LEGAL_FORM: "TEXT",
    COL_LEGAL_FORM_CODE: "TEXT",

    COL_COMPANY_STATUS: "TEXT",
    COL_COMPANY_STATUS_CODE: "TEXT",

    COL_REG_SKV: "TEXT",
    COL_REG_SKV_CODE: "TEXT",

    COL_SNI5: "TEXT",
    COL_SNI5P: "TEXT",

    COL_PRIVATE_PUBLIC: "TEXT",
    COL_PRIVATE_PUBLIC_CODE: "TEXT",
    COL_SECTOR: "TEXT",
    COL_SECTOR_CODE: "TEXT",
}


def ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cols = {row[1] for row in cur.execute(f"PRAGMA table_info({TABLE})").fetchall()}
    for col, typ in WANTED_COLS.items():
        if col not in cols:
            cur.execute(f"ALTER TABLE {TABLE} ADD COLUMN {col} {typ}")
    con.commit()


# =========================
# SCB session
# =========================
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


def fetch_one(session: requests.Session, orgnr: str) -> Tuple[Dict[str, Any], str, str]:
    base = must_env("SCB_BASE_URL").rstrip("/")
    endpoint = os.getenv("SCB_JE_ENDPOINT", "/api/Je/HamtaForetag").strip()
    url = f"{base}/{endpoint.lstrip('/')}"

    org10 = digits_only(orgnr)
    if len(org10) != 10:
        return {}, "err", f"bad_orgnr:{org10}"

    def call(payload: dict) -> Tuple[Optional[list], int, str]:
        r = session.post(url, json=payload, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
        if r.status_code != 200:
            return None, r.status_code, (r.text or "")
        try:
            data = r.json()
        except Exception:
            return None, 200, (r.text or "")
        if isinstance(data, list):
            return data, 200, ""
        return None, 200, f"unexpected_shape:{type(data).__name__}"

    # 3-stegs fallback:
    # 1) aktiv + registrerad
    # 2) avreg/nedlagd (företagsstatus=0)
    # 3) utan statusfilter (bara orgnr) -> finns men oklar
    attempts = [
        ("aktiv", {
            "Företagsstatus": "1",
            "Registreringsstatus": "1",
            "variabler": [{"Varde1": org10, "Varde2": "", "Operator": "ArLikaMed", "Variabel": "OrgNr (10 siffror)"}],
        }),
        ("avreg", {
            "Företagsstatus": "0",
            "variabler": [{"Varde1": org10, "Varde2": "", "Operator": "ArLikaMed", "Variabel": "OrgNr (10 siffror)"}],
        }),
        ("oklar", {
            "variabler": [{"Varde1": org10, "Varde2": "", "Operator": "ArLikaMed", "Variabel": "OrgNr (10 siffror)"}],
        }),
    ]

    for label, payload in attempts:
        # retries för transient fel
        for attempt in range(MAX_RETRIES):
            try:
                data, code, msg = call(payload)
            except requests.Timeout:
                time.sleep(BASE_BACKOFF * (2 ** attempt))
                if attempt == MAX_RETRIES - 1:
                    return {}, "err", "timeout"
                continue
            except requests.RequestException as e:
                return {}, "err", f"request_exception:{type(e).__name__}"

            if code != 200:
                # retry på 429/5xx, annars fail direkt
                if code == 429 or (500 <= code <= 599):
                    time.sleep(BASE_BACKOFF * (2 ** attempt))
                    if attempt == MAX_RETRIES - 1:
                        return {}, "err", f"{code}:{_snip(msg)}"
                    continue
                return {}, "err", f"{code}:{_snip(msg)}"

            # code == 200
            if data is None:
                return {}, "err", _snip(msg)

            if len(data) == 0:
                break  # prova nästa label

            obj = data[0] if isinstance(data[0], dict) else {}
            # Sätt tydlig klass i objektet så mappningen kan plocka upp det
            if label == "aktiv":
                obj.setdefault("Företagsstatus, kod", "1")
                obj.setdefault("Företagsstatus", "Är verksam")
            elif label == "avreg":
                obj.setdefault("Företagsstatus, kod", "0")
                obj.setdefault("Företagsstatus", "Ej verksam")
            else:
                # oklar: lämna som SCB gav, men markera om inget finns
                obj.setdefault("Företagsstatus", obj.get("Företagsstatus") or "Oklar")

            return obj, "ok", ""

    # Ingen träff i någon variant
    return {}, "not_found", "saknas_i_scb_dataset"



def map_je_to_fields(obj: Dict[str, Any], cat_maps: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    emp_code = to_str(obj.get("Stkl, kod"))
    mn, mx = emp_code_to_span(emp_code)

    # Privat/publikt & Sektor kan komma som text + kod i JE
    pp_code = to_str(obj.get("Privat/publikt, kod"))
    pp_text = to_str(obj.get("Privat/publikt"))
    if pp_code and not pp_text:
        pp_text = (cat_maps.get("Privat/publikt") or {}).get(pp_code, "")

    sector_code = to_str(obj.get("Sektor, kod"))
    sector_text = to_str(obj.get("Sektor"))
    if sector_code and not sector_text:
        sector_text = (cat_maps.get("Sektor") or {}).get(sector_code, "")

    return {
        COL_NAME: to_str(obj.get("Företagsnamn")) or None,

        COL_EMP_CLASS: to_str(obj.get("Storleksklass")) or None,
        COL_EMP_CLASS_CODE: emp_code or None,
        COL_EMP_MIN: mn,
        COL_EMP_MAX: mx,

        COL_WORKPLACES: to_int_maybe(obj.get("Antal arbetsställen")),

        COL_POST_ADR: to_str(obj.get("PostAdress")) or None,
        COL_POSTNR: to_str(obj.get("PostNr")) or None,
        COL_POSTORT: to_str(obj.get("PostOrt")) or None,

        COL_MUNICIPALITY: to_str(obj.get("Säteskommun")) or None,
        COL_MUNICIPALITY_CODE: to_str(obj.get("Säteskommun, kod")) or None,

        COL_REGION: to_str(obj.get("Säteslän")) or None,
        COL_REGION_CODE: to_str(obj.get("Säteslän, kod")) or None,

        COL_LEGAL_FORM: to_str(obj.get("Juridisk form")) or None,
        COL_LEGAL_FORM_CODE: to_str(obj.get("Juridisk form, kod")) or None,

        COL_COMPANY_STATUS: to_str(obj.get("Företagsstatus")) or None,
        COL_COMPANY_STATUS_CODE: to_str(obj.get("Företagsstatus, kod")) or None,

        COL_REG_SKV: to_str(obj.get("Registrerad hos SKV")) or None,
        COL_REG_SKV_CODE: to_str(obj.get("Registrerad hos SKV, kod")) or None,

        COL_SNI5: to_str(obj.get("Bransch_1, kod")) or None,
        COL_SNI5P: to_str(obj.get("Bransch_1P, kod")) or None,

        COL_PRIVATE_PUBLIC_CODE: pp_code or None,
        COL_PRIVATE_PUBLIC: pp_text or None,

        COL_SECTOR_CODE: sector_code or None,
        COL_SECTOR: sector_text or None,
    }


def safe_count(cur: sqlite3.Cursor, sql: str, params: tuple = ()) -> int:
    row = cur.execute(sql, params).fetchone()
    return int(row[0] if row and row[0] is not None else 0)


def main() -> None:
    load_dotenv()

    con = sqlite3.connect(db_path())
    con.execute("PRAGMA journal_mode=WAL;")
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    ensure_columns(con)

    # laddar kodtabeller om filen finns
    cat_maps = load_category_maps()

    session = make_scb_session()

    now = iso_now()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)).replace(microsecond=0).isoformat()

    total_companies = safe_count(cur, f"SELECT COUNT(*) FROM {TABLE} WHERE {COL_ORGNR} IS NOT NULL AND {COL_ORGNR} != ''")
    checked_last = safe_count(cur, f"SELECT COUNT(*) FROM {TABLE} WHERE {COL_SCB_CHECKED_AT} IS NOT NULL AND {COL_SCB_CHECKED_AT} >= ?", (cutoff,))
    due_now = safe_count(
        cur,
        f"""
        SELECT COUNT(*)
        FROM {TABLE}
        WHERE {COL_ORGNR} IS NOT NULL AND {COL_ORGNR} != ''
          AND ({COL_SCB_NEXT_CHECK_AT} IS NULL OR {COL_SCB_NEXT_CHECK_AT} <= ?)
        """,
        (now,),
    )

    print("ENRICH START ✅")
    print(f"total_companies={total_companies}")
    print(f"checked_last_{REFRESH_DAYS}d={checked_last}")
    print(f"due_now={due_now}")
    print(f"refresh_days={REFRESH_DAYS} sleep={SLEEP_SECONDS}s batch_limit=200")
    print("-" * 60)

    scanned = ok = not_found = err = 0
    err_403 = err_timeout = err_429 = err_5xx = err_other = 0

    active = avreg = missing = 0
    pp_dist = {"privat": 0, "offentlig": 0, "unknown": 0}

    # field fill stats (för allt utom drift)
    field_keys = [k for k in WANTED_COLS.keys() if k not in {COL_SCB_STATUS, COL_SCB_ERR_REASON, COL_SCB_CHECKED_AT, COL_SCB_NEXT_CHECK_AT}]
    field_filled = {k: 0 for k in field_keys}
    field_null = {k: 0 for k in field_keys}

    first_printed = False
    sample_ok_seen = 0
    start = time.time()

    try:
        while True:
            now = iso_now()
            cur.execute(
                f"""
                SELECT {COL_ORGNR}
                FROM {TABLE}
                WHERE {COL_ORGNR} IS NOT NULL AND {COL_ORGNR} != ''
                  AND ({COL_SCB_NEXT_CHECK_AT} IS NULL OR {COL_SCB_NEXT_CHECK_AT} <= ?)
                ORDER BY COALESCE({COL_SCB_NEXT_CHECK_AT}, '1970-01-01') ASC
                LIMIT 200
                """,
                (now,),
            )
            batch = [row[COL_ORGNR] for row in cur.fetchall()]
            if not batch:
                break

            for orgnr in batch:
                orgnr = str(orgnr).strip()

                obj, status, err_reason = fetch_one(session, orgnr)

                checked_at = iso_now()
                next_check = iso_plus_days(REFRESH_DAYS)

                mapped: Dict[str, Any] = {}
                if status == "ok":
                    mapped = map_je_to_fields(obj, cat_maps)

                # Aktiv/avreg
                cs_code = to_str(mapped.get(COL_COMPANY_STATUS_CODE))
                if status == "ok":
                    if cs_code == "1":
                        active += 1
                    elif cs_code == "0":
                        avreg += 1
                if status == "not_found":
                    missing += 1

                # privat/offentlig bucket
                bucket = public_bucket(to_str(mapped.get(COL_PRIVATE_PUBLIC)), to_str(mapped.get(COL_SECTOR)))
                pp_dist[bucket] += 1

                # Field fill stats
                if status == "ok":
                    for k in field_keys:
                        v = mapped.get(k)
                        if v is None or (isinstance(v, str) and v.strip() == ""):
                            field_null[k] += 1
                        else:
                            field_filled[k] += 1

                # Update DB (fill-if-null)
                cur.execute(
                    f"""
                    UPDATE {TABLE}
                    SET
                      {COL_NAME} = CASE WHEN {COL_NAME} IS NULL OR {COL_NAME}='' THEN ? ELSE {COL_NAME} END,

                      {COL_EMP_CLASS} = CASE WHEN {COL_EMP_CLASS} IS NULL OR {COL_EMP_CLASS}='' THEN ? ELSE {COL_EMP_CLASS} END,
                      {COL_EMP_CLASS_CODE} = CASE WHEN {COL_EMP_CLASS_CODE} IS NULL OR {COL_EMP_CLASS_CODE}='' THEN ? ELSE {COL_EMP_CLASS_CODE} END,
                      {COL_EMP_MIN} = CASE WHEN {COL_EMP_MIN} IS NULL THEN ? ELSE {COL_EMP_MIN} END,
                      {COL_EMP_MAX} = CASE WHEN {COL_EMP_MAX} IS NULL THEN ? ELSE {COL_EMP_MAX} END,

                      {COL_WORKPLACES} = CASE WHEN {COL_WORKPLACES} IS NULL THEN ? ELSE {COL_WORKPLACES} END,

                      {COL_POST_ADR} = CASE WHEN {COL_POST_ADR} IS NULL OR {COL_POST_ADR}='' THEN ? ELSE {COL_POST_ADR} END,
                      {COL_POSTNR} = CASE WHEN {COL_POSTNR} IS NULL OR {COL_POSTNR}='' THEN ? ELSE {COL_POSTNR} END,
                      {COL_POSTORT} = CASE WHEN {COL_POSTORT} IS NULL OR {COL_POSTORT}='' THEN ? ELSE {COL_POSTORT} END,

                      {COL_MUNICIPALITY} = CASE WHEN {COL_MUNICIPALITY} IS NULL OR {COL_MUNICIPALITY}='' THEN ? ELSE {COL_MUNICIPALITY} END,
                      {COL_MUNICIPALITY_CODE} = CASE WHEN {COL_MUNICIPALITY_CODE} IS NULL OR {COL_MUNICIPALITY_CODE}='' THEN ? ELSE {COL_MUNICIPALITY_CODE} END,

                      {COL_REGION} = CASE WHEN {COL_REGION} IS NULL OR {COL_REGION}='' THEN ? ELSE {COL_REGION} END,
                      {COL_REGION_CODE} = CASE WHEN {COL_REGION_CODE} IS NULL OR {COL_REGION_CODE}='' THEN ? ELSE {COL_REGION_CODE} END,

                      {COL_LEGAL_FORM} = CASE WHEN {COL_LEGAL_FORM} IS NULL OR {COL_LEGAL_FORM}='' THEN ? ELSE {COL_LEGAL_FORM} END,
                      {COL_LEGAL_FORM_CODE} = CASE WHEN {COL_LEGAL_FORM_CODE} IS NULL OR {COL_LEGAL_FORM_CODE}='' THEN ? ELSE {COL_LEGAL_FORM_CODE} END,

                      {COL_COMPANY_STATUS} = CASE WHEN {COL_COMPANY_STATUS} IS NULL OR {COL_COMPANY_STATUS}='' THEN ? ELSE {COL_COMPANY_STATUS} END,
                      {COL_COMPANY_STATUS_CODE} = CASE WHEN {COL_COMPANY_STATUS_CODE} IS NULL OR {COL_COMPANY_STATUS_CODE}='' THEN ? ELSE {COL_COMPANY_STATUS_CODE} END,

                      {COL_REG_SKV} = CASE WHEN {COL_REG_SKV} IS NULL OR {COL_REG_SKV}='' THEN ? ELSE {COL_REG_SKV} END,
                      {COL_REG_SKV_CODE} = CASE WHEN {COL_REG_SKV_CODE} IS NULL OR {COL_REG_SKV_CODE}='' THEN ? ELSE {COL_REG_SKV_CODE} END,

                      {COL_SNI5} = CASE WHEN {COL_SNI5} IS NULL OR {COL_SNI5}='' THEN ? ELSE {COL_SNI5} END,
                      {COL_SNI5P} = CASE WHEN {COL_SNI5P} IS NULL OR {COL_SNI5P}='' THEN ? ELSE {COL_SNI5P} END,

                      {COL_PRIVATE_PUBLIC_CODE} = CASE WHEN {COL_PRIVATE_PUBLIC_CODE} IS NULL OR {COL_PRIVATE_PUBLIC_CODE}='' THEN ? ELSE {COL_PRIVATE_PUBLIC_CODE} END,
                      {COL_PRIVATE_PUBLIC} = CASE WHEN {COL_PRIVATE_PUBLIC} IS NULL OR {COL_PRIVATE_PUBLIC}='' THEN ? ELSE {COL_PRIVATE_PUBLIC} END,

                      {COL_SECTOR_CODE} = CASE WHEN {COL_SECTOR_CODE} IS NULL OR {COL_SECTOR_CODE}='' THEN ? ELSE {COL_SECTOR_CODE} END,
                      {COL_SECTOR} = CASE WHEN {COL_SECTOR} IS NULL OR {COL_SECTOR}='' THEN ? ELSE {COL_SECTOR} END,

                      {COL_SCB_STATUS}=?,
                      {COL_SCB_ERR_REASON}=?,
                      {COL_SCB_CHECKED_AT}=?,
                      {COL_SCB_NEXT_CHECK_AT}=?
                    WHERE {COL_ORGNR}=?
                    """,
                    (
                        mapped.get(COL_NAME),

                        mapped.get(COL_EMP_CLASS),
                        mapped.get(COL_EMP_CLASS_CODE),
                        mapped.get(COL_EMP_MIN),
                        mapped.get(COL_EMP_MAX),

                        mapped.get(COL_WORKPLACES),

                        mapped.get(COL_POST_ADR),
                        mapped.get(COL_POSTNR),
                        mapped.get(COL_POSTORT),

                        mapped.get(COL_MUNICIPALITY),
                        mapped.get(COL_MUNICIPALITY_CODE),

                        mapped.get(COL_REGION),
                        mapped.get(COL_REGION_CODE),

                        mapped.get(COL_LEGAL_FORM),
                        mapped.get(COL_LEGAL_FORM_CODE),

                        mapped.get(COL_COMPANY_STATUS),
                        mapped.get(COL_COMPANY_STATUS_CODE),

                        mapped.get(COL_REG_SKV),
                        mapped.get(COL_REG_SKV_CODE),

                        mapped.get(COL_SNI5),
                        mapped.get(COL_SNI5P),

                        mapped.get(COL_PRIVATE_PUBLIC_CODE),
                        mapped.get(COL_PRIVATE_PUBLIC),

                        mapped.get(COL_SECTOR_CODE),
                        mapped.get(COL_SECTOR),

                        status,
                        err_reason,
                        checked_at,
                        next_check,
                        orgnr,
                    ),
                )

                scanned += 1
                if status == "ok":
                    ok += 1
                elif status == "not_found":
                    not_found += 1
                else:
                    err += 1
                    if err_reason.startswith("403"):
                        err_403 += 1
                    elif err_reason.startswith("timeout"):
                        err_timeout += 1
                    elif err_reason.startswith("429"):
                        err_429 += 1
                    elif err_reason.startswith("5xx"):
                        err_5xx += 1
                    else:
                        err_other += 1

                if not first_printed:
                    first_printed = True
                    print("FIRST ENRICH ✅")
                    print(f"orgnr={orgnr} status={status} err_reason={err_reason}")
                    if status == "ok":
                        print(
                            "name={0} status={1} legal={2} sni={3} emp={4}({5}-{6}) kommun={7} län={8} pp={9} sector={10}".format(
                                to_str(mapped.get(COL_NAME)),
                                to_str(mapped.get(COL_COMPANY_STATUS)),
                                to_str(mapped.get(COL_LEGAL_FORM)),
                                to_str(mapped.get(COL_SNI5)),
                                to_str(mapped.get(COL_EMP_CLASS)),
                                mapped.get(COL_EMP_MIN),
                                mapped.get(COL_EMP_MAX),
                                to_str(mapped.get(COL_MUNICIPALITY)),
                                to_str(mapped.get(COL_REGION)),
                                to_str(mapped.get(COL_PRIVATE_PUBLIC)) or to_str(mapped.get(COL_PRIVATE_PUBLIC_CODE)),
                                to_str(mapped.get(COL_SECTOR)) or to_str(mapped.get(COL_SECTOR_CODE)),
                            )
                        )
                    print("-" * 40)

                if status == "ok":
                    sample_ok_seen += 1
                    if sample_ok_seen % SAMPLE_EVERY_OK == 0:
                        print(
                            "SAMPLE ✅ org={0} name={1} status={2} legal={3} sni={4} emp={5}({6}-{7}) arb={8} kommun={9} län={10} pp={11} sector={12}".format(
                                orgnr,
                                to_str(mapped.get(COL_NAME)),
                                to_str(mapped.get(COL_COMPANY_STATUS)),
                                to_str(mapped.get(COL_LEGAL_FORM)),
                                to_str(mapped.get(COL_SNI5)),
                                to_str(mapped.get(COL_EMP_CLASS)),
                                mapped.get(COL_EMP_MIN),
                                mapped.get(COL_EMP_MAX),
                                to_str(mapped.get(COL_WORKPLACES)),
                                to_str(mapped.get(COL_MUNICIPALITY)),
                                to_str(mapped.get(COL_REGION)),
                                to_str(mapped.get(COL_PRIVATE_PUBLIC)) or to_str(mapped.get(COL_PRIVATE_PUBLIC_CODE)),
                                to_str(mapped.get(COL_SECTOR)) or to_str(mapped.get(COL_SECTOR_CODE)),
                            )
                        )

                if scanned % COMMIT_EVERY == 0:
                    con.commit()

                if scanned % PRINT_EVERY == 0:
                    rate = scanned / max(1e-9, time.time() - start)
                    print(f"[{scanned}] ok={ok} saknas={not_found} err={err} | {rate:.2f}/s")
                    print(f"errors: 403={err_403} timeout={err_timeout} 429={err_429} 5xx={err_5xx} other={err_other}")
                    print(f"activity: aktiv={active} avreg={avreg} saknas={missing}")
                    print(
                        f"public_private: privat={pp_dist['privat']} offentlig={pp_dist['offentlig']} unknown={pp_dist['unknown']}"
                    )
                    print("field_fill (this_run):")
                    for k in field_keys:
                        print(f"  {k}: filled={field_filled[k]} null={field_null[k]}")
                    print("-" * 40)

                time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\n⛔ Avbruten – committar data...")

    finally:
        con.commit()
        con.close()

    rate = scanned / max(1e-9, time.time() - start)
    print("DONE ✅ Enrich")
    print(f"scanned={scanned} ok={ok} saknas={not_found} err={err} | {rate:.2f}/s")
    print(f"errors: 403={err_403} timeout={err_timeout} 429={err_429} 5xx={err_5xx} other={err_other}")


if __name__ == "__main__":
    main()
