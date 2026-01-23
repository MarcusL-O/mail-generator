# companies/control/site_line_of_work_shard.py
# Kommentar: shard-script som klassar bolag baserat på website + SNI och skriver NDJSON (ingen DB-write)

import re
import time
import json
import sqlite3
import hashlib
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit, urljoin

import requests
from requests.exceptions import ConnectionError, InvalidURL
from urllib3.exceptions import LocationParseError

ap = argparse.ArgumentParser()
ap.add_argument("--shard-id", type=int, required=True)
ap.add_argument("--shard-total", type=int, default=4)
args = ap.parse_args()

SHARD_ID = args.shard_id
SHARD_TOTAL = args.shard_total

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/db/companies.db.sqlite")
OUT_PATH = Path(f"data/out/shards/line_of_work_shard{SHARD_ID}.ndjson")
LIMIT = 0              # 0 = ALLA
RESUME = True
PRINT_EVERY = 50
REFRESH_DAYS = 120     # Kommentar: rerun klassning efter X dagar
# =========================

TIMEOUT_SECONDS = 12
SLEEP_BETWEEN_REQUESTS = 0.20
MAX_BYTES = 400_000

MAX_PAGES = 3  # Kommentar: 2 i praktiken, 3 om vi behöver

session = requests.Session()
session.headers.update({
    "User-Agent": f"Mozilla/5.0 (Didup-LineOfWork/1.0; shard={SHARD_ID})"
})

HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def needs_refresh(checked_at: Optional[str]) -> bool:
    dt = parse_iso(checked_at)
    if not dt:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=REFRESH_DAYS)
    return dt < cutoff

def in_shard(orgnr: str) -> bool:
    h = hashlib.md5(orgnr.encode("utf-8")).hexdigest()
    return (int(h, 16) % SHARD_TOTAL) == SHARD_ID

def _valid_hostname(host: str) -> bool:
    if not host:
        return False
    host = host.strip().lower().rstrip(".")
    if len(host) > 253:
        return False
    if any(c.isspace() for c in host):
        return False
    if ".." in host:
        return False
    labels = host.split(".")
    if len(labels) < 2:
        return False
    for lab in labels:
        if not lab or len(lab) > 63:
            return False
        if lab.startswith("-") or lab.endswith("-"):
            return False
        if not re.fullmatch(r"[a-z0-9-]+", lab):
            return False
    return True

def _safe_url(url: str) -> bool:
    try:
        u = url.strip()
        parts = urlsplit(u)
        if parts.scheme not in ("http", "https"):
            return False
        return _valid_hostname(parts.hostname or "")
    except Exception:
        return False

def _is_retryable_status(code: int) -> bool:
    return code in (403, 429, 500, 502, 503, 504)

def _is_dns_miss_error(e: Exception) -> bool:
    msg = str(e).lower()
    return (
        "name or service not known" in msg
        or "failed to resolve" in msg
        or "nodename nor servname" in msg
        or "temporary failure in name resolution" in msg
        or "getaddrinfo failed" in msg
    )

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u
    return u

def fetch_html(url: str) -> tuple[Optional[str], str]:
    """
    Returns (html_text_or_none, err_reason)
    err_reason: "" | "403" | "429" | "timeout" | "other" | "not_html"
    """
    if not _safe_url(url):
        return (None, "other")

    try:
        r = session.get(
            url,
            timeout=(3, TIMEOUT_SECONDS),
            allow_redirects=True,
            stream=True,
        )

        if _is_retryable_status(r.status_code):
            r.close()
            return (None, str(r.status_code))

        if not (200 <= r.status_code < 400):
            r.close()
            return (None, "other")

        ct = (r.headers.get("Content-Type") or "").lower()
        if "text/html" not in ct and "application/xhtml" not in ct and not ct.startswith("text/"):
            r.close()
            return (None, "not_html")

        chunks = []
        read = 0
        for chunk in r.iter_content(chunk_size=32_768):
            if not chunk:
                break
            chunks.append(chunk)
            read += len(chunk)
            if read >= MAX_BYTES:
                break
        r.close()

        raw = b"".join(chunks)
        html = raw.decode("utf-8", errors="ignore")
        return (html, "")

    except requests.Timeout:
        return (None, "timeout")
    except (LocationParseError, InvalidURL):
        return (None, "other")
    except (ConnectionError, requests.RequestException) as e:
        if _is_dns_miss_error(e):
            return (None, "other")
        return (None, "other")
    except Exception:
        return (None, "other")

def strip_html_to_text(html: str) -> str:
    # Kommentar: billig text-extraktion (bra nog för keyword-match)
    s = html.lower()
    s = re.sub(r"<script[\s\S]*?</script>", " ", s)
    s = re.sub(r"<style[\s\S]*?</style>", " ", s)
    s = re.sub(r"<!--[\s\S]*?-->", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_internal_candidate_links(base_url: str, html: str) -> list[str]:
    # Kommentar: välj ett fåtal “bra” interna länkar
    parts = urlsplit(base_url)
    base_root = f"{parts.scheme}://{parts.netloc}"
    html_lower = html.lower()

    hrefs = HREF_RE.findall(html_lower)
    candidates: list[str] = []

    wanted = [
        "om", "om-oss", "about", "company",
        "tjanster", "tjänster", "services", "service",
        "produkter", "products", "product",
        "vad-vi-gor", "what-we-do",
        "case", "referens", "referenser", "portfolio",
    ]

    for h in hrefs:
        h = (h or "").strip()
        if not h or h.startswith("#"):
            continue
        if h.startswith("mailto:") or h.startswith("tel:"):
            continue

        u = urljoin(base_root + "/", h)
        up = urlsplit(u)

        # Kommentar: bara samma host
        if up.netloc != parts.netloc:
            continue
        if not _safe_url(u):
            continue

        path = (up.path or "").lower()
        if any(k in path for k in wanted):
            candidates.append(u)

    # Kommentar: dedupe + begränsa
    seen = set()
    out = []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= 12:
            break
    return out

# =========================
# Taxonomi (40–80-ish)
# =========================

# Kommentar: "line_of_work" (normaliserad) + keywords (sv/en)
CATEGORIES: dict[str, dict] = {
    "architecture": {"raw": ["arkitekt", "arkitektkontor"], "kw": ["arkitekt", "arkitektur", "architect", "architecture", "gestaltning", "projektering"]},
    "construction": {"raw": ["bygg", "entreprenad"], "kw": ["bygg", "entreprenad", "construction", "renovering", "byggservice", "byggfirma", "anläggning"]},
    "electrical": {"raw": ["elektriker"], "kw": ["elektriker", "elinstallation", "elservice", "electrician", "electrical"]},
    "plumbing_hvac": {"raw": ["vvs", "ventilation"], "kw": ["vvs", "rör", "rormokare", "ventilation", "värmepump", "heat pump", "hvac", "plumber"]},
    "carpentry": {"raw": ["snickare", "finsnickare"], "kw": ["snickare", "finsnickeri", "carpenter", "joinery", "träarbete", "trappräcke", "kökssnickeri"]},
    "painting": {"raw": ["målare"], "kw": ["målare", "måleri", "painting", "tapetsering"]},
    "cleaning": {"raw": ["städ"], "kw": ["städ", "städning", "cleaning", "hemstäd", "kontorsstäd", "flyttstäd"]},
    "it_services": {"raw": ["it-konsult", "it-support"], "kw": ["it", "it-konsult", "konsult", "managed services", "msp", "it-support", "helpdesk", "drift", "cloud", "azure", "microsoft 365", "cybersäkerhet", "cyber security", "it-tjänster"]},
    "software": {"raw": ["systemutveckling"], "kw": ["systemutveckling", "software", "saas", "app", "utvecklar", "development", "platform", "api"]},
    "marketing": {"raw": ["marknadsföring"], "kw": ["marknadsföring", "marketing", "seo", "annonser", "ads", "google ads", "sociala medier", "content", "branding"]},
    "recruitment": {"raw": ["rekrytering"], "kw": ["rekrytering", "recruitment", "bemanning", "staffing", "headhunting"]},
    "accounting": {"raw": ["redovisning"], "kw": ["redovisning", "bokföring", "accounting", "lön", "payroll", "revisor", "revision"]},
    "legal": {"raw": ["juridik"], "kw": ["advokat", "jurist", "law", "legal", "juridik"]},
    "real_estate": {"raw": ["fastighet"], "kw": ["fastighet", "real estate", "mäklare", "property", "förvaltning", "uthyrning"]},
    "finance_insurance": {"raw": ["försäkring", "finans"], "kw": ["försäkring", "insurance", "kredit", "lån", "finance", "fond", "investment", "kapital"]},
    "transport_logistics": {"raw": ["transport", "logistik"], "kw": ["transport", "logistik", "shipping", "frakt", "åkeri", "lager", "distribution"]},
    "manufacturing": {"raw": ["tillverkning"], "kw": ["tillverkning", "manufacturer", "produktion", "industry", "industri", "fabrik", "cnc"]},
    "automotive": {"raw": ["bilverkstad"], "kw": ["bil", "verkstad", "auto", "automotive", "service", "däck", "tire"]},
    "healthcare": {"raw": ["vård"], "kw": ["vård", "healthcare", "klinik", "läkare", "rehab", "fysioterapi", "physio"]},
    "dental": {"raw": ["tandvård"], "kw": ["tand", "tandvård", "dentist", "dental"]},
    "education": {"raw": ["utbildning"], "kw": ["utbildning", "education", "kurs", "training", "skola"]},
    "restaurant_cafe": {"raw": ["restaurang"], "kw": ["restaurang", "restaurant", "café", "cafe", "takeaway", "mat"]},
    "hotel_travel": {"raw": ["hotell"], "kw": ["hotell", "hotel", "boende", "conference", "konferens", "travel", "resa"]},
    "security": {"raw": ["säkerhet"], "kw": ["säkerhet", "security", "larm", "bevakning", "cctv", "kameraövervakning"]},
    "ecommerce_retail": {"raw": ["butik", "e-handel"], "kw": ["webshop", "e-handel", "shop", "butik", "cart", "checkout", "köp"]},
    "events": {"raw": ["event"], "kw": ["event", "mässa", "konferens", "festival", "arrangemang"]},
    "agriculture": {"raw": ["lantbruk"], "kw": ["lantbruk", "agriculture", "farm", "gård", "skog", "forestry"]},
    "industrial_services": {"raw": ["industriservice"], "kw": ["industriservice", "service", "underhåll", "maintenance", "installation", "montage"]},
}

# Kommentar: grupper för “samma sektor”-check
GROUPS: dict[str, str] = {
    "architecture": "construction_sector",
    "construction": "construction_sector",
    "electrical": "construction_sector",
    "plumbing_hvac": "construction_sector",
    "carpentry": "construction_sector",
    "painting": "construction_sector",

    "it_services": "it_sector",
    "software": "it_sector",

    "accounting": "finance_sector",
    "finance_insurance": "finance_sector",
    "real_estate": "finance_sector",

    "marketing": "business_services",
    "recruitment": "business_services",
    "legal": "business_services",
    "cleaning": "business_services",
    "security": "business_services",
    "industrial_services": "business_services",

    "transport_logistics": "transport_sector",
    "manufacturing": "industry_sector",
    "automotive": "consumer_services",

    "healthcare": "health_sector",
    "dental": "health_sector",
    "education": "education_sector",
    "restaurant_cafe": "hospitality_sector",
    "hotel_travel": "hospitality_sector",
    "ecommerce_retail": "retail_sector",
    "events": "events_sector",
    "agriculture": "agri_sector",
}

SNI_GROUP_KEYWORDS: list[tuple[str, str]] = [
    ("construction_sector", "bygg"),
    ("construction_sector", "entreprenad"),
    ("construction_sector", "anläggning"),
    ("construction_sector", "elinstallation"),
    ("construction_sector", "vvs"),

    ("it_sector", "it"),
    ("it_sector", "data"),
    ("it_sector", "programmer"),
    ("it_sector", "systemutveck"),

    ("finance_sector", "försäkring"),
    ("finance_sector", "kredit"),
    ("finance_sector", "bank"),
    ("finance_sector", "redovis"),
    ("finance_sector", "revision"),
    ("finance_sector", "fastighet"),

    ("transport_sector", "transport"),
    ("transport_sector", "sjötrafik"),
    ("transport_sector", "frakt"),
    ("transport_sector", "logistik"),

    ("hospitality_sector", "restaurang"),
    ("hospitality_sector", "hotell"),

    ("health_sector", "vård"),
    ("health_sector", "tand"),

    ("education_sector", "utbild"),
    ("retail_sector", "handel"),
    ("retail_sector", "butik"),
]

def sni_group(sni_text: str) -> str:
    t = (sni_text or "").lower().strip()
    if not t or t in ('""', "__no_sni__", "okänd"):
        return ""
    for grp, kw in SNI_GROUP_KEYWORDS:
        if kw in t:
            return grp
    return ""

def classify_from_text(text: str) -> tuple[str, str, float]:
    """
    Returns (w_label, w_raw, w_conf)
    """
    if not text or len(text) < 200:
        return ("", "", 0.0)

    scores: dict[str, float] = {}
    for cat, meta in CATEGORIES.items():
        kw = meta["kw"]
        hit = 0
        for k in kw:
            if k in text:
                hit += 1
        if hit:
            # Kommentar: viktning (många olika träffar => högre score)
            scores[cat] = float(hit)

    if not scores:
        return ("", "", 0.15)

    # sort
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_cat, top_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0

    # Kommentar: bas-conf från dominans
    ratio = (top_score / (second_score + 0.5))
    base = min(1.0, 0.35 + 0.15 * top_score + 0.10 * ratio)

    # clamp
    w_conf = max(0.0, min(1.0, base))

    # raw: ta första rå-etiketten för kategorin
    raw = CATEGORIES[top_cat]["raw"][0] if CATEGORIES[top_cat].get("raw") else top_cat
    return (top_cat, raw, w_conf)

def combine_pages_text(texts: list[str]) -> str:
    # Kommentar: slå ihop, men håll det lagom
    joined = " ".join([t for t in texts if t])
    return joined[:200_000]

def load_done_set(path: Path) -> set[str]:
    done = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as rf:
        for line in rf:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                o = (obj.get("orgnr") or "").strip()
                if o:
                    done.add(o)
            except Exception:
                pass
    return done

def pick_targets(conn: sqlite3.Connection, limit: Optional[int]) -> list[tuple[str, str, str, Optional[str], Optional[str]]]:
    cur = conn.cursor()

    # Kommentar: vi tar bara bolag med website, och som behöver refresh på line_of_work_updated_at
    if limit is None:
        cur.execute(
            """
            SELECT orgnr, name, website, sni_text, line_of_work_updated_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) != ''
            """
        )
        rows = cur.fetchall()
    else:
        cur.execute(
            """
            SELECT orgnr, name, website, sni_text, line_of_work_updated_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) != ''
            LIMIT ?
            """,
            (limit * 5,),
        )
        rows = cur.fetchall()

    out = []
    for orgnr, name, website, sni_text, low_checked_at in rows:
        if not orgnr or not website:
            continue
        if needs_refresh(low_checked_at):
            out.append((orgnr, name or "", website, sni_text or "", low_checked_at))
            if limit is not None and len(out) >= limit:
                break
    return out

def bucket(conf: float) -> str:
    if conf >= 0.80:
        return "HIGH"
    if conf >= 0.50:
        return "MID"
    return "LOW"

def decide_final(w_label: str, w_conf: float, sni_text_val: str) -> tuple[str, float, str]:
    """
    Returns (final_label, final_conf, source)
    source: website | sni | blend
    """
    sni_t = (sni_text_val or "").strip()
    has_sni = bool(sni_t) and sni_t.lower() not in ("__no_sni__", "okänd", '""')

    # Kommentar: om ingen website label => SNI eller unknown
    if not w_label:
        if has_sni:
            return ("unknown", 0.30, "sni")  # Kommentar: final label normaliseras senare av ditt targeting
        return ("unknown", 0.10, "sni")

    if w_conf >= 0.80:
        return (w_label, w_conf, "website")

    if 0.50 <= w_conf < 0.80:
        if not has_sni:
            return (w_label, w_conf, "website")

        wg = GROUPS.get(w_label, "")
        sg = sni_group(sni_t)
        if wg and sg and wg == sg:
            return (w_label, w_conf, "blend")
        # konflikt => SNI för att undvika grova fel
        return ("unknown", 0.35, "sni")

    # LOW
    if has_sni:
        return ("unknown", 0.25, "sni")
    return (w_label, w_conf, "website")

def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    done = load_done_set(OUT_PATH) if RESUME else set()
    limit = None if LIMIT == 0 else LIMIT

    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA journal_mode=WAL;")

    targets = pick_targets(conn, limit)
    targets = [(o, n, w, s, c) for (o, n, w, s, c) in targets if in_shard(o)]

    if RESUME and done:
        targets = [(o, n, w, s, c) for (o, n, w, s, c) in targets if o not in done]

    print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={SHARD_ID}/{SHARD_TOTAL}, REFRESH_DAYS={REFRESH_DAYS})")

    processed = ok = 0
    err_403 = err_429 = err_timeout = err_other = err_not_html = 0

    bucket_counts = {"HIGH": 0, "MID": 0, "LOW": 0}
    conflict = 0

    start = time.time()

    try:
        with OUT_PATH.open("a", encoding="utf-8") as out_f:
            for orgnr, name, website, sni_text_val, prev_checked_at in targets:
                processed += 1

                base_url = normalize_url(website)
                urls_used: list[str] = []
                page_texts: list[str] = []

                # 1) start
                html, err = fetch_html(base_url)
                time.sleep(SLEEP_BETWEEN_REQUESTS)

                # Kommentar: timeout = temporärt => skriv INTE rad (så den kan köras om)
                if err == "timeout":
                    err_timeout += 1
                    if processed % PRINT_EVERY == 0:
                        rate = processed / max(1e-9, time.time() - start)
                        print(f"[{processed}] ok={ok} 403={err_403} 429={err_429} timeout={err_timeout} other={err_other} not_html={err_not_html} | {rate:.2f}/s "
                              f"| buckets: {bucket_counts} conflict={conflict}")
                    continue

                row = {
                    "orgnr": orgnr,
                    "name": name,
                    "website": base_url,
                    "checked_at": utcnow_iso(),
                    "err_reason": "",
                    "sni_text": sni_text_val or "",
                    "w_label": "",
                    "w_raw": "",
                    "w_conf": 0.0,
                    "w_bucket": "LOW",
                    "final_label": "unknown",
                    "final_conf": 0.0,
                    "final_bucket": "LOW",
                    "source": "sni",
                    "urls_used": [],
                }

                if err in ("403", "429"):
                    if err == "403":
                        err_403 += 1
                    else:
                        err_429 += 1
                    row["err_reason"] = err
                    # Kommentar: vid block => fall tillbaka på unknown/sni
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    continue

                if err:
                    if err == "not_html":
                        err_not_html += 1
                        row["err_reason"] = "not_html"
                    else:
                        err_other += 1
                        row["err_reason"] = "other"
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    continue

                urls_used.append(base_url)
                page_texts.append(strip_html_to_text(html or ""))

                # 2) intern “bra” sida
                cand_links = extract_internal_candidate_links(base_url, html or "")
                pages_fetched = 1

                for u in cand_links:
                    if pages_fetched >= MAX_PAGES:
                        break
                    h2, e2 = fetch_html(u)
                    time.sleep(SLEEP_BETWEEN_REQUESTS)

                    if e2 == "timeout":
                        err_timeout += 1
                        continue
                    if e2 in ("403", "429"):
                        if e2 == "403":
                            err_403 += 1
                        else:
                            err_429 += 1
                        continue
                    if e2:
                        continue

                    urls_used.append(u)
                    page_texts.append(strip_html_to_text(h2 or ""))
                    pages_fetched += 1

                    # Kommentar: försök stoppa tidigt om vi redan fått tydlig text
                    if sum(len(t) for t in page_texts) > 20_000:
                        break

                text = combine_pages_text(page_texts)
                w_label, w_raw, w_conf = classify_from_text(text)

                # Kommentar: bestäm final enligt överenskommen regel
                final_label, final_conf, source = decide_final(w_label, w_conf, sni_text_val or "")

                # Kommentar: konflikt-indikator (enkelt)
                if w_label and (source == "sni") and bucket(w_conf) in ("MID", "HIGH"):
                    conflict += 1

                w_bucket = bucket(w_conf)
                bucket_counts[w_bucket] += 1

                row.update({
                    "w_label": w_label or "",
                    "w_raw": w_raw or "",
                    "w_conf": float(round(w_conf, 4)),
                    "w_bucket": w_bucket,
                    "final_label": final_label or "unknown",
                    "final_conf": float(round(final_conf, 4)),
                    "final_bucket": bucket(final_conf),
                    "source": source,
                    "urls_used": urls_used,
                })

                out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                ok += 1

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(f"[{processed}] ok={ok} 403={err_403} 429={err_429} timeout={err_timeout} other={err_other} not_html={err_not_html} | {rate:.2f}/s "
                          f"| buckets: {bucket_counts} conflict={conflict}")

    except KeyboardInterrupt:
        print("\nAvbruten (Ctrl+C) — filen är sparad ✅")
    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed} | OK: {ok}")
    print(f"Errors: 403={err_403} 429={err_429} timeout={err_timeout} other={err_other} not_html={err_not_html}")
    print(f"Buckets: {bucket_counts} | conflict={conflict}")
    print(f"OUT: {OUT_PATH.resolve()}")

if __name__ == "__main__":
    main()
