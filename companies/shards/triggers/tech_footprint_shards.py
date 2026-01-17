# tech_footprint_shards.py
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

import dns.resolver

ap = argparse.ArgumentParser()
ap.add_argument("--shard-id", type=int, required=True)
ap.add_argument("--shard-total", type=int, default=4)
args = ap.parse_args()

SHARD_ID = args.shard_id
SHARD_TOTAL = args.shard_total

# =========================
# ÄNDRA HÄR
# =========================
DB_PATH = Path("data/companies.db.sqlite")
OUT_PATH = Path(f"data/out/tech_footprint_shard{SHARD_ID}.ndjson")
LIMIT = 0              # 0 = ALLA
RESUME = True
PRINT_EVERY = 50
REFRESH_DAYS = 30      # Kommentar: rerun efter 30 dagar
# =========================

TIMEOUT_SECONDS = 12
SLEEP_BETWEEN_REQUESTS = 0.15
MAX_BYTES = 450_000    # Kommentar: vi läser max ~450KB HTML per sida
MAX_PAGES = 5          # Kommentar: liten crawl-budget

session = requests.Session()
session.headers.update({
    "User-Agent": f"Mozilla/5.0 (Didup-TechFootprint/1.0; shard={SHARD_ID})"
})

# Kommentar: interna sidor vi vill prova/leta efter
TECH_PATH_HINTS = [
    "/it", "/it/", "/support", "/support/", "/help", "/help/",
    "/helpdesk", "/helpdesk/", "/servicedesk", "/servicedesk/",
    "/felanmalan", "/felanmälan", "/drift", "/drift/",
    "/kontakt", "/kontakt/", "/contact", "/contact/",
    "/om-oss", "/om-oss/", "/about", "/about/",
    "/privacy", "/integritet", "/cookie"
]

# Kommentar: Microsoft web-triggers (vi räknar kategorier)
MS_TRIGGERS = {
    "collab": ["teams", "sharepoint", "onedrive"],
    "identity_device": ["entra", "entra id", "azure ad", "azure active directory", "intune"],
    "security": ["defender", "microsoft defender"],
    "cloud": ["azure", "microsoft azure"],
}

# Kommentar: “mail-only” texttriggers (om DNS saknas kan detta ge weak/medium)
MS_MAIL_TEXT = ["microsoft 365", "office 365", "o365", "m365", "exchange online", "outlook"]

# Kommentar: Azure-resurs-hints som höjer confidence
AZURE_RESOURCE_HINTS = [
    "azurewebsites.net",
    "blob.core.windows.net",
    "azureedge.net",
    "cloudapp.azure.com",
]

# Kommentar: IT-support signal-ord (stark vs medium)
IT_SUPPORT_STRONG = [
    "it-support", "itsupport", "helpdesk", "service desk", "servicedesk",
    "felanmäl", "felanmal", "supportportal", "ticket", "ärende", "arende"
]
IT_SUPPORT_MEDIUM = [
    "it-avdelning", "it avdelning", "it drift", "it-drift", "drift",
    "infrastruktur", "systemförvaltning", "systemforvaltning"
]

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
        host = parts.hostname or ""
        return _valid_hostname(host)
    except Exception:
        return False

def _is_retryable_status(code: int) -> bool:
    # Kommentar: vi retryar INTE 403/429, bara timeout (timeout => ingen rad)
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

        # Kommentar: läs max MAX_BYTES
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

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not u.startswith("http://") and not u.startswith("https://"):
        u = "https://" + u
    return u

def strip_text(html: str) -> str:
    # Kommentar: snabb text-extraktion
    s = html.lower()
    s = re.sub(r"<script[\s\S]*?</script>", " ", s)
    s = re.sub(r"<style[\s\S]*?</style>", " ", s)
    s = re.sub(r"<!--[\s\S]*?-->", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_internal_links(base_url: str, html: str) -> list[str]:
    # Kommentar: enkel href-extraktion, men bara interna + tech-liknande paths
    html_lower = html.lower()
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html_lower)
    out: list[str] = []

    base_parts = urlsplit(base_url)
    base_host = (base_parts.hostname or "").lower()

    for h in hrefs[:3500]:
        h = (h or "").strip()
        if not h or h.startswith("#"):
            continue
        if h.startswith("mailto:") or h.startswith("tel:"):
            continue

        absu = urljoin(base_url, h)
        parts = urlsplit(absu)
        if parts.scheme not in ("http", "https"):
            continue

        host = (parts.hostname or "").lower()
        if host != base_host:
            continue

        path = (parts.path or "").lower()
        if any(p in path for p in TECH_PATH_HINTS):
            out.append(absu)

    # Kommentar: dedupe + begränsa
    seen = set()
    uniq = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
        if len(uniq) >= 25:
            break
    return uniq

def dns_lookup_m365(domain: str) -> tuple[bool, bool, bool]:
    """
    Returns (m365_mail, spf_hit, mx_hit)
    Kommentar: timeout bubbla upp som exception (vi hanterar i main => ingen rad)
    """
    res = dns.resolver.Resolver(configure=True)
    res.lifetime = 3.0
    res.timeout = 2.0

    spf_hit = False
    mx_hit = False

    # TXT/SPF
    try:
        answers = res.resolve(domain, "TXT")
        for rr in answers:
            txt = b"".join(getattr(rr, "strings", [])).decode("utf-8", errors="ignore").lower()
            if "spf.protection.outlook.com" in txt:
                spf_hit = True
                break
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers):
        pass

    # MX
    try:
        answers = res.resolve(domain, "MX")
        for rr in answers:
            exch = str(rr.exchange).rstrip(".").lower()
            if exch.endswith("mail.protection.outlook.com"):
                mx_hit = True
                break
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, dns.resolver.NoNameservers):
        pass

    m365_mail = spf_hit or mx_hit
    return (m365_mail, spf_hit, mx_hit)

def count_ms_categories(text: str) -> tuple[int, dict[str, bool]]:
    hit = {k: False for k in MS_TRIGGERS.keys()}
    for cat, words in MS_TRIGGERS.items():
        for w in words:
            if w in text:
                hit[cat] = True
                break
    cnt = sum(1 for v in hit.values() if v)
    return cnt, hit

def has_azure_resource_hints(text: str) -> bool:
    return any(h in text for h in AZURE_RESOURCE_HINTS)

def detect_it_support(text: str) -> tuple[str, str]:
    # Returns (signal, confidence)
    if any(w in text for w in IT_SUPPORT_STRONG):
        return ("yes", "high")
    if any(w in text for w in IT_SUPPORT_MEDIUM):
        return ("yes", "medium")
    return ("no", "low")

def detect_microsoft_from_web(text: str) -> tuple[str, Optional[str], str]:
    """
    Returns (status, strength, confidence) baserat på webbsignaler enbart
    """
    cat_count, _hit = count_ms_categories(text)
    azure_hint = has_azure_resource_hints(text)

    if cat_count >= 2:
        # Kommentar: strong via 2+ kategorier
        conf = "medium"
        if azure_hint or cat_count >= 3:
            conf = "high"
        return ("yes", "strong", conf)

    # Kommentar: weak via mail-text eller 1 kategori (svag)
    if any(w in text for w in MS_MAIL_TEXT):
        return ("yes", "weak", "medium")

    if cat_count == 1:
        return ("yes", "weak", "low")

    return ("no", None, "low")

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

def pick_targets(conn: sqlite3.Connection, limit: Optional[int]) -> list[tuple[str, str, str, Optional[str]]]:
    cur = conn.cursor()

    # Kommentar: vi tar bara bolag med website, och bara de som behöver refresh
    if limit is None:
        cur.execute(
            """
            SELECT orgnr, name, website, tech_checked_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) != ''
            """
        )
        rows = cur.fetchall()
    else:
        cur.execute(
            """
            SELECT orgnr, name, website, tech_checked_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) != ''
            LIMIT ?
            """,
            (limit * 5,),
        )
        rows = cur.fetchall()

    out = []
    for orgnr, name, website, checked_at in rows:
        if not orgnr or not website:
            continue
        if needs_refresh(checked_at):
            out.append((orgnr, name or "", website, checked_at))
            if limit is not None and len(out) >= limit:
                break
    return out

def domain_from_website(url: str) -> str:
    parts = urlsplit(url)
    host = (parts.hostname or "").strip().lower().rstrip(".")
    return host

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    done = load_done_set(OUT_PATH) if RESUME else set()
    limit = None if LIMIT == 0 else LIMIT

    conn = sqlite3.connect(f"file:{DB_PATH.as_posix()}?mode=ro", uri=True)
    conn.execute("PRAGMA journal_mode=WAL;")

    targets = pick_targets(conn, limit)
    targets = [(o, n, w, c) for (o, n, w, c) in targets if in_shard(o)]

    if RESUME and done:
        targets = [(o, n, w, c) for (o, n, w, c) in targets if o not in done]

    print(f"Targets: {len(targets)} (LIMIT={LIMIT}, RESUME={RESUME}, SHARD={SHARD_ID}/{SHARD_TOTAL}, REFRESH_DAYS={REFRESH_DAYS})")

    processed = ok = skipped_not_html = 0
    err_403 = err_429 = err_timeout = err_other = 0

    start = time.time()

    try:
        with OUT_PATH.open("a", encoding="utf-8") as out_f:
            for orgnr, name, website, _prev_checked_at in targets:
                processed += 1
                base_url = normalize_url(website)

                # Kommentar: timeout-hantering per orgnr
                timeout_flag = False

                row = {
                    "orgnr": orgnr,
                    "name": name,
                    "website": base_url,
                    "checked_at": utcnow_iso(),
                    "err_reason": "",
                    "microsoft_status": "unknown",
                    "microsoft_strength": None,
                    "microsoft_confidence": "low",
                    "it_support_signal": "unknown",
                    "it_support_confidence": "low",
                }

                # 1) DNS-check (hög signal för mail)
                domain = domain_from_website(base_url)
                try:
                    m365_mail, _spf_hit, _mx_hit = dns_lookup_m365(domain)
                except dns.exception.Timeout:
                    err_timeout += 1
                    continue
                except Exception:
                    # Kommentar: DNS-fel ska inte döda allt; vi fortsätter med webben
                    m365_mail = False

                # 2) Hämta startsidan
                html, err = fetch_html(base_url)
                time.sleep(SLEEP_BETWEEN_REQUESTS)

                # Kommentar: timeout => skriv INTE rad
                if err == "timeout":
                    err_timeout += 1
                    continue

                # 403/429/other/not_html på startsidan => unknown + skriv rad
                if err in ("403", "429"):
                    row["err_reason"] = err
                    if err == "403":
                        err_403 += 1
                    else:
                        err_429 += 1
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    continue

                if err:
                    if err == "not_html":
                        skipped_not_html += 1
                        row["err_reason"] = "not_html"
                    else:
                        err_other += 1
                        row["err_reason"] = "other"
                    out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    continue

                # Kommentar: samla text från crawlade sidor (max budget)
                all_texts: list[str] = []
                t0 = strip_text(html or "")
                all_texts.append(t0)

                visited = set([base_url])
                queue: list[str] = []

                # Kommentar: prova vanliga paths direkt
                for p in TECH_PATH_HINTS:
                    u = urljoin(base_url.rstrip("/") + "/", p.lstrip("/"))
                    if u not in visited:
                        queue.append(u)

                # Kommentar: och länkar från startsidan (endast interna)
                queue.extend(extract_internal_links(base_url, html or ""))

                pages_used = 1
                while queue and pages_used < MAX_PAGES:
                    u = queue.pop(0)
                    if u in visited:
                        continue
                    visited.add(u)

                    h2, e2 = fetch_html(u)
                    time.sleep(SLEEP_BETWEEN_REQUESTS)

                    if e2 == "timeout":
                        timeout_flag = True
                        break

                    if e2 in ("403", "429"):
                        pages_used += 1
                        continue

                    if e2 or not h2:
                        pages_used += 1
                        continue

                    pages_used += 1
                    all_texts.append(strip_text(h2))

                # Kommentar: timeout under crawl => skriv INTE rad
                if timeout_flag:
                    err_timeout += 1
                    continue

                combined_text = " ".join(all_texts)

                # 3) IT-support
                it_signal, it_conf = detect_it_support(combined_text)
                row["it_support_signal"] = it_signal
                row["it_support_confidence"] = it_conf

                # 4) Microsoft från webben
                ms_status_web, ms_strength_web, ms_conf_web = detect_microsoft_from_web(combined_text)

                # 5) Slutbeslut för Microsoft (web strong vinner, annars DNS mail => weak/high)
                if ms_status_web == "yes" and ms_strength_web == "strong":
                    row["microsoft_status"] = "yes"
                    row["microsoft_strength"] = "strong"
                    row["microsoft_confidence"] = ms_conf_web
                elif m365_mail:
                    row["microsoft_status"] = "yes"
                    row["microsoft_strength"] = "weak"
                    row["microsoft_confidence"] = "high"
                elif ms_status_web == "yes":
                    row["microsoft_status"] = "yes"
                    row["microsoft_strength"] = ms_strength_web or "weak"
                    row["microsoft_confidence"] = ms_conf_web
                else:
                    row["microsoft_status"] = "no"
                    row["microsoft_strength"] = None
                    row["microsoft_confidence"] = "low"

                # 6) skriv rad
                out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
                ok += 1

                if processed % PRINT_EVERY == 0:
                    rate = processed / max(1e-9, time.time() - start)
                    print(
                        f"[{processed}] ok={ok} not_html={skipped_not_html} "
                        f"403={err_403} 429={err_429} timeout={err_timeout} other={err_other} | {rate:.1f}/s"
                    )

    except KeyboardInterrupt:
        print("\nAvbruten (Ctrl+C) — filen är sparad ✅")
    finally:
        conn.close()

    print("KLART ✅")
    print(f"Processade: {processed} | OK: {ok} | not_html: {skipped_not_html}")
    print(f"Errors: 403={err_403} 429={err_429} timeout={err_timeout} other={err_other}")
    print(f"OUT: {OUT_PATH.resolve()}")

if __name__ == "__main__":
    main()
