# hiring_review_shards.py
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
DB_PATH = Path("data/companies.db.sqlite")
OUT_PATH = Path(f"data/out/hiring_review_shard{SHARD_ID}.ndjson")
LIMIT = 0              # 0 = ALLA
RESUME = True
PRINT_EVERY = 50
REFRESH_DAYS = 30      # Kommentar: rerun efter 30 dagar
# =========================

TIMEOUT_SECONDS = 12
SLEEP_BETWEEN_REQUESTS = 0.15
MAX_BYTES = 650_000    # Kommentar: vi läser max ~650KB HTML per sida
MAX_PAGES = 7          # Kommentar: liten crawl-budget

session = requests.Session()
session.headers.update({
    "User-Agent": f"Mozilla/5.0 (Didup-HiringReview/1.0; shard={SHARD_ID})"
})

# Kommentar: vi kollar bara egen domän (ingen Indeed/Jobbsafari/ATS-externt)
CAREER_PATH_HINTS = [
    "/karriar", "/karriar/", "/karriar/lediga-jobb", "/jobb", "/jobb/", "/lediga-jobb", "/lediga-jobb/",
    "/career", "/careers", "/careers/", "/jobs", "/jobs/", "/job", "/job/",
    "/work-with-us", "/work-with-us/", "/join", "/join/", "/join-us", "/join-us/",
    "/om-oss/karriar", "/om-oss/karriar/", "/about/careers", "/about/careers/"
]

# Kommentar: “soft” nyckelord (används för att hitta relevanta sidor, men inte för YES längre)
HIRING_KEYWORDS = [
    "vi söker", "vi soeker", "vi anställer", "vi anstaller", "vi rekryterar",
    "lediga jobb", "ledig tjänst", "ledig tjanst", "karriär", "karriar",
    "ansök", "ansok", "rekrytering",
    "we are hiring", "we're hiring", "join our team", "open positions", "open roles",
    "careers", "career", "jobs", "job openings", "apply now"
]

# Kommentar: triggers där vi försöker fånga rolltext efter frasen
ROLE_TRIGGERS = [
    r"\bvi\s+söker\s+",
    r"\bvi\s+soeker\s+",
    r"\bvi\s+anställer\s+",
    r"\bvi\s+anstaller\s+",
    r"\bwe\s+are\s+hiring\s+",
    r"\bwe['’]re\s+hiring\s+",
    r"\bwe\s+are\s+looking\s+for\s+",
]

# Kommentar: ord som ofta är “inte en rolltitel”
GENERIC_NOT_ROLES = {
    "karriär", "karriar", "career", "careers", "jobb", "jobs", "job", "job openings",
    "open positions", "open roles", "join us", "join our team", "about", "om oss",
    "kontakt", "contact", "ansök", "ansok", "apply", "apply now"
}

EMAIL_RE = re.compile(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", re.I)

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

def extract_headings(html: str) -> list[str]:
    # Kommentar: plocka rubriker (ofta jobbtitlar) ur h1-h4
    html_lower = html.lower()
    hs = re.findall(r"<h[1-4][^>]*>([\s\S]{0,200}?)</h[1-4]>", html_lower)
    out: list[str] = []
    for raw in hs[:400]:
        t = re.sub(r"<[^>]+>", " ", raw)
        t = re.sub(r"\s+", " ", t).strip()
        if not t:
            continue
        if 3 <= len(t) <= 80:
            out.append(t)
    return out

def looks_like_role(title: str) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return False
    if t in GENERIC_NOT_ROLES:
        return False
    if "cookie" in t or "integritet" in t or "privacy" in t:
        return False
    if EMAIL_RE.search(t):
        return False
    # Kommentar: minst 2 bokstäver, inte bara siffror
    if not re.search(r"[a-zåäö]", t):
        return False
    if re.fullmatch(r"[\d\W_]+", t):
        return False
    # Kommentar: undvik superkorta generiska rubriker
    if len(t) < 6 and t in {"jobb", "jobs", "career", "karriar"}:
        return False
    return True

def extract_roles_from_triggers(text: str) -> list[str]:
    roles: list[str] = []
    for trig in ROLE_TRIGGERS:
        for m in re.finditer(trig + r"(.{0,90})", text):
            cand = (m.group(1) or "").strip()
            cand = re.split(r"[\.!\?\|\;\:\(\)\[\]\{\\\/]", cand)[0].strip()
            cand = re.sub(r"\s+", " ", cand).strip()
            # Kommentar: rensa vanliga fyllnadsord
            cand = re.sub(r"\b(nu|idag|hos oss|till vårt team|till vart team|just nu|omgående|immediately)\b", "", cand).strip()
            cand = re.sub(r"\s+", " ", cand).strip()
            if 3 <= len(cand) <= 60 and looks_like_role(cand):
                roles.append(cand)
    # dedupe
    seen = set()
    uniq = []
    for r in roles:
        k = r.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)
    return uniq

def count_jobcards_and_apply(html: str, role_candidates: list[str]) -> tuple[int, bool]:
    """
    jobcards_count: antal distinkta rubriker som ser ut som roller
    apply_with_role: om apply/ansök förekommer nära roll-rubrik eller rolltext
    """
    html_lower = html.lower()

    # Kommentar: jobcards = rubriker som ser ut som roller
    headings = extract_headings(html_lower)
    role_headings = [h for h in headings if looks_like_role(h)]
    # dedupe rubriker
    seen = set()
    role_headings_uniq = []
    for h in role_headings:
        k = h.strip().lower()
        if k in seen:
            continue
        seen.add(k)
        role_headings_uniq.append(h)

    jobcards_count = len(role_headings_uniq)

    # Kommentar: apply/ansök kopplad till roll = “apply/ansök” inom närhet av rubrik/rolltext
    apply_words = ["ansök", "ansok", "apply", "apply now", "sök tjänsten", "soek tjansten"]
    apply_with_role = False

    # 1) nära rubriker (snabbt)
    for h in role_headings_uniq[:40]:
        idx = html_lower.find(h)
        if idx == -1:
            continue
        window = html_lower[idx: idx + 800]  # Kommentar: “samma block-ish”
        if any(w in window for w in apply_words):
            apply_with_role = True
            break

    # 2) fallback: nära trigger-extraherade roller
    if not apply_with_role:
        for r in role_candidates[:40]:
            rr = r.lower()
            idx = html_lower.find(rr)
            if idx == -1:
                continue
            window = html_lower[idx: idx + 700]
            if any(w in window for w in apply_words):
                apply_with_role = True
                break

    return jobcards_count, apply_with_role

def is_relevant_page(text: str) -> bool:
    # Kommentar: för att prioritera sidor som verkar vara karriär/jobb
    hits = sum(1 for w in HIRING_KEYWORDS if w in text)
    return hits >= 1

def extract_internal_career_links(base_url: str, html: str) -> list[str]:
    # Kommentar: enkel href-extraktion, men bara interna + karriär-liknande paths
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
        if any(p in path for p in CAREER_PATH_HINTS):
            out.append(absu)

    # dedupe + begränsa
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

def hard_hiring_decision(html: str) -> tuple[bool, str, int, list[str]]:
    """
    HÅRD REGEL:
      YES om minst en av:
        - vi hittar en rolltitel (trigger-extraherad eller rubrik som ser ut som roll)
        - vi hittar apply/ansök-indikator kopplad till roll
        - vi hittar minst 2 distinkta jobbcards/rubriker
      annars NO
    """
    text = strip_text(html)

    roles_from_triggers = extract_roles_from_triggers(text)
    jobcards_count, apply_with_role = count_jobcards_and_apply(html, roles_from_triggers)

    # Kommentar: “rolltitel hittad” = trigger-roll ELLER minst 1 roll-rubrik
    # Vi räknar rubrik-roller via jobcards_count > 0
    role_title_found = (len(roles_from_triggers) >= 1) or (jobcards_count >= 1)

    yes = role_title_found or apply_with_role or (jobcards_count >= 2)

    if yes:
        # Kommentar: count = jobcards om vi har flera, annars 1
        count = jobcards_count if jobcards_count >= 1 else 1
        # Kommentar: vad = trigger-extraherade roller (om tomt, lämna tomt)
        what = "; ".join(roles_from_triggers[:12])[:500]
        return True, what, int(count), roles_from_triggers[:12]

    return False, "", 0, roles_from_triggers[:12]

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
            SELECT orgnr, name, website, hiring_checked_at
            FROM companies
            WHERE website IS NOT NULL AND TRIM(website) != ''
            """
        )
        rows = cur.fetchall()
    else:
        cur.execute(
            """
            SELECT orgnr, name, website, hiring_checked_at
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

                # 1) Hämta startsidan
                html, err = fetch_html(base_url)
                time.sleep(SLEEP_BETWEEN_REQUESTS)

                # Kommentar: timeout => skriv INTE rad
                if err == "timeout":
                    err_timeout += 1
                    continue

                row = {
                    "orgnr": orgnr,
                    "name": name,
                    "website": base_url,
                    "checked_at": utcnow_iso(),
                    "err_reason": "",
                    "hiring_status": "unknown",
                    "hiring_what_text": "",
                    "hiring_count": None,
                    "evidence_url": "",
                }

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

                # 2) Best effort: utvärdera startsida + relevanta karriärsidor
                best_yes = False
                best_what = ""
                best_count = 0
                best_evidence = ""

                # Kommentar: vi utvärderar startsidan först
                yes0, what0, count0, _roles0 = hard_hiring_decision(html or "")
                if yes0:
                    best_yes = True
                    best_what = what0
                    best_count = count0
                    best_evidence = base_url

                # Kommentar: bygg kö med karriär-URL:er
                visited = set([base_url])
                queue: list[str] = []

                # Kommentar: prova vanliga paths direkt
                for p in CAREER_PATH_HINTS:
                    u = urljoin(base_url.rstrip("/") + "/", p.lstrip("/"))
                    if u not in visited:
                        queue.append(u)

                # Kommentar: och länkar från startsidan (endast interna)
                queue.extend(extract_internal_career_links(base_url, html or ""))

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

                    # Kommentar: mur => vi skriver INTE err här (startsidan var OK), bara skippar denna URL
                    if e2 in ("403", "429"):
                        pages_used += 1
                        continue

                    if e2 or not h2:
                        pages_used += 1
                        continue

                    pages_used += 1

                    # Kommentar: vi skippar irrelevanta sidor snabbt
                    if not is_relevant_page(strip_text(h2)):
                        continue

                    y, w, c, _roles = hard_hiring_decision(h2)
                    if y:
                        # Kommentar: första YES vinner (vi vill ha snabb, strikt signal)
                        best_yes = True
                        best_what = w
                        best_count = c
                        best_evidence = u
                        break

                # Kommentar: timeout under crawl => skriv INTE rad
                if timeout_flag:
                    err_timeout += 1
                    continue

                # 3) Sätt output enligt hårda regler
                if best_yes:
                    row["hiring_status"] = "yes"
                    row["hiring_count"] = int(best_count) if best_count >= 1 else 1
                    row["hiring_what_text"] = best_what
                    row["evidence_url"] = best_evidence
                    row["err_reason"] = ""
                else:
                    row["hiring_status"] = "no"
                    row["hiring_count"] = 0
                    row["hiring_what_text"] = ""
                    row["evidence_url"] = ""
                    row["err_reason"] = ""

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
