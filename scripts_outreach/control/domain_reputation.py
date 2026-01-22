# outreach/control/domain_reputation.py
from __future__ import annotations

import re
import socket
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

# =========================
# KONFIG
# =========================
DB_PATH = Path("data/db/outreach.db.sqlite")

# Sätt din domän här när du har den (eller lämna tom för auto från settings.from_email)
DOMAIN = ""  # ex: "didup.se"

# SMTP (för blacklist-check på rätt IP senare)
# - Om SMTP_HOST finns (här eller i settings.smtp_host) -> vi resolver den och kollar RBL på den IP:n
SMTP_HOST = ""  # ex: "smtp.mailgun.org" / "email-smtp.eu-north-1.amazonaws.com"
SMTP_PORT = 587  # bara info

# Om du vill tvinga en specifik sending-ip (t.ex. dedikerad IP), sätt här eller i settings.sending_ip
SENDING_IP = ""  # ex: "203.0.113.10"

# DKIM (valfritt): selectors om du vet dem senare
DKIM_SELECTORS: list[str] = []  # ex: ["s1", "selector1"]

# DNSBL-zoner (IP-baserade). Kan rate-limita -> "unknown".
RBL_ZONES: list[str] = [
    "zen.spamhaus.org",
    "bl.spamcop.net",
    "b.barracudacentral.org",
    "dnsbl.sorbs.net",
]

# Trösklar
WARN_BOUNCE_RATE_7D = 0.05   # 5%
DEAD_BOUNCE_RATE_7D = 0.10   # 10%
WARN_COMPLAINT_RATE_30D = 0.003  # 0.3%
DEAD_COMPLAINT_RATE_30D = 0.01   # 1%

SOCKET_TIMEOUT_SEC = 3.0
# =========================


def utc_now_str() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def print_kv(key: str, value: Any) -> None:
    print(f"{key:<24} {value}")


def one(cur: sqlite3.Cursor, sql: str, params: Iterable[Any] = ()) -> Any:
    row = cur.execute(sql, tuple(params)).fetchone()
    return None if row is None else row[0]


def table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    return one(cur, "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)) is not None


def get_setting(cur: sqlite3.Cursor, key: str) -> Optional[str]:
    if not table_exists(cur, "settings"):
        return None
    row = cur.execute("SELECT value FROM settings WHERE key=? LIMIT 1", (key,)).fetchone()
    return None if row is None else str(row[0])


def extract_domain_from_email(email: str) -> Optional[str]:
    s = (email or "").strip()
    if "@" not in s:
        return None
    return s.split("@", 1)[1].strip().lower() or None


def normalize_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = d.split("/", 1)[0]
    return d


# -------------------------
# DNS helpers (dnspython om den finns, annars nslookup/socket)
# -------------------------
def _try_import_dnspython():
    try:
        import dns.resolver  # type: ignore
        return dns.resolver
    except Exception:
        return None


DNSPYTHON = _try_import_dnspython()


def dns_txt(name: str) -> list[str]:
    name = normalize_domain(name)
    out: list[str] = []

    if DNSPYTHON is not None:
        try:
            resolver = DNSPYTHON.Resolver()
            resolver.lifetime = SOCKET_TIMEOUT_SEC
            for rdata in resolver.resolve(name, "TXT"):
                parts = []
                for p in getattr(rdata, "strings", []) or []:
                    try:
                        parts.append(p.decode("utf-8", "ignore"))
                    except Exception:
                        parts.append(str(p))
                if parts:
                    out.append("".join(parts))
                else:
                    out.append(str(rdata).strip().strip('"'))
        except Exception:
            pass
        return out

    try:
        cp = subprocess.run(
            ["nslookup", "-type=txt", name],
            capture_output=True,
            text=True,
            timeout=SOCKET_TIMEOUT_SEC,
        )
        txt = cp.stdout + "\n" + cp.stderr
        for line in txt.splitlines():
            if "text =" in line:
                val = line.split("text =", 1)[1].strip()
                out.append(val.strip('"'))
    except Exception:
        pass
    return out


def dns_mx(domain: str) -> list[str]:
    domain = normalize_domain(domain)
    mx: list[str] = []

    if DNSPYTHON is not None:
        try:
            resolver = DNSPYTHON.Resolver()
            resolver.lifetime = SOCKET_TIMEOUT_SEC
            for rdata in resolver.resolve(domain, "MX"):
                mx.append(str(rdata.exchange).rstrip("."))
        except Exception:
            pass
        return mx

    try:
        cp = subprocess.run(
            ["nslookup", "-type=mx", domain],
            capture_output=True,
            text=True,
            timeout=SOCKET_TIMEOUT_SEC,
        )
        txt = cp.stdout + "\n" + cp.stderr
        for line in txt.splitlines():
            if "mail exchanger" in line:
                mx.append(line.split("=", 1)[1].strip().rstrip("."))
    except Exception:
        pass
    return mx


def resolve_host_ips(host: str) -> list[str]:
    host = normalize_domain(host)
    ips: list[str] = []
    try:
        infos = socket.getaddrinfo(host, None)
        for info in infos:
            ip = info[4][0]
            # Kommentar (svenska): bara IPv4 just nu (RBL)
            if ":" not in ip:
                ips.append(ip)
    except Exception:
        pass
    return sorted(set(ips))


def dns_a(domain: str) -> list[str]:
    return resolve_host_ips(domain)


# -------------------------
# RBL checks (IP)
# -------------------------
@dataclass
class RblResult:
    zone: str
    status: str  # "clean" | "listed" | "unknown"
    response: Optional[str] = None


def _reverse_ip(ip: str) -> str:
    parts = ip.split(".")
    if len(parts) != 4:
        return ""
    return ".".join(reversed(parts))


def rbl_check_ip(ip: str, zone: str) -> RblResult:
    q = f"{_reverse_ip(ip)}.{zone}".strip(".")
    if not q or q.startswith("."):
        return RblResult(zone=zone, status="unknown", response="bad ip")

    if DNSPYTHON is not None:
        try:
            resolver = DNSPYTHON.Resolver()
            resolver.lifetime = SOCKET_TIMEOUT_SEC
            ans = resolver.resolve(q, "A")
            resp = ",".join(str(r) for r in ans)
            return RblResult(zone=zone, status="listed", response=resp)
        except Exception:
            return RblResult(zone=zone, status="clean")

    try:
        socket.gethostbyname(q)
        return RblResult(zone=zone, status="listed", response="A")
    except Exception:
        return RblResult(zone=zone, status="clean")


# -------------------------
# DB trend checks
# -------------------------
def _count_events(cur: sqlite3.Cursor, event_types: tuple[str, ...], days: int) -> int:
    if not table_exists(cur, "events"):
        return 0
    q = f"""
      SELECT COUNT(*)
      FROM events
      WHERE type IN ({",".join("?" for _ in event_types)})
        AND created_at >= datetime('now', ?)
    """
    return int(one(cur, q, (*event_types, f"-{days} day")) or 0)


def _count_sent_like(cur: sqlite3.Cursor, days: int) -> int:
    if not table_exists(cur, "email_messages"):
        return 0
    return int(
        one(
            cur,
            """
            SELECT COUNT(*)
            FROM email_messages
            WHERE created_at >= datetime('now', ?)
              AND status IN ('sent','accepted','queued','scheduled','failed','error')
            """,
            (f"-{days} day",),
        )
        or 0
    )


def _severity_label(score: int) -> str:
    return {0: "OK", 1: "WARNING", 2: "DEAD"}.get(score, "OK")


def _pick_sending_ips(cur: sqlite3.Cursor, domain: str) -> tuple[str, list[str]]:
    """
    Kommentar (svenska):
    Prioritet:
      1) SENDING_IP (konfig)
      2) settings.sending_ip
      3) SMTP_HOST (konfig) -> resolve
      4) settings.smtp_host -> resolve
      5) domain A-record -> resolve
    Returnerar (source_label, ips)
    """
    # 1) hardcoded
    if SENDING_IP.strip():
        return ("sending_ip (config)", [SENDING_IP.strip()])

    # 2) settings
    s_ip = (get_setting(cur, "sending_ip") or "").strip()
    if s_ip:
        return ("sending_ip (settings)", [s_ip])

    # 3) smtp host (config)
    if SMTP_HOST.strip():
        ips = resolve_host_ips(SMTP_HOST.strip())
        return ("smtp_host (config)", ips)

    # 4) smtp host (settings)
    s_host = (get_setting(cur, "smtp_host") or "").strip()
    if s_host:
        ips = resolve_host_ips(s_host)
        return ("smtp_host (settings)", ips)

    # 5) domain A record
    return ("domain A-record", resolve_host_ips(domain))


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB saknas: {DB_PATH}")

    socket.setdefaulttimeout(SOCKET_TIMEOUT_SEC)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()

        # Domain
        dom = normalize_domain(DOMAIN)
        if not dom:
            from_email = get_setting(cur, "from_email") or ""
            dom = normalize_domain(extract_domain_from_email(from_email) or "")

        print("DOMAIN REPUTATION")
        print_kv("Database", str(DB_PATH))
        print_kv("Generated", utc_now_str())
        print_kv("Domain", dom or "(not set)")

        if not dom:
            print("\nSätt DOMAIN i filen (eller settings.from_email i DB).")
            return

        # DNS basics
        print_section("DNS BASICS")
        a_ips = dns_a(dom)
        mx_hosts = dns_mx(dom)
        print_kv("A (ips)", ", ".join(a_ips) if a_ips else "(none)")
        print_kv("MX", ", ".join(mx_hosts) if mx_hosts else "(none)")

        # SPF / DMARC / DKIM
        print_section("AUTH RECORDS")
        txt_root = dns_txt(dom)
        spf = next((t for t in txt_root if t.lower().startswith("v=spf1")), None)
        print_kv("SPF", "PASS" if spf else "MISSING")

        dmarc_name = f"_dmarc.{dom}"
        dmarc_txt = dns_txt(dmarc_name)
        dmarc = next((t for t in dmarc_txt if t.lower().startswith("v=dmarc1")), None)
        print_kv("DMARC", "PASS" if dmarc else "MISSING")

        if DKIM_SELECTORS:
            ok = 0
            for sel in DKIM_SELECTORS:
                name = f"{sel}._domainkey.{dom}"
                txt = dns_txt(name)
                has = any("v=dkim1" in t.lower() for t in txt)
                ok += 1 if has else 0
                print_kv(f"DKIM {sel}", "PASS" if has else "MISSING")
            print_kv("DKIM selectors ok", f"{ok}/{len(DKIM_SELECTORS)}")
        else:
            print_kv("DKIM", "(selectors not set)")

        # Blacklist checks: pick correct IP source
        print_section("BLACKLIST (RBL, IP-based)")
        ip_source, ips = _pick_sending_ips(cur, dom)
        print_kv("ip source", ip_source)
        if ip_source.startswith("smtp_host"):
            print_kv("smtp port", str(SMTP_PORT))

        rbl_any_listed = False
        if not ips:
            print("(no IPs to check)")
        else:
            for ip in ips[:3]:  # Kommentar: begränsa
                print_kv("IP", ip)
                listed_here = 0
                unknown_here = 0
                for zone in RBL_ZONES:
                    res = rbl_check_ip(ip, zone)
                    if res.status == "listed":
                        listed_here += 1
                        rbl_any_listed = True
                    if res.status == "unknown":
                        unknown_here += 1
                    print_kv(f"- {zone}", f"{res.status}{' ' + str(res.response) if res.response else ''}")
                print_kv("listed zones", f"{listed_here}/{len(RBL_ZONES)}")
                if unknown_here:
                    print_kv("unknown zones", f"{unknown_here}")

        # DB trend signals
        print_section("OUTREACH SIGNALS (from DB)")
        sent_7d = _count_sent_like(cur, 7)
        sent_30d = _count_sent_like(cur, 30)

        bounces_7d = _count_events(cur, ("bounced", "bounce"), 7)
        complaints_30d = _count_events(cur, ("complaint",), 30)

        bounce_rate_7d = (bounces_7d / sent_7d) if sent_7d > 0 else 0.0
        complaint_rate_30d = (complaints_30d / sent_30d) if sent_30d > 0 else 0.0

        print_kv("sent last 7d", f"{sent_7d:,}")
        print_kv("bounces last 7d", f"{bounces_7d:,}")
        print_kv("bounce rate 7d", f"{bounce_rate_7d*100:.2f}%")

        print_kv("sent last 30d", f"{sent_30d:,}")
        print_kv("complaints last 30d", f"{complaints_30d:,}")
        print_kv("complaint rate 30d", f"{complaint_rate_30d*100:.3f}%")

        # Verdict
        severity = 0  # 0 OK, 1 WARN, 2 DEAD

        # Missing auth records -> warn
        if not spf or not dmarc:
            severity = max(severity, 1)

        # RBL listing -> dead (konservativ)
        if rbl_any_listed:
            severity = max(severity, 2)

        # DB rates (ignorera små samples)
        if sent_7d >= 20:
            if bounce_rate_7d >= DEAD_BOUNCE_RATE_7D:
                severity = max(severity, 2)
            elif bounce_rate_7d >= WARN_BOUNCE_RATE_7D:
                severity = max(severity, 1)

        if sent_30d >= 100:
            if complaint_rate_30d >= DEAD_COMPLAINT_RATE_30D:
                severity = max(severity, 2)
            elif complaint_rate_30d >= WARN_COMPLAINT_RATE_30D:
                severity = max(severity, 1)

        print("\nSUMMARY")
        print_kv("verdict", _severity_label(severity))
        print_kv("spf", "present" if spf else "missing")
        print_kv("dmarc", "present" if dmarc else "missing")
        print_kv("rbl listed", "yes" if rbl_any_listed else "no")
        print_kv("ip source", ip_source)

    finally:
        con.close()


if __name__ == "__main__":
    main()
