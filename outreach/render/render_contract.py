# scripts_outreach/render/render_contract.py
# Kommentar (svenska):
# - Läser avtalsmall från templates/contracts/supplier_contract.md
# - Ersätter {{placeholders}} med värden från context
# - Returnerar renderad avtals-text (str)

import re
from pathlib import Path
from typing import Any, Dict

CONTRACT_TEMPLATE_PATH = Path("templates/contracts/supplier_contract.md")

_PLACEHOLDER_RE = re.compile(
    r"\{\{\s*(?P<key>[a-zA-Z0-9_]+)\s*(\|\s*default\s*:\s*\"(?P<default>[^\"]*)\")?\s*\}\}"
)


def _render_placeholders(text: str, context: Dict[str, Any]) -> str:
    def repl(m: re.Match) -> str:
        key = m.group("key")
        default = m.group("default")
        if key in context and context[key] is not None:
            return str(context[key])
        return default if default is not None else ""

    return _PLACEHOLDER_RE.sub(repl, text)


def render_contract(*, context: Dict[str, Any], template_path: Path = CONTRACT_TEMPLATE_PATH) -> str:
    if not template_path.exists():
        raise FileNotFoundError(f"Kontraktsmall saknas: {template_path}")

    raw = template_path.read_text(encoding="utf-8")
    rendered = _render_placeholders(raw, context)

    # Kommentar (svenska): Trimma onödiga trailing spaces
    return "\n".join([line.rstrip() for line in rendered.splitlines()]).strip() + "\n"


if __name__ == "__main__":
    demo = {
        "YOUR_COMPANY_NAME": "Din Firma AB",
        "YOUR_ORGNR": "559000-0000",
        "SUPPLIER_COMPANY_NAME": "Leverantör AB",
        "SUPPLIER_ORGNR": "556000-0000",
        "PRICE_PER_MEETING": "2 500 kr",
        "SUCCESS_FEE": "5% av affärsvärdet",
        "PAYMENT_TERMS": "10",
    }
    print(render_contract(context=demo)[:1200])
