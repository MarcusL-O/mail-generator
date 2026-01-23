# Skapar lokal preview av mejl.
# Används för test innan skick.
# Kör render_email()
# Skriver HTML-outputen till en .html-fil
# Du öppnar filen i webbläsaren och ser exakt hur mejlet kommer se ut

import argparse
from pathlib import Path
from datetime import datetime

from render_email import render_email


def safe(s: str) -> str:
    # Kommentar (svenska): Gör strängar filnamnssäkra
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True, help="Template name i DB")
    ap.add_argument("--company-name", default="Demo_AB")
    ap.add_argument("--contact-name", default="Anna")
    ap.add_argument("--your-company", default="Din_Firma")
    ap.add_argument("--city", default="Göteborg")
    ap.add_argument("--industry-or-service", default="IT-konsult")
    ap.add_argument("--your-contact-info", default="marcus@example.com")
    args = ap.parse_args()

    context = {
        "company_name": args.company_name.replace("_", " "),
        "your_company": args.your_company.replace("_", " "),
        "city": args.city,
        "industry_or_service": args.industry_or_service,
        "your_contact_info": args.your_contact_info,
    }

    subject, html, _txt = render_email(
        template_name=args.template,
        context=context
    )

    # Kommentar (svenska): Filnamn: datum_template_företag.html
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tpl = safe(args.template.replace(".html", "").replace(".txt", "").replace("/", "_"))
    company = safe(args.company_name)

    out_dir = Path("data/out/email_previews")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{stamp}_{tpl}_{company}.html"

    wrapped = f"""<!doctype html>
<html lang="sv">
<head>
  <meta charset="utf-8" />
  <title>{subject}</title>
</head>
<body>
{html}
</body>
</html>
"""

    out_path.write_text(wrapped, encoding="utf-8")

    print("✓ Preview skapad")
    print(f"template={args.template}")
    print(f"out={out_path.resolve()}")


if __name__ == "__main__":
    main()
