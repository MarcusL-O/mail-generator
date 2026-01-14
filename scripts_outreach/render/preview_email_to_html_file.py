#Skapar lokal preview av mejl.
#Används för test innan skick.
#Kör render_email()
#Skriver HTML-outputen till en .html-fil
#Du öppnar filen i webbläsaren och ser exakt hur mejlet kommer se ut
#Varför det är viktigt:
#Du ser att placeholders, radbrytningar och signature ser rätt ut
#Du felsöker utan att skicka mejl

import argparse
from pathlib import Path
from datetime import datetime

from render_email import render_email


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", required=True, help="Template name i DB, ex: email_customer_intro/A.html")
    ap.add_argument("--out", default="", help="Output path .html (valfritt)")
    ap.add_argument("--company-name", default="Demo AB")
    ap.add_argument("--contact-name", default="Anna")
    ap.add_argument("--your-company", default="Din Firma")
    ap.add_argument("--city", default="Göteborg")
    ap.add_argument("--industry-or-service", default="IT-konsult")
    ap.add_argument("--your-contact-info", default="marcus@example.com")
    args = ap.parse_args()

    context = {
        "company_name": args.company_name,
        "contact_name": args.contact_name,
        "your_company": args.your_company,
        "city": args.city,
        "industry_or_service": args.industry_or_service,
        "your_contact_info": args.your_contact_info,
    }

    subject, html, _txt = render_email(template_name=args.template, context=context)

    out_path = Path(args.out) if args.out else Path(
        f"data/out/preview_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Kommentar (svenska): Wrap så du kan öppna filen direkt i browser.
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
