# scripts_outreach/render/render_contract_to_pdf.py
# Kommentar (svenska):
# - Tar renderad avtals-text och skriver till PDF
# - Sparar under data/out/contracts/
# - Returnerar Path till PDF-filen

from pathlib import Path
from datetime import datetime, timezone

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4

OUT_DIR = Path("data/out/contracts")


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def contract_text_to_pdf(
    *,
    contract_text: str,
    supplier_orgnr: str,
    title: str = "Förmedlingsavtal – B2B",
    out_dir: Path = OUT_DIR,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_orgnr = (supplier_orgnr or "unknown").replace("/", "_").replace("\\", "_").replace(" ", "")
    pdf_path = out_dir / f"contract_{safe_orgnr}_{_now_stamp()}.pdf"

    c = canvas.Canvas(str(pdf_path), pagesize=A4)
    width, height = A4

    # Kommentar (svenska): Grundlayout
    x = 50
    y = height - 60
    line_height = 14

    # Titel
    c.setFont("Helvetica-Bold", 14)
    c.drawString(x, y, title)
    y -= 26

    c.setFont("Helvetica", 10)

    # Kommentar (svenska): Skriv rad för rad, skapa ny sida vid behov
    for line in contract_text.splitlines():
        if y < 60:
            c.showPage()
            c.setFont("Helvetica", 10)
            y = height - 60

        # Enkel “wrap” om raden är lång (grovt men funkar bra i MVP)
        # Kommentar (svenska): Vi wrappar på teckenlängd, inte ordperfekt.
        max_chars = 110
        if len(line) <= max_chars:
            c.drawString(x, y, line)
            y -= line_height
        else:
            chunk = line
            while chunk:
                part = chunk[:max_chars]
                chunk = chunk[max_chars:]
                c.drawString(x, y, part)
                y -= line_height
                if y < 60 and chunk:
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    y = height - 60

    c.save()
    return pdf_path


if __name__ == "__main__":
    demo_text = "DEMO AVTAL\n\nRad 1\nRad 2\n" + ("Lång rad " * 30)
    p = contract_text_to_pdf(contract_text=demo_text, supplier_orgnr="556000-0000")
    print("Skapad:", p)
