import sqlite3
from pathlib import Path

# Svensk kommentar: ändra bara sökvägarna om dina DB ligger någon annanstans
COMPANIES_DB = Path("data/companies.db.sqlite")
OUTREACH_DB  = Path("data/outreach.db.sqlite")

def show_table(conn, table: str):
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    print(f"\n=== {table} kolumner ===")
    for c in cols:
        # c: (cid, name, type, notnull, dflt_value, pk)
        print("-", c[1])

def show_example_row(conn, table: str):
    row = conn.execute(f"SELECT * FROM {table} LIMIT 1").fetchone()
    if row is None:
        print(f"(ingen data i {table})")
        return
    cols = [c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    print(f"\n=== Exempelrad: {table} ===")
    for k, v in zip(cols, row):
        print(f"{k} = {v}")

def main():
    with sqlite3.connect(COMPANIES_DB) as c:
        # Svensk kommentar: byt tabellnamn om din companies-db skiljer sig
        show_table(c, "companies")
        show_example_row(c, "companies")

    with sqlite3.connect(OUTREACH_DB) as o:
        # Svensk kommentar: byt tabellnamn om din outreach-db skiljer sig
        # (jag gissar "leads", annars kommer felet visa vad som saknas)
        show_table(o, "leads")
        show_example_row(o, "leads")

if __name__ == "__main__":
    main()
