# scripts_debug/list_email_template_names.py
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "outreach.db.sqlite"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

for row in cur.execute("""
    SELECT id, name
    FROM templates
    WHERE channel='email'
    ORDER BY id
"""):
    print(row)

con.close()
