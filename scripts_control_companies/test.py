import sqlite3

DB_PATH = "data/companies.db.sqlite"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

rows = cur.execute("""
SELECT TRIM(COALESCE(city,'')) AS city, COUNT(*) AS cnt
FROM companies
GROUP BY TRIM(COALESCE(city,''))
ORDER BY cnt DESC
LIMIT 200
""").fetchall()

for city, cnt in rows:
    print(f"{cnt:>8}  {repr(city)}")

con.close()
