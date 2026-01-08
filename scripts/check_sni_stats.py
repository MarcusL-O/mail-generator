import sqlite3

DB_PATH = "data/mail_generator_db.sqlite"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

total = cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
with_sni = cur.execute("SELECT COUNT(*) FROM companies WHERE TRIM(COALESCE(sni_codes,'')) != ''").fetchone()[0]
missing = cur.execute("SELECT COUNT(*) FROM companies WHERE TRIM(COALESCE(sni_codes,'')) = ''").fetchone()[0]

print("TOTAL:", total)
print("WITH_SNI:", with_sni)
print("MISSING_SNI:", missing)

print("\nEXAMPLE WITH SNI:")
for (orgnr, sni) in cur.execute(
    "SELECT orgnr, sni_codes FROM companies WHERE TRIM(COALESCE(sni_codes,'')) != '' LIMIT 5"
).fetchall():
    print(orgnr, sni)

con.close()
