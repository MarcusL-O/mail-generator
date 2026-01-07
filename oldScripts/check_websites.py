import sqlite3

conn = sqlite3.connect("data/mail_generator_db.sqlite")
cur = conn.cursor()

# Visa senast kollade företag
rows = cur.execute(
    """
    SELECT orgnr, name, website, website_status, website_checked_at
    FROM companies
    WHERE website_checked_at IS NOT NULL
    ORDER BY website_checked_at DESC
    LIMIT 100
    """
).fetchall()

print("Senast kollade företag:")
for r in rows:
    print(r)

conn.close()