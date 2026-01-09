import sqlite3

DB_PATH = "data/companies.db.sqlite"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

total = cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

has_website = cur.execute(
    "SELECT COUNT(*) FROM companies WHERE TRIM(COALESCE(website,'')) != ''"
).fetchone()[0]

no_website_but_checked = cur.execute(
    """
    SELECT COUNT(*)
    FROM companies
    WHERE TRIM(COALESCE(website,'')) = ''
      AND TRIM(COALESCE(website_checked_at,'')) != ''
    """
).fetchone()[0]

not_checked_yet = cur.execute(
    """
    SELECT COUNT(*)
    FROM companies
    WHERE TRIM(COALESCE(website_checked_at,'')) = ''
    """
).fetchone()[0]

print("TOTAL:", total)
print("HAS_WEBSITE:", has_website)
print("NO_WEBSITE_BUT_CHECKED:", no_website_but_checked)
print("NOT_CHECKED_YET:", not_checked_yet)

con.close()
