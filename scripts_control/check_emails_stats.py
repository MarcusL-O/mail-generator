import sqlite3

DB_PATH = "data/companies.db.sqlite"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

total = cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

has_emails = cur.execute(
    "SELECT COUNT(*) FROM companies WHERE TRIM(COALESCE(emails,'')) != ''"
).fetchone()[0]

no_emails_but_checked = cur.execute(
    """
    SELECT COUNT(*)
    FROM companies
    WHERE TRIM(COALESCE(emails,'')) = ''
      AND TRIM(COALESCE(emails_checked_at,'')) != ''
    """
).fetchone()[0]

not_checked_yet = cur.execute(
    """
    SELECT COUNT(*)
    FROM companies
    WHERE TRIM(COALESCE(emails_checked_at,'')) = ''
    """
).fetchone()[0]

print("TOTAL:", total)
print("HAS_EMAILS:", has_emails)
print("NO_EMAILS_BUT_CHECKED:", no_emails_but_checked)
print("NOT_CHECKED_YET:", not_checked_yet)

con.close()
