import sqlite3

DB_PATH = "data/companies.db.sqlite"

NO_SNI_MARK = "__NO_SNI__"
BAD_SNI = "00000"  # räknas som "inte kollad"

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

total = cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

valid_sni = cur.execute(
    """
    SELECT COUNT(*)
    FROM companies
    WHERE TRIM(COALESCE(sni_codes,'')) != ''
      AND TRIM(sni_codes) != ?
      AND TRIM(sni_codes) != ?
    """,
    (NO_SNI_MARK, BAD_SNI),
).fetchone()[0]

no_sni = cur.execute(
    """
    SELECT COUNT(*)
    FROM companies
    WHERE TRIM(COALESCE(sni_codes,'')) = ?
    """,
    (NO_SNI_MARK,),
).fetchone()[0]

not_checked = cur.execute(
    """
    SELECT COUNT(*)
    FROM companies
    WHERE TRIM(COALESCE(sni_codes,'')) = ''
       OR TRIM(sni_codes) = ?
    """,
    (BAD_SNI,),
).fetchone()[0]

checked_total = valid_sni + no_sni

print("TOTAL:", total)
print("CHECKED_TOTAL:", checked_total)
print("VALID_SNI:", valid_sni)
print("NO_SNI (__NO_SNI__):", no_sni)
print("NOT_CHECKED_YET:", not_checked)

con.close()
