import sqlite3

DB_PATH = "data/mail_generator_db.sqlite"

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1) Hur många företag har emails?
    count = cur.execute(
        """
        SELECT COUNT(*)
        FROM companies
        WHERE emails IS NOT NULL
          AND TRIM(emails) <> ''
        """
    ).fetchone()[0]

    print(f"Antal företag med emails: {count}")

    # 2) Visa senaste 50 som fick emails
    rows = cur.execute(
        """
        SELECT orgnr, name, emails, email_status, emails_checked_at
        FROM companies
        WHERE emails IS NOT NULL
          AND TRIM(emails) <> ''
        ORDER BY emails_checked_at DESC
        LIMIT 50
        """
    ).fetchall()

    print("\nSenast hittade emails:")
    for r in rows:
        print(r)

    conn.close()


if __name__ == "__main__":
    main()
