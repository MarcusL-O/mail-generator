import sqlite3

c = sqlite3.connect("data/outreach.db.sqlite")

print("TABLES:")
for (n,) in c.execute("select name from sqlite_master where type='table' order by name"):
    print("-", n)

print("\nCOLUMNS:")
for (n,) in c.execute("select name from sqlite_master where type='table' order by name"):
    cols = [r[1] for r in c.execute(f"pragma table_info({n})")]
    print(n, ":", ", ".join(cols))
