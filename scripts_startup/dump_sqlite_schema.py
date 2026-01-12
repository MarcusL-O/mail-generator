# dump_sqlite_schema.py
import sqlite3
from pathlib import Path

def dump(db_path: str):
    db_path = str(Path(db_path))
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    print("\n" + "="*80)
    print(f"DB: {db_path}")
    print("="*80)

    # SQLite settings
    cur.execute("PRAGMA foreign_keys;")
    print(f"\nPRAGMA foreign_keys = {cur.fetchone()[0]}")

    # List tables/views
    cur.execute("""
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE type IN ('table','view')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name;
    """)
    objects = cur.fetchall()

    for obj in objects:
        otype, name, tbl_name, sql = obj["type"], obj["name"], obj["tbl_name"], obj["sql"]
        print("\n" + "-"*80)
        print(f"{otype.upper()}: {name}")
        print("-"*80)

        if sql:
            print("\n[CREATE SQL]")
            print(sql.strip() + ";")
        else:
            print("\n[CREATE SQL] <none>")

        if otype == "table":
            # Columns
            cur.execute(f"PRAGMA table_info('{name}');")
            cols = cur.fetchall()
            print("\n[COLUMNS]")
            for c in cols:
                # cid, name, type, notnull, dflt_value, pk
                print(f"- {c['name']:<25} {c['type']:<15} "
                      f"{'NOT NULL' if c['notnull'] else 'NULL':<8} "
                      f"DEFAULT={c['dflt_value']} PK={c['pk']}")

            # Foreign keys
            cur.execute(f"PRAGMA foreign_key_list('{name}');")
            fks = cur.fetchall()
            print("\n[FOREIGN KEYS]")
            if not fks:
                print("- (none)")
            else:
                for fk in fks:
                    # id, seq, table, from, to, on_update, on_delete, match
                    print(f"- {fk['from']} -> {fk['table']}.{fk['to']} "
                          f"(ON UPDATE {fk['on_update']}, ON DELETE {fk['on_delete']}, MATCH {fk['match']})")

            # Indexes
            cur.execute(f"PRAGMA index_list('{name}');")
            idxs = cur.fetchall()
            print("\n[INDEXES]")
            if not idxs:
                print("- (none)")
            else:
                for idx in idxs:
                    # seq, name, unique, origin, partial
                    idx_name = idx["name"]
                    print(f"- {idx_name} (UNIQUE={idx['unique']}, ORIGIN={idx['origin']}, PARTIAL={idx['partial']})")
                    cur.execute(f"PRAGMA index_info('{idx_name}');")
                    idx_cols = cur.fetchall()
                    if idx_cols:
                        print("  cols:", ", ".join([ic["name"] for ic in idx_cols]))

    con.close()


if __name__ == "__main__":
    # Ändra paths eller kör med egna
    dump("data/companies.db.sqlite")
    dump("data/outreach.db.sqlite")
