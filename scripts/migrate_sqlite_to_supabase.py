#!/usr/bin/env python3
"""
One-time copy: local cot_quant_master.db -> Supabase PostgreSQL.

Requires:
  DATABASE_URL in env (postgresql://... from Supabase Connect → Transaction pooler)

Usage:
  export DATABASE_URL='postgresql://...'
  python3 scripts/migrate_sqlite_to_supabase.py /path/to/cot_quant_master.db

Copies tables that exist in SQLite (skips missing). Uses pandas to_sql replace for dynamic COT tables.
"""
from __future__ import annotations

import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# pylint: disable=wrong-import-position
from sqlalchemy import create_engine, inspect, text

from engine.pg_schema import ensure_postgres_schema


def main() -> None:
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not dsn.lower().startswith("postgres"):
        print("Set DATABASE_URL to your Supabase Postgres URI.")
        sys.exit(1)
    sqlite_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "cot_quant_master.db")
    if not os.path.isfile(sqlite_path):
        print(f"SQLite file not found: {sqlite_path}")
        sys.exit(1)

    eng = create_engine(dsn, pool_pre_ping=True)
    ensure_postgres_schema()

    with sqlite3.connect(sqlite_path) as sl:
        sl.row_factory = sqlite3.Row
        tables = [r[0] for r in sl.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
        tables = [t for t in tables if not t.startswith("sqlite_")]

    insp = inspect(eng)
    existing_pg = set(insp.get_table_names())

    for table in tables:
        print(f"Copying {table}...")
        import pandas as pd

        with sqlite3.connect(sqlite_path) as sl:
            df = pd.read_sql_query(f'SELECT * FROM "{table}"', sl)
        if df.empty:
            print(f"  skip empty {table}")
            continue
        if_exists = "replace" if table in existing_pg else "replace"
        df.to_sql(table, eng, if_exists=if_exists, index=False, method="multi", chunksize=500)
        with eng.connect() as c:
            if table == "signal_tracker":
                try:
                    c.execute(text("SELECT setval(pg_get_serial_sequence('signal_tracker','id'), COALESCE(MAX(id),1)) FROM signal_tracker"))
                    c.commit()
                except Exception:
                    c.rollback()
            if table == "system_performance_ledger":
                try:
                    c.execute(
                        text(
                            "SELECT setval(pg_get_serial_sequence('system_performance_ledger','id'), COALESCE(MAX(id),1)) FROM system_performance_ledger"
                        )
                    )
                    c.commit()
                except Exception:
                    c.rollback()
        existing_pg.add(table)

    print("Done. Verify in Supabase Table Editor.")


if __name__ == "__main__":
    main()
