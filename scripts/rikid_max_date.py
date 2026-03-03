#!/usr/bin/env python3
"""Print the first day of the latest month in the rikid parquet, for use in Makefile."""
import sys
import datetime
import duckdb
from pathlib import Path

parquet = Path(__file__).resolve().parents[1] / "data/rikid/parquet/opnirreikningar.parquet"

try:
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW d AS SELECT * FROM read_parquet('{parquet}')")
    row = con.execute('SELECT MAX("Dags.greiðslu") FROM d').fetchone()
    d = row[0] if row and row[0] else datetime.date(2017, 1, 1)
    print(d.replace(day=1).isoformat())
except Exception:
    print("2017-01-01")
