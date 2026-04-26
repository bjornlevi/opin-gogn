#!/usr/bin/env python3
"""Detect the maximum date in a Parquet file's date column.

Used to determine where to start incremental downloads.
"""
import sys
from datetime import datetime
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("2017-01-01")
    sys.exit(0)


def get_max_date(parquet_file: Path, date_column: str = "Dags.greiðslu") -> str:
    """Get the maximum date from a Parquet file's date column.

    Returns date in YYYY-MM-DD format, or 2017-01-01 if file doesn't exist.
    """
    if not parquet_file.exists():
        return "2017-01-01"

    try:
        con = duckdb.connect(":memory:")
        result = con.execute(
            f"SELECT MAX(\"{date_column}\") FROM read_parquet('{parquet_file}')"
        ).fetchone()

        if result and result[0]:
            date_obj = result[0]
            if isinstance(date_obj, str):
                return date_obj.split()[0]  # Handle datetime string
            else:
                return date_obj.isoformat()

        return "2017-01-01"
    except Exception:
        return "2017-01-01"


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: detect_max_date.py <parquet_file> [date_column]")
        sys.exit(1)

    parquet_file = Path(sys.argv[1])
    date_column = sys.argv[2] if len(sys.argv) > 2 else "Dags.greiðslu"

    print(get_max_date(parquet_file, date_column))
