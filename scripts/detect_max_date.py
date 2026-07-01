#!/usr/bin/env python3
"""Detect the maximum date in a Parquet file's date column.

Used to determine where to start incremental downloads.
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("2017-01-01")
    sys.exit(0)


def get_max_date(parquet_file: Path, date_column: str = "Dags.greiðslu", next_day: bool = False) -> str:
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
                date_obj = datetime.fromisoformat(date_obj.split()[0]).date()
            else:
                date_obj = date_obj.date() if hasattr(date_obj, "date") else date_obj
            if next_day:
                date_obj = date_obj + timedelta(days=1)
            return date_obj.isoformat()

        return "2017-01-01"
    except Exception:
        return "2017-01-01"


if __name__ == "__main__":
    next_day = "--next-day" in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != "--next-day"]
    if len(args) < 1:
        print("Usage: detect_max_date.py <parquet_file> [date_column] [--next-day]")
        sys.exit(1)

    parquet_file = Path(args[0])
    date_column = args[1] if len(args) > 1 else "Dags.greiðslu"

    print(get_max_date(parquet_file, date_column, next_day=next_day))
