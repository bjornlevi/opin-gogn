#!/usr/bin/env python3
"""Rebuild the Rikið master parquet from cached monthly chunk files."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def rebuild(chunks_dir: Path, output_file: Path) -> None:
    chunk_files = sorted(chunks_dir.glob("opnirreikningar_*.parquet"))
    if not chunk_files:
        raise RuntimeError(f"No chunk parquet files found in {chunks_dir}")

    selects = [
        f"SELECT * FROM read_parquet('{sql_path(path)}')"
        for path in chunk_files
    ]
    union_sql = " UNION ALL ".join(selects)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_output = output_file.with_suffix(".tmp.parquet")
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"COPY (SELECT DISTINCT * FROM ({union_sql}) t) "
            f"TO '{sql_path(tmp_output)}' (FORMAT PARQUET)"
        )
        tmp_output.replace(output_file)
    finally:
        con.close()
        tmp_output.unlink(missing_ok=True)

    print(f"Rebuilt {output_file} from {len(chunk_files)} chunk files")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild Rikið parquet from cached chunk files")
    parser.add_argument("--chunks-dir", default="data/rikid/parquet/chunks", help="Chunk parquet directory")
    parser.add_argument("--output", default="data/rikid/parquet/opnirreikningar.parquet", help="Rebuilt output parquet")
    args = parser.parse_args()

    try:
        rebuild(Path(args.chunks_dir), Path(args.output))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
