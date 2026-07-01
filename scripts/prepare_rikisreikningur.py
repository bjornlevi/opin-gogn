#!/usr/bin/env python3
"""Prepare Ríkisreikningur CSV files into a combined Parquet dataset."""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Error: pandas required. Install with: pip install pandas pyarrow", file=sys.stderr)
    sys.exit(1)


FILE_RE = re.compile(r"Rikisreikningur_gogn_(\d{4})(?:_(\d{2}))?\.csv$")


def normalize_column_name(col: str) -> str:
    col = col.strip().lstrip("\ufeff")
    mapping = {
        "TimabilAr": "year",
        "Samtals": "amount",
    }
    return mapping.get(col, col)


def parse_file_info(path: Path) -> tuple[int, int | None]:
    match = FILE_RE.match(path.name)
    if not match:
        raise ValueError(f"Unexpected file name format: {path.name}")
    year = int(match.group(1))
    month = int(match.group(2)) if match.group(2) else None
    return year, month


def read_csv(path: Path) -> pd.DataFrame:
    year, month = parse_file_info(path)
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    df.columns = [normalize_column_name(c) for c in df.columns]
    df["year"] = pd.to_numeric(df.get("year"), errors="coerce").fillna(year).astype("Int64")
    df["amount"] = pd.to_numeric(df.get("amount"), errors="coerce")
    df["source_file"] = path.name
    df["coverage_months"] = month if month is not None else 12
    df["is_partial_year"] = month is not None and month < 12
    return df


def combine_csvs(input_dir: Path, output_file: Path) -> None:
    csv_files = sorted(input_dir.glob("Rikisreikningur_gogn_*.csv"))
    if not csv_files:
        raise RuntimeError(f"No input files found in {input_dir}")

    print(f"Reading {len(csv_files)} Ríkisreikningur files...")
    frames = []
    for path in csv_files:
        print(f"  {path.name}")
        frames.append(read_csv(path))

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(
        by=["year", "Timabil", "RaduneytiNumer", "StofnunNumer", "FjarlagavidfangNumer", "TegundNumer"],
        kind="stable",
    )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(output_file, index=False, compression="snappy")
    print(f"Wrote {output_file} ({len(combined)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare Ríkisreikningur CSV files into Parquet")
    parser.add_argument(
        "--input-dir",
        default="data/rikisreikningur",
        help="Directory containing Rikisreikningur_gogn_*.csv files",
    )
    parser.add_argument(
        "--output",
        default="data/rikisreikningur/processed/rikisreikningur_combined.parquet",
        help="Output Parquet file",
    )
    args = parser.parse_args()

    combine_csvs(Path(args.input_dir), Path(args.output))


if __name__ == "__main__":
    main()
