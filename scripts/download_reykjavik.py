#!/usr/bin/env python3
"""Download Reykjavík annual reports (arsuppgjör) from gagnagatt.reykjavik.is (CKAN API).

Fetches CSV files for each year from the CKAN API and combines them into a single Parquet file.
"""
import sys
import argparse
import json
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
import tempfile
import shutil

try:
    import duckdb
    import pandas as pd
except ImportError:
    print("Error: duckdb and pandas required. Install with: pip install duckdb pandas", file=sys.stderr)
    sys.exit(1)


GAGNAGATT_API = "https://gagnagatt.reykjavik.is/api/3/action/package_show?id=arsuppgjor"


def fetch_json(url: str) -> dict:
    """Fetch JSON from URL."""
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.URLError as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        raise


def get_dataset_resources() -> list[dict]:
    """Fetch available resources from CKAN API."""
    print("Fetching dataset metadata from gagnagatt.reykjavik.is...")
    data = fetch_json(GAGNAGATT_API)

    if not data.get("success"):
        raise RuntimeError(f"CKAN API error: {data.get('error')}")

    resources = data["result"]["resources"]
    csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]

    print(f"Found {len(csv_resources)} CSV files")
    return csv_resources


def download_csv(url: str, output_path: Path) -> bool:
    """Download a CSV file."""
    try:
        print(f"  Downloading {output_path.name}...")
        with urllib.request.urlopen(url, timeout=60) as response:
            with open(output_path, "wb") as f:
                f.write(response.read())
        return True
    except urllib.error.URLError as e:
        print(f"    Error: {e}", file=sys.stderr)
        return False


def normalize_column_name(col: str) -> str:
    """Normalize column names to match expected schema."""
    col = col.strip().lower()
    # Map common variations
    mapping = {
        "year": "year",
        "ár": "year",
        "year_of_record": "year",
        "raun": "raun",
        "samtala0": "samtala0",
        "samtala1": "samtala1",
        "samtala2": "samtala2_canonical",  # Use canonical name
        "samtala3": "samtala3",
        "tegund0": "tegund0",
        "tegund1": "tegund1",
        "tegund2": "tegund2",
        "tegund3": "tegund3",
        "is_correction": "is_correction",
    }
    return mapping.get(col, col)


def parse_icelandic_number(val) -> float | None:
    """Parse Icelandic-formatted numbers (dots for thousands, comma for decimal)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        val = str(val).strip()
        if not val:
            return None
        # Icelandic format: "123.456,78" → convert to "123456.78"
        val = val.replace(".", "").replace(",", ".")
        return float(val)
    except (ValueError, AttributeError):
        return None


def extract_year_from_filename(filename: str) -> int | None:
    """Extract year from filename like 'arsuppgjor_2023.csv' or '2023.csv'."""
    import re
    # Try to find a 4-digit year (1900-2099)
    match = re.search(r'(19|20)\d{2}', filename)
    if match:
        return int(match.group(0))
    return None


def combine_csvs_to_parquet(csv_files: list[Path], output_file: Path) -> None:
    """Combine multiple CSV files into a single Parquet file."""
    print(f"\nCombining {len(csv_files)} CSV files into Parquet...")

    dfs = []
    for csv_file in csv_files:
        try:
            print(f"  Reading {csv_file.name}...")
            # Reykjavik CSVs are semicolon-delimited
            df = pd.read_csv(csv_file, sep=";", dtype_backend="numpy_nullable", low_memory=False)

            # Extract year from filename first
            year_from_file = extract_year_from_filename(csv_file.name)

            # Reykjavik uses 'ar' for year, rename to 'year'
            if "ar" in df.columns:
                df = df.rename(columns={"ar": "year"})

            # If year column still doesn't exist or is missing values, use year from filename
            if "year" not in df.columns and year_from_file is not None:
                df["year"] = year_from_file
            elif "year" in df.columns and year_from_file is not None:
                df["year"] = df["year"].fillna(year_from_file)

            # Parse Icelandic-formatted numbers in 'raun' column
            if "raun" in df.columns:
                df["raun"] = df["raun"].apply(parse_icelandic_number)

            # Normalize column names
            df.columns = [normalize_column_name(c) for c in df.columns]

            # Add correction flag if not present
            if "is_correction" not in df.columns:
                df["is_correction"] = False

            dfs.append(df)
        except Exception as e:
            print(f"    Error reading {csv_file}: {e}", file=sys.stderr)

    if not dfs:
        raise RuntimeError("No CSV files were successfully read")

    combined = pd.concat(dfs, ignore_index=True)
    print(f"  Combined {len(combined)} rows")

    # Convert to Parquet
    combined.to_parquet(output_file, index=False, compression="snappy")
    print(f"Wrote {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Download Reykjavík annual reports from CKAN"
    )
    parser.add_argument("--output", dest="output_file",
                       default="arsuppgjor_combined.parquet",
                       help="Output Parquet file")
    parser.add_argument("--years", dest="years", default=None,
                       help="Comma-separated years to download (e.g., 2023,2024)")
    args = parser.parse_args()

    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        resources = get_dataset_resources()
    except Exception as e:
        print(f"Failed to fetch dataset: {e}", file=sys.stderr)
        sys.exit(1)

    # Filter by year if specified
    if args.years:
        year_list = set(args.years.split(","))
        resources = [r for r in resources if any(y in r.get("name", "") for y in year_list)]

    # Create temp dir for CSVs
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        csv_files = []

        for resource in resources:
            url = resource.get("url")
            name = resource.get("name", "resource")

            if not url:
                continue

            output_path = tmpdir / f"{name}.csv"
            if download_csv(url, output_path):
                csv_files.append(output_path)

        if csv_files:
            combine_csvs_to_parquet(csv_files, output_file)
        else:
            print("No CSV files were successfully downloaded", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
