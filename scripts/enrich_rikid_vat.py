#!/usr/bin/env python3
"""
Extract and create a VAT number lookup table from Rikið data.

This script creates a separate parquet file with seller -> VAT number mappings.
Unlike Reykjavík data, Rikið doesn't have built-in VSK numbers, so this file
serves as a template for enrichment from external sources.

Usage:
    python3 enrich_rikid_vat.py --input <path> --output <path>
"""
import argparse
import duckdb
from pathlib import Path
import sys

def main():
    parser = argparse.ArgumentParser(description="Extract VAT numbers from Rikið data")
    parser.add_argument("--input", required=True, help="Input parquet file")
    parser.add_argument("--output", required=True, help="Output parquet file with VAT lookup table")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading from {input_path}")
    con = duckdb.connect(":memory:")

    # Read the parquet file with computed year column
    con.execute(f"""
        CREATE TABLE data AS
        SELECT *, YEAR("Dags.greiðslu") AS year
        FROM read_parquet('{input_path}')
    """)

    # Create VAT lookup table: seller name -> VAT number (currently empty, for external enrichment)
    con.execute("""
        CREATE TABLE vat_lookup AS
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM("Birgi"), ''), '(óskráð)') AS seller_name,
            NULL AS vm_numer,
            NULL AS vsk_link
        FROM data
        ORDER BY seller_name
    """)

    sellers_total = con.execute("SELECT COUNT(*) FROM vat_lookup").fetchone()[0]
    sellers_with_vat = con.execute("SELECT COUNT(*) FROM vat_lookup WHERE vm_numer IS NOT NULL").fetchone()[0]

    print(f"Found {sellers_with_vat}/{sellers_total} sellers with VAT numbers")

    # Write the VAT lookup table
    con.execute(f"""
        COPY vat_lookup TO '{output_path}' (FORMAT PARQUET)
    """)

    print(f"Wrote VAT lookup table to {output_path}")
    print(f"Note: VAT numbers can be enriched from external sources using the vm_numer column")

if __name__ == "__main__":
    main()
