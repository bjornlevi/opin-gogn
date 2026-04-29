#!/usr/bin/env python3
"""
Extract and create a VAT number lookup table from Reykjavík data.

This script creates a separate parquet file with supplier -> VAT number mappings
and fyrirtækjaskrá links. This allows the app to look up and display links
without modifying the main data.

Usage:
    python3 enrich_vat_numbers.py --input <path> --output <path>
"""
import argparse
import duckdb
from pathlib import Path
import sys

def main():
    parser = argparse.ArgumentParser(description="Extract VAT numbers from Reykjavík data")
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

    # Read the parquet file
    con.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{input_path}')")

    # Create VAT lookup table: supplier name -> VAT number + link
    con.execute("""
        CREATE TABLE vat_lookup AS
        SELECT DISTINCT
            COALESCE(NULLIF(TRIM(vm_nafn), ''),
                     NULLIF(TRIM(fyrirtaeki), ''),
                     NULLIF(TRIM(CAST(vm_numer AS VARCHAR)), '')) AS supplier_name,
            vm_numer,
            CASE
                WHEN vm_numer IS NOT NULL AND vm_numer > 0
                THEN 'https://www.skatturinn.is/fyrirtaekjaskra/leit/vsk-numer/' || CAST(vm_numer AS VARCHAR)
                ELSE NULL
            END AS vsk_link
        FROM data
        WHERE vm_numer IS NOT NULL
        ORDER BY supplier_name
    """)

    suppliers_with_vat = con.execute("SELECT COUNT(*) FROM vat_lookup").fetchone()[0]
    total_suppliers = con.execute("""
        SELECT COUNT(DISTINCT
            COALESCE(NULLIF(TRIM(vm_nafn), ''),
                     NULLIF(TRIM(fyrirtaeki), ''),
                     NULLIF(TRIM(CAST(vm_numer AS VARCHAR)), '')))
        FROM data
    """).fetchone()[0]

    print(f"Found {suppliers_with_vat}/{total_suppliers} suppliers with VAT numbers")

    # Write the VAT lookup table
    con.execute(f"""
        COPY vat_lookup TO '{output_path}' (FORMAT PARQUET)
    """)

    print(f"Wrote VAT lookup table to {output_path}")

if __name__ == "__main__":
    main()
