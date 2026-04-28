#!/usr/bin/env python3
"""
Detect correction transaction pairs in Rikið data.

A correction pair is when a large transaction is followed by its negative offset,
often with a small final corrected amount. These are marked so they can be filtered.

Usage:
  python3 detect_corrections.py \
    --input data/rikid/parquet/opnirreikningar.parquet \
    --output data/rikid/parquet/opnirreikningar_corrected.parquet
"""
import argparse
import duckdb
from pathlib import Path


def detect_corrections(input_file: Path, output_file: Path, threshold: float = 1e9):
    """
    Detect correction pairs where transactions cancel each other out.

    A correction is identified when:
    - Same buyer, vendor, type within a 2-day window
    - Positive and negative amounts that are nearly equal
    - Amounts exceed threshold (likely errors)
    """
    con = duckdb.connect(":memory:")

    # Read data using absolute path
    input_path = str(input_file.resolve())
    output_path = str(output_file.resolve())

    con.execute(f"CREATE TABLE data_raw AS SELECT * FROM read_parquet('{input_path}')")

    # Add is_correction column
    con.execute("""
    CREATE TABLE data AS
    SELECT
        *,
        CAST(FALSE AS BOOLEAN) AS is_correction
    FROM data_raw
    """)

    # Find correction pairs: same buyer/vendor/type, opposite signs, similar amounts
    # Use CAST to DOUBLE early to avoid INT64 overflow
    con.execute("""
    WITH pairs AS (
        SELECT
            a."Númer reiknings" as a_id,
            b."Númer reiknings" as b_id,
            CAST(a."Upphæð línu" AS DOUBLE) as amount_a,
            CAST(b."Upphæð línu" AS DOUBLE) as amount_b
        FROM data a
        JOIN data b ON
            a."Kaupandi" = b."Kaupandi" AND
            a."Birgi" = b."Birgi" AND
            a."Tegund" = b."Tegund" AND
            CAST(a."Dags.greiðslu" AS DATE) <= CAST(b."Dags.greiðslu" AS DATE) AND
            DATEDIFF('day', CAST(a."Dags.greiðslu" AS DATE), CAST(b."Dags.greiðslu" AS DATE)) <= 2 AND
            (CAST(a."Upphæð línu" AS DOUBLE) > 0) != (CAST(b."Upphæð línu" AS DOUBLE) > 0)  -- opposite signs
        WHERE
            ABS(CAST(a."Upphæð línu" AS DOUBLE)) > ? AND
            ABS(ABS(CAST(a."Upphæð línu" AS DOUBLE)) - ABS(CAST(b."Upphæð línu" AS DOUBLE))) < 1000  -- nearly equal
    )
    SELECT DISTINCT a_id FROM pairs
    """, [threshold])

    correction_ids = set(row[0] for row in con.fetchall())

    print(f"Found {len(correction_ids)} correction transactions")

    if correction_ids:
        # Mark corrections
        id_list = ','.join([f"'{id}'" for id in correction_ids])
        con.execute(f"""
        UPDATE data
        SET is_correction = TRUE
        WHERE "Númer reiknings" IN ({id_list})
        """)

    # Export using DuckDB's native parquet write
    con.execute(f'CREATE TABLE export_data AS SELECT * FROM data')
    con.execute(f'COPY export_data TO "{output_path}" (FORMAT PARQUET)')

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect correction transactions")
    parser.add_argument("--input", required=True, type=Path, help="Input parquet file")
    parser.add_argument("--output", required=True, type=Path, help="Output parquet file")
    parser.add_argument("--threshold", type=float, default=1e9, help="Amount threshold for flagging")

    args = parser.parse_args()
    detect_corrections(args.input, args.output, args.threshold)
