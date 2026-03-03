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

    # Read data
    con.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{input_file}')")

    # Create a view with window functions to detect potential corrections
    con.execute("""
    CREATE TABLE data_with_flags AS
    SELECT
        *,
        FALSE AS is_correction
    FROM data
    """)

    # Find correction pairs: same buyer/vendor/type, opposite signs, similar amounts
    con.execute("""
    WITH pairs AS (
        SELECT
            a."Númer reiknings" as a_id,
            b."Númer reiknings" as b_id,
            a."Kaupandi" as buyer,
            a."Birgi" as vendor,
            a."Tegund" as type,
            a."Upphæð línu" as amount_a,
            b."Upphæð línu" as amount_b,
            ABS(CAST(a."Upphæð línu" AS DOUBLE)) as abs_a,
            ABS(CAST(b."Upphæð línu" AS DOUBLE)) as abs_b,
            DATEDIFF('day', CAST(a."Dags.greiðslu" AS DATE), CAST(b."Dags.greiðslu" AS DATE)) as days_apart
        FROM data a
        JOIN data b ON
            a."Kaupandi" = b."Kaupandi" AND
            a."Birgi" = b."Birgi" AND
            a."Tegund" = b."Tegund" AND
            CAST(a."Dags.greiðslu" AS DATE) <= CAST(b."Dags.greiðslu" AS DATE) AND
            DATEDIFF('day', CAST(a."Dags.greiðslu" AS DATE), CAST(b."Dags.greiðslu" AS DATE)) <= 2 AND
            a."Upphæð línu" * b."Upphæð línu" < 0  -- opposite signs
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
        id_list = ','.join(f"'{id}'" for id in correction_ids)
        con.execute(f"""
        UPDATE data_with_flags
        SET is_correction = TRUE
        WHERE "Númer reiknings" IN ({id_list})
        """)

    # Export to parquet
    con.execute(f"""
    COPY data_with_flags
    TO '{output_file}' (FORMAT PARQUET)
    """)

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect correction transactions")
    parser.add_argument("--input", required=True, type=Path, help="Input parquet file")
    parser.add_argument("--output", required=True, type=Path, help="Output parquet file")
    parser.add_argument("--threshold", type=float, default=1e9, help="Amount threshold for flagging")

    args = parser.parse_args()
    detect_corrections(args.input, args.output, args.threshold)
