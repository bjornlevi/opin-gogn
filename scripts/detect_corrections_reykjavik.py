#!/usr/bin/env python3
"""
Detect correction transaction pairs in Reykjavík data.

A correction pair is when a large transaction is followed by its negative offset,
often with a small final corrected amount. These are marked so they can be filtered.

Usage:
  python3 detect_corrections_reykjavik.py \
    --input data/reykjavik/processed/arsuppgjor_combined.parquet \
    --output data/reykjavik/processed/arsuppgjor_combined_with_corrections.parquet
"""
import argparse
import duckdb
from pathlib import Path


def detect_corrections(input_file: Path, output_file: Path, threshold: float = 1e9):
    """
    Detect correction pairs where transactions cancel each other out in Reykjavík data.

    A correction is identified when:
    - Same year, tegund hierarchy, samtala hierarchy, fyrirtaeki
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

    # Convert raun (Icelandic format) to double for comparison
    con.execute("""
    ALTER TABLE data_with_flags
    ADD COLUMN raun_numeric DOUBLE
    """)

    con.execute("""
    UPDATE data_with_flags
    SET raun_numeric = TRY_CAST(REPLACE(REPLACE(raun, '.', ''), ',', '.') AS DOUBLE)
    """)

    # Find correction pairs: same year/tegund/samtala/fyrirtaeki, opposite signs, similar amounts
    con.execute("""
    WITH pairs AS (
        SELECT
            a.rowid as a_id,
            b.rowid as b_id,
            a.year as year,
            a.tegund0, a.tegund1, a.tegund2, a.tegund3, a.tegund4,
            a.samtala0, a.samtala1, a.samtala2, a.samtala3, a.samtala4,
            a.fyrirtaeki,
            a.raun_numeric as amount_a,
            b.raun_numeric as amount_b,
            ABS(a.raun_numeric) as abs_a,
            ABS(b.raun_numeric) as abs_b
        FROM data_with_flags a
        JOIN data_with_flags b ON
            a.year = b.year AND
            COALESCE(a.tegund0, '') = COALESCE(b.tegund0, '') AND
            COALESCE(a.tegund1, '') = COALESCE(b.tegund1, '') AND
            COALESCE(a.tegund2, '') = COALESCE(b.tegund2, '') AND
            COALESCE(a.tegund3, '') = COALESCE(b.tegund3, '') AND
            COALESCE(a.tegund4, '') = COALESCE(b.tegund4, '') AND
            COALESCE(a.samtala0, '') = COALESCE(b.samtala0, '') AND
            COALESCE(a.samtala1, '') = COALESCE(b.samtala1, '') AND
            COALESCE(a.samtala2, '') = COALESCE(b.samtala2, '') AND
            COALESCE(a.samtala3, '') = COALESCE(b.samtala3, '') AND
            COALESCE(a.samtala4, '') = COALESCE(b.samtala4, '') AND
            COALESCE(a.fyrirtaeki, '') = COALESCE(b.fyrirtaeki, '') AND
            a.raun_numeric * b.raun_numeric < 0  -- opposite signs
        WHERE
            ABS(a.raun_numeric) > ? AND
            ABS(ABS(a.raun_numeric) - ABS(b.raun_numeric)) < 1000  -- nearly equal
    )
    SELECT DISTINCT a_id FROM pairs
    """, [threshold])

    correction_ids = [row[0] for row in con.fetchall()]
    print(f"Found {len(correction_ids)} correction transactions")

    if correction_ids:
        # Mark corrections using rowid
        for row_id in correction_ids:
            con.execute("""
            UPDATE data_with_flags
            SET is_correction = TRUE
            WHERE rowid = ?
            """, [row_id])

    # Remove temporary numeric column and export
    con.execute("""
    ALTER TABLE data_with_flags
    DROP COLUMN raun_numeric
    """)

    con.execute(f"""
    COPY data_with_flags
    TO '{output_file}' (FORMAT PARQUET)
    """)

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect correction transactions in Reykjavík data")
    parser.add_argument("--input", required=True, type=Path, help="Input parquet file")
    parser.add_argument("--output", required=True, type=Path, help="Output parquet file")
    parser.add_argument("--threshold", type=float, default=1e9, help="Amount threshold for flagging")

    args = parser.parse_args()
    detect_corrections(args.input, args.output, args.threshold)
