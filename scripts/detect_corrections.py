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


def detect_corrections(input_file: Path, output_file: Path, threshold: float = 1e9,
                       memory_limit: str | None = None, temp_dir: str | None = None):
    """
    Detect correction pairs where transactions cancel each other out.

    A correction is identified when:
    - Same buyer, vendor, type within a 2-day window
    - Positive and negative amounts that are nearly equal
    - Amounts exceed threshold (likely errors)

    The input is never materialised as a table: the pair search reads only the
    handful of rows above the threshold, and the output is written by streaming
    the parquet file straight through a single COPY. This keeps peak memory flat
    regardless of input size.
    """
    con = duckdb.connect(":memory:")
    if memory_limit:
        con.execute(f"SET memory_limit='{memory_limit}'")
    if temp_dir:
        con.execute(f"SET temp_directory='{temp_dir}'")
    # We do not care about row order in the output, and not preserving it lets
    # DuckDB stream the COPY below instead of buffering the whole result.
    con.execute("SET preserve_insertion_order=false")

    input_path = str(input_file.resolve())
    output_path = str(output_file.resolve())

    # Only rows near the threshold can take part in a pair: |a| > threshold and
    # ||a| - |b|| < 1000 together imply |b| > threshold - 1000. Filtering both
    # sides up front turns a scan of the whole table into a scan of a few rows.
    con.execute(
        """
    CREATE TEMP TABLE candidates AS
    SELECT
        "Númer reiknings" AS invoice,
        "Kaupandi" AS kaupandi,
        "Birgi" AS birgi,
        "Tegund" AS tegund,
        CAST("Dags.greiðslu" AS DATE) AS dags,
        CAST("Upphæð línu" AS DOUBLE) AS amount
    FROM read_parquet(?)
    WHERE ABS(CAST("Upphæð línu" AS DOUBLE)) > ?
    """,
        [input_path, threshold - 1000],
    )

    con.execute(
        """
    CREATE TEMP TABLE corrections AS
    SELECT DISTINCT a.invoice
    FROM candidates a
    JOIN candidates b ON
        a.kaupandi = b.kaupandi AND
        a.birgi = b.birgi AND
        a.tegund = b.tegund AND
        a.dags <= b.dags AND
        DATEDIFF('day', a.dags, b.dags) <= 2 AND
        (a.amount > 0) != (b.amount > 0)  -- opposite signs
    WHERE
        ABS(a.amount) > ? AND
        ABS(ABS(a.amount) - ABS(b.amount)) < 1000  -- nearly equal
    """,
        [threshold],
    )

    n_corrections = con.execute("SELECT COUNT(*) FROM corrections").fetchone()[0]
    print(f"Found {n_corrections} correction transactions")

    # Stream input -> output in one pass, adding the flag as the last column.
    con.execute(
        f"""
    COPY (
        SELECT
            d.*,
            -- COALESCE: a NULL invoice number makes IN return NULL, but the
            -- flag must stay boolean-valued for downstream filters.
            COALESCE("Númer reiknings" IN (SELECT invoice FROM corrections), FALSE)
                AS is_correction
        FROM read_parquet('{input_path}') d
    ) TO '{output_path}' (FORMAT PARQUET)
    """
    )

    print(f"Saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect correction transactions")
    parser.add_argument("--input", required=True, type=Path, help="Input parquet file")
    parser.add_argument("--output", required=True, type=Path, help="Output parquet file")
    parser.add_argument("--threshold", type=float, default=1e9, help="Amount threshold for flagging")
    parser.add_argument("--memory-limit", default=None,
                        help="DuckDB memory limit, e.g. 1GB (default: DuckDB's own)")
    parser.add_argument("--temp-dir", default=None,
                        help="Directory for DuckDB spill files")

    args = parser.parse_args()
    detect_corrections(args.input, args.output, args.threshold,
                       args.memory_limit, args.temp_dir)
