#!/usr/bin/env python3
"""
Detect year-over-year anomalies in Reykjavík data.

An anomaly is detected when spending by a category changes significantly
between consecutive years (e.g., > 20% or > 50M amount change).

Outputs two Parquet files:
- anomalies_yoy_all.parquet: All YoY changes
- anomalies_flagged.parquet: Only significant changes (threshold-based)
"""
import argparse
import duckdb
from pathlib import Path


def detect_anomalies(input_file: Path, output_all: Path, output_flagged: Path,
                    min_pct_change: float = 20.0, min_amount_change: float = 50e6):
    """
    Detect YoY anomalies in Reykjavík data.

    An anomaly is flagged when:
    - YoY percentage change >= min_pct_change (default 20%)
    - OR absolute amount change >= min_amount_change (default 50M)
    """
    con = duckdb.connect(":memory:")

    # Read data
    con.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{input_file}')")

    # Build aggregated data grouped by year and key categories
    con.execute("""
    CREATE TABLE yearly_agg AS
    SELECT
        year,
        samtala0,
        samtala1,
        samtala2_canonical,
        samtala3,
        samtala4,
        tegund0,
        tegund1,
        tegund2,
        tegund3,
        tegund4,
        fyrirtaeki,
        vm_numer,
        vm_nafn,
        SUM(raun) AS amount
    FROM data
    WHERE (is_correction = FALSE OR is_correction IS NULL)
      AND year IS NOT NULL
    GROUP BY year, samtala0, samtala1, samtala2_canonical, samtala3, samtala4,
             tegund0, tegund1, tegund2, tegund3, tegund4, fyrirtaeki, vm_numer, vm_nafn
    """)

    # Create YoY comparisons
    con.execute("""
    CREATE TABLE yoy_data AS
    WITH prior AS (
        SELECT
            year + 1 AS next_year,
            samtala0, samtala1, samtala2_canonical, samtala3, samtala4,
            tegund0, tegund1, tegund2, tegund3, tegund4,
            fyrirtaeki, vm_numer, vm_nafn,
            amount AS prior_amount
        FROM yearly_agg
    )
    SELECT
        y.year,
        y.samtala0, y.samtala1, y.samtala2_canonical, y.samtala3, y.samtala4,
        y.tegund0, y.tegund1, y.tegund2, y.tegund3, y.tegund4,
        y.fyrirtaeki, y.vm_numer, y.vm_nafn,
        y.amount AS actual_real,
        COALESCE(p.prior_amount, 0) AS prior_real,
        (y.amount - COALESCE(p.prior_amount, 0)) AS yoy_real_change,
        CASE
            WHEN COALESCE(p.prior_amount, 0) = 0 THEN NULL
            ELSE ((y.amount - COALESCE(p.prior_amount, 0)) / ABS(COALESCE(p.prior_amount, 0))) * 100
        END AS yoy_real_pct
    FROM yearly_agg y
    LEFT JOIN prior p ON
        y.year = p.next_year AND
        y.samtala0 IS NOT DISTINCT FROM p.samtala0 AND
        y.samtala1 IS NOT DISTINCT FROM p.samtala1 AND
        y.samtala2_canonical IS NOT DISTINCT FROM p.samtala2_canonical AND
        y.samtala3 IS NOT DISTINCT FROM p.samtala3 AND
        y.samtala4 IS NOT DISTINCT FROM p.samtala4 AND
        y.tegund0 IS NOT DISTINCT FROM p.tegund0 AND
        y.tegund1 IS NOT DISTINCT FROM p.tegund1 AND
        y.tegund2 IS NOT DISTINCT FROM p.tegund2 AND
        y.tegund3 IS NOT DISTINCT FROM p.tegund3 AND
        y.tegund4 IS NOT DISTINCT FROM p.tegund4 AND
        y.fyrirtaeki IS NOT DISTINCT FROM p.fyrirtaeki AND
        y.vm_numer IS NOT DISTINCT FROM p.vm_numer AND
        y.vm_nafn IS NOT DISTINCT FROM p.vm_nafn
    ORDER BY y.year DESC, ABS(yoy_real_change) DESC NULLS LAST
    """)

    # Write all YoY data
    con.execute(f"""
    COPY yoy_data
    TO '{output_all}' (FORMAT PARQUET)
    """)
    print(f"Wrote all YoY data: {output_all}")

    # Write flagged anomalies (significant changes)
    con.execute(f"""
    COPY (
        SELECT * FROM yoy_data
        WHERE ABS(yoy_real_change) >= {min_amount_change}
           OR ABS(COALESCE(yoy_real_pct, 0)) >= {min_pct_change}
    )
    TO '{output_flagged}' (FORMAT PARQUET)
    """)
    print(f"Wrote flagged anomalies: {output_flagged}")

    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect YoY anomalies in Reykjavík data")
    parser.add_argument("--input", required=True, type=Path, help="Input parquet file")
    parser.add_argument("--output-all", required=True, type=Path, help="Output all YoY changes")
    parser.add_argument("--output-flagged", required=True, type=Path, help="Output flagged anomalies")
    parser.add_argument("--min-pct", type=float, default=20.0, help="Min % change to flag (default 20)")
    parser.add_argument("--min-amount", type=float, default=50e6, help="Min amount change to flag (default 50M)")

    args = parser.parse_args()
    detect_anomalies(args.input, args.output_all, args.output_flagged, args.min_pct, args.min_amount)
