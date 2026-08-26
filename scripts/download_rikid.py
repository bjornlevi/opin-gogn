#!/usr/bin/env python3
"""Download Rikið (government spending) data from opnirreikningar.is CSV export API.

Downloads monthly chunks to handle the 500k record limit, converts to Parquet.
Based on: https://github.com/bjornlevi/rikid-opnir-reikningar/blob/master/scripts/pipeline.py
"""
import sys
import argparse
import calendar
import datetime as dt
import zipfile
from pathlib import Path
from datetime import timedelta

try:
    import duckdb
    import requests
    import pandas as pd
except ImportError as e:
    print(f"Error: Required package not installed: {e}", file=sys.stderr)
    print("Install with: pip install duckdb requests pandas openpyxl", file=sys.stderr)
    sys.exit(1)


DEFAULT_EXPORT_URL = "https://opnirreikningar.is/rest/csvExport"


def parse_date(value: str) -> dt.date:
    """Parse YYYY-MM-DD format date."""
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', use YYYY-MM-DD") from exc


def month_chunks(start: dt.date, end: dt.date):
    """Yield (start_date, end_date) tuples for each month in range."""
    if start > end:
        raise ValueError("--from must be <= --to")
    cur = start
    while cur <= end:
        # Last day of the month
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        chunk_end = dt.date(cur.year, cur.month, last_day)
        if chunk_end > end:
            chunk_end = end
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def fmt_export_date(value: dt.date) -> str:
    """Format date as DD.MM.YYYY for API."""
    return value.strftime("%d.%m.%Y")


def sql_str(value: str) -> str:
    """Escape string for SQL."""
    return "'" + value.replace("'", "''") + "'"


def guess_format(content_type: str, sniff: bytes) -> str:
    """Guess file format from content-type and file signature."""
    sniff = sniff.lstrip()
    if sniff.startswith(b"PK"):
        return "zip"
    if sniff.startswith(b"{") or sniff.startswith(b"["):
        return "json"
    ct = (content_type or "").lower()
    if "zip" in ct:
        return "zip"
    if "json" in ct:
        return "json"
    if "csv" in ct or "excel" in ct or "text" in ct:
        return "csv"
    return "csv"


def download_chunk(
    export_url: str,
    params: dict,
    chunk_label: str,
) -> bytes | None:
    """Download a single month chunk from the API."""
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/zip, text/csv, */*",
        "Referer": "https://www.opnirreikningar.is/",
    }
    try:
        print(f"  Downloading {chunk_label}...", end="", flush=True)
        resp = requests.get(export_url, params=params, headers=headers, timeout=120)
        resp.raise_for_status()
        print(f" {len(resp.content)} bytes")
        return resp.content
    except requests.RequestException as e:
        print(f" ERROR: {e}", file=sys.stderr)
        return None


def extract_from_zip(content: bytes) -> bytes | None:
    """Extract first file from ZIP archive."""
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            names = zf.namelist()
            if not names:
                return None
            # Look for .xlsx files first
            for name in names:
                if name.lower().endswith(".xlsx") or name.lower().endswith(".xls"):
                    return zf.read(name)
            # Fall back to first file
            return zf.read(names[0])
    except Exception as e:
        print(f"    ZIP extraction error: {e}", file=sys.stderr)
        return None


def convert_to_parquet(content: bytes, output_path: Path, chunk_label: str) -> bool:
    """Convert downloaded content to Parquet using DuckDB."""
    if not content or len(content) < 100:
        print(f"    Skipping empty chunk")
        return False

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Detect format
    sniff = content[:2048]
    file_format = guess_format("", sniff)

    # For ZIP (OOXML Excel files), treat as XLSX directly
    # Don't try to extract individual files
    if file_format == "zip":
        file_format = "xlsx"

    # Write temp file for pandas to read
    temp_path = output_path.with_suffix(".tmp")
    if file_format == "xlsx":
        temp_path = temp_path.with_suffix(".xlsx")
    temp_path.write_bytes(content)

    try:
        df = None

        # Try CSV first
        try:
            df = pd.read_csv(temp_path)
        except Exception as e_csv:
            # Try XLSX with pandas
            try:
                df = pd.read_excel(temp_path)
            except Exception as e_xlsx:
                print(f"    Failed CSV: {str(e_csv)[:50]}", file=sys.stderr)
                print(f"    Failed XLSX: {str(e_xlsx)[:50]}", file=sys.stderr)
                return False

        if df is None or len(df) == 0:
            print(f"    No data rows found")
            return False

        # Convert to Parquet using pandas
        df.to_parquet(output_path, index=False, compression="snappy")
        print(f"    Converted: {len(df)} rows → {output_path.name}")
        return True
    except Exception as e:
        print(f"    Conversion error: {e}", file=sys.stderr)
        return False
    finally:
        temp_path.unlink(missing_ok=True)


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def combine_parquets(existing_file: Path | None, chunk_files: list[Path],
                     output_file: Path) -> None:
    """Merge freshly downloaded chunks into the existing parquet file.

    Rows are deduplicated by *source date range*, not by row content: existing
    rows whose payment date falls inside the newly downloaded range are dropped
    and replaced wholesale by the fresh chunks.

    Do NOT use `SELECT DISTINCT *` here. The source legitimately contains
    repeated identical invoice lines — one Vegagerðin/Síminn invoice carries the
    same 430 kr. line 387 times — so content dedup silently deletes ~11% of all
    rows. It also collapses the handful of ±232-trillion outlier rows
    asymmetrically, which corrupts whole-year totals.
    """
    if not chunk_files:
        raise RuntimeError("No parquet files to combine")

    n_inputs = len(chunk_files) + (1 if existing_file and existing_file.exists() else 0)
    print(f"Combining {n_inputs} parquet inputs...")

    chunk_union = " UNION ALL ".join(
        f"SELECT * FROM read_parquet('{sql_path(path)}')" for path in chunk_files
    )

    con = duckdb.connect(":memory:")
    # Streaming append; nothing here needs a global sort or hash aggregation.
    con.execute("SET preserve_insertion_order=false")
    tmp_output = output_file.with_suffix(".tmp.parquet")
    try:
        con.execute(f"CREATE TEMP VIEW new_rows AS {chunk_union}")
        lo, hi = con.execute(
            'SELECT MIN(CAST("Dags.greiðslu" AS DATE)), '
            '       MAX(CAST("Dags.greiðslu" AS DATE)) FROM new_rows'
        ).fetchone()

        if existing_file and existing_file.exists() and lo is not None:
            print(f"  Replacing existing rows dated {lo} .. {hi}")
            combined_sql = (
                f"SELECT * FROM read_parquet('{sql_path(existing_file)}') "
                f"WHERE \"Dags.greiðslu\" IS NULL "
                f"   OR CAST(\"Dags.greiðslu\" AS DATE) NOT BETWEEN DATE '{lo}' AND DATE '{hi}' "
                f"UNION ALL SELECT * FROM new_rows"
            )
        else:
            combined_sql = "SELECT * FROM new_rows"

        con.execute(
            f"COPY ({combined_sql}) TO '{sql_path(tmp_output)}' (FORMAT PARQUET)"
        )
        tmp_output.replace(output_file)
        print(f"Combined to: {output_file}")
    finally:
        con.close()
        tmp_output.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description="Download Rikið data from opnirreikningar.is in monthly chunks"
    )
    parser.add_argument("--from", dest="from_date", type=parse_date, required=True,
                       help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=parse_date, required=True,
                       help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", dest="output_file", default="opnirreikningar.parquet",
                       help="Output Parquet file")
    parser.add_argument("--export-url", default=DEFAULT_EXPORT_URL,
                       help="opnirreikningar.is export API URL")
    parser.add_argument("--org-id", default="", help="Organization ID filter")
    parser.add_argument("--vendor-id", default="", help="Vendor ID filter")
    parser.add_argument("--type-id", default="", help="Type ID filter")

    args = parser.parse_args()

    output_file = Path(args.output_file)
    chunks_dir = output_file.parent / "chunks"

    print(f"Downloading Rikið data from {args.from_date} to {args.to_date}")

    chunk_files = []
    total_rows = 0

    for start_date, end_date in month_chunks(args.from_date, args.to_date):
        chunk_label = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

        params = {
            "vendor_id": args.vendor_id or "",
            "type_id": args.type_id or "",
            "org_id": args.org_id or "",
            "timabil_fra": fmt_export_date(start_date),
            "timabil_til": fmt_export_date(end_date),
        }

        content = download_chunk(args.export_url, params, chunk_label)
        if not content:
            print(f"  Skipping chunk {chunk_label}")
            continue

        chunk_parquet = chunks_dir / f"opnirreikningar_{chunk_label}.parquet"
        if convert_to_parquet(content, chunk_parquet, chunk_label):
            chunk_files.append(chunk_parquet)
            try:
                con = duckdb.connect(":memory:")
                count = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{chunk_parquet}')"
                ).fetchone()[0]
                total_rows += count
                con.close()
            except:
                pass

    if not chunk_files:
        print("No data downloaded", file=sys.stderr)
        sys.exit(1)

    combine_parquets(output_file if output_file.exists() else None,
                     chunk_files, output_file)

    print(f"✓ Total: {total_rows} rows in {output_file}")


if __name__ == "__main__":
    main()
