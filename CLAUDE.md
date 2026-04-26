# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

### Development
```bash
make dev                    # Run Flask dev server on :5002
make install               # Set up .venv and install dependencies
```

### Data Pipeline
```bash
# Incremental update: downloads new data since last known date
make pipeline

# Full refresh: deletes existing data and redownloads everything
make pipeline RESET=1

# Individual sources
make rikid-pipeline        # Update Rikið (requires API or manual download)
make reykjavik-pipeline    # Update Reykjavík from CKAN API
make anomalies            # Rebuild anomaly detection for both sources
```

### Data Sources

**Reykjavík** (✓ Fully Automated):
- Source: `https://gagnagatt.reykjavik.is/dataset/arsuppgjor` (CKAN API)
- Downloads CSV files for 2014-2025+ automatically
- Script: `scripts/download_reykjavik.py`
- Status: Ready to use with `make reykjavik-pipeline`

**Rikið** (Requires Manual Setup):
- API: `https://opnirreikningar.is/rest/csvExport`
- Script available: `scripts/download_rikid.py`
- Note: API currently returns empty results—may be temporary issue or require authentication

**Options for Rikið data:**

1. **Manual Download** (Most Reliable):
   ```bash
   # Visit https://opnirreikningar.is/ and:
   # 1. Set date range (e.g., 2017-01-01 to 2024-12-31)
   # 2. Click "Flytja út" (Export) → Save Excel file
   # 3. Convert to Parquet:
   python3 -c "
   import pandas as pd
   df = pd.read_excel('opnirreikningar.xlsx')
   df.to_parquet('data/rikid/parquet/opnirreikningar.parquet', index=False)
   "
   ```

2. **Use API Script** (If endpoint returns data):
   ```bash
   make rikid-pipeline FROM=2017-01-01 TO=2024-12-31
   # Or for a specific date range
   .venv/bin/python3 scripts/download_rikid.py --from 2024-01-01 --to 2024-12-31
   ```

3. **Check Original Repo**:
   - Original implementation: https://github.com/bjornlevi/rikid-opnir-reikningar
   - May have pre-downloaded Parquet files or alternative approaches

### Production
```bash
gunicorn wsgi:app          # WSGI entry point for production deployment
```

## Architecture Overview

**Opin Gogn** combines procurement data from two Icelandic sources:

1. **Rikið** (Central Government) — `data/rikid/parquet/opnirreikningar_with_corrections.parquet`
   - No native `year` column; computed from "Dags.greiðslu" (payment date)
   - Columns: Kaupandi (buyer), Birgi (seller), Tegund (type), Upphæð línu (amount), Dags.greiðslu, Númer reiknings
   - Anomalies: `anomalies_flagged.parquet`, `anomalies_yearly_all.parquet`

2. **Reykjavík** (Municipality) — `data/reykjavik/processed/arsuppgjor_combined_with_corrections.parquet`
   - Has native `year` column
   - Columns include: `raun` (amount in Icelandic number format, `.` for thousands), hierarchical org codes (`samtala0-3`) and type codes (`tegund0-3`)
   - Anomalies: `anomalies_flagged.parquet`, `anomalies_yoy_all.parquet`

### Data Access Pattern

All database access is in-memory DuckDB:

- `open_rikid_con()` — Connect to rikid data with computed `year` column
- `open_con()` — Connect to any parquet file (defaults to view name "data")
- `build_where()` — Construct parameterized WHERE clauses from filter pairs, skipping "all" values

Example:
```python
con = open_rikid_con(RIKID_DATA)
where, params = build_where([("year", 2024), ("Tegund", "type")])
result = con.execute(f"SELECT ... FROM data {where}", params).fetchall()
```

### Route Structure

All routes are in `app.py`. Each source has parallel endpoints:

**Rikið:**
- `/rikid/` — Explorer with pagination, filters, yearly/type/buyer breakdowns
- `/rikid/analysis` — Interactive tables with sums by category
- `/rikid/anomalies` — Flagged anomalies with details
- `/rikid/reports` — Static annual reports

**Reykjavík:**
- `/reykjavik/` — Explorer (same pattern as rikid)
- `/reykjavik/analysis` — Interactive tables
- `/reykjavik/anomalies` — Anomalies (YoY format)
- `/reykjavik/reports` — Static reports

**Home:**
- `/` — Overview with yearly charts and headline stats for both sources

All routes support filtering via query parameters (e.g., `?year=2024&buyer=X&seller=Y`). The `build_where()` function filters out "all" values automatically.

## Key Patterns

### Number Formatting

Icelandic convention: dot for thousands, no decimals.

```python
fmt(value)        # Float → "1.234.567"
fmt_pct(value)    # Float → "+1.5%"
```

### Column Display Names

Two mapping dicts for user-facing column names:

- `RIKID_DISPLAY` — Maps internal rikid columns to display labels
- `RKV_DISPLAY` — Maps reykjavik columns to display labels

### Amount Expressions

Amount extraction varies per source:

- **Rikið**: Direct `"Upphæð línu"` column
- **Reykjavík**: `TRY_CAST(REPLACE(REPLACE(raun, '.', ''), ',', '.') AS DOUBLE)` (handles Icelandic number format)

### Corrections & Anomalies

Both sources have correction-detection scripts that flag reversals and add an `is_correction` column:

- `scripts/detect_corrections.py` — Rikið corrections
- `scripts/detect_corrections_reykjavik.py` — Reykjavík corrections

Routes can show/hide corrections via `show_corrections` query parameter.

## Template Structure

Templates in `templates/`:

- **base.html** — Master template with navigation, CSS, Jinja globals (fmt, fmt_pct)
- **home.html** — Landing page with yearly charts, headline stats
- **explorer.html** — Data table with pagination, filters, breakdowns
- **analysis.html** — Interactive category-sum tables
- **anomalies.html** — Flagged anomalies with drill-down
- **reports.html** — Static annual reports

**Color scheme:**
- Rikið: Blue (#2563eb)
- Reykjavík: Teal (#0d9488)

## Configuration

Environment variables override default data paths:

- `RIKID_PARQUET` — Rikið main data (default: `data/rikid/parquet/opnirreikningar_with_corrections.parquet`)
- `RIKID_ANOMALIES` — Rikið flagged anomalies
- `RIKID_ANOMALIES_ALL` — Rikið yearly anomalies
- `REYKJAVIK_PARQUET` — Reykjavík main data
- `REYKJAVIK_ANOMALIES` — Reykjavík flagged anomalies
- `REYKJAVIK_ANOMALIES_ALL` — Reykjavík YoY anomalies
- `PREFIX` — URL prefix for reverse proxy deployments (e.g., `/app/`)

## Dependencies

- **flask** >= 3.0.3 — Web framework
- **duckdb** >= 1.0.0 — In-memory SQL database
- **gunicorn** — WSGI server

Jinja2 is included with Flask; Chart.js is loaded from CDN.

## Common Development Tasks

**Restart the dev server:** Changes to `app.py` auto-reload; restart for dependency changes.

**Add a new route:**
1. Define route in `app.py` within `create_app()`
2. Query data using `open_rikid_con()` or `open_con()`
3. Use `build_where()` to construct filter clauses
4. Render a template, passing data and context

**Modify a template:** Changes reflect instantly in dev mode; check `base.html` for shared CSS/globals.

**Rebuild pipeline data:** Run `make pipeline` or specific source pipelines. Auto-detect respects the latest rikid date; reykjavik is always fully refreshed.

**Check pipeline date range:** `scripts/rikid_max_date.py` returns the first day of the latest month in rikid data.
