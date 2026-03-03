# Opin Gogn (Open Accounts)

A unified Flask application exposing open accounts data from Icelandic government institutions under a single web interface.

## Overview

Opin Gogn combines and visualizes procurement data from two sources:
- **Rikið** (Government/State): Central government open accounts data
- **Reykjavík** (City): Reykjavík municipality open accounts data

The application provides interactive exploration, analysis, and anomaly detection across both datasets.

## Tech Stack

- **Backend**: Python 3 + Flask
- **Database**: DuckDB (in-memory)
- **Data Format**: Parquet files (columnar storage)
- **Frontend**: Jinja2 templates + Chart.js (CDN)
- **Build**: Make

## Project Structure

```
opin_gogn/
├── app.py                 # Main Flask application
├── wsgi.py               # WSGI entry point
├── Makefile              # Build and pipeline tasks
├── requirements.txt      # Python dependencies
├── data/                 # Local data files
│   ├── rikid/           # Government data
│   └── reykjavik/       # Municipality data
├── templates/           # Jinja2 HTML templates
└── scripts/            # Utility scripts
```

## Data Sources

### Rikið (Government)
- **File**: `data/rikid/parquet/opnirreikningar.parquet`
- **Columns**: Kaupandi (buyer), Birgi (seller), Tegund (type), Upphæð línu (amount), Dags.greiðslu (payment date), Númer reiknings (invoice number)
- **Note**: No `year` column; computed via `YEAR("Dags.greiðslu")`
- **Anomalies**: `anomalies_flagged.parquet`, `anomalies_yearly_all.parquet`

### Reykjavík (Municipality)
- **File**: `data/reykjavik/processed/arsuppgjor_combined.parquet`
- **Columns**: Include `raun` (amount in Icelandic format), `year` (native)
- **Hierarchy**: samtala0-3 (organization), tegund0-3 (type)

## Setup & Installation

1. **Clone and navigate**:
   ```bash
   cd opin_gogn
   ```

2. **Create virtual environment** (optional):
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Application

### Development Mode
```bash
python3 -m flask --app app run --debug --port 5002
```

Then visit: `http://localhost:5002`

### Production (WSGI)
```bash
gunicorn wsgi:app
```

## Build & Pipeline

### Refresh Data
```bash
make pipeline           # Run full pipeline for both sources
make refresh FROM=2024-01-01 TO=2024-12-31  # Force-redownload date range
```

The pipeline:
- Detects the latest Rikið date from previous runs
- Redownloads any new data
- Rebuilds Parquet files
- Refreshes the in-memory DuckDB database

## Routes

### Home
- `/` — Combined overview of both data sources

### Rikið (Government)
- `/rikid/` — Data explorer
- `/rikid/analysis` — Interactive analysis
- `/rikid/anomalies` — Anomaly detection & reporting
- `/rikid/reports` — Static reports

### Reykjavík (Municipality)
- `/reykjavik/` — Data explorer
- `/reykjavik/analysis` — Interactive analysis
- `/reykjavik/anomalies` — Anomaly detection & reporting
- `/reykjavik/reports` — Static reports

## Template Structure

- `base.html` — Master template with navigation, shared CSS
- `home.html` — Landing page overview
- `explorer.html` — Data explorer interface
- `analysis.html` — Analysis tables with visualizations
- `anomalies.html` — Anomaly detection results
- `reports.html` — Static report views

### Color Scheme
- **Rikið**: Blue (#2563eb)
- **Reykjavík**: Teal (#0d9488)

## Key Implementation Details

- `open_rikid_con()` — Returns DuckDB connection with computed `year` column for Rikið data
- `build_where()` — Generates SQL WHERE clauses from filter parameters, excluding "all" values
- `year_col_totals` — Python dict passed to templates for footer row sums in analysis views
- All routes support filtering via query parameters

## Development Notes

- Data pipelines use separate Python virtual environments:
  - `../rikid/.venv/bin/python3` for Rikið pipeline
  - `../reykjavik/.venv/bin/python3` for Reykjavík pipeline
- Scripts adapted from respective source repositories
- DuckDB queries handle Icelandic number format conversion (both `.` and `,` as separators)

## License

(Add appropriate license information)
