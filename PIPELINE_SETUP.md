# Pipeline Setup Summary

This document summarizes the updates made to use direct data sources from opnirreikningar.is and gagnagatt.reykjavik.is.

## What's Changed

### Removed Dependencies
- ❌ Sibling repos (`../rikid/` and `../reykjavik/`)
- No longer requires separate Python environments

### Added Direct Data Downloads
- ✅ **Reykjavík**: Automated download from CKAN API
- ✅ **Rikið**: Script available (API currently returning empty results)

### New Files
- `scripts/download_reykjavik.py` — Downloads from gagnagatt.reykjavik.is
- `scripts/download_rikid.py` — Downloads from opnirreikningar.is (API-based)

### Updated Files
- `Makefile` — Simplified pipeline targets
- `requirements.txt` — Added pandas, pyarrow, requests, openpyxl
- `CLAUDE.md` — Updated with new data source information

## Current Status

### ✅ Reykjavík Data (Fully Working)
```bash
make reykjavik-pipeline
```
- Downloads 12 CSV files (2014-2025+) from CKAN API
- Combines ~1.08M rows into Parquet
- Routes: `/reykjavik/`, `/reykjavik/analysis`, `/reykjavik/anomalies`, `/reykjavik/reports`

### ⚠️ Rikið Data (Requires Manual Setup)
```bash
make rikid-pipeline FROM=2017-01-01 TO=2024-12-31
```
- Script: `scripts/download_rikid.py`
- Issue: API currently returns empty results
- Workaround: Manual download from web UI (see CLAUDE.md)

## Testing

All routes load successfully:
```bash
make dev
```

Browser: `http://localhost:5002`
- Home page: ✓ Works
- Reykjavík explorer: ✓ Works with data
- Rikið explorer: ✓ Loads (shows "data unavailable")

## Next Steps

1. **For Rikið data**, choose one:
   - **Manual download**: Visit opnirreikningar.is, export Excel, convert to Parquet
   - **Check if API recovers**: Retry `make rikid-pipeline` later
   - **Review original repo**: https://github.com/bjornlevi/rikid-opnir-reikningar

2. **Implement anomaly detection** (currently stubbed in Makefile)

3. **Optional**: Automate Rikið downloads once API stability is confirmed

## API Details

### Reykjavík CKAN API
- **Endpoint**: `https://gagnagatt.reykjavik.is/api/3/action/package_show`
- **Dataset ID**: `arsuppgjor`
- **Files**: 12 CSV files (annual reports 2014-2025+)
- **Format**: Semicolon-delimited with Icelandic number formatting (dot for thousands, comma for decimals)

### Rikið CSV Export
- **Endpoint**: `https://opnirreikningar.is/rest/csvExport`
- **Parameters**: `timabil_fra`, `timabil_til` (DD.MM.YYYY format), optional `org_id`, `vendor_id`, `type_id`
- **Format**: Returns XLSX (despite Content-Type: text/csv)
- **Status**: Currently returns empty results—reason unknown

## File Locations

```
data/
├── rikid/
│   └── parquet/
│       ├── opnirreikningar.parquet (if downloaded)
│       └── opnirreikningar_with_corrections.parquet (if processed)
└── reykjavik/
    └── processed/
        ├── arsuppgjor_combined.parquet ✓
        └── arsuppgjor_combined_with_corrections.parquet ✓
```

## Troubleshooting

**Q: Reykjavík download fails**
- Check internet connection
- Verify CKAN API is accessible: curl https://gagnagatt.reykjavik.is/api/3/action/package_show?id=arsuppgjor
- Ensure pandas, pyarrow, requests installed: `pip install -r requirements.txt`

**Q: Rikið download returns 0 rows**
- API may be returning empty results (check manually at opnirreikningar.is)
- Use manual download method as workaround
- Check original repo for alternative approaches

**Q: Web server doesn't load**
- Ensure Flask is installed: `.venv/bin/pip install flask duckdb`
- Check that Reykjavík Parquet file exists: `ls data/reykjavik/processed/arsuppgjor_combined.parquet`
- Run: `make dev`
