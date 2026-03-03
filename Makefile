# Makefile for opin_gogn
# All data is stored locally under data/rikid/ and data/reykjavik/.
# Pipeline scripts are borrowed from the sibling repos but run here.

SHELL  := /bin/bash
PYTHON_RIKID     := ../rikid/.venv/bin/python3
PYTHON_REYKJAVIK := ../reykjavik/.venv/bin/python3
PYTHON           := $(PYTHON_RIKID)   # used for the web server and auto-detect query

RIKID_SCRIPTS     := ../rikid/scripts
REYKJAVIK_SCRIPTS := ../reykjavik/scripts

RIKID_RAW_DIR    := data/rikid/raw
RIKID_PARQUET_DIR := data/rikid/parquet
RKV_RAW_DIR      := data/reykjavik/raw
RKV_PROCESSED_DIR := data/reykjavik/processed

# Rikid pipeline overrides
FROM      ?=
TO        ?=
ORG_ID    ?=
VENDOR_ID ?=
TYPE_ID   ?=

.PHONY: run dev install pipeline refresh anomalies \
        rikid-pipeline rikid-refresh rikid-anomalies \
        reykjavik-pipeline reykjavik-anomalies

# ── Web ──────────────────────────────────────────────────────────────────────

run:
	$(PYTHON) -m gunicorn -b 0.0.0.0:5002 wsgi:app

dev:
	$(PYTHON) -m flask --app app run --debug --port 5002

install:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

# ── Combined pipelines ───────────────────────────────────────────────────────

# Run both source pipelines and rebuild anomalies.
# FROM defaults to the first day of the latest month already in the rikid parquet.
# TO defaults to today. Both can be overridden: make pipeline FROM=... TO=...
pipeline:
	@from="$(FROM)"; \
	if [ -z "$$from" ]; then \
		from=$$($(PYTHON) scripts/rikid_max_date.py 2>/dev/null || echo "2017-01-01"); \
	fi; \
	to="$(TO)"; \
	if [ -z "$$to" ]; then to=$$(date +%Y-%m-%d); fi; \
	echo "==> rikid:     FROM=$$from TO=$$to"; \
	echo "==> reykjavik: full refresh"; \
	$(MAKE) rikid-pipeline FROM=$$from TO=$$to \
		ORG_ID="$(ORG_ID)" VENDOR_ID="$(VENDOR_ID)" TYPE_ID="$(TYPE_ID)" && \
	$(MAKE) reykjavik-pipeline && \
	$(MAKE) anomalies

# Force-redownload a date range for rikid, then rebuild everything
refresh:
	@if [ -z "$(FROM)" ] || [ -z "$(TO)" ]; then \
		echo "Usage: make refresh FROM=YYYY-MM-DD TO=YYYY-MM-DD"; \
		exit 1; \
	fi
	$(MAKE) rikid-refresh FROM=$(FROM) TO=$(TO) \
		ORG_ID="$(ORG_ID)" VENDOR_ID="$(VENDOR_ID)" TYPE_ID="$(TYPE_ID)"
	$(MAKE) reykjavik-pipeline
	$(MAKE) anomalies

# Rebuild anomaly parquets for both sources
anomalies: rikid-anomalies reykjavik-anomalies

# ── Rikid ────────────────────────────────────────────────────────────────────

rikid-pipeline:
	@if [ -z "$(FROM)" ] || [ -z "$(TO)" ]; then \
		echo "Usage: make rikid-pipeline FROM=YYYY-MM-DD TO=YYYY-MM-DD"; \
		exit 1; \
	fi
	mkdir -p $(RIKID_RAW_DIR) $(RIKID_PARQUET_DIR)
	$(PYTHON_RIKID) $(RIKID_SCRIPTS)/pipeline.py \
		--from $(FROM) --to $(TO) \
		--raw-dir $(RIKID_RAW_DIR) \
		--parquet-dir $(RIKID_PARQUET_DIR) \
		--org-id "$(ORG_ID)" --vendor-id "$(VENDOR_ID)" --type-id "$(TYPE_ID)"

rikid-refresh:
	@if [ -z "$(FROM)" ] || [ -z "$(TO)" ]; then \
		echo "Usage: make rikid-refresh FROM=YYYY-MM-DD TO=YYYY-MM-DD"; \
		exit 1; \
	fi
	mkdir -p $(RIKID_RAW_DIR) $(RIKID_PARQUET_DIR)
	$(PYTHON_RIKID) $(RIKID_SCRIPTS)/pipeline.py \
		--from $(FROM) --to $(TO) \
		--raw-dir $(RIKID_RAW_DIR) \
		--parquet-dir $(RIKID_PARQUET_DIR) \
		--org-id "$(ORG_ID)" --vendor-id "$(VENDOR_ID)" --type-id "$(TYPE_ID)" \
		--force-download

rikid-anomalies:
	mkdir -p $(RIKID_PARQUET_DIR)
	$(PYTHON_RIKID) $(RIKID_SCRIPTS)/build_anomalies.py \
		--input $(RIKID_PARQUET_DIR)/opnirreikningar.parquet \
		--out-flagged $(RIKID_PARQUET_DIR)/anomalies_flagged.parquet \
		--out-all $(RIKID_PARQUET_DIR)/anomalies_yearly_all.parquet

# ── Reykjavík ────────────────────────────────────────────────────────────────

reykjavik-pipeline: reykjavik-download reykjavik-prepare reykjavik-lookup reykjavik-anomalies

reykjavik-download:
	mkdir -p $(RKV_RAW_DIR)
	$(PYTHON_REYKJAVIK) $(REYKJAVIK_SCRIPTS)/download_arsuppgjor.py \
		--raw-dir $(RKV_RAW_DIR)

reykjavik-prepare:
	mkdir -p $(RKV_PROCESSED_DIR)
	$(PYTHON_REYKJAVIK) $(REYKJAVIK_SCRIPTS)/prepare_arsuppgjor.py \
		--raw-dir $(RKV_RAW_DIR) \
		--processed-dir $(RKV_PROCESSED_DIR)

reykjavik-lookup:
	$(PYTHON_REYKJAVIK) $(REYKJAVIK_SCRIPTS)/lookup_vm_entities.py \
		--parquet $(RKV_PROCESSED_DIR)/arsuppgjor_combined.parquet \
		--out     $(RKV_PROCESSED_DIR)/vm_entities.csv \
		--cache   $(RKV_PROCESSED_DIR)/vm_entities.json

reykjavik-anomalies:
	$(PYTHON_REYKJAVIK) $(REYKJAVIK_SCRIPTS)/detect_anomalies.py \
		--parquet $(RKV_PROCESSED_DIR)/arsuppgjor_combined.parquet \
		--out-dir $(RKV_PROCESSED_DIR)
