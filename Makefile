# Makefile for opin_gogn
# All data is stored locally under data/rikid/ and data/reykjavik/.
# Pipeline scripts are borrowed from the sibling repos but run here.

SHELL  := /bin/bash
PYTHON := .venv/bin/python3   # Python for web server and pipeline

SCRIPTS := scripts

RIKID_PARQUET_DIR := data/rikid/parquet
RKV_PROCESSED_DIR := data/reykjavik/processed

# Pipeline overrides
FROM      ?=
TO        ?=

.PHONY: web dev install pipeline anomalies \
        rikid-pipeline rikid-anomalies \
        reykjavik-pipeline reykjavik-anomalies

# ── Web ──────────────────────────────────────────────────────────────────────

web:
	$(PYTHON) -m flask --app app run --debug --port 5002

dev:
	$(PYTHON) -m flask --app app run --debug --port 5002

install:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

# ── Combined pipelines ───────────────────────────────────────────────────────

# Run both source pipelines and rebuild anomalies.
# Usage:
#   make pipeline         - Incremental update (from last date to today)
#   make pipeline RESET=1 - Full refresh (delete and redownload everything)
RESET ?=

pipeline:
	@reset="$(RESET)"; \
	if [ "$$reset" = "1" ]; then \
		echo "==> RESET MODE: Full refresh"; \
		rm -f $(RIKID_PARQUET_DIR)/opnirreikningar*.parquet; \
		rm -f $(RKV_PROCESSED_DIR)/arsuppgjor*.parquet; \
		$(MAKE) rikid-pipeline FROM=2017-01-01 || echo "⚠ Rikið pipeline failed (API issue)"; \
		$(MAKE) reykjavik-pipeline && \
		$(MAKE) anomalies; \
	else \
		echo "==> Incremental update"; \
		from=$$($(PYTHON) scripts/detect_max_date.py "$(RIKID_PARQUET_DIR)/opnirreikningar_with_corrections.parquet" 2>/dev/null || echo "2017-01-01"); \
		to=$$(date +%Y-%m-%d); \
		echo "==> rikid:     FROM=$$from TO=$$to"; \
		echo "==> reykjavik: refresh all years"; \
		$(MAKE) rikid-pipeline FROM=$$from TO=$$to || echo "⚠ Rikið pipeline failed (API issue)"; \
		$(MAKE) reykjavik-pipeline && \
		$(MAKE) anomalies; \
	fi

# ── Rikid ────────────────────────────────────────────────────────────────────

rikid-pipeline:
	mkdir -p $(RIKID_PARQUET_DIR)
	@from="$(FROM)"; to="$(TO)"; \
	if [ -z "$$from" ]; then from="2017-01-01"; fi; \
	if [ -z "$$to" ]; then to=$$(date +%Y-%m-%d); fi; \
	echo "==> Downloading Rikið data from $$from to $$to (monthly chunks)"; \
	$(PYTHON) $(SCRIPTS)/download_rikid.py \
		--from "$$from" --to "$$to" \
		--output "$(RIKID_PARQUET_DIR)/opnirreikningar.parquet" || true
	@if [ -f "$(RIKID_PARQUET_DIR)/opnirreikningar.parquet" ]; then \
		echo "==> Detecting correction transactions..."; \
		$(PYTHON) $(SCRIPTS)/detect_corrections.py \
			--input "$(RIKID_PARQUET_DIR)/opnirreikningar.parquet" \
			--output "$(RIKID_PARQUET_DIR)/opnirreikningar_with_corrections.parquet"; \
	fi

rikid-anomalies:
	mkdir -p $(RIKID_PARQUET_DIR)
	@if [ -f "$(RIKID_PARQUET_DIR)/opnirreikningar_with_corrections.parquet" ]; then \
		echo "==> Building anomalies for Rikið..."; \
		$(PYTHON) $(SCRIPTS)/detect_anomalies_rikid.py \
			--input "$(RIKID_PARQUET_DIR)/opnirreikningar_with_corrections.parquet" \
			--output-all "$(RIKID_PARQUET_DIR)/anomalies_yearly_all.parquet" \
			--output-flagged "$(RIKID_PARQUET_DIR)/anomalies_flagged.parquet"; \
	fi

# ── Reykjavík ────────────────────────────────────────────────────────────────

reykjavik-pipeline:
	mkdir -p $(RKV_PROCESSED_DIR)
	@echo "==> Downloading Reykjavík data from gagnagatt.reykjavik.is..."
	$(PYTHON) $(SCRIPTS)/download_reykjavik.py \
		--output "$(RKV_PROCESSED_DIR)/arsuppgjor_combined.parquet"
	@echo "==> Detecting correction transactions..."
	$(PYTHON) $(SCRIPTS)/detect_corrections_reykjavik.py \
		--input "$(RKV_PROCESSED_DIR)/arsuppgjor_combined.parquet" \
		--output "$(RKV_PROCESSED_DIR)/arsuppgjor_combined_with_corrections.parquet"

reykjavik-anomalies:
	mkdir -p $(RKV_PROCESSED_DIR)
	@if [ -f "$(RKV_PROCESSED_DIR)/arsuppgjor_combined_with_corrections.parquet" ]; then \
		echo "==> Building anomalies for Reykjavík..."; \
		$(PYTHON) $(SCRIPTS)/detect_anomalies_reykjavik.py \
			--input "$(RKV_PROCESSED_DIR)/arsuppgjor_combined_with_corrections.parquet" \
			--output-all "$(RKV_PROCESSED_DIR)/anomalies_yoy_all.parquet" \
			--output-flagged "$(RKV_PROCESSED_DIR)/anomalies_flagged.parquet"; \
	fi

# ── Anomalies (rebuild for both) ──────────────────────────────────────────────

anomalies: rikid-anomalies reykjavik-anomalies

# ── VAT enrichment (optional, separate from main pipeline) ───────────────────

enrich-vat: enrich-vat-rikid enrich-vat-reykjavik

enrich-vat-rikid:
	@echo "==> Enriching VAT numbers in Rikið data..."
	$(PYTHON) $(SCRIPTS)/enrich_rikid_vat.py \
		--input "$(RIKID_PARQUET_DIR)/opnirreikningar_with_corrections.parquet" \
		--output "$(RIKID_PARQUET_DIR)/opnirreikningar_with_corrections_vat_enriched.parquet"

enrich-vat-reykjavik:
	@echo "==> Enriching VAT numbers in Reykjavík data..."
	$(PYTHON) $(SCRIPTS)/enrich_vat_numbers.py \
		--input "$(RKV_PROCESSED_DIR)/arsuppgjor_combined_with_corrections.parquet" \
		--output "$(RKV_PROCESSED_DIR)/arsuppgjor_combined_with_corrections_vat_enriched.parquet"
