#!/usr/bin/env python3
"""Combined Open Accounts explorer — Rikið and Reykjavík."""
from __future__ import annotations

import math
import os
from pathlib import Path

import duckdb
from flask import Flask, render_template, request, url_for, redirect, Response
from werkzeug.middleware.proxy_fix import ProxyFix

BASE_DIR = Path(__file__).resolve().parent

# URL prefix for reverse proxy deployment
PREFIX = os.getenv("PREFIX", "").rstrip("/")

# ---------------------------------------------------------------------------
# Data paths – override via environment variables
# ---------------------------------------------------------------------------
RIKID_DATA = Path(
    os.getenv("RIKID_PARQUET",
              str(BASE_DIR / "data/rikid/parquet/opnirreikningar_with_corrections.parquet"))
)
RIKID_ANOMALIES = Path(
    os.getenv("RIKID_ANOMALIES",
              str(BASE_DIR / "data/rikid/parquet/anomalies_flagged.parquet"))
)
RIKID_ANOMALIES_ALL = Path(
    os.getenv("RIKID_ANOMALIES_ALL",
              str(BASE_DIR / "data/rikid/parquet/anomalies_yearly_all.parquet"))
)
REYKJAVIK_DATA = Path(
    os.getenv("REYKJAVIK_PARQUET",
              str(BASE_DIR / "data/reykjavik/processed/arsuppgjor_combined_with_corrections.parquet"))
)
REYKJAVIK_ANOMALIES = Path(
    os.getenv("REYKJAVIK_ANOMALIES",
              str(BASE_DIR / "data/reykjavik/processed/anomalies_flagged.parquet"))
)
REYKJAVIK_ANOMALIES_ALL = Path(
    os.getenv("REYKJAVIK_ANOMALIES_ALL",
              str(BASE_DIR / "data/reykjavik/processed/anomalies_yoy_all.parquet"))
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def fmt(value) -> str:
    """Format a number in Icelandic style: dot thousands, no decimals."""
    if value is None:
        return "–"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{n:,.0f}".replace(",", ".")


def fmt_pct(value) -> str:
    if value is None:
        return "–"
    try:
        return f"{float(value):+.1f}%"
    except (TypeError, ValueError):
        return str(value)


def safe_path(p: Path) -> str:
    return str(p).replace("'", "''")


def open_con(parquet: Path, view: str = "data") -> duckdb.DuckDBPyConnection | None:
    if not parquet.exists():
        return None
    con = duckdb.connect(":memory:")
    con.execute(
        f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{safe_path(parquet)}')"
    )
    return con


def open_rikid_con(parquet: Path) -> duckdb.DuckDBPyConnection | None:
    """Open rikid main data with a computed `year` column from Dags.greiðslu."""
    if not parquet.exists():
        return None
    con = duckdb.connect(":memory:")
    con.execute(
        f"CREATE VIEW data AS "
        f"SELECT *, YEAR(\"Dags.greiðslu\") AS year "
        f"FROM read_parquet('{safe_path(parquet)}')"
    )
    return con


def build_where(conditions: list[tuple[str, object]]) -> tuple[str, list]:
    """Build a DuckDB WHERE clause from (column, value) pairs. Skips 'all'."""
    clauses, params = [], []
    for col, val in conditions:
        if val and val != "all":
            clauses.append(f'"{col}" = ?')
            params.append(val)
    sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    return sql, params


def _rikid_headline() -> dict:
    con = open_rikid_con(RIKID_DATA)
    if con is None:
        return {"available": False}
    try:
        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL AND (is_correction = FALSE OR is_correction IS NULL) ORDER BY year DESC"
        ).fetchall()]
        yearly = con.execute(
            'SELECT year, '
            'SUM(CASE WHEN "Upphæð línu" > 0 THEN "Upphæð línu" END) AS pos, '
            'SUM(CASE WHEN "Upphæð línu" < 0 THEN "Upphæð línu" END) AS neg, '
            'SUM("Upphæð línu") AS net '
            'FROM data WHERE is_correction = FALSE OR is_correction IS NULL GROUP BY year ORDER BY year'
        ).fetchall()
        latest = next((r for r in yearly if r[0] == years[0]), None) if years else None
        return {
            "available": True,
            "years": years,
            "yearly_labels": [str(r[0]) + ("*" if i == len(yearly) - 1 else "") for i, r in enumerate(yearly)],
            "yearly_pos":    [float(r[1]) if r[1] is not None else 0 for r in yearly],
            "yearly_neg":    [float(r[2]) if r[2] is not None else 0 for r in yearly],
            "yearly_net":    [float(r[3]) if r[3] is not None else 0 for r in yearly],
            "latest_year":   (str(years[0]) + "*") if years else None,
            "latest_total":  fmt(latest[3]) if latest else "–",
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


def _rkv_headline() -> dict:
    con = open_con(REYKJAVIK_DATA)
    if con is None:
        return {"available": False}
    try:
        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL AND (is_correction = FALSE OR is_correction IS NULL) ORDER BY year DESC"
        ).fetchall()]
        amt = "raun"  # raun is already numeric after download processing
        yearly = con.execute(
            f"SELECT year, "
            f"SUM(CASE WHEN {amt} > 0 THEN {amt} END) AS pos, "
            f"SUM(CASE WHEN {amt} < 0 THEN {amt} END) AS neg, "
            f"SUM({amt}) AS net "
            f"FROM data WHERE is_correction = FALSE OR is_correction IS NULL GROUP BY year ORDER BY year"
        ).fetchall()
        latest = next((r for r in yearly if r[0] == years[0]), None) if years else None
        return {
            "available": True,
            "years": years,
            "yearly_labels": [str(r[0]) + ("*" if i == len(yearly) - 1 else "") for i, r in enumerate(yearly)],
            "yearly_pos":    [float(r[1]) if r[1] is not None else 0 for r in yearly],
            "yearly_neg":    [float(r[2]) if r[2] is not None else 0 for r in yearly],
            "yearly_net":    [float(r[3]) if r[3] is not None else 0 for r in yearly],
            "latest_year":   (str(years[0]) + "*") if years else None,
            "latest_total":  fmt(latest[3]) if latest else "–",
        }
    except Exception as e:
        return {"available": False, "error": str(e)}


# ===========================================================================
# RIKID
# ===========================================================================

RIKID_TYPE_COLS = ["Tegund"]
RIKID_ORG_COLS = ["Kaupandi", "Birgi"]
RIKID_AMOUNT = '"Upphæð línu"'

RIKID_DISPLAY = {
    "year": "Ár",
    "Tegund": "Tegund",
    "Kaupandi": "Stofnun (kaupandi)",
    "Birgi": "Birgir",
    "Upphæð línu": "Upphæð",
    "Dags.greiðslu": "Dags.",
    "Númer reiknings": "Reikningur",
}


def rikid_dn(col: str) -> str:
    return RIKID_DISPLAY.get(col, col)


# ===========================================================================
# REYKJAVIK
# ===========================================================================

RKV_AMOUNT_EXPR = "raun"
RKV_SUPPLIER_EXPR = (
    "COALESCE(NULLIF(TRIM(vm_nafn), ''), "
    "NULLIF(TRIM(fyrirtaeki), ''), "
    "NULLIF(TRIM(CAST(vm_numer AS VARCHAR)), ''))"
)

RKV_TYPE_COLS = ["tegund0", "tegund1", "tegund2", "tegund3"]
RKV_ORG_COLS = ["samtala0", "samtala1", "samtala2_canonical", "samtala3"]
RKV_ALL_GROUP_COLS = [
    "tegund0", "tegund1", "tegund2", "tegund3",
    "samtala0", "samtala1", "samtala2_canonical", "samtala3",
]

RKV_DISPLAY = {
    "year": "Ár",
    "tegund0": "Tegundaflokkur",
    "tegund1": "Tegund 1",
    "tegund2": "Tegund 2",
    "tegund3": "Tegund 3",
    "samtala0": "Stofnun",
    "samtala1": "Svið",
    "samtala2_canonical": "Þjónusta",
    "samtala3": "Undireining",
    "raun": "Upphæð (raun)",
    "fyrirtaeki": "Fyrirtæki",
    "vm_numer": "VSK-númer",
    "supplier_name": "VSK-heiti",
    "vm_nafn": "VSK-heiti",
}


def rkv_dn(col: str) -> str:
    return RKV_DISPLAY.get(col, col)


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)

    if PREFIX:
        app.config["APPLICATION_ROOT"] = PREFIX
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

        class PrefixMiddleware:
            def __init__(self, app, prefix: str):
                self.app = app
                self.prefix = prefix

            def __call__(self, environ, start_response):
                script_name = self.prefix
                path_info = environ.get("PATH_INFO", "")
                if path_info.startswith(script_name):
                    environ["SCRIPT_NAME"] = script_name
                    environ["PATH_INFO"] = path_info[len(script_name):] or "/"
                return self.app(environ, start_response)

        app.wsgi_app = PrefixMiddleware(app.wsgi_app, PREFIX)

    app.jinja_env.globals.update(fmt=fmt, fmt_pct=fmt_pct)

    # ===========================================================================
    # HOME
    # ===========================================================================

    @app.route("/")
    def home():
        # Quick headline stats for each source
        rikid_stat = _rikid_headline()
        rkv_stat = _rkv_headline()
        return render_template("home.html", rikid=rikid_stat, reykjavik=rkv_stat)

    # ===========================================================================
    # RIKID
    # ===========================================================================

    @app.route("/rikid/")
    def rikid_explorer():
        year = request.args.get("year", "all").rstrip("*")  # Remove asterisk indicator
        tegund = request.args.get("tegund", "all")
        buyer = request.args.get("buyer", "all")
        seller = request.args.get("seller", "all")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        page = max(1, int(request.args.get("page", 1)))
        offset = (page - 1) * limit

        con = open_rikid_con(RIKID_DATA)
        if con is None:
            return render_template("explorer.html", source="rikid", data_loaded=False,
                                   error=f"Gögn finnast ekki: {RIKID_DATA}")

        where, params = build_where([
            ("year", year if year != "all" else None),
            ("Tegund", tegund if tegund != "all" else None),
            ("Kaupandi", buyer if buyer != "all" else None),
            ("Birgi", seller if seller != "all" else None),
        ])
        chart_where, chart_params = build_where([
            ("Tegund", tegund if tegund != "all" else None),
            ("Kaupandi", buyer if buyer != "all" else None),
            ("Birgi", seller if seller != "all" else None),
        ])

        # Add filter for corrections
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"
            chart_where += " AND (is_correction = FALSE OR is_correction IS NULL)" if chart_where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        years_raw = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        tegund_opts = [r[0] for r in con.execute(
            f'SELECT DISTINCT "Tegund" FROM data WHERE "Tegund" IS NOT NULL ORDER BY "Tegund"'
        ).fetchall()]

        buyer_opts = [r[0] for r in con.execute(
            f'SELECT DISTINCT "Kaupandi" FROM data WHERE "Kaupandi" IS NOT NULL ORDER BY "Kaupandi"'
        ).fetchall()]

        # Yearly totals for chart
        yearly = con.execute(
            f'SELECT year, SUM({RIKID_AMOUNT}) FROM data {chart_where} GROUP BY year ORDER BY year',
            chart_params,
        ).fetchall()

        # Type breakdown (top 30)
        type_breakdown = con.execute(
            f'SELECT "Tegund", SUM({RIKID_AMOUNT}) AS s, COUNT(*) AS n '
            f'FROM data {where} GROUP BY "Tegund" ORDER BY s DESC LIMIT 30',
            params,
        ).fetchall()
        buyer_breakdown = con.execute(
            f'SELECT "Kaupandi", SUM({RIKID_AMOUNT}) AS s, COUNT(*) AS n '
            f'FROM data {where} GROUP BY "Kaupandi" ORDER BY s DESC LIMIT 30',
            params,
        ).fetchall()
        seller_breakdown = con.execute(
            f'SELECT "Birgi", SUM({RIKID_AMOUNT}) AS s, COUNT(*) AS n '
            f'FROM data {where} GROUP BY "Birgi" ORDER BY s DESC LIMIT 30',
            params,
        ).fetchall()

        # Summary totals
        tot = con.execute(
            f'SELECT COUNT(*) AS n, SUM({RIKID_AMOUNT}) AS s, '
            f'SUM(CASE WHEN {RIKID_AMOUNT} > 0 THEN {RIKID_AMOUNT} END) AS pos, '
            f'SUM(CASE WHEN {RIKID_AMOUNT} < 0 THEN {RIKID_AMOUNT} END) AS neg '
            f'FROM data {where}',
            params,
        ).fetchone()

        # Preview rows
        rows = con.execute(
            f'SELECT year, "Kaupandi", "Birgi", "Tegund", {RIKID_AMOUNT} AS amount, "Dags.greiðslu" '
            f'FROM data {where} ORDER BY "Dags.greiðslu" DESC LIMIT {limit} OFFSET {offset}',
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "Kaupandi": r[1], "Birgi": r[2], "Tegund": r[3],
             "amount_raw": r[4], "amount": fmt(r[4]), "Dags": str(r[5])[:10] if r[5] else ""}
            for r in rows
        ]

        total_count = tot[0] if tot else 0
        total_pages = max(1, math.ceil(total_count / limit))
        active_filters = []
        if year != "all":
            active_filters.append({"label": "Ár", "value": str(year), "param": "year"})
        if tegund != "all":
            active_filters.append({"label": "Tegund", "value": tegund, "param": "tegund"})
        if buyer != "all":
            active_filters.append({"label": "Kaupandi", "value": buyer, "param": "buyer"})
        if seller != "all":
            active_filters.append({"label": "Birgi", "value": seller, "param": "seller"})

        return render_template(
            "explorer.html",
            source="rikid",
            data_loaded=True,
            year=year, tegund=tegund, buyer=buyer, seller=seller,
            years=years_raw,
            tegund_opts=tegund_opts,
            buyer_opts=buyer_opts,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            breakdown_sections=[
                {"title": "Sundurliðun eftir tegund (topp 30)", "label": "Tegund", "rows": type_breakdown, "filter_param": "tegund"},
                {"title": "Sundurliðun eftir kaupanda (topp 30)", "label": "Kaupandi", "rows": buyer_breakdown, "filter_param": "buyer"},
                {"title": "Sundurliðun eftir birgja (topp 30)", "label": "Birgi", "rows": seller_breakdown, "filter_param": "seller"},
            ],
            totals={"count": tot[0], "sum": tot[1], "pos": tot[2], "neg": tot[3]} if tot else {},
            preview_rows=preview_rows,
            preview_cols=["year", "Kaupandi", "Birgi", "Tegund", "amount", "Dags"],
            page=page, limit=limit, total_pages=total_pages,
            active_filters=active_filters,
            dn=rikid_dn,
        )

    @app.route("/rikid/analysis")
    def rikid_analysis():
        focus = request.args.get("focus", request.args.get("group_by", "tegund"))
        focus_value = request.args.get("focus_value", "all")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        page = max(1, int(request.args.get("page", 1)))
        offset = (page - 1) * limit

        focus_col = "Tegund" if focus == "tegund" else "Kaupandi"
        if focus not in ("tegund", "buyer"):
            focus = "tegund"
            focus_col = "Tegund"

        con = open_rikid_con(RIKID_DATA)
        if con is None:
            return render_template("analysis.html", source="rikid", data_loaded=False,
                                   error=f"Gögn finnast ekki: {RIKID_DATA}")

        where, params = build_where([(focus_col, focus_value if focus_value != "all" else None)])
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        focus_options = [r[0] for r in con.execute(
            f'SELECT DISTINCT "{focus_col}" FROM data WHERE "{focus_col}" IS NOT NULL ORDER BY "{focus_col}"'
        ).fetchall()]
        if focus_value != "all" and focus_value not in focus_options:
            focus_value = "all"

        yearly_selected = con.execute(
            f"WITH yearly AS ("
            f'  SELECT year, SUM({RIKID_AMOUNT}) AS s '
            f"  FROM data {where} GROUP BY year"
            f"), ch AS ("
            f"  SELECT year, s, "
            f"         CASE WHEN LAG(s) OVER (ORDER BY year) IS NULL OR LAG(s) OVER (ORDER BY year) = 0 THEN NULL "
            f"              ELSE ((s - LAG(s) OVER (ORDER BY year)) / ABS(LAG(s) OVER (ORDER BY year))) * 100 END AS change_pct "
            f"  FROM yearly"
            f") "
            f"SELECT year, s, change_pct FROM ch ORDER BY year",
            params,
        ).fetchall()

        avg_change = con.execute(
            f"WITH gy AS ("
            f'  SELECT "{focus_col}" AS g, year, SUM({RIKID_AMOUNT}) AS s '
            f'  FROM data WHERE "{focus_col}" IS NOT NULL '
            + ("AND (is_correction = FALSE OR is_correction IS NULL) " if not show_corrections else "")
            + f'  GROUP BY "{focus_col}", year'
            f"), ch AS ("
            f"  SELECT g, year, "
            f"         CASE WHEN LAG(s) OVER (PARTITION BY g ORDER BY year) IS NULL OR LAG(s) OVER (PARTITION BY g ORDER BY year) = 0 THEN NULL "
            f"              ELSE ((s - LAG(s) OVER (PARTITION BY g ORDER BY year)) / ABS(LAG(s) OVER (PARTITION BY g ORDER BY year))) * 100 END AS change_pct "
            f"  FROM gy"
            f") "
            f"SELECT year, AVG(change_pct) AS avg_change_pct "
            f"FROM ch WHERE change_pct IS NOT NULL GROUP BY year ORDER BY year"
        ).fetchall()
        avg_change_map = {int(r[0]): float(r[1]) for r in avg_change}

        change_rows = []
        for yr, total, chg_pct in yearly_selected:
            yr_i = int(yr)
            chg_pct_f = float(chg_pct) if chg_pct is not None else None
            avg_pct_f = avg_change_map.get(yr_i)
            diff_pct = (chg_pct_f - avg_pct_f) if (chg_pct_f is not None and avg_pct_f is not None) else None
            change_rows.append(
                {
                    "year": yr_i,
                    "total_raw": float(total) if total is not None else 0.0,
                    "total": fmt(total),
                    "change_pct_raw": chg_pct_f,
                    "change_pct": fmt_pct(chg_pct_f) if chg_pct_f is not None else "–",
                    "avg_change_pct_raw": avg_pct_f,
                    "avg_change_pct": fmt_pct(avg_pct_f) if avg_pct_f is not None else "–",
                    "diff_pct_raw": diff_pct,
                    "diff_pct": fmt_pct(diff_pct) if diff_pct is not None else "–",
                }
            )

        rows = con.execute(
            f'SELECT year, "Kaupandi", "Birgi", "Tegund", {RIKID_AMOUNT} AS amount, "Dags.greiðslu" '
            f'FROM data {where} ORDER BY "Dags.greiðslu" DESC LIMIT {limit} OFFSET {offset}',
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "Kaupandi": r[1], "Birgi": r[2], "Tegund": r[3],
             "amount_raw": r[4], "amount": fmt(r[4]), "Dags": str(r[5])[:10] if r[5] else ""}
            for r in rows
        ]

        tot = con.execute(
            f'SELECT COUNT(*) AS n FROM data {where}',
            params,
        ).fetchone()
        total_count = int(tot[0]) if tot and tot[0] is not None else 0
        total_pages = max(1, math.ceil(total_count / limit))

        chart_years = [r["year"] for r in change_rows if r["change_pct_raw"] is not None]
        chart_selected_change = [r["change_pct_raw"] for r in change_rows if r["change_pct_raw"] is not None]
        chart_avg_change = [r["avg_change_pct_raw"] for r in change_rows if r["change_pct_raw"] is not None]
        chart_diff = [r["diff_pct_raw"] for r in change_rows if r["change_pct_raw"] is not None]

        latest = next((r for r in reversed(change_rows) if r["change_pct_raw"] is not None), None)

        active_filters = []
        if focus_value != "all":
            active_filters.append({"label": rikid_dn(focus_col), "value": focus_value, "param": "focus_value"})

        return render_template(
            "analysis.html",
            source="rikid",
            data_loaded=True,
            focus=focus,
            focus_value=focus_value,
            focus_label=rikid_dn(focus_col),
            focus_options=focus_options,
            chart_labels=[str(y) for y in chart_years],
            chart_selected_change=chart_selected_change,
            chart_avg_change=chart_avg_change,
            chart_diff=chart_diff,
            change_rows=change_rows,
            latest=latest,
            active_filters=active_filters,
            preview_rows=preview_rows,
            preview_cols=["year", "Kaupandi", "Birgi", "Tegund", "amount", "Dags"],
            page=page, total_pages=total_pages,
            limit=limit,
            dn=rikid_dn,
        )

    @app.route("/rikid/anomalies")
    def rikid_anomalies():
        focus = request.args.get("focus")
        if not focus:
            legacy_group = request.args.get("group_col", "Tegund")
            focus = "buyer" if legacy_group in ("Kaupandi", "Birgi") else "tegund"
        focus_value = request.args.get("focus_value", "all")
        within_value = request.args.get("within_value", "all")
        year = request.args.get("year", "all").rstrip("*")  # Remove asterisk indicator
        direction = request.args.get("direction", "all")
        min_change_pct = request.args.get("min_change_pct", request.args.get("min_change", ""))
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        focus_col = "Tegund" if focus == "tegund" else "Kaupandi"
        driver_col = "Kaupandi" if focus == "tegund" else "Tegund"
        if focus not in ("tegund", "buyer"):
            focus = "tegund"
            focus_col = "Tegund"
            driver_col = "Kaupandi"

        con_f = open_con(RIKID_ANOMALIES, "anomalies")
        con_a = open_con(RIKID_ANOMALIES_ALL, "anomalies_all")

        if con_f is None and con_a is None:
            return render_template("anomalies.html", source="rikid", data_loaded=False,
                                   error=f"Anomaly-gögn finnast ekki: {RIKID_ANOMALIES}")

        # Use flagged if available, else all
        con = con_f or con_a
        view = "anomalies" if con_f else "anomalies_all"

        focus_options = [r[0] for r in con.execute(
            f'SELECT DISTINCT "{focus_col}" FROM {view} WHERE "{focus_col}" IS NOT NULL ORDER BY "{focus_col}"'
        ).fetchall()]
        if focus_value != "all" and focus_value not in focus_options:
            focus_value = "all"

        global_clauses, global_params = [], []
        if year != "all":
            global_clauses.append("year = ?")
            global_params.append(int(year))
        if direction == "up":
            global_clauses.append("yoy_real_pct > 0")
        elif direction == "down":
            global_clauses.append("yoy_real_pct < 0")
        if min_change_pct:
            try:
                global_clauses.append("ABS(yoy_real_pct) >= ?")
                global_params.append(float(min_change_pct))
            except ValueError:
                min_change_pct = ""
        global_where = "WHERE " + " AND ".join(global_clauses) if global_clauses else ""

        scoped_clauses = list(global_clauses)
        scoped_params = list(global_params)
        if focus_value != "all":
            scoped_clauses.append(f'"{focus_col}" = ?')
            scoped_params.append(focus_value)
        scoped_where = "WHERE " + " AND ".join(scoped_clauses) if scoped_clauses else ""

        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM {view} WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        def score_by_col(col: str, where_sql: str, where_params: list, top_n: int = 15) -> list[dict]:
            out = con.execute(
                f"WITH agg AS ("
                f'  SELECT "{col}" AS g, year, '
                f"         SUM(actual_real) AS actual_real, "
                f"         SUM(prior_real) AS prior_real, "
                f"         SUM(yoy_real_change) AS yoy_real_change "
                f"  FROM (SELECT * FROM {view} {where_sql}) t "
                f'  WHERE "{col}" IS NOT NULL '
                f"  GROUP BY g, year"
                f"), score AS ("
                f"  SELECT g, "
                f"         COUNT(*) AS years_flagged, "
                f"         SUM(ABS(yoy_real_change)) AS abs_change_sum, "
                f"         MAX(ABS(CASE WHEN prior_real = 0 OR prior_real IS NULL THEN NULL "
                f"                      ELSE (yoy_real_change / ABS(prior_real)) * 100 END)) AS max_abs_pct "
                f"  FROM agg GROUP BY g"
                f") "
                f"SELECT g, years_flagged, abs_change_sum, max_abs_pct "
                f"FROM score ORDER BY abs_change_sum DESC NULLS LAST LIMIT {top_n}",
                where_params,
            ).fetchall()
            return [
                {
                    "group": r[0],
                    "years_flagged": int(r[1]) if r[1] is not None else 0,
                    "avg_change_amount": fmt((float(r[2]) / float(r[1])) if (r[1] not in (None, 0) and r[2] is not None) else None),
                    "abs_change_sum": fmt(r[2]),
                    "max_abs_pct": fmt_pct(r[3]),
                }
                for r in out
            ]

        overview_mode = focus_value == "all"
        if overview_mode:
            within_value = "all"
        if overview_mode:
            summary_rows = []
            anomaly_rows = []
            overall_buyer_rows = score_by_col("Kaupandi", global_where, global_params, 20)
            overall_type_rows = score_by_col("Tegund", global_where, global_params, 20)
            row_label = ""
            context_rows = []
        else:
            overall_buyer_rows = []
            overall_type_rows = []
            summary_rows = score_by_col(driver_col, scoped_where, scoped_params, 15)
            valid_within = {r["group"] for r in summary_rows if r.get("group")}
            if within_value != "all" and within_value not in valid_within:
                within_value = "all"
            row_label = rikid_dn(driver_col)
            detail_clauses = list(scoped_clauses)
            detail_params = list(scoped_params)
            if within_value != "all":
                detail_clauses.append(f'"{driver_col}" = ?')
                detail_params.append(within_value)
            detail_where = "WHERE " + " AND ".join(detail_clauses) if detail_clauses else ""
            rows = con.execute(
                f"WITH agg AS ("
                f'  SELECT "{driver_col}" AS g, year, '
                f"         SUM(actual_real) AS actual_real, "
                f"         SUM(prior_real) AS prior_real, "
                f"         SUM(yoy_real_change) AS yoy_real_change "
                f"  FROM (SELECT * FROM {view} {detail_where}) t "
                f'  WHERE "{driver_col}" IS NOT NULL '
                f"  GROUP BY g, year"
                f") "
                f"SELECT g, year, actual_real, prior_real, yoy_real_change, "
                f"       CASE WHEN prior_real = 0 OR prior_real IS NULL THEN NULL "
                f"            ELSE (yoy_real_change / ABS(prior_real)) * 100 END AS yoy_real_pct "
                f"FROM agg "
                f"ORDER BY ABS(yoy_real_pct) DESC NULLS LAST LIMIT {limit}",
                detail_params,
            ).fetchall()
            anomaly_rows = [
                {
                    "group": r[0], "year": r[1],
                    "period": f"{int(r[1]) - 1} → {int(r[1])}" if r[1] is not None else "–",
                    "actual": fmt(r[2]), "prior": fmt(r[3]),
                    "change": fmt(r[4]), "pct": fmt_pct(r[5]),
                    "direction": "up" if (r[4] or 0) >= 0 else "down",
                }
                for r in rows
            ]

            anomaly_count_rows = con.execute(
                f'SELECT "{driver_col}" AS g, COUNT(DISTINCT year) AS n_anom_years '
                f"FROM (SELECT * FROM {view} {scoped_where}) t "
                f'WHERE "{driver_col}" IS NOT NULL '
                f'GROUP BY "{driver_col}"',
                scoped_params,
            ).fetchall()
            anomaly_count_map = {str(r[0]): int(r[1]) for r in anomaly_count_rows if r[0] is not None}

            con_main = open_rikid_con(RIKID_DATA)
            if con_main is not None:
                main_clauses = [f'"{focus_col}" = ?', "(is_correction = FALSE OR is_correction IS NULL)"]
                main_params = [focus_value]
                if year != "all":
                    main_clauses.append("year = ?")
                    main_params.append(int(year))
                main_where = "WHERE " + " AND ".join(main_clauses)
                context_raw = con_main.execute(
                    f'SELECT "{driver_col}" AS g, '
                    f'SUM({RIKID_AMOUNT}) AS total_amount, '
                    f'COUNT(DISTINCT year) AS years_with_spend '
                    f'FROM data {main_where} '
                    f'AND "{driver_col}" IS NOT NULL '
                    f'GROUP BY "{driver_col}" '
                    f'ORDER BY ABS(total_amount) DESC LIMIT 30',
                    main_params,
                ).fetchall()
                context_rows = [
                    {
                        "group": r[0],
                        "total_amount": fmt(r[1]),
                        "years_with_spend": int(r[2]) if r[2] is not None else 0,
                        "anomaly_years": anomaly_count_map.get(str(r[0]), 0),
                        "is_selected": within_value != "all" and r[0] == within_value,
                    }
                    for r in context_raw
                ]
            else:
                context_rows = []

        con_main = open_rikid_con(RIKID_DATA)
        if con_main is not None:
            year_domain = [int(r[0]) for r in con_main.execute(
                "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year"
            ).fetchall()]
            main_clauses = ["(is_correction = FALSE OR is_correction IS NULL)"]
            main_params: list = []
            if not overview_mode:
                main_clauses.append(f'"{focus_col}" = ?')
                main_params.append(focus_value)
                if within_value != "all":
                    main_clauses.append(f'"{driver_col}" = ?')
                    main_params.append(within_value)
            main_where = "WHERE " + " AND ".join(main_clauses)
            yearly_main = con_main.execute(
                f"SELECT year, SUM({RIKID_AMOUNT}) AS s FROM data {main_where} GROUP BY year ORDER BY year",
                main_params,
            ).fetchall()
            yearly_amount_map = {int(r[0]): float(r[1]) if r[1] is not None else 0.0 for r in yearly_main}
        else:
            year_domain = [int(r[0]) for r in con.execute(
                f"SELECT DISTINCT year FROM {view} WHERE year IS NOT NULL ORDER BY year"
            ).fetchall()]
            yearly_amount_map = {y: 0.0 for y in year_domain}

        yearly_values, yearly_change_pct = [], []
        prev_amount = None
        for y in year_domain:
            cur = yearly_amount_map.get(y, 0.0)
            yearly_values.append(cur)
            if prev_amount is None or prev_amount == 0:
                yearly_change_pct.append(None)
            else:
                yearly_change_pct.append(((cur - prev_amount) / abs(prev_amount)) * 100)
            prev_amount = cur

        anomaly_clauses, anomaly_params = [], []
        if direction == "up":
            anomaly_clauses.append("yoy_real_pct > 0")
        elif direction == "down":
            anomaly_clauses.append("yoy_real_pct < 0")
        if min_change_pct:
            try:
                anomaly_clauses.append("ABS(yoy_real_pct) >= ?")
                anomaly_params.append(float(min_change_pct))
            except ValueError:
                pass
        if not overview_mode:
            anomaly_clauses.append(f'"{focus_col}" = ?')
            anomaly_params.append(focus_value)
            if within_value != "all":
                anomaly_clauses.append(f'"{driver_col}" = ?')
                anomaly_params.append(within_value)
        anomaly_where = "WHERE " + " AND ".join(anomaly_clauses) if anomaly_clauses else ""
        anomaly_years = {
            int(r[0]) for r in con.execute(
                f"SELECT DISTINCT year FROM {view} {anomaly_where}",
                anomaly_params,
            ).fetchall()
            if r[0] is not None
        }
        anomaly_flags = [y in anomaly_years for y in year_domain]

        active_filters = []
        if focus_value != "all":
            active_filters.append({"label": rikid_dn(focus_col), "value": focus_value, "param": "focus_value"})
        if within_value != "all":
            active_filters.append({"label": rikid_dn(driver_col), "value": within_value, "param": "within_value"})
        if year != "all":
            active_filters.append({"label": "Ár", "value": str(year), "param": "year"})
        if direction != "all":
            active_filters.append({"label": "Stefna", "value": "Hækkun" if direction == "up" else "Lækkun", "param": "direction"})
        if min_change_pct:
            active_filters.append({"label": "Lágmarks breyting (%)", "value": str(min_change_pct), "param": "min_change_pct"})

        return render_template(
            "anomalies.html",
            source="rikid",
            data_loaded=True,
            focus=focus, focus_value=focus_value, focus_label=rikid_dn(focus_col),
            driver_label=rikid_dn(driver_col),
            within_value=within_value,
            row_label=row_label,
            overview_mode=overview_mode,
            focus_options=focus_options,
            year=year, direction=direction, min_change_pct=min_change_pct,
            years=years,
            active_filters=active_filters,
            summary_rows=summary_rows,
            context_rows=context_rows,
            overall_buyer_rows=overall_buyer_rows,
            overall_type_rows=overall_type_rows,
            anomaly_rows=anomaly_rows,
            yearly_labels=[str(y) for y in year_domain],
            yearly_values=yearly_values,
            yearly_avg_abs_pct=yearly_change_pct,
            anomaly_flags=anomaly_flags,
            limit=limit,
            dn=rikid_dn,
        )

    @app.route("/rikid/reports")
    def rikid_reports():
        year = request.args.get("year", "all").rstrip("*")  # Remove asterisk indicator
        mode = request.args.get("mode", "tegund")  # tegund | buyer
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"

        con = open_rikid_con(RIKID_DATA)
        if con is None:
            return render_template("reports.html", source="rikid", data_loaded=False,
                                   error=f"Gögn finnast ekki: {RIKID_DATA}")

        where, params = build_where([("year", year if year != "all" else None)])

        # Add filter for corrections
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        years_raw = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        years = [str(y) + ("*" if i == 0 else "") for i, y in enumerate(years_raw)]

        group_col = '"Tegund"' if mode == "tegund" else '"Kaupandi"'

        # Yearly totals for chart
        yearly = con.execute(
            f'SELECT year, SUM({RIKID_AMOUNT}) FROM data GROUP BY year ORDER BY year'
        ).fetchall()

        # Top groups
        top_rows = con.execute(
            f'SELECT {group_col}, SUM({RIKID_AMOUNT}) AS s, COUNT(*) AS n, '
            f'SUM(CASE WHEN {RIKID_AMOUNT} > 0 THEN {RIKID_AMOUNT} END) AS pos, '
            f'SUM(CASE WHEN {RIKID_AMOUNT} < 0 THEN {RIKID_AMOUNT} END) AS neg '
            f'FROM data {where} GROUP BY {group_col} ORDER BY ABS(s) DESC LIMIT 30',
            params,
        ).fetchall()

        # YoY for top groups (last two years)
        years_all = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data ORDER BY year DESC LIMIT 2"
        ).fetchall()]
        yoy_rows = []
        if len(years_all) >= 2:
            cur_y, prev_y = years_all[0], years_all[1]
            yoy_rows = con.execute(
                f'SELECT a.g, a.cur, b.prev, a.cur - b.prev AS chg, '
                f'(a.cur - b.prev) / NULLIF(ABS(b.prev), 0) * 100 AS pct '
                f'FROM '
                f'(SELECT {group_col} AS g, SUM({RIKID_AMOUNT}) AS cur FROM data WHERE year = ? GROUP BY {group_col}) a '
                f'JOIN '
                f'(SELECT {group_col} AS g, SUM({RIKID_AMOUNT}) AS prev FROM data WHERE year = ? GROUP BY {group_col}) b '
                f'ON a.g = b.g ORDER BY ABS(chg) DESC LIMIT 20',
                [cur_y, prev_y],
            ).fetchall()
            yoy_rows = [
                {"group": r[0], "cur": fmt(r[1]), "prev": fmt(r[2]),
                 "change": fmt(r[3]), "pct": fmt_pct(r[4]),
                 "direction": "up" if (r[3] or 0) >= 0 else "down"}
                for r in yoy_rows
            ]

        report_rows = [
            {"group": r[0], "sum": fmt(r[1]), "count": r[2],
             "pos": fmt(r[3]), "neg": fmt(r[4])}
            for r in top_rows
        ]

        return render_template(
            "reports.html",
            source="rikid",
            data_loaded=True,
            year=year, mode=mode, years=years,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            report_rows=report_rows,
            yoy_rows=yoy_rows,
            yoy_years=years_all[:2] if len(years_all) >= 2 else [],
            mode_label="Tegund" if mode == "tegund" else "Stofnun",
            dn=rikid_dn,
        )

    # ===========================================================================
    # REYKJAVIK
    # ===========================================================================

    @app.route("/reykjavik/")
    def rkv_explorer():
        year = request.args.get("year", "all").rstrip("*")  # Remove asterisk indicator
        tegund0 = request.args.get("tegund", request.args.get("tegund0", "all"))
        samtala0 = request.args.get("buyer", request.args.get("samtala0", "all"))
        samtala1 = request.args.get("samtala1", "all")
        seller = request.args.get("seller", "all")
        sign = request.args.get("sign", "all")  # "pos", "neg", or "all"
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        page = max(1, int(request.args.get("page", 1)))
        offset = (page - 1) * limit

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("explorer.html", source="reykjavik", data_loaded=False,
                                   error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        where, params = build_where([
            ("year", year if year != "all" else None),
            ("tegund0", tegund0 if tegund0 != "all" else None),
            ("samtala0", samtala0 if samtala0 != "all" else None),
            ("samtala1", samtala1 if samtala1 != "all" else None),
        ])
        chart_where, chart_params = build_where([
            ("tegund0", tegund0 if tegund0 != "all" else None),
            ("samtala0", samtala0 if samtala0 != "all" else None),
            ("samtala1", samtala1 if samtala1 != "all" else None),
        ])
        if seller != "all":
            seller_clause = f"{RKV_SUPPLIER_EXPR} = ?"
            where += f" AND {seller_clause}" if where else f"WHERE {seller_clause}"
            chart_where += f" AND {seller_clause}" if chart_where else f"WHERE {seller_clause}"
            params.append(seller)
            chart_params.append(seller)

        # Add filter for sign (positive/negative)
        if sign == "pos":
            sign_clause = f"{RKV_AMOUNT_EXPR} > 0"
            where += f" AND {sign_clause}" if where else f"WHERE {sign_clause}"
            chart_where += f" AND {sign_clause}" if chart_where else f"WHERE {sign_clause}"
        elif sign == "neg":
            sign_clause = f"{RKV_AMOUNT_EXPR} < 0"
            where += f" AND {sign_clause}" if where else f"WHERE {sign_clause}"
            chart_where += f" AND {sign_clause}" if chart_where else f"WHERE {sign_clause}"

        # Add filter for corrections
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"
            chart_where += " AND (is_correction = FALSE OR is_correction IS NULL)" if chart_where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        years_raw = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        tegund0_opts = [r[0] for r in con.execute(
            "SELECT DISTINCT tegund0 FROM data WHERE tegund0 IS NOT NULL ORDER BY tegund0"
        ).fetchall()]

        samtala0_opts = [r[0] for r in con.execute(
            "SELECT DISTINCT samtala0 FROM data WHERE samtala0 IS NOT NULL ORDER BY samtala0"
        ).fetchall()]

        samtala1_opts = [r[0] for r in con.execute(
            "SELECT DISTINCT samtala1 FROM data WHERE samtala1 IS NOT NULL ORDER BY samtala1"
        ).fetchall()]

        # Yearly totals
        yearly = con.execute(
            f"SELECT year, SUM({RKV_AMOUNT_EXPR}) FROM data {chart_where} GROUP BY year ORDER BY year",
            chart_params,
        ).fetchall()

        # Expense type breakdown (tegund0)
        type_breakdown = con.execute(
            f"SELECT COALESCE(tegund0, 'Ótilgreint') AS tegund0, SUM({RKV_AMOUNT_EXPR}) AS s, COUNT(*) AS n "
            f"FROM data {where} GROUP BY tegund0 ORDER BY s DESC LIMIT 30",
            params,
        ).fetchall()
        buyer_breakdown = con.execute(
            f"SELECT samtala0, SUM({RKV_AMOUNT_EXPR}) AS s, COUNT(*) AS n "
            f"FROM data {where} GROUP BY samtala0 ORDER BY s DESC LIMIT 30",
            params,
        ).fetchall()
        seller_breakdown = con.execute(
            f"SELECT {RKV_SUPPLIER_EXPR} AS supplier_name, SUM({RKV_AMOUNT_EXPR}) AS s, COUNT(*) AS n "
            f"FROM data {where} GROUP BY supplier_name ORDER BY s DESC LIMIT 30",
            params,
        ).fetchall()

        # Totals
        tot = con.execute(
            f"SELECT COUNT(*) AS n, SUM({RKV_AMOUNT_EXPR}) AS s, "
            f"SUM(CASE WHEN {RKV_AMOUNT_EXPR} > 0 THEN {RKV_AMOUNT_EXPR} END) AS pos, "
            f"SUM(CASE WHEN {RKV_AMOUNT_EXPR} < 0 THEN {RKV_AMOUNT_EXPR} END) AS neg "
            f"FROM data {where}",
            params,
        ).fetchone()

        # Preview rows
        rows = con.execute(
            f"SELECT year, samtala0, samtala1, tegund0, tegund1, raun, {RKV_SUPPLIER_EXPR} AS supplier_name "
            f"FROM data {where} LIMIT {limit} OFFSET {offset}",
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "samtala0": r[1], "samtala1": r[2],
             "tegund0": r[3], "tegund1": r[4], "raun": fmt(float(r[5]) if r[5] else 0), "supplier_name": r[6]}
            for r in rows
        ]

        total_count = tot[0] if tot else 0
        total_pages = max(1, math.ceil(total_count / limit))
        active_filters = []
        if year != "all":
            active_filters.append({"label": "Ár", "value": str(year), "param": "year"})
        if tegund0 != "all":
            active_filters.append({"label": "Tegundaflokkur", "value": tegund0, "param": "tegund"})
        if samtala0 != "all":
            active_filters.append({"label": "Stofnun", "value": samtala0, "param": "buyer"})
        if samtala1 != "all":
            active_filters.append({"label": "Svið", "value": samtala1, "param": "samtala1"})
        if seller != "all":
            active_filters.append({"label": "VSK-heiti", "value": seller, "param": "seller"})
        if sign != "all":
            sign_label = "Jákvæðar" if sign == "pos" else "Neikvæðar"
            active_filters.append({"label": sign_label, "value": "", "param": "sign"})

        return render_template(
            "explorer.html",
            source="reykjavik",
            data_loaded=True,
            year=year, tegund=tegund0, buyer=samtala0, samtala1=samtala1, seller=seller, sign=sign,
            years=years_raw,
            tegund_opts=tegund0_opts,
            buyer_opts=samtala0_opts,
            samtala1_opts=samtala1_opts,
            tegund_label="Tegundaflokkur",
            buyer_label="Stofnun",
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            breakdown_sections=[
                {"title": "Sundurliðun eftir tegund (topp 30)", "label": "Tegund", "rows": type_breakdown, "filter_param": "tegund"},
                {"title": "Sundurliðun eftir kaupanda (topp 30)", "label": "Svið", "rows": buyer_breakdown, "filter_param": "buyer"},
                {"title": "Sundurliðun eftir seljanda (topp 30)", "label": "VSK-heiti", "rows": seller_breakdown, "filter_param": "seller"},
            ],
            totals={"count": tot[0], "sum": tot[1], "pos": tot[2], "neg": tot[3]} if tot else {},
            preview_rows=preview_rows,
            preview_cols=["year", "samtala0", "samtala1", "tegund0", "tegund1", "raun", "supplier_name"],
            page=page, limit=limit, total_pages=total_pages,
            active_filters=active_filters,
            dn=rkv_dn,
        )

    @app.route("/reykjavik/analysis")
    def rkv_analysis():
        focus = request.args.get("focus", request.args.get("group_by", "tegund"))
        focus_value = request.args.get("focus_value", "all")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        page = max(1, int(request.args.get("page", 1)))
        offset = (page - 1) * limit

        focus_col = "tegund0" if focus == "tegund" else "samtala0"
        if focus not in ("tegund", "buyer"):
            focus = "tegund"
            focus_col = "tegund0"

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("analysis.html", source="reykjavik", data_loaded=False,
                                   error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        where, params = build_where([(focus_col, focus_value if focus_value != "all" else None)])
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        focus_options = [r[0] for r in con.execute(
            f'SELECT DISTINCT "{focus_col}" FROM data WHERE "{focus_col}" IS NOT NULL ORDER BY "{focus_col}"'
        ).fetchall()]
        if focus_value != "all" and focus_value not in focus_options:
            focus_value = "all"

        yearly_selected = con.execute(
            f"WITH yearly AS ("
            f"  SELECT year, SUM({RKV_AMOUNT_EXPR}) AS s "
            f"  FROM data {where} GROUP BY year"
            f"), ch AS ("
            f"  SELECT year, s, "
            f"         CASE WHEN LAG(s) OVER (ORDER BY year) IS NULL OR LAG(s) OVER (ORDER BY year) = 0 THEN NULL "
            f"              ELSE ((s - LAG(s) OVER (ORDER BY year)) / ABS(LAG(s) OVER (ORDER BY year))) * 100 END AS change_pct "
            f"  FROM yearly"
            f") "
            f"SELECT year, s, change_pct FROM ch ORDER BY year",
            params,
        ).fetchall()

        avg_change = con.execute(
            f"WITH gy AS ("
            f'  SELECT "{focus_col}" AS g, year, SUM({RKV_AMOUNT_EXPR}) AS s '
            f'  FROM data WHERE "{focus_col}" IS NOT NULL '
            + ("AND (is_correction = FALSE OR is_correction IS NULL) " if not show_corrections else "")
            + f'  GROUP BY "{focus_col}", year'
            f"), ch AS ("
            f"  SELECT g, year, "
            f"         CASE WHEN LAG(s) OVER (PARTITION BY g ORDER BY year) IS NULL OR LAG(s) OVER (PARTITION BY g ORDER BY year) = 0 THEN NULL "
            f"              ELSE ((s - LAG(s) OVER (PARTITION BY g ORDER BY year)) / ABS(LAG(s) OVER (PARTITION BY g ORDER BY year))) * 100 END AS change_pct "
            f"  FROM gy"
            f") "
            f"SELECT year, AVG(change_pct) AS avg_change_pct "
            f"FROM ch WHERE change_pct IS NOT NULL GROUP BY year ORDER BY year"
        ).fetchall()
        avg_change_map = {int(r[0]): float(r[1]) for r in avg_change}

        change_rows = []
        for yr, total, chg_pct in yearly_selected:
            if yr is None:
                continue
            yr_i = int(yr)
            chg_pct_f = float(chg_pct) if chg_pct is not None else None
            avg_pct_f = avg_change_map.get(yr_i)
            diff_pct = (chg_pct_f - avg_pct_f) if (chg_pct_f is not None and avg_pct_f is not None) else None
            change_rows.append(
                {
                    "year": yr_i,
                    "total_raw": float(total) if total is not None else 0.0,
                    "total": fmt(total),
                    "change_pct_raw": chg_pct_f,
                    "change_pct": fmt_pct(chg_pct_f) if chg_pct_f is not None else "–",
                    "avg_change_pct_raw": avg_pct_f,
                    "avg_change_pct": fmt_pct(avg_pct_f) if avg_pct_f is not None else "–",
                    "diff_pct_raw": diff_pct,
                    "diff_pct": fmt_pct(diff_pct) if diff_pct is not None else "–",
                }
            )

        rows = con.execute(
            f"SELECT year, samtala0, samtala1, tegund0, tegund1, raun, fyrirtaeki "
            f"FROM data {where} LIMIT {limit} OFFSET {offset}",
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "samtala0": r[1], "samtala1": r[2],
             "tegund0": r[3], "tegund1": r[4], "raun": r[5], "fyrirtaeki": r[6]}
            for r in rows
        ]

        tot = con.execute(
            f"SELECT COUNT(*) AS n FROM data {where}",
            params,
        ).fetchone()
        total_count = int(tot[0]) if tot and tot[0] is not None else 0
        total_pages = max(1, math.ceil(total_count / limit))

        chart_years = [r["year"] for r in change_rows if r["change_pct_raw"] is not None]
        chart_selected_change = [r["change_pct_raw"] for r in change_rows if r["change_pct_raw"] is not None]
        chart_avg_change = [r["avg_change_pct_raw"] for r in change_rows if r["change_pct_raw"] is not None]
        chart_diff = [r["diff_pct_raw"] for r in change_rows if r["change_pct_raw"] is not None]

        latest = next((r for r in reversed(change_rows) if r["change_pct_raw"] is not None), None)
        active_filters = []
        if focus_value != "all":
            active_filters.append({"label": rkv_dn(focus_col), "value": focus_value, "param": "focus_value"})

        return render_template(
            "analysis.html",
            source="reykjavik",
            data_loaded=True,
            focus=focus,
            focus_value=focus_value,
            focus_label=rkv_dn(focus_col),
            focus_options=focus_options,
            chart_labels=[str(y) for y in chart_years],
            chart_selected_change=chart_selected_change,
            chart_avg_change=chart_avg_change,
            chart_diff=chart_diff,
            change_rows=change_rows,
            latest=latest,
            active_filters=active_filters,
            preview_rows=preview_rows,
            preview_cols=["year", "samtala0", "samtala1", "tegund0", "tegund1", "raun", "fyrirtaeki"],
            page=page, total_pages=total_pages,
            limit=limit,
            dn=rkv_dn,
        )

    @app.route("/reykjavik/anomalies")
    def rkv_anomalies():
        focus = request.args.get("focus")
        if not focus:
            legacy_group = request.args.get("group_col", "tegund0")
            focus = "buyer" if legacy_group in RKV_ORG_COLS else "tegund"
        focus_value = request.args.get("focus_value", "all")
        within_value = request.args.get("within_value", "all")
        year = request.args.get("year", "all")
        direction = request.args.get("direction", "all")
        min_change_pct = request.args.get("min_change_pct", request.args.get("min_change", ""))
        limit = max(1, min(500, int(request.args.get("limit", 50))))

        focus_col = "tegund0" if focus == "tegund" else "samtala0"
        driver_col = "samtala0" if focus == "tegund" else "tegund0"
        if focus not in ("tegund", "buyer"):
            focus = "tegund"
            focus_col = "tegund0"
            driver_col = "samtala0"

        con_f = open_con(REYKJAVIK_ANOMALIES, "anomalies")
        con_a = open_con(REYKJAVIK_ANOMALIES_ALL, "anomalies_all")

        if con_f is None and con_a is None:
            return render_template("anomalies.html", source="reykjavik", data_loaded=False,
                                   error=f"Anomaly-gögn finnast ekki: {REYKJAVIK_ANOMALIES}")

        con = con_f or con_a
        view = "anomalies" if con_f else "anomalies_all"

        cols_in_view = [r[1] for r in con.execute(f"PRAGMA table_info('{view}')").fetchall()]
        if focus_col not in cols_in_view:
            preferred = RKV_TYPE_COLS if focus == "tegund" else RKV_ORG_COLS
            for c in preferred:
                if c in cols_in_view:
                    focus_col = c
                    break
        if driver_col not in cols_in_view:
            preferred_driver = RKV_ORG_COLS if focus == "tegund" else RKV_TYPE_COLS
            for c in preferred_driver:
                if c in cols_in_view:
                    driver_col = c
                    break

        focus_options = [r[0] for r in con.execute(
            f'SELECT DISTINCT "{focus_col}" FROM {view} WHERE "{focus_col}" IS NOT NULL ORDER BY "{focus_col}"'
        ).fetchall()]
        if focus_value != "all" and focus_value not in focus_options:
            focus_value = "all"

        global_clauses, global_params = [], []
        if year != "all":
            global_clauses.append("year = ?")
            global_params.append(int(year))
        if direction == "up":
            global_clauses.append("yoy_real_pct > 0")
        elif direction == "down":
            global_clauses.append("yoy_real_pct < 0")
        if min_change_pct:
            try:
                global_clauses.append("ABS(yoy_real_pct) >= ?")
                global_params.append(float(min_change_pct))
            except ValueError:
                min_change_pct = ""
        global_where = "WHERE " + " AND ".join(global_clauses) if global_clauses else ""

        scoped_clauses = list(global_clauses)
        scoped_params = list(global_params)
        if focus_value != "all":
            scoped_clauses.append(f'"{focus_col}" = ?')
            scoped_params.append(focus_value)
        scoped_where = "WHERE " + " AND ".join(scoped_clauses) if scoped_clauses else ""

        years_raw = [r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM {view} WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        years = [str(y) for y in years_raw]

        def score_by_col(col: str, where_sql: str, where_params: list, top_n: int = 15) -> list[dict]:
            out = con.execute(
                f"WITH agg AS ("
                f'  SELECT "{col}" AS g, year, '
                f"         SUM(actual_real) AS actual_real, "
                f"         SUM(prior_real) AS prior_real, "
                f"         SUM(yoy_real_change) AS yoy_real_change "
                f"  FROM (SELECT * FROM {view} {where_sql}) t "
                f'  WHERE "{col}" IS NOT NULL '
                f"  GROUP BY g, year"
                f"), score AS ("
                f"  SELECT g, "
                f"         COUNT(*) AS years_flagged, "
                f"         SUM(ABS(yoy_real_change)) AS abs_change_sum, "
                f"         MAX(ABS(CASE WHEN prior_real = 0 OR prior_real IS NULL THEN NULL "
                f"                      ELSE (yoy_real_change / ABS(prior_real)) * 100 END)) AS max_abs_pct "
                f"  FROM agg GROUP BY g"
                f") "
                f"SELECT g, years_flagged, abs_change_sum, max_abs_pct "
                f"FROM score ORDER BY abs_change_sum DESC NULLS LAST LIMIT {top_n}",
                where_params,
            ).fetchall()
            return [
                {
                    "group": r[0],
                    "years_flagged": int(r[1]) if r[1] is not None else 0,
                    "avg_change_amount": fmt((float(r[2]) / float(r[1])) if (r[1] not in (None, 0) and r[2] is not None) else None),
                    "abs_change_sum": fmt(r[2]),
                    "max_abs_pct": fmt_pct(r[3]),
                }
                for r in out
            ]

        overview_mode = focus_value == "all"
        if overview_mode:
            within_value = "all"
        if overview_mode:
            summary_rows = []
            anomaly_rows = []
            overall_buyer_rows = score_by_col("samtala0", global_where, global_params, 20)
            overall_type_rows = score_by_col("tegund0", global_where, global_params, 20)
            row_label = ""
            context_rows = []
        else:
            overall_buyer_rows = []
            overall_type_rows = []
            summary_rows = score_by_col(driver_col, scoped_where, scoped_params, 15)
            valid_within = {r["group"] for r in summary_rows if r.get("group")}
            if within_value != "all" and within_value not in valid_within:
                within_value = "all"
            row_label = rkv_dn(driver_col)
            detail_clauses = list(scoped_clauses)
            detail_params = list(scoped_params)
            if within_value != "all":
                detail_clauses.append(f'"{driver_col}" = ?')
                detail_params.append(within_value)
            detail_where = "WHERE " + " AND ".join(detail_clauses) if detail_clauses else ""
            rows = con.execute(
                f"WITH agg AS ("
                f'  SELECT "{driver_col}" AS g, year, '
                f"         SUM(actual_real) AS actual_real, "
                f"         SUM(prior_real) AS prior_real, "
                f"         SUM(yoy_real_change) AS yoy_real_change "
                f"  FROM (SELECT * FROM {view} {detail_where}) t "
                f'  WHERE "{driver_col}" IS NOT NULL '
                f"  GROUP BY g, year"
                f") "
                f"SELECT g, year, actual_real, prior_real, yoy_real_change, "
                f"       CASE WHEN prior_real = 0 OR prior_real IS NULL THEN NULL "
                f"            ELSE (yoy_real_change / ABS(prior_real)) * 100 END AS yoy_real_pct "
                f"FROM agg "
                f"ORDER BY ABS(yoy_real_pct) DESC NULLS LAST LIMIT {limit}",
                detail_params,
            ).fetchall()
            anomaly_rows = [
                {
                    "group": r[0], "year": r[1],
                    "period": f"{int(r[1]) - 1} → {int(r[1])}" if r[1] is not None else "–",
                    "actual": fmt(r[2]), "prior": fmt(r[3]),
                    "change": fmt(r[4]), "pct": fmt_pct(r[5]),
                    "direction": "up" if (r[4] or 0) >= 0 else "down",
                }
                for r in rows
            ]

            anomaly_count_rows = con.execute(
                f'SELECT "{driver_col}" AS g, COUNT(DISTINCT year) AS n_anom_years '
                f"FROM (SELECT * FROM {view} {scoped_where}) t "
                f'WHERE "{driver_col}" IS NOT NULL '
                f'GROUP BY "{driver_col}"',
                scoped_params,
            ).fetchall()
            anomaly_count_map = {str(r[0]): int(r[1]) for r in anomaly_count_rows if r[0] is not None}

            con_main = open_con(REYKJAVIK_DATA)
            if con_main is not None:
                main_clauses = [f'"{focus_col}" = ?', "(is_correction = FALSE OR is_correction IS NULL)"]
                main_params = [focus_value]
                if year != "all":
                    main_clauses.append("year = ?")
                    main_params.append(int(year))
                main_where = "WHERE " + " AND ".join(main_clauses)
                context_raw = con_main.execute(
                    f'SELECT "{driver_col}" AS g, '
                    f'SUM({RKV_AMOUNT_EXPR}) AS total_amount, '
                    f'COUNT(DISTINCT year) AS years_with_spend '
                    f'FROM data {main_where} '
                    f'AND "{driver_col}" IS NOT NULL '
                    f'GROUP BY "{driver_col}" '
                    f'ORDER BY ABS(total_amount) DESC LIMIT 30',
                    main_params,
                ).fetchall()
                context_rows = [
                    {
                        "group": r[0],
                        "total_amount": fmt(r[1]),
                        "years_with_spend": int(r[2]) if r[2] is not None else 0,
                        "anomaly_years": anomaly_count_map.get(str(r[0]), 0),
                        "is_selected": within_value != "all" and r[0] == within_value,
                    }
                    for r in context_raw
                ]
            else:
                context_rows = []

        con_main = open_con(REYKJAVIK_DATA)
        if con_main is not None:
            year_domain = [int(r[0]) for r in con_main.execute(
                "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year"
            ).fetchall()]
            main_clauses = ["(is_correction = FALSE OR is_correction IS NULL)"]
            main_params: list = []
            if not overview_mode:
                main_clauses.append(f'"{focus_col}" = ?')
                main_params.append(focus_value)
                if within_value != "all":
                    main_clauses.append(f'"{driver_col}" = ?')
                    main_params.append(within_value)
            main_where = "WHERE " + " AND ".join(main_clauses)
            yearly_main = con_main.execute(
                f"SELECT year, SUM({RKV_AMOUNT_EXPR}) AS s FROM data {main_where} GROUP BY year ORDER BY year",
                main_params,
            ).fetchall()
            yearly_amount_map = {int(r[0]): float(r[1]) if r[1] is not None else 0.0 for r in yearly_main}
        else:
            year_domain = [int(r[0]) for r in con.execute(
                f"SELECT DISTINCT year FROM {view} WHERE year IS NOT NULL ORDER BY year"
            ).fetchall()]
            yearly_amount_map = {y: 0.0 for y in year_domain}

        yearly_values, yearly_change_pct = [], []
        prev_amount = None
        for y in year_domain:
            cur = yearly_amount_map.get(y, 0.0)
            yearly_values.append(cur)
            if prev_amount is None or prev_amount == 0:
                yearly_change_pct.append(None)
            else:
                yearly_change_pct.append(((cur - prev_amount) / abs(prev_amount)) * 100)
            prev_amount = cur

        anomaly_clauses, anomaly_params = [], []
        if direction == "up":
            anomaly_clauses.append("yoy_real_pct > 0")
        elif direction == "down":
            anomaly_clauses.append("yoy_real_pct < 0")
        if min_change_pct:
            try:
                anomaly_clauses.append("ABS(yoy_real_pct) >= ?")
                anomaly_params.append(float(min_change_pct))
            except ValueError:
                pass
        if not overview_mode:
            anomaly_clauses.append(f'"{focus_col}" = ?')
            anomaly_params.append(focus_value)
            if within_value != "all":
                anomaly_clauses.append(f'"{driver_col}" = ?')
                anomaly_params.append(within_value)
        anomaly_where = "WHERE " + " AND ".join(anomaly_clauses) if anomaly_clauses else ""
        anomaly_years = {
            int(r[0]) for r in con.execute(
                f"SELECT DISTINCT year FROM {view} {anomaly_where}",
                anomaly_params,
            ).fetchall()
            if r[0] is not None
        }
        anomaly_flags = [y in anomaly_years for y in year_domain]

        active_filters = []
        if focus_value != "all":
            active_filters.append({"label": rkv_dn(focus_col), "value": focus_value, "param": "focus_value"})
        if within_value != "all":
            active_filters.append({"label": rkv_dn(driver_col), "value": within_value, "param": "within_value"})
        if year != "all":
            active_filters.append({"label": "Ár", "value": str(year), "param": "year"})
        if direction != "all":
            active_filters.append({"label": "Stefna", "value": "Hækkun" if direction == "up" else "Lækkun", "param": "direction"})
        if min_change_pct:
            active_filters.append({"label": "Lágmarks breyting (%)", "value": str(min_change_pct), "param": "min_change_pct"})

        return render_template(
            "anomalies.html",
            source="reykjavik",
            data_loaded=True,
            focus=focus, focus_value=focus_value, focus_label=rkv_dn(focus_col),
            driver_label=rkv_dn(driver_col),
            within_value=within_value,
            row_label=row_label,
            overview_mode=overview_mode,
            focus_options=focus_options,
            year=year, direction=direction, min_change_pct=min_change_pct,
            years=years,
            active_filters=active_filters,
            summary_rows=summary_rows,
            context_rows=context_rows,
            overall_buyer_rows=overall_buyer_rows,
            overall_type_rows=overall_type_rows,
            anomaly_rows=anomaly_rows,
            yearly_labels=[str(y) for y in year_domain],
            yearly_values=yearly_values,
            yearly_avg_abs_pct=yearly_change_pct,
            anomaly_flags=anomaly_flags,
            limit=limit,
            dn=rkv_dn,
        )

    @app.route("/reykjavik/reports")
    def rkv_reports():
        year = request.args.get("year", "all").rstrip("*")  # Remove asterisk indicator
        mode = request.args.get("mode", "tegund")  # tegund | org
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("reports.html", source="reykjavik", data_loaded=False,
                                   error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        where, params = build_where([("year", year if year != "all" else None)])

        # Add filter for corrections
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        years_raw = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        years = [str(y) + ("*" if i == 0 else "") for i, y in enumerate(years_raw)]

        group_col = "tegund0" if mode == "tegund" else "samtala0"

        yearly = con.execute(
            f"SELECT year, SUM({RKV_AMOUNT_EXPR}) FROM data GROUP BY year ORDER BY year"
        ).fetchall()

        top_rows = con.execute(
            f'SELECT "{group_col}", SUM({RKV_AMOUNT_EXPR}) AS s, COUNT(*) AS n, '
            f'SUM(CASE WHEN {RKV_AMOUNT_EXPR} > 0 THEN {RKV_AMOUNT_EXPR} END) AS pos, '
            f'SUM(CASE WHEN {RKV_AMOUNT_EXPR} < 0 THEN {RKV_AMOUNT_EXPR} END) AS neg '
            f'FROM data {where} GROUP BY "{group_col}" ORDER BY ABS(s) DESC LIMIT 30',
            params,
        ).fetchall()

        years_desc = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC LIMIT 2"
        ).fetchall()]

        yoy_rows = []
        if len(years_desc) >= 2:
            cur_y, prev_y = years_desc[0], years_desc[1]
            yoy_rows = con.execute(
                f'SELECT a.g, a.cur, b.prev, a.cur - b.prev AS chg, '
                f'(a.cur - b.prev) / NULLIF(ABS(b.prev), 0) * 100 AS pct '
                f'FROM '
                f'(SELECT "{group_col}" AS g, SUM({RKV_AMOUNT_EXPR}) AS cur FROM data WHERE year = ? GROUP BY "{group_col}") a '
                f'JOIN '
                f'(SELECT "{group_col}" AS g, SUM({RKV_AMOUNT_EXPR}) AS prev FROM data WHERE year = ? GROUP BY "{group_col}") b '
                f'ON a.g = b.g ORDER BY ABS(chg) DESC LIMIT 20',
                [cur_y, prev_y],
            ).fetchall()
            yoy_rows = [
                {"group": r[0], "cur": fmt(r[1]), "prev": fmt(r[2]),
                 "change": fmt(r[3]), "pct": fmt_pct(r[4]),
                 "direction": "up" if (r[3] or 0) >= 0 else "down"}
                for r in yoy_rows
            ]

        report_rows = [
            {"group": r[0], "sum": fmt(r[1]), "count": r[2],
             "pos": fmt(r[3]), "neg": fmt(r[4])}
            for r in top_rows
        ]

        return render_template(
            "reports.html",
            source="reykjavik",
            data_loaded=True,
            year=year, mode=mode, years=years,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            report_rows=report_rows,
            yoy_rows=yoy_rows,
            yoy_years=years_desc[:2] if len(years_desc) >= 2 else [],
            mode_label="Tegundaflokkur" if mode == "tegund" else "Svið",
            dn=rkv_dn,
        )

    # =========================================================================
    # RIKID DRILLDOWNS
    # =========================================================================

    @app.route("/rikid/types")
    def rikid_types():
        year = request.args.get("year", "all")
        value = request.args.get("value", "")

        con = open_rikid_con(RIKID_DATA)
        if con is None:
            return render_template("drilldown.html", source="rikid", page_id="types",
                                   data_loaded=False, error=f"Gögn finnast ekki: {RIKID_DATA}")

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        where_base = "WHERE (is_correction = FALSE OR is_correction IS NULL)"
        if year != "all":
            where_base += f" AND year = {int(year) if year.isdigit() else 0}"

        if not value:
            # Level 0: all types
            rows = con.execute(
                f'SELECT "Tegund", SUM({RIKID_AMOUNT}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} GROUP BY "Tegund" ORDER BY total DESC'
            ).fetchall()
            return render_template("drilldown.html", source="rikid", page_id="types",
                                   data_loaded=True, level=0, selected_year=year, selected_value=value,
                                   years=years, rows=rows, explorer_base="")
        else:
            # Level 1: buyers for selected type
            rows = con.execute(
                f'SELECT "Kaupandi", SUM({RIKID_AMOUNT}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} AND "Tegund" = ? '
                f'GROUP BY "Kaupandi" ORDER BY total DESC',
                [value]
            ).fetchall()
            return render_template("drilldown.html", source="rikid", page_id="types",
                                   data_loaded=True, level=1, selected_year=year, selected_value=value,
                                   years=years, rows=rows, drill_label="Kaupandi",
                                   explorer_base="/rikid/", explorer_type_param="tegund", explorer_buyer_param="buyer")

    @app.route("/rikid/sellers")
    def rikid_sellers():
        year = request.args.get("year", "all")
        value = request.args.get("value", "")

        con = open_rikid_con(RIKID_DATA)
        if con is None:
            return render_template("drilldown.html", source="rikid", page_id="sellers",
                                   data_loaded=False, error=f"Gögn finnast ekki: {RIKID_DATA}")

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        where_base = "WHERE (is_correction = FALSE OR is_correction IS NULL)"
        if year != "all":
            where_base += f" AND year = {int(year) if year.isdigit() else 0}"

        if not value:
            # Level 0: all sellers
            rows = con.execute(
                f'SELECT "Birgi", SUM({RIKID_AMOUNT}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} GROUP BY "Birgi" ORDER BY total DESC'
            ).fetchall()
            return render_template("drilldown.html", source="rikid", page_id="sellers",
                                   data_loaded=True, level=0, selected_year=year, selected_value=value,
                                   years=years, rows=rows, explorer_base="")
        else:
            # Level 1: buyers for selected seller
            rows = con.execute(
                f'SELECT "Kaupandi", SUM({RIKID_AMOUNT}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} AND "Birgi" = ? '
                f'GROUP BY "Kaupandi" ORDER BY total DESC',
                [value]
            ).fetchall()
            return render_template("drilldown.html", source="rikid", page_id="sellers",
                                   data_loaded=True, level=1, selected_year=year, selected_value=value,
                                   years=years, rows=rows, drill_label="Kaupandi",
                                   explorer_base="/rikid/", explorer_type_param="seller", explorer_buyer_param="buyer")

    # =========================================================================
    # REYKJAVIK DRILLDOWNS
    # =========================================================================

    @app.route("/reykjavik/types")
    def rkv_types():
        year = request.args.get("year", "all")
        value = request.args.get("value", "")

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("drilldown.html", source="reykjavik", page_id="types",
                                   data_loaded=False, error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        where_base = "WHERE (is_correction = FALSE OR is_correction IS NULL)"
        if year != "all":
            where_base += f" AND year = {int(year) if year.isdigit() else 0}"

        if not value:
            # Level 0: all types
            rows = con.execute(
                f'SELECT tegund0, SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} GROUP BY tegund0 ORDER BY total DESC'
            ).fetchall()
            return render_template("drilldown.html", source="reykjavik", page_id="types",
                                   data_loaded=True, level=0, selected_year=year, selected_value=value,
                                   years=years, rows=rows, explorer_base="")
        else:
            # Level 1: organizations for selected type
            rows = con.execute(
                f'SELECT samtala0, SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} AND tegund0 = ? AND samtala0 IS NOT NULL '
                f'GROUP BY samtala0 ORDER BY total DESC',
                [value]
            ).fetchall()
            return render_template("drilldown.html", source="reykjavik", page_id="types",
                                   data_loaded=True, level=1, selected_year=year, selected_value=value,
                                   years=years, rows=rows, drill_label="Stofnun",
                                   explorer_base="/reykjavik/", explorer_type_param="tegund", explorer_buyer_param="buyer")

    @app.route("/reykjavik/sellers")
    def rkv_sellers():
        year = request.args.get("year", "all")
        value = request.args.get("value", "")

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("drilldown.html", source="reykjavik", page_id="sellers",
                                   data_loaded=False, error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        where_base = "WHERE (is_correction = FALSE OR is_correction IS NULL)"
        if year != "all":
            where_base += f" AND year = {int(year) if year.isdigit() else 0}"

        if not value:
            # Level 0: all sellers
            rows = con.execute(
                f'SELECT {RKV_SUPPLIER_EXPR} AS supplier_name, SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} GROUP BY supplier_name ORDER BY total DESC'
            ).fetchall()
            return render_template("drilldown.html", source="reykjavik", page_id="sellers",
                                   data_loaded=True, level=0, selected_year=year, selected_value=value,
                                   years=years, rows=rows, explorer_base="")
        else:
            # Level 1: drill by expense type hierarchy (tgr1)
            rows = con.execute(
                f'SELECT COALESCE(xtgr1, CAST(tgr1 AS VARCHAR), \'(óskráð)\') AS category, '
                f'SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt '
                f'FROM data {where_base} AND ({RKV_SUPPLIER_EXPR}) = ? '
                f'GROUP BY tgr1, xtgr1 ORDER BY total DESC',
                [value]
            ).fetchall()
            return render_template("drilldown.html", source="reykjavik", page_id="sellers",
                                   data_loaded=True, level=1, selected_year=year, selected_value=value,
                                   years=years, rows=rows, drill_label="Tegund útgjalda",
                                   explorer_base="/reykjavik/", explorer_type_param="seller", explorer_buyer_param="buyer")

    return app


app = create_app()


if __name__ == "__main__":
    create_app().run(debug=True)
