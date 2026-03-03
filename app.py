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
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        yearly = con.execute(
            'SELECT year, '
            'SUM(CASE WHEN "Upphæð línu" > 0 THEN "Upphæð línu" END) AS pos, '
            'SUM(CASE WHEN "Upphæð línu" < 0 THEN "Upphæð línu" END) AS neg, '
            'SUM("Upphæð línu") AS net '
            'FROM data GROUP BY year ORDER BY year'
        ).fetchall()
        latest = next((r for r in yearly if r[0] == years[0]), None) if years else None
        return {
            "available": True,
            "years": years,
            "yearly_labels": [str(r[0]) for r in yearly],
            "yearly_pos":    [float(r[1]) if r[1] is not None else 0 for r in yearly],
            "yearly_neg":    [float(r[2]) if r[2] is not None else 0 for r in yearly],
            "yearly_net":    [float(r[3]) if r[3] is not None else 0 for r in yearly],
            "latest_year":   years[0] if years else None,
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
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        amt = "TRY_CAST(REPLACE(REPLACE(raun, '.', ''), ',', '.') AS DOUBLE)"
        yearly = con.execute(
            f"SELECT year, "
            f"SUM(CASE WHEN {amt} > 0 THEN {amt} END) AS pos, "
            f"SUM(CASE WHEN {amt} < 0 THEN {amt} END) AS neg, "
            f"SUM({amt}) AS net "
            f"FROM data GROUP BY year ORDER BY year"
        ).fetchall()
        latest = next((r for r in yearly if r[0] == years[0]), None) if years else None
        return {
            "available": True,
            "years": years,
            "yearly_labels": [str(r[0]) for r in yearly],
            "yearly_pos":    [float(r[1]) if r[1] is not None else 0 for r in yearly],
            "yearly_neg":    [float(r[2]) if r[2] is not None else 0 for r in yearly],
            "yearly_net":    [float(r[3]) if r[3] is not None else 0 for r in yearly],
            "latest_year":   years[0] if years else None,
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

RKV_AMOUNT_EXPR = "TRY_CAST(REPLACE(REPLACE(raun, '.', ''), ',', '.') AS DOUBLE)"

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
    "samtala0": "Svið",
    "samtala1": "Deild",
    "samtala2_canonical": "Þjónusta",
    "samtala3": "Undireining",
    "raun": "Upphæð (raun)",
    "fyrirtaeki": "Fyrirtæki",
    "vm_numer": "VSK-númer",
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
        year = request.args.get("year", "all")
        tegund = request.args.get("tegund", "all")
        buyer = request.args.get("buyer", "all")
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
        ])

        # Add filter for corrections
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        tegund_opts = [r[0] for r in con.execute(
            f'SELECT DISTINCT "Tegund" FROM data WHERE "Tegund" IS NOT NULL ORDER BY "Tegund" LIMIT 300'
        ).fetchall()]

        buyer_opts = [r[0] for r in con.execute(
            f'SELECT DISTINCT "Kaupandi" FROM data WHERE "Kaupandi" IS NOT NULL ORDER BY "Kaupandi" LIMIT 300'
        ).fetchall()]

        # Yearly totals for chart
        yearly = con.execute(
            f'SELECT year, SUM({RIKID_AMOUNT}) FROM data {where} GROUP BY year ORDER BY year',
            params,
        ).fetchall()

        # Type breakdown (top 30)
        type_breakdown = con.execute(
            f'SELECT "Tegund", SUM({RIKID_AMOUNT}) AS s, COUNT(*) AS n '
            f'FROM data {where} GROUP BY "Tegund" ORDER BY s DESC LIMIT 30',
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

        return render_template(
            "explorer.html",
            source="rikid",
            data_loaded=True,
            year=year, tegund=tegund, buyer=buyer,
            years=years,
            tegund_opts=tegund_opts,
            buyer_opts=buyer_opts,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            type_breakdown=type_breakdown,
            totals={"count": tot[0], "sum": tot[1], "pos": tot[2], "neg": tot[3]} if tot else {},
            preview_rows=preview_rows,
            preview_cols=["year", "Kaupandi", "Birgi", "Tegund", "amount", "Dags"],
            page=page, limit=limit, total_pages=total_pages,
            dn=rikid_dn,
        )

    @app.route("/rikid/analysis")
    def rikid_analysis():
        group_by = request.args.get("group_by", "Tegund")
        year = request.args.get("year", "all")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))

        if group_by not in ("Tegund", "Kaupandi", "Birgi"):
            group_by = "Tegund"

        con = open_rikid_con(RIKID_DATA)
        if con is None:
            return render_template("analysis.html", source="rikid", data_loaded=False,
                                   error=f"Gögn finnast ekki: {RIKID_DATA}")

        where, params = build_where([("year", year if year != "all" else None)])

        # Add filter for corrections
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        years_all = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year"
        ).fetchall()]

        # Yearly totals per group (pivot-style: one row per group, columns per year)
        group_yearly = con.execute(
            f'SELECT "{group_by}", year, SUM({RIKID_AMOUNT}) AS s '
            f'FROM data WHERE "{group_by}" IS NOT NULL '
            + (f'AND year = {int(year)} ' if year != "all" else "")
            + f'GROUP BY "{group_by}", year ORDER BY "{group_by}", year',
        ).fetchall()

        # Aggregate into dict: {group: {year: total}}
        pivot: dict[str, dict[int, float]] = {}
        for grp, yr, s in group_yearly:
            pivot.setdefault(grp, {})[yr] = float(s) if s is not None else 0.0

        # Sort groups by total descending
        group_totals = {g: sum(v.values()) for g, v in pivot.items()}
        sorted_groups = sorted(pivot.keys(), key=lambda g: group_totals[g], reverse=True)[:limit]

        # Column totals per year (for tfoot)
        year_col_totals = {
            y: sum(pivot[g].get(y, 0) for g in sorted_groups)
            for y in years_all
        }

        # Overall yearly totals for chart
        yearly = con.execute(
            f'SELECT year, SUM({RIKID_AMOUNT}) FROM data GROUP BY year ORDER BY year'
        ).fetchall()

        group_options = ["Tegund", "Kaupandi", "Birgi"]
        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        return render_template(
            "analysis.html",
            source="rikid",
            data_loaded=True,
            group_by=group_by, year=year,
            group_options=group_options,
            years_all=years_all,
            years=years,
            sorted_groups=sorted_groups,
            pivot=pivot,
            group_totals=group_totals,
            year_col_totals=year_col_totals,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            limit=limit,
            dn=rikid_dn,
        )

    @app.route("/rikid/anomalies")
    def rikid_anomalies():
        year = request.args.get("year", "all")
        group_col = request.args.get("group_col", "Tegund")
        direction = request.args.get("direction", "all")
        min_change = request.args.get("min_change", "")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))

        if group_col not in ("Tegund", "Kaupandi", "Birgi"):
            group_col = "Tegund"

        con_f = open_con(RIKID_ANOMALIES, "anomalies")
        con_a = open_con(RIKID_ANOMALIES_ALL, "anomalies_all")

        if con_f is None and con_a is None:
            return render_template("anomalies.html", source="rikid", data_loaded=False,
                                   error=f"Anomaly-gögn finnast ekki: {RIKID_ANOMALIES}")

        # Use flagged if available, else all
        con = con_f or con_a
        view = "anomalies" if con_f else "anomalies_all"

        clauses, params = [], []
        if year != "all":
            clauses.append("year = ?"); params.append(int(year))
        if group_col:
            clauses.append(f'"{group_col}" IS NOT NULL')
        if direction == "up":
            clauses.append("yoy_real_change > 0")
        elif direction == "down":
            clauses.append("yoy_real_change < 0")
        if min_change:
            try:
                clauses.append("ABS(yoy_real_change) >= ?"); params.append(float(min_change))
            except ValueError:
                pass
        if not show_corrections:
            clauses.append("(is_correction = FALSE OR is_correction IS NULL)")
        where = "WHERE " + " AND ".join(clauses) if clauses else ""

        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM {view} WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        rows = con.execute(
            f'SELECT "{group_col}", year, actual_real, prior_real, yoy_real_change, yoy_real_pct '
            f'FROM {view} {where} ORDER BY ABS(yoy_real_change) DESC LIMIT {limit}',
            params,
        ).fetchall()

        anomaly_rows = [
            {
                "group": r[0], "year": r[1],
                "actual": fmt(r[2]), "prior": fmt(r[3]),
                "change": fmt(r[4]), "pct": fmt_pct(r[5]),
                "direction": "up" if (r[4] or 0) >= 0 else "down",
            }
            for r in rows
        ]

        # Yearly totals for overview chart (from main data if available)
        con_main = open_rikid_con(RIKID_DATA)
        yearly = []
        if con_main:
            yearly = con_main.execute(
                f'SELECT year, SUM({RIKID_AMOUNT}) FROM data GROUP BY year ORDER BY year'
            ).fetchall()

        return render_template(
            "anomalies.html",
            source="rikid",
            data_loaded=True,
            year=year, group_col=group_col, direction=direction, min_change=min_change,
            years=years,
            group_options=["Tegund", "Kaupandi", "Birgi"],
            anomaly_rows=anomaly_rows,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            limit=limit,
            dn=rikid_dn,
        )

    @app.route("/rikid/reports")
    def rikid_reports():
        year = request.args.get("year", "all")
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

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

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
        year = request.args.get("year", "all")
        tegund0 = request.args.get("tegund0", "all")
        samtala0 = request.args.get("samtala0", "all")
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
        ])

        # Add filter for corrections
        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        tegund0_opts = [r[0] for r in con.execute(
            "SELECT DISTINCT tegund0 FROM data WHERE tegund0 IS NOT NULL ORDER BY tegund0 LIMIT 200"
        ).fetchall()]

        samtala0_opts = [r[0] for r in con.execute(
            "SELECT DISTINCT samtala0 FROM data WHERE samtala0 IS NOT NULL ORDER BY samtala0 LIMIT 200"
        ).fetchall()]

        # Yearly totals
        yearly = con.execute(
            f"SELECT year, SUM({RKV_AMOUNT_EXPR}) FROM data {where} GROUP BY year ORDER BY year",
            params,
        ).fetchall()

        # Expense type breakdown (tegund0)
        type_breakdown = con.execute(
            f"SELECT tegund0, SUM({RKV_AMOUNT_EXPR}) AS s, COUNT(*) AS n "
            f"FROM data {where} GROUP BY tegund0 ORDER BY s DESC LIMIT 30",
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
            f"SELECT year, samtala0, samtala1, tegund0, tegund1, raun, fyrirtaeki "
            f"FROM data {where} LIMIT {limit} OFFSET {offset}",
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "samtala0": r[1], "samtala1": r[2],
             "tegund0": r[3], "tegund1": r[4], "raun": r[5], "fyrirtaeki": r[6]}
            for r in rows
        ]

        total_count = tot[0] if tot else 0
        total_pages = max(1, math.ceil(total_count / limit))

        return render_template(
            "explorer.html",
            source="reykjavik",
            data_loaded=True,
            year=year, tegund=tegund0, buyer=samtala0,
            years=years,
            tegund_opts=tegund0_opts,
            buyer_opts=samtala0_opts,
            tegund_label="Tegundaflokkur",
            buyer_label="Svið (stofnun)",
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            type_breakdown=type_breakdown,
            totals={"count": tot[0], "sum": tot[1], "pos": tot[2], "neg": tot[3]} if tot else {},
            preview_rows=preview_rows,
            preview_cols=["year", "samtala0", "samtala1", "tegund0", "tegund1", "raun", "fyrirtaeki"],
            page=page, limit=limit, total_pages=total_pages,
            dn=rkv_dn,
        )

    @app.route("/reykjavik/analysis")
    def rkv_analysis():
        group_by = request.args.get("group_by", "tegund0")
        year = request.args.get("year", "all")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))

        valid_groups = RKV_TYPE_COLS + RKV_ORG_COLS
        if group_by not in valid_groups:
            group_by = "tegund0"

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("analysis.html", source="reykjavik", data_loaded=False,
                                   error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        years_all = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year"
        ).fetchall()]

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        year_filter = f"AND year = {int(year)}" if year != "all" else ""
        correction_filter = "" if show_corrections else "AND (is_correction = FALSE OR is_correction IS NULL)"

        group_yearly = con.execute(
            f"SELECT \"{group_by}\", year, SUM({RKV_AMOUNT_EXPR}) AS s "
            f'FROM data WHERE "{group_by}" IS NOT NULL {year_filter} {correction_filter} '
            f'GROUP BY "{group_by}", year ORDER BY "{group_by}", year'
        ).fetchall()

        pivot: dict[str, dict[int, float]] = {}
        for grp, yr, s in group_yearly:
            pivot.setdefault(grp, {})[yr] = float(s) if s is not None else 0.0

        group_totals = {g: sum(v.values()) for g, v in pivot.items()}
        sorted_groups = sorted(pivot.keys(), key=lambda g: group_totals[g], reverse=True)[:limit]

        yearly = con.execute(
            f"SELECT year, SUM({RKV_AMOUNT_EXPR}) FROM data GROUP BY year ORDER BY year"
        ).fetchall()

        year_col_totals = {
            y: sum(pivot[g].get(y, 0) for g in sorted_groups)
            for y in years_all
        }

        return render_template(
            "analysis.html",
            source="reykjavik",
            data_loaded=True,
            group_by=group_by, year=year,
            group_options=valid_groups,
            years_all=years_all,
            years=years,
            sorted_groups=sorted_groups,
            pivot=pivot,
            group_totals=group_totals,
            year_col_totals=year_col_totals,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            limit=limit,
            dn=rkv_dn,
        )

    @app.route("/reykjavik/anomalies")
    def rkv_anomalies():
        year = request.args.get("year", "all")
        group_col = request.args.get("group_col", "tegund0")
        direction = request.args.get("direction", "all")
        min_change = request.args.get("min_change", "")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"
        limit = max(1, min(500, int(request.args.get("limit", 50))))

        valid_groups = RKV_TYPE_COLS + RKV_ORG_COLS
        if group_col not in valid_groups:
            group_col = "tegund0"

        con_f = open_con(REYKJAVIK_ANOMALIES, "anomalies")
        con_a = open_con(REYKJAVIK_ANOMALIES_ALL, "anomalies_all")

        if con_f is None and con_a is None:
            return render_template("anomalies.html", source="reykjavik", data_loaded=False,
                                   error=f"Anomaly-gögn finnast ekki: {REYKJAVIK_ANOMALIES}")

        con = con_f or con_a
        view = "anomalies" if con_f else "anomalies_all"

        clauses, params = [], []
        if year != "all":
            clauses.append("year = ?"); params.append(int(year))
        if group_col:
            clauses.append(f'"{group_col}" IS NOT NULL')
        if direction == "up":
            clauses.append("yoy_real_change > 0")
        elif direction == "down":
            clauses.append("yoy_real_change < 0")
        if min_change:
            try:
                clauses.append("ABS(yoy_real_change) >= ?"); params.append(float(min_change))
            except ValueError:
                pass
        if not show_corrections:
            clauses.append("(is_correction = FALSE OR is_correction IS NULL)")
        where = "WHERE " + " AND ".join(clauses) if clauses else ""

        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year FROM {view} WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        # Find which group column is actually in the anomalies parquet
        cols_in_view = [r[0] for r in con.execute(f"PRAGMA table_info('{view}')").fetchall()]
        # Fallback if group_col not in anomalies
        if group_col not in cols_in_view:
            # Try to find a matching column
            for candidate in valid_groups:
                if candidate in cols_in_view:
                    group_col = candidate
                    break

        rows = con.execute(
            f'SELECT "{group_col}", year, actual_real, prior_real, yoy_real_change, yoy_real_pct '
            f'FROM {view} {where} ORDER BY ABS(yoy_real_change) DESC LIMIT {limit}',
            params,
        ).fetchall()

        anomaly_rows = [
            {
                "group": r[0], "year": r[1],
                "actual": fmt(r[2]), "prior": fmt(r[3]),
                "change": fmt(r[4]), "pct": fmt_pct(r[5]),
                "direction": "up" if (r[4] or 0) >= 0 else "down",
            }
            for r in rows
        ]

        con_main = open_con(REYKJAVIK_DATA)
        yearly = []
        if con_main:
            yearly = con_main.execute(
                f"SELECT year, SUM({RKV_AMOUNT_EXPR}) FROM data GROUP BY year ORDER BY year"
            ).fetchall()

        return render_template(
            "anomalies.html",
            source="reykjavik",
            data_loaded=True,
            year=year, group_col=group_col, direction=direction, min_change=min_change,
            years=years,
            group_options=valid_groups,
            anomaly_rows=anomaly_rows,
            yearly_labels=[str(r[0]) for r in yearly],
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            limit=limit,
            dn=rkv_dn,
        )

    @app.route("/reykjavik/reports")
    def rkv_reports():
        year = request.args.get("year", "all")
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

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

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

    return app


app = create_app()


if __name__ == "__main__":
    create_app().run(debug=True)
