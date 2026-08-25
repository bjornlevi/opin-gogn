#!/usr/bin/env python3
"""Combined Open Accounts explorer — Rikið and Reykjavík."""
from __future__ import annotations

import csv
import io
import math
import os
import re
import unicodedata
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
RIKISREIKNINGUR_DATA = Path(
    os.getenv("RIKISREIKNINGUR_PARQUET",
              str(BASE_DIR / "data/rikisreikningur/processed/rikisreikningur_combined.parquet"))
)
RIKID_INSTITUTIONS_RECONCILIATION = Path(
    os.getenv("RIKID_INSTITUTIONS_RECONCILIATION",
              str(BASE_DIR / "data/rikisreikningur/processed/rikid_institutions_reconciliation.csv"))
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


def normalize_name(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " og ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\b(hf|ohf|ehf|ses|slf)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


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


def _rikisreikningur_headline() -> dict:
    con = open_con(RIKISREIKNINGUR_DATA)
    if con is None:
        return {"available": False}
    try:
        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        partial_years = {
            int(r[0]) for r in con.execute(
                "SELECT DISTINCT year FROM data WHERE is_partial_year = TRUE AND year IS NOT NULL"
            ).fetchall()
        }
        yearly = con.execute(
            "SELECT year, "
            "SUM(CASE WHEN amount > 0 THEN amount END) AS pos, "
            "SUM(CASE WHEN amount < 0 THEN amount END) AS neg, "
            "SUM(amount) AS net "
            "FROM data GROUP BY year ORDER BY year"
        ).fetchall()
        latest = next((r for r in yearly if r[0] == years[0]), None) if years else None
        return {
            "available": True,
            "years": years,
            "yearly_labels": [str(r[0]) + ("*" if int(r[0]) in partial_years else "") for r in yearly],
            "yearly_pos": [float(r[1]) if r[1] is not None else 0 for r in yearly],
            "yearly_neg": [float(r[2]) if r[2] is not None else 0 for r in yearly],
            "yearly_net": [float(r[3]) if r[3] is not None else 0 for r in yearly],
            "latest_year": (str(years[0]) + "*") if years and int(years[0]) in partial_years else (str(years[0]) if years else None),
            "latest_total": fmt(latest[3]) if latest else "–",
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


# Category mapping for Reykjavík wage reports
RKV_WAGE_CATEGORIES = {
    "Menntun": {
        "label": "Menntun",
        "color": "#4f46e5",
        "departments": [
            "Skóla- og frístundasvið",
        ]
    },
    "Félagsmál": {
        "label": "Félagsmál",
        "color": "#ec4899",
        "departments": [
            "Velferðarsvið",
        ]
    },
    "Menning og íþróttir": {
        "label": "Menning og íþróttir",
        "color": "#f59e0b",
        "departments": [
            "Menningar- og ferðamálasvið",
            "Menningar- og íþróttasvið RVK",
            "Íþrótta- og tómstundasvið",
        ]
    },
    "Stjórnsýsla": {
        "label": "Stjórnsýsla",
        "color": "#06b6d4",
        "departments": [
            "Skrifstofur miðlægrar stjórnsýslu",
            "Fjármála- og áhættustýringarsvið",
            "Mannauðs- og starfsumhverfissvið",
            "Mannauðs- og starfsþróunarsvið",
            "Þjónustu- og nýsköpunarsvið",
        ]
    },
    "Umhverfi": {
        "label": "Umhverfi og skipulag",
        "color": "#10b981",
        "departments": [
            "Umhverfis- og skipulagssvið Aðalsjóðs",
            "Umhverfis- og skipulagssvið aðalsjóður",
        ]
    },
    "Sameiginlegur": {
        "label": "Sameiginlegur kostnaður",
        "color": "#8b5cf6",
        "departments": [
            "Sameiginlegur kostnaður",
        ]
    }
}


def get_wage_category(department: str) -> str | None:
    """Map a department (samtala1) to a category."""
    for cat, info in RKV_WAGE_CATEGORIES.items():
        if department in info["departments"]:
            return cat
    return None


# ---------------------------------------------------------------------------
# Miðstöðvaskýrsla — hlutdeild málaflokks fatlaðs fólks í rekstri miðstöðvanna
#
# Ársuppgjörið hefur enga miðstöðvavídd (xeining* er tómt frá og með 2018), svo
# umfang miðstöðvanna er endurgert út frá samtala0 innan velferðarsviðs. Hver
# samtala0-gildi er flokkað í nákvæmlega einn af þremur flokkum hér að neðan;
# allt sem ekki er talið upp lendir í "midlaegt".
# ---------------------------------------------------------------------------

MIDST_FATLAD = [
    # Búsetuþjónusta fatlaðs fólks (eldri kóðar og G-/Þ-þyngdarflokkar frá 2023)
    "Húsnæðisúrræði fyrir fatlaða", "Búsetukjarnar", "Búsetukjarnar - sameiginlegt",
    "Sameiginlegt v. búsetuúrræða fatlaðra",
    "G-I", "G-II", "G-III", "G-IV",
    "Þ-I", "Þ-II", "Þ-III", "Þ-III A", "Þ-III B", "Þ-III C", "Þ-IV", "Þ-Börn",
    # Önnur þjónusta í málaflokknum
    "Dagþjónusta - Málefni fatlaðs fólks", "Skammtímadvöl", "Skammtímavistun",
    "Frekari liðveisla", "Stoðþjónusta (Frekari liðveisla)",
    "Stuðningsfjölskyldur - Fatlaðir", "Málefni fatlaðs fólks", "Túlkaþjónusta",
]

MIDST_ONNUR = [
    # Barnavernd / börn og fjölskyldur
    "Barnavernd Reykjavíkur", "Börn og fjölskyldur",
    # Heimaþjónusta og heimastuðningur (heitin breytast 2023)
    "Heimaþjónusta", "Heimaþjónusta - dagþjónusta",
    "Heimaþjónusta - kvöld- og helgarþjónusta",
    "Heimastuðningur", "Heimastuðningur - dagþjónusta",
    "Heimastuðningur - kvöld og helgarþjónusta",
    "Heimahjúkrun", "Endurhæfingarteymi", "Dagdeildir",
    # Virkni, ráðgjöf og félagsstarf
    "Fullorðnir (18-67) - Virkni og ráðgjöf", "Virkniverkefni", "Unglingasmiðjur",
    "Keðjan", "Félagsmiðstöðvar", "Öldrunarmál",
    "Stuðningsþjónusta", "Stuðningsfjölskyldur",
    "Stuðningsfjölskyldur og stuðningsþjónusta",
    # Húsnæði og búseta á vegum miðstöðva
    "Þjónustuíbúðir", "Húsnæðisaðstoð", "Húsnæði fyrir heimilislausa",
    "Áfangaheimili", "Þjónusta við flóttafólk og hælisleitendur",
    # Rekstur miðstöðvanna sjálfra
    "Rekstur miðstöðva", "Rekstur þjónustumiðstöðvar",
    "Sameiginlegt vegna þjónustumiðstöðva", "Þjónusta v/velferðarmála",
    "Rafræn þjónusta", "Framleiðslueldhús", "Þekkingarstöð",
    "Önnur úrræði", "Önnur starfsemi",
]

# NPA tilheyrir málaflokknum en er umsýslað miðlægt — valkvætt í teljara.
MIDST_NPA = "Notendastýrð persónuleg aðstoð (NPA)"

# Heimahjúkrun er ríkisfjármögnuð — valkvætt úr nefnara.
MIDST_RIKISFJARMOGNUD = "Heimahjúkrun"

MIDST_MAELINGAR = {
    "gjold": ("Rekstrargjöld (brúttó)",
              "SUM(CASE WHEN tegund1 = 'Rekstrargjöld' THEN raun END)"),
    # 2018 notar sameinaðan lið "Laun og launatengd gjöld"; frá 2019 er honum skipt.
    "laun": ("Laun og launatengd gjöld",
             "SUM(CASE WHEN tegund0 IN ('Laun', 'Launatengd gjöld', "
             "'Laun og launatengd gjöld') THEN raun END)"),
    "netto": ("Nettó (gjöld að frádregnum tekjum)", "SUM(raun)"),
}

# Launavísitala, ársmeðaltöl. Heimild: Hagstofa Íslands, LAU04200.
LAUNAVISITALA = {
    2014: 483.5, 2015: 518.2, 2016: 577.1, 2017: 616.6, 2018: 656.4,
    2019: 688.5, 2020: 732.0, 2021: 792.7, 2022: 858.4, 2023: 942.4,
    2024: 1004.6, 2025: 1084.0,
}


def midst_flokkur(samtala0: str, telja_npa: bool) -> str:
    """Flokka samtala0-gildi í fatlad / midstod / midlaegt."""
    if samtala0 in MIDST_FATLAD:
        return "fatlad"
    if samtala0 == MIDST_NPA:
        return "fatlad" if telja_npa else "midlaegt"
    if samtala0 in MIDST_ONNUR:
        return "midstod"
    return "midlaegt"


# ===========================================================================
# RIKISREIKNINGUR
# ===========================================================================

RIKISREIKNINGUR_AMOUNT = "amount"
RIKISREIKNINGUR_DISPLAY = {
    "year": "Ár",
    "Timabil": "Tímabil",
    "RaduneytiHeiti": "Ráðuneyti",
    "StofnunHeiti": "Stofnun",
    "FjarlagavidfangHeiti": "Fjárlagaliður",
    "MalefnasvidHeiti": "Málefnasvið",
    "MalaflokkurNumerOgHeiti": "Málaflokkur",
    "TegundL2Heiti": "Tegund L2",
    "TegundL3Heiti": "Tegund L3",
    "TegundHeiti": "Tegund",
    "amount": "Upphæð",
}


def rikisreikningur_dn(col: str) -> str:
    return RIKISREIKNINGUR_DISPLAY.get(col, col)


RIKID_COMPARISON_BUCKET_SQL = """
CASE
    WHEN lower("Tegund") LIKE '%leiga%' OR lower("Tegund") LIKE '%húsnæði%' THEN 'Húsnæði og leiga'
    WHEN lower("Tegund") LIKE '%þjónust%' OR lower("Tegund") LIKE '%sérfræði%' OR lower("Tegund") LIKE '%verkkaup%'
      OR lower("Tegund") LIKE '%verkfræð%' OR lower("Tegund") LIKE '%lögfræð%' OR lower("Tegund") LIKE '%rannsóknarstof%'
      OR lower("Tegund") LIKE '%öryggisgæsla%' OR lower("Tegund") LIKE '%ræsting%' OR lower("Tegund") LIKE '%sjúkraflutn%'
      OR lower("Tegund") LIKE '%tölvuvinnsla%' THEN 'Þjónusta og ráðgjöf'
    WHEN lower("Tegund") LIKE '%mannvirkja%' OR lower("Tegund") LIKE '%viðhald%' OR lower("Tegund") LIKE '%rafverk%'
      OR lower("Tegund") LIKE '%tréverk%' OR lower("Tegund") LIKE '%verkstæði%' OR lower("Tegund") LIKE '%múrverk%'
      OR lower("Tegund") LIKE '%vegir%' THEN 'Framkvæmdir og viðhald'
    WHEN lower("Tegund") LIKE '%lyf%' OR lower("Tegund") LIKE '%prófefni%' OR lower("Tegund") LIKE '%matv%'
      OR lower("Tegund") LIKE '%einnota%' OR lower("Tegund") LIKE '%sjúkrahúsvörur%' OR lower("Tegund") LIKE '%rafmagn%'
      OR lower("Tegund") LIKE '%heitt vatn%' OR lower("Tegund") LIKE '%fasteignagjöld%' THEN 'Vörur, lyf og rekstrarinnkaup'
    WHEN lower("Tegund") LIKE '%hugbúnaður%' OR lower("Tegund") LIKE '%hugbúnaðargerð%' OR lower("Tegund") LIKE '%tæki%'
      OR lower("Tegund") LIKE '%áhöld%' OR lower("Tegund") LIKE '%eignir%' OR lower("Tegund") LIKE '%farartæki%' THEN 'Tæki, hugbúnaður og eignir'
    WHEN lower("Tegund") LIKE 'til %' OR lower("Tegund") LIKE '%millifærsl%' OR lower("Tegund") LIKE '%vsk%'
      OR lower("Tegund") LIKE '%ríkissjóður%' OR lower("Tegund") LIKE '%ríkisstofnanir%' OR lower("Tegund") LIKE '%lánastofnana%' THEN 'Tilfærslur, skattar og uppgjör'
    ELSE 'Annað'
END
"""

RIKISREIKNINGUR_COMPARISON_BUCKET_SQL = """
CASE
    WHEN lower("TegundHeiti") LIKE '%grunnstörf%' OR lower("TegundHeiti") LIKE '%yfirvinna%' OR lower("TegundHeiti") LIKE '%vaktaálag%'
      OR lower("TegundHeiti") LIKE '%dagvinnu%' OR lower("TegundHeiti") LIKE '%tímakaup%' OR lower("TegundHeiti") LIKE '%orlof%'
      OR lower("TegundHeiti") LIKE '%aukagreiðsl%' OR lower("TegundHeiti") LIKE '%launatengd%' THEN 'Laun og launatengd gjöld'
    WHEN lower("TegundHeiti") LIKE 'til %' OR lower("TegundHeiti") LIKE '%hluti sveitarfélaga%' OR lower("TegundHeiti") LIKE '%hluti rétthafa%'
      OR lower("TegundHeiti") LIKE '%framlag%' THEN 'Tilfærslur og framlög'
    WHEN lower("TegundHeiti") LIKE '%vaxta%' OR lower("TegundHeiti") LIKE '%verðbóta%' OR lower("TegundHeiti") LIKE '%skatt%'
      OR lower("TegundHeiti") LIKE '%virðisaukaskatt%' OR lower("TegundHeiti") LIKE '% vsk %' OR lower("TegundHeiti") LIKE 'egr. vsk%'
      OR lower("TegundHeiti") LIKE '%fjármagn%' OR lower("TegundHeiti") LIKE '%ríkisábyrgða%' OR lower("TegundHeiti") LIKE '%lífeyrisskuldbinding%'
      OR lower("TegundHeiti") LIKE '%virðisrýrnun%' OR lower("TegundHeiti") LIKE '%afskrif%' THEN 'Fjármagnsliðir, skattar og uppgjör'
    WHEN lower("TegundHeiti") LIKE '%leiga%' OR lower("TegundHeiti") LIKE '%húseignir%' THEN 'Húsnæði og leiga'
    WHEN lower("TegundHeiti") LIKE '%þjónust%' OR lower("TegundHeiti") LIKE '%sérfræði%' OR lower("TegundHeiti") LIKE '%verkkaup%'
      OR lower("TegundHeiti") LIKE '%verkfræð%' OR lower("TegundHeiti") LIKE '%lögfræð%' OR lower("TegundHeiti") LIKE '%rannsóknarstof%'
      OR lower("TegundHeiti") LIKE '%öryggisgæsla%' OR lower("TegundHeiti") LIKE '%ræsting%' OR lower("TegundHeiti") LIKE '%sjúkraflutn%'
      OR lower("TegundHeiti") LIKE '%tölvuvinnsla%' THEN 'Þjónusta og ráðgjöf'
    WHEN lower("TegundHeiti") LIKE '%mannvirkja%' OR lower("TegundHeiti") LIKE '%viðhald%' OR lower("TegundHeiti") LIKE '%rafverk%'
      OR lower("TegundHeiti") LIKE '%tréverk%' OR lower("TegundHeiti") LIKE '%verkstæði%' OR lower("TegundHeiti") LIKE '%múrverk%'
      OR lower("TegundHeiti") LIKE '%vegir%' THEN 'Framkvæmdir og viðhald'
    WHEN lower("TegundHeiti") LIKE '%lyf%' OR lower("TegundHeiti") LIKE '%prófefni%' OR lower("TegundHeiti") LIKE '%matv%'
      OR lower("TegundHeiti") LIKE '%einnota%' OR lower("TegundHeiti") LIKE '%rafmagn%' OR lower("TegundHeiti") LIKE '%heitt vatn%'
      OR lower("TegundHeiti") LIKE '%sorphreinsun%' OR lower("TegundHeiti") LIKE '%fasteignagjöld%' THEN 'Vörur, lyf og rekstrarinnkaup'
    WHEN lower("TegundHeiti") LIKE '%hugbúnaður%' OR lower("TegundHeiti") LIKE '%hugbúnaðargerð%' OR lower("TegundHeiti") LIKE '%tæki%'
      OR lower("TegundHeiti") LIKE '%áhöld%' OR lower("TegundHeiti") LIKE '%eignir%' OR lower("TegundHeiti") LIKE '%farartæki%' THEN 'Tæki, hugbúnaður og eignir'
    ELSE 'Annað'
END
"""

RIKISREIKNINGUR_WAGE_BUCKET_SQL = """
CASE
    WHEN "TegundL2Heiti" <> 'Launagjöld' THEN NULL
    WHEN "TegundL3Heiti" = 'Dagvinnulaun' THEN 'Kjarna-laun'
    WHEN "TegundL3Heiti" IN ('Yfirvinna', 'Vaktaálagsgreiðslur', 'Aukagreiðslur') THEN 'Yfirvinna og álag'
    WHEN "TegundL3Heiti" = 'Launatengd gjöld' THEN 'Launatengd gjöld'
    WHEN "TegundL3Heiti" = 'Breyting á orlofsskuldbindingu' THEN 'Orlofsskuldbinding'
    WHEN lower("TegundHeiti") LIKE '%lífeyrisskuldbinding%'
      OR lower("TegundHeiti") LIKE '%lifeyrisskuldbinding%'
    THEN 'Lífeyrisskuldbindingar'
    WHEN "TegundL3Heiti" = 'Starfsmannakostnaður' THEN 'Starfsmannakostnaður'
    WHEN lower("TegundHeiti") LIKE '%lækkun launaliða%'
      OR lower("TegundHeiti") LIKE '%ábyrgðasjóðs launa%'
      OR lower("TegundHeiti") LIKE '%ábyrgðagjald atvinnurekenda vegna launa%'
      OR lower("TegundHeiti") LIKE '%umboðslaun%'
      OR lower("TegundHeiti") LIKE '%sölulaun%'
      OR lower("TegundHeiti") LIKE '%innheimtulaun%'
      OR lower("TegundHeiti") LIKE '%sjómannslaun%'
      OR lower("TegundHeiti") LIKE '%framtölum og staðgreiðsluskrá%'
    THEN 'Mótfærslur og leiðréttingar'
    WHEN "TegundL3Heiti" IN ('Ræsting', 'Önnur launagjöld ótalin annars staðar') THEN 'Annað launatengt'
    WHEN lower("TegundHeiti") LIKE '%laun%'
      OR lower("TegundHeiti") LIKE '%yfirvinna%'
      OR lower("TegundHeiti") LIKE '%vakta%'
      OR lower("TegundHeiti") LIKE '%orlof%'
      OR lower("TegundHeiti") LIKE '%tímakaup%'
      OR lower("TegundHeiti") LIKE '%timakaup%'
      OR lower("TegundHeiti") LIKE '%lífeyr%'
      OR lower("TegundHeiti") LIKE '%lifeyr%'
    THEN 'Annað launatengt'
    ELSE NULL
END
"""

RIKISREIKNINGUR_WAGE_BUCKET_ORDER = [
    "Kjarna-laun",
    "Yfirvinna og álag",
    "Orlofsskuldbinding",
    "Launatengd gjöld",
    "Lífeyrisskuldbindingar",
    "Starfsmannakostnaður",
    "Mótfærslur og leiðréttingar",
    "Annað launatengt",
]

COMPARABLE_BUCKETS = {
    "Húsnæði og leiga",
    "Þjónusta og ráðgjöf",
    "Framkvæmdir og viðhald",
    "Vörur, lyf og rekstrarinnkaup",
    "Tæki, hugbúnaður og eignir",
}

RIKIS_TO_RIKID_ALIASES = {
    "Landlæknir": ("Embætti landlæknis", "alias"),
    "Gljúfrasteinn - Hús skáldsins": ("Gljúfrasteinn: hús skáldsins", "alias"),
    "Hæstiréttur": ("Hæstiréttur Íslands", "alias"),
    "Framhaldsskólinn í A-Skaftafellssýslu": ("Framhaldsskólinn í Austur-Skaftafellssýslu", "alias"),
    "Sjúkrahúsið á Akureyri": ("Sjúkrahúsið Akureyri", "alias"),
    "Tilraunastöð Háskólans að Keldum": ("Tilraunastöð Háskóla Íslands í meinafræði að Keldum", "alias"),
    "Vegagerðin, rekstur": ("Vegagerðin", "alias"),
    "Náttúrufræðistofnun Íslands": ("Náttúrufræðistofnun", "alias"),
    "Mannvirkjastofnun": ("Húsnæðis-, mannvirkja- og skipulagsstofnun", "predecessor"),
    "Skipulagsstofnun": ("Húsnæðis-, mannvirkja- og skipulagsstofnun", "predecessor"),
    "Ríkisskattstjóri": ("Skatturinn", "predecessor"),
    "Skattrannsóknarstjóri ríkisins": ("Skatturinn", "predecessor"),
    "Héraðsdómstólar": ("Héraðsdómar", "predecessor"),
    "Dómstólasýslan": ("Héraðsdómar", "predecessor"),
    "Sýslumaður Austurlands": ("Sýslumaðurinn á Austurlandi", "alias"),
    "Sýslumaður höfuðborgarsvæðisins": ("Sýslumaðurinn á Höfuðborgarsvæðinu", "alias"),
    "Sýslumaður Norðurlands eystra": ("Sýslumaðurinn á Norðurlandi eystra", "alias"),
    "Sýslumaður Norðurlands vestra": ("Sýslumaðurinn á Norðurlandi vestra", "alias"),
    "Sýslumaður Suðurlands": ("Sýslumaðurinn á Suðurlandi", "alias"),
    "Sýslumaður Suðurnesja": ("Sýslumaðurinn á Suðurnesjum", "alias"),
    "Sýslumaður Vestfjarða": ("Sýslumaðurinn á Vestfjörðum", "alias"),
    "Sýslumaður Vesturlands": ("Sýslumaðurinn á Vesturlandi", "alias"),
    "Sýslumaður Vestmannaeyja": ("Sýslumaðurinn í Vestmannaeyjum", "alias"),
}


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
        rikisreikningur_stat = _rikisreikningur_headline()
        return render_template("home.html", rikid=rikid_stat, reykjavik=rkv_stat, rikisreikningur=rikisreikningur_stat)

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
            f'SELECT year, "Kaupandi", "Birgi", "Tegund", {RIKID_AMOUNT} AS amount, "Dags.greiðslu", "Númer reiknings" '
            f'FROM data {where} ORDER BY "Dags.greiðslu" DESC LIMIT {limit} OFFSET {offset}',
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "Kaupandi": r[1], "Birgi": r[2], "Tegund": r[3],
             "amount_raw": r[4], "amount": fmt(r[4]), "Dags": str(r[5])[:10] if r[5] else "",
             "upphæð_fmt": fmt(r[4]), "Dags.greiðslu": str(r[5])[:10] if r[5] else "", "Númer reiknings": r[6] or "",
             "_source": "rikid"}
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

    @app.route("/rikid/download")
    def rikid_explorer_download():
        """Download rikid explorer records as CSV."""
        year = request.args.get("year", "all").rstrip("*")
        tegund = request.args.get("tegund", "all")
        buyer = request.args.get("buyer", "all")
        seller = request.args.get("seller", "all")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"

        con = open_rikid_con(RIKID_DATA)
        if con is None:
            return "Data not found", 404

        where, params = build_where([
            ("year", year if year != "all" else None),
            ("Tegund", tegund if tegund != "all" else None),
            ("Kaupandi", buyer if buyer != "all" else None),
            ("Birgi", seller if seller != "all" else None),
        ])

        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        # Get all records (limit to 100k for safety)
        rows = con.execute(
            f"SELECT * FROM data {where} ORDER BY \"Dags.greiðslu\" DESC LIMIT 100000",
            params,
        ).fetchall()

        if not rows:
            return "No records found", 404

        # Get column names
        columns = [desc[0] for desc in con.description]

        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=rikid_records.csv"}
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
            f'SELECT year, "Kaupandi", "Birgi", "Tegund", {RIKID_AMOUNT} AS amount, "Dags.greiðslu", "Númer reiknings" '
            f'FROM data {where} ORDER BY "Dags.greiðslu" DESC LIMIT {limit} OFFSET {offset}',
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "Kaupandi": r[1], "Birgi": r[2], "Tegund": r[3],
             "amount_raw": r[4], "amount": fmt(r[4]), "Dags": str(r[5])[:10] if r[5] else "",
             "upphæð_fmt": fmt(r[4]), "Dags.greiðslu": str(r[5])[:10] if r[5] else "", "Númer reiknings": r[6] or "",
             "_source": "rikid"}
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
            f"SELECT year, samtala0, samtala1, tegund0, tegund1, raun, {RKV_SUPPLIER_EXPR} AS supplier_name, "
            f"samtala2_canonical, samtala3, tegund2, tegund3, vm_nafn, fyrirtaeki, CAST(vm_numer AS VARCHAR) AS vm_numer, "
            f"arsfjordungur, fjarfesting "
            f"FROM data {where} LIMIT {limit} OFFSET {offset}",
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "samtala0": r[1], "samtala1": r[2],
             "tegund0": r[3], "tegund1": r[4], "raun": fmt(float(r[5]) if r[5] else 0), "supplier_name": r[6],
             "samtala2_canonical": r[7] or "", "samtala3": r[8] or "", "tegund2": r[9] or "", "tegund3": r[10] or "",
             "raun_fmt": fmt(float(r[5]) if r[5] else 0), "vm_nafn": r[11] or "", "fyrirtaeki": r[12] or "",
             "vm_numer": r[13] or "", "arsfjordungur": r[14] or "", "fjarfesting": r[15] or "",
             "_source": "rkv"}
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

    @app.route("/reykjavik/download")
    def rkv_explorer_download():
        """Download reykjavik explorer records as CSV."""
        year = request.args.get("year", "all").rstrip("*")
        tegund0 = request.args.get("tegund", request.args.get("tegund0", "all"))
        samtala0 = request.args.get("buyer", request.args.get("samtala0", "all"))
        samtala1 = request.args.get("samtala1", "all")
        seller = request.args.get("seller", "all")
        show_corrections = request.args.get("show_corrections", "false").lower() == "true"

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return "Data not found", 404

        where, params = build_where([
            ("year", year if year != "all" else None),
            ("tegund0", tegund0 if tegund0 != "all" else None),
            ("samtala0", samtala0 if samtala0 != "all" else None),
            ("samtala1", samtala1 if samtala1 != "all" else None),
        ])

        if seller != "all":
            seller_clause = f"{RKV_SUPPLIER_EXPR} = ?"
            where += f" AND {seller_clause}" if where else f"WHERE {seller_clause}"
            params.append(seller)

        if not show_corrections:
            where += " AND (is_correction = FALSE OR is_correction IS NULL)" if where else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        # Get all records (limit to 100k for safety)
        rows = con.execute(
            f"SELECT * FROM data {where} LIMIT 100000",
            params,
        ).fetchall()

        if not rows:
            return "No records found", 404

        # Get column names
        columns = [desc[0] for desc in con.description]

        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=reykjavik_records.csv"}
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
            f"SELECT year, samtala0, samtala1, tegund0, tegund1, raun, fyrirtaeki, "
            f"samtala2_canonical, samtala3, tegund2, tegund3, vm_nafn, CAST(vm_numer AS VARCHAR) AS vm_numer, "
            f"{RKV_SUPPLIER_EXPR} AS supplier_name, arsfjordungur, fjarfesting "
            f"FROM data {where} LIMIT {limit} OFFSET {offset}",
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "samtala0": r[1], "samtala1": r[2],
             "tegund0": r[3], "tegund1": r[4], "raun": r[5], "fyrirtaeki": r[6] or "",
             "samtala2_canonical": r[7] or "", "samtala3": r[8] or "", "tegund2": r[9] or "", "tegund3": r[10] or "",
             "raun_fmt": fmt(float(r[5]) if r[5] else 0), "vm_nafn": r[11] or "", "vm_numer": r[12] or "",
             "supplier_name": r[13] or "", "arsfjordungur": r[14] or "", "fjarfesting": r[15] or "",
             "_source": "rkv"}
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

    @app.route("/reykjavik/reports/wages")
    def rkv_reports_wages():
        year = request.args.get("year", "all").rstrip("*")
        category = request.args.get("category", "")
        department = request.args.get("department", "")
        institution = request.args.get("institution", "")

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("report_wages.html", source="reykjavik",
                                   page_id="reports", data_loaded=False,
                                   error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL AND tegund0 = 'Laun' "
            "AND fyrirtaeki = 'Reykjavíkurborg' ORDER BY year DESC"
        ).fetchall()]

        all_wage_years = sorted(years)
        year_filter = f"AND year = {year}" if year != "all" else ""
        chart_data = []

        if not category:
            # Level 0: Categories with totals
            level = 0
            wage_rows = con.execute(
                f"SELECT samtala1, year, SUM({RKV_AMOUNT_EXPR}) AS total "
                f"FROM data "
                f"WHERE tegund0 = 'Laun' AND fyrirtaeki = 'Reykjavíkurborg' "
                f"AND (is_correction = FALSE OR is_correction IS NULL) "
                f"GROUP BY samtala1, year ORDER BY samtala1, year"
            ).fetchall()

            category_yearly = {}
            for cat_key in RKV_WAGE_CATEGORIES:
                category_yearly[cat_key] = {}

            for dept, wy, total in wage_rows:
                if dept is None:
                    continue
                cat = get_wage_category(dept)
                if cat:
                    if wy not in category_yearly[cat]:
                        category_yearly[cat][wy] = 0
                    category_yearly[cat][wy] += total

            rows = []
            for cat_key, cat_info in RKV_WAGE_CATEGORIES.items():
                if not category_yearly[cat_key]:
                    continue
                yearly_values = [
                    float(category_yearly[cat_key].get(yr, 0)) for yr in all_wage_years
                ]
                if sum(yearly_values) > 0:
                    rows.append({
                        "name": cat_info["label"],
                        "key": cat_key,
                        "years": all_wage_years,
                        "yearly": yearly_values,
                        "total": sum(yearly_values),
                    })
            rows.sort(key=lambda x: x["total"], reverse=True)
            chart_data = rows

        elif not department:
            # Level 1: Departments in selected category
            level = 1
            dept_rows = con.execute(
                f"SELECT samtala1, year, SUM({RKV_AMOUNT_EXPR}) AS total "
                f"FROM data "
                f"WHERE tegund0 = 'Laun' AND fyrirtaeki = 'Reykjavíkurborg' "
                f"AND (is_correction = FALSE OR is_correction IS NULL) "
                f"GROUP BY samtala1, year ORDER BY samtala1, year"
            ).fetchall()

            dept_yearly = {}
            for dept, wy, total in dept_rows:
                if dept is None:
                    continue
                cat = get_wage_category(dept)
                if cat == category:
                    if dept not in dept_yearly:
                        dept_yearly[dept] = {}
                    if wy not in dept_yearly[dept]:
                        dept_yearly[dept][wy] = 0
                    dept_yearly[dept][wy] += total

            rows = []
            for dept, yearly_dict in sorted(dept_yearly.items()):
                yearly_values = [
                    float(yearly_dict.get(yr, 0)) for yr in all_wage_years
                ]
                if sum(yearly_values) > 0:
                    rows.append({
                        "name": dept or "Óskilgreint",
                        "key": dept,
                        "years": all_wage_years,
                        "yearly": yearly_values,
                        "total": sum(yearly_values),
                    })
            rows.sort(key=lambda x: x["total"], reverse=True)
            chart_data = rows

        elif not institution:
            # Level 2: Institutions in selected department
            level = 2
            inst_rows = con.execute(
                f"SELECT samtala0, year, SUM({RKV_AMOUNT_EXPR}) AS total "
                f"FROM data "
                f"WHERE tegund0 = 'Laun' AND fyrirtaeki = 'Reykjavíkurborg' AND samtala1 = ? "
                f"AND (is_correction = FALSE OR is_correction IS NULL) "
                f"GROUP BY samtala0, year ORDER BY samtala0, year",
                [department]
            ).fetchall()

            inst_yearly = {}
            for inst, wy, total in inst_rows:
                if inst is None:
                    continue
                if inst not in inst_yearly:
                    inst_yearly[inst] = {}
                if wy not in inst_yearly[inst]:
                    inst_yearly[inst][wy] = 0
                inst_yearly[inst][wy] += total

            rows = []
            for inst, yearly_dict in sorted(inst_yearly.items()):
                yearly_values = [
                    float(yearly_dict.get(yr, 0)) for yr in all_wage_years
                ]
                if sum(yearly_values) > 0:
                    rows.append({
                        "name": inst or "Óskilgreint",
                        "key": inst,
                        "years": all_wage_years,
                        "yearly": yearly_values,
                        "total": sum(yearly_values),
                    })
            rows.sort(key=lambda x: x["total"], reverse=True)
            chart_data = rows

        else:
            # Level 3: Individual records for institution
            level = 3
            records = con.execute(
                f"SELECT year, vm_nafn, fyrirtaeki, CAST(vm_numer AS VARCHAR), "
                f"raun, samtala0, samtala1, tegund0, tegund1, tegund2, tegund3 "
                f"FROM data "
                f"WHERE tegund0 = 'Laun' AND fyrirtaeki = 'Reykjavíkurborg' AND samtala0 = ? "
                f"AND (is_correction = FALSE OR is_correction IS NULL) {year_filter} "
                f"ORDER BY raun DESC LIMIT 1000",
                [institution]
            ).fetchall()

            rows = [
                {
                    "year": r[0],
                    "vm_nafn": r[1] or "",
                    "fyrirtaeki": r[2] or "",
                    "vm_numer": r[3] or "",
                    "raun": r[4],
                    "raun_fmt": fmt(r[4]),
                    "samtala0": r[5] or "",
                    "samtala1": r[6] or "",
                    "tegund0": r[7] or "",
                    "tegund1": r[8] or "",
                    "tegund2": r[9] or "",
                    "tegund3": r[10] or "",
                }
                for r in records
            ]

        # Calculate totals for each year
        year_totals = []
        if level < 3 and rows:
            for year_idx in range(len(all_wage_years)):
                total = sum(row["yearly"][year_idx] if year_idx < len(row["yearly"]) else 0 for row in rows)
                year_totals.append(total)

        return render_template(
            "report_wages.html",
            source="reykjavik",
            page_id="reports",
            data_loaded=True,
            level=level,
            year=year,
            years=years,
            category=category,
            department=department,
            institution=institution,
            wage_years=all_wage_years,
            rows=rows,
            year_totals=year_totals,
            chart_data=chart_data,
            category_name=RKV_WAGE_CATEGORIES.get(category, {}).get("label", "") if category else "",
        )

    @app.route("/reykjavik/reports/leikskoli")
    def rkv_reports_leikskoli():
        year = request.args.get("year", "all").rstrip("*")
        leikskoli = request.args.get("leikskoli", "")   # samtala0 value
        tegund = request.args.get("tegund", "")         # tegund0 value

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("report_leikskoli.html", source="reykjavik",
                                   page_id="reports", data_loaded=False,
                                   error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        years = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]

        base_clauses = [
            "(is_correction = FALSE OR is_correction IS NULL)",
            "samtala2_canonical = 'Leikskólar og dagforeldrar'",
            "tegund3 = 'Viðhald og framkvæmdir'",
        ]
        base_params = []
        if year != "all":
            base_clauses.append("year = ?")
            base_params.append(int(year))

        def where(extra_clauses=(), extra_params=()):
            all_clauses = base_clauses + list(extra_clauses)
            all_params = base_params + list(extra_params)
            return "WHERE " + " AND ".join(all_clauses), all_params

        if not leikskoli:
            # Level 0: per-institution totals
            w, p = where()
            rows = con.execute(
                f"SELECT samtala0, SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt "
                f"FROM data {w} AND samtala0 IS NOT NULL "
                f"GROUP BY samtala0 ORDER BY total DESC", p
            ).fetchall()
            level = 0

        elif not tegund:
            # Level 1: expense type breakdown for selected institution
            w, p = where(["samtala0 = ?"], [leikskoli])
            rows = con.execute(
                f"SELECT tegund0, SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt "
                f"FROM data {w} AND tegund0 IS NOT NULL "
                f"GROUP BY tegund0 ORDER BY total DESC", p
            ).fetchall()
            level = 1

        else:
            # Level 2: individual records with full details
            w, p = where(["samtala0 = ?", "tegund0 = ?"], [leikskoli, tegund])
            raw = con.execute(
                f"SELECT year, vm_nafn, fyrirtaeki, CAST(vm_numer AS VARCHAR) AS vm_numer, "
                f"raun, samtala0, samtala1, samtala2_canonical, samtala3, "
                f"tegund0, tegund1, tegund2, tegund3, "
                f"arsfjordungur, fjarfesting "
                f"FROM data {w} ORDER BY raun DESC", p
            ).fetchall()
            rows = [
                {
                    "year": r[0],
                    "vm_nafn": r[1] or "",
                    "fyrirtaeki": r[2] or "",
                    "vm_numer": r[3] or "",
                    "raun": r[4],
                    "raun_fmt": fmt(r[4]),
                    "samtala0": r[5] or "",
                    "samtala1": r[6] or "",
                    "samtala2_canonical": r[7] or "",
                    "samtala3": r[8] or "",
                    "tegund0": r[9] or "",
                    "tegund1": r[10] or "",
                    "tegund2": r[11] or "",
                    "tegund3": r[12] or "",
                    "arsfjordungur": r[13] or "",
                    "fjarfesting": r[14] or "",
                    "_source": "rkv"
                }
                for r in raw
            ]
            level = 2

        # Calculate totals and year breakdown
        w, p = where()
        total_result = con.execute(
            f"SELECT SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt FROM data {w}",
            p
        ).fetchone()
        total_amount = total_result[0] if total_result[0] else 0
        total_count = total_result[1] if total_result[1] else 0

        # Year breakdown
        yearly_result = con.execute(
            f"SELECT year, SUM({RKV_AMOUNT_EXPR}) AS total FROM data {w} GROUP BY year ORDER BY year DESC",
            p
        ).fetchall()
        yearly = [(r[0], r[1]) for r in yearly_result]

        return render_template("report_leikskoli.html",
            source="reykjavik", page_id="reports", data_loaded=True,
            level=level, year=year, years=years,
            leikskoli=leikskoli, tegund=tegund, rows=rows,
            total_amount=total_amount, total_count=total_count, yearly=yearly)

    @app.route("/reykjavik/reports/leikskoli/download")
    def rkv_reports_leikskoli_download():
        """Download leikskoli records as CSV."""
        year = request.args.get("year", "all").rstrip("*")
        leikskoli = request.args.get("leikskoli", "")
        tegund = request.args.get("tegund", "")

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return "Data not found", 404

        base_clauses = [
            "(is_correction = FALSE OR is_correction IS NULL)",
            "samtala2_canonical = 'Leikskólar og dagforeldrar'",
            "tegund3 = 'Viðhald og framkvæmdir'",
        ]
        base_params = []
        if year != "all":
            base_clauses.append("year = ?")
            base_params.append(int(year))

        def where(extra_clauses=(), extra_params=()):
            all_clauses = base_clauses + list(extra_clauses)
            all_params = base_params + list(extra_params)
            return "WHERE " + " AND ".join(all_clauses), all_params

        # Get all columns for the selected records
        if not leikskoli or not tegund:
            return "Must select both institution and category", 400

        w, p = where(["samtala0 = ?", "tegund0 = ?"], [leikskoli, tegund])
        rows = con.execute(
            f"SELECT * FROM data {w} ORDER BY raun DESC LIMIT 10000",
            p
        ).fetchall()

        if not rows:
            return "No records found", 404

        # Get column names
        columns = [desc[0] for desc in con.description]

        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)

        # Return as file
        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=leikskoli_records.csv"}
        )

    @app.route("/reykjavik/reports/midstodvar")
    def rkv_reports_midstodvar():
        maeling = request.args.get("maeling", "gjold")
        if maeling not in MIDST_MAELINGAR:
            maeling = "gjold"
        telja_npa = request.args.get("npa", "false").lower() == "true"
        med_heimahjukrun = request.args.get("heimahjukrun", "true").lower() == "true"
        raunvirdi = request.args.get("raunvirdi", "true").lower() == "true"

        con = open_con(REYKJAVIK_DATA)
        if con is None:
            return render_template("report_midstodvar.html", source="reykjavik",
                                   page_id="reports", data_loaded=False,
                                   error=f"Gögn finnast ekki: {REYKJAVIK_DATA}")

        maeling_label, amount_expr = MIDST_MAELINGAR[maeling]

        rows = con.execute(
            f"SELECT year, samtala0, {amount_expr} AS s "
            "FROM data "
            "WHERE samtala1 = 'Velferðarsvið' "
            "  AND year IS NOT NULL "
            "  AND (is_correction = FALSE OR is_correction IS NULL) "
            "GROUP BY year, samtala0 "
            "HAVING s IS NOT NULL AND s <> 0 "
            "ORDER BY year, samtala0"
        ).fetchall()

        years = sorted({r[0] for r in rows})
        base_year = int(request.args.get("grunnar", years[0] if years else 2019))
        if base_year not in years:
            base_year = years[0] if years else base_year

        # Per-year group totals, and per-samtala0 series for the detail table.
        totals = {y: {"fatlad": 0.0, "midstod": 0.0, "midlaegt": 0.0} for y in years}
        detail: dict[str, dict] = {}
        for year_, samtala0, amount in rows:
            name = samtala0 or "–"
            group = midst_flokkur(samtala0, telja_npa)
            # Heimahjúkrun er ríkisfjármögnuð; má taka úr nefnara miðstöðvanna.
            if group == "midstod" and samtala0 == MIDST_RIKISFJARMOGNUD \
                    and not med_heimahjukrun:
                group = "midlaegt"
            totals[year_][group] += float(amount)
            entry = detail.setdefault(name, {"name": name, "group": group, "by_year": {}})
            entry["by_year"][year_] = entry["by_year"].get(year_, 0.0) + float(amount)

        def real_index(series: dict, group: str) -> dict:
            """Vísitala, 100 = grunnár. Verðleiðrétt með launavísitölu ef beðið er um."""
            base = series.get(base_year, {}).get(group) or 0.0
            out = {}
            if not base:
                return out
            base_lv = LAUNAVISITALA.get(base_year)
            for y in years:
                value = series[y][group]
                idx = value / base * 100
                if raunvirdi and base_lv and LAUNAVISITALA.get(y):
                    idx = idx / (LAUNAVISITALA[y] / base_lv)
                out[y] = idx
            return out

        idx_fatlad = real_index(totals, "fatlad")
        idx_midstod = real_index(totals, "midstod")

        year_rows = []
        for i, y in enumerate(years):
            t = totals[y]
            scope = t["fatlad"] + t["midstod"]
            share = (t["fatlad"] / scope * 100) if scope else None
            prev_share = None
            if i > 0:
                pt = totals[years[i - 1]]
                pscope = pt["fatlad"] + pt["midstod"]
                prev_share = (pt["fatlad"] / pscope * 100) if pscope else None
            year_rows.append({
                "year": y,
                "fatlad": fmt(t["fatlad"]),
                "midstod": fmt(t["midstod"]),
                "midlaegt": fmt(t["midlaegt"]),
                "scope": fmt(scope),
                "share": f"{share:.1f}%" if share is not None else "–",
                "share_chg": (f"{share - prev_share:+.1f}"
                              if share is not None and prev_share is not None else "–"),
                "share_dir": ("up" if share is not None and prev_share is not None
                              and share >= prev_share else "down"),
                "idx_fatlad": f"{idx_fatlad[y]:.1f}" if y in idx_fatlad else "–",
                "idx_midstod": f"{idx_midstod[y]:.1f}" if y in idx_midstod else "–",
                "lv": LAUNAVISITALA.get(y),
            })

        group_labels = {
            "fatlad": "Málaflokkur fatlaðs fólks",
            "midstod": "Önnur verkefni miðstöðva",
            "midlaegt": "Miðlægt / ekki á miðstöðvum",
        }
        detail_rows = []
        for entry in detail.values():
            series = entry["by_year"]
            total = sum(series.values())
            first = next((series[y] for y in years if series.get(y)), None)
            last = next((series[y] for y in reversed(years) if series.get(y)), None)
            detail_rows.append({
                "name": entry["name"],
                "group": entry["group"],
                "group_label": group_labels[entry["group"]],
                "total": total,
                "total_fmt": fmt(total),
                "cells": [fmt(series[y]) if series.get(y) else "" for y in years],
                "trend": (fmt_pct((last - first) / abs(first) * 100)
                          if first and last else "–"),
            })
        detail_rows.sort(key=lambda r: (
            {"fatlad": 0, "midstod": 1, "midlaegt": 2}[r["group"]], -abs(r["total"])
        ))

        return render_template(
            "report_midstodvar.html",
            source="reykjavik", page_id="reports", data_loaded=True,
            years=years, year_rows=year_rows, detail_rows=detail_rows,
            maeling=maeling, maeling_label=maeling_label,
            maelingar=[(k, v[0]) for k, v in MIDST_MAELINGAR.items()],
            telja_npa=telja_npa, med_heimahjukrun=med_heimahjukrun,
            raunvirdi=raunvirdi, base_year=base_year,
            chart_years=[str(y) for y in years],
            chart_share=[round(totals[y]["fatlad"] /
                               (totals[y]["fatlad"] + totals[y]["midstod"]) * 100, 1)
                         if (totals[y]["fatlad"] + totals[y]["midstod"]) else None
                         for y in years],
            chart_idx_fatlad=[round(idx_fatlad[y], 1) if y in idx_fatlad else None
                              for y in years],
            chart_idx_midstod=[round(idx_midstod[y], 1) if y in idx_midstod else None
                               for y in years],
        )

    # =========================================================================
    # RIKISREIKNINGUR
    # =========================================================================

    @app.route("/rikisreikningur/")
    def rikisreikningur_explorer():
        year = request.args.get("year", "all").rstrip("*")
        tegund = request.args.get("tegund", "all")
        buyer = request.args.get("buyer", "all")
        samtala1 = request.args.get("samtala1", "all")
        sign = request.args.get("sign", "all")
        limit = max(1, min(500, int(request.args.get("limit", 50))))
        page = max(1, int(request.args.get("page", 1)))
        offset = (page - 1) * limit

        con = open_con(RIKISREIKNINGUR_DATA)
        if con is None:
            return render_template("explorer.html", source="rikisreikningur", data_loaded=False,
                                   error=f"Gögn finnast ekki: {RIKISREIKNINGUR_DATA}")

        where, params = build_where([
            ("year", year if year != "all" else None),
            ("TegundHeiti", tegund if tegund != "all" else None),
            ("RaduneytiHeiti", buyer if buyer != "all" else None),
            ("StofnunHeiti", samtala1 if samtala1 != "all" else None),
        ])
        chart_where, chart_params = build_where([
            ("TegundHeiti", tegund if tegund != "all" else None),
            ("RaduneytiHeiti", buyer if buyer != "all" else None),
            ("StofnunHeiti", samtala1 if samtala1 != "all" else None),
        ])

        if sign == "pos":
            sign_clause = f"{RIKISREIKNINGUR_AMOUNT} > 0"
            where += f" AND {sign_clause}" if where else f"WHERE {sign_clause}"
            chart_where += f" AND {sign_clause}" if chart_where else f"WHERE {sign_clause}"
        elif sign == "neg":
            sign_clause = f"{RIKISREIKNINGUR_AMOUNT} < 0"
            where += f" AND {sign_clause}" if where else f"WHERE {sign_clause}"
            chart_where += f" AND {sign_clause}" if chart_where else f"WHERE {sign_clause}"

        partial_years = {
            int(r[0]) for r in con.execute(
                "SELECT DISTINCT year FROM data WHERE is_partial_year = TRUE AND year IS NOT NULL"
            ).fetchall()
        }
        years_raw = [r[0] for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        years = [f"{y}*" if int(y) in partial_years else y for y in years_raw]

        tegund_opts = [r[0] for r in con.execute(
            'SELECT DISTINCT "TegundHeiti" FROM data WHERE "TegundHeiti" IS NOT NULL ORDER BY "TegundHeiti"'
        ).fetchall()]
        buyer_opts = [r[0] for r in con.execute(
            'SELECT DISTINCT "RaduneytiHeiti" FROM data WHERE "RaduneytiHeiti" IS NOT NULL ORDER BY "RaduneytiHeiti"'
        ).fetchall()]
        samtala1_opts = [r[0] for r in con.execute(
            'SELECT DISTINCT "StofnunHeiti" FROM data WHERE "StofnunHeiti" IS NOT NULL ORDER BY "StofnunHeiti"'
        ).fetchall()]

        yearly = con.execute(
            f"SELECT year, SUM({RIKISREIKNINGUR_AMOUNT}) FROM data {chart_where} GROUP BY year ORDER BY year",
            chart_params,
        ).fetchall()
        yearly_labels = [f"{r[0]}*" if int(r[0]) in partial_years else str(r[0]) for r in yearly]

        type_breakdown = con.execute(
            f'SELECT "TegundHeiti", SUM({RIKISREIKNINGUR_AMOUNT}) AS s, COUNT(*) AS n '
            f'FROM data {where} GROUP BY "TegundHeiti" ORDER BY s DESC LIMIT 30',
            params,
        ).fetchall()
        buyer_breakdown = con.execute(
            f'SELECT "RaduneytiHeiti", SUM({RIKISREIKNINGUR_AMOUNT}) AS s, COUNT(*) AS n '
            f'FROM data {where} GROUP BY "RaduneytiHeiti" ORDER BY s DESC LIMIT 30',
            params,
        ).fetchall()
        institution_breakdown = con.execute(
            f'SELECT "StofnunHeiti", SUM({RIKISREIKNINGUR_AMOUNT}) AS s, COUNT(*) AS n '
            f'FROM data {where} GROUP BY "StofnunHeiti" ORDER BY s DESC LIMIT 30',
            params,
        ).fetchall()

        tot = con.execute(
            f"SELECT COUNT(*) AS n, SUM({RIKISREIKNINGUR_AMOUNT}) AS s, "
            f"SUM(CASE WHEN {RIKISREIKNINGUR_AMOUNT} > 0 THEN {RIKISREIKNINGUR_AMOUNT} END) AS pos, "
            f"SUM(CASE WHEN {RIKISREIKNINGUR_AMOUNT} < 0 THEN {RIKISREIKNINGUR_AMOUNT} END) AS neg "
            f"FROM data {where}",
            params,
        ).fetchone()

        rows = con.execute(
            f'SELECT year, "Timabil", "RaduneytiHeiti", "StofnunHeiti", "FjarlagavidfangHeiti", '
            f'"MalaflokkurNumerOgHeiti", "TegundHeiti", "TegundL2Heiti", "TegundL3Heiti", {RIKISREIKNINGUR_AMOUNT} AS amount '
            f'FROM data {where} ORDER BY year DESC, "Timabil" DESC LIMIT {limit} OFFSET {offset}',
            params,
        ).fetchall()
        preview_rows = [
            {"year": r[0], "Timabil": r[1], "RaduneytiHeiti": r[2], "StofnunHeiti": r[3],
             "FjarlagavidfangHeiti": r[4], "MalaflokkurNumerOgHeiti": r[5], "TegundHeiti": r[6],
             "TegundL2Heiti": r[7], "TegundL3Heiti": r[8], "amount_raw": r[9], "amount": fmt(r[9]),
             "amount_fmt": fmt(r[9]), "_source": "rikisreikningur"}
            for r in rows
        ]

        total_count = int(tot[0]) if tot and tot[0] is not None else 0
        total_pages = max(1, math.ceil(total_count / limit))
        active_filters = []
        if year != "all":
            active_filters.append({"label": "Ár", "value": str(year), "param": "year"})
        if tegund != "all":
            active_filters.append({"label": "Tegund", "value": tegund, "param": "tegund"})
        if buyer != "all":
            active_filters.append({"label": "Ráðuneyti", "value": buyer, "param": "buyer"})
        if samtala1 != "all":
            active_filters.append({"label": "Stofnun", "value": samtala1, "param": "samtala1"})
        if sign != "all":
            sign_label = "Jákvæðar" if sign == "pos" else "Neikvæðar"
            active_filters.append({"label": sign_label, "value": "", "param": "sign"})

        return render_template(
            "explorer.html",
            source="rikisreikningur",
            data_loaded=True,
            year=year, tegund=tegund, buyer=buyer, samtala1=samtala1, seller="all", sign=sign,
            years=years,
            tegund_opts=tegund_opts,
            buyer_opts=buyer_opts,
            samtala1_opts=samtala1_opts,
            tegund_label="Tegund",
            buyer_label="Ráðuneyti",
            samtala1_label="Stofnun",
            yearly_labels=yearly_labels,
            yearly_values=[float(r[1]) if r[1] else 0 for r in yearly],
            breakdown_sections=[
                {"title": "Sundurliðun eftir tegund (topp 30)", "label": "Tegund", "rows": type_breakdown, "filter_param": "tegund"},
                {"title": "Sundurliðun eftir ráðuneyti (topp 30)", "label": "Ráðuneyti", "rows": buyer_breakdown, "filter_param": "buyer"},
                {"title": "Sundurliðun eftir stofnun (topp 30)", "label": "Stofnun", "rows": institution_breakdown, "filter_param": "samtala1"},
            ],
            totals={"count": tot[0], "sum": tot[1], "pos": tot[2], "neg": tot[3]} if tot else {},
            preview_rows=preview_rows,
            preview_cols=["year", "Timabil", "RaduneytiHeiti", "StofnunHeiti", "TegundHeiti", "amount"],
            page=page, limit=limit, total_pages=total_pages,
            active_filters=active_filters,
            dn=rikisreikningur_dn,
        )

    @app.route("/rikisreikningur/download")
    def rikisreikningur_explorer_download():
        year = request.args.get("year", "all").rstrip("*")
        tegund = request.args.get("tegund", "all")
        buyer = request.args.get("buyer", "all")
        samtala1 = request.args.get("samtala1", "all")
        sign = request.args.get("sign", "all")

        con = open_con(RIKISREIKNINGUR_DATA)
        if con is None:
            return "Data not found", 404

        where, params = build_where([
            ("year", year if year != "all" else None),
            ("TegundHeiti", tegund if tegund != "all" else None),
            ("RaduneytiHeiti", buyer if buyer != "all" else None),
            ("StofnunHeiti", samtala1 if samtala1 != "all" else None),
        ])

        if sign == "pos":
            where += f" AND {RIKISREIKNINGUR_AMOUNT} > 0" if where else f"WHERE {RIKISREIKNINGUR_AMOUNT} > 0"
        elif sign == "neg":
            where += f" AND {RIKISREIKNINGUR_AMOUNT} < 0" if where else f"WHERE {RIKISREIKNINGUR_AMOUNT} < 0"

        rows = con.execute(
            f"SELECT * FROM data {where} ORDER BY year DESC, \"Timabil\" DESC LIMIT 100000",
            params,
        ).fetchall()
        if not rows:
            return "No records found", 404

        columns = [desc[0] for desc in con.description]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)

        output.seek(0)
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=rikisreikningur_records.csv"}
        )

    @app.route("/rikisreikningur/comparison")
    def rikisreikningur_comparison():
        con_rikis = open_con(RIKISREIKNINGUR_DATA)
        con_rikid = open_rikid_con(RIKID_DATA)
        if con_rikis is None or con_rikid is None:
            missing = []
            if con_rikis is None:
                missing.append(str(RIKISREIKNINGUR_DATA))
            if con_rikid is None:
                missing.append(str(RIKID_DATA))
            return render_template(
                "comparison_rikisreikningur.html",
                source="rikisreikningur",
                page_id="comparison",
                data_loaded=False,
                error=f"Gögn finnast ekki: {', '.join(missing)}",
            )

        rikis_partial_years = {
            int(r[0]) for r in con_rikis.execute(
                "SELECT DISTINCT year FROM data WHERE is_partial_year = TRUE AND year IS NOT NULL"
            ).fetchall()
        }
        rikis_years = {int(r[0]) for r in con_rikis.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL"
        ).fetchall()}
        rikid_years = {int(r[0]) for r in con_rikid.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL AND (is_correction = FALSE OR is_correction IS NULL)"
        ).fetchall()}
        common_years = sorted(rikis_years & rikid_years)
        if not common_years:
            return render_template(
                "comparison_rikisreikningur.html",
                source="rikisreikningur",
                page_id="comparison",
                data_loaded=False,
                error="Engin sameiginleg ár fundust milli Ríkisins og ríkisreiknings.",
            )

        year_param = request.args.get("year", "all").rstrip("*")
        year = year_param if year_param == "all" else str(int(year_param))

        years = [f"{y}*" if y in rikis_partial_years else str(y) for y in sorted(common_years, reverse=True)]

        rikis_yearly = con_rikis.execute(
            f"""
            WITH base AS (
                SELECT
                    year,
                    amount,
                    {RIKISREIKNINGUR_COMPARISON_BUCKET_SQL} AS bucket
                FROM data
                WHERE year IN ({",".join(str(y) for y in common_years)})
            )
            SELECT
                year,
                SUM(amount) AS rikis_total,
                SUM(CASE WHEN bucket IN ({",".join(repr(b) for b in sorted(COMPARABLE_BUCKETS))}) THEN amount END) AS comparable_total
            FROM base
            GROUP BY year
            ORDER BY year
            """
        ).fetchall()
        rikis_yearly_map = {int(r[0]): (float(r[1] or 0), float(r[2] or 0)) for r in rikis_yearly}

        rikid_yearly = con_rikid.execute(
            f"""
            SELECT year, SUM({RIKID_AMOUNT}) AS rikid_total
            FROM data
            WHERE year IN ({",".join(str(y) for y in common_years)})
              AND (is_correction = FALSE OR is_correction IS NULL)
            GROUP BY year
            ORDER BY year
            """
        ).fetchall()
        rikid_yearly_map = {int(r[0]): float(r[1] or 0) for r in rikid_yearly}

        chart_years = common_years if year == "all" else [int(year)]
        chart_labels = [f"{y}*" if y in rikis_partial_years else str(y) for y in chart_years]
        chart_rikis_total = [rikis_yearly_map.get(y, (0.0, 0.0))[0] for y in chart_years]
        chart_rikis_comparable = [rikis_yearly_map.get(y, (0.0, 0.0))[1] for y in chart_years]
        chart_rikid_total = [rikid_yearly_map.get(y, 0.0) for y in chart_years]
        chart_scope_gap = [chart_rikis_total[i] - chart_rikis_comparable[i] for i in range(len(chart_years))]
        chart_timing_gap = [chart_rikis_comparable[i] - chart_rikid_total[i] for i in range(len(chart_years))]
        chart_residual = [chart_rikis_total[i] - chart_rikid_total[i] for i in range(len(chart_years))]

        selected_year = None if year == "all" else int(year)
        selected_rikis_total = sum(chart_rikis_total)
        selected_rikis_comparable = sum(chart_rikis_comparable)
        selected_rikid_total = sum(chart_rikid_total)
        selected_scope_gap = selected_rikis_total - selected_rikis_comparable
        selected_timing_gap = selected_rikis_comparable - selected_rikid_total
        selected_residual = selected_rikis_total - selected_rikid_total

        year_filter_rikis = f"WHERE year = {selected_year}" if selected_year is not None else ""
        year_filter_rikid = f"WHERE year = {selected_year} AND (is_correction = FALSE OR is_correction IS NULL)" if selected_year is not None else "WHERE (is_correction = FALSE OR is_correction IS NULL)"

        rikis_bucket_rows = con_rikis.execute(
            f"""
            WITH base AS (
                SELECT
                    {RIKISREIKNINGUR_COMPARISON_BUCKET_SQL} AS bucket,
                    amount
                FROM data
                {year_filter_rikis}
            )
            SELECT bucket, SUM(amount) AS total
            FROM base
            GROUP BY bucket
            """
        ).fetchall()
        rikid_bucket_rows = con_rikid.execute(
            f"""
            WITH base AS (
                SELECT
                    {RIKID_COMPARISON_BUCKET_SQL} AS bucket,
                    {RIKID_AMOUNT} AS amount
                FROM data
                {year_filter_rikid}
            )
            SELECT bucket, SUM(amount) AS total
            FROM base
            GROUP BY bucket
            """
        ).fetchall()

        rikis_bucket_map = {str(r[0]): float(r[1] or 0) for r in rikis_bucket_rows}
        rikid_bucket_map = {str(r[0]): float(r[1] or 0) for r in rikid_bucket_rows}
        bucket_order = [
            "Húsnæði og leiga",
            "Þjónusta og ráðgjöf",
            "Framkvæmdir og viðhald",
            "Vörur, lyf og rekstrarinnkaup",
            "Tæki, hugbúnaður og eignir",
            "Tilfærslur og framlög",
            "Laun og launatengd gjöld",
            "Fjármagnsliðir, skattar og uppgjör",
            "Tilfærslur, skattar og uppgjör",
            "Annað",
        ]
        bucket_rows = []
        for bucket in bucket_order:
            rikis_val = rikis_bucket_map.get(bucket, 0.0)
            rikid_val = rikid_bucket_map.get(bucket, 0.0)
            if rikis_val == 0.0 and rikid_val == 0.0:
                continue
            comparable = bucket in COMPARABLE_BUCKETS
            bucket_rows.append({
                "bucket": bucket,
                "rikis_total_raw": rikis_val,
                "rikis_total": fmt(rikis_val),
                "rikid_total_raw": rikid_val,
                "rikid_total": fmt(rikid_val),
                "difference_raw": rikis_val - rikid_val,
                "difference": fmt(rikis_val - rikid_val),
                "comparable": comparable,
                "scope_label": "Ætti að vera samanburðarhæft" if comparable else "Bara í ríkisreikningi / óbeinn samanburður",
            })

        active_filters = []
        if year != "all":
            active_filters.append({"label": "Ár", "value": f"{year}*" if int(year) in rikis_partial_years else year, "param": "year"})

        caveats = [
            "Ríkið er greiðslugögn eftir greiðsludegi; ríkisreikningur er bókhalds- og uppgjörsgögn eftir tímabili.",
            "Samanburðurinn hér notar grófa flokkun á tegundum. Hann er gagnlegur til að finna stærstu skýringar, ekki sem full endurskoðunarjöfnun.",
            "Laun, tilfærslur, skattar, vextir og uppgjörsliðir eiga almennt ekki að birtast að fullu í Ríkinu.",
        ]
        if any(y in rikis_partial_years for y in chart_years):
            caveats.append("Merkt ár með * er ófullkomið í ríkisreikningi.")
        if min(common_years) > 2017:
            caveats.append(f"Sameiginleg staðbundin gögn byrja á {min(common_years)}.")

        return render_template(
            "comparison_rikisreikningur.html",
            source="rikisreikningur",
            page_id="comparison",
            data_loaded=True,
            year=year,
            years=years,
            active_filters=active_filters,
            selected_rikis_total=fmt(selected_rikis_total),
            selected_rikis_total_raw=selected_rikis_total,
            selected_rikis_comparable=fmt(selected_rikis_comparable),
            selected_rikis_comparable_raw=selected_rikis_comparable,
            selected_rikid_total=fmt(selected_rikid_total),
            selected_rikid_total_raw=selected_rikid_total,
            selected_scope_gap=fmt(selected_scope_gap),
            selected_scope_gap_raw=selected_scope_gap,
            selected_timing_gap=fmt(selected_timing_gap),
            selected_timing_gap_raw=selected_timing_gap,
            selected_residual=fmt(selected_residual),
            selected_residual_raw=selected_residual,
            chart_labels=chart_labels,
            chart_rikis_total=chart_rikis_total,
            chart_rikis_comparable=chart_rikis_comparable,
            chart_rikid_total=chart_rikid_total,
            chart_scope_gap=chart_scope_gap,
            chart_timing_gap=chart_timing_gap,
            chart_residual=chart_residual,
            bucket_rows=bucket_rows,
            caveats=caveats,
        )

    @app.route("/rikisreikningur/institutions")
    def rikisreikningur_institutions():
        year = request.args.get("year", "").rstrip("*")
        match = request.args.get("match", "all")
        con_rikis = open_con(RIKISREIKNINGUR_DATA)
        con_rikid = open_rikid_con(RIKID_DATA)
        if con_rikis is None or con_rikid is None:
            return render_template(
                "rikid_institutions.html",
                source="rikisreikningur",
                page_id="institutions",
                data_loaded=False,
                error="Vantar rikisreikningur eða rikid gögn fyrir stofnanasamanburð.",
            )

        valid_matches = {"all", "exact", "alias", "predecessor", "missing"}
        if match not in valid_matches:
            match = "all"

        partial_years = {
            int(r[0]) for r in con_rikis.execute(
                "SELECT DISTINCT year FROM data WHERE is_partial_year = TRUE AND year IS NOT NULL"
            ).fetchall()
        }
        years_raw = [int(r[0]) for r in con_rikis.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        if not years_raw:
            return render_template(
                "rikid_institutions.html",
                source="rikisreikningur",
                page_id="institutions",
                data_loaded=False,
                error="Engin rikisreikningur-gögn fundust.",
            )
        default_year = next((y for y in years_raw if y not in partial_years), years_raw[0])
        if not year or not year.isdigit():
            year = str(default_year)
        selected_year = int(year)
        years = [f"{y}*" if y in partial_years else str(y) for y in years_raw]

        rikis_rows = con_rikis.execute(
            """
            SELECT DISTINCT
                CASE
                    WHEN "StofnunHeiti" = "FjarlagavidfangHeiti" THEN "RaduneytiHeiti"
                    ELSE "StofnunHeiti"
                END AS canonical_institution,
                "RaduneytiHeiti"
            FROM data
            WHERE year = ? AND "StofnunHeiti" IS NOT NULL
            ORDER BY canonical_institution
            """,
            [selected_year],
        ).fetchall()
        rikid_buyers = [r[0] for r in con_rikid.execute(
            """
            SELECT DISTINCT "Kaupandi"
            FROM data
            WHERE year = ? AND "Kaupandi" IS NOT NULL AND (is_correction = FALSE OR is_correction IS NULL)
            ORDER BY "Kaupandi"
            """,
            [selected_year],
        ).fetchall()]
        rikid_buyer_set = set(rikid_buyers)
        rikid_by_norm: dict[str, list[str]] = {}
        for buyer in rikid_buyers:
            rikid_by_norm.setdefault(normalize_name(buyer), []).append(buyer)

        website_rows: list[dict[str, str]] = []
        fetched_at = ""
        if RIKID_INSTITUTIONS_RECONCILIATION.exists():
            with RIKID_INSTITUTIONS_RECONCILIATION.open(encoding="utf-8", newline="") as f:
                website_rows = list(csv.DictReader(f))
            fetched_at = website_rows[0]["fetched_at_utc"] if website_rows else ""
        website_by_exact = {row["institution"]: row for row in website_rows}
        website_by_norm: dict[str, dict[str, str]] = {}
        for row in website_rows:
            website_by_norm.setdefault(normalize_name(row["institution"]), row)

        rows = []
        counts: dict[str, int] = {}
        for institution, ministry in rikis_rows:
            if institution in rikid_buyer_set:
                match_type = "exact"
                rikid_match = institution
                note = ""
            elif institution in RIKIS_TO_RIKID_ALIASES:
                rikid_match, match_type = RIKIS_TO_RIKID_ALIASES[institution]
                note = "Handvirk vörpun"
            elif normalize_name(institution) in rikid_by_norm:
                rikid_match = rikid_by_norm[normalize_name(institution)][0]
                match_type = "alias"
                note = "Sama heiti eftir einföldun"
            else:
                rikid_match = ""
                match_type = "missing"
                note = ""

            website_match = website_by_exact.get(institution)
            if website_match:
                website_name = website_match["institution"]
                website_status = "exact"
            elif normalize_name(institution) in website_by_norm:
                website_name = website_by_norm[normalize_name(institution)]["institution"]
                website_status = "alias"
            else:
                website_name = ""
                website_status = "missing"

            counts[match_type] = counts.get(match_type, 0) + 1
            rows.append(
                {
                    "institution": institution,
                    "ministry": ministry,
                    "match_type": match_type,
                    "rikid_match": rikid_match,
                    "note": note,
                    "website_name": website_name,
                    "website_status": website_status,
                }
            )

        filtered_rows = [row for row in rows if match == "all" or row["match_type"] == match]
        sort_order = {"missing": 1, "predecessor": 2, "alias": 3, "exact": 4}
        filtered_rows.sort(key=lambda row: (sort_order.get(row["match_type"], 9), row["institution"]))

        return render_template(
            "rikid_institutions.html",
            source="rikisreikningur",
            page_id="institutions",
            data_loaded=True,
            year=year,
            years=years,
            match=match,
            counts=counts,
            fetched_at=fetched_at,
            rows=filtered_rows,
        )

    @app.route("/rikisreikningur/wages")
    def rikisreikningur_wages():
        con = open_con(RIKISREIKNINGUR_DATA)
        if con is None:
            return render_template(
                "rikisreikningur_wages.html",
                source="rikisreikningur",
                page_id="wages",
                data_loaded=False,
                error=f"Gögn finnast ekki: {RIKISREIKNINGUR_DATA}",
            )

        partial_years = {
            int(r[0]) for r in con.execute(
                "SELECT DISTINCT year FROM data WHERE is_partial_year = TRUE AND year IS NOT NULL"
            ).fetchall()
        }
        years_raw = [int(r[0]) for r in con.execute(
            "SELECT DISTINCT year FROM data WHERE year IS NOT NULL ORDER BY year DESC"
        ).fetchall()]
        if not years_raw:
            return render_template(
                "rikisreikningur_wages.html",
                source="rikisreikningur",
                page_id="wages",
                data_loaded=False,
                error="Engin rikisreikningur-gögn fundust.",
            )

        year_param = request.args.get("year", "all").rstrip("*")
        year = year_param if year_param == "all" else str(int(year_param))
        years = [f"{y}*" if y in partial_years else str(y) for y in years_raw]

        yearly_rows = con.execute(
            f"""
            WITH wages AS (
                SELECT
                    year,
                    amount,
                    {RIKISREIKNINGUR_WAGE_BUCKET_SQL} AS bucket
                FROM data
            ),
            yearly AS (
                SELECT
                    year,
                    SUM(amount) AS wage_total,
                    SUM(CASE WHEN bucket = 'Kjarna-laun' THEN amount ELSE 0 END) AS core_pay,
                    SUM(CASE WHEN bucket = 'Yfirvinna og álag' THEN amount ELSE 0 END) AS overtime,
                    SUM(CASE WHEN bucket = 'Orlofsskuldbinding' THEN amount ELSE 0 END) AS leave_cost,
                    SUM(CASE WHEN bucket = 'Launatengd gjöld' THEN amount ELSE 0 END) AS payroll_taxes,
                    SUM(CASE WHEN bucket = 'Lífeyrisskuldbindingar' THEN amount ELSE 0 END) AS pension_change,
                    SUM(CASE WHEN bucket = 'Starfsmannakostnaður' THEN amount ELSE 0 END) AS staff_cost,
                    SUM(CASE WHEN bucket = 'Mótfærslur og leiðréttingar' THEN amount ELSE 0 END) AS offsets,
                    SUM(CASE WHEN bucket = 'Annað launatengt' THEN amount ELSE 0 END) AS other_wage
                FROM wages
                WHERE bucket IS NOT NULL
                GROUP BY year
            )
            SELECT
                year,
                wage_total,
                core_pay,
                overtime,
                leave_cost,
                payroll_taxes,
                pension_change,
                staff_cost,
                offsets,
                other_wage
            FROM yearly
            ORDER BY year
            """
        ).fetchall()
        yearly_map = {
            int(r[0]): {
                "total": float(r[1] or 0),
                "core_pay": float(r[2] or 0),
                "overtime": float(r[3] or 0),
                "leave_cost": float(r[4] or 0),
                "payroll_taxes": float(r[5] or 0),
                "pension_change": float(r[6] or 0),
                "staff_cost": float(r[7] or 0),
                "offsets": float(r[8] or 0),
                "other_wage": float(r[9] or 0),
            }
            for r in yearly_rows
        }

        filtered_years = sorted(yearly_map)
        if year != "all":
            selected_year = int(year)
            filtered_years = [y for y in filtered_years if y == selected_year]
        else:
            selected_year = None

        focus_year = selected_year
        if focus_year is None:
            focus_year = next((y for y in sorted(yearly_map, reverse=True) if y not in partial_years), max(yearly_map))
        focus_prev = max((y for y in yearly_map if y < focus_year), default=None)
        focus = yearly_map[focus_year]
        prev = yearly_map.get(focus_prev)

        def pct_change(cur: float, old: float | None) -> float | None:
            if old in (None, 0):
                return None
            return ((cur / old) - 1.0) * 100.0

        year_rows = []
        for y in sorted(filtered_years, reverse=True):
            row = yearly_map[y]
            prev_row = yearly_map.get(y - 1)
            year_rows.append(
                {
                    "year": f"{y}*" if y in partial_years else str(y),
                    "total_raw": row["total"],
                    "total": fmt(row["total"]),
                    "total_pct": pct_change(row["total"], prev_row["total"] if prev_row else None),
                    "core_pay": fmt(row["core_pay"]),
                    "core_pay_raw": row["core_pay"],
                    "core_pct": pct_change(row["core_pay"], prev_row["core_pay"] if prev_row else None),
                    "overtime": fmt(row["overtime"]),
                    "leave_cost": fmt(row["leave_cost"]),
                    "payroll_taxes": fmt(row["payroll_taxes"]),
                    "pension_change": fmt(row["pension_change"]),
                    "staff_cost": fmt(row["staff_cost"]),
                    "offsets": fmt(row["offsets"]),
                }
            )

        bucket_rows_raw = con.execute(
            f"""
            WITH wages AS (
                SELECT
                    "TegundHeiti" AS category,
                    amount,
                    {RIKISREIKNINGUR_WAGE_BUCKET_SQL} AS bucket
                FROM data
                {"WHERE year = ?" if selected_year is not None else ""}
            )
            SELECT
                bucket,
                category,
                SUM(amount) AS total
            FROM wages
            WHERE bucket IS NOT NULL
            GROUP BY bucket, category
            ORDER BY bucket, total DESC
            """,
            [selected_year] if selected_year is not None else [],
        ).fetchall()

        bucket_groups: dict[str, list[dict[str, object]]] = {bucket: [] for bucket in RIKISREIKNINGUR_WAGE_BUCKET_ORDER}
        bucket_totals = {bucket: 0.0 for bucket in RIKISREIKNINGUR_WAGE_BUCKET_ORDER}
        for bucket, category, total in bucket_rows_raw:
            bucket = str(bucket)
            total_value = float(total or 0)
            if bucket not in bucket_groups:
                bucket_groups[bucket] = []
                bucket_totals[bucket] = 0.0
            bucket_totals[bucket] += total_value
            if len(bucket_groups[bucket]) < 8:
                bucket_groups[bucket].append({
                    "category": category,
                    "amount": fmt(total_value),
                    "amount_raw": total_value,
                })

        bucket_rows = []
        for bucket in RIKISREIKNINGUR_WAGE_BUCKET_ORDER:
            total_value = bucket_totals.get(bucket, 0.0)
            if total_value == 0 and not bucket_groups.get(bucket):
                continue
            bucket_rows.append(
                {
                    "bucket": bucket,
                    "total": fmt(total_value),
                    "total_raw": total_value,
                    "top_categories": bucket_groups.get(bucket, []),
                }
            )

        active_filters = []
        if year != "all":
            active_filters.append({"label": "Ár", "value": f"{selected_year}*" if selected_year in partial_years else str(selected_year)})

        caveats = [
            "Þetta eru bókhaldsfærslur, ekki launavísitala eða meðaltal launa á starfsmann.",
            "Launatölur hér hreyfast líka vegna mönnunar, yfirvinnu, vakta, orlofs, lífeyrisskuldbindinga og leiðréttinga.",
            "Flokkunin hér byggir fyrst og fremst á launaflokkum ríkisreikningsins (`TegundL3Heiti`) frekar en einstökum undirtegundum.",
            "Árið 2025 er aðeins fyrstu sex mánuðirnir í ríkisreikningsgögnunum og er því merkt með *.",
        ]

        chart_years = sorted(filtered_years)
        chart_labels = [f"{y}*" if y in partial_years else str(y) for y in chart_years]

        return render_template(
            "rikisreikningur_wages.html",
            source="rikisreikningur",
            page_id="wages",
            data_loaded=True,
            year=year,
            years=years,
            active_filters=active_filters,
            focus_year=f"{focus_year}*" if focus_year in partial_years else str(focus_year),
            focus_total=fmt(focus["total"]),
            focus_total_raw=focus["total"],
            focus_total_pct=pct_change(focus["total"], prev["total"] if prev else None),
            focus_core_pay=fmt(focus["core_pay"]),
            focus_core_pay_raw=focus["core_pay"],
            focus_core_pct=pct_change(focus["core_pay"], prev["core_pay"] if prev else None),
            focus_overtime=fmt(focus["overtime"]),
            focus_overtime_raw=focus["overtime"],
            focus_leave_cost=fmt(focus["leave_cost"]),
            focus_leave_cost_raw=focus["leave_cost"],
            focus_payroll_taxes=fmt(focus["payroll_taxes"]),
            focus_payroll_taxes_raw=focus["payroll_taxes"],
            focus_pension_change=fmt(focus["pension_change"]),
            focus_pension_change_raw=focus["pension_change"],
            focus_staff_cost=fmt(focus["staff_cost"]),
            focus_staff_cost_raw=focus["staff_cost"],
            focus_offsets=fmt(focus["offsets"]),
            focus_offsets_raw=focus["offsets"],
            chart_labels=chart_labels,
            chart_core_pay=[yearly_map[y]["core_pay"] for y in chart_years],
            chart_overtime=[yearly_map[y]["overtime"] for y in chart_years],
            chart_leave_cost=[yearly_map[y]["leave_cost"] for y in chart_years],
            chart_payroll_taxes=[yearly_map[y]["payroll_taxes"] for y in chart_years],
            chart_pension_change=[yearly_map[y]["pension_change"] for y in chart_years],
            chart_staff_cost=[yearly_map[y]["staff_cost"] for y in chart_years],
            chart_offsets=[yearly_map[y]["offsets"] for y in chart_years],
            chart_other_wage=[yearly_map[y]["other_wage"] for y in chart_years],
            chart_total=[yearly_map[y]["total"] for y in chart_years],
            year_rows=year_rows,
            bucket_rows=bucket_rows,
            caveats=caveats,
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
                                   explorer_base=url_for('rikid_explorer'), explorer_type_param="tegund", explorer_buyer_param="buyer")

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
                                   explorer_base=url_for('rikid_explorer'), explorer_type_param="seller", explorer_buyer_param="buyer")

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
                                   explorer_base=url_for('rkv_explorer'), explorer_type_param="tegund", explorer_buyer_param="buyer")

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
            # Level 1: drill by whatever hierarchy is available (expense type or organization)
            # First check if this supplier has expense type data
            has_types = con.execute(
                f'SELECT COUNT(*) FROM data {where_base} AND ({RKV_SUPPLIER_EXPR}) = ? AND tgr1 IS NOT NULL',
                [value]
            ).fetchone()[0] > 0

            if has_types:
                # Drill by expense type (tgr1)
                rows = con.execute(
                    f'SELECT COALESCE(xtgr1, CAST(tgr1 AS VARCHAR), \'(óskráð)\') AS category, '
                    f'SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt '
                    f'FROM data {where_base} AND ({RKV_SUPPLIER_EXPR}) = ? '
                    f'GROUP BY tgr1, xtgr1 ORDER BY total DESC',
                    [value]
                ).fetchall()
                drill_label = "Tegund útgjalda"
            else:
                # Drill by organization (samtala0)
                rows = con.execute(
                    f'SELECT COALESCE(samtala0, \'(óskráð)\') AS category, '
                    f'SUM({RKV_AMOUNT_EXPR}) AS total, COUNT(*) AS cnt '
                    f'FROM data {where_base} AND ({RKV_SUPPLIER_EXPR}) = ? '
                    f'GROUP BY samtala0 ORDER BY total DESC',
                    [value]
                ).fetchall()
                drill_label = "Stofnun"

            return render_template("drilldown.html", source="reykjavik", page_id="sellers",
                                   data_loaded=True, level=1, selected_year=year, selected_value=value,
                                   years=years, rows=rows, drill_label=drill_label,
                                   explorer_base=url_for('rkv_explorer'), explorer_type_param="seller", explorer_buyer_param="buyer")

    return app


app = create_app()


if __name__ == "__main__":
    create_app().run(debug=True)
