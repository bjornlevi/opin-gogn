#!/usr/bin/env python3
"""Fetch current government institutions and compare them to rikid buyers."""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

import duckdb
import requests


SOURCE_URL = "https://www.stjornarradid.is/raduneyti/stofnanir/"

MANUAL_ALIASES = {
    "Embætti landlæknis": ("Landlæknir", "alias"),
    "Gljúfrasteinn: hús skáldsins": ("Gljúfrasteinn - Hús skáldsins", "alias"),
    "Hæstiréttur Íslands": ("Hæstiréttur", "alias"),
    "Hafrannsóknastofnun - Rannsókna- og ráðgjafarstofnun hafs og vatna": ("Hafrannsóknastofnun", "alias"),
    "Framkvæmdasýslan - Ríkiseignir": ("Ríkiseignir", "predecessor"),
    "Framhaldsskólinn í Austur-Skaftafellssýslu": ("Framhaldsskólinn í A-Skaftafellssýslu", "alias"),
    "Sjúkrahúsið Akureyri": ("Sjúkrahúsið á Akureyri", "alias"),
    "Tilraunastöð Háskóla Íslands í meinafræði að Keldum": ("Tilraunastöð Háskólans að Keldum", "alias"),
    "Vegagerðin": ("Vegagerðin, rekstur", "alias"),
    "Náttúrufræðistofnun": ("Náttúrufræðistofnun Íslands", "alias"),
    "Land og skógur": ("Land og skógur", "alias"),
    "Skatturinn": ("Ríkisskattstjóri", "predecessor"),
    "Húsnæðis-, mannvirkja- og skipulagsstofnun": ("Mannvirkjastofnun", "predecessor"),
}

KEYWORD_PREDECESSORS = {
    "Héraðsdómur Austurlands": "Héraðsdómstólar",
    "Héraðsdómur Norðurlands eystra": "Héraðsdómstólar",
    "Héraðsdómur Norðurlands vestra": "Héraðsdómstólar",
    "Héraðsdómur Reykjaness": "Héraðsdómstólar",
    "Héraðsdómur Reykjavíkur": "Héraðsdómstólar",
    "Héraðsdómur Suðurlands": "Héraðsdómstólar",
    "Héraðsdómur Vestfjarða": "Héraðsdómstólar",
    "Héraðsdómur Vesturlands": "Héraðsdómstólar",
    "Sýslumaðurinn á Austurlandi": "Sýslumaður Austurlands",
    "Sýslumaðurinn á Höfuðborgarsvæðinu": "Sýslumaður höfuðborgarsvæðisins",
    "Sýslumaðurinn á Norðurlandi eystra": "Sýslumaður Norðurlands eystra",
    "Sýslumaðurinn á Norðurlandi vestra": "Sýslumaður Norðurlands vestra",
    "Sýslumaðurinn á Suðurlandi": "Sýslumaður Suðurlands",
    "Sýslumaðurinn á Suðurnesjum": "Sýslumaður Suðurnesja",
    "Sýslumaðurinn á Vestfjörðum": "Sýslumaður Vestfjarða",
    "Sýslumaðurinn á Vesturlandi": "Sýslumaður Vesturlands",
    "Sýslumaðurinn í Vestmannaeyjum": "Sýslumaður Vestmannaeyja",
}


def normalize(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("&", " og ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\b(hf|ohf|ehf|ses|slf)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_institutions() -> list[dict[str, str]]:
    html = requests.get(SOURCE_URL, timeout=60).text
    row_pattern = re.compile(
        r'<tr class="col\d+"><td headers="id1">(.*?)</td><td headers="id3">(.*?)</td><td headers="id5">(.*?)</td></tr>',
        re.S,
    )
    out = []
    seen = set()
    for inst_html, ministry_html, link_html in row_pattern.findall(html):
        institution = re.sub(r"<.*?>", "", inst_html)
        ministry = re.sub(r"<.*?>", "", ministry_html)
        link_match = re.search(r'href="([^"]+)"', link_html)
        website = link_match.group(1).strip() if link_match else ""
        institution = re.sub(r"\s+", " ", unescape(institution).replace("\xa0", " ")).strip()
        ministry = re.sub(r"\s+", " ", unescape(ministry).replace("\xa0", " ")).strip()
        if institution and institution not in seen:
            seen.add(institution)
            out.append({"institution": institution, "ministry": ministry, "website": website})
    return out


def classify(institution: str, buyers: list[str], buyers_by_norm: dict[str, list[str]]) -> tuple[str, str, str]:
    if institution in buyers:
        return "exact", institution, ""

    alias = MANUAL_ALIASES.get(institution)
    if alias:
        match_name, match_type = alias
        return match_type, match_name, "Handvirkt varpanir"

    norm_inst = normalize(institution)
    if norm_inst in buyers_by_norm:
        return "alias", buyers_by_norm[norm_inst][0], "Sama heiti eftir einföldun"

    predecessor = KEYWORD_PREDECESSORS.get(institution)
    if predecessor:
        return "predecessor", predecessor, "Núverandi stofnun virðist birtast undir eldra eða sameinuðu heiti"

    return "missing", "", ""


def build_rows(rikid_path: Path) -> list[dict[str, str]]:
    institutions = fetch_institutions()
    con = duckdb.connect()
    buyers = [r[0] for r in con.execute(
        f"""select distinct "Kaupandi"
            from read_parquet('{str(rikid_path).replace("'", "''")}')
            where "Kaupandi" is not null and (is_correction = false or is_correction is null)
            order by 1"""
    ).fetchall()]
    buyers_by_norm: dict[str, list[str]] = {}
    for buyer in buyers:
        buyers_by_norm.setdefault(normalize(buyer), []).append(buyer)

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for item in institutions:
        match_type, rikid_match, note = classify(item["institution"], buyers, buyers_by_norm)
        rows.append({
            "institution": item["institution"],
            "ministry": item["ministry"],
            "website": item["website"],
            "match_type": match_type,
            "rikid_match": rikid_match,
            "note": note,
            "source_url": SOURCE_URL,
            "fetched_at_utc": fetched_at,
        })
    return rows


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "institution",
                "ministry",
                "website",
                "match_type",
                "rikid_match",
                "note",
                "source_url",
                "fetched_at_utc",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh current government institution reconciliation against rikid")
    parser.add_argument("--rikid", default="data/rikid/parquet/opnirreikningar_with_corrections.parquet")
    parser.add_argument("--output", default="data/rikisreikningur/processed/rikid_institutions_reconciliation.csv")
    args = parser.parse_args()

    rikid_path = Path(args.rikid)
    if not rikid_path.exists():
        print(f"rikid file not found: {rikid_path}", file=sys.stderr)
        sys.exit(1)

    rows = build_rows(rikid_path)
    write_csv(rows, Path(args.output))
    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
