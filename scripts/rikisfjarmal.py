#!/usr/bin/env python3
"""Sækja heildartölur ríkissjóðs (A1-hluta, GFS) og VLF úr opinberum heimildum.

Heimildir:
  * VLF: Hagstofa Íslands, PX-vefþjónusta (THJ01102 raun, THJ07000 spá).
  * Ríkisreikningur: PDF-skjöl skráð í reference/rikisfjarmal/rikisreikningur_heimildir.csv
    (tafla „Heildarafkoma ... samanburður við fjárlög“, dálkur ársins sjálfs).
  * Fjárlög, fjáraukalög og fjármálaáætlanir: XML-þjónusta Alþingis. Fyrir hvert
    mál eru lesin bæði frumvarp/tillaga og samþykkt útgáfa (lög í heild / þingsályktun).

Úttak (vistað í git svo vefþjónn þurfi hvorki pdftotext né niðurhal):
  reference/rikisfjarmal/heildartolur.csv  — ein lína per (skjal, ár), m.kr.
  reference/rikisfjarmal/vlf.csv           — VLF í m.kr. per ár

PDF-skjöl eru geymd í skyndiminni (data/rikisfjarmal/pdf/) og aðeins sótt einu sinni.
Krefst `pdftotext` (poppler-utils).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
REF = ROOT / "reference/rikisfjarmal"
CACHE = ROOT / "data/rikisfjarmal/pdf"

PX = "https://px.hagstofa.is/pxis/api/v1/is/Efnahagur"
VLF_RAUN = f"{PX}/thjodhagsreikningar/landsframl/1_landsframleidsla/THJ01102.px"
VLF_SPA = f"{PX}/thjodhagsspa/THJ07000.px"

ALTHINGI_MALASKRA = "https://www.althingi.is/altext/xml/thingmalalisti/?lthing={thing}"
ALTHINGI_MAL = "https://www.althingi.is/altext/xml/thingmalalisti/thingmal/?lthing={thing}&malnr={malnr}"
FYRSTA_THING = 145  # Fyrsta fjármálaáætlun (2017–2021) og fjárlög 2017 skv. lögum um opinber fjármál
# Fjárlög fyrir 2017 eru ekki á GFS-sniði (1. gr. ósambærileg) og eru því ekki lesin.
FYRSTA_GFS_AR = 2017

FRUMVARP_TEGUNDIR = {"stjórnarfrumvarp", "stjórnartillaga"}
SAMTHYKKT_TEGUNDIR = {"lög í heild", "þingsályktun", "þál. í heild"}

OUT_COLS = ["ar", "heimild", "skjal", "stada", "dags", "tekjur", "gjold", "gjold_breyting", "mal", "slod"]

session = requests.Session()
session.headers["User-Agent"] = "opin-gogn/1.0 (rikisfjarmal pipeline)"


# ---------------------------------------------------------------------------
# Hjálparföll
# ---------------------------------------------------------------------------

NUM_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})+(?:,\d+)?(?![\d.])|-?\d+(?:,\d+)?")


def parse_numbers(text: str) -> list[float]:
    """Lesa allar tölur á íslensku sniði úr línu („1.708.451,3“, „-61,9“)."""
    # Punktalínur („.......“) og bil innan talna („-2 8, 1“) úr pdftotext
    text = re.sub(r"\.{2,}", " ", text)
    return [float(m.replace(".", "").replace(",", ".")) for m in NUM_RE.findall(text)]


def label_numbers(line: str, label: str) -> list[float] | None:
    """Tölur á línu sem byrjar á `label` (t.d. „Heildartekjur“), annars None."""
    m = re.match(rf"\s*{label}\b[\s.]*(.*)$", line)
    if not m:
        return None
    return parse_numbers(m.group(1))


def fetch_pdf_text(url: str) -> str:
    CACHE.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha1(url.encode()).hexdigest()[:16]
    pdf, txt = CACHE / f"{key}.pdf", CACHE / f"{key}.txt"
    if not txt.exists():
        if not pdf.exists():
            print(f"    sæki {url}", file=sys.stderr)
            r = session.get(url, timeout=300)
            r.raise_for_status()
            if not r.content.startswith(b"%PDF"):
                raise ValueError(f"Ekki PDF: {url}")
            pdf.write_bytes(r.content)
        subprocess.run(["pdftotext", "-layout", str(pdf), str(txt)],
                       check=True, stderr=subprocess.DEVNULL)
    return txt.read_text(encoding="utf-8", errors="replace")


def first_with_numbers(lines: list[str], start: int, label: str, min_nums: int,
                       window: int = 60) -> list[float] | None:
    for line in lines[start:start + window]:
        nums = label_numbers(line, label)
        if nums is not None and len(nums) >= min_nums:
            return nums
    return None


# ---------------------------------------------------------------------------
# VLF (Hagstofa)
# ---------------------------------------------------------------------------

def px_query(url: str, code: str, value: str) -> dict:
    body = {"query": [{"code": code, "selection": {"filter": "item", "values": [value]}}],
            "response": {"format": "json-stat2"}}
    r = session.post(url, json=body, timeout=60)
    r.raise_for_status()
    d = r.json()
    years = list(d["dimension"]["Ár"]["category"]["index"].keys())
    return dict(zip((int(y) for y in years), d["value"]))


def build_vlf() -> list[dict]:
    raun = px_query(VLF_RAUN, "Skipting", "11")          # 8. Verg landsframleiðsla, m.kr.
    spa = px_query(VLF_SPA, "Flokkun", "13")             # VLF á verðlagi hvers árs, ma.kr.
    rows = [{"ar": y, "vlf": round(v), "heimild": "raun", "tafla": "THJ01102"}
            for y, v in sorted(raun.items()) if v is not None]
    last = max(raun)
    rows += [{"ar": y, "vlf": round(v * 1000), "heimild": "spa", "tafla": "THJ07000"}
             for y, v in sorted(spa.items()) if y > last and v is not None]
    return rows


# ---------------------------------------------------------------------------
# Ríkisreikningur (PDF)
# ---------------------------------------------------------------------------

RR_ANCHOR = re.compile(r"Heildarafkoma.{0,20}samanburður við fjárlög", re.I)


def parse_rikisreikningur(text: str) -> tuple[float, float]:
    """Fyrsti dálkur („Reikningsskil aðlöguð að fjárlagagrunni“) í töflu um
    heildarafkomu í samanburði við fjárlög. Krafist er a.m.k. tveggja talna á
    línunni svo efnisyfirlit og IPSAS-brúin (ein tala) séu hunsuð."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not RR_ANCHOR.search(line):
            continue
        t = first_with_numbers(lines, i, "Heildartekjur", 2, window=15)
        g = first_with_numbers(lines, i, "Heildargjöld", 2, window=60)
        if t and g:
            return t[0], g[0]
    raise ValueError("Fann ekki töflu um heildarafkomu")


def build_rikisreikningur() -> list[dict]:
    rows = []
    with (REF / "rikisreikningur_heimildir.csv").open(encoding="utf-8") as f:
        for src in csv.DictReader(f):
            print(f"  {src['skjal']}", file=sys.stderr)
            try:
                t, g = parse_rikisreikningur(fetch_pdf_text(src["slod"]))
            except Exception as exc:  # noqa: BLE001 — eitt skjal má ekki stöðva hin
                print(f"    ⚠ {exc}", file=sys.stderr)
                continue
            mult = 1000 if src["eining"] == "ma" else 1
            rows.append({"ar": int(src["ar"]), "heimild": "rikisreikningur", "skjal": src["skjal"],
                         "stada": "uppgjor", "dags": src["utgefid"],
                         "tekjur": round(t * mult, 1), "gjold": round(g * mult, 1),
                         "gjold_breyting": "", "slod": src["slod"]})
    return rows


# ---------------------------------------------------------------------------
# Alþingi: fjárlög, fjáraukalög, fjármálaáætlanir
# ---------------------------------------------------------------------------

MAL_FJARLOG = re.compile(r"^fjárlög (\d{4})$")
MAL_FJARAUKI = re.compile(r"^fjáraukalög(?: ([IVX]+))? (\d{4})$")
MAL_AAETLUN = re.compile(r"^fjármálaáætlun (?:fyrir árin )?(\d{4})\s*[-–]+\s*(\d{4})$")


def get_xml(url: str) -> ET.Element | None:
    r = session.get(url, timeout=60)
    if r.status_code != 200 or not r.content.lstrip().startswith(b"<?xml"):
        return None
    return ET.fromstring(r.content)


def thingskjol(thing: int, malnr: str) -> list[dict]:
    """Frumvarp/tillaga og samþykkt útgáfa máls (ef til)."""
    root = get_xml(ALTHINGI_MAL.format(thing=thing, malnr=malnr))
    out = []
    if root is None:
        return out
    for s in root.iter("þingskjal"):
        tegund = (s.findtext("skjalategund") or "").strip()
        if tegund in FRUMVARP_TEGUNDIR:
            stada = "frumvarp"
        elif tegund in SAMTHYKKT_TEGUNDIR:
            stada = "samthykkt"
        else:
            continue
        pdf = (s.findtext("slóð/pdf") or "").strip()
        if pdf:
            out.append({"stada": stada, "dags": (s.findtext("útbýting") or "")[:10],
                        "slod": pdf.replace("http://", "https://"), "nr": s.get("skjalsnúmer")})
    return out


def parse_fjarlog(text: str) -> tuple[float, float]:
    """1. gr.: „Heildartekjur ... 1.592.791,2“ og „Heildargjöld ... 1.620.922,2“ (m.kr.).
    Í fjárlögum 2017 eru tveir dálkar (rekstrargrunnur, sjóðshreyfingar); sá fyrri er notaður."""
    lines = text.splitlines()
    t = first_with_numbers(lines, 0, "Heildartekjur", 1, window=len(lines))
    g = first_with_numbers(lines, 0, "Heildargjöld", 1, window=len(lines))
    if not (t and g):
        raise ValueError("Fann ekki heildartekjur/-gjöld í 1. gr.")
    return t[0], g[0]


def parse_fjarauki(text: str) -> float:
    """Heildarbreyting fjárheimilda („Samtals“-lína greinarinnar sem breytir
    fjárheimildum málefnasviða). Dálkurinn „Heildarfjárheimild“ er summa dálkanna
    á undan, svo hann er fundinn þannig (dálkafjöldi er breytilegur milli skjala).
    Fjáraukalög sem breyta engum fjárheimildum (t.d. aðeins lántökuheimildum) gefa 0."""
    lines = text.splitlines()
    intro = re.compile(r"breytingar verða á \d+\. gr\. fjárlaga", re.I)
    starts = [i for i, l in enumerate(lines)
              if intro.search(l) and "fjárheimild" in " ".join(lines[i:i + 3]).lower()]
    if not starts:
        return 0.0
    for start in starts:
        for line in lines[start:start + 150]:
            nums = label_numbers(line, "Samtals")
            if nums and not any(nums):
                return 0.0  # tilfærslur milli málefnasviða, nettó breyting engin
            if not nums or len(nums) < 2:
                continue
            for i in range(1, len(nums)):
                if abs(sum(nums[:i]) - nums[i]) < 0.15 and any(nums[:i]):
                    return nums[i]
    raise ValueError("Fann ekki samtölu breytinga á fjárheimildum")


AAETLUN_ANCHOR = re.compile(r"Rekstraryfirlit fyrir ríkissjóð \(A1-hluta\) árin (\d{4})\s*[-–]\s*(\d{4})")


def parse_aaetlun(text: str) -> dict[int, tuple[float, float]]:
    """Rekstraryfirlit A1-hluta í tillögugreininni (ma.kr., eitt gildi per ár)."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = AAETLUN_ANCHOR.search(line)
        if not m:
            continue
        years = list(range(int(m.group(1)), int(m.group(2)) + 1))
        t = first_with_numbers(lines, i, "Heildartekjur", len(years), window=12)
        g = first_with_numbers(lines, i, "Heildargjöld", len(years), window=50)
        if t and g:
            return {y: (t[k] * 1000, g[k] * 1000) for k, y in enumerate(years)}
    raise ValueError("Fann ekki rekstraryfirlit A1-hluta")


def build_althingi(last_thing: int | None) -> list[dict]:
    rows = []
    thing = FYRSTA_THING
    while last_thing is None or thing <= last_thing:
        root = get_xml(ALTHINGI_MALASKRA.format(thing=thing))
        if root is None or root.find("mál") is None:
            break
        for mal in root.iter("mál"):
            heiti = (mal.findtext("málsheiti") or "").strip()
            malnr = mal.get("málsnúmer")
            if (m := MAL_FJARLOG.match(heiti)):
                kind, ar = "fjarlog", int(m.group(1))
                skjal = f"Fjárlög {ar}"
            elif (m := MAL_FJARAUKI.match(heiti)):
                kind, ar = "fjaraukalog", int(m.group(2))
                skjal = f"Fjáraukalög {m.group(1) + ' ' if m.group(1) else ''}{ar}"
            elif (m := MAL_AAETLUN.match(heiti)):
                kind, ar = "fjarmalaaaetlun", None
                skjal = f"Fjármálaáætlun {m.group(1)}–{m.group(2)}"
            else:
                continue
            if ar is not None and ar < FYRSTA_GFS_AR:
                continue
            for doc in thingskjol(thing, malnr):
                label = f"{skjal} ({'frumvarp' if doc['stada'] == 'frumvarp' else 'samþykkt'}, þskj. {doc['nr']}/{thing})"
                print(f"  {label}", file=sys.stderr)
                try:
                    text = fetch_pdf_text(doc["slod"])
                    base = {"heimild": kind, "skjal": skjal, "stada": doc["stada"],
                            "dags": doc["dags"], "slod": doc["slod"], "mal": f"{thing}/{malnr}"}
                    if kind == "fjarlog":
                        t, g = parse_fjarlog(text)
                        rows.append({**base, "ar": ar, "tekjur": t, "gjold": g, "gjold_breyting": ""})
                    elif kind == "fjaraukalog":
                        rows.append({**base, "ar": ar, "tekjur": "", "gjold": "",
                                     "gjold_breyting": parse_fjarauki(text)})
                    else:
                        for y, (t, g) in parse_aaetlun(text).items():
                            rows.append({**base, "ar": y, "tekjur": round(t, 1),
                                         "gjold": round(g, 1), "gjold_breyting": ""})
                except Exception as exc:  # noqa: BLE001
                    print(f"    ⚠ {exc}", file=sys.stderr)
        thing += 1
    return rows


# ---------------------------------------------------------------------------

def write_csv(path: Path, rows: list[dict], cols: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--last-thing", type=int, help="Síðasta löggjafarþing (sjálfgefið: þar til ekkert finnst)")
    ap.add_argument("--skip-vlf", action="store_true")
    args = ap.parse_args()

    REF.mkdir(parents=True, exist_ok=True)
    if not args.skip_vlf:
        print("==> VLF (Hagstofa)", file=sys.stderr)
        vlf = build_vlf()
        write_csv(REF / "vlf.csv", vlf, ["ar", "vlf", "heimild", "tafla"])
        print(f"    {len(vlf)} ár", file=sys.stderr)

    print("==> Ríkisreikningur", file=sys.stderr)
    rows = build_rikisreikningur()
    print("==> Alþingi", file=sys.stderr)
    rows += build_althingi(args.last_thing)

    with (REF / "handvirkt.csv").open(encoding="utf-8") as f:
        rows += list(csv.DictReader(f))

    rows.sort(key=lambda r: (int(r["ar"]), r["heimild"], r["dags"] or "", r["skjal"]))
    write_csv(REF / "heildartolur.csv", rows, OUT_COLS)
    print(f"==> {len(rows)} línur í {REF / 'heildartolur.csv'}", file=sys.stderr)


if __name__ == "__main__":
    main()
