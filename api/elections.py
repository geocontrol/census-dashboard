"""
Election data — GE 2024 constituency results overlaid on a Leaflet layer.

Phase A: General Election 2024 only.
Future: local elections (Phase B), historical GE (Phase C).
"""

import csv
import json
import logging
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ONS ArcGIS FeatureServer — Westminster Parliamentary Constituencies July 2024, BGC
_PCON24_ARCGIS_URL = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
    "/Westminster_Parliamentary_Constituencies_July_2024_Boundaries_UK_BGC"
    "/FeatureServer/0/query"
)

# House of Commons Library CBP-10009 — GE 2024 results CSV
_GE2024_RESULTS_URL = (
    "https://researchbriefings.files.parliament.uk/documents/CBP-10009"
    "/HoC-GE2024-results-by-constituency.csv"
)

# Party abbreviation → display name + brand colour
PARTY_META: dict[str, dict] = {
    "Lab":      {"name": "Labour",          "colour": "#E4003B"},
    "Con":      {"name": "Conservative",    "colour": "#0087DC"},
    "LD":       {"name": "Liberal Democrat","colour": "#FAA61A"},
    "RUK":      {"name": "Reform UK",       "colour": "#12B6CF"},
    "Green":    {"name": "Green",           "colour": "#00B140"},
    "SNP":      {"name": "SNP",             "colour": "#FDF38E"},
    "PC":       {"name": "Plaid Cymru",     "colour": "#005B54"},
    "SF":       {"name": "Sinn Féin",       "colour": "#326760"},
    "DUP":      {"name": "DUP",             "colour": "#D46A4C"},
    "SDLP":     {"name": "SDLP",            "colour": "#2AA82C"},
    "Alliance": {"name": "Alliance",        "colour": "#F6CB2F"},
    "UUP":      {"name": "UUP",             "colour": "#48A5EE"},
    "TUV":      {"name": "TUV",             "colour": "#0C3A6A"},
    "Ind":      {"name": "Independent",     "colour": "#AAAAAA"},
    "Spk":      {"name": "Speaker",         "colour": "#909090"},
}

GE_AVAILABLE = [
    {"year": "2024", "label": "General Election 2024"},
]


async def download_constituency_boundaries(data_dir: Path) -> Path:
    out_file = data_dir / "elections_pcon24_bgc.geojson"
    if out_file.exists():
        logger.info("Constituency boundaries already cached")
        return out_file

    logger.info("Downloading 2024 constituency boundaries from ONS ArcGIS…")
    features = []
    offset = 0
    batch = 200  # ArcGIS FeatureServer hard-caps at 2 000; 200 is conservative

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            params = {
                "where": "1=1",
                "outFields": "PCON24CD,PCON24NM",
                "returnGeometry": "true",
                "f": "geojson",
                "resultRecordCount": batch,
                "resultOffset": offset,
            }
            resp = await client.get(_PCON24_ARCGIS_URL, params=params)
            resp.raise_for_status()
            page = resp.json()
            page_features = page.get("features", [])
            features.extend(page_features)
            if len(page_features) < batch:
                break
            offset += batch

    geojson = {"type": "FeatureCollection", "features": features}
    out_file.write_text(json.dumps(geojson))
    logger.info(f"Constituency boundaries cached: {len(features)} features, {out_file.stat().st_size / 1024:.0f} KB")
    return out_file


async def download_ge_results(data_dir: Path, year: str = "2024") -> Path:
    out_file = data_dir / f"elections_ge{year}_results.csv"
    if out_file.exists():
        logger.info(f"GE {year} results already cached")
        return out_file

    if year != "2024":
        raise ValueError(f"GE year {year} not yet supported")

    logger.info(f"Downloading GE {year} results from Commons Library…")
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(_GE2024_RESULTS_URL)
        resp.raise_for_status()

    out_file.write_bytes(resp.content)
    logger.info(f"GE {year} results cached: {out_file.stat().st_size / 1024:.0f} KB")
    return out_file


def process_ge_results(csv_path: Path) -> dict[str, dict]:
    """
    Parse the Commons Library GE results CSV.

    Expected columns (may vary by year):
      ons_id, constituency_name, first_party, electorate, valid_votes, majority,
      plus one column per party abbreviation containing vote counts.

    Returns dict keyed by PCON24CD (ons_id).
    """
    results: dict[str, dict] = {}
    known_parties = set(PARTY_META.keys())

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = list(reader.fieldnames or [])
        vote_cols = [h for h in headers if h in known_parties]

        for row in reader:
            code = row.get("ons_id", "").strip()
            if not code:
                continue

            electorate = _to_int(row.get("electorate"))
            valid_votes = _to_int(row.get("valid_votes"))
            majority = _to_int(row.get("majority"))
            first_party = row.get("first_party", "").strip()

            vote_counts: dict[str, int] = {}
            for col in vote_cols:
                v = _to_int(row.get(col))
                if v is not None and v > 0:
                    vote_counts[col] = v

            vote_share: dict[str, float] = {}
            if valid_votes:
                for party, count in vote_counts.items():
                    vote_share[party] = round((count / valid_votes) * 100, 1)

            turnout = round((valid_votes / electorate) * 100, 1) if electorate and valid_votes else None
            majority_pct = round((majority / valid_votes) * 100, 1) if majority and valid_votes else None

            results[code] = {
                "constituency_name": row.get("constituency_name", "").strip(),
                "first_party": first_party,
                "vote_counts": vote_counts,
                "vote_share": vote_share,
                "electorate": electorate,
                "valid_votes": valid_votes,
                "majority": majority,
                "majority_pct": majority_pct,
                "turnout": turnout,
            }

    return results


def build_ge_overlay(boundaries_path: Path, results: dict[str, dict]) -> dict:
    """Merge constituency boundaries with results, returning a GeoJSON FeatureCollection."""
    geojson = json.loads(boundaries_path.read_text())
    for feature in geojson.get("features", []):
        code = (feature.get("properties") or {}).get("PCON24CD", "")
        result = results.get(code)
        if result:
            feature["properties"].update(result)
    return geojson


def _to_int(s: object) -> Optional[int]:
    try:
        return int(str(s).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
