"""
Election data — GE 2024 constituency results overlaid on a Leaflet layer.

Data source: UK Parliament psephology SQLite database (GitHub, ~4.7 MB).
Boundaries:  ONS ArcGIS FeatureServer (Westminster 2024 BGC, ~21 MB).

Phase A: General Election 2024 only.
Future: local elections (Phase B), historical GE (Phase C).
"""

import json
import logging
import sqlite3
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

# UK Parliament psephology SQLite database (powers electionresults.parliament.uk)
_PSEPHOLOGY_DB_URL = (
    "https://raw.githubusercontent.com/ukparliament/psephology-datasette"
    "/main/psephology.db"
)

# psephology general_elections.id for each polling date
_GE_IDS = {"2024": 6}

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
    "APNI":     {"name": "Alliance",        "colour": "#F6CB2F"},
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
    batch = 200

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


async def download_psephology_db(data_dir: Path) -> Path:
    """Download the Parliament psephology SQLite database from GitHub."""
    out_file = data_dir / "elections_psephology.db"
    if out_file.exists():
        logger.info("Psephology DB already cached")
        return out_file

    logger.info("Downloading psephology DB from GitHub (~4.7 MB)…")
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get(_PSEPHOLOGY_DB_URL)
        resp.raise_for_status()

    out_file.write_bytes(resp.content)
    logger.info(f"Psephology DB cached: {out_file.stat().st_size / 1024:.0f} KB")
    return out_file


def process_ge_results(db_path: Path, year: str = "2024") -> dict[str, dict]:
    """
    Query psephology.db for GE results.
    Returns dict keyed by ONS constituency code (PCON24CD).
    """
    ge_id = _GE_IDS.get(year)
    if ge_id is None:
        raise ValueError(f"GE year {year} not in psephology DB mapping")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Per-constituency headline figures + winning party
    headline_sql = """
        SELECT
            ca.geographic_code  AS ons_id,
            cg.name             AS constituency_name,
            e.valid_vote_count  AS valid_votes,
            e.majority,
            el.population_count AS electorate,
            pp.abbreviation     AS first_party
        FROM elections e
        JOIN constituency_groups cg  ON e.constituency_group_id = cg.id
        JOIN constituency_areas  ca  ON cg.constituency_area_id  = ca.id
        LEFT JOIN electorates    el  ON e.electorate_id           = el.id
        LEFT JOIN candidacies    win ON win.election_id = e.id AND win.is_winning_candidacy = 1
        LEFT JOIN certifications wc  ON wc.candidacy_id = win.id AND wc.adjunct_to_certification_id IS NULL
        LEFT JOIN political_parties pp ON pp.id = wc.political_party_id
        WHERE e.general_election_id = ?
    """

    # Per-candidacy vote counts
    votes_sql = """
        SELECT
            ca.geographic_code  AS ons_id,
            pp.abbreviation     AS party,
            cand.vote_count
        FROM elections e
        JOIN constituency_groups cg  ON e.constituency_group_id = cg.id
        JOIN constituency_areas  ca  ON cg.constituency_area_id  = ca.id
        JOIN candidacies         cand ON cand.election_id = e.id
        LEFT JOIN certifications cert ON cert.candidacy_id = cand.id AND cert.adjunct_to_certification_id IS NULL
        LEFT JOIN political_parties pp ON pp.id = cert.political_party_id
        WHERE e.general_election_id = ?
          AND cand.vote_count IS NOT NULL
    """

    # Build per-constituency vote count dict first
    vote_counts_by_code: dict[str, dict[str, int]] = {}
    for row in con.execute(votes_sql, (ge_id,)):
        code = row["ons_id"]
        abbr = row["party"] or "Ind"
        votes = row["vote_count"] or 0
        if code not in vote_counts_by_code:
            vote_counts_by_code[code] = {}
        vote_counts_by_code[code][abbr] = vote_counts_by_code[code].get(abbr, 0) + votes

    results: dict[str, dict] = {}
    for row in con.execute(headline_sql, (ge_id,)):
        code = row["ons_id"]
        valid_votes = row["valid_votes"]
        electorate = row["electorate"]
        majority = row["majority"]
        first_party = row["first_party"] or "Ind"

        vote_counts = vote_counts_by_code.get(code, {})
        vote_share: dict[str, float] = {}
        if valid_votes:
            for abbr, count in vote_counts.items():
                vote_share[abbr] = round((count / valid_votes) * 100, 1)

        results[code] = {
            "constituency_name": row["constituency_name"],
            "first_party": first_party,
            "vote_counts": vote_counts,
            "vote_share": vote_share,
            "electorate": electorate,
            "valid_votes": valid_votes,
            "majority": majority,
            "majority_pct": round((majority / valid_votes) * 100, 1) if majority and valid_votes else None,
            "turnout": round((valid_votes / electorate) * 100, 1) if electorate and valid_votes else None,
        }

    con.close()
    logger.info(f"GE {year} results loaded from psephology DB: {len(results)} constituencies")
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
