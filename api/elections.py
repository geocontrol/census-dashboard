"""
Election data — GE 2024 constituency results overlaid on a Leaflet layer.

Data source: UK Parliament psephology SQLite database (GitHub, ~4.7 MB).
Boundaries:  ONS ArcGIS FeatureServer (Westminster 2024 BGC, ~21 MB).

Phase A: General Election 2024 only.
Future: local elections (Phase B), historical GE (Phase C).
"""

import asyncio
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
_GE_IDS = {
    "2024": 6,
    "2019": 5,  # notional results on 2024 boundaries
}

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

ELECTIONS_AVAILABLE = [
    {"type": "ge",    "year": "2024", "label": "General Election 2024"},
    {"type": "ge",    "year": "2019", "label": "General Election 2019"},
    {"type": "local", "year": "2024", "label": "Local Elections 2024 (England)"},
]

# ── ONS ArcGIS — Electoral Wards May 2024, BGC ─────────────────────────────
_WD24_ARCGIS_URL = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
    "/Wards_May_2024_Boundaries_UK_BGC/FeatureServer/0/query"
)

# Democracy Club result_sets API
_DC_RESULTS_URL = "https://candidates.democracyclub.org.uk/api/v0.9/result_sets/"

# Full party name → abbreviation (Democracy Club uses full names)
_DC_PARTY_MAP: dict[str, str] = {
    "labour party": "Lab",
    "labour and co-operative party": "Lab",
    "the labour party": "Lab",
    "conservative and unionist party": "Con",
    "conservative party": "Con",
    "the conservative party": "Con",
    "liberal democrats": "LD",
    "liberal democrat": "LD",
    "reform uk": "RUK",
    "green party": "Green",
    "the green party": "Green",
    "green party of england and wales": "Green",
    "scottish national party (snp)": "SNP",
    "scottish national party": "SNP",
    "plaid cymru - the party of wales": "PC",
    "plaid cymru": "PC",
    "sinn féin": "SF",
    "sinn fein": "SF",
    "democratic unionist party - dup": "DUP",
    "democratic unionist party": "DUP",
    "social democratic and labour party": "SDLP",
    "alliance - alliance party of northern ireland": "APNI",
    "alliance party of northern ireland": "APNI",
    "ulster unionist party": "UUP",
    "traditional unionist voice - tuv": "TUV",
    "traditional unionist voice": "TUV",
    "uk independence party (ukip)": "UKIP",
    "uk independence party": "UKIP",
    "workers party of britain": "WPB",
    "the workers party": "WPB",
    "speaker seeking re-election": "Spk",
    "independent": "Ind",
    "independents for frome": "Ind",
}


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


# ── Phase B: Local elections ────────────────────────────────────────────────

async def download_ward_boundaries(data_dir: Path) -> Path:
    """Download May 2024 ward boundaries from ONS ArcGIS (8,396 features)."""
    out_file = data_dir / "elections_wd24_bgc.geojson"
    if out_file.exists():
        logger.info("Ward boundaries already cached")
        return out_file

    logger.info("Downloading ward boundaries from ONS ArcGIS (8,396 features)…")
    features = []
    offset = 0
    batch = 200

    async with httpx.AsyncClient(timeout=120.0) as client:
        while True:
            params = {
                "where": "1=1",
                "outFields": "WD24CD,WD24NM",
                "returnGeometry": "true",
                "f": "geojson",
                "resultRecordCount": batch,
                "resultOffset": offset,
            }
            resp = await client.get(_WD24_ARCGIS_URL, params=params)
            resp.raise_for_status()
            page = resp.json()
            page_features = page.get("features", [])
            features.extend(page_features)
            logger.info(f"  Ward boundaries: {len(features)} fetched…")
            if len(page_features) < batch:
                break
            offset += batch

    geojson = {"type": "FeatureCollection", "features": features}
    out_file.write_text(json.dumps(geojson))
    logger.info(f"Ward boundaries cached: {len(features)} features, {out_file.stat().st_size / 1024 / 1024:.1f} MB")
    return out_file


async def download_local_results(data_dir: Path, election_date: str = "2024-05-02") -> Path:
    """
    Download Democracy Club result_sets for English local elections on election_date.
    Caches as JSON. 2,013 records ≈ 21 API pages.
    """
    out_file = data_dir / f"elections_local_{election_date.replace('-', '')}_results.json"
    if out_file.exists():
        logger.info(f"Local results {election_date} already cached")
        return out_file

    logger.info(f"Downloading local election results {election_date} from Democracy Club…")
    all_results = []
    url = _DC_RESULTS_URL
    params = {
        "ballot_paper_id__startswith": "local.",
        "election_date": election_date,
        "format": "json",
        "page_size": 100,
    }

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        while url:
            for attempt in range(3):
                resp = await client.get(url, params=params)
                if resp.status_code == 429:
                    wait = 60 if attempt == 0 else 120
                    logger.warning(f"  Rate limited, waiting {wait}s before retry…")
                    await asyncio.sleep(wait)
                else:
                    break
            resp.raise_for_status()
            data = resp.json()
            all_results.extend(data.get("results", []))
            url = data.get("next")
            params = {}  # next URL already has params encoded
            logger.info(f"  Local results: {len(all_results)} fetched…")
            await asyncio.sleep(1.5)  # 1.5s between pages ≈ 31s for 21 pages

    out_file.write_text(json.dumps(all_results))
    logger.info(f"Local results cached: {len(all_results)} result sets")
    return out_file


def _normalise_party(name: str) -> str:
    """Map full Democracy Club party name to our abbreviation."""
    return _DC_PARTY_MAP.get(name.strip().lower(), "Other")


def process_local_results(results_path: Path) -> dict[str, dict]:
    """
    Aggregate Democracy Club result_sets into per-ward results.

    Multi-member wards: multiple ballot papers for same ward GSS code are
    aggregated — dominant party is determined by seat count.

    Returns dict keyed by ward GSS code (e.g. 'E05001153').
    """
    from collections import defaultdict

    raw: list[dict] = json.loads(results_path.read_text())

    # Accumulate per ward
    wards: dict[str, dict] = {}  # gss_code → aggregated data

    for result_set in raw:
        # Extract ward GSS code from first candidate's post id
        candidates = result_set.get("candidate_results", [])
        if not candidates:
            continue

        first = candidates[0]
        post_id: str = (first.get("membership") or {}).get("post", {}).get("id", "")
        gss = post_id.replace("gss:", "").strip()
        if not gss:
            continue

        ward_name = (first.get("membership") or {}).get("post", {}).get("label", gss)
        turnout_str = result_set.get("turnout_percentage", "")
        turnout = _parse_pct(turnout_str)
        electorate = result_set.get("total_electorate")
        total_votes = result_set.get("num_turnout_reported")

        if gss not in wards:
            wards[gss] = {
                "ward_name": ward_name,
                "seats_by_party": defaultdict(int),
                "votes_by_party": defaultdict(int),
                "total_votes": 0,
                "electorate": electorate,
                "turnout": turnout,
                "winners": [],
            }

        w = wards[gss]
        if total_votes:
            w["total_votes"] = (w["total_votes"] or 0) + total_votes
        if electorate and not w["electorate"]:
            w["electorate"] = electorate
        if turnout and not w["turnout"]:
            w["turnout"] = turnout

        for cand in candidates:
            mem = cand.get("membership") or {}
            party_name = (mem.get("on_behalf_of") or {}).get("name", "")
            abbr = _normalise_party(party_name)
            votes = cand.get("num_ballots") or 0
            is_winner = cand.get("is_winner", False)

            w["votes_by_party"][abbr] += votes
            if is_winner:
                w["seats_by_party"][abbr] += 1
                w["winners"].append({"party": abbr, "name": (mem.get("person") or {}).get("name", "")})

    # Flatten into output format
    results: dict[str, dict] = {}
    for gss, w in wards.items():
        seats = dict(w["seats_by_party"])
        votes = dict(w["votes_by_party"])
        total = w["total_votes"] or sum(votes.values())

        # Dominant party = most seats; tie-break by votes
        if seats:
            first_party = max(seats, key=lambda p: (seats[p], votes.get(p, 0)))
        else:
            first_party = max(votes, key=votes.get) if votes else "Other"

        vote_share: dict[str, float] = {}
        if total:
            for abbr, count in votes.items():
                vote_share[abbr] = round((count / total) * 100, 1)

        total_seats = sum(seats.values())
        majority = None
        majority_pct = None
        if total_seats == 1 and len(seats) >= 1:
            sorted_votes = sorted(votes.values(), reverse=True)
            if len(sorted_votes) >= 2:
                majority = sorted_votes[0] - sorted_votes[1]
                if total:
                    majority_pct = round((majority / total) * 100, 1)

        results[gss] = {
            "ward_name": w["ward_name"],
            "first_party": first_party,
            "seats_by_party": seats,
            "total_seats": total_seats,
            "vote_counts": votes,
            "vote_share": vote_share,
            "winners": w["winners"],
            "electorate": w["electorate"],
            "total_votes": total,
            "majority": majority,
            "majority_pct": majority_pct,
            "turnout": w["turnout"],
        }

    logger.info(f"Local results processed: {len(results)} wards")
    return results


def build_local_overlay(boundaries_path: Path, results: dict[str, dict]) -> dict:
    """Merge ward boundaries with local election results."""
    geojson = json.loads(boundaries_path.read_text())
    matched = 0
    for feature in geojson.get("features", []):
        code = (feature.get("properties") or {}).get("WD24CD", "")
        result = results.get(code)
        if result:
            feature["properties"].update(result)
            matched += 1
    logger.info(f"Local overlay: {matched}/{len(geojson.get('features', []))} wards matched to results")
    return geojson


# ── Phase C: Swing ─────────────────────────────────────────────────────────

def compute_ge_swing(results_new: dict, results_old: dict) -> dict[str, dict]:
    """
    Compute per-constituency swing between two GE results dicts.

    Swing formula: (Lab_change - Con_change) / 2
    Positive = swing to Labour, Negative = swing to Conservative.

    Also stores raw per-party share changes and the old results for the
    comparison panel in the frontend.
    """
    swing: dict[str, dict] = {}

    for code, r_new in results_new.items():
        r_old = results_old.get(code)
        if not r_old:
            continue

        vs_new = r_new.get("vote_share", {})
        vs_old = r_old.get("vote_share", {})

        lab_new = vs_new.get("Lab", 0.0)
        lab_old = vs_old.get("Lab", 0.0)
        con_new = vs_new.get("Con", 0.0)
        con_old = vs_old.get("Con", 0.0)

        swing_val = round((lab_new - lab_old - (con_new - con_old)) / 2, 1)

        # Per-party share changes for all parties present in either election
        share_changes: dict[str, float] = {}
        for party in set(vs_new) | set(vs_old):
            delta = round(vs_new.get(party, 0.0) - vs_old.get(party, 0.0), 1)
            if abs(delta) >= 0.1:
                share_changes[party] = delta

        swing[code] = {
            "swing_to_lab": swing_val,
            "vote_share_prev": vs_old,
            "first_party_prev": r_old.get("first_party", ""),
            "share_changes": share_changes,
        }

    return swing


def embed_swing_in_overlay(overlay: dict, swing_data: dict) -> dict:
    """Merge swing properties into an existing GE overlay's features in-place."""
    matched = 0
    for feature in overlay.get("features", []):
        code = (feature.get("properties") or {}).get("PCON24CD", "")
        s = swing_data.get(code)
        if s:
            feature["properties"].update(s)
            matched += 1
    logger.info(f"Swing data embedded: {matched} constituencies")
    return overlay


def _parse_pct(s: object) -> Optional[float]:
    try:
        return round(float(str(s).replace("%", "").strip()), 1)
    except (ValueError, TypeError):
        return None


def _to_int(s: object) -> Optional[int]:
    try:
        return int(str(s).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
