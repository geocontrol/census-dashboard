"""
Northern Ireland Census 2021 — Data Zone (DZ2021) boundaries.

NISRA publishes DZ2021 boundaries directly as GeoJSON in WGS84 (CRS84),
so no reprojection is required (unlike Scotland's BNG shapefiles).

This sub-module currently covers boundary ingestion and the LGD2014
catalogue. Indicator ingestion from NISRA bulk CSVs is a separate
sub-task and is intentionally not implemented here yet.
"""
import json
import logging
import zipfile
from pathlib import Path

import httpx
from shapely.geometry import MultiPolygon, Polygon, mapping, shape

logger = logging.getLogger(__name__)

DZ2021_BOUNDARY_GEOJSON_URL = (
    "https://www.nisra.gov.uk/files/nisra/publications/geography-dz2021-geojson.zip"
)
DZ2021_LOOKUP_XLSX_URL = (
    "https://www.nisra.gov.uk/files/nisra/documents/2025-04/"
    "geography-data-zone-and-super-data-zone-lookups-v3.xlsx"
)

TARGET_VERTICES = 30


def _simplify_ring(coords: list, target: int = TARGET_VERTICES) -> list:
    n = len(coords)
    if n <= target:
        return coords
    step = max(1, n // target)
    simplified = coords[::step]
    if simplified[0] != simplified[-1]:
        simplified.append(simplified[0])
    return simplified


def _simplify_geometry(geom):
    if geom.geom_type == "Polygon":
        exterior = _simplify_ring(list(geom.exterior.coords))
        interiors = [_simplify_ring(list(ring.coords)) for ring in geom.interiors]
        try:
            return Polygon(exterior, interiors)
        except Exception:
            return geom
    if geom.geom_type == "MultiPolygon":
        parts = []
        for part in geom.geoms:
            simplified = _simplify_geometry(part)
            if simplified.is_valid and not simplified.is_empty:
                parts.append(simplified)
        if parts:
            return MultiPolygon(parts)
    return geom


async def download_dz21_boundaries(data_dir: Path) -> Path:
    """Download NISRA DZ2021 GeoJSON, simplify, normalise properties, save."""
    output = data_dir / "boundaries_ni_dz21.geojson"
    if output.exists():
        logger.info(
            f"NI DZ2021 boundaries cached ({output.stat().st_size / 1024 / 1024:.1f} MB)"
        )
        return output

    logger.info("Downloading NI DZ2021 boundaries (~26 MB)...")
    zip_path = data_dir / "geography-dz2021-geojson.zip"

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get(DZ2021_BOUNDARY_GEOJSON_URL)
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)
        logger.info(f"  Downloaded {zip_path.stat().st_size / 1024 / 1024:.1f} MB")

    with zipfile.ZipFile(zip_path) as zf:
        geojson_name = next(
            (n for n in zf.namelist() if n.lower().endswith(".geojson")), None
        )
        if not geojson_name:
            raise FileNotFoundError("No .geojson file found in DZ2021 boundary zip")
        with zf.open(geojson_name) as f:
            raw = json.loads(f.read().decode("utf-8"))

    features = []
    for feat in raw.get("features", []):
        props = feat.get("properties", {}) or {}
        dz_code = props.get("DZ2021_cd") or props.get("DZ2021_CD")
        dz_name = props.get("DZ2021_nm") or props.get("DZ2021_NM") or dz_code
        if not dz_code:
            continue

        try:
            geom = shape(feat["geometry"])
        except Exception:
            continue
        if geom.is_empty:
            continue
        if not geom.is_valid:
            geom = geom.buffer(0)

        simplified = _simplify_geometry(geom)
        if not simplified.is_valid:
            simplified = simplified.buffer(0)

        try:
            area_ha = float(props.get("Area_ha") or 0)
        except (TypeError, ValueError):
            area_ha = 0.0
        area_km2 = round(area_ha / 100.0, 4) if area_ha else 0.0

        features.append(
            {
                "type": "Feature",
                "properties": {
                    "DZ2021CD": dz_code,
                    "DZ2021NM": dz_name,
                    "SDZ2021CD": props.get("SDZ2021_cd", ""),
                    "SDZ2021NM": props.get("SDZ2021_nm", ""),
                    "LGD2014CD": props.get("LGD2014_cd", ""),
                    "LGD2014NM": props.get("LGD2014_nm", ""),
                    "nation": "NI",
                    "area_km2": area_km2,
                },
                "geometry": mapping(simplified),
            }
        )

    geojson = {"type": "FeatureCollection", "features": features}
    output.write_text(json.dumps(geojson))
    logger.info(
        f"NI DZ2021 boundaries: {len(features)} features, "
        f"{output.stat().st_size / 1024 / 1024:.1f} MB"
    )

    try:
        zip_path.unlink()
    except Exception:
        pass

    return output


# ═══════ Northern Ireland Local Government Districts (LGD2014) ═══════
# Sourced from NISRA DZ2021 lookup table (geography-data-zone-and-super-data-zone-lookups-v3.xlsx).
# These 11 LGDs are the equivalent of Scotland's Council Areas / E&W's LADs.

NI_LOCAL_GOVERNMENT_DISTRICTS = [
    {"code": "N09000001", "name": "Antrim and Newtownabbey"},
    {"code": "N09000002", "name": "Armagh City, Banbridge and Craigavon"},
    {"code": "N09000003", "name": "Belfast"},
    {"code": "N09000004", "name": "Causeway Coast and Glens"},
    {"code": "N09000005", "name": "Derry City and Strabane"},
    {"code": "N09000006", "name": "Fermanagh and Omagh"},
    {"code": "N09000007", "name": "Lisburn and Castlereagh"},
    {"code": "N09000008", "name": "Mid and East Antrim"},
    {"code": "N09000009", "name": "Mid Ulster"},
    {"code": "N09000010", "name": "Newry, Mourne and Down"},
    {"code": "N09000011", "name": "Ards and North Down"},
]


def get_ni_lgd_dzs(lgd_code: str, dz21_geojson_path: Path) -> list[str]:
    """Return all DZ2021 codes inside a Northern Ireland Local Government District."""
    geojson = json.loads(dz21_geojson_path.read_text())
    return [
        feat["properties"]["DZ2021CD"]
        for feat in geojson.get("features", [])
        if feat.get("properties", {}).get("LGD2014CD") == lgd_code
        and feat.get("properties", {}).get("DZ2021CD")
    ]


# ═══════ Indicator mapping (placeholder for future sub-task) ═══════
# A subsequent sub-task will populate this with the same shape as
# scotland.SCOTLAND_INDICATOR_MAP, mapping E&W dataset IDs to NISRA
# table references and column extraction rules.

NI_INDICATOR_MAP: dict = {}
