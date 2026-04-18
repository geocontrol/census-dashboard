"""
Northern Ireland Census 2021 — Data Zone (DZ2021) boundaries and
population data (MS-A01).

NISRA publishes DZ2021 boundaries directly as GeoJSON in WGS84 (CRS84),
so no reprojection is required (unlike Scotland's BNG shapefiles). The
MS-A01 usual-resident-population table is published as XLSX with a
clean DZ-level sheet, so no OA→DZ aggregation is needed either.

Indicator ingestion from NISRA cross-tab CSVs is a separate sub-task
and is intentionally not implemented here yet.
"""
import json
import logging
import zipfile
from pathlib import Path
from typing import Optional

import httpx
from openpyxl import load_workbook
from shapely.geometry import MultiPolygon, Polygon, mapping, shape

logger = logging.getLogger(__name__)

DZ2021_BOUNDARY_GEOJSON_URL = (
    "https://www.nisra.gov.uk/files/nisra/publications/geography-dz2021-geojson.zip"
)
DZ2021_LOOKUP_XLSX_URL = (
    "https://www.nisra.gov.uk/files/nisra/documents/2025-04/"
    "geography-data-zone-and-super-data-zone-lookups-v3.xlsx"
)
MS_A01_URL = "https://www.nisra.gov.uk/system/files/statistics/census-2021-ms-a01.xlsx"

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


# ═══════ Population data (MS-A01) ═══════


async def download_ni_population_xlsx(data_dir: Path) -> Path:
    """Download NISRA MS-A01 (Usual resident population by Data Zone)."""
    output = data_dir / "ni_ms_a01.xlsx"
    if output.exists():
        logger.info(f"NI MS-A01 cached ({output.stat().st_size / 1024:.0f} KB)")
        return output

    logger.info("Downloading NI MS-A01 population (~470 KB)...")
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(MS_A01_URL)
        resp.raise_for_status()
        output.write_bytes(resp.content)
        logger.info(f"  Downloaded {output.stat().st_size / 1024:.0f} KB")
    return output


def parse_ni_population_dz(xlsx_path: Path) -> dict[str, int]:
    """Parse the DZ sheet of MS-A01. Returns {dz_code: usual_residents}."""
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb["DZ"]

    populations: dict[str, int] = {}
    for row in ws.iter_rows(min_row=7, values_only=True):
        if not row or len(row) < 3:
            continue
        code = (row[1] or "").strip() if isinstance(row[1], str) else None
        if not code or not code.startswith("N20"):
            continue
        try:
            populations[code] = int(row[2])
        except (TypeError, ValueError):
            continue
    return populations


def _stats(values: dict[str, float]) -> dict:
    sv = sorted(values.values())
    n = len(sv)
    if n == 0:
        return {}
    return {
        "min": sv[0], "max": sv[-1],
        "mean": round(sum(sv) / n, 2),
        "p10": sv[int(n * 0.1)], "p25": sv[int(n * 0.25)],
        "p50": sv[int(n * 0.5)], "p75": sv[int(n * 0.75)],
        "p90": sv[int(n * 0.9)], "count": n,
    }


def compute_ni_population_data(
    dz21_geojson_path: Path,
    ms_a01_xlsx_path: Path,
) -> dict[str, dict]:
    """Build population_total + population_density payloads for NI Data Zones.

    Population comes from MS-A01; area_km2 comes from the boundary GeoJSON
    (already populated during boundary ingestion).
    """
    geojson = json.loads(dz21_geojson_path.read_text())
    areas: dict[str, float] = {}
    names: dict[str, str] = {}
    for feat in geojson.get("features", []):
        props = feat.get("properties", {}) or {}
        code = props.get("DZ2021CD")
        if not code:
            continue
        names[code] = props.get("DZ2021NM", code)
        try:
            areas[code] = float(props.get("area_km2") or 0.0)
        except (TypeError, ValueError):
            areas[code] = 0.0

    populations = parse_ni_population_dz(ms_a01_xlsx_path)

    total_values: dict[str, float] = {}
    density_values: dict[str, float] = {}
    for code, pop in populations.items():
        if code not in areas:
            continue
        total_values[code] = float(pop)
        area = areas[code]
        density_values[code] = round(pop / area, 2) if area > 0 else 0.0

    source = "NISRA Census 2021 — MS-A01 (population) + DZ2021 boundary area"
    return {
        "population_total": {
            "dataset_id": "population_total",
            "values": total_values,
            "names": {c: names.get(c, c) for c in total_values},
            "stats": _stats(total_values),
            "source": source,
        },
        "population_density": {
            "dataset_id": "population_density",
            "values": density_values,
            "names": {c: names.get(c, c) for c in density_values},
            "stats": _stats(density_values),
            "source": source,
        },
    }


def get_ni_data_for_dataset(
    dataset_id: str,
    data_dir: Path,
) -> Optional[dict]:
    """Load cached NI data payload for a dataset, if present."""
    cache_file = data_dir / f"data_{dataset_id}_national_ni.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text())
    return None
