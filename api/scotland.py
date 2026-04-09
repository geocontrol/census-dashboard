"""
Scotland Census 2022 — Data Zone boundaries, OA→DZ lookup, and census CSV processing.

Downloads DZ22 boundaries (shapefile), reprojects BNG→WGS84, simplifies,
and processes OA-level census CSVs aggregated to Data Zone level.
"""
import csv
import io
import json
import logging
import zipfile
from pathlib import Path
from typing import Optional

import httpx
import shapefile
from pyproj import Transformer
from shapely.geometry import shape, mapping, Polygon, MultiPolygon

logger = logging.getLogger(__name__)

DZ22_BOUNDARY_URL = "https://maps.gov.scot/ATOM/shapefiles/SG_DataZoneBdry_2022.zip"
OA_DZ_LOOKUP_URL = "https://www.nrscotland.gov.uk/media/iz3evrqt/oa22_dz22_iz22.zip"
CENSUS_OA_URL = "https://www.scotlandscensus.gov.uk/media/zz8620250326_10_44_kfutoikgulhiulksdufgkguoiu68kg/OutputArea.zip"

# Scottish Council Area codes (S12) mapped from DZ22 codes
# DZ22 codes: S01xxxxxx — first 6 chars encode the council area
# We'll build this mapping from the lookup data

# Target vertices per polygon ring after simplification
TARGET_VERTICES = 30


def _simplify_ring(coords: list, target: int = TARGET_VERTICES) -> list:
    """Keep every Nth point to reduce a ring to ~target vertices."""
    n = len(coords)
    if n <= target:
        return coords
    step = max(1, n // target)
    simplified = coords[::step]
    # Ensure ring is closed
    if simplified[0] != simplified[-1]:
        simplified.append(simplified[0])
    return simplified


def _simplify_geometry(geom):
    """Simplify a Shapely geometry to reduce vertex count."""
    if geom.geom_type == "Polygon":
        exterior = _simplify_ring(list(geom.exterior.coords))
        interiors = [_simplify_ring(list(ring.coords)) for ring in geom.interiors]
        try:
            return Polygon(exterior, interiors)
        except Exception:
            return geom
    elif geom.geom_type == "MultiPolygon":
        parts = []
        for part in geom.geoms:
            simplified = _simplify_geometry(part)
            if simplified.is_valid and not simplified.is_empty:
                parts.append(simplified)
        if parts:
            return MultiPolygon(parts)
    return geom


async def download_dz22_boundaries(data_dir: Path) -> Path:
    """Download DZ22 shapefile, reproject BNG→WGS84, simplify, save as GeoJSON."""
    output = data_dir / "boundaries_scotland_dz22.geojson"
    if output.exists():
        logger.info(f"Scotland DZ22 boundaries cached ({output.stat().st_size / 1024 / 1024:.1f} MB)")
        return output

    logger.info("Downloading Scotland DZ22 boundaries shapefile...")
    zip_path = data_dir / "SG_DataZoneBdry_2022.zip"

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get(DZ22_BOUNDARY_URL)
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)
        logger.info(f"  Downloaded {zip_path.stat().st_size / 1024 / 1024:.1f} MB")

    # Extract shapefile components
    extract_dir = data_dir / "dz22_shp"
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    # Find the .shp file
    shp_files = list(extract_dir.rglob("*.shp"))
    if not shp_files:
        raise FileNotFoundError("No .shp file found in DZ22 boundary zip")
    shp_path = shp_files[0]
    logger.info(f"  Reading shapefile: {shp_path.name}")

    # Reproject BNG (EPSG:27700) → WGS84 (EPSG:4326)
    transformer = Transformer.from_crs("EPSG:27700", "EPSG:4326", always_xy=True)

    reader = shapefile.Reader(str(shp_path))
    fields = [f[0] for f in reader.fields[1:]]  # skip DeletionFlag

    features = []
    for sr in reader.shapeRecords():
        rec = dict(zip(fields, sr.record))
        dz_code = rec.get("dzcode", rec.get("DataZone", rec.get("DZ2022", "")))
        dz_name = rec.get("dzname", rec.get("Name", rec.get("DZ2022Name", "")))

        if not dz_code:
            continue

        # Convert shapefile geometry to Shapely, reproject
        geom_json = sr.shape.__geo_interface__
        geom = shape(geom_json)

        if geom.is_empty:
            continue

        # Reproject coordinates
        if geom.geom_type == "Polygon":
            exterior = [transformer.transform(x, y) for x, y in geom.exterior.coords]
            interiors = [[transformer.transform(x, y) for x, y in ring.coords] for ring in geom.interiors]
            reprojected = Polygon(exterior, interiors)
        elif geom.geom_type == "MultiPolygon":
            parts = []
            for part in geom.geoms:
                ext = [transformer.transform(x, y) for x, y in part.exterior.coords]
                ints = [[transformer.transform(x, y) for x, y in ring.coords] for ring in part.interiors]
                parts.append(Polygon(ext, ints))
            reprojected = MultiPolygon(parts)
        else:
            continue

        if not reprojected.is_valid:
            reprojected = reprojected.buffer(0)

        # Simplify
        simplified = _simplify_geometry(reprojected)
        if not simplified.is_valid:
            simplified = simplified.buffer(0)

        feature = {
            "type": "Feature",
            "properties": {
                "DZ22CD": dz_code,
                "DZ22NM": dz_name,
                "nation": "SC",
            },
            "geometry": mapping(simplified),
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    # Write output
    output.write_text(json.dumps(geojson))
    logger.info(
        f"Scotland DZ22 boundaries: {len(features)} features, "
        f"{output.stat().st_size / 1024 / 1024:.1f} MB"
    )

    # Cleanup shapefile zip and extracted files
    try:
        zip_path.unlink()
        import shutil
        shutil.rmtree(extract_dir, ignore_errors=True)
    except Exception:
        pass

    return output


async def download_oa_dz_lookup(data_dir: Path) -> tuple[dict[str, str], dict[str, str]]:
    """Download OA→DZ lookup. Returns (oa_to_dz, dz_names) dicts."""
    lookup_file = data_dir / "oa22_dz22_lookup.json"
    names_file = data_dir / "dz22_names.json"

    if lookup_file.exists() and names_file.exists():
        oa_to_dz = json.loads(lookup_file.read_text())
        dz_names = json.loads(names_file.read_text())
        logger.info(f"OA→DZ lookup cached: {len(oa_to_dz)} OAs → {len(dz_names)} DZs")
        return oa_to_dz, dz_names

    logger.info("Downloading OA→DZ lookup...")
    zip_path = data_dir / "oa22_dz22_iz22.zip"

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(OA_DZ_LOOKUP_URL)
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)

    oa_to_dz = {}
    dz_names = {}

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        logger.info(f"  Lookup zip contains: {names}")

        # Find OA→DZ CSV
        oa_dz_csv = None
        dz_name_csv = None
        for name in names:
            lower = name.lower()
            if "oa" in lower and "dz" in lower and lower.endswith(".csv"):
                oa_dz_csv = name
            if "lookup" in lower and lower.endswith(".csv"):
                dz_name_csv = name

        if oa_dz_csv:
            with zf.open(oa_dz_csv) as f:
                text = f.read().decode("utf-8-sig")
                reader = csv.DictReader(io.StringIO(text))
                for row in reader:
                    oa = row.get("OA22", row.get("OA2022", "")).strip()
                    dz = row.get("DZ22", row.get("DZ2022", "")).strip()
                    if oa and dz:
                        oa_to_dz[oa] = dz

        if dz_name_csv:
            with zf.open(dz_name_csv) as f:
                text = f.read().decode("utf-8-sig")
                reader = csv.DictReader(io.StringIO(text))
                for row in reader:
                    dz = row.get("DZ22", row.get("DZ2022", "")).strip()
                    name = row.get("DZ22Name", row.get("DZ2022Name", row.get("Name", ""))).strip()
                    if dz and name:
                        dz_names[dz] = name

    # Persist
    lookup_file.write_text(json.dumps(oa_to_dz))
    names_file.write_text(json.dumps(dz_names))
    logger.info(f"OA→DZ lookup: {len(oa_to_dz)} OAs → {len(set(oa_to_dz.values()))} DZs")

    try:
        zip_path.unlink()
    except Exception:
        pass

    return oa_to_dz, dz_names


async def download_scotland_census_csvs(data_dir: Path) -> Path:
    """Download the Scotland Census 2022 OA-level CSV zip."""
    extract_dir = data_dir / "scotland_oa_csvs"
    marker = extract_dir / ".downloaded"

    if marker.exists():
        logger.info("Scotland Census OA CSVs already downloaded")
        return extract_dir

    logger.info("Downloading Scotland Census 2022 OA CSVs (92 MB)...")
    zip_path = data_dir / "OutputArea.zip"

    async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
        resp = await client.get(CENSUS_OA_URL)
        resp.raise_for_status()
        zip_path.write_bytes(resp.content)
        logger.info(f"  Downloaded {zip_path.stat().st_size / 1024 / 1024:.1f} MB")

    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    marker.touch()
    logger.info(f"  Extracted to {extract_dir}")

    try:
        zip_path.unlink()
    except Exception:
        pass

    return extract_dir


def parse_scotland_csv(csv_path: Path) -> tuple[list[str], dict[str, list[float]]]:
    """
    Parse a Scotland Census multivariate CSV with multi-row headers.

    Returns:
        (column_labels, oa_data) where:
        - column_labels: list of combined header labels for each data column
        - oa_data: dict {oa_code: [values per column]}
    """
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()

    if len(lines) < 4:
        return [], {}

    # Find where data rows start (rows beginning with S00)
    data_start = None
    header_rows = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and (stripped.startswith('"S00') or stripped.startswith('S00')):
            data_start = i
            break
        # Skip blank lines and the title/description rows
        if i >= 2:  # skip title line (0) and table description (1)
            header_rows.append(i)

    if data_start is None:
        return [], {}

    # Parse header rows to build column labels
    # Use the last few header rows (typically 2-3 rows of category labels)
    raw_headers = []
    for idx in header_rows:
        if idx < len(lines):
            row = next(csv.reader(io.StringIO(lines[idx])))
            raw_headers.append(row)

    # Build combined column labels from header rows
    n_cols = max(len(row) for row in raw_headers) if raw_headers else 0
    column_labels = []
    for col_idx in range(1, n_cols):  # skip column 0 (OA code)
        parts = []
        for row in raw_headers:
            if col_idx < len(row):
                val = row[col_idx].strip()
                if val and val not in parts:
                    parts.append(val)
        column_labels.append(" | ".join(parts) if parts else f"col_{col_idx}")

    # Parse data rows
    oa_data = {}
    for i in range(data_start, len(lines)):
        line = lines[i].strip()
        if not line:
            continue
        row = next(csv.reader(io.StringIO(line)))
        if not row:
            continue
        oa_code = row[0].strip().strip('"')
        if not oa_code.startswith("S00"):
            continue
        values = []
        for j in range(1, len(row)):
            val = row[j].strip()
            if val == "-" or val == "":
                values.append(0.0)
            else:
                try:
                    values.append(float(val))
                except ValueError:
                    values.append(0.0)
        oa_data[oa_code] = values

    return column_labels, oa_data


def aggregate_oa_to_dz(
    oa_data: dict[str, list[float]],
    oa_to_dz: dict[str, str],
    numerator_cols: list[int],
    denominator_col: Optional[int] = None,
) -> dict[str, float]:
    """
    Aggregate OA-level data to DZ level.

    Args:
        oa_data: {oa_code: [values]}
        oa_to_dz: {oa_code: dz_code}
        numerator_cols: column indices for the numerator
        denominator_col: column index for the denominator (None = return sum)

    Returns:
        {dz_code: aggregated_value}
    """
    dz_num = {}
    dz_den = {}

    for oa, values in oa_data.items():
        dz = oa_to_dz.get(oa)
        if not dz:
            continue

        # Sum numerator columns
        num = sum(values[c] for c in numerator_cols if c < len(values))
        dz_num[dz] = dz_num.get(dz, 0) + num

        if denominator_col is not None and denominator_col < len(values):
            dz_den[dz] = dz_den.get(dz, 0) + values[denominator_col]

    result = {}
    for dz, num in dz_num.items():
        if denominator_col is not None:
            den = dz_den.get(dz, 0)
            result[dz] = round(num / den * 100, 2) if den > 0 else 0.0
        else:
            result[dz] = round(num, 2)

    return result


# ═══════ Scotland indicator mappings ═══════
# Maps E&W dataset IDs to Scottish CSV table + column extraction config
# Each entry specifies: csv file stem, numerator column indices, denominator column index

SCOTLAND_INDICATOR_MAP = {
    # --- Housing ---
    "home_ownership": {
        "csv": "MV409",
        "description": "Tenure by household composition by central heating",
        "extract": "rate",
        # Column indices determined at runtime from parsed headers
        "header_match": {
            "numerator": ["Owned"],
            "denominator": ["All households"],
        },
    },
    "social_rented": {
        "csv": "MV409",
        "extract": "rate",
        "header_match": {
            "numerator": ["Social rented"],
            "denominator": ["All households"],
        },
    },
    "private_rented": {
        "csv": "MV409",
        "extract": "rate",
        "header_match": {
            "numerator": ["Private rented"],
            "denominator": ["All households"],
        },
    },
    "accommodation_detached": {
        "csv": "MV401",
        "extract": "rate",
        "header_match": {
            "numerator": ["Detached"],
            "denominator": ["All households"],
        },
    },
    "accommodation_flat": {
        "csv": "MV401",
        "extract": "rate",
        "header_match": {
            "numerator": ["flat", "Flat"],
            "denominator": ["All households"],
        },
    },
    "accommodation_terraced": {
        "csv": "MV401",
        "extract": "rate",
        "header_match": {
            "numerator": ["Terraced"],
            "denominator": ["All households"],
        },
    },
    "no_central_heating": {
        "csv": "MV403",
        "extract": "rate",
        "header_match": {
            "numerator": ["No central heating"],
            "denominator": ["All households"],
        },
    },
    "gas_heating": {
        "csv": "MV403",
        "extract": "rate",
        "header_match": {
            "numerator": ["Gas"],
            "denominator": ["All households"],
        },
    },
    "electric_heating": {
        "csv": "MV403",
        "extract": "rate",
        "header_match": {
            "numerator": ["Electric"],
            "denominator": ["All households"],
        },
    },
    # --- Cars ---
    "car_none": {
        "csv": "MV407",
        "extract": "rate",
        "header_match": {
            "numerator": ["No cars or vans", "None"],
            "denominator": ["All households"],
        },
    },
    # --- Ethnicity ---
    "white_british": {
        "csv": "MV102",
        "extract": "rate",
        "header_match": {
            "numerator": ["White"],
            "denominator": ["All people"],
        },
    },
    "ethnic_asian": {
        "csv": "MV102",
        "extract": "rate",
        "header_match": {
            "numerator": ["Asian"],
            "denominator": ["All people"],
        },
    },
    "ethnic_black": {
        "csv": "MV102",
        "extract": "rate",
        "header_match": {
            "numerator": ["African", "Caribbean", "Black"],
            "denominator": ["All people"],
        },
    },
    "ethnic_mixed": {
        "csv": "MV102",
        "extract": "rate",
        "header_match": {
            "numerator": ["Mixed"],
            "denominator": ["All people"],
        },
    },
    # --- Health ---
    "health_good": {
        "csv": "MV301",
        "extract": "rate",
        "header_match": {
            "numerator": ["Very good", "Good"],
            "denominator": ["All people"],
        },
    },
    "health_bad": {
        "csv": "MV301",
        "extract": "rate",
        "header_match": {
            "numerator": ["Bad", "Very bad"],
            "denominator": ["All people"],
        },
    },
    # --- Religion ---
    "christian": {
        "csv": "MV204",
        "extract": "rate",
        "header_match": {
            "numerator": ["Church of Scotland", "Roman Catholic", "Other Christian"],
            "denominator": ["All people"],
        },
    },
    "no_religion": {
        "csv": "MV204",
        "extract": "rate",
        "header_match": {
            "numerator": ["No religion"],
            "denominator": ["All people"],
        },
    },
    "muslim": {
        "csv": "MV204",
        "extract": "rate",
        "header_match": {
            "numerator": ["Muslim"],
            "denominator": ["All people"],
        },
    },
    "hindu": {
        "csv": "MV204",
        "extract": "rate",
        "header_match": {
            "numerator": ["Hindu"],
            "denominator": ["All people"],
        },
    },
    "sikh": {
        "csv": "MV204",
        "extract": "rate",
        "header_match": {
            "numerator": ["Sikh"],
            "denominator": ["All people"],
        },
    },
}


def find_matching_columns(column_labels: list[str], search_terms: list[str]) -> list[int]:
    """Find column indices where any search term appears in the label (case-insensitive)."""
    matches = []
    for i, label in enumerate(column_labels):
        label_lower = label.lower()
        for term in search_terms:
            if term.lower() in label_lower:
                matches.append(i)
                break
    return matches


def find_first_matching_column(column_labels: list[str], search_terms: list[str]) -> Optional[int]:
    """Find the first column where the label exactly starts with a search term or is the 'All' total."""
    # First try: exact "All" total column (column 0 is typically the total)
    for i, label in enumerate(column_labels):
        label_lower = label.lower()
        for term in search_terms:
            if term.lower() == label_lower.split(" | ")[0].lower():
                return i
    # Fallback: partial match
    matches = find_matching_columns(column_labels, search_terms)
    return matches[0] if matches else None


def process_scotland_indicator(
    dataset_id: str,
    csv_dir: Path,
    oa_to_dz: dict[str, str],
    dz_names: dict[str, str],
) -> Optional[dict]:
    """
    Process a single Scotland indicator from OA CSVs to DZ-aggregated values.

    Returns a dict compatible with the E&W data format:
    {values: {dz_code: value}, names: {dz_code: name}, stats: {...}}
    """
    mapping_info = SCOTLAND_INDICATOR_MAP.get(dataset_id)
    if not mapping_info:
        return None

    csv_stem = mapping_info["csv"].upper()
    # Find the CSV file — filenames include description, e.g. "MV409 - Tenure (6) by ...csv"
    csv_file = None
    for f in csv_dir.iterdir():
        if f.suffix.lower() == ".csv" and f.name.upper().startswith(csv_stem):
            csv_file = f
            break

    # Also check subdirectories
    if not csv_file:
        for f in csv_dir.rglob("*.csv"):
            if f.name.upper().startswith(csv_stem):
                csv_file = f
                break

    if not csv_file:
        logger.warning(f"Scotland CSV not found for {dataset_id}: {csv_stem}")
        return None

    column_labels, oa_data = parse_scotland_csv(csv_file)
    if not oa_data:
        logger.warning(f"No OA data parsed from {csv_file.name}")
        return None

    header_match = mapping_info["header_match"]

    # Find numerator columns
    num_cols = find_matching_columns(column_labels, header_match["numerator"])
    # Find denominator column (first match for "All" totals)
    den_col = find_first_matching_column(column_labels, header_match["denominator"])

    if not num_cols:
        logger.warning(
            f"No numerator columns found for {dataset_id} in {csv_stem}. "
            f"Searched for {header_match['numerator']} in {column_labels[:10]}"
        )
        return None

    # Aggregate to DZ level
    if mapping_info["extract"] == "rate" and den_col is not None:
        values = aggregate_oa_to_dz(oa_data, oa_to_dz, num_cols, den_col)
    else:
        values = aggregate_oa_to_dz(oa_data, oa_to_dz, num_cols)

    if not values:
        return None

    # Build names dict
    names = {dz: dz_names.get(dz, dz) for dz in values}

    # Compute stats
    vals = sorted(values.values())
    n = len(vals)
    stats = {}
    if n > 0:
        stats = {
            "min": vals[0],
            "max": vals[-1],
            "mean": round(sum(vals) / n, 2),
            "p10": vals[int(n * 0.1)],
            "p25": vals[int(n * 0.25)],
            "p50": vals[int(n * 0.5)],
            "p75": vals[int(n * 0.75)],
            "p90": vals[int(n * 0.9)],
            "count": n,
        }

    return {
        "dataset_id": dataset_id,
        "values": values,
        "names": names,
        "stats": stats,
        "source": "Scotland's Census 2022 — NRS",
    }


async def process_all_scotland_indicators(
    data_dir: Path,
    oa_to_dz: dict[str, str],
    dz_names: dict[str, str],
) -> dict[str, dict]:
    """Process all mapped Scotland indicators. Returns {dataset_id: data_dict}."""
    csv_dir = data_dir / "scotland_oa_csvs"
    if not csv_dir.exists():
        csv_dir = await download_scotland_census_csvs(data_dir)

    results = {}
    for dataset_id in SCOTLAND_INDICATOR_MAP:
        cache_file = data_dir / f"data_{dataset_id}_national_sc.json"
        if cache_file.exists():
            results[dataset_id] = json.loads(cache_file.read_text())
            continue

        result = process_scotland_indicator(dataset_id, csv_dir, oa_to_dz, dz_names)
        if result:
            cache_file.write_text(json.dumps(result))
            results[dataset_id] = result
            logger.info(f"  Scotland {dataset_id}: {len(result['values'])} DZs")
        else:
            logger.warning(f"  Scotland {dataset_id}: no data extracted")

    return results


# ═══════ Scottish Council Areas ═══════

SCOTTISH_COUNCIL_AREAS = [
    {"code": "S12000005", "name": "Clackmannanshire"},
    {"code": "S12000006", "name": "Dumfries and Galloway"},
    {"code": "S12000008", "name": "East Ayrshire"},
    {"code": "S12000010", "name": "East Lothian"},
    {"code": "S12000011", "name": "East Renfrewshire"},
    {"code": "S12000013", "name": "Na h-Eileanan Siar"},
    {"code": "S12000014", "name": "Falkirk"},
    {"code": "S12000015", "name": "Fife"},
    {"code": "S12000017", "name": "Highland"},
    {"code": "S12000018", "name": "Inverclyde"},
    {"code": "S12000019", "name": "Midlothian"},
    {"code": "S12000020", "name": "Moray"},
    {"code": "S12000021", "name": "North Ayrshire"},
    {"code": "S12000023", "name": "Orkney Islands"},
    {"code": "S12000024", "name": "Perth and Kinross"},
    {"code": "S12000026", "name": "Scottish Borders"},
    {"code": "S12000027", "name": "Shetland Islands"},
    {"code": "S12000028", "name": "South Ayrshire"},
    {"code": "S12000029", "name": "South Lanarkshire"},
    {"code": "S12000030", "name": "Stirling"},
    {"code": "S12000033", "name": "Aberdeen City"},
    {"code": "S12000034", "name": "Aberdeenshire"},
    {"code": "S12000035", "name": "Argyll and Bute"},
    {"code": "S12000036", "name": "City of Edinburgh"},
    {"code": "S12000038", "name": "Renfrewshire"},
    {"code": "S12000039", "name": "West Dunbartonshire"},
    {"code": "S12000040", "name": "West Lothian"},
    {"code": "S12000041", "name": "Angus"},
    {"code": "S12000042", "name": "Dundee City"},
    {"code": "S12000045", "name": "East Dunbartonshire"},
    {"code": "S12000047", "name": "Fife"},
    {"code": "S12000048", "name": "Perth and Kinross"},
    {"code": "S12000049", "name": "Glasgow City"},
    {"code": "S12000050", "name": "North Lanarkshire"},
]

# DZ22 code prefix → council area code mapping
# Built at runtime from boundary data
_dz_to_council: dict[str, str] = {}


def build_dz_council_mapping(dz22_geojson_path: Path) -> dict[str, str]:
    """Build a mapping from DZ22 code to council area code from boundary properties."""
    global _dz_to_council
    if _dz_to_council:
        return _dz_to_council

    # For now, we can't easily map without LA codes in the shapefile.
    # We'll use a different approach: filter DZs by spatial containment.
    # But for the MVP, we'll return all Scotland DZs for any S12 code.
    # This mapping will be refined when we have the actual data.
    return _dz_to_council


def get_council_area_dzs(
    council_code: str,
    dz22_geojson_path: Path,
) -> list[str]:
    """Get all DZ22 codes for a Scottish council area."""
    # Load all DZ features and check which ones belong to the council area
    # For now, return all Scotland DZs (to be refined with spatial lookup)
    geojson = json.loads(dz22_geojson_path.read_text())
    return [f["properties"]["DZ22CD"] for f in geojson.get("features", [])]
