import io
import json
import logging
import tempfile
import zipfile
from pathlib import Path

import aiofiles
import httpx
import shapefile
from fastapi import HTTPException
from shapely.geometry import mapping, shape
from shapely.geometry.polygon import orient
from shapely.ops import unary_union
from shapely import buffer as shp_buffer

logger = logging.getLogger(__name__)

ARCGIS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
BSC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BSC_V4/FeatureServer/0/query"
BGC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V5/FeatureServer/0/query"
BFC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"
LOOKUP_URL = f"{ARCGIS}/LSOA21_WD22_LAD22_EW_LU_v3/FeatureServer/0/query"

adjacency_graph: dict = {}
lsoa_geometries: dict = {}


async def build_adjacency_graph(bsc_file: Path):
    """Build LSOA adjacency graph from cached BSC boundaries using Shapely."""
    adj_file = bsc_file.parent / "adjacency_graph.json"
    if adj_file.exists():
        async with aiofiles.open(adj_file) as f:
            adjacency_graph.update(json.loads(await f.read()))
        logger.info(f"Adjacency graph loaded from cache: {len(adjacency_graph)} LSOAs")
        await build_geometry_index(bsc_file)
        return

    logger.info("Building adjacency graph from BSC boundaries (one-time, ~30s)...")
    async with aiofiles.open(bsc_file) as f:
        geojson = json.loads(await f.read())

    geoms = {}
    for feat in geojson.get("features", []):
        code = feat["properties"].get("LSOA21CD", "")
        if not code:
            continue
        try:
            geom = shape(feat["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)
            geoms[code] = geom
            lsoa_geometries[code] = geom
        except Exception:
            continue

    logger.info(f"  Parsed {len(geoms)} geometries, computing adjacency...")

    from shapely import STRtree

    codes = list(geoms.keys())
    polys = [geoms[c] for c in codes]
    tree = STRtree(polys)

    adj = {c: [] for c in codes}
    for i, code in enumerate(codes):
        geom = polys[i]
        for j in tree.query(geom):
            if j == i:
                continue
            other_geom = polys[j]
            try:
                intersection = geom.intersection(other_geom)
                if intersection.is_empty:
                    continue
                if intersection.geom_type in (
                    "LineString",
                    "MultiLineString",
                    "GeometryCollection",
                    "Polygon",
                    "MultiPolygon",
                ):
                    adj[code].append(codes[j])
            except Exception:
                continue

    adjacency_graph.update(adj)
    async with aiofiles.open(adj_file, "w") as f:
        await f.write(json.dumps(adj))

    total_edges = sum(len(v) for v in adj.values()) // 2
    logger.info(f"  Adjacency graph built: {len(adj)} LSOAs, {total_edges} edges")


async def build_geometry_index(bsc_file: Path):
    """Load BSC geometries into memory for dissolve operations."""
    if lsoa_geometries:
        return

    async with aiofiles.open(bsc_file) as f:
        geojson = json.loads(await f.read())

    for feat in geojson.get("features", []):
        code = feat["properties"].get("LSOA21CD", "")
        if not code:
            continue
        try:
            geom = shape(feat["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)
            lsoa_geometries[code] = geom
        except Exception:
            continue

    logger.info(f"Geometry index loaded: {len(lsoa_geometries)} LSOAs")


async def build_scotland_geometry_index(dz22_file: Path):
    """Load Scotland DZ22 geometries into the shared geometry index."""
    async with aiofiles.open(dz22_file) as f:
        geojson = json.loads(await f.read())

    count = 0
    for feat in geojson.get("features", []):
        code = feat["properties"].get("DZ22CD", "")
        if not code:
            continue
        try:
            geom = shape(feat["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)
            lsoa_geometries[code] = geom
            count += 1
        except Exception:
            continue

    logger.info(f"Scotland geometry index loaded: {count} DZs (total geometries: {len(lsoa_geometries)})")


async def build_scotland_adjacency(dz22_file: Path):
    """Build adjacency graph for Scotland DZ22 features and merge into main graph."""
    adj_file = dz22_file.parent / "adjacency_graph_scotland.json"
    if adj_file.exists():
        async with aiofiles.open(adj_file) as f:
            sc_adj = json.loads(await f.read())
        adjacency_graph.update(sc_adj)
        logger.info(f"Scotland adjacency graph loaded from cache: {len(sc_adj)} DZs")
        return

    logger.info("Building Scotland DZ22 adjacency graph...")
    async with aiofiles.open(dz22_file) as f:
        geojson = json.loads(await f.read())

    geoms = {}
    for feat in geojson.get("features", []):
        code = feat["properties"].get("DZ22CD", "")
        if not code:
            continue
        try:
            geom = shape(feat["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)
            geoms[code] = geom
        except Exception:
            continue

    from shapely import STRtree

    codes = list(geoms.keys())
    polys = [geoms[c] for c in codes]
    tree = STRtree(polys)

    adj = {c: [] for c in codes}
    for i, code in enumerate(codes):
        geom = polys[i]
        for j in tree.query(geom):
            if j == i:
                continue
            try:
                intersection = geom.intersection(polys[j])
                if not intersection.is_empty and intersection.geom_type in (
                    "LineString",
                    "MultiLineString",
                    "GeometryCollection",
                    "Polygon",
                    "MultiPolygon",
                ):
                    adj[code].append(codes[j])
            except Exception:
                continue

    adjacency_graph.update(adj)
    async with aiofiles.open(adj_file, "w") as f:
        await f.write(json.dumps(adj))

    total_edges = sum(len(v) for v in adj.values()) // 2
    logger.info(f"Scotland adjacency graph built: {len(adj)} DZs, {total_edges} edges")


async def fetch_all_boundaries(service_url: str, label: str) -> dict:
    all_features = []
    offset = 0
    page = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            page += 1
            resp = await client.post(
                service_url,
                data={
                    "where": "1=1",
                    "outFields": "LSOA21CD,LSOA21NM",
                    "outSR": "4326",
                    "f": "geojson",
                    "resultOffset": str(offset),
                    "resultRecordCount": "2000",
                },
            )
            features = resp.json().get("features", [])
            all_features.extend(features)
            logger.info(f"  {label} page {page}: +{len(features)} (total {len(all_features)})")
            if len(features) < 2000:
                break
            offset += 2000

    return {"type": "FeatureCollection", "features": all_features}


async def fetch_lad_boundaries(lad_code: str, service_url: str) -> dict:
    all_features = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        lsoa_codes = []
        offset = 0
        while True:
            resp = await client.post(
                LOOKUP_URL,
                data={
                    "where": f"LAD22CD='{lad_code}'",
                    "outFields": "LSOA21CD",
                    "f": "json",
                    "resultOffset": str(offset),
                    "resultRecordCount": "1000",
                },
            )
            data = resp.json()
            if "error" in data:
                break
            features = data.get("features", [])
            lsoa_codes.extend(f["attributes"]["LSOA21CD"] for f in features)
            if len(features) < 1000:
                break
            offset += 1000

        logger.info(f"Resolved {len(lsoa_codes)} LSOAs for {lad_code}")
        if not lsoa_codes:
            return {"type": "FeatureCollection", "features": []}

        for i in range(0, len(lsoa_codes), 150):
            batch = lsoa_codes[i:i + 150]
            codes_sql = ",".join(f"'{code}'" for code in batch)
            try:
                resp = await client.post(
                    service_url,
                    data={
                        "where": f"LSOA21CD IN ({codes_sql})",
                        "outFields": "LSOA21CD,LSOA21NM",
                        "outSR": "4326",
                        "f": "geojson",
                        "resultRecordCount": "1000",
                    },
                )
                data = resp.json()
                if "error" not in data:
                    feats = data.get("features", [])
                    for feat in feats:
                        feat["properties"]["LAD22CD"] = lad_code
                    all_features.extend(feats)
            except Exception as exc:
                logger.error(f"Boundary batch error: {exc}")

    return {"type": "FeatureCollection", "features": all_features}


def get_adjacency_payload() -> dict:
    if not adjacency_graph:
        raise HTTPException(503, "Adjacency graph not yet built")
    return {"count": len(adjacency_graph), "graph": adjacency_graph}


def dissolve_selected_geometries(lsoa_codes: list[str]) -> dict:
    if not lsoa_geometries:
        raise HTTPException(503, "Geometry index not yet loaded")
    if not lsoa_codes:
        raise HTTPException(400, "No LSOA codes provided")
    if len(lsoa_codes) > 500:
        raise HTTPException(400, "Maximum 500 LSOAs per selection")

    geoms = []
    missing = []
    for code in lsoa_codes:
        if code in lsoa_geometries:
            geoms.append(lsoa_geometries[code])
        else:
            missing.append(code)

    if not geoms:
        raise HTTPException(404, "No geometries found for given LSOA codes")

    try:
        buffered = [shp_buffer(geom, 0.00001) for geom in geoms]
        merged = unary_union(buffered)
        merged = shp_buffer(merged, -0.00001)
        if not merged.is_valid:
            merged = merged.buffer(0)
    except Exception as exc:
        logger.error(f"Dissolve error: {exc}")
        merged = unary_union(geoms)

    centroid = merged.centroid
    bounds = merged.bounds
    area_approx_km2 = merged.area * 111 * 68
    perimeter_approx_km = merged.length * 90
    n_components = len(list(merged.geoms)) if merged.geom_type == "MultiPolygon" else 1

    border_neighbours = set()
    if adjacency_graph:
        selection_set = set(lsoa_codes)
        for code in lsoa_codes:
            for neighbour in adjacency_graph.get(code, []):
                if neighbour not in selection_set:
                    border_neighbours.add(neighbour)

    return {
        "type": "Feature",
        "geometry": mapping(merged),
        "properties": {
            "lsoa_count": len(geoms),
            "missing_codes": missing,
            "components": n_components,
            "contiguous": n_components == 1,
            "area_km2": round(area_approx_km2, 3),
            "perimeter_km": round(perimeter_approx_km, 3),
            "centroid": [round(centroid.x, 6), round(centroid.y, 6)],
            "bounds": [round(bound, 6) for bound in bounds],
            "border_neighbours": sorted(border_neighbours)[:200],
        },
    }


WGS84_WKT = (
    'GEOGCS["WGS 84",DATUM["WGS_1984",'
    'SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],'
    'AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],'
    'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],'
    'AUTHORITY["EPSG","4326"]]'
)


def _polygon_parts(polygon) -> list[list[list[float]]]:
    """Convert a shapely Polygon into shapefile parts (CW exterior, CCW holes)."""
    oriented = orient(polygon, sign=-1.0)
    parts = [[list(coord) for coord in oriented.exterior.coords]]
    for interior in oriented.interiors:
        parts.append([list(coord) for coord in interior.coords])
    return parts


def export_dissolve_as_shapefile(lsoa_codes: list[str]) -> bytes:
    """Dissolve a selection and return a zipped ESRI Shapefile (.shp/.shx/.dbf/.prj) as bytes."""
    dissolve = dissolve_selected_geometries(lsoa_codes)
    geom = shape(dissolve["geometry"])
    props = dissolve["properties"]

    if geom.geom_type == "Polygon":
        polygons = [geom]
    elif geom.geom_type == "MultiPolygon":
        polygons = list(geom.geoms)
    else:
        raise HTTPException(500, f"Unsupported geometry type for shapefile export: {geom.geom_type}")

    all_parts: list[list[list[float]]] = []
    for poly in polygons:
        all_parts.extend(_polygon_parts(poly))

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir) / "census_selection"
        writer = shapefile.Writer(str(base), shapeType=shapefile.POLYGON)
        writer.field("lsoa_count", "N", size=10, decimal=0)
        writer.field("area_km2", "N", size=19, decimal=3)
        writer.field("perim_km", "N", size=19, decimal=3)
        writer.field("contiguous", "L")
        writer.field("components", "N", size=10, decimal=0)

        writer.poly(all_parts)
        writer.record(
            props.get("lsoa_count", 0),
            props.get("area_km2", 0.0),
            props.get("perimeter_km", 0.0),
            bool(props.get("contiguous", False)),
            props.get("components", 1),
        )
        writer.close()

        (base.with_suffix(".prj")).write_text(WGS84_WKT)

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for suffix in (".shp", ".shx", ".dbf", ".prj"):
                path = base.with_suffix(suffix)
                if path.exists():
                    zf.write(path, arcname=path.name)
        return buf.getvalue()
