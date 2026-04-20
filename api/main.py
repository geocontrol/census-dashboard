"""
UK Census Explorer API
National-scale LSOA (E&W) + Data Zone (Scotland + Northern Ireland) data
"""

import asyncio
import csv
import io
import json
import logging
from pathlib import Path
from typing import List, Optional

import aiofiles
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel

from elections import (
    GE_AVAILABLE,
    PARTY_META,
    build_ge_overlay,
    download_constituency_boundaries,
    download_psephology_db,
    process_ge_results,
)
from northern_ireland import (
    NI_INDICATOR_MAP,
    NI_LOCAL_GOVERNMENT_DISTRICTS,
    compute_ni_population_data,
    download_dz21_boundaries,
    download_ni_main_statistics,
    download_ni_population_xlsx,
    get_ni_lgd_dzs,
    process_all_ni_indicators,
    process_ni_indicator,
)
from scotland import (
    SCOTLAND_INDICATOR_MAP,
    SCOTTISH_COUNCIL_AREAS,
    compute_scotland_population_data,
    download_dz22_boundaries,
    download_oa_dz_lookup,
    download_scotland_census_csvs,
    get_council_area_dzs,
    process_all_scotland_indicators,
    process_scotland_indicator,
)
from services.dataset_config import CENSUS_DATASETS
from services.datasets import (
    aggregate_rate_dataset,
    build_dataset_catalog,
    build_explorer_table,
    compute_stats,
    fetch_lad_list,
    fetch_lsoa_detail,
    fetch_nomis_data,
)
from services.geometry import (
    BFC_URL,
    BGC_URL,
    BSC_URL,
    adjacency_graph,
    build_adjacency_graph,
    build_geometry_index,
    build_ni_adjacency,
    build_ni_geometry_index,
    build_scotland_adjacency,
    build_scotland_geometry_index,
    dissolve_selected_geometries,
    export_dissolve_as_shapefile,
    fetch_all_boundaries,
    fetch_lad_boundaries,
    get_adjacency_payload,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="UK Census Explorer API", version="5.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

data_cache = TTLCache(maxsize=500, ttl=86400)
DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(exist_ok=True)

scotland_oa_to_dz: dict = {}
scotland_dz_names: dict = {}
scotland_data_cache: dict = {}
ni_data_cache: dict = {}
election_overlay_cache: dict = {}  # {"{type}_{year}": geojson_dict}


class SelectionRequest(BaseModel):
    lsoa_codes: List[str]


class AggregateRequest(BaseModel):
    lsoa_codes: List[str]
    dataset_ids: Optional[List[str]] = None


@app.on_event("startup")
async def startup_prefetch():
    bsc_file = DATA_DIR / "boundaries_national_bsc.geojson"
    if not bsc_file.exists():
        logger.info("Pre-fetching national BSC boundaries (~19 MB, ~20s)...")
        try:
            geojson = await fetch_all_boundaries(BSC_URL, "BSC")
            async with aiofiles.open(bsc_file, "w") as f:
                await f.write(json.dumps(geojson))
            logger.info(
                f"BSC cached: {len(geojson['features'])} features, "
                f"{bsc_file.stat().st_size / 1024 / 1024:.1f} MB"
            )
        except Exception as exc:
            logger.error(f"BSC pre-fetch failed: {exc}")
            return
    else:
        logger.info(f"National BSC boundaries cached ({bsc_file.stat().st_size / 1024 / 1024:.1f} MB)")

    await build_adjacency_graph(bsc_file)

    try:
        dz22_file = await download_dz22_boundaries(DATA_DIR)
        await build_scotland_geometry_index(dz22_file)
        await build_scotland_adjacency(dz22_file)
    except Exception as exc:
        logger.error(f"Scotland boundary setup failed: {exc}")

    try:
        global scotland_oa_to_dz, scotland_dz_names
        scotland_oa_to_dz, scotland_dz_names = await download_oa_dz_lookup(DATA_DIR)
        asyncio.create_task(_prefetch_scotland_census_data())
    except Exception as exc:
        logger.error(f"Scotland lookup/census setup failed: {exc}")

    try:
        dz21_file = await download_dz21_boundaries(DATA_DIR)
        await build_ni_geometry_index(dz21_file)
        await build_ni_adjacency(dz21_file)
        asyncio.create_task(_prefetch_ni_census_data(dz21_file))
    except Exception as exc:
        logger.error(f"NI boundary setup failed: {exc}")

    asyncio.create_task(_prefetch_election_data())


async def _prefetch_ni_census_data(dz21_file: Path):
    try:
        xlsx_file = await download_ni_population_xlsx(DATA_DIR)
        pop_data = compute_ni_population_data(dz21_file, xlsx_file)
        for dataset_id, data in pop_data.items():
            cache_file = DATA_DIR / f"data_{dataset_id}_national_ni.json"
            cache_file.write_text(json.dumps(data))
            ni_data_cache[dataset_id] = data
            logger.info(f"  NI {dataset_id}: {len(data['values'])} DZs (from MS-A01 + boundary area)")
    except Exception as exc:
        logger.error(f"NI population data setup failed: {exc}")

    try:
        ms_dir = await download_ni_main_statistics(DATA_DIR)
        for dataset_id in NI_INDICATOR_MAP:
            cache_file = DATA_DIR / f"data_{dataset_id}_national_ni.json"
            if cache_file.exists():
                ni_data_cache[dataset_id] = json.loads(cache_file.read_text())
                continue
            result = process_ni_indicator(dataset_id, ms_dir, dz21_file)
            if result:
                cache_file.write_text(json.dumps(result))
                ni_data_cache[dataset_id] = result
                logger.info(f"  NI {dataset_id}: {len(result['values'])} DZs (LGD-level from MS-{result['source'].split('MS-')[1].split(' ')[0]})")
            else:
                logger.warning(f"  NI {dataset_id}: not produced")
        logger.info(f"NI indicator data ready: {len(ni_data_cache)} datasets cached")
    except Exception as exc:
        logger.error(f"NI indicator data setup failed: {exc}")


async def _prefetch_scotland_census_data():
    try:
        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
        if dz22_file.exists():
            pop_data = compute_scotland_population_data(dz22_file, scotland_dz_names)
            for dataset_id, data in pop_data.items():
                cache_file = DATA_DIR / f"data_{dataset_id}_national_sc.json"
                cache_file.write_text(json.dumps(data))
                scotland_data_cache[dataset_id] = data
                logger.info(f"  Scotland {dataset_id}: {len(data['values'])} DZs (from boundary data)")

        await download_scotland_census_csvs(DATA_DIR)
        results = await process_all_scotland_indicators(DATA_DIR, scotland_oa_to_dz, scotland_dz_names)
        scotland_data_cache.update(results)
        logger.info(f"Scotland census data processed: {len(scotland_data_cache)} indicators total")
    except Exception as exc:
        logger.error(f"Scotland census data processing failed: {exc}")


async def _prefetch_election_data():
    try:
        boundaries_file = await download_constituency_boundaries(DATA_DIR)
        db_file = await download_psephology_db(DATA_DIR)
        results = process_ge_results(db_file, year="2024")
        overlay = build_ge_overlay(boundaries_file, results)
        election_overlay_cache["ge_2024"] = overlay
        logger.info(f"Election overlay ready: GE 2024 ({len(overlay['features'])} constituencies)")
    except Exception as exc:
        logger.error(f"Election data prefetch failed: {exc}")


def _scotland_rate_components_available(data: dict) -> bool:
    return bool(data.get("numerators")) and bool(data.get("denominators"))


def _rebuild_scotland_indicator(dataset_id: str) -> Optional[dict]:
    if not scotland_oa_to_dz:
        return None
    csv_dir = DATA_DIR / "scotland_oa_csvs"
    if not csv_dir.exists():
        return None

    result = process_scotland_indicator(dataset_id, csv_dir, scotland_oa_to_dz, scotland_dz_names)
    if result:
        cache_file = DATA_DIR / f"data_{dataset_id}_national_sc.json"
        cache_file.write_text(json.dumps(result))
        scotland_data_cache[dataset_id] = result
    return result


def _get_scotland_data(dataset_id: str) -> Optional[dict]:
    cached_data = scotland_data_cache.get(dataset_id)
    if cached_data:
        if dataset_id in SCOTLAND_INDICATOR_MAP and not _scotland_rate_components_available(cached_data):
            refreshed = _rebuild_scotland_indicator(dataset_id)
            if refreshed:
                return refreshed
        return cached_data

    cache_file = DATA_DIR / f"data_{dataset_id}_national_sc.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        if dataset_id in SCOTLAND_INDICATOR_MAP and not _scotland_rate_components_available(data):
            refreshed = _rebuild_scotland_indicator(dataset_id)
            if refreshed:
                return refreshed
        scotland_data_cache[dataset_id] = data
        return data

    if dataset_id in ("population_density", "population_total"):
        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
        if dz22_file.exists():
            pop_data = compute_scotland_population_data(dz22_file, scotland_dz_names)
            if dataset_id in pop_data:
                result = pop_data[dataset_id]
                cache_file.write_text(json.dumps(result))
                scotland_data_cache[dataset_id] = result
                return result

    if dataset_id in SCOTLAND_INDICATOR_MAP and scotland_oa_to_dz:
        return _rebuild_scotland_indicator(dataset_id)

    return None


def _get_scottish_council_dz_codes(lad_code: str) -> set[str]:
    dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
    if not dz22_file.exists():
        return set()
    return set(get_council_area_dzs(lad_code, dz22_file))


def _filter_scotland_data_for_council(lad_code: str, sc_data: dict) -> dict:
    allowed_codes = _get_scottish_council_dz_codes(lad_code)
    if not allowed_codes:
        return {**sc_data, "values": {}, "names": {}, "stats": {}}

    values = {code: value for code, value in sc_data.get("values", {}).items() if code in allowed_codes}
    names = {code: name for code, name in sc_data.get("names", {}).items() if code in allowed_codes}
    filtered = {**sc_data, "values": values, "names": names, "stats": compute_stats(list(values.values())) if values else {}}

    if sc_data.get("numerators"):
        filtered["numerators"] = {code: value for code, value in sc_data["numerators"].items() if code in allowed_codes}
    if sc_data.get("denominators"):
        filtered["denominators"] = {
            code: value for code, value in sc_data["denominators"].items() if code in allowed_codes
        }
    return filtered


def _get_ni_data(dataset_id: str) -> Optional[dict]:
    cached = ni_data_cache.get(dataset_id)
    if cached:
        return cached
    cache_file = DATA_DIR / f"data_{dataset_id}_national_ni.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        ni_data_cache[dataset_id] = data
        return data

    if dataset_id in ("population_density", "population_total"):
        dz21_file = DATA_DIR / "boundaries_ni_dz21.geojson"
        xlsx_file = DATA_DIR / "ni_ms_a01.xlsx"
        if dz21_file.exists() and xlsx_file.exists():
            pop_data = compute_ni_population_data(dz21_file, xlsx_file)
            if dataset_id in pop_data:
                result = pop_data[dataset_id]
                cache_file.write_text(json.dumps(result))
                ni_data_cache[dataset_id] = result
                return result

    if dataset_id in NI_INDICATOR_MAP:
        dz21_file = DATA_DIR / "boundaries_ni_dz21.geojson"
        ms_dir = DATA_DIR / "ni_main_statistics"
        if dz21_file.exists() and (ms_dir / ".downloaded").exists():
            result = process_ni_indicator(dataset_id, ms_dir, dz21_file)
            if result:
                cache_file.write_text(json.dumps(result))
                ni_data_cache[dataset_id] = result
                return result

    return None


def _get_ni_lgd_dz_codes(lad_code: str) -> set[str]:
    dz21_file = DATA_DIR / "boundaries_ni_dz21.geojson"
    if not dz21_file.exists():
        return set()
    return set(get_ni_lgd_dzs(lad_code, dz21_file))


def _filter_ni_data_for_lgd(lad_code: str, ni_data: dict) -> dict:
    allowed_codes = _get_ni_lgd_dz_codes(lad_code)
    if not allowed_codes:
        return {**ni_data, "values": {}, "names": {}, "stats": {}}
    values = {code: value for code, value in ni_data.get("values", {}).items() if code in allowed_codes}
    names = {code: name for code, name in ni_data.get("names", {}).items() if code in allowed_codes}
    return {
        **ni_data,
        "values": values,
        "names": names,
        "stats": compute_stats(list(values.values())) if values else {},
    }


def _build_ni_detail(dz_code: str) -> dict:
    """Detail payload for an NI Data Zone — boundary metadata + population.

    Other indicators will populate once NI_INDICATOR_MAP is implemented.
    """
    name = dz_code
    lgd_name = ""
    dz21_file = DATA_DIR / "boundaries_ni_dz21.geojson"
    if dz21_file.exists():
        try:
            geojson = json.loads(dz21_file.read_text())
            for feat in geojson.get("features", []):
                props = feat.get("properties", {}) or {}
                if props.get("DZ2021CD") == dz_code:
                    name = props.get("DZ2021NM", dz_code)
                    lgd_name = props.get("LGD2014NM", "")
                    break
        except Exception:
            pass

    categories: dict = {}
    if lgd_name:
        categories["Geography"] = {"Local Government District": lgd_name}

    indicator_ids = ["population_total", "population_density", *NI_INDICATOR_MAP.keys()]
    for dataset_id in indicator_ids:
        dataset = CENSUS_DATASETS.get(dataset_id)
        ni_data = _get_ni_data(dataset_id)
        if not dataset or not ni_data:
            continue
        value = ni_data.get("values", {}).get(dz_code)
        if value is None:
            continue
        categories.setdefault(dataset["category"], {})[dataset["label"]] = value

    return {
        "lsoa_code": dz_code,
        "name": name,
        "source": "NISRA Census 2021 — MS bulk tables (most indicators are LGD-level; population is DZ-level via MS-A01)",
        "categories": categories,
        "precomputed_percentages": True,
    }


def _build_scotland_detail(dz_code: str) -> dict:
    name = scotland_dz_names.get(dz_code, dz_code)
    categories = {}
    for dataset_id in SCOTLAND_INDICATOR_MAP:
        dataset = CENSUS_DATASETS.get(dataset_id)
        if not dataset:
            continue
        sc_data = _get_scotland_data(dataset_id)
        if not sc_data or dz_code not in sc_data.get("values", {}):
            continue
        categories.setdefault(dataset["category"], {})[dataset["label"]] = sc_data["values"][dz_code]

    return {
        "lsoa_code": dz_code,
        "name": name,
        "source": "Scotland's Census 2022 — NRS",
        "categories": categories,
        "precomputed_percentages": True,
    }


@app.get("/api/datasets")
async def get_datasets():
    return build_dataset_catalog(CENSUS_DATASETS)


@app.get("/api/lsoa/data/{dataset_id}")
async def get_lsoa_data(dataset_id: str, lad_code: Optional[str] = Query(None)):
    if dataset_id not in CENSUS_DATASETS:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if lad_code and lad_code.startswith("S12"):
        sc_data = _get_scotland_data(dataset_id)
        if sc_data:
            return _filter_scotland_data_for_council(lad_code, sc_data)
        return {"dataset_id": dataset_id, "values": {}, "names": {}, "stats": {}, "source": "No Scotland data"}

    if lad_code and lad_code.startswith("N09"):
        ni_data = _get_ni_data(dataset_id)
        if ni_data:
            return _filter_ni_data_for_lgd(lad_code, ni_data)
        return {
            "dataset_id": dataset_id,
            "values": {},
            "names": {},
            "stats": {},
            "source": "Northern Ireland data not available for this dataset",
        }

    scope = lad_code or "national"
    cache_key = f"data:{dataset_id}:{scope}"
    if cache_key in data_cache:
        ew_result = data_cache[cache_key]
    else:
        cache_file = DATA_DIR / f"data_{dataset_id}_{scope}.json"
        if cache_file.exists():
            async with aiofiles.open(cache_file) as f:
                ew_result = json.loads(await f.read())
        else:
            ew_result = await fetch_nomis_data(dataset_id, CENSUS_DATASETS[dataset_id], lad_code)
            async with aiofiles.open(cache_file, "w") as f:
                await f.write(json.dumps(ew_result))
        data_cache[cache_key] = ew_result

    if not lad_code:
        sc_data = _get_scotland_data(dataset_id)
        ni_data = _get_ni_data(dataset_id)
        if (sc_data and sc_data.get("values")) or (ni_data and ni_data.get("values")):
            merged_values = {**ew_result.get("values", {})}
            merged_names = {**ew_result.get("names", {})}
            sources = ["ONS Census 2021 (E&W)"]
            if sc_data and sc_data.get("values"):
                merged_values.update(sc_data["values"])
                merged_names.update(sc_data["names"])
                sources.append("Scotland's Census 2022")
            if ni_data and ni_data.get("values"):
                merged_values.update(ni_data["values"])
                merged_names.update(ni_data["names"])
                sources.append("NISRA Census 2021")
            merged = {
                **ew_result,
                "values": merged_values,
                "names": merged_names,
                "stats": compute_stats(list(merged_values.values())) if merged_values else {},
                "source": " + ".join(sources),
            }
            return merged

    return ew_result


@app.get("/api/boundaries/lsoa")
async def get_lsoa_boundaries(lad_code: Optional[str] = Query(None), resolution: str = Query("bsc")):
    if not lad_code:
        cache_key = "boundaries:national:merged"
        if cache_key in data_cache:
            return data_cache[cache_key]

        bsc_file = DATA_DIR / "boundaries_national_bsc.geojson"
        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
        dz21_file = DATA_DIR / "boundaries_ni_dz21.geojson"
        if not bsc_file.exists():
            raise HTTPException(status_code=503, detail="National boundaries not yet loaded")

        async with aiofiles.open(bsc_file) as f:
            ew_geojson = json.loads(await f.read())
        for feat in ew_geojson.get("features", []):
            feat["properties"]["nation"] = "EW"

        if dz22_file.exists():
            async with aiofiles.open(dz22_file) as f:
                sc_geojson = json.loads(await f.read())
            ew_geojson["features"].extend(sc_geojson.get("features", []))
        if dz21_file.exists():
            async with aiofiles.open(dz21_file) as f:
                ni_geojson = json.loads(await f.read())
            ew_geojson["features"].extend(ni_geojson.get("features", []))
        data_cache[cache_key] = ew_geojson
        return ew_geojson

    if lad_code.startswith("S12"):
        cache_key = f"boundaries:{lad_code}:dz22"
        if cache_key in data_cache:
            return data_cache[cache_key]

        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
        if not dz22_file.exists():
            raise HTTPException(status_code=503, detail="Scotland boundaries not yet loaded")

        async with aiofiles.open(dz22_file) as f:
            geojson = json.loads(await f.read())
        allowed_codes = _get_scottish_council_dz_codes(lad_code)
        geojson["features"] = [
            feature for feature in geojson.get("features", [])
            if feature.get("properties", {}).get("DZ22CD") in allowed_codes
        ]
        data_cache[cache_key] = geojson
        return geojson

    if lad_code.startswith("N09"):
        cache_key = f"boundaries:{lad_code}:dz21"
        if cache_key in data_cache:
            return data_cache[cache_key]

        dz21_file = DATA_DIR / "boundaries_ni_dz21.geojson"
        if not dz21_file.exists():
            raise HTTPException(status_code=503, detail="NI boundaries not yet loaded")

        async with aiofiles.open(dz21_file) as f:
            geojson = json.loads(await f.read())
        geojson["features"] = [
            feature for feature in geojson.get("features", [])
            if feature.get("properties", {}).get("LGD2014CD") == lad_code
        ]
        data_cache[cache_key] = geojson
        return geojson

    res = resolution.lower()
    service_url = {"bsc": BSC_URL, "bgc": BGC_URL, "bfc": BFC_URL}.get(res, BGC_URL)
    cache_key = f"boundaries:{lad_code}:{res}"
    if cache_key in data_cache:
        return data_cache[cache_key]

    geo_file = DATA_DIR / f"boundaries_{lad_code}_{res}.geojson"
    if geo_file.exists():
        async with aiofiles.open(geo_file) as f:
            geojson = json.loads(await f.read())
        data_cache[cache_key] = geojson
        return geojson

    geojson = await fetch_lad_boundaries(lad_code, service_url)
    async with aiofiles.open(geo_file, "w") as f:
        await f.write(json.dumps(geojson))
    data_cache[cache_key] = geojson
    return geojson


@app.get("/api/lsoa/detail/{lsoa_code}")
async def get_lsoa_detail_ep(lsoa_code: str):
    cache_key = f"detail:{lsoa_code}"
    if cache_key in data_cache:
        return data_cache[cache_key]

    cache_file = DATA_DIR / f"detail_{lsoa_code}.json"
    if cache_file.exists():
        async with aiofiles.open(cache_file) as f:
            result = json.loads(await f.read())
        data_cache[cache_key] = result
        return result

    if lsoa_code.startswith("S01"):
        result = _build_scotland_detail(lsoa_code)
    elif lsoa_code.startswith("N20"):
        result = _build_ni_detail(lsoa_code)
    else:
        result = await fetch_lsoa_detail(lsoa_code)

    async with aiofiles.open(cache_file, "w") as f:
        await f.write(json.dumps(result))
    data_cache[cache_key] = result
    return result


@app.get("/api/lad/list")
async def get_lad_list():
    cache_key = "lad_list"
    if cache_key in data_cache:
        return data_cache[cache_key]

    result = await fetch_lad_list()
    result["lads"].extend(SCOTTISH_COUNCIL_AREAS)
    result["lads"].extend(NI_LOCAL_GOVERNMENT_DISTRICTS)
    result["lads"].sort(key=lambda item: item["name"])

    cache_file = DATA_DIR / "lad_list.json"
    async with aiofiles.open(cache_file, "w") as f:
        await f.write(json.dumps(result))
    data_cache[cache_key] = result
    return result


@app.get("/api/elections/available")
async def get_elections_available():
    return {"elections": GE_AVAILABLE}


@app.get("/api/elections/ge/overlay")
async def get_ge_overlay(year: str = Query("2024")):
    cache_key = f"ge_{year}"
    if cache_key in election_overlay_cache:
        return election_overlay_cache[cache_key]

    boundaries_file = DATA_DIR / "elections_pcon24_bgc.geojson"
    db_file = DATA_DIR / "elections_psephology.db"

    if not boundaries_file.exists():
        raise HTTPException(status_code=503, detail="Election boundaries not yet loaded")
    if not db_file.exists():
        raise HTTPException(status_code=503, detail="Election results DB not yet loaded")

    results = process_ge_results(db_file, year=year)
    overlay = build_ge_overlay(boundaries_file, results)
    election_overlay_cache[cache_key] = overlay
    return overlay


@app.get("/api/elections/party_meta")
async def get_party_meta():
    return PARTY_META


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "cached_items": len(data_cache),
        "boundaries_ready": (DATA_DIR / "boundaries_national_bsc.geojson").exists(),
        "scotland_boundaries_ready": (DATA_DIR / "boundaries_scotland_dz22.geojson").exists(),
        "scotland_data_indicators": len(scotland_data_cache),
        "ni_boundaries_ready": (DATA_DIR / "boundaries_ni_dz21.geojson").exists(),
        "ni_data_indicators": len(ni_data_cache),
        "election_overlays_ready": list(election_overlay_cache.keys()),
    }


@app.get("/api/debug/cache")
async def debug_cache():
    files = []
    for file in sorted(DATA_DIR.iterdir()):
        size = file.stat().st_size
        features = None
        if file.suffix == ".geojson":
            try:
                features = len(json.loads(file.read_text()).get("features", []))
            except Exception:
                features = -1
        files.append({"name": file.name, "size_kb": round(size / 1024, 1), "features": features})
    return {"files": files, "memory_cache_items": len(data_cache)}


@app.delete("/api/debug/cache")
async def clear_cache(lad_code: Optional[str] = Query(None)):
    deleted = []
    for file in list(DATA_DIR.iterdir()):
        if lad_code and lad_code not in file.name:
            continue
        file.unlink()
        deleted.append(file.name)
    data_cache.clear()
    return {"deleted": deleted, "count": len(deleted)}


@app.get("/api/adjacency")
async def get_adjacency():
    return get_adjacency_payload()


@app.post("/api/selection/dissolve")
async def dissolve_selection(req: SelectionRequest):
    return dissolve_selected_geometries(req.lsoa_codes)


@app.post("/api/selection/export/shapefile")
async def export_selection_shapefile(req: SelectionRequest):
    archive = export_dissolve_as_shapefile(req.lsoa_codes)
    filename = f"census_selection_{len(req.lsoa_codes)}_areas.zip"
    return Response(
        content=archive,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/selection/aggregate")
async def aggregate_selection(req: AggregateRequest):
    if not req.lsoa_codes:
        raise HTTPException(400, "No LSOA codes provided")

    dataset_ids = req.dataset_ids or list(CENSUS_DATASETS.keys())
    selection_set = set(req.lsoa_codes)
    results = {}

    for dataset_id in dataset_ids:
        if dataset_id not in CENSUS_DATASETS:
            continue

        dataset = CENSUS_DATASETS[dataset_id]
        scope = "national"
        cache_key = f"data:{dataset_id}:{scope}"
        data = data_cache.get(cache_key)
        if not data:
            cache_file = DATA_DIR / f"data_{dataset_id}_{scope}.json"
            if cache_file.exists():
                async with aiofiles.open(cache_file) as f:
                    data = json.loads(await f.read())

        if not data or not data.get("values"):
            results[dataset_id] = {"label": dataset["label"], "unit": dataset["unit"], "value": None, "note": "Data not yet loaded"}
            continue

        values = data["values"]
        selected_values = [values[code] for code in selection_set if code in values]
        if not selected_values:
            results[dataset_id] = {"label": dataset["label"], "unit": dataset["unit"], "value": None, "note": "No data for selection"}
            continue

        if dataset["mode"] == "value":
            agg_value = sum(selected_values)
        elif dataset["mode"] == "density":
            agg_value = round(sum(selected_values) / len(selected_values), 2)
        elif dataset["mode"] == "rate":
            agg_value = await aggregate_rate_dataset(dataset_id, dataset, selection_set, selected_values, _get_scotland_data)
            if agg_value is None:
                agg_value = round(sum(selected_values) / len(selected_values), 2)
        else:
            agg_value = round(sum(selected_values) / len(selected_values), 2)

        results[dataset_id] = {
            "label": dataset["label"],
            "unit": dataset["unit"],
            "value": agg_value,
            "lsoa_count": len(selected_values),
            "stats": compute_stats(selected_values),
        }

    return {"selection_size": len(selection_set), "datasets": results}


MAX_EXPLORER_DATASETS = 10


async def _gather_explorer_payloads(dataset_ids: List[str], lad_code: Optional[str]):
    payload: dict = {}
    meta: dict = {}
    for dataset_id in dataset_ids:
        ds_data = await get_lsoa_data(dataset_id, lad_code)
        payload[dataset_id] = {
            "values": ds_data.get("values", {}),
            "names": ds_data.get("names", {}),
        }
        ds = CENSUS_DATASETS[dataset_id]
        meta[dataset_id] = {"label": ds["label"], "unit": ds["unit"]}
    return payload, meta


def _parse_dataset_ids(raw: str) -> List[str]:
    ids = [d.strip() for d in raw.split(",") if d.strip()]
    valid = [d for d in ids if d in CENSUS_DATASETS]
    return valid[:MAX_EXPLORER_DATASETS]


@app.get("/api/explorer/data")
async def explorer_data(
    datasets: str = Query(..., description="Comma-separated dataset IDs"),
    lad_code: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_dir: str = Query("asc"),
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    filter_nation: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
):
    ids = _parse_dataset_ids(datasets)
    if not ids:
        raise HTTPException(400, "No valid dataset IDs provided")

    payload, meta = await _gather_explorer_payloads(ids, lad_code)
    return build_explorer_table(
        payload,
        meta,
        sort_by=sort_by,
        sort_dir=sort_dir,
        offset=offset,
        limit=limit,
        filter_nation=filter_nation,
        search=search,
    )


@app.get("/api/explorer/export")
async def explorer_export(
    datasets: str = Query(...),
    lad_code: Optional[str] = Query(None),
    filter_nation: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_dir: str = Query("asc"),
):
    ids = _parse_dataset_ids(datasets)
    if not ids:
        raise HTTPException(400, "No valid dataset IDs provided")

    payload, meta = await _gather_explorer_payloads(ids, lad_code)
    table = build_explorer_table(
        payload,
        meta,
        sort_by=sort_by,
        sort_dir=sort_dir,
        offset=0,
        limit=0,
        filter_nation=filter_nation,
        search=search,
    )

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([table["column_labels"].get(col, col) for col in table["columns"]])
    for row in table["rows"]:
        writer.writerow([
            "" if row.get(col) is None else row.get(col)
            for col in table["columns"]
        ])

    filename = f"census_explorer_{len(ids)}_datasets.csv"
    return Response(
        content=buf.getvalue().encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
