import csv
import io
import logging
from typing import Callable, Optional

import httpx

from scotland import SCOTLAND_INDICATOR_MAP
from services.dataset_config import DETAIL_DATASETS, NOMIS

logger = logging.getLogger(__name__)


def compute_stats(values):
    if not values:
        return {}
    sorted_values = sorted(values)
    n = len(sorted_values)
    return {
        "min": sorted_values[0],
        "max": sorted_values[-1],
        "mean": round(sum(values) / n, 2),
        "p10": sorted_values[int(n * 0.1)],
        "p25": sorted_values[int(n * 0.25)],
        "p50": sorted_values[int(n * 0.5)],
        "p75": sorted_values[int(n * 0.75)],
        "p90": sorted_values[int(n * 0.9)],
        "count": n,
    }


def build_dataset_catalog(census_datasets: dict) -> dict:
    sc_ids = set(SCOTLAND_INDICATOR_MAP.keys()) | {"population_density", "population_total"}
    categories = {}
    for key, dataset in census_datasets.items():
        categories.setdefault(dataset["category"], []).append(
            {
                "id": key,
                "label": dataset["label"],
                "description": dataset["description"],
                "unit": dataset["unit"],
                "color_scheme": dataset["color_scheme"],
                "coverage": "uk" if key in sc_ids else "ew",
            }
        )
    return {"categories": categories, "total": len(census_datasets)}


async def fetch_nomis_data(dataset_id, ds, lad_code):
    nomis_id = ds["nomis_id"]
    cat_dim = ds["cat_dim"]
    cat_code = ds["cat_code"]
    cat_code_total = ds["cat_code_total"]
    mode = ds["mode"]
    geo = f"{lad_code}TYPE151" if lad_code else "TYPE151"

    async with httpx.AsyncClient(timeout=120.0) as client:
        if mode in ("density", "value"):
            values, names = await _nomis_single(client, nomis_id, geo, cat_dim, cat_code, bool(lad_code))
        elif mode == "rate":
            values, names = await _nomis_rate(client, nomis_id, geo, cat_dim, cat_code, cat_code_total, bool(lad_code))
        else:
            values, names = {}, {}

    if not values:
        return {"dataset_id": dataset_id, "values": {}, "names": {}, "stats": {}, "source": "No data"}

    return {
        "dataset_id": dataset_id,
        "values": values,
        "names": names,
        "stats": compute_stats(list(values.values())),
        "source": "ONS Census 2021 via Nomis",
    }


async def _nomis_single(client, nomis_id, geo, cat_dim, cat_code, is_lad):
    values = {}
    names = {}
    offset = 0
    while True:
        params = {
            "date": "latest",
            "geography": geo,
            "measures": "20100",
            "select": "geography_code,geography_name,obs_value",
            "recordoffset": str(offset),
            "recordlimit": "25000",
        }
        if cat_dim and cat_code:
            params[cat_dim] = cat_code if isinstance(cat_code, str) else ",".join(cat_code)
        resp = await client.get(f"{NOMIS}/{nomis_id}.data.csv", params=params)
        resp.raise_for_status()
        count = 0
        for row in csv.DictReader(io.StringIO(resp.text)):
            code = row.get("GEOGRAPHY_CODE", "")
            if not code:
                continue
            try:
                values[code] = round(float(row.get("OBS_VALUE", 0)), 2)
            except Exception:
                values[code] = 0
            names[code] = row.get("GEOGRAPHY_NAME", "")
            count += 1
        logger.info(f"  Nomis {nomis_id} offset={offset}: {count} rows")
        if count < 25000 or is_lad:
            break
        offset += 25000
    return values, names


async def _nomis_rate(client, nomis_id, geo, cat_dim, cat_code, cat_code_total, is_lad):
    codes_to_fetch = set(cat_code if isinstance(cat_code, list) else [cat_code])
    codes_to_fetch.add(cat_code_total)
    lsoa_cats = {}
    names = {}
    dim_col = cat_dim.upper()
    offset = 0

    while True:
        params = {
            "date": "latest",
            "geography": geo,
            "measures": "20100",
            cat_dim: ",".join(sorted(codes_to_fetch)),
            "select": f"geography_code,geography_name,{cat_dim},obs_value",
            "recordoffset": str(offset),
            "recordlimit": "25000",
        }
        resp = await client.get(f"{NOMIS}/{nomis_id}.data.csv", params=params)
        resp.raise_for_status()
        count = 0
        for row in csv.DictReader(io.StringIO(resp.text)):
            code = row.get("GEOGRAPHY_CODE", "")
            if not code:
                continue
            names[code] = row.get("GEOGRAPHY_NAME", "")
            cv = row.get(dim_col, "")
            try:
                obs = float(row.get("OBS_VALUE", 0))
            except Exception:
                obs = 0
            lsoa_cats.setdefault(code, {})[cv] = obs
            count += 1
        logger.info(f"  Nomis rate {nomis_id} offset={offset}: {count} rows")
        if count < 25000 or is_lad:
            break
        offset += 25000

    numerator_codes = cat_code if isinstance(cat_code, list) else [cat_code]
    values = {}
    for lsoa, cats in lsoa_cats.items():
        total = cats.get(cat_code_total, 0)
        values[lsoa] = round(sum(cats.get(code, 0) for code in numerator_codes) / total * 100, 2) if total > 0 else 0.0
    return values, names


async def aggregate_rate_dataset(
    ds_id: str,
    ds: dict,
    selection_set: set[str],
    selected_values: list[float],
    get_scotland_data: Callable[[str], Optional[dict]],
) -> Optional[float]:
    ew_codes = sorted(code for code in selection_set if not code.startswith("S01"))
    sc_codes = sorted(code for code in selection_set if code.startswith("S01"))

    total_num = 0.0
    total_den = 0.0

    if ew_codes:
        ew_num, ew_den = await fetch_nomis_rate_components_for_codes(ds, ew_codes)
        total_num += ew_num
        total_den += ew_den

    if sc_codes and ds_id in SCOTLAND_INDICATOR_MAP:
        sc_data = get_scotland_data(ds_id)
        if sc_data:
            numerators = sc_data.get("numerators", {})
            denominators = sc_data.get("denominators", {})
            total_num += sum(numerators.get(code, 0.0) for code in sc_codes)
            total_den += sum(denominators.get(code, 0.0) for code in sc_codes)

    if total_den > 0:
        return round(total_num / total_den * 100, 2)
    if selected_values:
        return round(sum(selected_values) / len(selected_values), 2)
    return None


async def fetch_nomis_rate_components_for_codes(ds: dict, geography_codes: list[str]) -> tuple[float, float]:
    if not geography_codes or not ds.get("cat_dim") or ds.get("cat_code_total") is None:
        return 0.0, 0.0

    numerator_codes = ds["cat_code"] if isinstance(ds["cat_code"], list) else [ds["cat_code"]]
    codes_to_fetch = set(numerator_codes)
    codes_to_fetch.add(ds["cat_code_total"])
    total_num = 0.0
    total_den = 0.0

    async with httpx.AsyncClient(timeout=60.0) as client:
        for i in range(0, len(geography_codes), 100):
            batch = geography_codes[i:i + 100]
            params = {
                "date": "latest",
                "geography": ",".join(batch),
                "measures": "20100",
                ds["cat_dim"]: ",".join(sorted(codes_to_fetch)),
                "select": f"geography_code,{ds['cat_dim']},obs_value",
            }
            resp = await client.get(f"{NOMIS}/{ds['nomis_id']}.data.csv", params=params)
            resp.raise_for_status()

            dim_col = ds["cat_dim"].upper()
            for row in csv.DictReader(io.StringIO(resp.text)):
                cat_value = row.get(dim_col, "")
                try:
                    obs = float(row.get("OBS_VALUE", 0))
                except Exception:
                    obs = 0.0
                if cat_value in numerator_codes:
                    total_num += obs
                elif cat_value == ds["cat_code_total"]:
                    total_den += obs

    return total_num, total_den


async def fetch_lsoa_detail(lsoa_code):
    detail = {"lsoa_code": lsoa_code, "name": lsoa_code, "source": "ONS Census 2021 via Nomis", "categories": {}}
    async with httpx.AsyncClient(timeout=30.0) as client:
        for dataset in DETAIL_DATASETS:
            try:
                cat_dim = dataset["cat_dim"]
                params = {
                    "date": "latest",
                    "geography": lsoa_code,
                    "measures": "20100",
                    "select": f"geography_name,{cat_dim},{cat_dim}_name,obs_value",
                }
                resp = await client.get(f"{NOMIS}/{dataset['nomis_id']}.data.csv", params=params, timeout=15.0)
                if resp.status_code != 200:
                    continue

                cat_data = {}
                display_name_col = f"{cat_dim.upper()}_NAME"
                for row in csv.DictReader(io.StringIO(resp.text)):
                    label = row.get(display_name_col, "")
                    if not label:
                        continue
                    try:
                        value = float(row.get("OBS_VALUE", 0))
                    except Exception:
                        value = 0
                    if detail["name"] == lsoa_code:
                        geography_name = row.get("GEOGRAPHY_NAME", "")
                        if geography_name:
                            detail["name"] = geography_name
                    cat_data[label] = value
                if cat_data:
                    detail["categories"][dataset["label"]] = cat_data
            except Exception as exc:
                logger.warning(f"Detail error {dataset['label']}/{lsoa_code}: {exc}")

    return detail


async def fetch_lad_list():
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(f"{NOMIS}/NM_2026_1/geography/TYPE154.def.sdmx.json")
            resp.raise_for_status()
            data = resp.json()
            lads = []
            for code_list in data.get("structure", {}).get("codelists", {}).get("codelist", []):
                for code in code_list.get("code", []):
                    geog_code = ""
                    for annotation in code.get("annotations", {}).get("annotation", []):
                        if annotation.get("annotationtitle") == "GeogCode":
                            geog_code = str(annotation.get("annotationtext", ""))
                    name = code.get("description", {}).get("value", "")
                    if geog_code and name:
                        lads.append({"code": geog_code, "name": name})
            lads.sort(key=lambda item: item["name"])
            return {"lads": lads}
        except Exception as exc:
            logger.warning(f"LAD list error: {exc}")
            return {"lads": [{"code": "E09000033", "name": "Westminster"}, {"code": "E08000003", "name": "Manchester"}]}
