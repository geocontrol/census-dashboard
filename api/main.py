"""
UK Census 2021 — LSOA Explorer API  (v3)
National-scale LSOA data via Nomis + pre-fetched BSC/BGC boundaries
"""
import csv, io, json, asyncio, logging
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from cachetools import TTLCache
import aiofiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="UK Census LSOA Explorer API", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

data_cache = TTLCache(maxsize=500, ttl=86400)
DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(exist_ok=True)

NOMIS = "https://www.nomisweb.co.uk/api/v01/dataset"
ARCGIS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
BSC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BSC_V4/FeatureServer/0/query"
BGC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V5/FeatureServer/0/query"
BFC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"
LOOKUP_URL = f"{ARCGIS}/LSOA21_WD22_LAD22_EW_LU_v3/FeatureServer/0/query"

CENSUS_DATASETS = {
    "population_density": {"label":"Population Density","category":"Population","description":"Usual resident population per km²","unit":"per km²","color_scheme":"YlOrRd","nomis_id":"NM_2026_1","cat_dim":None,"cat_code_total":None,"cat_code":None,"mode":"density"},
    "population_total": {"label":"Total Population","category":"Population","description":"Usual resident population count","unit":"persons","color_scheme":"PuBu","nomis_id":"NM_2021_1","cat_dim":"c2021_restype_3","cat_code_total":None,"cat_code":"0","mode":"value"},
    "sex_female": {"label":"Female %","category":"Population","description":"% of usual residents who are female","unit":"%","color_scheme":"RdPu","nomis_id":"NM_2028_1","cat_dim":"c_sex","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "qualifications_level4": {"label":"Level 4+ Qualifications","category":"Education","description":"% with degree-level or higher","unit":"%","color_scheme":"YlGn","nomis_id":"NM_2084_1","cat_dim":"c2021_hiqual_8","cat_code_total":"0","cat_code":"6","mode":"rate"},
    "no_qualifications": {"label":"No Qualifications","category":"Education","description":"% with no formal qualifications","unit":"%","color_scheme":"Reds","nomis_id":"NM_2084_1","cat_dim":"c2021_hiqual_8","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "health_good": {"label":"Good / Very Good Health","category":"Health","description":"% reporting good or very good health","unit":"%","color_scheme":"Greens","nomis_id":"NM_2055_1","cat_dim":"c2021_health_6","cat_code_total":"0","cat_code":["1","2"],"mode":"rate"},
    "health_bad": {"label":"Bad / Very Bad Health","category":"Health","description":"% reporting bad or very bad health","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2055_1","cat_dim":"c2021_health_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "unpaid_care": {"label":"Provides Unpaid Care","category":"Health","description":"% providing any unpaid care","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2057_1","cat_dim":"c2021_carer_7","cat_code_total":"0","cat_code":["101","102","6"],"mode":"rate"},
    "economic_activity": {"label":"Economically Active","category":"Economy","description":"% economically active (excl. FT students)","unit":"%","color_scheme":"GnBu","nomis_id":"NM_2083_1","cat_dim":"c2021_eastat_20","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "unemployment": {"label":"Unemployment","category":"Economy","description":"% unemployed of economically active","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2083_1","cat_dim":"c2021_eastat_20","cat_code_total":"1001","cat_code":"7","mode":"rate"},
    "nssec_higher_managerial": {"label":"Higher Managerial / Professional","category":"Economy","description":"% in higher managerial & professional occupations","unit":"%","color_scheme":"Blues","nomis_id":"NM_2079_1","cat_dim":"c2021_nssec_10","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "nssec_routine": {"label":"Routine Occupations","category":"Economy","description":"% in routine occupations","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2079_1","cat_dim":"c2021_nssec_10","cat_code_total":"0","cat_code":"7","mode":"rate"},
    "home_ownership": {"label":"Home Ownership","category":"Housing","description":"% owner-occupied (outright + mortgage)","unit":"%","color_scheme":"Blues","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "social_rented": {"label":"Social Rented","category":"Housing","description":"% social rented households","unit":"%","color_scheme":"Purples","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9","cat_code_total":"0","cat_code":"1003","mode":"rate"},
    "private_rented": {"label":"Private Rented","category":"Housing","description":"% private rented households","unit":"%","color_scheme":"BuPu","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9","cat_code_total":"0","cat_code":"1004","mode":"rate"},
    "accommodation_detached": {"label":"Detached Houses","category":"Housing","description":"% living in detached houses","unit":"%","color_scheme":"YlGn","nomis_id":"NM_2062_1","cat_dim":"c2021_acctype_9","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "accommodation_flat": {"label":"Flats","category":"Housing","description":"% living in purpose-built flats","unit":"%","color_scheme":"GnBu","nomis_id":"NM_2062_1","cat_dim":"c2021_acctype_9","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "overcrowded": {"label":"Overcrowded","category":"Housing","description":"% with occupancy rating -1 or worse (bedrooms)","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2070_1","cat_dim":"c2021_occrat_bedrooms_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "deprivation_none": {"label":"Not Deprived","category":"Deprivation","description":"% households not deprived in any dimension","unit":"%","color_scheme":"Greens","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "deprivation_3plus": {"label":"Deprived 3+ Dimensions","category":"Deprivation","description":"% households deprived in 3 or 4 dimensions","unit":"%","color_scheme":"Reds","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "car_none": {"label":"No Car Households","category":"Transport","description":"% households with no car or van","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2063_1","cat_dim":"c2021_cars_5","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "travel_car": {"label":"Drive to Work","category":"Transport","description":"% commuting by car or van","unit":"%","color_scheme":"Greys","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12","cat_code_total":"0","cat_code":"7","mode":"rate"},
    "travel_public": {"label":"Public Transport to Work","category":"Transport","description":"% commuting by bus, train, tube or tram","unit":"%","color_scheme":"BuGn","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12","cat_code_total":"0","cat_code":["2","3","4"],"mode":"rate"},
    "work_from_home": {"label":"Work from Home","category":"Transport","description":"% working mainly at or from home","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "white_british": {"label":"White British","category":"Ethnicity & Identity","description":"% White: English/Welsh/Scottish/NI/British","unit":"%","color_scheme":"Greys","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "ethnic_asian": {"label":"Asian","category":"Ethnicity & Identity","description":"% Asian, Asian British or Asian Welsh","unit":"%","color_scheme":"YlGnBu","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "ethnic_black": {"label":"Black","category":"Ethnicity & Identity","description":"% Black, Black British, Black Welsh, Caribbean or African","unit":"%","color_scheme":"BuPu","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1002","mode":"rate"},
    "born_uk": {"label":"Born in UK","category":"Ethnicity & Identity","description":"% born in the United Kingdom","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2024_1","cat_dim":"c2021_cob_12","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "no_english": {"label":"No English in Household","category":"Ethnicity & Identity","description":"% households with no English speakers","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2044_1","cat_dim":"c2021_hhlang_5","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "christian": {"label":"Christian","category":"Religion","description":"% identifying as Christian","unit":"%","color_scheme":"RdPu","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"2","mode":"rate"},
    "no_religion": {"label":"No Religion","category":"Religion","description":"% with no religion","unit":"%","color_scheme":"YlGnBu","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "muslim": {"label":"Muslim","category":"Religion","description":"% identifying as Muslim","unit":"%","color_scheme":"BuGn","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"6","mode":"rate"},
}

DETAIL_DATASETS = [
    {"label":"Population","nomis_id":"NM_2021_1","cat_dim":"c2021_restype_3"},
    {"label":"Ethnic Group","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20"},
    {"label":"Country of Birth","nomis_id":"NM_2024_1","cat_dim":"c2021_cob_12"},
    {"label":"Religion","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10"},
    {"label":"Health","nomis_id":"NM_2055_1","cat_dim":"c2021_health_6"},
    {"label":"Qualifications","nomis_id":"NM_2084_1","cat_dim":"c2021_hiqual_8"},
    {"label":"Economic Activity","nomis_id":"NM_2083_1","cat_dim":"c2021_eastat_20"},
    {"label":"NS-SeC","nomis_id":"NM_2079_1","cat_dim":"c2021_nssec_10"},
    {"label":"Tenure","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9"},
    {"label":"Accommodation Type","nomis_id":"NM_2062_1","cat_dim":"c2021_acctype_9"},
    {"label":"Bedrooms","nomis_id":"NM_2068_1","cat_dim":"c2021_bedrooms_5"},
    {"label":"Occupancy (Bedrooms)","nomis_id":"NM_2070_1","cat_dim":"c2021_occrat_bedrooms_6"},
    {"label":"Deprivation","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6"},
    {"label":"Car/Van Availability","nomis_id":"NM_2063_1","cat_dim":"c2021_cars_5"},
    {"label":"Travel to Work","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12"},
    {"label":"Unpaid Care","nomis_id":"NM_2057_1","cat_dim":"c2021_carer_7"},
    {"label":"Household Language","nomis_id":"NM_2044_1","cat_dim":"c2021_hhlang_5"},
]

# ═══════ Startup: pre-fetch national BSC boundaries ═══════

@app.on_event("startup")
async def startup_prefetch():
    bsc_file = DATA_DIR / "boundaries_national_bsc.geojson"
    if bsc_file.exists():
        logger.info(f"National BSC boundaries cached ({bsc_file.stat().st_size/1024/1024:.1f} MB)")
        return
    logger.info("Pre-fetching national BSC boundaries (~19 MB, ~20s)...")
    try:
        geojson = await _fetch_all_boundaries(BSC_URL, "BSC")
        async with aiofiles.open(bsc_file, "w") as f:
            await f.write(json.dumps(geojson))
        logger.info(f"BSC cached: {len(geojson['features'])} features, {bsc_file.stat().st_size/1024/1024:.1f} MB")
    except Exception as e:
        logger.error(f"BSC pre-fetch failed: {e}")

async def _fetch_all_boundaries(service_url, label):
    all_features = []
    offset = 0
    page = 0
    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            page += 1
            resp = await client.post(service_url, data={
                "where": "1=1", "outFields": "LSOA21CD,LSOA21NM",
                "outSR": "4326", "f": "geojson",
                "resultOffset": str(offset), "resultRecordCount": "2000",
            })
            features = resp.json().get("features", [])
            all_features.extend(features)
            logger.info(f"  {label} page {page}: +{len(features)} (total {len(all_features)})")
            if len(features) < 2000:
                break
            offset += 2000
    return {"type": "FeatureCollection", "features": all_features}

# ═══════ Endpoints ═══════

@app.get("/api/datasets")
async def get_datasets():
    categories = {}
    for key, ds in CENSUS_DATASETS.items():
        cat = ds["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append({"id": key, "label": ds["label"], "description": ds["description"], "unit": ds["unit"], "color_scheme": ds["color_scheme"]})
    return {"categories": categories, "total": len(CENSUS_DATASETS)}

@app.get("/api/lsoa/data/{dataset_id}")
async def get_lsoa_data(dataset_id: str, lad_code: Optional[str] = Query(None)):
    if dataset_id not in CENSUS_DATASETS:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    scope = lad_code or "national"
    cache_key = f"data:{dataset_id}:{scope}"
    if cache_key in data_cache:
        return data_cache[cache_key]
    cache_file = DATA_DIR / f"data_{dataset_id}_{scope}.json"
    if cache_file.exists():
        async with aiofiles.open(cache_file) as f:
            result = json.loads(await f.read())
        data_cache[cache_key] = result
        return result
    ds = CENSUS_DATASETS[dataset_id]
    result = await fetch_nomis_data(dataset_id, ds, lad_code)
    async with aiofiles.open(cache_file, "w") as f:
        await f.write(json.dumps(result))
    data_cache[cache_key] = result
    return result

@app.get("/api/boundaries/lsoa")
async def get_lsoa_boundaries(lad_code: Optional[str] = Query(None), resolution: str = Query("bsc")):
    if not lad_code:
        bsc_file = DATA_DIR / "boundaries_national_bsc.geojson"
        if bsc_file.exists():
            ck = "boundaries:national:bsc"
            if ck in data_cache:
                return data_cache[ck]
            async with aiofiles.open(bsc_file) as f:
                geojson = json.loads(await f.read())
            data_cache[ck] = geojson
            return geojson
        raise HTTPException(status_code=503, detail="National boundaries not yet loaded")
    res = resolution.lower()
    svc = {"bsc": BSC_URL, "bgc": BGC_URL, "bfc": BFC_URL}.get(res, BGC_URL)
    ck = f"boundaries:{lad_code}:{res}"
    if ck in data_cache:
        return data_cache[ck]
    geo_file = DATA_DIR / f"boundaries_{lad_code}_{res}.geojson"
    if geo_file.exists():
        async with aiofiles.open(geo_file) as f:
            geojson = json.loads(await f.read())
        data_cache[ck] = geojson
        return geojson
    geojson = await fetch_lad_boundaries(lad_code, svc)
    async with aiofiles.open(geo_file, "w") as f:
        await f.write(json.dumps(geojson))
    data_cache[ck] = geojson
    return geojson

@app.get("/api/lsoa/detail/{lsoa_code}")
async def get_lsoa_detail_ep(lsoa_code: str):
    ck = f"detail:{lsoa_code}"
    if ck in data_cache:
        return data_cache[ck]
    cf = DATA_DIR / f"detail_{lsoa_code}.json"
    if cf.exists():
        async with aiofiles.open(cf) as f:
            result = json.loads(await f.read())
        data_cache[ck] = result
        return result
    result = await fetch_lsoa_detail(lsoa_code)
    async with aiofiles.open(cf, "w") as f:
        await f.write(json.dumps(result))
    data_cache[ck] = result
    return result

@app.get("/api/lad/list")
async def get_lad_list():
    ck = "lad_list"
    if ck in data_cache:
        return data_cache[ck]
    cf = DATA_DIR / "lad_list.json"
    if cf.exists():
        async with aiofiles.open(cf) as f:
            result = json.loads(await f.read())
        data_cache[ck] = result
        return result
    result = await fetch_lad_list()
    async with aiofiles.open(cf, "w") as f:
        await f.write(json.dumps(result))
    data_cache[ck] = result
    return result

@app.get("/api/health")
async def health():
    bsc = DATA_DIR / "boundaries_national_bsc.geojson"
    return {"status": "ok", "cached_items": len(data_cache), "boundaries_ready": bsc.exists()}

@app.get("/api/debug/cache")
async def debug_cache():
    files = []
    for f in sorted(DATA_DIR.iterdir()):
        s = f.stat().st_size
        fc = None
        if f.suffix == ".geojson":
            try: fc = len(json.loads(f.read_text()).get("features", []))
            except: fc = -1
        files.append({"name": f.name, "size_kb": round(s/1024, 1), "features": fc})
    return {"files": files, "memory_cache_items": len(data_cache)}

@app.delete("/api/debug/cache")
async def clear_cache(lad_code: Optional[str] = Query(None)):
    deleted = []
    for f in list(DATA_DIR.iterdir()):
        if lad_code:
            if lad_code in f.name: f.unlink(); deleted.append(f.name)
        else:
            f.unlink(); deleted.append(f.name)
    data_cache.clear()
    return {"deleted": deleted, "count": len(deleted)}

# ═══════ Nomis fetchers ═══════

async def fetch_nomis_data(dataset_id, ds, lad_code):
    nomis_id, cat_dim, cat_code, cat_code_total, mode = ds["nomis_id"], ds["cat_dim"], ds["cat_code"], ds["cat_code_total"], ds["mode"]
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
    return {"dataset_id": dataset_id, "values": values, "names": names, "stats": compute_stats(list(values.values())), "source": "ONS Census 2021 via Nomis"}

async def _nomis_single(client, nomis_id, geo, cat_dim, cat_code, is_lad):
    values, names = {}, {}
    offset = 0
    while True:
        params = {"date": "latest", "geography": geo, "measures": "20100",
                  "select": "geography_code,geography_name,obs_value",
                  "recordoffset": str(offset), "recordlimit": "25000"}
        if cat_dim and cat_code:
            params[cat_dim] = cat_code if isinstance(cat_code, str) else ",".join(cat_code)
        resp = await client.get(f"{NOMIS}/{nomis_id}.data.csv", params=params)
        resp.raise_for_status()
        count = 0
        for row in csv.DictReader(io.StringIO(resp.text)):
            code = row.get("GEOGRAPHY_CODE", "")
            if code:
                try: values[code] = round(float(row.get("OBS_VALUE", 0)), 2)
                except: values[code] = 0
                names[code] = row.get("GEOGRAPHY_NAME", "")
                count += 1
        logger.info(f"  Nomis {nomis_id} offset={offset}: {count} rows")
        if count < 25000 or is_lad: break
        offset += 25000
    return values, names

async def _nomis_rate(client, nomis_id, geo, cat_dim, cat_code, cat_code_total, is_lad):
    codes_to_fetch = set()
    if isinstance(cat_code, list): codes_to_fetch.update(cat_code)
    else: codes_to_fetch.add(cat_code)
    codes_to_fetch.add(cat_code_total)
    lsoa_cats, names = {}, {}
    dim_col = cat_dim.upper()
    offset = 0
    while True:
        params = {"date": "latest", "geography": geo, "measures": "20100",
                  cat_dim: ",".join(sorted(codes_to_fetch)),
                  "select": f"geography_code,geography_name,{cat_dim},obs_value",
                  "recordoffset": str(offset), "recordlimit": "25000"}
        resp = await client.get(f"{NOMIS}/{nomis_id}.data.csv", params=params)
        resp.raise_for_status()
        count = 0
        for row in csv.DictReader(io.StringIO(resp.text)):
            code = row.get("GEOGRAPHY_CODE", "")
            if code:
                names[code] = row.get("GEOGRAPHY_NAME", "")
                cv = row.get(dim_col, "")
                try: obs = float(row.get("OBS_VALUE", 0))
                except: obs = 0
                lsoa_cats.setdefault(code, {})[cv] = obs
                count += 1
        logger.info(f"  Nomis rate {nomis_id} offset={offset}: {count} rows")
        if count < 25000 or is_lad: break
        offset += 25000
    numerator_codes = cat_code if isinstance(cat_code, list) else [cat_code]
    values = {}
    for lsoa, cats in lsoa_cats.items():
        total = cats.get(cat_code_total, 0)
        if total > 0:
            values[lsoa] = round(sum(cats.get(c, 0) for c in numerator_codes) / total * 100, 2)
        else:
            values[lsoa] = 0.0
    return values, names

async def fetch_lsoa_detail(lsoa_code):
    detail = {"lsoa_code": lsoa_code, "name": lsoa_code, "source": "ONS Census 2021 via Nomis", "categories": {}}
    async with httpx.AsyncClient(timeout=30.0) as client:
        for dd in DETAIL_DATASETS:
            try:
                cd = dd["cat_dim"]
                params = {"date": "latest", "geography": lsoa_code, "measures": "20100",
                          "select": f"geography_name,{cd},{cd}_name,obs_value"}
                resp = await client.get(f"{NOMIS}/{dd['nomis_id']}.data.csv", params=params, timeout=15.0)
                if resp.status_code != 200: continue
                cat_data = {}
                dnc = f"{cd.upper()}_NAME"
                for row in csv.DictReader(io.StringIO(resp.text)):
                    label = row.get(dnc, "")
                    if not label: continue
                    try: val = float(row.get("OBS_VALUE", 0))
                    except: val = 0
                    if detail["name"] == lsoa_code:
                        gn = row.get("GEOGRAPHY_NAME", "")
                        if gn: detail["name"] = gn
                    cat_data[label] = val
                if cat_data: detail["categories"][dd["label"]] = cat_data
            except Exception as e:
                logger.warning(f"Detail error {dd['label']}/{lsoa_code}: {e}")
    return detail

async def fetch_lad_boundaries(lad_code, service_url):
    all_features = []
    async with httpx.AsyncClient(timeout=60.0) as client:
        lsoa_codes, offset = [], 0
        while True:
            resp = await client.post(LOOKUP_URL, data={
                "where": f"LAD22CD='{lad_code}'", "outFields": "LSOA21CD",
                "f": "json", "resultOffset": str(offset), "resultRecordCount": "1000"})
            data = resp.json()
            if "error" in data: break
            features = data.get("features", [])
            lsoa_codes.extend(f["attributes"]["LSOA21CD"] for f in features)
            if len(features) < 1000: break
            offset += 1000
        logger.info(f"Resolved {len(lsoa_codes)} LSOAs for {lad_code}")
        if not lsoa_codes:
            return {"type": "FeatureCollection", "features": []}
        for i in range(0, len(lsoa_codes), 150):
            batch = lsoa_codes[i:i+150]
            codes_sql = ",".join(f"'{c}'" for c in batch)
            try:
                resp = await client.post(service_url, data={
                    "where": f"LSOA21CD IN ({codes_sql})", "outFields": "LSOA21CD,LSOA21NM",
                    "outSR": "4326", "f": "geojson", "resultRecordCount": "1000"})
                data = resp.json()
                if "error" not in data:
                    feats = data.get("features", [])
                    for feat in feats: feat["properties"]["LAD22CD"] = lad_code
                    all_features.extend(feats)
            except Exception as e:
                logger.error(f"Boundary batch error: {e}")
    return {"type": "FeatureCollection", "features": all_features}

async def fetch_lad_list():
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(f"{NOMIS}/NM_2026_1/geography/TYPE154.def.sdmx.json")
            resp.raise_for_status()
            data = resp.json()
            lads = []
            for cl in data.get("structure",{}).get("codelists",{}).get("codelist",[]):
                for code in cl.get("code",[]):
                    gc = ""
                    for ann in code.get("annotations",{}).get("annotation",[]):
                        if ann.get("annotationtitle") == "GeogCode":
                            gc = str(ann.get("annotationtext",""))
                    name = code.get("description",{}).get("value","")
                    if gc and name: lads.append({"code": gc, "name": name})
            lads.sort(key=lambda x: x["name"])
            return {"lads": lads}
        except Exception as e:
            logger.warning(f"LAD list error: {e}")
            return {"lads": [{"code":"E09000033","name":"Westminster"},{"code":"E08000003","name":"Manchester"}]}

def compute_stats(values):
    if not values: return {}
    sv = sorted(values)
    n = len(sv)
    return {"min":sv[0],"max":sv[-1],"mean":round(sum(values)/n,2),
            "p10":sv[int(n*0.1)],"p25":sv[int(n*0.25)],"p50":sv[int(n*0.5)],
            "p75":sv[int(n*0.75)],"p90":sv[int(n*0.9)],"count":n}
