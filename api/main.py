"""
UK Census Explorer API  (v4)
National-scale LSOA (E&W) + Data Zone (Scotland) data
"""
import csv, io, json, asyncio, logging
from pathlib import Path
from typing import Optional, List

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from cachetools import TTLCache
import aiofiles
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from shapely import buffer as shp_buffer

from scotland import (
    download_dz22_boundaries,
    download_oa_dz_lookup,
    download_scotland_census_csvs,
    process_all_scotland_indicators,
    process_scotland_indicator,
    compute_scotland_population_data,
    SCOTLAND_INDICATOR_MAP,
    SCOTTISH_COUNCIL_AREAS,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="UK Census Explorer API", version="4.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

data_cache = TTLCache(maxsize=500, ttl=86400)
DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(exist_ok=True)

# In-memory adjacency graph: {area_code: [neighbour_codes]}
adjacency_graph: dict = {}
# In-memory geometry index: {area_code: shapely.Geometry}
lsoa_geometries: dict = {}

# Scotland state
scotland_oa_to_dz: dict = {}
scotland_dz_names: dict = {}
scotland_data_cache: dict = {}  # {dataset_id: {values, names, stats}}

class SelectionRequest(BaseModel):
    lsoa_codes: List[str]

class AggregateRequest(BaseModel):
    lsoa_codes: List[str]
    dataset_ids: Optional[List[str]] = None  # None = all datasets

NOMIS = "https://www.nomisweb.co.uk/api/v01/dataset"
ARCGIS = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
BSC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BSC_V4/FeatureServer/0/query"
BGC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V5/FeatureServer/0/query"
BFC_URL = f"{ARCGIS}/Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0/query"
LOOKUP_URL = f"{ARCGIS}/LSOA21_WD22_LAD22_EW_LU_v3/FeatureServer/0/query"

CENSUS_DATASETS = {
    # ── Population ──────────────────────────────
    "population_density": {"label":"Population Density","category":"Population","description":"Usual residents per km²","unit":"per km²","color_scheme":"YlOrRd","nomis_id":"NM_2026_1","cat_dim":None,"cat_code_total":None,"cat_code":None,"mode":"density"},
    "population_total": {"label":"Total Population","category":"Population","description":"Usual resident population count","unit":"persons","color_scheme":"PuBu","nomis_id":"NM_2021_1","cat_dim":"c2021_restype_3","cat_code_total":None,"cat_code":"0","mode":"value"},
    "sex_female": {"label":"Female %","category":"Population","description":"% of usual residents who are female","unit":"%","color_scheme":"RdPu","nomis_id":"NM_2028_1","cat_dim":"c_sex","cat_code_total":"0","cat_code":"1","mode":"rate"},
    # Age structure (TS007A)
    "age_0_14": {"label":"Aged 0–14","category":"Age","description":"% aged 0 to 14","unit":"%","color_scheme":"YlGn","nomis_id":"NM_2020_1","cat_dim":"c2021_age_19","cat_code_total":"0","cat_code":["1","2","3"],"mode":"rate"},
    "age_15_24": {"label":"Aged 15–24","category":"Age","description":"% aged 15 to 24","unit":"%","color_scheme":"BuGn","nomis_id":"NM_2020_1","cat_dim":"c2021_age_19","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "age_25_44": {"label":"Aged 25–44","category":"Age","description":"% aged 25 to 44","unit":"%","color_scheme":"GnBu","nomis_id":"NM_2020_1","cat_dim":"c2021_age_19","cat_code_total":"0","cat_code":["6","7","8","9"],"mode":"rate"},
    "age_45_64": {"label":"Aged 45–64","category":"Age","description":"% aged 45 to 64","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2020_1","cat_dim":"c2021_age_19","cat_code_total":"0","cat_code":["10","11","12","13"],"mode":"rate"},
    "age_65_plus": {"label":"Aged 65+","category":"Age","description":"% aged 65 and over","unit":"%","color_scheme":"Purples","nomis_id":"NM_2020_1","cat_dim":"c2021_age_19","cat_code_total":"0","cat_code":["14","15","16","17","18"],"mode":"rate"},
    # ── Education ──────────────────────────────
    "qualifications_level4": {"label":"Level 4+ Qualifications","category":"Education","description":"% with degree-level or higher","unit":"%","color_scheme":"YlGn","nomis_id":"NM_2084_1","cat_dim":"c2021_hiqual_8","cat_code_total":"0","cat_code":"6","mode":"rate"},
    "no_qualifications": {"label":"No Qualifications","category":"Education","description":"% with no formal qualifications","unit":"%","color_scheme":"Reds","nomis_id":"NM_2084_1","cat_dim":"c2021_hiqual_8","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "students": {"label":"Students","category":"Education","description":"% who are schoolchildren or full-time students","unit":"%","color_scheme":"BuPu","nomis_id":"NM_2085_1","cat_dim":"c2021_student_3","cat_code_total":"0","cat_code":"1","mode":"rate"},
    # ── Health ──────────────────────────────────
    "health_good": {"label":"Good / Very Good Health","category":"Health","description":"% reporting good or very good health","unit":"%","color_scheme":"Greens","nomis_id":"NM_2055_1","cat_dim":"c2021_health_6","cat_code_total":"0","cat_code":["1","2"],"mode":"rate"},
    "health_bad": {"label":"Bad / Very Bad Health","category":"Health","description":"% reporting bad or very bad health","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2055_1","cat_dim":"c2021_health_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "disability": {"label":"Disabled (Equality Act)","category":"Health","description":"% disabled under the Equality Act","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2056_1","cat_dim":"c2021_disability_5","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "disability_limited_lot": {"label":"Activities Limited a Lot","category":"Health","description":"% with day-to-day activities limited a lot","unit":"%","color_scheme":"Reds","nomis_id":"NM_2056_1","cat_dim":"c2021_disability_5","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "unpaid_care": {"label":"Provides Unpaid Care","category":"Health","description":"% providing any unpaid care","unit":"%","color_scheme":"BuPu","nomis_id":"NM_2057_1","cat_dim":"c2021_carer_7","cat_code_total":"0","cat_code":["101","102","6"],"mode":"rate"},
    "hh_disabled": {"label":"Household with Disabled Person","category":"Health","description":"% households with 1+ disabled person","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2058_1","cat_dim":"c2021_hhdisabled_4","cat_code_total":"0","cat_code":["2","3"],"mode":"rate"},
    # ── Economy ─────────────────────────────────
    "economic_activity": {"label":"Economically Active","category":"Economy","description":"% economically active (excl. FT students)","unit":"%","color_scheme":"GnBu","nomis_id":"NM_2083_1","cat_dim":"c2021_eastat_20","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "unemployment": {"label":"Unemployment","category":"Economy","description":"% unemployed of economically active","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2083_1","cat_dim":"c2021_eastat_20","cat_code_total":"1001","cat_code":"7","mode":"rate"},
    "nssec_higher_managerial": {"label":"Higher Managerial / Professional","category":"Economy","description":"% in higher managerial & professional occupations","unit":"%","color_scheme":"Blues","nomis_id":"NM_2079_1","cat_dim":"c2021_nssec_10","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "nssec_routine": {"label":"Routine Occupations","category":"Economy","description":"% in routine occupations","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2079_1","cat_dim":"c2021_nssec_10","cat_code_total":"0","cat_code":"7","mode":"rate"},
    "nssec_never_worked": {"label":"Never Worked / Long-term Unemployed","category":"Economy","description":"% never worked or long-term unemployed","unit":"%","color_scheme":"Reds","nomis_id":"NM_2079_1","cat_dim":"c2021_nssec_10","cat_code_total":"0","cat_code":"8","mode":"rate"},
    "occ_professional": {"label":"Professional Occupations","category":"Economy","description":"% in professional occupations (SOC 2)","unit":"%","color_scheme":"Blues","nomis_id":"NM_2080_1","cat_dim":"c2021_occ_10","cat_code_total":"0","cat_code":"2","mode":"rate"},
    "occ_elementary": {"label":"Elementary Occupations","category":"Economy","description":"% in elementary occupations (SOC 9)","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2080_1","cat_dim":"c2021_occ_10","cat_code_total":"0","cat_code":"9","mode":"rate"},
    "hours_fulltime": {"label":"Full-time Workers","category":"Economy","description":"% working full-time (31+ hours)","unit":"%","color_scheme":"GnBu","nomis_id":"NM_2076_1","cat_dim":"c2021_hours_5","cat_code_total":"0","cat_code":"1002","mode":"rate"},
    "employment_history_never": {"label":"Never Worked","category":"Economy","description":"% of non-employed who have never worked","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2082_1","cat_dim":"c2021_worked_4","cat_code_total":"0","cat_code":"3","mode":"rate"},
    # ── Housing ─────────────────────────────────
    "home_ownership": {"label":"Home Ownership","category":"Housing","description":"% owner-occupied (outright + mortgage)","unit":"%","color_scheme":"Blues","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "social_rented": {"label":"Social Rented","category":"Housing","description":"% social rented households","unit":"%","color_scheme":"Purples","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9","cat_code_total":"0","cat_code":"1003","mode":"rate"},
    "private_rented": {"label":"Private Rented","category":"Housing","description":"% private rented households","unit":"%","color_scheme":"BuPu","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9","cat_code_total":"0","cat_code":"1004","mode":"rate"},
    "accommodation_detached": {"label":"Detached Houses","category":"Housing","description":"% living in detached houses","unit":"%","color_scheme":"YlGn","nomis_id":"NM_2062_1","cat_dim":"c2021_acctype_9","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "accommodation_flat": {"label":"Flats","category":"Housing","description":"% living in purpose-built flats","unit":"%","color_scheme":"GnBu","nomis_id":"NM_2062_1","cat_dim":"c2021_acctype_9","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "accommodation_terraced": {"label":"Terraced Houses","category":"Housing","description":"% living in terraced houses","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2062_1","cat_dim":"c2021_acctype_9","cat_code_total":"0","cat_code":"3","mode":"rate"},
    "overcrowded": {"label":"Overcrowded (Bedrooms)","category":"Housing","description":"% with occupancy rating -1 or worse","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2070_1","cat_dim":"c2021_occrat_bedrooms_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "overcrowded_rooms": {"label":"Overcrowded (Rooms)","category":"Housing","description":"% with room occupancy rating -1 or worse","unit":"%","color_scheme":"Reds","nomis_id":"NM_2071_1","cat_dim":"c2021_occrat_rooms_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "bedrooms_1": {"label":"1-Bedroom Households","category":"Housing","description":"% households with only 1 bedroom","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2068_1","cat_dim":"c2021_bedrooms_5","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "bedrooms_4plus": {"label":"4+ Bedroom Households","category":"Housing","description":"% households with 4 or more bedrooms","unit":"%","color_scheme":"BuGn","nomis_id":"NM_2068_1","cat_dim":"c2021_bedrooms_5","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "no_central_heating": {"label":"No Central Heating","category":"Housing","description":"% households with no central heating","unit":"%","color_scheme":"Greys","nomis_id":"NM_2064_1","cat_dim":"c2021_heating_13","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "gas_heating": {"label":"Mains Gas Heating","category":"Housing","description":"% households with mains gas only","unit":"%","color_scheme":"YlOrRd","nomis_id":"NM_2064_1","cat_dim":"c2021_heating_13","cat_code_total":"0","cat_code":"2","mode":"rate"},
    "electric_heating": {"label":"Electric Heating Only","category":"Housing","description":"% households with electric heating only","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2064_1","cat_dim":"c2021_heating_13","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "hh_one_person": {"label":"One-Person Households","category":"Households","description":"% one-person households","unit":"%","color_scheme":"Greys","nomis_id":"NM_2023_1","cat_dim":"c2021_hhcomp_15","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "hh_lone_parent": {"label":"Lone Parent Households","category":"Households","description":"% lone parent family households","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2023_1","cat_dim":"c2021_hhcomp_15","cat_code_total":"0","cat_code":"1005","mode":"rate"},
    "hh_married_couple": {"label":"Married Couple Households","category":"Households","description":"% married or civil partnership couple households","unit":"%","color_scheme":"Blues","nomis_id":"NM_2023_1","cat_dim":"c2021_hhcomp_15","cat_code_total":"0","cat_code":"1003","mode":"rate"},
    "hh_cohabiting": {"label":"Cohabiting Couple Households","category":"Households","description":"% cohabiting couple family households","unit":"%","color_scheme":"BuGn","nomis_id":"NM_2023_1","cat_dim":"c2021_hhcomp_15","cat_code_total":"0","cat_code":"1004","mode":"rate"},
    "hh_size_1": {"label":"1-Person Households (Size)","category":"Households","description":"% households with 1 person","unit":"%","color_scheme":"Greys","nomis_id":"NM_2037_1","cat_dim":"c2021_hhsize_10","cat_code_total":"0","cat_code":"2","mode":"rate"},
    "hh_size_5plus": {"label":"5+ Person Households","category":"Households","description":"% households with 5 or more people","unit":"%","color_scheme":"YlOrRd","nomis_id":"NM_2037_1","cat_dim":"c2021_hhsize_10","cat_code_total":"0","cat_code":["6","7","8","9"],"mode":"rate"},
    # ── Deprivation ─────────────────────────────
    "deprivation_none": {"label":"Not Deprived","category":"Deprivation","description":"% households not deprived in any dimension","unit":"%","color_scheme":"Greens","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "deprivation_1": {"label":"Deprived 1 Dimension","category":"Deprivation","description":"% households deprived in 1 dimension","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6","cat_code_total":"0","cat_code":"2","mode":"rate"},
    "deprivation_2": {"label":"Deprived 2 Dimensions","category":"Deprivation","description":"% households deprived in 2 dimensions","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6","cat_code_total":"0","cat_code":"3","mode":"rate"},
    "deprivation_3plus": {"label":"Deprived 3+ Dimensions","category":"Deprivation","description":"% households deprived in 3 or 4 dimensions","unit":"%","color_scheme":"Reds","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    # ── Transport ───────────────────────────────
    "car_none": {"label":"No Car Households","category":"Transport","description":"% households with no car or van","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2063_1","cat_dim":"c2021_cars_5","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "travel_car": {"label":"Drive to Work","category":"Transport","description":"% commuting by car or van","unit":"%","color_scheme":"Greys","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12","cat_code_total":"0","cat_code":"7","mode":"rate"},
    "travel_public": {"label":"Public Transport to Work","category":"Transport","description":"% commuting by bus, train, tube or tram","unit":"%","color_scheme":"BuGn","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12","cat_code_total":"0","cat_code":["2","3","4"],"mode":"rate"},
    "work_from_home": {"label":"Work from Home","category":"Transport","description":"% working mainly at or from home","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "travel_walk_cycle": {"label":"Walk or Cycle to Work","category":"Transport","description":"% walking or cycling to work","unit":"%","color_scheme":"Greens","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12","cat_code_total":"0","cat_code":["9","10"],"mode":"rate"},
    "commute_under5km": {"label":"Commute < 5km","category":"Transport","description":"% commuting less than 5km","unit":"%","color_scheme":"YlGn","nomis_id":"NM_2075_1","cat_dim":"c2021_ttwdist_11","cat_code_total":"0","cat_code":["1","2"],"mode":"rate"},
    "commute_20km_plus": {"label":"Commute 20km+","category":"Transport","description":"% commuting 20km or more","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2075_1","cat_dim":"c2021_ttwdist_11","cat_code_total":"0","cat_code":["5","6","7","8"],"mode":"rate"},
    # ── Ethnicity & Identity ────────────────────
    "white_british": {"label":"White British","category":"Ethnicity & Identity","description":"% White: English/Welsh/Scottish/NI/British","unit":"%","color_scheme":"Greys","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "ethnic_asian": {"label":"Asian","category":"Ethnicity & Identity","description":"% Asian, Asian British or Asian Welsh","unit":"%","color_scheme":"YlGnBu","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "ethnic_black": {"label":"Black","category":"Ethnicity & Identity","description":"% Black, Black British, Black Welsh, Caribbean or African","unit":"%","color_scheme":"BuPu","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1002","mode":"rate"},
    "ethnic_mixed": {"label":"Mixed / Multiple","category":"Ethnicity & Identity","description":"% Mixed or Multiple ethnic groups","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1003","mode":"rate"},
    "ethnic_other": {"label":"Other Ethnic Group","category":"Ethnicity & Identity","description":"% Other ethnic group","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20","cat_code_total":"0","cat_code":"1005","mode":"rate"},
    "born_uk": {"label":"Born in UK","category":"Ethnicity & Identity","description":"% born in the United Kingdom","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2024_1","cat_dim":"c2021_cob_12","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "identity_british_only": {"label":"British Only Identity","category":"Ethnicity & Identity","description":"% identifying as British only","unit":"%","color_scheme":"Blues","nomis_id":"NM_2046_1","cat_dim":"c2021_natiduk_17","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "identity_non_uk": {"label":"Non-UK Identity Only","category":"Ethnicity & Identity","description":"% with non-UK identity only","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2046_1","cat_dim":"c2021_natiduk_17","cat_code_total":"0","cat_code":"9996","mode":"rate"},
    "passport_uk": {"label":"UK Passport","category":"Ethnicity & Identity","description":"% holding a UK passport","unit":"%","color_scheme":"Blues","nomis_id":"NM_2025_1","cat_dim":"c2021_pass_27","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "passport_eu": {"label":"EU Passport (excl. UK/Ireland)","category":"Ethnicity & Identity","description":"% holding an EU member country passport","unit":"%","color_scheme":"YlGnBu","nomis_id":"NM_2025_1","cat_dim":"c2021_pass_27","cat_code_total":"0","cat_code":"1003","mode":"rate"},
    "multi_ethnic_hh": {"label":"Multi-Ethnic Households","category":"Ethnicity & Identity","description":"% households with different ethnic groups within partnerships","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2042_1","cat_dim":"c2021_meth_6","cat_code_total":"0","cat_code":"4","mode":"rate"},
    # ── Language ────────────────────────────────
    "no_english": {"label":"No English in Household","category":"Language","description":"% households where no one has English as main language","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2044_1","cat_dim":"c2021_hhlang_5","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "english_not_main": {"label":"English Not Main Language","category":"Language","description":"% whose main language is not English","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2048_1","cat_dim":"c2021_engprf_6","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "cannot_speak_english": {"label":"Cannot Speak English Well/At All","category":"Language","description":"% who cannot speak English well or at all","unit":"%","color_scheme":"Reds","nomis_id":"NM_2048_1","cat_dim":"c2021_engprf_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    "multi_lang_hh": {"label":"Multiple Main Languages in HH","category":"Language","description":"% households where main language differs within partnerships","unit":"%","color_scheme":"BuPu","nomis_id":"NM_2045_1","cat_dim":"c2021_mhhlang_5","cat_code_total":"0","cat_code":"4","mode":"rate"},
    # ── Religion ────────────────────────────────
    "christian": {"label":"Christian","category":"Religion","description":"% identifying as Christian","unit":"%","color_scheme":"RdPu","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"2","mode":"rate"},
    "no_religion": {"label":"No Religion","category":"Religion","description":"% with no religion","unit":"%","color_scheme":"YlGnBu","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "muslim": {"label":"Muslim","category":"Religion","description":"% identifying as Muslim","unit":"%","color_scheme":"BuGn","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"6","mode":"rate"},
    "hindu": {"label":"Hindu","category":"Religion","description":"% identifying as Hindu","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "jewish": {"label":"Jewish","category":"Religion","description":"% identifying as Jewish","unit":"%","color_scheme":"Blues","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"5","mode":"rate"},
    "sikh": {"label":"Sikh","category":"Religion","description":"% identifying as Sikh","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10","cat_code_total":"0","cat_code":"7","mode":"rate"},
    "multi_religion_hh": {"label":"Multiple Religions in Household","category":"Religion","description":"% households with 2+ different religions","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2097_1","cat_dim":"c2021_relmult_7","cat_code_total":"0","cat_code":"6","mode":"rate"},
    # ── Migration ───────────────────────────────
    "migrant_within_uk": {"label":"Migrant Within UK (1 Year)","category":"Migration","description":"% who moved address within UK in last year","unit":"%","color_scheme":"GnBu","nomis_id":"NM_2039_1","cat_dim":"c2021_migind_4","cat_code_total":"0","cat_code":"3","mode":"rate"},
    "migrant_from_abroad": {"label":"Migrant from Abroad (1 Year)","category":"Migration","description":"% who moved from outside UK in last year","unit":"%","color_scheme":"OrRd","nomis_id":"NM_2039_1","cat_dim":"c2021_migind_4","cat_code_total":"0","cat_code":"4","mode":"rate"},
    "arrived_2011_plus": {"label":"Arrived UK 2011+","category":"Migration","description":"% who arrived in UK from 2011 onwards","unit":"%","color_scheme":"YlOrBr","nomis_id":"NM_2035_1","cat_dim":"c2021_arruk_13","cat_code_total":"0","cat_code":["9","10","11","12"],"mode":"rate"},
    "resident_under_5yr": {"label":"UK Resident < 5 Years","category":"Migration","description":"% non-UK born with less than 5 years residence","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2036_1","cat_dim":"c2021_resuk_6","cat_code_total":"0","cat_code":["4","5"],"mode":"rate"},
    # ── Partnership ─────────────────────────────
    "married": {"label":"Married / Civil Partnership","category":"Partnership","description":"% married or in a registered civil partnership","unit":"%","color_scheme":"Blues","nomis_id":"NM_2022_1","cat_dim":"c2021_lpstat_12","cat_code_total":"0","cat_code":"1001","mode":"rate"},
    "never_married": {"label":"Never Married","category":"Partnership","description":"% never married and never in civil partnership","unit":"%","color_scheme":"PuBu","nomis_id":"NM_2022_1","cat_dim":"c2021_lpstat_12","cat_code_total":"0","cat_code":"1","mode":"rate"},
    "divorced": {"label":"Divorced / Dissolved","category":"Partnership","description":"% divorced or civil partnership dissolved","unit":"%","color_scheme":"PuRd","nomis_id":"NM_2022_1","cat_dim":"c2021_lpstat_12","cat_code_total":"0","cat_code":"1005","mode":"rate"},
    "widowed": {"label":"Widowed","category":"Partnership","description":"% widowed or surviving civil partnership partner","unit":"%","color_scheme":"Greys","nomis_id":"NM_2022_1","cat_dim":"c2021_lpstat_12","cat_code_total":"0","cat_code":"1006","mode":"rate"},
}

DETAIL_DATASETS = [
    {"label":"Population","nomis_id":"NM_2021_1","cat_dim":"c2021_restype_3"},
    {"label":"Age Structure","nomis_id":"NM_2020_1","cat_dim":"c2021_age_19"},
    {"label":"Ethnic Group","nomis_id":"NM_2041_1","cat_dim":"c2021_eth_20"},
    {"label":"Country of Birth","nomis_id":"NM_2024_1","cat_dim":"c2021_cob_12"},
    {"label":"National Identity","nomis_id":"NM_2046_1","cat_dim":"c2021_natiduk_17"},
    {"label":"Passports Held","nomis_id":"NM_2025_1","cat_dim":"c2021_pass_27"},
    {"label":"Religion","nomis_id":"NM_2049_1","cat_dim":"c2021_religion_10"},
    {"label":"Legal Partnership","nomis_id":"NM_2022_1","cat_dim":"c2021_lpstat_12"},
    {"label":"Health","nomis_id":"NM_2055_1","cat_dim":"c2021_health_6"},
    {"label":"Disability","nomis_id":"NM_2056_1","cat_dim":"c2021_disability_5"},
    {"label":"Unpaid Care","nomis_id":"NM_2057_1","cat_dim":"c2021_carer_7"},
    {"label":"Qualifications","nomis_id":"NM_2084_1","cat_dim":"c2021_hiqual_8"},
    {"label":"Economic Activity","nomis_id":"NM_2083_1","cat_dim":"c2021_eastat_20"},
    {"label":"NS-SeC","nomis_id":"NM_2079_1","cat_dim":"c2021_nssec_10"},
    {"label":"Occupation","nomis_id":"NM_2080_1","cat_dim":"c2021_occ_10"},
    {"label":"Hours Worked","nomis_id":"NM_2076_1","cat_dim":"c2021_hours_5"},
    {"label":"Tenure","nomis_id":"NM_2072_1","cat_dim":"c2021_tenure_9"},
    {"label":"Accommodation Type","nomis_id":"NM_2062_1","cat_dim":"c2021_acctype_9"},
    {"label":"Bedrooms","nomis_id":"NM_2068_1","cat_dim":"c2021_bedrooms_5"},
    {"label":"Occupancy (Bedrooms)","nomis_id":"NM_2070_1","cat_dim":"c2021_occrat_bedrooms_6"},
    {"label":"Central Heating","nomis_id":"NM_2064_1","cat_dim":"c2021_heating_13"},
    {"label":"Household Composition","nomis_id":"NM_2023_1","cat_dim":"c2021_hhcomp_15"},
    {"label":"Household Size","nomis_id":"NM_2037_1","cat_dim":"c2021_hhsize_10"},
    {"label":"Deprivation","nomis_id":"NM_2031_1","cat_dim":"c2021_dep_6"},
    {"label":"Car/Van Availability","nomis_id":"NM_2063_1","cat_dim":"c2021_cars_5"},
    {"label":"Travel to Work","nomis_id":"NM_2078_1","cat_dim":"c2021_ttwmeth_12"},
    {"label":"Distance to Work","nomis_id":"NM_2075_1","cat_dim":"c2021_ttwdist_11"},
    {"label":"English Proficiency","nomis_id":"NM_2048_1","cat_dim":"c2021_engprf_6"},
    {"label":"Household Language","nomis_id":"NM_2044_1","cat_dim":"c2021_hhlang_5"},
    {"label":"Migration (1yr)","nomis_id":"NM_2039_1","cat_dim":"c2021_migind_4"},
    {"label":"Year of Arrival","nomis_id":"NM_2035_1","cat_dim":"c2021_arruk_13"},
]

# ═══════ Startup: pre-fetch boundaries + build adjacency graph ═══════

@app.on_event("startup")
async def startup_prefetch():
    bsc_file = DATA_DIR / "boundaries_national_bsc.geojson"
    if not bsc_file.exists():
        logger.info("Pre-fetching national BSC boundaries (~19 MB, ~20s)...")
        try:
            geojson = await _fetch_all_boundaries(BSC_URL, "BSC")
            async with aiofiles.open(bsc_file, "w") as f:
                await f.write(json.dumps(geojson))
            logger.info(f"BSC cached: {len(geojson['features'])} features, {bsc_file.stat().st_size/1024/1024:.1f} MB")
        except Exception as e:
            logger.error(f"BSC pre-fetch failed: {e}")
            return
    else:
        logger.info(f"National BSC boundaries cached ({bsc_file.stat().st_size/1024/1024:.1f} MB)")
    # Build adjacency graph and geometry index
    await _build_adjacency_graph(bsc_file)

    # ── Scotland DZ22 boundaries ──
    try:
        dz22_file = await download_dz22_boundaries(DATA_DIR)
        await _build_scotland_geometry_index(dz22_file)
        await _build_scotland_adjacency(dz22_file)
    except Exception as e:
        logger.error(f"Scotland boundary setup failed: {e}")

    # ── Scotland OA→DZ lookup and census data ──
    try:
        global scotland_oa_to_dz, scotland_dz_names
        scotland_oa_to_dz, scotland_dz_names = await download_oa_dz_lookup(DATA_DIR)
        # Download census CSVs (non-blocking for startup speed — data processed on demand)
        asyncio.create_task(_prefetch_scotland_census_data())
    except Exception as e:
        logger.error(f"Scotland lookup/census setup failed: {e}")


async def _prefetch_scotland_census_data():
    """Background task to download and process Scotland census data."""
    try:
        # Compute population density/total from boundary shapefile attributes
        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
        if dz22_file.exists():
            pop_data = compute_scotland_population_data(dz22_file, scotland_dz_names)
            for ds_id, data in pop_data.items():
                cache_file = DATA_DIR / f"data_{ds_id}_national_sc.json"
                cache_file.write_text(json.dumps(data))
                scotland_data_cache[ds_id] = data
                logger.info(f"  Scotland {ds_id}: {len(data['values'])} DZs (from boundary data)")

        await download_scotland_census_csvs(DATA_DIR)
        results = await process_all_scotland_indicators(DATA_DIR, scotland_oa_to_dz, scotland_dz_names)
        scotland_data_cache.update(results)
        logger.info(f"Scotland census data processed: {len(scotland_data_cache)} indicators total")
    except Exception as e:
        logger.error(f"Scotland census data processing failed: {e}")

async def _build_adjacency_graph(bsc_file: Path):
    """Build LSOA adjacency graph from cached BSC boundaries using Shapely."""
    adj_file = DATA_DIR / "adjacency_graph.json"
    if adj_file.exists():
        async with aiofiles.open(adj_file) as f:
            adjacency_graph.update(json.loads(await f.read()))
        logger.info(f"Adjacency graph loaded from cache: {len(adjacency_graph)} LSOAs")
        # Still need to build geometry index for dissolve operations
        await _build_geometry_index(bsc_file)
        return

    logger.info("Building adjacency graph from BSC boundaries (one-time, ~30s)...")
    async with aiofiles.open(bsc_file) as f:
        geojson = json.loads(await f.read())

    features = geojson.get("features", [])
    # Build Shapely geometries with small buffer for topology healing
    geoms = {}
    for feat in features:
        code = feat["properties"].get("LSOA21CD", "")
        if not code:
            continue
        try:
            geom = shape(feat["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)  # fix self-intersections
            geoms[code] = geom
            lsoa_geometries[code] = geom
        except Exception:
            continue

    logger.info(f"  Parsed {len(geoms)} geometries, computing adjacency...")

    # Build STRtree spatial index for efficient neighbour detection
    from shapely import STRtree
    codes = list(geoms.keys())
    polys = [geoms[c] for c in codes]
    tree = STRtree(polys)

    adj = {c: [] for c in codes}
    for i, code in enumerate(codes):
        geom = polys[i]
        # Find candidate neighbours via bounding box overlap
        candidate_indices = tree.query(geom)
        for j in candidate_indices:
            if j == i:
                continue
            other_code = codes[j]
            other_geom = polys[j]
            # Two LSOAs are adjacent if they share more than a single point
            try:
                intersection = geom.intersection(other_geom)
                if intersection.is_empty:
                    continue
                # Shared edge = LineString or MultiLineString, not just Point
                if intersection.geom_type in ("LineString", "MultiLineString",
                                               "GeometryCollection", "Polygon",
                                               "MultiPolygon"):
                    adj[code].append(other_code)
            except Exception:
                continue

    adjacency_graph.update(adj)

    # Persist to disk
    async with aiofiles.open(adj_file, "w") as f:
        await f.write(json.dumps(adj))

    total_edges = sum(len(v) for v in adj.values()) // 2
    logger.info(f"  Adjacency graph built: {len(adj)} LSOAs, {total_edges} edges")

async def _build_geometry_index(bsc_file: Path):
    """Load BSC geometries into memory for dissolve operations."""
    if lsoa_geometries:
        return  # already loaded
    async with aiofiles.open(bsc_file) as f:
        geojson = json.loads(await f.read())
    for feat in geojson.get("features", []):
        code = feat["properties"].get("LSOA21CD", "")
        if code:
            try:
                geom = shape(feat["geometry"])
                if not geom.is_valid:
                    geom = geom.buffer(0)
                lsoa_geometries[code] = geom
            except Exception:
                continue
    logger.info(f"Geometry index loaded: {len(lsoa_geometries)} LSOAs")

async def _build_scotland_geometry_index(dz22_file: Path):
    """Load Scotland DZ22 geometries into the shared geometry index."""
    async with aiofiles.open(dz22_file) as f:
        geojson = json.loads(await f.read())
    count = 0
    for feat in geojson.get("features", []):
        code = feat["properties"].get("DZ22CD", "")
        if code:
            try:
                geom = shape(feat["geometry"])
                if not geom.is_valid:
                    geom = geom.buffer(0)
                lsoa_geometries[code] = geom
                count += 1
            except Exception:
                continue
    logger.info(f"Scotland geometry index loaded: {count} DZs (total geometries: {len(lsoa_geometries)})")

async def _build_scotland_adjacency(dz22_file: Path):
    """Build adjacency graph for Scotland DZ22 features and merge into main graph."""
    adj_file = DATA_DIR / "adjacency_graph_scotland.json"
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
            other_code = codes[j]
            try:
                intersection = geom.intersection(polys[j])
                if not intersection.is_empty and intersection.geom_type in (
                    "LineString", "MultiLineString", "GeometryCollection", "Polygon", "MultiPolygon"
                ):
                    adj[code].append(other_code)
            except Exception:
                continue

    adjacency_graph.update(adj)
    async with aiofiles.open(adj_file, "w") as f:
        await f.write(json.dumps(adj))
    total_edges = sum(len(v) for v in adj.values()) // 2
    logger.info(f"Scotland adjacency graph built: {len(adj)} DZs, {total_edges} edges")

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

    # Scottish council area: return Scotland-only data
    if lad_code and lad_code.startswith("S12"):
        sc_data = _get_scotland_data(dataset_id)
        if sc_data:
            return sc_data
        return {"dataset_id": dataset_id, "values": {}, "names": {}, "stats": {}, "source": "No Scotland data"}

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
            ds = CENSUS_DATASETS[dataset_id]
            ew_result = await fetch_nomis_data(dataset_id, ds, lad_code)
            async with aiofiles.open(cache_file, "w") as f:
                await f.write(json.dumps(ew_result))
        data_cache[cache_key] = ew_result

    # For national view, merge Scotland data if available
    if not lad_code:
        sc_data = _get_scotland_data(dataset_id)
        if sc_data and sc_data.get("values"):
            merged = {**ew_result}
            merged["values"] = {**ew_result.get("values", {}), **sc_data["values"]}
            merged["names"] = {**ew_result.get("names", {}), **sc_data["names"]}
            # Recompute stats across merged dataset
            all_vals = list(merged["values"].values())
            merged["stats"] = compute_stats(all_vals) if all_vals else {}
            merged["source"] = "ONS Census 2021 (E&W) + Scotland's Census 2022"
            return merged

    return ew_result


def _get_scotland_data(dataset_id: str) -> Optional[dict]:
    """Get Scotland data for a dataset, from cache or disk."""
    if dataset_id in scotland_data_cache:
        return scotland_data_cache[dataset_id]
    cache_file = DATA_DIR / f"data_{dataset_id}_national_sc.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        scotland_data_cache[dataset_id] = data
        return data
    # Try computing population data from boundary attributes
    if dataset_id in ("population_density", "population_total"):
        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
        if dz22_file.exists():
            pop_data = compute_scotland_population_data(dz22_file, scotland_dz_names)
            if dataset_id in pop_data:
                result = pop_data[dataset_id]
                cache_file.write_text(json.dumps(result))
                scotland_data_cache[dataset_id] = result
                return result
    # Try processing from CSV on demand
    if dataset_id in SCOTLAND_INDICATOR_MAP and scotland_oa_to_dz:
        csv_dir = DATA_DIR / "scotland_oa_csvs"
        if csv_dir.exists():
            result = process_scotland_indicator(dataset_id, csv_dir, scotland_oa_to_dz, scotland_dz_names)
            if result:
                cache_file.write_text(json.dumps(result))
                scotland_data_cache[dataset_id] = result
                return result
    return None

@app.get("/api/boundaries/lsoa")
async def get_lsoa_boundaries(lad_code: Optional[str] = Query(None), resolution: str = Query("bsc")):
    if not lad_code:
        # National view: merge E&W BSC + Scotland DZ22
        ck = "boundaries:national:merged"
        if ck in data_cache:
            return data_cache[ck]

        bsc_file = DATA_DIR / "boundaries_national_bsc.geojson"
        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"

        if not bsc_file.exists():
            raise HTTPException(status_code=503, detail="National boundaries not yet loaded")

        async with aiofiles.open(bsc_file) as f:
            ew_geojson = json.loads(await f.read())

        # Add nation property to E&W features
        for feat in ew_geojson.get("features", []):
            feat["properties"]["nation"] = "EW"

        # Merge Scotland DZ22 features if available
        if dz22_file.exists():
            async with aiofiles.open(dz22_file) as f:
                sc_geojson = json.loads(await f.read())
            ew_geojson["features"].extend(sc_geojson.get("features", []))
            logger.info(f"Merged boundaries: {len(ew_geojson['features'])} total features")

        data_cache[ck] = ew_geojson
        return ew_geojson

    # Scottish council area filter
    if lad_code.startswith("S12"):
        ck = f"boundaries:{lad_code}:dz22"
        if ck in data_cache:
            return data_cache[ck]
        geo_file = DATA_DIR / f"boundaries_{lad_code}_dz22.geojson"
        if geo_file.exists():
            async with aiofiles.open(geo_file) as f:
                geojson = json.loads(await f.read())
            data_cache[ck] = geojson
            return geojson
        # Filter DZ22 features for this council area
        # For now, return all Scotland DZs (council area filtering requires spatial lookup)
        dz22_file = DATA_DIR / "boundaries_scotland_dz22.geojson"
        if dz22_file.exists():
            async with aiofiles.open(dz22_file) as f:
                geojson = json.loads(await f.read())
            data_cache[ck] = geojson
            return geojson
        raise HTTPException(status_code=503, detail="Scotland boundaries not yet loaded")

    # E&W LAD filter
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

    # Scotland DZ codes start with S01
    if lsoa_code.startswith("S01"):
        result = _build_scotland_detail(lsoa_code)
        async with aiofiles.open(cf, "w") as f:
            await f.write(json.dumps(result))
        data_cache[ck] = result
        return result

    result = await fetch_lsoa_detail(lsoa_code)
    async with aiofiles.open(cf, "w") as f:
        await f.write(json.dumps(result))
    data_cache[ck] = result
    return result


def _build_scotland_detail(dz_code: str) -> dict:
    """Build a detail panel response for a Scotland Data Zone from cached indicator data."""
    name = scotland_dz_names.get(dz_code, dz_code)
    categories = {}

    # Group Scotland indicators by their E&W category
    for ds_id, mapping_info in SCOTLAND_INDICATOR_MAP.items():
        ds = CENSUS_DATASETS.get(ds_id)
        if not ds:
            continue
        sc_data = _get_scotland_data(ds_id)
        if not sc_data or dz_code not in sc_data.get("values", {}):
            continue
        cat_name = ds["category"]
        if cat_name not in categories:
            categories[cat_name] = {}
        categories[cat_name][ds["label"]] = sc_data["values"][dz_code]

    return {
        "lsoa_code": dz_code,
        "name": name,
        "source": "Scotland's Census 2022 — NRS",
        "categories": categories,
    }

@app.get("/api/lad/list")
async def get_lad_list():
    ck = "lad_list"
    if ck in data_cache:
        return data_cache[ck]
    cf = DATA_DIR / "lad_list.json"
    # Clear old cache to pick up Scotland additions
    if cf.exists():
        cf.unlink()
    result = await fetch_lad_list()
    # Add Scottish council areas
    result["lads"].extend(SCOTTISH_COUNCIL_AREAS)
    result["lads"].sort(key=lambda x: x["name"])
    async with aiofiles.open(cf, "w") as f:
        await f.write(json.dumps(result))
    data_cache[ck] = result
    return result

@app.get("/api/health")
async def health():
    bsc = DATA_DIR / "boundaries_national_bsc.geojson"
    dz22 = DATA_DIR / "boundaries_scotland_dz22.geojson"
    return {
        "status": "ok",
        "cached_items": len(data_cache),
        "boundaries_ready": bsc.exists(),
        "scotland_boundaries_ready": dz22.exists(),
        "scotland_data_indicators": len(scotland_data_cache),
    }

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

# ═══════ Selection / Dissolve / Aggregate ═══════

@app.get("/api/adjacency")
async def get_adjacency():
    """Return the full LSOA adjacency graph for client-side contiguity checking."""
    if not adjacency_graph:
        raise HTTPException(503, "Adjacency graph not yet built")
    return {"count": len(adjacency_graph), "graph": adjacency_graph}

@app.post("/api/selection/dissolve")
async def dissolve_selection(req: SelectionRequest):
    """
    Compute the dissolved (unioned) boundary of selected LSOAs using Shapely.
    Returns merged GeoJSON geometry, area, perimeter, centroid, and contiguity info.
    """
    if not lsoa_geometries:
        raise HTTPException(503, "Geometry index not yet loaded")
    if not req.lsoa_codes:
        raise HTTPException(400, "No LSOA codes provided")
    if len(req.lsoa_codes) > 500:
        raise HTTPException(400, "Maximum 500 LSOAs per selection")

    # Collect geometries
    geoms = []
    missing = []
    for code in req.lsoa_codes:
        if code in lsoa_geometries:
            geoms.append(lsoa_geometries[code])
        else:
            missing.append(code)

    if not geoms:
        raise HTTPException(404, "No geometries found for given LSOA codes")

    # Dissolve with topology healing
    try:
        # Small buffer to heal micro-gaps between adjacent LSOAs, then debuffer
        buffered = [shp_buffer(g, 0.00001) for g in geoms]
        merged = unary_union(buffered)
        merged = shp_buffer(merged, -0.00001)
        if not merged.is_valid:
            merged = merged.buffer(0)
    except Exception as e:
        logger.error(f"Dissolve error: {e}")
        # Fallback: try without buffer trick
        merged = unary_union(geoms)

    # Compute properties
    centroid = merged.centroid
    # Area in approximate sq km (rough WGS84 conversion at UK latitude ~52°)
    # 1 degree lat ≈ 111km, 1 degree lon ≈ 111km * cos(52°) ≈ 68km
    bounds = merged.bounds  # (minx, miny, maxx, maxy)
    area_approx_km2 = merged.area * 111 * 68  # very rough
    perimeter_approx_km = merged.length * 90   # rough

    # Contiguity analysis: how many disconnected components?
    if merged.geom_type == "MultiPolygon":
        n_components = len(list(merged.geoms))
    else:
        n_components = 1

    # Check which selected LSOAs are neighbours of the selection boundary
    # (useful for "expand selection" feature)
    border_neighbours = set()
    if adjacency_graph:
        selection_set = set(req.lsoa_codes)
        for code in req.lsoa_codes:
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
            "bounds": [round(b, 6) for b in bounds],
            "border_neighbours": sorted(border_neighbours)[:200],
        },
    }

@app.post("/api/selection/aggregate")
async def aggregate_selection(req: AggregateRequest):
    """
    Compute aggregated census statistics for a set of selected LSOAs.
    For rate datasets: re-derives from raw numerator + denominator (not averaged percentages).
    """
    if not req.lsoa_codes:
        raise HTTPException(400, "No LSOA codes provided")

    dataset_ids = req.dataset_ids or list(CENSUS_DATASETS.keys())
    selection_set = set(req.lsoa_codes)

    results = {}
    for ds_id in dataset_ids:
        if ds_id not in CENSUS_DATASETS:
            continue
        ds = CENSUS_DATASETS[ds_id]

        # Load the national (or most recently cached) data for this dataset
        scope = "national"
        cache_key = f"data:{ds_id}:{scope}"
        data = data_cache.get(cache_key)
        if not data:
            cache_file = DATA_DIR / f"data_{ds_id}_{scope}.json"
            if cache_file.exists():
                async with aiofiles.open(cache_file) as f:
                    data = json.loads(await f.read())

        if not data or not data.get("values"):
            results[ds_id] = {"label": ds["label"], "unit": ds["unit"],
                              "value": None, "note": "Data not yet loaded"}
            continue

        values = data["values"]
        # Collect values for selected LSOAs
        selected_values = [values[c] for c in selection_set if c in values]

        if not selected_values:
            results[ds_id] = {"label": ds["label"], "unit": ds["unit"],
                              "value": None, "note": "No data for selection"}
            continue

        mode = ds["mode"]
        if mode in ("density", "value"):
            # Simple mean for density, sum for value
            if mode == "value":
                agg_value = sum(selected_values)
            else:
                agg_value = round(sum(selected_values) / len(selected_values), 2)
        elif mode == "rate":
            # For rates, the stored value is already a percentage.
            # Ideally we'd re-derive from raw counts, but that would require
            # fetching the raw Nomis data for every dataset in the selection.
            # Pragmatic approach: population-weighted average using population_total
            # if available, otherwise simple mean.
            pop_data = data_cache.get(f"data:population_total:{scope}")
            if not pop_data:
                pop_file = DATA_DIR / f"data_population_total_{scope}.json"
                if pop_file.exists():
                    async with aiofiles.open(pop_file) as f:
                        pop_data = json.loads(await f.read())

            if pop_data and pop_data.get("values"):
                pop_values = pop_data["values"]
                weighted_sum = 0
                total_pop = 0
                for c in selection_set:
                    if c in values and c in pop_values:
                        pop = pop_values[c]
                        weighted_sum += values[c] * pop
                        total_pop += pop
                agg_value = round(weighted_sum / total_pop, 2) if total_pop > 0 else 0
            else:
                agg_value = round(sum(selected_values) / len(selected_values), 2)
        else:
            agg_value = round(sum(selected_values) / len(selected_values), 2)

        results[ds_id] = {
            "label": ds["label"],
            "unit": ds["unit"],
            "value": agg_value,
            "lsoa_count": len(selected_values),
            "stats": compute_stats(selected_values),
        }

    return {"selection_size": len(selection_set), "datasets": results}

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
