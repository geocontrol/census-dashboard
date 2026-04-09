# Scotland Census 2022 Integration — Design Decisions

## 1. Geography Mapping

LSOA (England & Wales) maps to Data Zone (Scotland). Both are small-area statistical geographies with populations of approximately 500–1,500 people. They are not identical in methodology but are broadly comparable for choropleth visualisation and cross-national comparison.

- England & Wales: 35,672 LSOAs (2021 vintage)
- Scotland: 7,392 Data Zones (2022 vintage, DZ22)

## 2. Census Year Difference

- **England & Wales**: Census 2021 (21 March 2021)
- **Scotland**: Census 2022 (20 March 2022)

Scotland's census was delayed by one year due to COVID-19. Data is comparable in scope but not contemporaneous. Users should be aware of the one-year gap when making cross-border comparisons.

## 3. Boundary Vintage

- **E&W**: 2021 LSOA boundaries (BSC V4) from ONS Open Geography Portal
- **Scotland**: 2022 Data Zone boundaries from Scottish Government (`maps.gov.scot`)

Both are current operational geographies for their respective census outputs.

## 4. Data Source Difference

- **E&W data**: Fetched on-demand from the Nomis API (`nomisweb.co.uk`), supporting fine-grained category queries
- **Scotland data**: Bulk CSV download from `scotlandscensus.gov.uk`, processed locally

Scotland's Census 2022 does not have an equivalent Nomis-style API. The OA-level CSVs are multivariate cross-tabulations that require parsing multi-row headers and aggregating from Output Area to Data Zone level.

## 5. Simplification Approach

DZ22 boundaries are distributed as an ESRI Shapefile in EPSG:27700 (British National Grid). During ingestion:

1. Reprojected from BNG to WGS84 (EPSG:4326) using `pyproj`
2. Simplified by retaining approximately 30 vertices per polygon ring
3. Stored as GeoJSON (~11 MB vs ~60 MB unsimplified)

This maintains visual fidelity at typical dashboard zoom levels (z6–z12).

## 6. Feature Property Convention

All boundary features carry a `nation` property for frontend disambiguation:

| Property | E&W Value | Scotland Value |
|----------|-----------|----------------|
| `nation` | `"EW"` | `"SC"` |
| Area code | `LSOA21CD` | `DZ22CD` |
| Area name | `LSOA21NM` | `DZ22NM` |

Frontend code uses `getFeatureCode()` and `getFeatureName()` helper functions to abstract over these differences.

## 7. Indicator Coverage

Not all 86 E&W indicators have direct Scottish equivalents. The Scottish Census uses different variable classifications in some areas (e.g., ethnic group categories differ, Scottish religion data includes "Church of Scotland" separately).

Current coverage maps ~20 key indicators across Housing, Ethnicity, Health, Religion, and Transport categories. Indicators without Scottish data show "No data" styling for DZ features rather than being hidden.

The `SCOTLAND_INDICATOR_MAP` in `api/scotland.py` defines the mapping between E&W dataset IDs and Scottish CSV tables with column extraction rules.

## 8. Aggregation Method

Scottish OA-level counts are summed to DZ level using the `OA22_DZ22_IZ22.csv` lookup. Rates are recomputed from summed numerators and denominators at DZ level (not averaged from OA-level percentages). This matches the statistical approach used for E&W LSOA data and avoids the ecological fallacy of averaging rates.

## 9. Scottish Council Areas

Scottish Council Areas (codes starting `S12`) are added to the Local Authority dropdown. When a council area is selected, only Scotland DZ boundaries and data are displayed. Cross-border council areas do not exist (the E&W/Scotland border is a national boundary).
