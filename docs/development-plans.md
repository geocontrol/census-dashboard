# Census Dashboard — Development Plans

**Document version:** 1.0  
**Date:** 15 April 2026  
**Codebase version:** 5.0.0  
**Prepared for:** Mark Simpkins

---

## Architectural Principles

All four plans follow the existing modular architecture established during the recent refactor. Each new capability is delivered as a self-contained module with clear integration points into `main.py` (backend orchestration) and the frontend JS module system. No changes to the existing E&W or Scotland data paths are required unless explicitly noted.

The current architecture layers are:

1. **Dataset catalogue** — `api/services/dataset_config.py`
2. **Normalised area data** — `api/services/datasets.py` + nation-specific modules (`scotland.py`)
3. **Geometry & boundaries** — `api/services/geometry.py`
4. **Frontend rendering & interaction** — `frontend/static/js/modules/`

---

## Plan 1: Shapefile Export for Selected Regions

### 1.1 Objective

Add an `.shp` (ESRI Shapefile) export option alongside the existing GeoJSON export when a user exports a dissolved selection boundary.

### 1.2 Context

The current `exportSelection()` function in `selection.js` serialises the dissolve result as GeoJSON and triggers a browser download. Many GIS workflows (QGIS, ArcGIS, MapInfo) expect Shapefile format. Shapefile is actually a bundle of files (`.shp`, `.shx`, `.dbf`, and optionally `.prj`) which must be delivered as a `.zip` archive.

### 1.3 Approach — Server-Side Generation

Shapefile generation cannot be done cleanly in the browser. The backend already has `pyshp` and `shapely` as dependencies, so generating a Shapefile from the dissolve result is straightforward. We add a new endpoint that accepts the same LSOA code list, performs the dissolve (or re-uses the cached result), writes a Shapefile to a temp directory, zips the component files, and returns the archive.

### 1.4 Implementation Steps

**Step 1 — New endpoint in `main.py`**

Add `POST /api/selection/export/shapefile` accepting a `SelectionRequest` body. This endpoint will:

- Call the existing `dissolve_selected_geometries()` from `geometry.py` to get the merged geometry and properties.
- Use `pyshp` (already in `requirements.txt`) to write the `.shp`, `.shx`, `.dbf` files into a `tempfile.TemporaryDirectory`.
- Write a `.prj` file containing the WKT for EPSG:4326 (WGS84) — this is a static string constant.
- Include the dissolve properties as DBF attribute fields: `lsoa_count`, `area_km2`, `perim_km`, `contiguous`, `components`.
- Zip the four files together and return a `StreamingResponse` with `content-type: application/zip`.

**Step 2 — Helper function in `api/services/geometry.py`**

Add a function `export_dissolve_as_shapefile(lsoa_codes: list[str]) -> bytes` that encapsulates the shapefile creation logic. This keeps the endpoint in `main.py` thin and the geometry logic in its proper module. The function should:

- Call `dissolve_selected_geometries()` for the geometry.
- Handle both `Polygon` and `MultiPolygon` output types.
- Write the shapefile components using `shapefile.Writer` from `pyshp`.
- Return the zip archive as bytes.

**Step 3 — Frontend export UI update in `selection.js`**

Modify `showDissolvePanel()` to offer two export buttons instead of one:

```
⬇ Export GeoJSON
⬇ Export Shapefile (.shp)
```

Add an `exportShapefile()` function that POSTs to the new endpoint with the selected LSOA codes, receives a blob, and triggers a download of the `.zip` file. The existing `exportSelection()` / `doExport()` functions remain unchanged.

**Step 4 — Update CSS if needed**

The two export buttons should stack naturally. Minimal CSS change — possibly just adding a `gap` between the buttons or a separator.

### 1.5 Dependencies

No new dependencies. `pyshp` (v2.3.1) is already present. `shapely` geometry objects can be decomposed into coordinate lists for `pyshp` consumption.

### 1.6 Edge Cases & Notes

- **MultiPolygon output:** The dissolve can return a MultiPolygon when the selection is non-contiguous. `pyshp` supports MultiPolygon natively — each component becomes a separate part within a single record.
- **Filename:** Use `census_selection_{count}_areas.zip` to match the GeoJSON naming convention.
- **CRS:** Always WGS84, since all stored geometries are in EPSG:4326. The `.prj` file makes this explicit for GIS software.
- **Large selections:** The 500-area limit already enforced in `dissolve_selected_geometries` applies here too.

### 1.7 Estimated Complexity

Low. This is a well-bounded addition — one new endpoint, one helper function, one frontend function, and a small UI tweak. No changes to existing data flow.

---

## Plan 2: Northern Ireland Census Data

### 2.1 Objective

Add Northern Ireland Census 2021 data to the dashboard, using the same pattern established for Scotland: a dedicated backend module (`api/northern_ireland.py`) handling NI-specific ingestion and transformation, with data normalised into the shared `{values, names, stats}` shape.

### 2.2 Context — NI Census Geography & Data

**Geography:** NISRA (Northern Ireland Statistics and Research Agency) introduced **Data Zones (DZ2021)** as the small-area statistical geography for Census 2021. There are 3,780 Data Zones across NI, nesting within 850 Super Data Zones, 80 District Electoral Areas, and 11 Local Government Districts (LGDs). This is directly analogous to Scotland's DZ22 geography and England & Wales' LSOAs.

**Boundaries:** DZ2021 boundaries are available from NISRA in multiple formats including GeoJSON and ESRI Shapefile. The GeoJSON download eliminates the reprojection step needed for Scotland (Scotland's boundaries were BNG-only shapefiles). If the NISRA GeoJSON is already WGS84, ingestion is significantly simpler than Scotland's.

**Census data:** NI Census 2021 took place on the same day as E&W (21 March 2021), so there is no year-gap issue like Scotland. NISRA publishes bulk download files in CSV and Excel format, organised by release phase:

- Phase 1: Demography, ethnicity, identity, language, religion
- Phase 2: Health and housing
- Phase 3: Partnership, household composition, qualifications, labour market, travel, migration

Data is available at Data Zone level via the NISRA Flexible Table Builder (`build.nisra.gov.uk`) which supports CSV/XLSX/JSON export. Bulk CSVs are also available from the NISRA publications page.

**Key data differences from E&W:**

- NI Census includes "Religion or religion brought up in" (unique to NI, reflects community background context).
- NI uses the same ethnic group classification framework but with fewer categories at the most detailed level.
- NI includes "Type of long-term condition" which differs from the E&W disability classification.
- Sexual orientation data is published at Data Zone level in NI.

### 2.3 Approach — Dedicated NI Module

Following the Scotland pattern exactly:

1. A new `api/northern_ireland.py` module handles all NI-specific logic.
2. `main.py` orchestrates NI startup, boundary loading, and data merging alongside E&W and Scotland.
3. The frontend requires no structural changes — the existing `nation` property convention and `getFeatureCode()`/`getFeatureName()` abstraction extend naturally.

### 2.4 Implementation Steps

**Step 1 — Create `api/northern_ireland.py`**

This module mirrors the structure of `scotland.py` and should contain:

- **Constants:** Download URLs for DZ2021 boundaries (GeoJSON preferred to avoid reprojection), DZ-to-LGD lookup, and census CSV bulk downloads.
- **`NI_LOCAL_GOVERNMENT_DISTRICTS`** list: 11 LGDs with their codes (N09 prefix codes). Analogous to `SCOTTISH_COUNCIL_AREAS`.
- **`NI_INDICATOR_MAP`** dict: Mapping from E&W dataset IDs to NI CSV tables and column extraction rules. Analogous to `SCOTLAND_INDICATOR_MAP`. This will require careful mapping work — see Step 2.
- **Boundary download & processing:**
  - `download_dz21_boundaries(data_dir)` — If GeoJSON is available directly from NISRA, this is simpler than Scotland (no shapefile → GeoJSON conversion needed). If only Shapefile is available, follow the Scotland reprojection pattern. Simplification should still be applied for performance. Output property convention: `DZ2021CD`, `DZ2021NM`, `nation: "NI"`.
  - Optional: `download_dz_lgd_lookup(data_dir)` — Builds the DZ → LGD mapping for filtering. NISRA publishes Data Zone to Super Data Zone to LGD lookup tables.
- **Census data processing:**
  - `download_ni_census_csvs(data_dir)` — Downloads bulk CSV zip files from NISRA.
  - `parse_ni_csv(csv_path)` — NISRA CSV format may differ from Scotland's multi-row header format. Initial investigation will be needed to determine the parsing approach. NISRA's Flexible Table Builder produces clean, flat CSVs with a single header row, which would be simpler than Scotland's OA-level multivariate cross-tabs.
  - `process_ni_indicator(dataset_id, csv_dir, ...)` — Process a single indicator from NI CSVs to DZ-aggregated values. If NISRA data is already at DZ level (likely, since DZ is their primary output geography), no OA→DZ aggregation step is needed — another simplification over Scotland.
  - `process_all_ni_indicators(data_dir, ...)` — Batch processor.
  - `compute_ni_population_data(...)` — Population density and total from boundary attributes or dedicated population table.

**Step 2 — NI Indicator Mapping (Research Task)**

This is the most labour-intensive part. For each of the ~86 E&W datasets in `CENSUS_DATASETS`, determine:

- Whether an equivalent NI dataset exists at DZ level.
- The NISRA table reference and column names.
- Any classification differences that require mapping (e.g., NI ethnic group categories, NI religion categories including "Religion brought up in").

**Priority indicators to map first** (high coverage value, likely available):

| Category | E&W Dataset ID | NI Availability | Notes |
|----------|---------------|----------------|-------|
| Population | `population_density`, `population_total` | Very likely | Should be in boundary attributes or MS-A01 |
| Health | `health_good`, `health_bad` | Very likely | MS-D tables, Phase 2 |
| Housing | `home_ownership`, `social_rented`, `private_rented` | Very likely | MS-D tables, Phase 2 |
| Housing | `accommodation_detached`, `accommodation_flat` | Very likely | Phase 2 |
| Ethnicity | `white_british`, `ethnic_asian`, `ethnic_black` | Available with caveats | NI uses "White" broadly; "British" identity is distinct from ethnicity in NI context |
| Religion | `christian`, `no_religion`, `muslim` | Very likely, plus NI-specific | NI has richer religion data including community background |
| Transport | `car_none`, `travel_car`, `work_from_home` | Very likely | Phase 3 |
| Education | `qualifications_level4`, `no_qualifications` | Very likely | Phase 3 |
| Economy | `economic_activity`, `unemployment` | Very likely | Phase 3 |

**NI-specific indicators to consider adding** (new `dataset_config.py` entries):

- Religion brought up in (unique NI question)
- Community background (Catholic/Protestant/Other)

These would only have data for NI and would show as "No data" for E&W and Scotland.

**Step 3 — Integration into `main.py`**

Add NI to the startup orchestration, following the Scotland pattern:

- Import NI module functions.
- In `startup_prefetch()`: download NI DZ2021 boundaries, build NI geometry index and adjacency graph, download lookup tables.
- Create `_prefetch_ni_census_data()` background task.
- Add NI data cache (`ni_data_cache` dict or extend `scotland_data_cache` naming to be nation-specific).
- In `get_lsoa_data()`: merge NI data when returning national view, filter for NI LGD when `lad_code` starts with `N09`.
- In `get_lsoa_boundaries()`: merge NI boundaries into national view, filter for NI LGD view.
- In `get_lsoa_detail_ep()`: detect NI DZ codes (prefix `95` per NISRA scheme — needs verification) and build NI detail.
- In `get_lad_list()`: append NI LGDs to the LAD dropdown.
- In `health()`: add NI readiness indicators.

**Step 4 — Frontend `core.js` updates**

Extend the feature property abstraction:

```javascript
function getFeatureCode(feature) {
  return feature.properties.LSOA21CD 
      || feature.properties.DZ22CD 
      || feature.properties.DZ2021CD  // NI
      || '';
}

function getFeatureName(feature) {
  return feature.properties.LSOA21NM 
      || feature.properties.DZ22NM 
      || feature.properties.DZ2021NM  // NI
      || getFeatureCode(feature);
}

function isNorthernIrelandFeature(feature) {
  return feature.properties.nation === 'NI' || !!feature.properties.DZ2021CD;
}
```

**Step 5 — Dataset coverage labelling**

Update `build_dataset_catalog()` in `datasets.py` to include a third coverage tier. Currently datasets are labelled `"uk"` (E&W + Scotland) or `"ew"` (E&W only). Add `"ni"` to the NI indicator map set, then compute coverage as:

- `"uk"` → available in E&W + Scotland + NI
- `"gb"` → available in E&W + Scotland only
- `"ew"` → E&W only
- `"ni_only"` → NI-specific indicators (if added)

The frontend dataset list rendering should display these coverage labels so users know which indicators are available for which nations.

**Step 6 — Update geometry module**

In `api/services/geometry.py`, add:

- `build_ni_geometry_index(dz21_file)` — Load NI DZ2021 geometries into the shared `lsoa_geometries` dict.
- `build_ni_adjacency(dz21_file)` — Build NI adjacency graph and merge into the shared `adjacency_graph`.

These follow the existing `build_scotland_geometry_index()` and `build_scotland_adjacency()` patterns exactly.

**Step 7 — Documentation**

Create `docs/northern-ireland-integration.md` following the structure of `docs/scotland-integration.md`, documenting:

- Geography mapping (LSOA ↔ DZ2021)
- Census year alignment (same year as E&W, unlike Scotland)
- Data source differences (NISRA vs Nomis)
- Boundary format and any simplification applied
- Feature property conventions (`nation: "NI"`)
- Indicator coverage and classification differences
- NI-specific considerations (community background, political sensitivity of some indicators)

### 2.5 Dependencies

May need to add dependencies depending on boundary format:

- If Shapefile only: no new deps (already have `pyshp`, `pyproj`, `shapely`).
- If GeoJSON directly available: no new deps at all.

### 2.6 Edge Cases & Notes

- **Data Zone code format:** NI DZ2021 codes follow pattern `N00` prefix — this needs verification from the NISRA documentation. The code prefix is critical for routing logic in `main.py` (like `S01` for Scotland, `E0`/`W0` for E&W).
- **LGD code format:** NI LGDs use `N09` prefix codes. Verify exact format.
- **Cross-border areas:** There are no cross-border statistical areas between NI and the Republic of Ireland, or between NI and GB. The Irish Sea is the boundary.
- **Political sensitivity:** Some NI census indicators (religion, community background, national identity) carry particular political significance in the NI context. The dashboard should present data neutrally without editorial framing.
- **Boundary file size:** 3,780 DZ boundaries — smaller than Scotland's 7,392 DZs. Performance impact should be minimal.

### 2.7 Estimated Complexity

Medium-high. The architectural pattern is established (Scotland is the template), but the indicator mapping research and NI CSV format investigation represent significant effort. Recommended to break into sub-tasks:

1. Boundary ingestion & geometry (can be verified independently)
2. Population data (quick win from boundary attributes)
3. Indicator mapping research (desk work)
4. CSV parsing & processing (depends on NISRA format)
5. Integration & testing

---

## Plan 3: Election Voting Data Overlay

### 3.1 Objective

Add a toggleable election data overlay to the map, displaying voting results for:

- **General Elections** (UK Parliamentary constituencies — 650 areas)
- **Local Council Elections** (ward-level — several thousand areas)

Because election geographies (constituencies, wards) do not align with census geographies (LSOAs, Data Zones), this overlay operates as a separate layer system that sits above the census choropleth.

### 3.2 Context — Geography Mismatch

This is the most architecturally complex of the four features because the election data uses entirely different boundary sets:

| Layer | Geography | Count (approx) | Boundary source |
|-------|-----------|----------------|-----------------|
| Census | LSOA / DZ / DZ2021 | ~46,844 | ONS, Scottish Gov, NISRA |
| General Election | Parliamentary Constituency (2024 boundaries) | 650 | ONS Open Geography Portal |
| Local Elections | Electoral Ward | ~8,500 (England) | ONS Open Geography Portal |

There is no clean nesting relationship between these geographies. A constituency may span parts of multiple LADs, and a ward may contain parts of multiple LSOAs.

### 3.3 Approach — Layered Overlay Architecture

The election overlay should be implemented as an independent Leaflet layer group that can be toggled on/off without affecting the census choropleth underneath. This follows the principle of an "overlay" in the GIS sense — a separate thematic layer rendered on top of the base data.

### 3.4 Implementation Steps — Phase A: General Election Data

**Step A1 — Create `api/elections.py` module**

A dedicated module for all election data ingestion and processing:

- **Constants:**
  - Constituency boundary ArcGIS FeatureServer URL (ONS): `https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Westminster_Parliamentary_Constituencies_July_2024_Boundaries_UK_BGC/FeatureServer/0/query` (BGC = generalised, good for dashboard use)
  - House of Commons Library results CSV URL for 2024 GE
  - Optionally: historical results from `electionresults.parliament.uk` Datasette API
- **Boundary download:**
  - `download_constituency_boundaries(data_dir)` — Fetch 2024 constituency boundaries from ONS ArcGIS. Use BGC (generalised 20m) resolution for reasonable file size. Store as GeoJSON.
  - Feature properties should include: `PCON24CD` (constituency code), `PCON24NM` (constituency name).
- **Results download & processing:**
  - `download_ge_results(data_dir, year)` — Download CSV from Commons Library. For 2024: constituency code, constituency name, winning party, winning candidate, vote counts per party, turnout, electorate, majority.
  - `process_ge_results(data_dir, year)` — Parse CSV into a structured dict keyed by constituency code. Compute derived metrics:
    - Vote share per party (%)
    - Turnout (%)
    - Majority (votes and %)
    - Swing from previous election (if historical data available)
    - Winning party
  - Return format: `{constituency_code: {party: str, vote_share: {party: float}, turnout: float, majority_pct: float, ...}}`
- **Endpoint data shape:**
  - `get_ge_overlay(year)` → returns GeoJSON FeatureCollection with results properties embedded in each constituency feature.

**Step A2 — New API endpoints in `main.py`**

```
GET /api/elections/ge/boundaries?year=2024
GET /api/elections/ge/results?year=2024
GET /api/elections/ge/overlay?year=2024     (boundaries + results merged)
GET /api/elections/available                 (list available elections)
```

The `/overlay` endpoint merges constituency boundaries with results data so the frontend receives a single GeoJSON with all properties needed for rendering. This avoids a client-side join.

**Step A3 — Frontend overlay module: `frontend/static/js/modules/elections.js`**

A new JS module following the existing module pattern:

- **State additions** (in `core.js`):
  ```javascript
  electionOverlay: null,       // Leaflet GeoJSON layer
  electionData: null,          // raw overlay data
  electionMode: null,          // 'ge' | 'local' | null
  electionYear: null,          // '2024' etc.
  electionMetric: 'winner',    // 'winner' | 'turnout' | 'majority' | 'party_share'
  electionPartyFilter: null,   // for party-specific share view
  ```
- **Rendering functions:**
  - `loadElectionOverlay(type, year)` — fetch overlay GeoJSON from API, create Leaflet layer.
  - `styleElectionFeature(feature)` — choropleth styling based on `electionMetric`. For "winner" mode, colour by party (using standard UK party colours: Labour red, Conservative blue, Lib Dem orange, SNP yellow, Reform teal, Green green, etc.). For "turnout"/"majority", use a sequential colour scale.
  - `onElectionFeatureClick(feature)` — show constituency results detail in the sidebar (or a popup), including candidate list, vote counts, swing.
  - `renderElectionLegend()` — party colour key or quantile legend depending on metric.
  - `removeElectionOverlay()` — clean removal of the layer.
- **Layer interaction:**
  - The election layer should be semi-transparent (e.g., `fillOpacity: 0.5`) so the census choropleth is visible underneath.
  - Hover behaviour: show constituency name and headline result.
  - Click behaviour: show full results panel.
  - The election layer should have `interactive: true` and sit above the census layer in z-order but below the dissolve selection layer.

**Step A4 — Frontend UI controls**

Add an "Election Overlay" section to the sidebar, below the dataset selector:

- Toggle switch: "Show election overlay"
- Dropdown: Election type (General Election / Local Elections)
- Dropdown: Year (2024, with extensibility for historical)
- Dropdown: Display metric (Winning party / Turnout / Majority / Party vote share)
- If "Party vote share" selected: party selector dropdown

**Step A5 — Styling in `dashboard.css`**

- Election overlay legend styles (party colour chips).
- Semi-transparent overlay visual treatment.
- Election results detail panel styling (candidate table, vote bars).

### 3.5 Implementation Steps — Phase B: Local Council Elections

**Step B1 — Extend `api/elections.py`**

Local council elections are significantly more complex:

- **Geography:** Electoral wards. Boundaries available from ONS ArcGIS (Wards, December 2023 or later vintage).
- **Results data:** Fragmented across individual local authorities. The best aggregated sources are:
  - Democracy Club API / CSV exports
  - Electoral Commission data
  - House of Commons Library local election handbooks (for English elections)
- **England-only scope initially:** Local elections are administered separately across the UK. Start with English council elections (the most complete and accessible dataset). Welsh, Scottish, and NI local elections use different electoral systems (STV in Scotland and NI, FPTP in Wales and most of England).
- **Electoral system complexity:** Some English councils use all-up elections, others elect by thirds. Multi-member wards exist. This means "results" for a ward may represent multiple seats with different outcomes.

**Step B2 — Ward boundary download**

Add to `elections.py`:

- `download_ward_boundaries(data_dir)` — Fetch electoral ward boundaries from ONS ArcGIS. Use generalised resolution.
- Ward property convention: `WD23CD`, `WD23NM` (or appropriate vintage).

**Step B3 — Local results processing**

- `download_local_results(data_dir, year)` — Download from Commons Library or Democracy Club.
- `process_local_results(data_dir, year)` — Parse and normalise. Key challenge: matching ward codes between results data and boundary data (code vintages may differ).
- Simplified metrics for local elections: winning party, vote share, turnout where available.

**Step B4 — New endpoints**

```
GET /api/elections/local/boundaries?year=2024
GET /api/elections/local/results?year=2024
GET /api/elections/local/overlay?year=2024
```

**Step B5 — Frontend**

Extend the election overlay module to handle ward-level display. The rendering logic is the same (party-colour choropleth) but with more features and potentially slower rendering. Consider:

- Canvas renderer for ward-level display (Leaflet's default SVG renderer may struggle with ~8,500 features).
- Simplification of ward boundaries for overview zoom levels.
- LAD filtering: when a user selects a LAD from the existing dropdown, the ward overlay should filter to that area.

### 3.6 Implementation Steps — Phase C: Historical & Comparison

Once Phase A and B are working:

- Add historical GE data (2019, 2017, 2015) with appropriate boundary vintages.
- Add "swing" and "change" metrics comparing elections.
- Consider a split-view or difference map mode.

### 3.7 Data Sources Summary

| Data | Source | Format | URL Pattern |
|------|--------|--------|-------------|
| Constituency boundaries (2024) | ONS Open Geography Portal | GeoJSON via ArcGIS FeatureServer | `services1.arcgis.com/...Westminster_Parliamentary_Constituencies_July_2024_Boundaries_UK_BGC/...` |
| GE 2024 results | House of Commons Library | CSV | `commonslibrary.parliament.uk/research-briefings/cbp-10009/` |
| GE historical results | `electionresults.parliament.uk` | Datasette/CSV | `electionresults.parliament.uk/` |
| Ward boundaries | ONS Open Geography Portal | GeoJSON via ArcGIS FeatureServer | Various — vintage-dependent |
| Local election results | Commons Library / Democracy Club | CSV/API | Multiple sources |

### 3.8 Edge Cases & Notes

- **Boundary vintage alignment:** Constituency boundaries changed significantly in 2024 (the first boundary review since 2010). Historical results must be shown on historical boundaries, or notional results on current boundaries (the Commons Library provides both).
- **Northern Ireland elections:** NI uses STV (Single Transferable Vote) for all elections. Results data is structured differently (transfer counts, quotas). Phase A should include NI constituencies but display may need adaptation for the multi-count STV format.
- **Scotland and Wales:** Devolved parliament elections (Holyrood, Senedd) use different constituency boundaries and mixed electoral systems (constituency + regional list). These are out of scope initially but the architecture should not preclude them.
- **Party colour standardisation:** Need a consistent party colour map. Suggest defining this as a constant in `core.js` or in the elections module.
- **Performance:** 650 constituency polygons is trivial. 8,500 ward polygons is manageable but should use the canvas renderer.

### 3.9 Estimated Complexity

High. This is the largest of the four features. Recommended phasing:

- **Phase A (General Election overlay):** Medium complexity, high value, well-defined data sources. Deliver first.
- **Phase B (Local elections):** High complexity due to data fragmentation and electoral system variety. Deliver second.
- **Phase C (Historical/comparison):** Medium complexity, depends on A and B. Deliver third.

---

## Plan 4: Data Explorer (Tabular View)

### 4.1 Objective

Add a tabular data exploration interface that allows users to query, sort, filter, and compare the census data that is already cached locally by the dashboard. This provides an alternative to the map-based view for users who need to work with the numbers directly.

### 4.2 Context

The dashboard already caches significant amounts of data locally (in `/app/data` and in the `data_cache` TTL cache). For a national dataset, the cached JSON contains `{values: {area_code: value}, names: {area_code: name}, stats: {...}}` with up to ~46,844 entries. The Data Explorer exposes this data in a structured, queryable table format.

### 4.3 Approach — New Frontend Module + Lightweight API Extension

The Data Explorer should be primarily a frontend feature, since the data is already available via existing API endpoints. The main new work is a frontend module that fetches dataset values and renders them in an interactive table. A small API extension provides multi-dataset queries for comparison views.

### 4.4 Implementation Steps

**Step 1 — New API endpoint for multi-dataset export**

Add `GET /api/explorer/data` to `main.py`:

```
GET /api/explorer/data?datasets=population_density,health_good,home_ownership&lad_code=E09000033
```

Returns a combined table structure:

```json
{
  "columns": ["area_code", "area_name", "nation", "population_density", "health_good", "home_ownership"],
  "column_labels": {"population_density": "Population Density (per km²)", ...},
  "rows": [
    {"area_code": "E01000001", "area_name": "City of London 001A", "nation": "EW", "population_density": 5234.5, "health_good": 87.2, "home_ownership": 34.1},
    ...
  ],
  "stats": {
    "population_density": {"min": 0.5, "max": 25000, "mean": 4200, ...},
    ...
  },
  "total_rows": 35672
}
```

This endpoint assembles data from the existing cache files/memory cache, joining multiple datasets by area code. It supports pagination (`offset`, `limit` query params), sorting (`sort_by`, `sort_dir`), and basic filtering (`filter_nation`, `filter_lad`).

**Step 2 — Backend implementation in `api/services/datasets.py`**

Add a function `build_explorer_table(dataset_ids, lad_code, sort_by, sort_dir, offset, limit)`:

- For each requested dataset, load from cache (memory → disk → skip if unavailable).
- Build a unified row set keyed by area code.
- Include Scotland and NI data where available.
- Apply sorting and pagination server-side (important — returning 46K unsorted rows to the browser is expensive).
- Return the paginated slice plus total row count for pagination controls.

**Step 3 — CSV/Excel export from explorer**

Add `GET /api/explorer/export?datasets=...&format=csv` endpoint:

- Same data assembly as the table endpoint but returns the full dataset (no pagination) as a downloadable CSV or Excel file.
- CSV is straightforward (use Python `csv` module).
- Excel (`.xlsx`) would require adding `openpyxl` to dependencies — consider whether this is needed or whether CSV suffices.

**Step 4 — Frontend module: `frontend/static/js/modules/explorer.js`**

A new JS module providing the Data Explorer UI:

- **Table rendering:**
  - Virtual scrolling or paginated table (46K rows cannot be DOM-rendered at once).
  - Column headers with sort indicators (click to sort ascending/descending).
  - Sticky header row.
  - Alternating row shading for readability.
  - Cell formatting: numbers formatted using the existing `fmt()` helper, with unit suffixes.
- **Dataset selection:**
  - Multi-select dataset picker (checkboxes grouped by category, matching the sidebar dataset list).
  - "Add column" / "Remove column" interaction.
  - Limit to a reasonable maximum (e.g., 10 datasets at once) to keep the table readable.
- **Filtering:**
  - Nation filter (E&W / Scotland / NI / All).
  - LAD/Council filter (re-use the existing LAD dropdown).
  - Value range filter per column (min/max inputs).
  - Text search on area name.
- **Sorting:**
  - Click column header to sort. Server-side sorting via API re-fetch.
  - Secondary sort on area name for stability.
- **Pagination:**
  - Page size selector (50 / 100 / 500).
  - Page navigation controls.
  - "Showing rows X–Y of Z" indicator.
- **Row interaction:**
  - Click a row to highlight that area on the map (if map is visible).
  - Click area code to open the detail panel for that area.
- **Export:**
  - "Download CSV" button triggers the export endpoint.
  - "Download selection" if user has filtered/sorted — exports the current query result.

**Step 5 — UI integration**

Two approaches to consider (recommend Option A):

**Option A — Tab/View toggle:** Add a view toggle at the top of the page: "Map View" / "Table View". The Data Explorer replaces the map area when active. The sidebar remains available for dataset selection and filtering. This is the simplest integration and avoids layout complexity.

**Option B — Split panel:** Show the table below or beside the map. More complex layout but allows cross-referencing. Could be resource-heavy on smaller screens.

For Option A, add to `index.html`:

- A view toggle control in the top bar.
- A `<div id="explorer-container">` that is shown/hidden alongside the map container.
- The explorer container holds: dataset selector, filter controls, table area, pagination, export button.

**Step 6 — Styling in `dashboard.css`**

- Table styles: borders, header background, row hover, sticky header.
- Pagination controls.
- Filter input styling.
- View toggle button styling.
- Responsive considerations (table should scroll horizontally on narrow screens).

**Step 7 — Summary statistics row**

Below (or above) the table, show aggregate statistics for the visible/filtered data:

- Count of areas.
- For each numeric column: min, max, mean, median.
- This provides a quick sanity check and summary without scrolling.

### 4.5 Dependencies

- No new backend dependencies for CSV export.
- If Excel export is desired: add `openpyxl` to `requirements.txt`.
- Frontend: no new libraries. The table is built with vanilla HTML/CSS/JS, keeping consistent with the existing stack.

### 4.6 Edge Cases & Notes

- **Data availability gaps:** Not all datasets have data for all areas (especially Scotland and NI indicators). The table should show empty cells (or "—") rather than zeros for genuinely missing data. This requires distinguishing "no data" from "zero value" — which the current data shape doesn't explicitly do. Consider adding a `coverage` set to the API response listing which area codes have data for each dataset.
- **Performance — Server-side pagination is essential.** Loading 46K rows × 10 columns into the browser and then sorting client-side will be sluggish. Server-side sort/filter/paginate keeps the browser responsive.
- **Caching:** The explorer endpoint can use the same `data_cache` as the map endpoints. No additional caching layer needed.
- **Interaction with map:** When the user clicks a row in the table, it would be ideal to fly the map to that area and highlight it. This requires the explorer and map modules to communicate via the shared `state` object — straightforward given the existing architecture.

### 4.7 Estimated Complexity

Medium. The backend work is modest (assembling already-cached data into a table shape). The frontend work is the bulk — building a performant, paginated, sortable table with filtering. No exotic dependencies, but attention to UX detail is needed to make the table genuinely useful rather than just a data dump.

---

## Implementation Priority & Sequencing

| Plan | Complexity | Dependencies | Suggested Order |
|------|-----------|-------------|-----------------|
| 1. Shapefile Export | Low | None | First — quick win, self-contained |
| 4. Data Explorer | Medium | None (benefits from NI data but not blocked) | Second — high utility, independent |
| 2. Northern Ireland Data | Medium-High | Research on NISRA data formats | Third — established pattern, significant research |
| 3. Election Overlay (Phase A) | Medium | None | Fourth — or parallel with NI if capacity allows |
| 3. Election Overlay (Phase B+C) | High | Phase A complete | Last — most complex, most fragmented data |

Plans 1 and 4 can be developed in parallel. Plan 2 follows the Scotland template closely and can proceed once the NISRA data format research is done. Plan 3 Phase A can run in parallel with Plan 2 since they touch different parts of the codebase.

---

## File Change Summary

| File | Plan 1 | Plan 2 | Plan 3 | Plan 4 |
|------|--------|--------|--------|--------|
| `api/main.py` | New endpoint | NI startup + routes | Election endpoints | Explorer endpoint |
| `api/services/geometry.py` | `export_dissolve_as_shapefile()` | NI geometry/adjacency | — | — |
| `api/services/datasets.py` | — | Coverage labels | — | `build_explorer_table()` |
| `api/services/dataset_config.py` | — | NI-specific datasets (optional) | — | — |
| `api/scotland.py` | — | — | — | — |
| **`api/northern_ireland.py`** | — | **New file** | — | — |
| **`api/elections.py`** | — | — | **New file** | — |
| `frontend/static/js/modules/core.js` | — | Feature code helpers | Election state | Explorer state |
| `frontend/static/js/modules/selection.js` | Export UI | — | — | — |
| **`frontend/static/js/modules/elections.js`** | — | — | **New file** | — |
| **`frontend/static/js/modules/explorer.js`** | — | — | — | **New file** |
| `frontend/static/js/modules/rendering.js` | — | Coverage badges | Election legend | — |
| `frontend/static/js/modules/app.js` | — | — | Overlay toggle wiring | View toggle wiring |
| `frontend/static/css/dashboard.css` | Minimal | Coverage badges | Election styles | Table styles |
| `frontend/templates/index.html` | — | — | Overlay controls | Explorer container |
| `api/requirements.txt` | — | Possibly none | — | Possibly `openpyxl` |
| `docker-compose.yml` | — | — | — | — |
| **`docs/northern-ireland-integration.md`** | — | **New file** | — | — |
| **`docs/election-overlay.md`** | — | — | **New file** | — |
| **`docs/data-explorer.md`** | — | — | — | **New file** |
