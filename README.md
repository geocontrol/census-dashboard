# Census Dashboard

UK Census Explorer for England, Wales, and Scotland.

The app combines:
- a FastAPI backend for census data, boundary processing, and aggregation
- a static frontend served by nginx
- Docker Compose for local orchestration

**Architecture**

The codebase is split into a few clear layers so data access, geometry work, and UI behavior stay easier to reason about.

Backend:
- [api/main.py](/Users/marksimpkins/TPM/census-dashboard/api/main.py) is the API entry point. It owns FastAPI route registration, startup orchestration, and cache coordination.
- [api/services/dataset_config.py](/Users/marksimpkins/TPM/census-dashboard/api/services/dataset_config.py) holds the dataset catalogue and detail-panel dataset definitions.
- [api/services/datasets.py](/Users/marksimpkins/TPM/census-dashboard/api/services/datasets.py) handles Nomis fetches, dataset catalog shaping, detail fetching, LAD list fetches, and rate aggregation helpers.
- [api/services/geometry.py](/Users/marksimpkins/TPM/census-dashboard/api/services/geometry.py) owns boundary fetches, geometry indexes, adjacency graphs, and dissolve operations.
- [api/scotland.py](/Users/marksimpkins/TPM/census-dashboard/api/scotland.py) contains Scotland-specific ingestion and transformation logic for Data Zones, OA-to-DZ lookup tables, and OA CSV processing.

Frontend:
- [frontend/templates/index.html](/Users/marksimpkins/TPM/census-dashboard/frontend/templates/index.html) is the single page shell.
- [frontend/static/js/modules/core.js](/Users/marksimpkins/TPM/census-dashboard/frontend/static/js/modules/core.js) defines shared constants and global state.
- [frontend/static/js/modules/utils.js](/Users/marksimpkins/TPM/census-dashboard/frontend/static/js/modules/utils.js) contains formatting and generic UI helpers.
- [frontend/static/js/modules/rendering.js](/Users/marksimpkins/TPM/census-dashboard/frontend/static/js/modules/rendering.js) owns map styling, legends, dataset list rendering, and detail rendering.
- [frontend/static/js/modules/selection.js](/Users/marksimpkins/TPM/census-dashboard/frontend/static/js/modules/selection.js) owns multi-area selection, dissolve, aggregate stats, and export behavior.
- [frontend/static/js/modules/app.js](/Users/marksimpkins/TPM/census-dashboard/frontend/static/js/modules/app.js) wires the page together and registers event handlers.

Runtime flow:
1. FastAPI startup prefetches national boundaries, builds adjacency and geometry indexes, and warms Scotland data in the background.
2. The frontend loads dataset metadata, LAD options, and waits for boundary readiness.
3. The frontend requests dataset values plus boundary GeoJSON, then renders the choropleth map.
4. Selection workflows call backend dissolve and aggregate endpoints for geometry unions and summary stats.

**Repo Map**

```text
api/
  main.py
  scotland.py
  services/
    dataset_config.py
    datasets.py
    geometry.py

frontend/
  templates/
    index.html
  static/
    css/
      dashboard.css
    js/
      modules/
        core.js
        utils.js
        rendering.js
        selection.js
        app.js

docs/
  codebase-explainer.md
  scotland-integration.md
```

**Running**

Use Docker Compose:

```bash
docker compose up --build
```

Frontend:
- `http://localhost:8080`

Useful docs:
- [docs/codebase-explainer.md](/Users/marksimpkins/TPM/census-dashboard/docs/codebase-explainer.md)
- [docs/scotland-integration.md](/Users/marksimpkins/TPM/census-dashboard/docs/scotland-integration.md)
