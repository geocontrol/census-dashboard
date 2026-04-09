/**
 * UK Census Explorer v5
 * England & Wales (LSOA) + Scotland (Data Zone)
 * National + LAD views · Selection mode with Shapely dissolve
 */

const API = '/api';

/** Get the area code from a GeoJSON feature (LSOA or DZ). */
function getFeatureCode(feature) {
  return feature.properties.LSOA21CD || feature.properties.DZ22CD || '';
}

/** Get the area name from a GeoJSON feature. */
function getFeatureName(feature) {
  return feature.properties.LSOA21NM || feature.properties.DZ22NM || getFeatureCode(feature);
}

/** Check if a feature is from Scotland. */
function isScotlandFeature(feature) {
  return feature.properties.nation === 'SC' || !!feature.properties.DZ22CD;
}

const COLOUR_SCHEMES = {
  YlOrRd:['#ffffb2','#fed976','#feb24c','#fd8d3c','#fc4e2a','#e31a1c','#b10026'],
  PuBu:['#f1eef6','#d0d1e6','#a6bddb','#74a9cf','#3690c0','#0570b0','#034e7b'],
  BuGn:['#edf8fb','#ccece6','#99d8c9','#66c2a4','#41ae76','#238b45','#005824'],
  GnBu:['#f0f9e8','#ccebc5','#a8ddb5','#7bccc4','#4eb3d3','#2b8cbe','#08589e'],
  OrRd:['#fef0d9','#fdd49e','#fdbb84','#fc8d59','#ef6548','#d7301f','#990000'],
  YlGn:['#ffffe5','#f7fcb9','#d9f0a3','#addd8e','#78c679','#31a354','#006837'],
  Reds:['#fee5d9','#fcbba1','#fc9272','#fb6a4a','#ef3b2c','#cb181d','#99000d'],
  Greens:['#f7fcf5','#e5f5e0','#c7e9c0','#a1d99b','#74c476','#41ab5d','#005a32'],
  PuRd:['#f1eef6','#d4b9da','#c994c7','#df65b0','#e7298a','#ce1256','#91003f'],
  Blues:['#eff3ff','#c6dbef','#9ecae1','#6baed6','#4292c6','#2171b5','#084594'],
  Purples:['#f2f0f7','#dadaeb','#bcbddc','#9e9ac8','#807dba','#6a51a3','#4a1486'],
  YlOrBr:['#ffffe5','#fff7bc','#fee391','#fec44f','#fe9929','#ec7014','#cc4c02'],
  Greys:['#f7f7f7','#d9d9d9','#bdbdbd','#969696','#737373','#525252','#252525'],
  BuPu:['#edf8fb','#bfd3e6','#9ebcda','#8c96c6','#8c6bb1','#88419d','#6e016b'],
  RdPu:['#feebe2','#fcc5c0','#fa9fb5','#f768a1','#dd3497','#ae017e','#7a0177'],
  YlGnBu:['#ffffd9','#edf8b1','#c7e9b4','#7fcdbb','#41b6c4','#1d91c0','#225ea8'],
};

const state = {
  map: null, geojsonLayer: null,
  currentDataset: 'population_density', currentLAD: '',
  datasets: {}, currentValues: {}, currentStats: {},
  currentColorScheme: 'YlOrRd', quantileBreaks: [],
  selectedLSOA: null, geojsonData: null, boundariesReady: false,
  // Selection mode state
  selectMode: false,
  selectedLSOAs: new Set(),
  dissolvedLayer: null,      // Leaflet layer for dissolved boundary
  dissolveResult: null,      // last dissolve API response
  adjacency: null,           // adjacency graph
};

// ═══════ Init ═══════

document.addEventListener('DOMContentLoaded', async () => {
  initMap();
  initSelectionUI();
  await loadDatasets();
  await loadLADList();
  await waitForBoundaries();
  await loadData();
  // Load adjacency graph in background
  loadAdjacency();
});

function initMap() {
  state.map = L.map('map', { center: [54.5, -3.0], zoom: 6, zoomControl: true, preferCanvas: true });
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO', subdomains: 'abcd', maxZoom: 19,
  }).addTo(state.map);
  document.getElementById('btn-zoom-fit').addEventListener('click', fitToBounds);
  document.getElementById('btn-reset').addEventListener('click', () => state.map.setView([54.5, -3.0], 6));
  document.getElementById('close-detail').addEventListener('click', closeDetail);
}

function initSelectionUI() {
  document.getElementById('btn-select-mode').addEventListener('click', toggleSelectMode);
  document.getElementById('btn-clear-selection').addEventListener('click', clearSelection);
  document.getElementById('btn-dissolve').addEventListener('click', dissolveSelection);
  document.getElementById('btn-stats').addEventListener('click', loadSelectionStats);
  document.getElementById('btn-export').addEventListener('click', exportSelection);
}

async function waitForBoundaries() {
  setOverlay(true, 'Loading national boundaries…');
  for (let i = 0; i < 60; i++) {
    try {
      const res = await fetch(`${API}/health`);
      const data = await res.json();
      if (data.boundaries_ready) { state.boundariesReady = true; return; }
    } catch (e) {}
    await new Promise(r => setTimeout(r, 1000));
    setOverlay(true, `Loading national boundaries… (${i+1}s)`);
  }
}

async function loadAdjacency() {
  try {
    const res = await fetch(`${API}/adjacency`);
    if (res.ok) {
      const data = await res.json();
      state.adjacency = data.graph;
      console.log(`Adjacency graph loaded: ${data.count} LSOAs`);
    }
  } catch (e) { console.warn('Adjacency not yet available'); }
}

// ═══════ Data Loading ═══════

async function loadDatasets() {
  try {
    const res = await fetch(`${API}/datasets`);
    state.datasets = await res.json();
    renderDatasetList(state.datasets.categories);
  } catch (e) { setStatus('API Error', false); }
}

async function loadLADList() {
  try {
    const res = await fetch(`${API}/lad/list`);
    const data = await res.json();
    const select = document.getElementById('lad-select');
    data.lads.forEach(lad => {
      const opt = document.createElement('option');
      opt.value = lad.code; opt.textContent = lad.name;
      select.appendChild(opt);
    });
    select.addEventListener('change', onLADChange);
  } catch (e) {}
}

async function loadData() {
  const ladParam = state.currentLAD ? `?lad_code=${state.currentLAD}` : '';
  const resolution = state.currentLAD ? 'bgc' : 'bsc';
  const boundaryParam = state.currentLAD
    ? `?lad_code=${state.currentLAD}&resolution=${resolution}`
    : `?resolution=${resolution}`;
  setOverlay(true, state.currentLAD ? 'Loading area data…' : 'Loading national census data…');
  document.getElementById('demo-banner').style.display = 'none';
  try {
    const [valuesRes, boundaryRes] = await Promise.all([
      fetch(`${API}/lsoa/data/${state.currentDataset}${ladParam}`),
      fetch(`${API}/boundaries/lsoa${boundaryParam}`),
    ]);
    const valuesData = await valuesRes.json();
    state.currentValues = valuesData.values || {};
    state.currentStats = valuesData.stats || {};
    state.geojsonData = await boundaryRes.json();
    const dsInfo = findDataset(state.currentDataset);
    if (dsInfo) state.currentColorScheme = dsInfo.color_scheme;
    computeQuantileBreaks();
    updateLegend();
    updateStatsGrid();
    renderMap();
    setStatus(`${Object.keys(state.currentValues).length.toLocaleString()} areas`, true);
    setOverlay(false);
  } catch (e) {
    console.error('Load error:', e);
    setStatus('Error', false);
    setOverlay(false);
  }
}

// ═══════ Rendering ═══════

function renderDatasetList(categories) {
  const container = document.getElementById('dataset-categories');
  container.innerHTML = '';
  Object.entries(categories).forEach(([cat, items]) => {
    const header = document.createElement('div');
    header.className = 'dataset-category-header'; header.textContent = cat;
    container.appendChild(header);
    items.forEach(item => {
      const el = document.createElement('div');
      el.className = 'dataset-item' + (item.id === state.currentDataset ? ' active' : '');
      el.dataset.id = item.id;
      el.innerHTML = `<div class="dataset-dot"></div><span class="dataset-label">${item.label}</span><span class="dataset-unit">${item.unit}</span>`;
      el.addEventListener('click', () => onDatasetChange(item.id, item.color_scheme));
      container.appendChild(el);
    });
  });
}

function renderMap() {
  if (state.geojsonLayer) state.map.removeLayer(state.geojsonLayer);
  if (!state.geojsonData || !state.geojsonData.features?.length) { showNoDataMessage(); return; }
  state.geojsonLayer = L.geoJSON(state.geojsonData, {
    style: f => styleFeature(f),
    onEachFeature: (f, layer) => {
      layer.on({
        mouseover: e => onFeatureHover(e, f),
        mouseout: e => onFeatureOut(e),
        click: e => onFeatureClick(e, f),
      });
    },
  }).addTo(state.map);
  if (state.currentLAD) fitToBounds();
}

function styleFeature(feature) {
  const code = getFeatureCode(feature);
  const isSelected = state.selectedLSOAs.has(code);
  const value = state.currentValues[code];
  const fill = value !== undefined ? getColour(value) : '#2a3044';

  if (isSelected) {
    return {
      fillColor: fill, fillOpacity: 0.85,
      color: '#00ffd5', weight: 2.5, opacity: 1,
    };
  }
  const sel = code === state.selectedLSOA;
  return { fillColor: fill, fillOpacity: sel ? 0.95 : 0.75,
           color: sel ? '#ffffff' : '#0f1117', weight: sel ? 1.5 : 0.3, opacity: 1 };
}

function getColour(value) {
  const b = state.quantileBreaks;
  const c = COLOUR_SCHEMES[state.currentColorScheme] || COLOUR_SCHEMES.YlOrRd;
  if (!b.length) return c[3];
  for (let i = 0; i < b.length; i++) { if (value <= b[i]) return c[i]; }
  return c[c.length - 1];
}

function computeQuantileBreaks() {
  const vals = Object.values(state.currentValues).filter(v => v != null);
  if (!vals.length) return;
  vals.sort((a, b) => a - b);
  const n = vals.length;
  state.quantileBreaks = [];
  for (let i = 1; i <= 7; i++) state.quantileBreaks.push(vals[Math.max(0, Math.floor((i/7)*n)-1)]);
}

function updateLegend() {
  const c = COLOUR_SCHEMES[state.currentColorScheme] || COLOUR_SCHEMES.YlOrRd;
  document.getElementById('legend-gradient').style.background = `linear-gradient(to right, ${c.join(', ')})`;
  const s = state.currentStats;
  if (s.min !== undefined)
    document.getElementById('legend-labels').innerHTML = `<span>${fmt(s.min)}</span><span>${fmt(s.p50)}</span><span>${fmt(s.max)}</span>`;
}

function updateStatsGrid() {
  const s = state.currentStats;
  const grid = document.getElementById('stats-grid');
  if (!s || s.min === undefined) { grid.innerHTML = ''; return; }
  grid.innerHTML = [['Min',fmt(s.min)],['Max',fmt(s.max)],['Median',fmt(s.p50)],
    ['Mean',fmt(s.mean)],['P25',fmt(s.p25)],['P75',fmt(s.p75)]].map(([l,v]) =>
    `<div class="stat-cell"><div class="stat-cell-label">${l}</div><div class="stat-cell-value">${v}</div></div>`).join('');
}

// ═══════ Interactions ═══════

let hoverPopup = null;
function onFeatureHover(e, feature) {
  const layer = e.target;
  const code = getFeatureCode(feature);
  const name = getFeatureName(feature);
  const value = state.currentValues[code];
  const dsInfo = findDataset(state.currentDataset);
  const isInSelection = state.selectedLSOAs.has(code);
  const areaType = isScotlandFeature(feature) ? 'Data Zone' : 'LSOA';

  if (!isInSelection) {
    layer.setStyle({ weight: 1.5, color: '#ffffff', fillOpacity: 0.9 });
  } else {
    layer.setStyle({ weight: 3, color: '#00ffd5', fillOpacity: 0.6 });
  }
  layer.bringToFront();
  if (hoverPopup) state.map.closePopup(hoverPopup);
  const selLabel = state.selectMode ? `<div style="font-size:10px;color:#00d2be;margin-top:6px">${isInSelection ? '⊖ Click to deselect' : '⊕ Click to select'}</div>` : '';
  const nationBadge = isScotlandFeature(feature) ? `<div style="font-size:9px;color:#8b97b5;margin-top:2px">${areaType} · Scotland</div>` : '';
  hoverPopup = L.popup({ closeButton: false, offset: [0, -4] })
    .setLatLng(e.latlng)
    .setContent(`<div class="lsoa-popup"><div class="lsoa-popup-name">${name}</div><div class="lsoa-popup-code">${code}</div>${nationBadge}<div class="lsoa-popup-value">${value !== undefined ? fmt(value) : 'No data'}</div><div class="lsoa-popup-unit">${dsInfo?.unit||''}</div>${selLabel}</div>`)
    .openOn(state.map);
}

function onFeatureOut(e) {
  const code = e.target.feature ? getFeatureCode(e.target.feature) : '';
  if (state.selectedLSOA === code) return;
  e.target.setStyle(styleFeature(e.target.feature));
  if (hoverPopup) { state.map.closePopup(hoverPopup); hoverPopup = null; }
}

function onFeatureClick(e, feature) {
  const code = getFeatureCode(feature);
  if (hoverPopup) { state.map.closePopup(hoverPopup); hoverPopup = null; }

  if (state.selectMode) {
    if (state.selectedLSOAs.has(code)) {
      state.selectedLSOAs.delete(code);
    } else {
      state.selectedLSOAs.add(code);
    }
    state.dissolveResult = null;
    if (state.dissolvedLayer) {
      state.map.removeLayer(state.dissolvedLayer);
      state.dissolvedLayer = null;
    }
    updateSelectionUI();
    if (state.geojsonLayer) {
      state.geojsonLayer.eachLayer(layer => {
        layer.setStyle(styleFeature(layer.feature));
      });
    }
    return;
  }

  // Normal mode — open detail panel
  state.selectedLSOA = code;
  if (state.geojsonLayer) {
    state.geojsonLayer.eachLayer(layer => {
      layer.setStyle(styleFeature(layer.feature));
    });
  }
  openDetail(code, getFeatureName(feature),
    state.currentValues[code], findDataset(state.currentDataset));
}

// ═══════ Selection Mode ═══════

function toggleSelectMode() {
  state.selectMode = !state.selectMode;
  updateSelectionUI();
  if (!state.selectMode && state.selectedLSOAs.size === 0) closeDetail();
}

function updateSelectionUI() {
  const bar = document.getElementById('select-count-bar');
  const count = state.selectedLSOAs.size;
  document.getElementById('select-count').textContent = count;
  // Show count bar whenever there is a selection, regardless of selectMode
  bar.classList.toggle('visible', count > 0);
  // Sync select mode button state
  const btn = document.getElementById('btn-select-mode');
  if (btn) {
    btn.classList.toggle('active', state.selectMode);
    btn.innerHTML = state.selectMode
      ? '<div class="select-toggle-icon"></div>Selection ON — click areas to add/remove'
      : '<div class="select-toggle-icon"></div>Paint selection mode';
  }
  const mapEl = document.getElementById('map');
  if (mapEl) mapEl.style.cursor = state.selectMode ? 'crosshair' : '';
}

function clearSelection() {
  state.selectedLSOAs.clear();
  state.dissolveResult = null;
  if (state.dissolvedLayer) {
    state.map.removeLayer(state.dissolvedLayer);
    state.dissolvedLayer = null;
  }
  updateSelectionUI();
  if (state.geojsonLayer) {
    state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
  }
  closeDetail();
}

async function dissolveSelection() {
  if (state.selectedLSOAs.size === 0) return;
  setOverlay(true, `Dissolving ${state.selectedLSOAs.size} areas…`);

  try {
    const res = await fetch(`${API}/selection/dissolve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lsoa_codes: [...state.selectedLSOAs] }),
    });
    const result = await res.json();
    state.dissolveResult = result;

    // Remove old dissolved layer
    if (state.dissolvedLayer) {
      state.map.removeLayer(state.dissolvedLayer);
    }

    // Add dissolved boundary to map
    state.dissolvedLayer = L.geoJSON(result, {
      style: {
        fillColor: 'transparent',
        color: '#00ffd5',
        weight: 3,
        opacity: 1,
        dashArray: '8,4',
      },
    }).addTo(state.map);

    // Open right panel with dissolve results
    showDissolvePanel(result);
    setOverlay(false);
  } catch (e) {
    console.error('Dissolve error:', e);
    setOverlay(false);
  }
}

function showDissolvePanel(result) {
  const panel = document.getElementById('sidebar-right');
  panel.classList.add('open');
  document.getElementById('detail-title').textContent = 'Selection Boundary';

  const body = document.getElementById('detail-body');
  const props = result.properties || {};
  const contiguous = props.contiguous;
  const badge = contiguous
    ? '<span class="contiguity-badge connected">Connected</span>'
    : `<span class="contiguity-badge disconnected">${props.components} components</span>`;

  body.innerHTML = `
    <div class="selection-summary">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <span style="font-size:12px;font-weight:600;color:var(--text-primary)">${props.lsoa_count} areas</span>
        ${badge}
      </div>
      <div class="selection-stat-grid">
        <div class="selection-stat">
          <div class="selection-stat-label">Area</div>
          <div class="selection-stat-value">${props.area_km2?.toFixed(2) || '—'} km²</div>
        </div>
        <div class="selection-stat">
          <div class="selection-stat-label">Perimeter</div>
          <div class="selection-stat-value">${props.perimeter_km?.toFixed(2) || '—'} km</div>
        </div>
        <div class="selection-stat">
          <div class="selection-stat-label">Centroid</div>
          <div class="selection-stat-value" style="font-size:10px">${props.centroid ? props.centroid[1].toFixed(4)+', '+props.centroid[0].toFixed(4) : '—'}</div>
        </div>
        <div class="selection-stat">
          <div class="selection-stat-label">Neighbours</div>
          <div class="selection-stat-value">${props.border_neighbours?.length || 0}</div>
        </div>
      </div>
      ${props.missing_codes?.length ? `<div style="font-size:10px;color:var(--accent-warn);margin-top:8px">${props.missing_codes.length} codes not found in geometry index</div>` : ''}
    </div>
    <div style="padding:12px 14px">
      <button class="selection-export-btn" onclick="exportSelection()">⬇ Export dissolved GeoJSON</button>
      <button class="selection-export-btn" style="margin-top:6px" onclick="loadSelectionStats()">📊 Load aggregate statistics</button>
    </div>
    <div id="selection-stats-container"></div>
    <div class="detail-source">Boundary computed via Shapely unary_union</div>
  `;
}

async function loadSelectionStats() {
  if (state.selectedLSOAs.size === 0) return;
  // Ensure the dissolve panel is open
  if (!state.dissolveResult) {
    await dissolveSelection();
  } else {
    // Re-show the panel if it was closed
    showDissolvePanel(state.dissolveResult);
  }
  const container = document.getElementById('selection-stats-container');
  if (!container) return;
  container.innerHTML = '<div class="detail-loading"><div class="loading-spinner"></div>Aggregating…</div>';

  try {
    const res = await fetch(`${API}/selection/aggregate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lsoa_codes: [...state.selectedLSOAs] }),
    });
    const data = await res.json();
    renderSelectionStats(container, data);
  } catch (e) {
    container.innerHTML = '<p style="padding:14px;color:var(--text-muted);font-size:11px">Failed to load stats</p>';
  }
}

function renderSelectionStats(container, data) {
  let html = '<div style="padding:0 14px 14px">';
  html += `<div style="font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;color:var(--text-muted);margin-bottom:8px;padding-top:8px;border-top:1px solid var(--border-subtle)">Aggregate Statistics (${data.selection_size} areas)</div>`;

  // Group by category from datasets
  const byCategory = {};
  for (const [dsId, info] of Object.entries(data.datasets || {})) {
    const dsDef = findDataset(dsId);
    const cat = dsDef ? (state.datasets.categories ? Object.entries(state.datasets.categories).find(([,items]) => items.some(i => i.id === dsId))?.[0] : 'Other') : 'Other';
    if (!byCategory[cat]) byCategory[cat] = [];
    byCategory[cat].push({ id: dsId, ...info });
  }

  for (const [cat, items] of Object.entries(byCategory)) {
    html += `<div style="font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);margin:10px 0 4px">${cat || 'Other'}</div>`;
    for (const item of items) {
      const val = item.value != null ? fmt(item.value) : '—';
      html += `<div class="selection-dataset-row"><span class="selection-dataset-label">${item.label}</span><span class="selection-dataset-value">${val} <span style="font-size:9px;color:var(--text-muted)">${item.unit}</span></span></div>`;
    }
  }
  html += '</div>';
  container.innerHTML = html;
}

function exportSelection() {
  if (!state.dissolveResult) {
    dissolveSelection().then(() => {
      if (state.dissolveResult) doExport();
    });
    return;
  }
  doExport();
}

function doExport() {
  const data = {
    ...state.dissolveResult,
    properties: {
      ...state.dissolveResult.properties,
      selected_lsoa_codes: [...state.selectedLSOAs],
      export_timestamp: new Date().toISOString(),
    },
  };
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/geo+json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `census_selection_${state.selectedLSOAs.size}_areas.geojson`;
  a.click();
  URL.revokeObjectURL(url);
}

// ═══════ Detail Panel ═══════

async function openDetail(code, name, value, dsInfo) {
  document.getElementById('sidebar-right').classList.add('open');
  document.getElementById('detail-title').textContent = name;
  const body = document.getElementById('detail-body');
  body.innerHTML = '<div class="detail-loading"><div class="loading-spinner"></div>Loading data…</div>';
  try {
    const res = await fetch(`${API}/lsoa/detail/${code}`);
    renderDetailPanel(await res.json(), value, dsInfo);
  } catch (e) { body.innerHTML = '<p style="padding:16px;color:var(--text-muted);font-size:12px;">Could not load detail data.</p>'; }
}

function renderDetailPanel(detail, mapValue, dsInfo) {
  const body = document.getElementById('detail-body');
  const isScotland = detail.lsoa_code?.startsWith('S01');
  const codeLabel = isScotland ? 'Data Zone' : 'LSOA Code';
  let html = `<div class="detail-meta"><div class="detail-meta-item"><div class="detail-meta-label">${codeLabel}</div><div class="detail-meta-value" style="font-size:11px">${detail.lsoa_code}</div></div><div class="detail-meta-item"><div class="detail-meta-label">Current View</div><div class="detail-meta-value" style="color:var(--accent)">${mapValue !== undefined ? fmt(mapValue) : '—'} ${dsInfo?.unit||''}</div></div></div>`;
  Object.entries(detail.categories || {}).forEach(([catName, catData]) => {
    const entries = Object.entries(catData);
    if (!entries.length) return;
    const total = entries.reduce((s, [, v]) => s + (v || 0), 0);
    html += `<div class="detail-category"><div class="detail-category-title" onclick="toggleCategory(this)">${catName}</div><div class="detail-rows">`;
    entries.forEach(([label, value]) => {
      const pct = total > 0 ? (value / total) * 100 : 0;
      html += `<div class="detail-row"><span class="detail-row-label" title="${label}">${label}</span><div class="detail-row-bar-wrap"><div class="detail-row-bar" style="width:${Math.round(pct)}%"></div></div><span class="detail-row-value">${pct > 0 ? pct.toFixed(1)+'%' : fmtInt(value)}</span></div>`;
    });
    html += '</div></div>';
  });
  html += `<div class="detail-source">Source: ${detail.source || 'ONS Census 2021'}</div>`;
  body.innerHTML = html;
}

function closeDetail() {
  document.getElementById('sidebar-right').classList.remove('open');
  state.selectedLSOA = null;
  if (state.geojsonLayer) {
    state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
  }
}
window.toggleCategory = function(el) {
  el.classList.toggle('collapsed');
  const rows = el.nextElementSibling;
  if (rows) rows.classList.toggle('collapsed');
};

// ═══════ Event Handlers ═══════

async function onDatasetChange(datasetId, colorScheme) {
  state.currentDataset = datasetId;
  state.currentColorScheme = colorScheme;
  state.selectedLSOA = null;
  // Only close detail panel if there's no active selection
  if (state.selectedLSOAs.size === 0) closeDetail();
  document.querySelectorAll('.dataset-item').forEach(el => el.classList.toggle('active', el.dataset.id === datasetId));
  setOverlay(true, 'Loading dataset…');
  try {
    const ladParam = state.currentLAD ? `?lad_code=${state.currentLAD}` : '';
    const res = await fetch(`${API}/lsoa/data/${datasetId}${ladParam}`);
    const valuesData = await res.json();
    state.currentValues = valuesData.values || {};
    state.currentStats = valuesData.stats || {};
    computeQuantileBreaks();
    updateLegend();
    updateStatsGrid();
    if (state.geojsonLayer) {
      state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
    }
    // Re-assert selection UI in case it was lost
    updateSelectionUI();
    setStatus(`${Object.keys(state.currentValues).length.toLocaleString()} areas`, true);
  } catch (e) { setStatus('Error', false); }
  setOverlay(false);
}

async function onLADChange(e) {
  state.currentLAD = e.target.value;
  state.selectedLSOA = null;
  closeDetail();
  clearSelection();
  await loadData();
}

// ═══════ Utilities ═══════

function setOverlay(show, message) {
  const o = document.getElementById('map-overlay');
  if (show) {
    o.innerHTML = `<div class="loading-spinner"></div><p class="loading-text">${message||'Loading…'}</p>`;
    o.classList.remove('hidden');
  } else o.classList.add('hidden');
}
function setStatus(text, ok) {
  const b = document.getElementById('status-badge');
  b.textContent = text; b.classList.toggle('ok', ok);
}
function fitToBounds() {
  if (state.geojsonLayer) {
    try { const b = state.geojsonLayer.getBounds(); if (b.isValid()) state.map.fitBounds(b, {padding:[20,20]}); } catch(e) {}
  }
}
function findDataset(id) {
  if (!state.datasets.categories) return null;
  for (const items of Object.values(state.datasets.categories)) {
    const f = items.find(i => i.id === id);
    if (f) return f;
  }
  return null;
}
function fmt(v) {
  if (v == null) return '—';
  if (typeof v === 'number') {
    if (v >= 10000) return v.toLocaleString('en-GB', {maximumFractionDigits:0});
    if (v >= 100) return v.toLocaleString('en-GB', {maximumFractionDigits:1});
    return v.toLocaleString('en-GB', {maximumFractionDigits:2});
  }
  return String(v);
}
function fmtInt(v) { return v == null ? '—' : Math.round(v).toLocaleString('en-GB'); }
function showNoDataMessage() {
  const o = document.getElementById('map-overlay');
  o.classList.remove('hidden');
  o.innerHTML = '<div style="text-align:center;padding:20px;max-width:380px"><div style="font-size:32px;margin-bottom:12px;opacity:0.4">⊙</div><p style="color:var(--text-primary);font-weight:600;margin-bottom:8px">No boundaries loaded</p><p style="color:var(--text-secondary);font-size:12px;line-height:1.6">Boundary data not yet loaded. Try refreshing.</p></div>';
}
