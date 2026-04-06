/**
 * UK Census 2021 — LSOA Explorer  (v3)
 * National + LAD views, 32 datasets, dual-resolution boundaries
 */

const API = '/api';

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
  selectedLSOA: null, geojsonData: null,
  boundariesReady: false,
};

// ═══════ Init ═══════

document.addEventListener('DOMContentLoaded', async () => {
  initMap();
  await loadDatasets();
  await loadLADList();
  // Check if national BSC boundaries are ready, then load
  await waitForBoundaries();
  await loadData();
});

function initMap() {
  state.map = L.map('map', { center: [52.5, -1.5], zoom: 7, zoomControl: true, preferCanvas: true });
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO', subdomains: 'abcd', maxZoom: 19,
  }).addTo(state.map);
  document.getElementById('btn-zoom-fit').addEventListener('click', fitToBounds);
  document.getElementById('btn-reset').addEventListener('click', () => state.map.setView([52.5, -1.5], 7));
  document.getElementById('close-detail').addEventListener('click', closeDetail);
}

async function waitForBoundaries() {
  setOverlay(true, 'Loading national LSOA boundaries…');
  for (let i = 0; i < 60; i++) {
    try {
      const res = await fetch(`${API}/health`);
      const data = await res.json();
      if (data.boundaries_ready) { state.boundariesReady = true; return; }
    } catch (e) {}
    await new Promise(r => setTimeout(r, 1000));
    setOverlay(true, `Loading national LSOA boundaries… (${i+1}s)`);
  }
  setOverlay(true, 'Boundaries taking longer than expected. Try refreshing.');
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

  const label = state.currentLAD ? 'Loading LAD data…' : 'Loading national census data…';
  setOverlay(true, label);
  document.getElementById('demo-banner').style.display = 'none';

  try {
    const [valuesRes, boundaryRes] = await Promise.all([
      fetch(`${API}/lsoa/data/${state.currentDataset}${ladParam}`),
      fetch(`${API}/boundaries/lsoa${boundaryParam}`),
    ]);
    const valuesData = await valuesRes.json();
    const boundaryData = await boundaryRes.json();

    state.currentValues = valuesData.values || {};
    state.currentStats = valuesData.stats || {};
    state.geojsonData = boundaryData;

    const dsInfo = findDataset(state.currentDataset);
    if (dsInfo) state.currentColorScheme = dsInfo.color_scheme;

    computeQuantileBreaks();
    updateLegend();
    updateStatsGrid();
    renderMap();

    const n = Object.keys(state.currentValues).length;
    setStatus(`${n.toLocaleString()} LSOAs`, true);
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
    header.className = 'dataset-category-header';
    header.textContent = cat;
    container.appendChild(header);
    items.forEach(item => {
      const el = document.createElement('div');
      el.className = 'dataset-item' + (item.id === state.currentDataset ? ' active' : '');
      el.dataset.id = item.id;
      el.innerHTML = `
        <div class="dataset-dot"></div>
        <span class="dataset-label">${item.label}</span>
        <span class="dataset-unit">${item.unit}</span>
      `;
      el.addEventListener('click', () => onDatasetChange(item.id, item.color_scheme));
      container.appendChild(el);
    });
  });
}

function renderMap() {
  if (state.geojsonLayer) state.map.removeLayer(state.geojsonLayer);
  if (!state.geojsonData || !state.geojsonData.features?.length) {
    showNoDataMessage(); return;
  }
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
  const code = feature.properties.LSOA21CD;
  const value = state.currentValues[code];
  const fill = value !== undefined ? getColour(value) : '#2a3044';
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
  const labels = document.getElementById('legend-labels');
  if (s.min !== undefined) {
    labels.innerHTML = `<span>${fmt(s.min)}</span><span>${fmt(s.p50)}</span><span>${fmt(s.max)}</span>`;
  }
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
  const code = feature.properties.LSOA21CD;
  const name = feature.properties.LSOA21NM || code;
  const value = state.currentValues[code];
  const dsInfo = findDataset(state.currentDataset);
  layer.setStyle({ weight: 1.5, color: '#ffffff', fillOpacity: 0.9 });
  layer.bringToFront();
  if (hoverPopup) state.map.closePopup(hoverPopup);
  hoverPopup = L.popup({ closeButton: false, offset: [0, -4] })
    .setLatLng(e.latlng)
    .setContent(`<div class="lsoa-popup"><div class="lsoa-popup-name">${name}</div><div class="lsoa-popup-code">${code}</div><div class="lsoa-popup-value">${value !== undefined ? fmt(value) : 'No data'}</div><div class="lsoa-popup-unit">${dsInfo?.unit||''}</div></div>`)
    .openOn(state.map);
}
function onFeatureOut(e) {
  if (state.selectedLSOA === e.target.feature?.properties?.LSOA21CD) return;
  e.target.setStyle(styleFeature(e.target.feature));
  if (hoverPopup) { state.map.closePopup(hoverPopup); hoverPopup = null; }
}
function onFeatureClick(e, feature) {
  state.selectedLSOA = feature.properties.LSOA21CD;
  if (state.geojsonLayer) state.geojsonLayer.resetStyle();
  openDetail(feature.properties.LSOA21CD, feature.properties.LSOA21NM || feature.properties.LSOA21CD,
    state.currentValues[feature.properties.LSOA21CD], findDataset(state.currentDataset));
  if (hoverPopup) { state.map.closePopup(hoverPopup); hoverPopup = null; }
}

// ═══════ Detail Panel ═══════

async function openDetail(code, name, value, dsInfo) {
  document.getElementById('sidebar-right').classList.add('open');
  document.getElementById('detail-title').textContent = name;
  const body = document.getElementById('detail-body');
  body.innerHTML = '<div class="detail-loading"><div class="loading-spinner"></div>Loading data…</div>';
  try {
    const res = await fetch(`${API}/lsoa/detail/${code}`);
    const detail = await res.json();
    renderDetailPanel(detail, value, dsInfo);
  } catch (e) { body.innerHTML = '<p style="padding:16px;color:var(--text-muted);font-size:12px;">Could not load detail data.</p>'; }
}

function renderDetailPanel(detail, mapValue, dsInfo) {
  const body = document.getElementById('detail-body');
  let html = `<div class="detail-meta"><div class="detail-meta-item"><div class="detail-meta-label">LSOA Code</div><div class="detail-meta-value" style="font-size:11px">${detail.lsoa_code}</div></div><div class="detail-meta-item"><div class="detail-meta-label">Current View</div><div class="detail-meta-value" style="color:var(--accent)">${mapValue !== undefined ? fmt(mapValue) : '—'} ${dsInfo?.unit||''}</div></div></div>`;
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
  if (state.geojsonLayer) state.geojsonLayer.resetStyle();
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
  closeDetail();
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
    if (state.geojsonLayer) state.geojsonLayer.setStyle(f => styleFeature(f));
    setStatus(`${Object.keys(state.currentValues).length.toLocaleString()} LSOAs`, true);
  } catch (e) { setStatus('Error', false); }
  setOverlay(false);
}

async function onLADChange(e) {
  state.currentLAD = e.target.value;
  state.selectedLSOA = null;
  closeDetail();
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
  o.innerHTML = '<div style="text-align:center;padding:20px;max-width:380px"><div style="font-size:32px;margin-bottom:12px;opacity:0.4">⊙</div><p style="color:var(--text-primary);font-weight:600;margin-bottom:8px">No boundaries loaded</p><p style="color:var(--text-secondary);font-size:12px;line-height:1.6">The boundary service did not return LSOA shapes. Try refreshing or selecting a different area.</p></div>';
}
