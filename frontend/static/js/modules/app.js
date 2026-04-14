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
      if (data.boundaries_ready) {
        state.boundariesReady = true;
        return;
      }
    } catch (e) {}
    await new Promise(resolve => setTimeout(resolve, 1000));
    setOverlay(true, `Loading national boundaries… (${i + 1}s)`);
  }
}

async function loadAdjacency() {
  try {
    const res = await fetch(`${API}/adjacency`);
    if (res.ok) {
      const data = await res.json();
      state.adjacency = data.graph;
    }
  } catch (e) {
    console.warn('Adjacency not yet available');
  }
}

async function loadDatasets() {
  try {
    const res = await fetch(`${API}/datasets`);
    state.datasets = await res.json();
    renderDatasetList(state.datasets.categories);
  } catch (e) {
    setStatus('API Error', false);
  }
}

async function loadLADList() {
  try {
    const res = await fetch(`${API}/lad/list`);
    const data = await res.json();
    const select = document.getElementById('lad-select');
    data.lads.forEach(lad => {
      const option = document.createElement('option');
      option.value = lad.code;
      option.textContent = lad.name;
      select.appendChild(option);
    });
    select.addEventListener('change', onLADChange);
  } catch (e) {}
}

async function loadData() {
  const ladParam = state.currentLAD ? `?lad_code=${state.currentLAD}` : '';
  const resolution = state.currentLAD ? 'bgc' : 'bsc';
  const boundaryParam = state.currentLAD ? `?lad_code=${state.currentLAD}&resolution=${resolution}` : `?resolution=${resolution}`;
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
    const dataset = findDataset(state.currentDataset);
    if (dataset) state.currentColorScheme = dataset.color_scheme;
    computeQuantileBreaks();
    updateLegend();
    updateStatsGrid();
    renderMap();
    setStatus(`${Object.keys(state.currentValues).length.toLocaleString()} areas`, true);
  } catch (e) {
    console.error('Load error:', e);
    setStatus('Error', false);
  }
  setOverlay(false);
}

function onFeatureHover(e, feature) {
  const layer = e.target;
  const code = getFeatureCode(feature);
  const name = getFeatureName(feature);
  const value = state.currentValues[code];
  const dataset = findDataset(state.currentDataset);
  const isInSelection = state.selectedLSOAs.has(code);
  const areaType = isScotlandFeature(feature) ? 'Data Zone' : 'LSOA';
  layer.setStyle(isInSelection ? { weight: 3, color: '#00ffd5', fillOpacity: 0.6 } : { weight: 1.5, color: '#ffffff', fillOpacity: 0.9 });
  layer.bringToFront();
  if (hoverPopup) state.map.closePopup(hoverPopup);
  const selLabel = state.selectMode ? `<div style="font-size:10px;color:#00d2be;margin-top:6px">${isInSelection ? '⊖ Click to deselect' : '⊕ Click to select'}</div>` : '';
  const nationBadge = isScotlandFeature(feature) ? `<div style="font-size:9px;color:#8b97b5;margin-top:2px">${areaType} · Scotland</div>` : '';
  hoverPopup = L.popup({ closeButton: false, offset: [0, -4] })
    .setLatLng(e.latlng)
    .setContent(`<div class="lsoa-popup"><div class="lsoa-popup-name">${name}</div><div class="lsoa-popup-code">${code}</div>${nationBadge}<div class="lsoa-popup-value">${value !== undefined ? fmt(value) : 'No data'}</div><div class="lsoa-popup-unit">${dataset?.unit || ''}</div>${selLabel}</div>`)
    .openOn(state.map);
}

function onFeatureOut(e) {
  const code = e.target.feature ? getFeatureCode(e.target.feature) : '';
  if (state.selectedLSOA === code) return;
  e.target.setStyle(styleFeature(e.target.feature));
  if (hoverPopup) {
    state.map.closePopup(hoverPopup);
    hoverPopup = null;
  }
}

function onFeatureClick(e, feature) {
  const code = getFeatureCode(feature);
  if (hoverPopup) {
    state.map.closePopup(hoverPopup);
    hoverPopup = null;
  }
  if (state.selectMode) {
    if (state.selectedLSOAs.has(code)) state.selectedLSOAs.delete(code);
    else state.selectedLSOAs.add(code);
    state.dissolveResult = null;
    if (state.dissolvedLayer) {
      state.map.removeLayer(state.dissolvedLayer);
      state.dissolvedLayer = null;
    }
    updateSelectionUI();
    if (state.geojsonLayer) state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
    return;
  }
  state.selectedLSOA = code;
  if (state.geojsonLayer) state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
  openDetail(code, getFeatureName(feature), state.currentValues[code], findDataset(state.currentDataset));
}

async function openDetail(code, name, value, dsInfo) {
  document.getElementById('sidebar-right').classList.add('open');
  document.getElementById('detail-title').textContent = name;
  const body = document.getElementById('detail-body');
  body.innerHTML = '<div class="detail-loading"><div class="loading-spinner"></div>Loading data…</div>';
  try {
    const res = await fetch(`${API}/lsoa/detail/${code}`);
    renderDetailPanel(await res.json(), value, dsInfo);
  } catch (e) {
    body.innerHTML = '<p style="padding:16px;color:var(--text-muted);font-size:12px;">Could not load detail data.</p>';
  }
}

function closeDetail() {
  document.getElementById('sidebar-right').classList.remove('open');
  state.selectedLSOA = null;
  if (state.geojsonLayer) state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
}

window.toggleCategory = function(el) {
  el.classList.toggle('collapsed');
  const rows = el.nextElementSibling;
  if (rows) rows.classList.toggle('collapsed');
};

async function onDatasetChange(datasetId, colorScheme) {
  state.currentDataset = datasetId;
  state.currentColorScheme = colorScheme;
  state.selectedLSOA = null;
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
    if (state.geojsonLayer) state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
    updateSelectionUI();
    setStatus(`${Object.keys(state.currentValues).length.toLocaleString()} areas`, true);
  } catch (e) {
    setStatus('Error', false);
  }
  setOverlay(false);
}

async function onLADChange(e) {
  state.currentLAD = e.target.value;
  state.selectedLSOA = null;
  closeDetail();
  clearSelection();
  await loadData();
}

document.addEventListener('DOMContentLoaded', async () => {
  initMap();
  initSelectionUI();
  await loadDatasets();
  await loadLADList();
  await waitForBoundaries();
  await loadData();
  loadAdjacency();
});
