function toggleSelectMode() {
  state.selectMode = !state.selectMode;
  updateSelectionUI();
  if (!state.selectMode && state.selectedLSOAs.size === 0) closeDetail();
}

function updateSelectionUI() {
  const bar = document.getElementById('select-count-bar');
  const count = state.selectedLSOAs.size;
  document.getElementById('select-count').textContent = count;
  bar.classList.toggle('visible', count > 0);
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
    if (state.dissolvedLayer) state.map.removeLayer(state.dissolvedLayer);
    state.dissolvedLayer = L.geoJSON(result, {
      style: { fillColor: 'transparent', color: '#00ffd5', weight: 3, opacity: 1, dashArray: '8,4' },
    }).addTo(state.map);
    showDissolvePanel(result);
  } catch (e) {
    console.error('Dissolve error:', e);
  }
  setOverlay(false);
}

function showDissolvePanel(result) {
  const panel = document.getElementById('sidebar-right');
  panel.classList.add('open');
  document.getElementById('detail-title').textContent = 'Selection Boundary';
  const body = document.getElementById('detail-body');
  const props = result.properties || {};
  const badge = props.contiguous
    ? '<span class="contiguity-badge connected">Connected</span>'
    : `<span class="contiguity-badge disconnected">${props.components} components</span>`;
  body.innerHTML = `
    <div class="selection-summary">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <span style="font-size:12px;font-weight:600;color:var(--text-primary)">${props.lsoa_count} areas</span>
        ${badge}
      </div>
      <div class="selection-stat-grid">
        <div class="selection-stat"><div class="selection-stat-label">Area</div><div class="selection-stat-value">${props.area_km2?.toFixed(2) || '—'} km²</div></div>
        <div class="selection-stat"><div class="selection-stat-label">Perimeter</div><div class="selection-stat-value">${props.perimeter_km?.toFixed(2) || '—'} km</div></div>
        <div class="selection-stat"><div class="selection-stat-label">Centroid</div><div class="selection-stat-value" style="font-size:10px">${props.centroid ? props.centroid[1].toFixed(4) + ', ' + props.centroid[0].toFixed(4) : '—'}</div></div>
        <div class="selection-stat"><div class="selection-stat-label">Neighbours</div><div class="selection-stat-value">${props.border_neighbours?.length || 0}</div></div>
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
  if (!state.dissolveResult) {
    await dissolveSelection();
  } else {
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
    renderSelectionStats(container, await res.json());
  } catch (e) {
    container.innerHTML = '<p style="padding:14px;color:var(--text-muted);font-size:11px">Failed to load stats</p>';
  }
}

function renderSelectionStats(container, data) {
  let html = '<div style="padding:0 14px 14px">';
  html += `<div style="font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:0.8px;color:var(--text-muted);margin-bottom:8px;padding-top:8px;border-top:1px solid var(--border-subtle)">Aggregate Statistics (${data.selection_size} areas)</div>`;
  const byCategory = {};
  for (const [datasetId, info] of Object.entries(data.datasets || {})) {
    const category = state.datasets.categories
      ? Object.entries(state.datasets.categories).find(([, items]) => items.some(item => item.id === datasetId))?.[0] || 'Other'
      : 'Other';
    if (!byCategory[category]) byCategory[category] = [];
    byCategory[category].push({ id: datasetId, ...info });
  }
  for (const [category, items] of Object.entries(byCategory)) {
    html += `<div style="font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;color:var(--text-muted);margin:10px 0 4px">${category || 'Other'}</div>`;
    for (const item of items) {
      const value = item.value != null ? fmt(item.value) : '—';
      html += `<div class="selection-dataset-row"><span class="selection-dataset-label">${item.label}</span><span class="selection-dataset-value">${value} <span style="font-size:9px;color:var(--text-muted)">${item.unit}</span></span></div>`;
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
