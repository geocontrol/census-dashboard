function renderCoverageBadge(coverage) {
  const tiers = {
    uk:      { cls: 'uk',      label: 'UK',     title: 'England, Wales, Scotland &amp; Northern Ireland' },
    gb:      { cls: 'gb',      label: 'GB',     title: 'England, Wales &amp; Scotland' },
    ni_only: { cls: 'ni-only', label: 'NI',     title: 'Northern Ireland only' },
    ew:      { cls: 'ew',      label: 'E&amp;W', title: 'England &amp; Wales only' },
  };
  const tier = tiers[coverage] || tiers.ew;
  return `<span class="dataset-coverage ${tier.cls}" title="${tier.title}">${tier.label}</span>`;
}

function renderDatasetList(categories) {
  const container = document.getElementById('dataset-categories');
  container.innerHTML = '';
  Object.entries(categories).forEach(([category, items]) => {
    const header = document.createElement('div');
    header.className = 'dataset-category-header';
    header.textContent = category;
    container.appendChild(header);
    items.forEach(item => {
      const el = document.createElement('div');
      el.className = 'dataset-item' + (item.id === state.currentDataset ? ' active' : '');
      el.dataset.id = item.id;
      const coverageBadge = renderCoverageBadge(item.coverage);
      el.innerHTML = `<div class="dataset-dot"></div><span class="dataset-label">${item.label}</span>${coverageBadge}<span class="dataset-unit">${item.unit}</span>`;
      el.addEventListener('click', () => onDatasetChange(item.id, item.color_scheme));
      container.appendChild(el);
    });
  });
}

function renderMap() {
  if (state.geojsonLayer) state.map.removeLayer(state.geojsonLayer);
  if (!state.geojsonData || !state.geojsonData.features?.length) {
    showNoDataMessage();
    return;
  }
  state.geojsonLayer = L.geoJSON(state.geojsonData, {
    style: feature => styleFeature(feature),
    onEachFeature: (feature, layer) => {
      layer.on({
        mouseover: e => onFeatureHover(e, feature),
        mouseout: e => onFeatureOut(e),
        click: e => onFeatureClick(e, feature),
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
    return { fillColor: fill, fillOpacity: 0.85, color: '#00ffd5', weight: 2.5, opacity: 1 };
  }
  const isFocused = code === state.selectedLSOA;
  return {
    fillColor: fill,
    fillOpacity: isFocused ? 0.95 : 0.75,
    color: isFocused ? '#ffffff' : '#0f1117',
    weight: isFocused ? 1.5 : 0.3,
    opacity: 1,
  };
}

function getColour(value) {
  const breaks = state.quantileBreaks;
  const colours = COLOUR_SCHEMES[state.currentColorScheme] || COLOUR_SCHEMES.YlOrRd;
  if (!breaks.length) return colours[3];
  for (let i = 0; i < breaks.length; i++) {
    if (value <= breaks[i]) return colours[i];
  }
  return colours[colours.length - 1];
}

function computeQuantileBreaks() {
  const values = Object.values(state.currentValues).filter(v => v != null);
  if (!values.length) return;
  values.sort((a, b) => a - b);
  state.quantileBreaks = [];
  for (let i = 1; i <= 7; i++) {
    state.quantileBreaks.push(values[Math.max(0, Math.floor((i / 7) * values.length) - 1)]);
  }
}

function updateLegend() {
  const colours = COLOUR_SCHEMES[state.currentColorScheme] || COLOUR_SCHEMES.YlOrRd;
  document.getElementById('legend-gradient').style.background = `linear-gradient(to right, ${colours.join(', ')})`;
  const stats = state.currentStats;
  if (stats.min !== undefined) {
    document.getElementById('legend-labels').innerHTML = `<span>${fmt(stats.min)}</span><span>${fmt(stats.p50)}</span><span>${fmt(stats.max)}</span>`;
  }
}

function updateStatsGrid() {
  const stats = state.currentStats;
  const grid = document.getElementById('stats-grid');
  if (!stats || stats.min === undefined) {
    grid.innerHTML = '';
    return;
  }
  grid.innerHTML = [['Min', fmt(stats.min)], ['Max', fmt(stats.max)], ['Median', fmt(stats.p50)], ['Mean', fmt(stats.mean)], ['P25', fmt(stats.p25)], ['P75', fmt(stats.p75)]]
    .map(([label, value]) => `<div class="stat-cell"><div class="stat-cell-label">${label}</div><div class="stat-cell-value">${value}</div></div>`)
    .join('');
}

function renderDetailPanel(detail, mapValue, dsInfo) {
  const body = document.getElementById('detail-body');
  const isScotland = detail.lsoa_code?.startsWith('S01');
  const codeLabel = isScotland ? 'Data Zone' : 'LSOA Code';
  const usePrecomputedPercentages = !!detail.precomputed_percentages;
  let html = `<div class="detail-meta"><div class="detail-meta-item"><div class="detail-meta-label">${codeLabel}</div><div class="detail-meta-value" style="font-size:11px">${detail.lsoa_code}</div></div><div class="detail-meta-item"><div class="detail-meta-label">Current View</div><div class="detail-meta-value" style="color:var(--accent)">${mapValue !== undefined ? fmt(mapValue) : '—'} ${dsInfo?.unit||''}</div></div></div>`;
  Object.entries(detail.categories || {}).forEach(([catName, catData]) => {
    const entries = Object.entries(catData);
    if (!entries.length) return;
    const total = entries.reduce((sum, [, value]) => sum + (value || 0), 0);
    html += `<div class="detail-category"><div class="detail-category-title" onclick="toggleCategory(this)">${catName}</div><div class="detail-rows">`;
    entries.forEach(([label, value]) => {
      const pct = usePrecomputedPercentages ? Math.max(0, Math.min(value || 0, 100)) : (total > 0 ? (value / total) * 100 : 0);
      const valueLabel = usePrecomputedPercentages ? `${fmt(value)}%` : (pct > 0 ? pct.toFixed(1) + '%' : fmtInt(value));
      html += `<div class="detail-row"><span class="detail-row-label" title="${label}">${label}</span><div class="detail-row-bar-wrap"><div class="detail-row-bar" style="width:${Math.round(pct)}%"></div></div><span class="detail-row-value">${valueLabel}</span></div>`;
    });
    html += '</div></div>';
  });
  html += `<div class="detail-source">Source: ${detail.source || 'ONS Census 2021'}</div>`;
  body.innerHTML = html;
}
