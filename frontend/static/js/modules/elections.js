/* ══════════════════════════════════════════════
   elections.js — Election overlay module
   Phase A: GE 2024 constituency results
   Phase B: Local Elections 2024 (England)
   ══════════════════════════════════════════════ */

const PARTY_COLOURS = {
  Lab:   { name: 'Labour',          colour: '#E4003B' },
  Con:   { name: 'Conservative',    colour: '#0087DC' },
  LD:    { name: 'Liberal Democrat',colour: '#FAA61A' },
  RUK:   { name: 'Reform UK',       colour: '#12B6CF' },
  Green: { name: 'Green',           colour: '#00B140' },
  SNP:   { name: 'SNP',             colour: '#FDF38E' },
  PC:    { name: 'Plaid Cymru',     colour: '#005B54' },
  SF:    { name: 'Sinn Féin',       colour: '#326760' },
  DUP:   { name: 'DUP',             colour: '#D46A4C' },
  SDLP:  { name: 'SDLP',            colour: '#2AA82C' },
  APNI:  { name: 'Alliance',        colour: '#F6CB2F' },
  Alliance: { name: 'Alliance',     colour: '#F6CB2F' },
  UUP:   { name: 'UUP',             colour: '#48A5EE' },
  TUV:   { name: 'TUV',             colour: '#0C3A6A' },
  UKIP:  { name: 'UKIP',            colour: '#6D3177' },
  WPB:   { name: "Workers' Party",  colour: '#8B0000' },
  Ind:   { name: 'Independent',     colour: '#AAAAAA' },
  Spk:   { name: 'Speaker',         colour: '#909090' },
  Other: { name: 'Other',           colour: '#666666' },
};

function _partyColour(abbr) {
  return (PARTY_COLOURS[abbr] || {}).colour || '#666666';
}

function _partyName(abbr) {
  return (PARTY_COLOURS[abbr] || {}).name || abbr;
}

// ── Sequential colour scale (turnout / majority) ───────────────────────────
const _SEQ_COLOURS = ['#c6dbef','#9ecae1','#6baed6','#4292c6','#2171b5','#084594'];

function _seqColour(value, min, max) {
  const t = max > min ? (value - min) / (max - min) : 0;
  return _SEQ_COLOURS[Math.min(_SEQ_COLOURS.length - 1, Math.floor(t * _SEQ_COLOURS.length))];
}

// ── Diverging swing scale (blue = Con gain, red = Lab gain) ───────────────
// 7-step: strong Con → neutral → strong Lab
const _SWING_COLOURS = ['#084594','#4292c6','#9ecae1','#e0e0e0','#fc8d59','#d7301f','#7f0000'];

function _swingColour(value) {
  // value in pp — clamp to ±10 for full scale
  const clamped = Math.max(-10, Math.min(10, value));
  const t = (clamped + 10) / 20;  // 0 = Con +10, 1 = Lab +10
  const idx = Math.min(_SWING_COLOURS.length - 1, Math.floor(t * _SWING_COLOURS.length));
  return _SWING_COLOURS[idx];
}

function _computeRange(features, accessor) {
  let min = Infinity, max = -Infinity;
  for (const f of features) {
    const v = accessor(f.properties);
    if (v != null && isFinite(v)) { if (v < min) min = v; if (v > max) max = v; }
  }
  return { min, max };
}

// ── Style ──────────────────────────────────────────────────────────────────
function styleElectionFeature(feature) {
  const props = feature.properties || {};
  const hasResult = !!props.first_party;

  // Wards/constituencies with no results — render as near-invisible
  if (!hasResult) {
    return { fillColor: '#333344', fillOpacity: 0.08, color: '#222233', weight: 0.3, opacity: 0.4 };
  }

  let fill = '#555566';

  if (state.electionMetric === 'winner') {
    fill = _partyColour(props.first_party);
  } else if (state.electionMetric === 'turnout' && state.electionData) {
    const { min, max } = _computeRange(state.electionData.features, p => p.turnout);
    fill = props.turnout != null ? _seqColour(props.turnout, min, max) : '#333';
  } else if (state.electionMetric === 'majority' && state.electionData) {
    const { min, max } = _computeRange(state.electionData.features, p => p.majority_pct);
    fill = props.majority_pct != null ? _seqColour(props.majority_pct, min, max) : '#333';
  } else if (state.electionMetric === 'swing') {
    fill = props.swing_to_lab != null ? _swingColour(props.swing_to_lab) : '#444';
  }

  return { fillColor: fill, fillOpacity: 0.48, color: '#1a1a2e', weight: 0.5, opacity: 0.8 };
}

// ── Hover popup ────────────────────────────────────────────────────────────
let _electionHoverPopup = null;

function _onElectionHover(e, feature) {
  const props = feature.properties || {};
  if (!props.first_party) return;  // skip no-data wards

  const isLocal = state.electionMode === 'local';
  const areaName = isLocal
    ? (props.WD24NM || props.ward_name || 'Ward')
    : (props.PCON24NM || props.constituency_name || 'Constituency');
  const areaCode = isLocal ? (props.WD24CD || '') : (props.PCON24CD || '');

  const party = props.first_party;
  const colour = _partyColour(party);
  const partyName = _partyName(party);
  const turnout = props.turnout != null ? props.turnout.toFixed(1) + '%' : '—';

  let secondLine = `Turnout ${turnout}`;
  if (state.electionMetric === 'swing' && props.swing_to_lab != null) {
    const s = props.swing_to_lab;
    secondLine = `Swing ${s >= 0 ? '+' : ''}${s.toFixed(1)}pp ${s >= 0 ? 'to Lab' : 'to Con'}`;
  } else if (isLocal && props.total_seats > 1) {
    secondLine += ` · ${props.total_seats} seats`;
  } else if (!isLocal && props.majority_pct != null) {
    secondLine += ` · Majority ${props.majority_pct.toFixed(1)}%`;
  }

  if (_electionHoverPopup) state.map.closePopup(_electionHoverPopup);
  _electionHoverPopup = L.popup({ closeButton: false, offset: [0, -4] })
    .setLatLng(e.latlng)
    .setContent(
      `<div class="lsoa-popup">` +
      `<div class="lsoa-popup-name">${areaName}</div>` +
      `<div class="lsoa-popup-code" style="color:#8892a8">${areaCode}</div>` +
      `<div style="margin-top:6px;display:flex;align-items:center;gap:6px">` +
      `<span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:${colour}"></span>` +
      `<span style="font-weight:600">${partyName}</span></div>` +
      `<div style="margin-top:4px;font-size:11px;color:#8892a8">${secondLine}</div>` +
      `</div>`
    )
    .openOn(state.map);
}

function _onElectionHoverOut() {
  if (_electionHoverPopup) { state.map.closePopup(_electionHoverPopup); _electionHoverPopup = null; }
}

// ── Click → right sidebar ─────────────────────────────────────────────────
function _onElectionClick(feature) {
  const props = feature.properties || {};
  if (!props.first_party) return;

  const isLocal = state.electionMode === 'local';
  const name = isLocal
    ? (props.WD24NM || props.ward_name || 'Ward')
    : (props.PCON24NM || props.constituency_name || 'Constituency');
  const code = isLocal ? (props.WD24CD || '') : (props.PCON24CD || '');

  document.getElementById('sidebar-right').classList.add('open');
  document.getElementById('detail-title').textContent = name;

  const body = document.getElementById('detail-body');
  const voteShare = props.vote_share || {};
  const voteCounts = props.vote_counts || {};
  const sortedParties = Object.entries(voteShare).sort((a, b) => b[1] - a[1]);

  const barsHtml = sortedParties.map(([abbr, share]) => {
    const colour = _partyColour(abbr);
    const pName = _partyName(abbr);
    const count = voteCounts[abbr] ? voteCounts[abbr].toLocaleString() : '';
    const seatsStr = isLocal && props.seats_by_party && props.seats_by_party[abbr]
      ? ` · ${props.seats_by_party[abbr]} seat${props.seats_by_party[abbr] > 1 ? 's' : ''}`
      : '';
    return `<div class="detail-row">` +
      `<span class="detail-row-label" title="${pName}">${pName}</span>` +
      `<div class="detail-row-bar-wrap">` +
      `<div class="detail-row-bar" style="width:${Math.round(share)}%;background:${colour};opacity:0.85"></div>` +
      `</div>` +
      `<span class="detail-row-value">${share.toFixed(1)}%` +
      `${count ? ` <span style="color:#4d5770;font-size:10px">(${count})</span>` : ''}` +
      `${seatsStr ? `<span style="color:var(--accent-2);font-size:10px">${seatsStr}</span>` : ''}` +
      `</span></div>`;
  }).join('');

  const turnout = props.turnout != null ? props.turnout.toFixed(1) + '%' : '—';
  const electorate = props.electorate != null ? props.electorate.toLocaleString() : '—';
  const electionYear = state.electionYear || '2024';
  const electionLabel = isLocal ? `Local Elections ${electionYear}` : `GE ${electionYear}`;
  const source = isLocal
    ? 'Democracy Club · ONS ward boundaries'
    : 'UK Parliament psephology DB · ONS boundaries';

  let resultRows = '';
  if (isLocal && props.total_seats > 1) {
    resultRows += `<div class="detail-row"><span class="detail-row-label">Seats elected</span><span class="detail-row-value">${props.total_seats}</span></div>`;
  } else if (!isLocal) {
    const majority = props.majority != null ? props.majority.toLocaleString() : '—';
    const majorityPct = props.majority_pct != null ? ` (${props.majority_pct.toFixed(1)}%)` : '';
    resultRows += `<div class="detail-row"><span class="detail-row-label">Majority</span><span class="detail-row-value">${majority}${majorityPct}</span></div>`;
  }
  resultRows += `<div class="detail-row"><span class="detail-row-label">Electorate</span><span class="detail-row-value">${electorate}</span></div>`;

  // Swing section — only for GE 2024 with embedded swing data
  let swingHtml = '';
  if (!isLocal && props.swing_to_lab != null) {
    const s = props.swing_to_lab;
    const swingDir = s >= 0 ? 'to Labour' : 'to Conservative';
    const swingColour = s >= 0 ? '#E4003B' : '#0087DC';
    const prevParty = props.first_party_prev || '';
    const prevName = _partyName(prevParty);
    const prevColour = _partyColour(prevParty);
    const prevVsNew = prevParty !== props.first_party
      ? `<div style="margin-top:4px;font-size:11px;color:var(--text-muted)">` +
        `Held by <span style="color:${prevColour}">${prevName}</span> in 2019</div>`
      : '';

    const changeRows = Object.entries(props.share_changes || {})
      .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
      .slice(0, 6)
      .map(([abbr, delta]) => {
        const colour = _partyColour(abbr);
        const sign = delta >= 0 ? '+' : '';
        const arrow = delta >= 0 ? '▲' : '▼';
        const arrowColour = delta >= 0 ? '#34c98a' : '#f05454';
        return `<div class="detail-row">` +
          `<span class="detail-row-label">${_partyName(abbr)}</span>` +
          `<span class="detail-row-value">` +
          `<span style="color:${arrowColour};font-size:10px">${arrow}</span> ` +
          `${sign}${delta.toFixed(1)}pp</span></div>`;
      }).join('');

    swingHtml =
      `<div class="detail-category">` +
      `<div class="detail-category-title">Swing 2019→2024</div>` +
      `<div class="detail-rows">` +
      `<div class="detail-row">` +
      `<span class="detail-row-label">Butler swing</span>` +
      `<span class="detail-row-value" style="color:${swingColour};font-weight:600">` +
      `${s >= 0 ? '+' : ''}${s.toFixed(1)}pp ${swingDir}</span></div>` +
      `${prevVsNew}` +
      `${changeRows}` +
      `</div></div>`;
  }

  body.innerHTML =
    `<div class="detail-meta">` +
    `<div class="detail-meta-item"><div class="detail-meta-label">Code</div><div class="detail-meta-value" style="font-size:11px">${code}</div></div>` +
    `<div class="detail-meta-item"><div class="detail-meta-label">Turnout</div><div class="detail-meta-value" style="color:var(--accent)">${turnout}</div></div>` +
    `</div>` +
    `<div class="detail-category">` +
    `<div class="detail-category-title">Vote shares — ${electionLabel}</div>` +
    `<div class="detail-rows">${barsHtml}</div>` +
    `</div>` +
    `<div class="detail-category">` +
    `<div class="detail-category-title">Result</div>` +
    `<div class="detail-rows">${resultRows}</div>` +
    `</div>` +
    `${swingHtml}` +
    `<div class="detail-source">Source: ${source}</div>`;
}

// ── Legend ─────────────────────────────────────────────────────────────────
function renderElectionLegend() {
  const container = document.getElementById('election-legend');
  if (!container) return;

  if (state.electionMetric === 'winner') {
    const represented = new Set(
      (state.electionData?.features || [])
        .map(f => f.properties?.first_party)
        .filter(Boolean)
    );
    const items = Object.entries(PARTY_COLOURS)
      .filter(([abbr]) => represented.has(abbr))
      .map(([abbr, meta]) =>
        `<div class="election-legend-item">` +
        `<span class="election-legend-chip" style="background:${meta.colour}"></span>` +
        `<span>${meta.name}</span></div>`
      ).join('');
    container.innerHTML = `<div class="election-legend-grid">${items}</div>`;
  } else if (state.electionMetric === 'swing') {
    container.innerHTML =
      `<div style="font-size:10px;color:var(--text-muted);margin-bottom:4px">Swing 2019→2024</div>` +
      `<div style="height:8px;border-radius:3px;background:linear-gradient(to right,${_SWING_COLOURS.join(',')})"></div>` +
      `<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text-muted);margin-top:2px">` +
      `<span style="color:#4292c6">Con gain</span><span style="color:#d7301f">Lab gain</span></div>`;
  } else {
    const label = state.electionMetric === 'turnout' ? 'Turnout %' : 'Majority %';
    container.innerHTML =
      `<div style="font-size:10px;color:var(--text-muted);margin-bottom:4px">${label}</div>` +
      `<div style="height:8px;border-radius:3px;background:linear-gradient(to right,${_SEQ_COLOURS.join(',')})"></div>` +
      `<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text-muted);margin-top:2px"><span>Low</span><span>High</span></div>`;
  }

  // Note about wards with no elections this cycle
  if (state.electionMode === 'local') {
    container.innerHTML += `<div style="margin-top:8px;font-size:10px;color:var(--text-muted)">Transparent wards had no election in May 2024</div>`;
  }

  container.style.display = 'block';
}

function _hideElectionLegend() {
  const el = document.getElementById('election-legend');
  if (el) el.style.display = 'none';
}

// ── Layer management ───────────────────────────────────────────────────────
async function loadElectionOverlay(mode, year) {
  removeElectionOverlay();
  setOverlay(true, `Loading ${mode === 'local' ? 'ward' : 'constituency'} election data…`);

  const url = `${API}/elections/${mode}/overlay?year=${year}`;
  let geojson;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    geojson = await res.json();
  } catch (e) {
    console.error('Election overlay load failed:', e);
    setOverlay(false);
    return;
  }

  state.electionData = geojson;
  state.electionMode = mode;
  state.electionYear = year;

  if (!state.map.getPane('electionPane')) {
    state.map.createPane('electionPane').style.zIndex = '450';
  }

  // Canvas renderer is essential for local elections (8,396 wards)
  const useCanvas = mode === 'local';
  const rendererOpts = useCanvas ? { renderer: L.canvas({ padding: 0.5 }) } : {};

  state.electionOverlay = L.geoJSON(geojson, {
    ...rendererOpts,
    pane: 'electionPane',
    style: feature => styleElectionFeature(feature),
    onEachFeature: (feature, layer) => {
      layer.on({
        mouseover: e => _onElectionHover(e, feature),
        mouseout: () => _onElectionHoverOut(),
        click: () => _onElectionClick(feature),
      });
    },
  }).addTo(state.map);

  _updateMetricOptions(mode, geojson);
  renderElectionLegend();
  setOverlay(false);
}

function _updateMetricOptions(mode, geojson) {
  const metricSelect = document.getElementById('election-metric');
  if (!metricSelect) return;

  const swingOpt = metricSelect.querySelector('option[value="swing"]');
  const hasSwing = mode === 'ge' && (geojson?.features || []).some(f => f.properties?.swing_to_lab != null);

  if (hasSwing && !swingOpt) {
    const opt = document.createElement('option');
    opt.value = 'swing';
    opt.textContent = 'Swing 2019→2024';
    metricSelect.appendChild(opt);
  } else if (!hasSwing && swingOpt) {
    swingOpt.remove();
    if (state.electionMetric === 'swing') {
      state.electionMetric = 'winner';
      metricSelect.value = 'winner';
    }
  }
}

function removeElectionOverlay() {
  if (state.electionOverlay) { state.map.removeLayer(state.electionOverlay); state.electionOverlay = null; }
  if (_electionHoverPopup) { state.map.closePopup(_electionHoverPopup); _electionHoverPopup = null; }
  state.electionData = null;
  state.electionMode = null;
  state.electionYear = null;
  _hideElectionLegend();
  _updateMetricOptions(null, null);
}

function _refreshElectionStyle() {
  if (!state.electionOverlay) return;
  state.electionOverlay.eachLayer(layer => layer.setStyle(styleElectionFeature(layer.feature)));
  renderElectionLegend();
}

// ── UI wiring ──────────────────────────────────────────────────────────────
async function initElectionControls() {
  const toggle = document.getElementById('election-toggle');
  const electionSelect = document.getElementById('election-year');
  const metricSelect = document.getElementById('election-metric');

  if (!toggle || !electionSelect || !metricSelect) return;

  // Populate from API — value is "type:year" to carry both pieces of info
  try {
    const res = await fetch(`${API}/elections/available`);
    if (res.ok) {
      const data = await res.json();
      electionSelect.innerHTML = '';
      data.elections.forEach(el => {
        const opt = document.createElement('option');
        opt.value = `${el.type}:${el.year}`;
        opt.textContent = el.label;
        electionSelect.appendChild(opt);
      });
    }
  } catch (e) {}

  const _load = async () => {
    const [type, year] = electionSelect.value.split(':');
    await loadElectionOverlay(type, year);
  };

  toggle.addEventListener('change', async () => {
    if (toggle.checked) await _load();
    else removeElectionOverlay();
  });

  electionSelect.addEventListener('change', async () => {
    if (toggle.checked) await _load();
  });

  metricSelect.addEventListener('change', () => {
    state.electionMetric = metricSelect.value;
    _refreshElectionStyle();
  });
}
