/* ══════════════════════════════════════════════
   elections.js — Election overlay module
   Phase A: GE 2024 constituency results
   ══════════════════════════════════════════════ */

const PARTY_COLOURS = {
  Lab:      { name: 'Labour',          colour: '#E4003B' },
  Con:      { name: 'Conservative',    colour: '#0087DC' },
  LD:       { name: 'Liberal Democrat',colour: '#FAA61A' },
  RUK:      { name: 'Reform UK',       colour: '#12B6CF' },
  Green:    { name: 'Green',           colour: '#00B140' },
  SNP:      { name: 'SNP',             colour: '#FDF38E' },
  PC:       { name: 'Plaid Cymru',     colour: '#005B54' },
  SF:       { name: 'Sinn Féin',       colour: '#326760' },
  DUP:      { name: 'DUP',             colour: '#D46A4C' },
  SDLP:     { name: 'SDLP',            colour: '#2AA82C' },
  Alliance: { name: 'Alliance',        colour: '#F6CB2F' },
  UUP:      { name: 'UUP',             colour: '#48A5EE' },
  TUV:      { name: 'TUV',             colour: '#0C3A6A' },
  Ind:      { name: 'Independent',     colour: '#AAAAAA' },
  Spk:      { name: 'Speaker',         colour: '#909090' },
};

function _partyColour(abbr) {
  return (PARTY_COLOURS[abbr] || {}).colour || '#666666';
}

function _partyName(abbr) {
  return (PARTY_COLOURS[abbr] || {}).name || abbr;
}

// ── Turnout / majority colour scale (sequential blue) ──────────────────────
const _SEQ_COLOURS = ['#c6dbef','#9ecae1','#6baed6','#4292c6','#2171b5','#084594'];

function _seqColour(value, min, max) {
  const t = max > min ? (value - min) / (max - min) : 0;
  const idx = Math.min(_SEQ_COLOURS.length - 1, Math.floor(t * _SEQ_COLOURS.length));
  return _SEQ_COLOURS[idx];
}

// ── Metric range computation (for turnout / majority scales) ───────────────
function _computeRange(features, accessor) {
  let min = Infinity, max = -Infinity;
  for (const f of features) {
    const v = accessor(f.properties);
    if (v != null && isFinite(v)) {
      if (v < min) min = v;
      if (v > max) max = v;
    }
  }
  return { min, max };
}

// ── Style function ──────────────────────────────────────────────────────────
function styleElectionFeature(feature) {
  const props = feature.properties || {};
  let fill = '#444444';

  if (state.electionMetric === 'winner') {
    fill = _partyColour(props.first_party);
  } else if (state.electionMetric === 'turnout' && state.electionData) {
    const { min, max } = _computeRange(state.electionData.features, p => p.turnout);
    fill = props.turnout != null ? _seqColour(props.turnout, min, max) : '#333';
  } else if (state.electionMetric === 'majority' && state.electionData) {
    const { min, max } = _computeRange(state.electionData.features, p => p.majority_pct);
    fill = props.majority_pct != null ? _seqColour(props.majority_pct, min, max) : '#333';
  }

  return { fillColor: fill, fillOpacity: 0.48, color: '#1a1a2e', weight: 0.8, opacity: 0.9 };
}

// ── Hover popup ─────────────────────────────────────────────────────────────
let _electionHoverPopup = null;

function _onElectionHover(e, feature) {
  const props = feature.properties || {};
  const party = props.first_party || '?';
  const partyName = _partyName(party);
  const colour = _partyColour(party);
  const turnout = props.turnout != null ? props.turnout.toFixed(1) + '%' : '—';
  const majority = props.majority_pct != null ? props.majority_pct.toFixed(1) + '%' : '—';

  if (_electionHoverPopup) state.map.closePopup(_electionHoverPopup);
  _electionHoverPopup = L.popup({ closeButton: false, offset: [0, -4] })
    .setLatLng(e.latlng)
    .setContent(
      `<div class="lsoa-popup">` +
      `<div class="lsoa-popup-name">${props.PCON24NM || props.constituency_name || 'Constituency'}</div>` +
      `<div class="lsoa-popup-code" style="color:#8892a8">${props.PCON24CD || ''}</div>` +
      `<div style="margin-top:6px;display:flex;align-items:center;gap:6px">` +
      `<span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:${colour}"></span>` +
      `<span style="font-weight:600">${partyName}</span></div>` +
      `<div style="margin-top:4px;font-size:11px;color:#8892a8">Turnout ${turnout} · Majority ${majority}</div>` +
      `</div>`
    )
    .openOn(state.map);
}

function _onElectionHoverOut() {
  if (_electionHoverPopup) {
    state.map.closePopup(_electionHoverPopup);
    _electionHoverPopup = null;
  }
}

// ── Click → show results in right sidebar ──────────────────────────────────
function _onElectionClick(feature) {
  const props = feature.properties || {};
  const name = props.PCON24NM || props.constituency_name || 'Constituency';
  const code = props.PCON24CD || '';

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
    return `<div class="detail-row">` +
      `<span class="detail-row-label" title="${pName}">${pName}</span>` +
      `<div class="detail-row-bar-wrap">` +
      `<div class="detail-row-bar" style="width:${Math.round(share)}%;background:${colour};opacity:0.85"></div>` +
      `</div>` +
      `<span class="detail-row-value">${share.toFixed(1)}%${count ? ' <span style="color:#4d5770;font-size:10px">('+count+')</span>' : ''}</span>` +
      `</div>`;
  }).join('');

  const turnout = props.turnout != null ? props.turnout.toFixed(1) + '%' : '—';
  const majority = props.majority != null ? props.majority.toLocaleString() : '—';
  const majorityPct = props.majority_pct != null ? ' (' + props.majority_pct.toFixed(1) + '%)' : '';
  const electorate = props.electorate != null ? props.electorate.toLocaleString() : '—';

  body.innerHTML =
    `<div class="detail-meta">` +
    `<div class="detail-meta-item"><div class="detail-meta-label">Code</div><div class="detail-meta-value" style="font-size:11px">${code}</div></div>` +
    `<div class="detail-meta-item"><div class="detail-meta-label">Turnout</div><div class="detail-meta-value" style="color:var(--accent)">${turnout}</div></div>` +
    `</div>` +
    `<div class="detail-category">` +
    `<div class="detail-category-title">Vote shares — GE 2024</div>` +
    `<div class="detail-rows">${barsHtml}</div>` +
    `</div>` +
    `<div class="detail-category">` +
    `<div class="detail-category-title">Result</div>` +
    `<div class="detail-rows">` +
    `<div class="detail-row"><span class="detail-row-label">Majority</span><span class="detail-row-value">${majority}${majorityPct}</span></div>` +
    `<div class="detail-row"><span class="detail-row-label">Electorate</span><span class="detail-row-value">${electorate}</span></div>` +
    `</div></div>` +
    `<div class="detail-source">Source: House of Commons Library CBP-10009 · ONS boundaries</div>`;
}

// ── Legend ──────────────────────────────────────────────────────────────────
function renderElectionLegend() {
  const container = document.getElementById('election-legend');
  if (!container) return;

  if (state.electionMetric === 'winner') {
    const represented = new Set(
      (state.electionData?.features || []).map(f => f.properties?.first_party).filter(Boolean)
    );
    const items = Object.entries(PARTY_COLOURS)
      .filter(([abbr]) => represented.has(abbr))
      .map(([abbr, meta]) =>
        `<div class="election-legend-item">` +
        `<span class="election-legend-chip" style="background:${meta.colour}"></span>` +
        `<span>${meta.name}</span>` +
        `</div>`
      ).join('');
    container.innerHTML = `<div class="election-legend-grid">${items}</div>`;
  } else {
    const label = state.electionMetric === 'turnout' ? 'Turnout %' : 'Majority %';
    const gradient = `linear-gradient(to right, ${_SEQ_COLOURS.join(', ')})`;
    container.innerHTML =
      `<div style="font-size:10px;color:var(--text-muted);margin-bottom:4px">${label}</div>` +
      `<div style="height:8px;border-radius:3px;background:${gradient}"></div>` +
      `<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text-muted);margin-top:2px"><span>Low</span><span>High</span></div>`;
  }
  container.style.display = 'block';
}

function _hideElectionLegend() {
  const container = document.getElementById('election-legend');
  if (container) container.style.display = 'none';
}

// ── Layer management ─────────────────────────────────────────────────────────
async function loadElectionOverlay(mode, year) {
  removeElectionOverlay();

  const url = `${API}/elections/${mode}/overlay?year=${year}`;
  let geojson;
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    geojson = await res.json();
  } catch (e) {
    console.error('Election overlay load failed:', e);
    return;
  }

  state.electionData = geojson;
  state.electionMode = mode;
  state.electionYear = year;

  // Dedicated pane above census layer (overlayPane zIndex=400) but below popups
  if (!state.map.getPane('electionPane')) {
    state.map.createPane('electionPane').style.zIndex = '450';
  }

  state.electionOverlay = L.geoJSON(geojson, {
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

  renderElectionLegend();
}

function removeElectionOverlay() {
  if (state.electionOverlay) {
    state.map.removeLayer(state.electionOverlay);
    state.electionOverlay = null;
  }
  if (_electionHoverPopup) {
    state.map.closePopup(_electionHoverPopup);
    _electionHoverPopup = null;
  }
  state.electionData = null;
  state.electionMode = null;
  state.electionYear = null;
  _hideElectionLegend();
}

function _refreshElectionStyle() {
  if (!state.electionOverlay) return;
  state.electionOverlay.eachLayer(layer => layer.setStyle(styleElectionFeature(layer.feature)));
  renderElectionLegend();
}

// ── UI wiring ────────────────────────────────────────────────────────────────
async function initElectionControls() {
  const toggle = document.getElementById('election-toggle');
  const yearSelect = document.getElementById('election-year');
  const metricSelect = document.getElementById('election-metric');

  if (!toggle || !yearSelect || !metricSelect) return;

  // Populate year options from API
  try {
    const res = await fetch(`${API}/elections/available`);
    if (res.ok) {
      const data = await res.json();
      yearSelect.innerHTML = '';
      data.elections.forEach(el => {
        const opt = document.createElement('option');
        opt.value = el.year;
        opt.textContent = el.label;
        yearSelect.appendChild(opt);
      });
    }
  } catch (e) {}

  toggle.addEventListener('change', async () => {
    if (toggle.checked) {
      const year = yearSelect.value;
      await loadElectionOverlay('ge', year);
    } else {
      removeElectionOverlay();
    }
  });

  yearSelect.addEventListener('change', async () => {
    if (toggle.checked) {
      const year = yearSelect.value;
      await loadElectionOverlay('ge', year);
    }
  });

  metricSelect.addEventListener('change', () => {
    state.electionMetric = metricSelect.value;
    _refreshElectionStyle();
  });
}
