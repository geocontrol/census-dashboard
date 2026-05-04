/* ══════════════════════════════════════════════
   Natural Language Query Interface
   ══════════════════════════════════════════════ */

const queryState = {
  confirmedFilters: [],
  pendingFilters: [],
  parsedSummary: '',
};

function initQueryInterface() {
  const input = document.getElementById('query-input');
  const btn   = document.getElementById('btn-query-find');
  const clear = document.getElementById('btn-query-clear');

  btn.addEventListener('click', () => submitQuery(input.value.trim()));
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      submitQuery(input.value.trim());
    }
  });
  clear.addEventListener('click', clearQuery);
}

async function submitQuery(text) {
  if (!text) return;

  _setQueryStatus('loading', 'Analysing query…');
  _clearQueryResults();
  queryState.confirmedFilters = [];
  queryState.pendingFilters   = [];

  try {
    const res = await fetch(`${API}/query/parse`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: text, lad_code: state.currentLAD || null }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || `Server error ${res.status}`);
    }

    const data = await res.json();
    queryState.pendingFilters = data.filters;
    queryState.parsedSummary  = data.parsed_summary;

    if (data.unrecognised_terms && data.unrecognised_terms.length) {
      _setQueryStatus('warn', `Could not recognise: ${data.unrecognised_terms.join(', ')}`);
    } else {
      _setQueryStatus('parsed', data.parsed_summary);
    }

    if (data.clarification_needed) {
      _showClarificationUI(data.filters);
    } else {
      await executeQuery(data.filters);
    }
  } catch (e) {
    _setQueryStatus('error', e.message);
  }
}

function _showClarificationUI(filters) {
  const container = document.getElementById('query-clarification');
  container.innerHTML = '';
  container.style.display = 'block';

  const ambiguous = filters.filter(f => f.vague_term !== null && f.vague_term !== undefined);
  if (!ambiguous.length) return;

  const heading = document.createElement('div');
  heading.className = 'query-clarify-heading';
  heading.textContent = `Clarify ${ambiguous.length} threshold${ambiguous.length > 1 ? 's' : ''}`;
  container.appendChild(heading);

  // Deep copy as confirmed working set — concrete thresholds copy through, nulls await user
  queryState.confirmedFilters = JSON.parse(JSON.stringify(filters));

  ambiguous.forEach(filter => {
    const filterIdx = filters.indexOf(filter);
    const card = document.createElement('div');
    card.className = 'query-clarify-card';

    const operatorText = (filter.operator === '>' || filter.operator === '>=') ? 'above' : 'below';
    const unit = _getFilterUnit(filter);

    card.innerHTML = `
      <div class="query-clarify-label">
        ${filter.label}
        <span class="query-vague-badge">"${filter.vague_term}"</span>
      </div>
      <div class="query-clarify-desc">Values ${operatorText} this threshold will match:</div>
      <div class="query-percentile-btns" id="pctls-${filterIdx}"></div>
      <div class="query-custom-input-row">
        <span class="query-custom-label">Custom:</span>
        <input type="number" class="query-custom-input" id="custom-${filterIdx}"
               placeholder="value" step="0.1" min="0" max="100" />
        <span class="query-custom-unit">${unit}</span>
        <button class="query-custom-apply" data-idx="${filterIdx}">Set</button>
      </div>
      <div class="query-confirmed-value" id="confirmed-${filterIdx}" style="display:none"></div>
    `;

    container.appendChild(card);

    // Percentile buttons
    const btnRow = card.querySelector(`#pctls-${filterIdx}`);
    (filter.threshold_options || []).forEach(opt => {
      const btn = document.createElement('button');
      btn.className = 'query-pctl-btn';
      btn.textContent = opt.label;
      btn.addEventListener('click', () => {
        _confirmThreshold(filterIdx, opt.value);
        _markCardConfirmed(card, filterIdx, opt.label, opt.value, filter.operator);
        _checkAllConfirmed(filters);
        // Deselect siblings, select this
        btnRow.querySelectorAll('.query-pctl-btn').forEach(b => b.classList.remove('selected'));
        btn.classList.add('selected');
      });
      btnRow.appendChild(btn);
    });

    // Custom apply
    card.querySelector('.query-custom-apply').addEventListener('click', () => {
      const val = parseFloat(card.querySelector(`#custom-${filterIdx}`).value);
      if (isNaN(val)) return;
      _confirmThreshold(filterIdx, val);
      _markCardConfirmed(card, filterIdx, `custom: ${val}`, val, filter.operator);
      _checkAllConfirmed(filters);
    });
  });

  // Execute button (disabled until all confirmed)
  const execBtn = document.createElement('button');
  execBtn.id = 'btn-query-execute';
  execBtn.className = 'query-execute-btn';
  execBtn.textContent = 'Find matching areas';
  execBtn.disabled = true;
  execBtn.addEventListener('click', async () => {
    execBtn.disabled = true;
    await executeQuery(queryState.confirmedFilters);
  });
  container.appendChild(execBtn);
}

function _getFilterUnit(filter) {
  if (!filter.is_computed && filter.dataset_id && typeof findDataset === 'function') {
    const ds = findDataset(filter.dataset_id);
    return ds ? (ds.unit || '') : '';
  }
  return '%';
}

function _confirmThreshold(filterIdx, value) {
  if (queryState.confirmedFilters[filterIdx]) {
    queryState.confirmedFilters[filterIdx].threshold = value;
    queryState.confirmedFilters[filterIdx].vague_term = null;
  }
}

function _markCardConfirmed(card, filterIdx, label, value, operator) {
  const badge = card.querySelector(`#confirmed-${filterIdx}`);
  const opText = (operator === '>' || operator === '>=') ? '>' : '<';
  badge.textContent = `${opText} ${value} — ${label}`;
  badge.style.display = 'block';
  card.classList.add('confirmed');
}

function _checkAllConfirmed(filters) {
  const allDone = queryState.confirmedFilters.every(
    f => f.threshold !== null && f.threshold !== undefined
  );
  const execBtn = document.getElementById('btn-query-execute');
  if (execBtn) execBtn.disabled = !allDone;
}

async function executeQuery(confirmedFilters) {
  _setQueryStatus('loading', 'Fetching census data…');

  // Collect unique dataset IDs
  const neededIds = new Set();
  confirmedFilters.forEach(f => {
    if (f.is_computed && f.datasets) {
      f.datasets.forEach(id => neededIds.add(id));
    } else if (f.dataset_id) {
      neededIds.add(f.dataset_id);
    }
  });

  // Fetch all datasets in parallel
  const ladParam = state.currentLAD ? `?lad_code=${state.currentLAD}` : '';
  const fetchPromises = {};
  for (const id of neededIds) {
    fetchPromises[id] = fetch(`${API}/lsoa/data/${id}${ladParam}`)
      .then(r => r.json())
      .then(d => d.values || {});
  }

  const datasetValues = {};
  await Promise.all(
    Object.entries(fetchPromises).map(async ([id, promise]) => {
      datasetValues[id] = await promise;
    })
  );

  // Build union of all LSOA codes across datasets
  const allCodes = new Set();
  Object.values(datasetValues).forEach(vals => {
    Object.keys(vals).forEach(code => allCodes.add(code));
  });

  // Apply AND-filter per LSOA
  const matchingCodes = new Set();

  for (const code of allCodes) {
    let passes = true;

    for (const filter of confirmedFilters) {
      if (filter.threshold === null || filter.threshold === undefined) {
        passes = false;
        break;
      }

      let value;
      if (filter.is_computed && filter.operation === 'sum' && filter.datasets) {
        // Sum values across constituent datasets
        const hasAny = filter.datasets.some(id => datasetValues[id]?.[code] !== undefined);
        if (!hasAny) { passes = false; break; }
        value = filter.datasets.reduce((acc, id) => {
          const v = datasetValues[id]?.[code];
          return v !== undefined ? acc + v : acc;
        }, 0);
      } else if (filter.dataset_id) {
        value = datasetValues[filter.dataset_id]?.[code];
        if (value === undefined) { passes = false; break; }
      } else {
        passes = false;
        break;
      }

      if (!_applyOperator(value, filter.operator, filter.threshold)) {
        passes = false;
        break;
      }
    }

    if (passes) matchingCodes.add(code);
  }

  // Update map selection
  state.selectedLSOAs = matchingCodes;
  if (state.geojsonLayer) {
    state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
  }
  updateSelectionUI();

  _showResultsBanner(matchingCodes.size, queryState.parsedSummary);
  _renderFilterChips(confirmedFilters);
  _setQueryStatus('done', '');

  if (matchingCodes.size > 20000) {
    _setQueryStatus('warn', `${matchingCodes.size.toLocaleString('en-GB')} areas matched — consider a stricter threshold`);
  }
}

function _applyOperator(value, operator, threshold) {
  switch (operator) {
    case '>':  return value >  threshold;
    case '>=': return value >= threshold;
    case '<':  return value <  threshold;
    case '<=': return value <= threshold;
    default:   return false;
  }
}

function _showResultsBanner(count, summary) {
  const banner = document.getElementById('query-results-banner');
  banner.style.display = 'flex';
  document.getElementById('query-result-count').textContent = count.toLocaleString('en-GB');
  document.getElementById('query-result-summary').textContent = summary;
}

function _renderFilterChips(filters) {
  const container = document.getElementById('query-filter-chips');
  container.innerHTML = '';
  filters.forEach(f => {
    if (f.threshold === null || f.threshold === undefined) return;
    const chip = document.createElement('span');
    chip.className = 'query-chip';
    const opText = (f.operator === '>' || f.operator === '>=') ? '>' : '<';
    chip.textContent = `${f.label} ${opText} ${f.threshold.toFixed(1)}`;
    container.appendChild(chip);
  });
}

function clearQuery() {
  queryState.confirmedFilters = [];
  queryState.pendingFilters   = [];
  queryState.parsedSummary    = '';

  state.selectedLSOAs.clear();
  if (state.geojsonLayer) {
    state.geojsonLayer.eachLayer(layer => layer.setStyle(styleFeature(layer.feature)));
  }
  updateSelectionUI();

  document.getElementById('query-results-banner').style.display = 'none';
  document.getElementById('query-clarification').style.display  = 'none';
  document.getElementById('query-clarification').innerHTML = '';
  document.getElementById('query-filter-chips').innerHTML = '';
  document.getElementById('query-input').value = '';
  _setQueryStatus('done', '');
}

function _setQueryStatus(status, message) {
  const el = document.getElementById('query-status');
  if (status === 'done' || !message) {
    el.style.display = 'none';
    return;
  }
  el.className = `query-status query-status--${status}`;
  el.textContent = message;
  el.style.display = 'block';
}

function _clearQueryResults() {
  document.getElementById('query-results-banner').style.display = 'none';
  document.getElementById('query-clarification').style.display  = 'none';
  document.getElementById('query-clarification').innerHTML = '';
  document.getElementById('query-filter-chips').innerHTML = '';
}
