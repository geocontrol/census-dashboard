const explorerState = {
  datasetIds: [],
  rows: [],
  columnLabels: {},
  columnStats: {},
  totalRows: 0,
  sortBy: null,
  sortDir: 'asc',
  offset: 0,
  limit: 100,
  filterNation: '',
  search: '',
  loading: false,
  searchDebounce: null,
  initialised: false,
};

function setAppView(view) {
  const explorer = document.getElementById('explorer-container');
  const mapBtn = document.getElementById('btn-view-map');
  const tableBtn = document.getElementById('btn-view-table');
  const isTable = view === 'table';

  explorer.hidden = !isTable;
  explorer.style.display = isTable ? 'flex' : 'none';
  mapBtn.classList.toggle('active', !isTable);
  tableBtn.classList.toggle('active', isTable);

  if (isTable) {
    if (!explorerState.initialised) initExplorer();
    if (explorerState.datasetIds.length === 0 && state.currentDataset) {
      explorerState.datasetIds = [state.currentDataset];
    }
    loadExplorerData();
  } else if (state.map) {
    setTimeout(() => state.map.invalidateSize(), 50);
  }
}

function initExplorer() {
  explorerState.initialised = true;
  const addSel = document.getElementById('explorer-add-dataset');
  addSel.innerHTML = '<option value="">— add dataset column —</option>';
  if (state.datasets.categories) {
    for (const [category, items] of Object.entries(state.datasets.categories)) {
      const group = document.createElement('optgroup');
      group.label = category;
      for (const item of items) {
        const opt = document.createElement('option');
        opt.value = item.id;
        opt.textContent = `${item.label} (${item.unit})`;
        group.appendChild(opt);
      }
      addSel.appendChild(group);
    }
  }
  addSel.addEventListener('change', e => {
    const id = e.target.value;
    if (id) explorerAddDataset(id);
    e.target.value = '';
  });

  document.getElementById('explorer-filter-nation').addEventListener('change', e => {
    explorerState.filterNation = e.target.value;
    explorerState.offset = 0;
    loadExplorerData();
  });

  document.getElementById('explorer-search').addEventListener('input', e => {
    clearTimeout(explorerState.searchDebounce);
    const value = e.target.value;
    explorerState.searchDebounce = setTimeout(() => {
      explorerState.search = value;
      explorerState.offset = 0;
      loadExplorerData();
    }, 250);
  });

  document.getElementById('explorer-page-size').addEventListener('change', e => {
    explorerState.limit = parseInt(e.target.value, 10) || 100;
    explorerState.offset = 0;
    loadExplorerData();
  });

  document.getElementById('btn-explorer-export').addEventListener('click', exportExplorerCsv);
}

function explorerOnDatasetChange(prevId, newId) {
  const explorer = document.getElementById('explorer-container');
  if (!explorer || explorer.hidden) return;
  if (!newId) return;
  if (explorerState.datasetIds.includes(newId)) {
    loadExplorerData();
    return;
  }
  const idx = explorerState.datasetIds.indexOf(prevId);
  if (idx >= 0) {
    explorerState.datasetIds[idx] = newId;
    if (explorerState.sortBy === prevId) explorerState.sortBy = newId;
  } else {
    explorerState.datasetIds.unshift(newId);
  }
  explorerState.offset = 0;
  loadExplorerData();
}

function explorerAddDataset(id) {
  if (explorerState.datasetIds.includes(id)) return;
  if (explorerState.datasetIds.length >= 10) {
    alert('Maximum 10 dataset columns.');
    return;
  }
  explorerState.datasetIds.push(id);
  loadExplorerData();
}

function explorerRemoveDataset(id) {
  explorerState.datasetIds = explorerState.datasetIds.filter(d => d !== id);
  if (explorerState.sortBy === id) explorerState.sortBy = null;
  if (explorerState.datasetIds.length === 0) {
    renderExplorer({ columns: ['area_code', 'area_name', 'nation'], column_labels: {}, rows: [], stats: {}, total_rows: 0, offset: 0, limit: explorerState.limit });
    return;
  }
  loadExplorerData();
}

function explorerSort(colId) {
  if (explorerState.sortBy === colId) {
    explorerState.sortDir = explorerState.sortDir === 'asc' ? 'desc' : 'asc';
  } else {
    explorerState.sortBy = colId;
    explorerState.sortDir = 'asc';
  }
  explorerState.offset = 0;
  loadExplorerData();
}

function explorerBuildQuery(extra = {}) {
  const params = new URLSearchParams();
  params.set('datasets', explorerState.datasetIds.join(','));
  if (state.currentLAD) params.set('lad_code', state.currentLAD);
  if (explorerState.filterNation) params.set('filter_nation', explorerState.filterNation);
  if (explorerState.search) params.set('search', explorerState.search);
  if (explorerState.sortBy) {
    params.set('sort_by', explorerState.sortBy);
    params.set('sort_dir', explorerState.sortDir);
  }
  for (const [k, v] of Object.entries(extra)) params.set(k, v);
  return params;
}

async function loadExplorerData() {
  if (explorerState.datasetIds.length === 0) {
    renderExplorer({ columns: ['area_code', 'area_name', 'nation'], column_labels: {}, rows: [], stats: {}, total_rows: 0, offset: 0, limit: explorerState.limit });
    return;
  }
  explorerState.loading = true;
  renderExplorerLoading();
  const params = explorerBuildQuery({ offset: explorerState.offset, limit: explorerState.limit });
  try {
    const res = await fetch(`${API}/explorer/data?${params.toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    explorerState.rows = data.rows;
    explorerState.columnLabels = data.column_labels;
    explorerState.columnStats = data.stats || {};
    explorerState.totalRows = data.total_rows;
    renderExplorer(data);
  } catch (e) {
    console.error('Explorer load error:', e);
    document.getElementById('explorer-tbody').innerHTML = '<tr><td colspan="99" class="explorer-empty">Failed to load data</td></tr>';
  }
  explorerState.loading = false;
}

function renderExplorerLoading() {
  document.getElementById('explorer-tbody').innerHTML = '<tr><td colspan="99" class="explorer-empty"><div class="loading-spinner" style="width:20px;height:20px;margin:20px auto"></div></td></tr>';
}

function renderExplorer(data) {
  renderExplorerHead(data);
  renderExplorerBody(data);
  renderExplorerSummary(data);
  renderExplorerPagination(data);
}

function renderExplorerHead(data) {
  const thead = document.getElementById('explorer-thead');
  const cols = data.columns || [];
  const parts = [];
  for (const col of cols) {
    const isDataset = explorerState.datasetIds.includes(col);
    const label = data.column_labels?.[col] || col;
    const sortIndicator = explorerState.sortBy === col
      ? (explorerState.sortDir === 'asc' ? ' ▲' : ' ▼')
      : '';
    const removeBtn = isDataset
      ? ` <button class="explorer-col-remove" data-id="${col}" title="Remove column">×</button>`
      : '';
    parts.push(`<th data-col="${col}" class="explorer-sortable"><span>${label}${sortIndicator}</span>${removeBtn}</th>`);
  }
  thead.innerHTML = `<tr>${parts.join('')}</tr>`;
  thead.querySelectorAll('.explorer-sortable').forEach(th => {
    th.addEventListener('click', e => {
      if (e.target.closest('.explorer-col-remove')) return;
      explorerSort(th.dataset.col);
    });
  });
  thead.querySelectorAll('.explorer-col-remove').forEach(btn => {
    btn.addEventListener('click', e => {
      e.stopPropagation();
      explorerRemoveDataset(btn.dataset.id);
    });
  });
}

function renderExplorerBody(data) {
  const tbody = document.getElementById('explorer-tbody');
  const cols = data.columns || [];
  if (!data.rows.length) {
    tbody.innerHTML = `<tr><td colspan="${cols.length || 1}" class="explorer-empty">No rows match the current filters.</td></tr>`;
    return;
  }
  const parts = [];
  for (const row of data.rows) {
    const cells = cols.map(col => {
      const value = row[col];
      if (col === 'area_code') {
        return `<td class="explorer-cell-code"><button class="explorer-code-link" data-code="${value}" data-name="${(row.area_name || '').replace(/"/g, '&quot;')}">${value}</button></td>`;
      }
      if (col === 'area_name') return `<td class="explorer-cell-name">${value || ''}</td>`;
      if (col === 'nation') return `<td class="explorer-cell-nation">${value || ''}</td>`;
      if (value == null) return '<td class="explorer-cell-null">—</td>';
      return `<td class="explorer-cell-num">${fmt(value)}</td>`;
    });
    parts.push(`<tr>${cells.join('')}</tr>`);
  }
  tbody.innerHTML = parts.join('');
  tbody.querySelectorAll('.explorer-code-link').forEach(btn => {
    btn.addEventListener('click', () => {
      const code = btn.dataset.code;
      const name = btn.dataset.name || code;
      openDetail(code, name, state.currentValues[code], findDataset(state.currentDataset));
    });
  });
}

function renderExplorerSummary(data) {
  const el = document.getElementById('explorer-summary');
  if (!explorerState.datasetIds.length) {
    el.innerHTML = '<span class="explorer-hint">Add at least one dataset column to begin.</span>';
    return;
  }
  const parts = [`<span class="explorer-total">${data.total_rows.toLocaleString('en-GB')} areas</span>`];
  for (const id of explorerState.datasetIds) {
    const stats = data.stats?.[id];
    const label = data.column_labels?.[id] || id;
    if (stats) {
      parts.push(`<span class="explorer-stat"><strong>${label}</strong> · min ${fmt(stats.min)} · med ${fmt(stats.p50)} · max ${fmt(stats.max)} · mean ${fmt(stats.mean)}</span>`);
    }
  }
  el.innerHTML = parts.join('');
}

function renderExplorerPagination(data) {
  const el = document.getElementById('explorer-pagination');
  const total = data.total_rows || 0;
  const limit = explorerState.limit;
  const offset = explorerState.offset;
  if (total === 0) { el.innerHTML = ''; return; }
  const start = offset + 1;
  const end = Math.min(offset + limit, total);
  const hasPrev = offset > 0;
  const hasNext = end < total;
  el.innerHTML = `
    <button class="explorer-btn" ${hasPrev ? '' : 'disabled'} id="explorer-prev">← Prev</button>
    <span class="explorer-page-info">${start.toLocaleString('en-GB')}–${end.toLocaleString('en-GB')} of ${total.toLocaleString('en-GB')}</span>
    <button class="explorer-btn" ${hasNext ? '' : 'disabled'} id="explorer-next">Next →</button>
  `;
  if (hasPrev) document.getElementById('explorer-prev').addEventListener('click', () => {
    explorerState.offset = Math.max(0, offset - limit);
    loadExplorerData();
  });
  if (hasNext) document.getElementById('explorer-next').addEventListener('click', () => {
    explorerState.offset = offset + limit;
    loadExplorerData();
  });
}

function exportExplorerCsv() {
  if (!explorerState.datasetIds.length) return;
  const params = explorerBuildQuery();
  const url = `${API}/explorer/export?${params.toString()}`;
  const a = document.createElement('a');
  a.href = url;
  a.download = '';
  a.click();
}
