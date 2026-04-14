function setOverlay(show, message) {
  const overlay = document.getElementById('map-overlay');
  if (show) {
    overlay.innerHTML = `<div class="loading-spinner"></div><p class="loading-text">${message || 'Loading…'}</p>`;
    overlay.classList.remove('hidden');
  } else {
    overlay.classList.add('hidden');
  }
}

function setStatus(text, ok) {
  const badge = document.getElementById('status-badge');
  badge.textContent = text;
  badge.classList.toggle('ok', ok);
}

function fitToBounds() {
  if (!state.geojsonLayer) return;
  try {
    const bounds = state.geojsonLayer.getBounds();
    if (bounds.isValid()) state.map.fitBounds(bounds, { padding: [20, 20] });
  } catch (e) {}
}

function findDataset(id) {
  if (!state.datasets.categories) return null;
  for (const items of Object.values(state.datasets.categories)) {
    const match = items.find(item => item.id === id);
    if (match) return match;
  }
  return null;
}

function fmt(v) {
  if (v == null) return '—';
  if (typeof v === 'number') {
    if (v >= 10000) return v.toLocaleString('en-GB', { maximumFractionDigits: 0 });
    if (v >= 100) return v.toLocaleString('en-GB', { maximumFractionDigits: 1 });
    return v.toLocaleString('en-GB', { maximumFractionDigits: 2 });
  }
  return String(v);
}

function fmtInt(v) {
  return v == null ? '—' : Math.round(v).toLocaleString('en-GB');
}

function showNoDataMessage() {
  const overlay = document.getElementById('map-overlay');
  overlay.classList.remove('hidden');
  overlay.innerHTML = '<div style="text-align:center;padding:20px;max-width:380px"><div style="font-size:32px;margin-bottom:12px;opacity:0.4">⊙</div><p style="color:var(--text-primary);font-weight:600;margin-bottom:8px">No boundaries loaded</p><p style="color:var(--text-secondary);font-size:12px;line-height:1.6">Boundary data not yet loaded. Try refreshing.</p></div>';
}
