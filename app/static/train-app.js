/* Train The Translator - interactive rating UI */
(function () {
  'use strict';

  let currentLang = 'en';
  let currentMode = 'dictionary';
  let reversed = false;
  let sessionRated = 0;

  const langNames = { en: 'English', es: 'Español', tol: 'Tol' };

  function esc(s) {
    const d = document.createElement('div');
    d.textContent = s || '';
    return d.innerHTML;
  }

  function updateDirectionLabels() {
    const from = reversed ? 'tol' : currentLang;
    const to = reversed ? currentLang : 'tol';
    document.getElementById('dir-from').textContent = langNames[from];
    document.getElementById('dir-to').textContent = langNames[to];
  }

  /* Language toggle */
  document.querySelectorAll('.lang-toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      currentLang = btn.dataset.lang;
      document.querySelectorAll('.lang-toggle-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      updateDirectionLabels();
      loadItems();
    });
  });

  /* Direction swap */
  document.getElementById('swap-dir').addEventListener('click', () => {
    reversed = !reversed;
    updateDirectionLabels();
    loadItems();
  });

  /* Mode tabs */
  document.querySelectorAll('.mode-tab').forEach(tab => {
    tab.addEventListener('click', () => {
      currentMode = tab.dataset.mode;
      document.querySelectorAll('.mode-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      loadItems();
    });
  });

  /* Refresh button */
  document.getElementById('refresh-btn').addEventListener('click', loadItems);

  async function loadItems() {
    const container = document.getElementById('train-items');
    const siteLang = (typeof window.siteLang === 'function') ? window.siteLang() : 'en';
    container.innerHTML = `<div class="loading">${siteLang === 'es' ? 'Cargando...' : 'Loading...'}</div>`;

    try {
      const res = await fetch(`/api/train/${currentMode}?lang=${currentLang}`);
      const data = await res.json();
      renderItems(data.items || []);
    } catch (e) {
      container.innerHTML = `<div class="loading">Error loading items</div>`;
      console.error(e);
    }
  }

  function renderItems(items) {
    const container = document.getElementById('train-items');
    if (!items.length) {
      container.innerHTML = '<div class="loading">No items available for this mode.</div>';
      return;
    }

    container.innerHTML = items.map((item, i) => {
      const fromLabel = langNames[item.from_lang] || item.from_lang;
      const toLabel = langNames[item.to_lang] || item.to_lang;

      let fromText = item.from_text;
      let toText = item.to_text;
      if (reversed) {
        fromText = item.to_text;
        toText = item.from_text;
      }

      const meta = [];
      if (item.category) meta.push(item.category);
      if (item.source) meta.push(item.source);
      if (item.method) meta.push(item.method);
      if (item.reference_tol && currentMode === 'translator') {
        meta.push('Ref: ' + item.reference_tol.substring(0, 60));
      }

      return `
        <div class="train-card" data-idx="${i}">
          <div class="card-content">
            <div class="card-lang-label">${esc(reversed ? toLabel : fromLabel)}</div>
            <div class="card-from">${esc(fromText)}</div>
            <div class="card-lang-label">${esc(reversed ? fromLabel : toLabel)}</div>
            <div class="card-to">${esc(toText)}</div>
            ${meta.length ? `<div class="card-meta">${esc(meta.join(' · '))}</div>` : ''}
          </div>
          <div class="card-actions">
            <button class="card-rate-btn" data-idx="${i}" data-rating="1" title="Good translation">👍</button>
            <button class="card-rate-btn" data-idx="${i}" data-rating="-1" title="Bad translation">👎</button>
          </div>
        </div>
      `;
    }).join('');

    container.querySelectorAll('.card-rate-btn').forEach(btn => {
      btn.addEventListener('click', () => rateItem(btn, items));
    });
  }

  async function rateItem(btn, items) {
    const idx = parseInt(btn.dataset.idx);
    const rating = parseInt(btn.dataset.rating);
    const item = items[idx];
    if (!item) return;

    const card = btn.closest('.train-card');
    card.querySelectorAll('.card-rate-btn').forEach(b => b.disabled = true);
    card.classList.add(rating === 1 ? 'rated-up' : 'rated-down');
    btn.classList.add(rating === 1 ? 'chosen-up' : 'chosen-down');

    const tableMap = { dictionary: 'dictionary', phrases: 'phrase', translator: 'translator' };
    const table = tableMap[currentMode] || 'translator';

    try {
      await fetch('/api/rate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table,
          from_lang: item.from_lang,
          to_lang: item.to_lang,
          from_text: item.from_text,
          to_text: item.to_text,
          tol: item.tol || '',
          method: item.method || '',
          rating,
        }),
      });
      sessionRated++;
      updateStats();
    } catch (e) {
      console.error('Rating failed:', e);
    }
  }

  function updateStats() {
    const el = document.getElementById('train-stats');
    const siteLang = (typeof window.siteLang === 'function') ? window.siteLang() : 'en';
    el.textContent = siteLang === 'es'
      ? `Has calificado ${sessionRated} en esta sesión`
      : `You've rated ${sessionRated} items this session`;
  }

  updateDirectionLabels();
  loadItems();
})();
