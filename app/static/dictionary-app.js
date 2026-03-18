/* ===== Tol Dictionary — search-first ===== */
(function () {
  const API = '';
  let page = 1;
  let debounce = null;

  const $ = id => document.getElementById(id);
  const esc = s => { if (!s) return ''; const d = document.createElement('div'); d.textContent = s; return d.innerHTML; };
  const enc = s => encodeURIComponent(s || '');

  document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    bindSearch();
    bindModal();

    const params = new URLSearchParams(location.search);
    if (params.get('word')) openWord(params.get('word'));
    if (params.get('q')) { $('q').value = params.get('q'); doSearch(); }
    else showWelcome();
  });

  /* ── Stats (fast, one query) ── */
  async function loadStats() {
    try {
      const r = await (await fetch(`${API}/api/dict/stats`)).json();
      $('dict-stats').innerHTML =
        ds(r.headwords, 'Tol headwords') + ds(r.dictionary, 'Dictionary entries') +
        ds(r.direct_en_tol, 'En→Tol') + ds(r.sentences, 'Parallel sentences') +
        ds(r.verbs, 'Verb forms');
    } catch (_) {}
  }
  function ds(n, l) { return `<div class="ds"><b>${(n||0).toLocaleString()}</b><span>${l}</span></div>`; }

  /* ── Search ── */
  function bindSearch() {
    const input = $('q');
    const clr = $('clear-btn');

    input.addEventListener('input', () => {
      clr.classList.toggle('show', input.value.length > 0);
      clearTimeout(debounce);
      debounce = setTimeout(() => { page = 1; doSearch(); }, 250);
    });
    input.addEventListener('keydown', e => {
      if (e.key === 'Enter') { clearTimeout(debounce); page = 1; doSearch(); }
    });
    clr.addEventListener('click', () => {
      input.value = ''; clr.classList.remove('show');
      $('results').innerHTML = ''; $('results-info').innerHTML = '';
      $('pagination').innerHTML = ''; showWelcome(); input.focus();
    });
  }

  async function doSearch() {
    const q = $('q').value.trim();
    if (!q) { showWelcome(); $('results-info').innerHTML = ''; $('pagination').innerHTML = ''; return; }

    $('results').innerHTML = '<div class="welcome"><p>Searching…</p></div>';
    $('results-info').innerHTML = '';

    try {
      const r = await (await fetch(`${API}/api/dict/search?q=${enc(q)}&page=${page}&per_page=30`)).json();
      if (!r.entries.length) {
        $('results').innerHTML = '<div class="welcome"><p>No results found. Try a different search term.</p></div>';
        $('results-info').innerHTML = '';
        $('pagination').innerHTML = '';
        return;
      }
      $('results-info').innerHTML = `<strong>${r.total}</strong> result${r.total !== 1 ? 's' : ''}` +
        (r.pages > 1 ? ` · page ${r.page} of ${r.pages}` : '');
      renderCards(r.entries);
      renderPages(r);
    } catch (e) {
      $('results').innerHTML = `<div class="welcome"><p>Error: ${esc(e.message)}</p></div>`;
    }
  }

  /* ── Welcome state ── */
  function showWelcome() {
    const hints = ['water', 'house', 'father', 'child', 'fire', 'sun', 'moon', 'dog', 'tree', 'eat'];
    $('results').innerHTML =
      '<div class="welcome">' +
      '<span class="welcome-icon">🔍</span>' +
      '<h2>Search the Tol Dictionary</h2>' +
      '<p>Type any word in Tol, English, or Spanish to find entries with meanings and example sentences.</p>' +
      '<div class="welcome-hints">' +
      hints.map(h => `<button class="hint-chip" onclick="document.getElementById('q').value='${h}';document.getElementById('q').dispatchEvent(new Event('input'))">${h}</button>`).join('') +
      '</div></div>';
  }

  /* ── Render result cards ── */
  function renderCards(entries) {
    let html = '';
    for (const e of entries) {
      const cats = [...new Set(e.meanings.map(m => m.category).filter(Boolean))];
      const en = [...new Set(e.meanings.map(m => m.english).filter(Boolean))].slice(0, 4).join(', ');
      const es = [...new Set(e.meanings.map(m => m.spanish).filter(Boolean))].slice(0, 3).join(', ');

      html += `<div class="d-card" data-w="${esc(e.headword_lower)}">`;
      html += `<div class="d-head"><span class="d-hw">${esc(e.headword)}</span>`;
      for (const c of cats) html += `<span class="d-cat">${esc(c)}</span>`;
      html += `</div><div class="d-defs">`;
      if (en) html += `<span class="d-lang">EN</span>${esc(en)}`;
      if (en && es) html += ' · ';
      if (es) html += `<span class="d-lang">ES</span>${esc(es)}`;
      html += `</div></div>`;
    }
    $('results').innerHTML = html;

    $('results').querySelectorAll('.d-card').forEach(card => {
      card.addEventListener('click', () => openWord(card.dataset.w));
    });
  }

  /* ── Word detail panel ── */
  function bindModal() {
    $('panel-close').addEventListener('click', closePanel);
    $('word-overlay').addEventListener('click', e => { if (e.target === $('word-overlay')) closePanel(); });
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closePanel(); });
  }

  function closePanel() {
    $('word-overlay').classList.remove('open');
    const u = new URL(location); u.searchParams.delete('word'); history.replaceState(null, '', u);
  }

  async function openWord(word) {
    $('panel-body').innerHTML = '<div class="welcome"><p>Loading…</p></div>';
    $('word-overlay').classList.add('open');
    const u = new URL(location); u.searchParams.set('word', word); history.replaceState(null, '', u);

    try {
      const r = await fetch(`${API}/api/dict/entry/${enc(word)}`);
      if (!r.ok) { $('panel-body').innerHTML = '<div class="p-empty">Word not found.</div>'; return; }
      renderPanel(await r.json());
    } catch (e) {
      $('panel-body').innerHTML = `<div class="p-empty">Error: ${esc(e.message)}</div>`;
    }
  }

  function renderPanel(e) {
    let h = `<div class="p-hw">${esc(e.headword)}</div>`;

    if (e.meanings.length) {
      h += '<div class="p-section"><div class="p-section-title">Meanings</div>';
      e.meanings.forEach((m, i) => {
        h += `<div class="p-def"><span class="p-num">${i+1}</span>` +
          `<span class="p-en">${esc(m.english) || '<em style="color:var(--text-muted)">—</em>'}</span>` +
          `<span class="p-es">${esc(m.spanish) || '<em style="color:var(--text-muted)">—</em>'}</span>` +
          (m.category ? `<span class="p-cat-tag">${esc(m.category)}</span>` : '') +
          `</div>`;
      });
      h += '</div>';
    }

    h += '<div class="p-section"><div class="p-section-title">Example Sentences</div>';
    if (e.samples && e.samples.length) {
      for (const s of e.samples) {
        h += '<div class="p-sample">';
        h += `<div class="p-s-label">Tol</div><div class="p-s-text p-s-tol">${esc(s.tol)}</div>`;
        if (s.english) h += `<div class="p-s-label">English</div><div class="p-s-text">${esc(s.english)}</div>`;
        if (s.spanish) h += `<div class="p-s-label">Español</div><div class="p-s-text">${esc(s.spanish)}</div>`;
        if (s.source) h += `<div class="p-s-src">${esc(s.source)}</div>`;
        h += '</div>';
      }
    } else {
      h += '<div class="p-empty">No example sentences found yet.</div>';
    }
    h += '</div>';

    $('panel-body').innerHTML = h;
  }

  /* ── Pagination ── */
  function renderPages(data) {
    const el = $('pagination');
    if (data.pages <= 1) { el.innerHTML = ''; return; }
    let h = `<button class="pg" ${data.page<=1?'disabled':''} onclick="dictGoPage(${data.page-1})">‹</button>`;
    const pgs = paginateNums(data.page, data.pages);
    for (const p of pgs) {
      if (p === '…') h += '<span style="padding:0 .2rem;color:var(--text-muted)">…</span>';
      else h += `<button class="pg${p===data.page?' on':''}" onclick="dictGoPage(${p})">${p}</button>`;
    }
    h += `<button class="pg" ${data.page>=data.pages?'disabled':''} onclick="dictGoPage(${data.page+1})">›</button>`;
    el.innerHTML = h;
  }

  function paginateNums(cur, tot) {
    if (tot <= 7) return Array.from({length:tot},(_,i)=>i+1);
    const p = [1];
    if (cur > 3) p.push('…');
    for (let i = Math.max(2, cur-1); i <= Math.min(tot-1, cur+1); i++) p.push(i);
    if (cur < tot-2) p.push('…');
    p.push(tot);
    return p;
  }

  window.dictGoPage = function(p) { page = p; doSearch(); window.scrollTo(0,0); };
  window.openWord = openWord;
})();
