(() => {
  "use strict";

  const $main = document.getElementById("bible-main");
  let booksCache = null;
  let currentAudio = null;
  let currentPlayingBtn = null;

  const SPEAKER_SVG = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>`;
  const STOP_SVG = `<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="1"/></svg>`;
  const MINI_SPEAKER = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M3 9v6h4l5 5V4L7 9H3z"/><path d="M16.5 12A4.5 4.5 0 0 0 14 8v8a4.47 4.47 0 0 0 2.5-4z"/></svg>`;
  const LEFT_ARROW = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>`;
  const RIGHT_ARROW = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 6 15 12 9 18"/></svg>`;

  let ttsAvailable = false;
  let activeTtsBtn = null;

  fetch("/api/tts-status").then(r => r.json()).then(d => { ttsAvailable = d.available; }).catch(() => {});

  function speakBrowser(text, langCode, btn) {
    if (!text || !window.speechSynthesis) return;
    window.speechSynthesis.cancel();
    clearTtsBtn();
    const utter = new SpeechSynthesisUtterance(text);
    utter.lang = langCode;
    utter.rate = 0.9;
    activeTtsBtn = btn;
    btn.classList.add("speaking");
    utter.onend = () => clearTtsBtn();
    utter.onerror = () => clearTtsBtn();
    window.speechSynthesis.speak(utter);
  }

  function speakTol(text, btn) {
    if (!text) return;
    if (!ttsAvailable) { speakBrowser(text, "es-HN", btn); return; }
    stopAudio();
    clearTtsBtn();
    activeTtsBtn = btn;
    btn.classList.add("speaking");
    fetch("/api/tts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    })
      .then(r => { if (!r.ok) throw new Error(); return r.blob(); })
      .then(blob => {
        const url = URL.createObjectURL(blob);
        currentAudio = new Audio(url);
        currentAudio.play().catch(() => {});
        currentAudio.addEventListener("ended", () => {
          clearTtsBtn();
          URL.revokeObjectURL(url);
          currentAudio = null;
        });
      })
      .catch(() => clearTtsBtn());
  }

  function clearTtsBtn() {
    window.speechSynthesis?.cancel();
    if (activeTtsBtn) { activeTtsBtn.classList.remove("speaking"); activeTtsBtn = null; }
  }

  /* ---- Search state ---- */
  let searchDebounce = null;

  /* ---- Routing ---- */
  function parseHash() {
    const h = location.hash.replace(/^#\/?/, "");
    if (!h) return { view: "books" };
    if (h.startsWith("search/")) return { view: "search", query: decodeURIComponent(h.slice(7)) };
    const parts = h.split("/");
    if (parts.length === 1) return { view: "chapters", book: parts[0].toUpperCase() };
    if (parts.length === 2) return { view: "read", book: parts[0].toUpperCase(), chapter: parseInt(parts[1], 10) };
    return { view: "books" };
  }

  function navigate(hash) {
    location.hash = hash;
  }

  window.addEventListener("hashchange", route);
  window.addEventListener("load", route);

  function route() {
    stopAudio();
    const state = parseHash();
    if (state.view === "search") showSearchResults(state.query);
    else if (state.view === "chapters") showChapterList(state.book);
    else if (state.view === "read") showChapter(state.book, state.chapter);
    else showBookList();
  }

  /* ---- Data Fetching ---- */
  async function fetchBooks() {
    if (booksCache) return booksCache;
    const res = await fetch("/api/bible/books");
    const data = await res.json();
    booksCache = data.books;
    return booksCache;
  }

  async function fetchChapter(book, chapter) {
    const res = await fetch(`/api/bible/${book}/${chapter}`);
    if (!res.ok) throw new Error(`Failed to load ${book} ${chapter}`);
    return res.json();
  }

  /* ---- Audio ---- */
  function stopAudio() {
    window.speechSynthesis?.cancel();
    if (currentAudio) {
      currentAudio.pause();
      currentAudio.currentTime = 0;
      currentAudio = null;
    }
    if (currentPlayingBtn) {
      currentPlayingBtn.classList.remove("playing");
      currentPlayingBtn.innerHTML = SPEAKER_SVG;
      currentPlayingBtn = null;
    }
    clearTtsBtn();
  }

  function playVerse(url, btn) {
    if (currentPlayingBtn === btn) {
      stopAudio();
      return;
    }
    stopAudio();
    currentAudio = new Audio(url);
    currentPlayingBtn = btn;
    btn.classList.add("playing");
    btn.innerHTML = STOP_SVG;
    currentAudio.play().catch(() => {});
    currentAudio.addEventListener("ended", () => {
      btn.classList.remove("playing");
      btn.innerHTML = SPEAKER_SVG;
      currentAudio = null;
      currentPlayingBtn = null;
    });
  }

  /* ---- Book List View ---- */
  async function showBookList() {
    $main.innerHTML = `<div class="bible-loading">Loading books...</div>`;
    const books = await fetchBooks();

    const gospels = books.filter(b => ["MAT", "MRK", "LUK", "JHN"].includes(b.code));
    const history = books.filter(b => b.code === "ACT");
    const pauline = books.filter(b => ["ROM","1CO","2CO","GAL","EPH","PHP","COL","1TH","2TH","1TI","2TI","TIT","PHM"].includes(b.code));
    const general = books.filter(b => ["HEB","JAS","1PE","2PE","1JN","2JN","3JN","JUD"].includes(b.code));
    const prophetic = books.filter(b => b.code === "REV");

    const sections = [
      { title: "Evangelios · Gospels", books: gospels },
      { title: "Historia · History", books: history },
      { title: "Epístolas Paulinas · Pauline Epistles", books: pauline },
      { title: "Epístolas Generales · General Epistles", books: general },
      { title: "Profecía · Prophecy", books: prophetic },
    ];

    let html = `<div class="bible-breadcrumb"><span>Dios Vele — New Testament</span></div>`;

    html += `
      <div class="bible-search-bar">
        <div class="bible-search-wrap">
          <svg class="bible-search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input type="text" id="bible-search-input" class="bible-search-input"
                 placeholder="Search verses in Tol, Spanish, or English…" autocomplete="off" />
        </div>
        <p class="bible-search-hint">Try: "quelel", "love", "beautiful", "Dios", "corazón"</p>
      </div>`;

    for (const sec of sections) {
      if (!sec.books.length) continue;
      html += `<h3 style="font-size:.85rem;color:var(--text-muted);margin:1.25rem 0 .5rem;font-weight:600;">${sec.title}</h3>`;
      html += `<div class="book-list">`;
      for (const b of sec.books) {
        html += `
          <div class="book-card" data-book="${b.code}">
            <div class="book-card-title">${b.names.es}</div>
            <div class="book-card-subtitle">${b.names.en}</div>
            <div class="book-card-meta">
              <span class="badge">${b.chapter_count} ${b.chapter_count === 1 ? "chapter" : "chapters"}</span>
            </div>
          </div>`;
      }
      html += `</div>`;
    }

    $main.innerHTML = html;
    $main.querySelectorAll(".book-card").forEach(card => {
      card.addEventListener("click", () => navigate(card.dataset.book));
    });

    const searchInput = document.getElementById("bible-search-input");
    if (searchInput) {
      searchInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          const q = searchInput.value.trim();
          if (q.length >= 2) navigate(`search/${encodeURIComponent(q)}`);
        }
      });
      searchInput.addEventListener("input", () => {
        clearTimeout(searchDebounce);
        const q = searchInput.value.trim();
        if (q.length >= 3) {
          searchDebounce = setTimeout(() => navigate(`search/${encodeURIComponent(q)}`), 600);
        }
      });
    }
  }

  /* ---- Search Results View ---- */
  async function showSearchResults(query) {
    $main.innerHTML = `<div class="bible-loading">Searching for "${escHtml(query)}"...</div>`;

    let data;
    try {
      const res = await fetch(`/api/bible/search?q=${encodeURIComponent(query)}`);
      data = await res.json();
    } catch {
      $main.innerHTML = `<div class="bible-loading">Search failed. Please try again.</div>`;
      return;
    }

    let html = `
      <div class="bible-breadcrumb">
        <a onclick="location.hash=''">Dios Vele</a>
        <span class="sep">›</span>
        <span>Search</span>
      </div>
      <div class="bible-search-bar">
        <div class="bible-search-wrap">
          <svg class="bible-search-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          <input type="text" id="bible-search-input" class="bible-search-input"
                 placeholder="Search verses in Tol, Spanish, or English…" value="${attr(query)}" autocomplete="off" />
        </div>
      </div>`;

    if (!data.results.length) {
      html += `<div class="bible-search-empty">No verses found matching "<strong>${escHtml(query)}</strong>"</div>`;
    } else {
      html += `<div class="bible-search-meta">Found <strong>${data.total}</strong> verse${data.total !== 1 ? "s" : ""} matching "<strong>${escHtml(query)}</strong>"${data.total > 10 ? " — showing 10 random results" : ""}</div>`;

      for (const v of data.results) {
        const refLabel = `${v.names.en} ${v.chapter}:${v.verse}`;
        const chapterLink = `${v.book}/${v.chapter}`;

        html += `
          <div class="search-result-card">
            <div class="search-result-ref">
              <a class="search-ref-link" data-nav="${chapterLink}" title="Open ${v.names.en} ${v.chapter}">${refLabel}</a>
              <span class="search-lang-badges">${v.matched_langs.map(l => `<span class="lang-badge lang-${l}">${l === "tol" ? "Tol" : l === "spanish" ? "ES" : "EN"}</span>`).join("")}</span>
            </div>
            <div class="search-result-verses">
              <div class="search-verse-col search-verse-tol">
                <div class="search-verse-lang">Tol</div>
                <div class="search-verse-text">${highlightMatch(v.tol, query)}</div>
              </div>
              <div class="search-verse-col search-verse-es">
                <div class="search-verse-lang">Español</div>
                <div class="search-verse-text">${highlightMatch(v.spanish, query)}</div>
              </div>
              <div class="search-verse-col search-verse-en">
                <div class="search-verse-lang">English</div>
                <div class="search-verse-text">${highlightMatch(v.english, query)}</div>
              </div>
            </div>
          </div>`;
      }
    }

    $main.innerHTML = html;

    $main.querySelectorAll("[data-nav]").forEach(el => {
      el.addEventListener("click", (e) => {
        e.preventDefault();
        navigate(el.dataset.nav);
      });
    });

    const searchInput = document.getElementById("bible-search-input");
    if (searchInput) {
      searchInput.focus();
      searchInput.setSelectionRange(searchInput.value.length, searchInput.value.length);
      searchInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter") {
          e.preventDefault();
          const q = searchInput.value.trim();
          if (q.length >= 2) navigate(`search/${encodeURIComponent(q)}`);
        }
      });
      searchInput.addEventListener("input", () => {
        clearTimeout(searchDebounce);
        const q = searchInput.value.trim();
        if (q.length >= 3) {
          searchDebounce = setTimeout(() => navigate(`search/${encodeURIComponent(q)}`), 600);
        }
      });
    }
  }

  function highlightMatch(text, query) {
    if (!text || !query) return escHtml(text);
    const escaped = escHtml(text);
    const qEsc = escHtml(query).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    try {
      return escaped.replace(new RegExp(`(${qEsc})`, "gi"), `<mark class="search-highlight">$1</mark>`);
    } catch {
      return escaped;
    }
  }

  /* ---- Chapter List View ---- */
  async function showChapterList(bookCode) {
    $main.innerHTML = `<div class="bible-loading">Loading chapters...</div>`;
    const books = await fetchBooks();
    const book = books.find(b => b.code === bookCode);
    if (!book) { showBookList(); return; }

    const idx = books.indexOf(book);
    const prev = idx > 0 ? books[idx - 1] : null;
    const next = idx < books.length - 1 ? books[idx + 1] : null;

    let html = `
      <div class="bible-breadcrumb">
        <a onclick="location.hash=''">Dios Vele</a>
        <span class="sep">›</span>
        <span>${book.names.es}</span>
      </div>
      <div class="chapter-nav">
        <div class="nav-arrow ${prev ? "" : "disabled"}" ${prev ? `data-nav-book="${prev.code}"` : ""}>
          ${LEFT_ARROW}
        </div>
        <div class="chapter-nav-title" data-nav-books>
          ${book.names.es}
          <small>${book.names.en} · ${book.chapter_count} chapters</small>
        </div>
        <div class="nav-arrow ${next ? "" : "disabled"}" ${next ? `data-nav-book="${next.code}"` : ""}>
          ${RIGHT_ARROW}
        </div>
      </div>
      <div class="chapter-grid">`;

    for (const ch of book.chapters) {
      html += `<div class="chapter-tile" data-ch="${ch}">${ch}</div>`;
    }

    html += `</div>`;
    $main.innerHTML = html;

    $main.querySelectorAll(".chapter-tile").forEach(tile => {
      tile.addEventListener("click", () => navigate(`${bookCode}/${tile.dataset.ch}`));
    });
    $main.querySelectorAll("[data-nav-book]").forEach(el => {
      el.addEventListener("click", () => navigate(el.dataset.navBook));
    });
    $main.querySelector("[data-nav-books]")?.addEventListener("click", () => navigate(""));
  }

  /* ---- Chapter Reading View ---- */
  async function showChapter(bookCode, chapter) {
    $main.innerHTML = `<div class="bible-loading">Loading ${bookCode} ${chapter}...</div>`;

    let data;
    try {
      data = await fetchChapter(bookCode, chapter);
    } catch {
      $main.innerHTML = `<div class="bible-loading">Chapter not found.</div>`;
      return;
    }

    const books = await fetchBooks();
    const bookIdx = books.findIndex(b => b.code === bookCode);
    const chIdx = data.chapters.indexOf(chapter);

    let prevHash = null, nextHash = null;

    if (chIdx > 0) {
      prevHash = `${bookCode}/${data.chapters[chIdx - 1]}`;
    } else if (bookIdx > 0) {
      const pb = books[bookIdx - 1];
      prevHash = `${pb.code}/${pb.chapters[pb.chapters.length - 1]}`;
    }

    if (chIdx < data.chapters.length - 1) {
      nextHash = `${bookCode}/${data.chapters[chIdx + 1]}`;
    } else if (bookIdx < books.length - 1) {
      const nb = books[bookIdx + 1];
      nextHash = `${nb.code}/${nb.chapters[0]}`;
    }

    let html = `
      <div class="bible-breadcrumb">
        <a onclick="location.hash=''">Dios Vele</a>
        <span class="sep">›</span>
        <a onclick="location.hash='${bookCode}'">${data.names.es}</a>
        <span class="sep">›</span>
        <span>Chapter ${chapter}</span>
      </div>
      <div class="chapter-nav">
        <div class="nav-arrow ${prevHash ? "" : "disabled"}" ${prevHash ? `data-nav-hash="${prevHash}"` : ""}>
          ${LEFT_ARROW}
        </div>
        <div class="chapter-nav-title" data-nav-chapters="${bookCode}">
          ${data.names.es} ${chapter}
          <small>${data.names.en} · Chapter ${chapter} of ${data.total_chapters}</small>
        </div>
        <div class="nav-arrow ${nextHash ? "" : "disabled"}" ${nextHash ? `data-nav-hash="${nextHash}"` : ""}>
          ${RIGHT_ARROW}
        </div>
      </div>
      <table class="verse-table">
        <thead>
          <tr>
            <th style="width:38%">Tol ${data.tol_chapter_audio
              ? `<button class="chapter-listen-btn" data-audio="${data.tol_chapter_audio}" title="Listen to full chapter in Tol">${SPEAKER_SVG} Listen</button>`
              : ""}</th>
            <th style="width:31%">Español</th>
            <th style="width:31%">English ${data.english_chapter_audio
              ? `<button class="chapter-listen-btn" data-audio="${data.english_chapter_audio}" title="Listen to full chapter in English">${SPEAKER_SVG} Listen</button>`
              : ""}</th>
          </tr>
        </thead>
        <tbody>`;

    for (const v of data.verses) {
      const tolBtn = v.tol ? `<button class="verse-tts-btn" data-tts-tol="${attr(v.tol)}" title="Listen in Tol">${MINI_SPEAKER}</button>` : "";
      const esBtn = v.spanish ? `<button class="verse-tts-btn" data-tts-es="${attr(v.spanish)}" title="Escuchar en Español">${MINI_SPEAKER}</button>` : "";
      const enBtn = v.english ? `<button class="verse-tts-btn" data-tts-en="${attr(v.english)}" title="Listen in English">${MINI_SPEAKER}</button>` : "";
      html += `
          <tr>
            <td data-lang="Tol">
              <div class="verse-cell-wrap">
                <span class="verse-num">${v.verse}</span>
                <span class="verse-text">${escHtml(v.tol)}</span>
                ${tolBtn}
              </div>
            </td>
            <td data-lang="Español">
              <div class="verse-cell-wrap">
                <span class="verse-num">${v.verse}</span>
                <span class="verse-text">${escHtml(v.spanish)}</span>
                ${esBtn}
              </div>
            </td>
            <td data-lang="English">
              <div class="verse-cell-wrap">
                <span class="verse-num">${v.verse}</span>
                <span class="verse-text">${escHtml(v.english)}</span>
                ${enBtn}
              </div>
            </td>
          </tr>`;
    }

    html += `
        </tbody>
      </table>
      <div class="bible-copyright">
        <h4>Copyright Information</h4>
        <p><strong>Tol (Tolupan):</strong> Scripture text used with permission. Source: ScriptureEarth / Wycliffe Bible Translators. The Tol New Testament is the work of dedicated translators serving the Tolupan people of La Montaña de la Flor, Honduras.</p>
        <p><strong>Español (Spanish):</strong> Reina-Valera 1960 (RVR1960). Copyright © Sociedades Bíblicas Unidas, 1960. Used with permission.</p>
        <p><strong>English:</strong> World English Bible (WEB). Public domain. No copyright restrictions.</p>
      </div>`;

    $main.innerHTML = html;

    $main.querySelectorAll("[data-nav-hash]").forEach(el => {
      el.addEventListener("click", () => navigate(el.dataset.navHash));
    });
    $main.querySelectorAll("[data-nav-chapters]").forEach(el => {
      el.addEventListener("click", () => navigate(el.dataset.navChapters));
    });
    $main.querySelectorAll(".chapter-listen-btn").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        playVerse(btn.dataset.audio, btn);
      });
    });

    $main.querySelectorAll("[data-tts-tol]").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        stopAudio();
        speakTol(btn.dataset.ttsTol, btn);
      });
    });
    $main.querySelectorAll("[data-tts-es]").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        stopAudio();
        speakBrowser(btn.dataset.ttsEs, "es-HN", btn);
      });
    });
    $main.querySelectorAll("[data-tts-en]").forEach(btn => {
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        stopAudio();
        speakBrowser(btn.dataset.ttsEn, "en-US", btn);
      });
    });

    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function escHtml(s) {
    if (!s) return "";
    const esc = s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    if (esc === "(see verse 1)") return `<em style="color:var(--text-muted);font-size:.8em">↑ see verse 1</em>`;
    return esc;
  }

  function attr(s) {
    if (!s) return "";
    return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }
})();
