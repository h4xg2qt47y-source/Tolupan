(() => {
  "use strict";

  const $main = document.getElementById("bible-main");
  let booksCache = null;
  let currentAudio = null;
  let currentPlayingBtn = null;

  const SPEAKER_SVG = `<svg viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>`;
  const STOP_SVG = `<svg viewBox="0 0 24 24" fill="currentColor"><rect x="6" y="6" width="12" height="12" rx="1"/></svg>`;
  const LEFT_ARROW = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>`;
  const RIGHT_ARROW = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 6 15 12 9 18"/></svg>`;

  /* ---- Routing ---- */
  function parseHash() {
    const h = location.hash.replace(/^#\/?/, "");
    if (!h) return { view: "books" };
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
    if (state.view === "chapters") showChapterList(state.book);
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
      html += `
          <tr>
            <td data-lang="Tol">
              <div class="verse-cell-wrap">
                <span class="verse-num">${v.verse}</span>
                <span class="verse-text">${escHtml(v.tol)}</span>
              </div>
            </td>
            <td data-lang="Español">
              <div class="verse-cell-wrap">
                <span class="verse-num">${v.verse}</span>
                <span class="verse-text">${escHtml(v.spanish)}</span>
              </div>
            </td>
            <td data-lang="English">
              <div class="verse-cell-wrap">
                <span class="verse-num">${v.verse}</span>
                <span class="verse-text">${escHtml(v.english)}</span>
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

    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function escHtml(s) {
    if (!s) return "";
    const esc = s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    if (esc === "(see verse 1)") return `<em style="color:var(--text-muted);font-size:.8em">↑ see verse 1</em>`;
    return esc;
  }
})();
