/*  ======================================================
    APRENDE TOL — Gamified Tol Language Learning
    ======================================================
    Pedagogical approach: Spaced repetition principles,
    multi-modal input (visual + auditory + kinesthetic),
    low-pressure "passive" engagement with incremental difficulty.
    Target: Spanish-speaking elementary-to-middle-school kids.
    ====================================================== */

const MAIN = document.getElementById("learn-main");

const CATEGORY_ICONS = {
  animals: "🐾", body: "🦴", family: "👨‍👩‍👧‍👦", food: "🌽", nature: "🌿",
  colors: "🎨", numbers: "🔢", house: "🏠", greetings: "👋", verbs: "⚡",
  phrases: "💬", other: "📚",
};
const CATEGORY_NAMES_ES = {
  animals: "Animales", body: "Cuerpo", family: "Familia", food: "Comida",
  nature: "Naturaleza", colors: "Colores", numbers: "Números", house: "Casa",
  greetings: "Saludos", verbs: "Verbos", phrases: "Frases", other: "Vocabulario",
};
const CATEGORY_NAMES_EN = {
  animals: "Animals", body: "Body", family: "Family", food: "Food",
  nature: "Nature", colors: "Colors", numbers: "Numbers", house: "House",
  greetings: "Greetings", verbs: "Verbs", phrases: "Phrases", other: "Vocabulary",
};
const ENCOURAGEMENTS_ES = [
  "¡Excelente! 🌟", "¡Muy bien! 💪", "¡Increíble! 🎉", "¡Genial! 🔥",
  "¡Perfecto! ⭐", "¡Sigue así! 🚀", "¡Fantástico! 🏆",
];
const ENCOURAGEMENTS_EN = [
  "Excellent! 🌟", "Great job! 💪", "Incredible! 🎉", "Awesome! 🔥",
  "Perfect! ⭐", "Keep it up! 🚀", "Fantastic! 🏆",
];
const CONSOLATIONS_ES = [
  "¡Casi! Intenta de nuevo 💪", "No te preocupes, estás aprendiendo 🌱",
  "¡Sigue practicando! 📖", "La próxima vez lo logras 🌟",
];
const CONSOLATIONS_EN = [
  "Almost! Try again 💪", "Don't worry, you're learning 🌱",
  "Keep practicing! 📖", "You'll get it next time 🌟",
];

const UI = {
  es: {
    catNames: CATEGORY_NAMES_ES, encouragements: ENCOURAGEMENTS_ES, consolations: CONSOLATIONS_ES,
    langLabel: "Español", tolLabel: "Tol", toggleLabel: "🇪🇸 Español",
    pageTitle: "¡Aprende el idioma de tus ancestros!",
    level: "Nivel", streak: "Racha", precision: "Precisión",
    categories: "Categorías", difficulty: "Dificultad", all: "Todas",
    easy: "Fácil", medium: "Medio", hard: "Difícil",
    quizSpaTol: "Español → Tol", quizTolSpa: "Tol → Español",
    quizSpaDesc: "¿Cómo se dice en Tol? Elige la respuesta correcta",
    quizTolDesc: "Descubre qué significa cada palabra en Tol",
    memory: "Memoria", memoryDesc: "Conecta las palabras en Tol con su traducción",
    explore: "Explorar", exploreDesc: "Tarjetas de vocabulario para estudiar a tu ritmo",
    howToSay: "¿Cómo se dice en Tol?", whatMeans: "¿Qué significa?",
    answerIs: "La respuesta es:", next: "Siguiente →", back: "Volver",
    playAgain: "Jugar de nuevo 🔄", goHome: "Volver al inicio 🏠",
    correct: "Correctas", incorrect: "Incorrectas", time: "Tiempo",
    pairs: "Pares", attempts: "Intentos", pairsFound: "pares encontrados",
    of: "de", perfectMemory: "¡Memoria perfecta!", greatJob: "¡Muy bien!",
    goodWork: "¡Buen trabajo!", incredible: "¡Increíble!", keepPracticing: "¡Sigue practicando!",
    prev: "← Anterior", shuffle: "🔀 Mezclar", listen: "🔊 Escuchar",
    tapToSeeTol: "Toca para ver en Tol ↻", tapToFlip: "Toca para voltear ↻",
    tolAudioActive: "🔊 Audio Tol activo", tolAudioTraining: "🔇 Audio Tol: entrenando...",
    listenSpa: "Escuchar en español", listenTol: "Escuchar en Tol", tolSoon: "Audio Tol disponible pronto",
  },
  en: {
    catNames: CATEGORY_NAMES_EN, encouragements: ENCOURAGEMENTS_EN, consolations: CONSOLATIONS_EN,
    langLabel: "English", tolLabel: "Tol", toggleLabel: "🇺🇸 English",
    pageTitle: "Learn the language of your ancestors!",
    level: "Level", streak: "Streak", precision: "Accuracy",
    categories: "Categories", difficulty: "Difficulty", all: "All",
    easy: "Easy", medium: "Medium", hard: "Hard",
    quizSpaTol: "English → Tol", quizTolSpa: "Tol → English",
    quizSpaDesc: "How do you say it in Tol? Choose the correct answer",
    quizTolDesc: "Discover what each Tol word means",
    memory: "Memory", memoryDesc: "Match the Tol words with their translation",
    explore: "Explore", exploreDesc: "Vocabulary flashcards to study at your own pace",
    howToSay: "How do you say it in Tol?", whatMeans: "What does it mean?",
    answerIs: "The answer is:", next: "Next →", back: "Back",
    playAgain: "Play again 🔄", goHome: "Back to home 🏠",
    correct: "Correct", incorrect: "Incorrect", time: "Time",
    pairs: "Pairs", attempts: "Attempts", pairsFound: "pairs found",
    of: "of", perfectMemory: "Perfect memory!", greatJob: "Great job!",
    goodWork: "Good work!", incredible: "Incredible!", keepPracticing: "Keep practicing!",
    prev: "← Previous", shuffle: "🔀 Shuffle", listen: "🔊 Listen",
    tapToSeeTol: "Tap to see in Tol ↻", tapToFlip: "Tap to flip ↻",
    tolAudioActive: "🔊 Tol Audio active", tolAudioTraining: "🔇 Tol Audio: training...",
    listenSpa: "Listen in English", listenTol: "Listen in Tol", tolSoon: "Tol audio coming soon",
  },
};

function _getSiteLang() { return (typeof siteLang === "function" ? siteLang() : null) || localStorage.getItem("tol_site_lang") || "es"; }
let uiLang = _getSiteLang();
function t() { return UI[uiLang]; }
function catName(cat) { return t().catNames[cat] || cat; }
function pickRandom(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function knownText(word) {
  if (uiLang === "en") return word.english || word.spanish || "";
  return word.spanish || word.english || "";
}
function knownTextFromRaw(spanish, english) {
  if (uiLang === "en") return english || spanish || "";
  return spanish || english || "";
}

const SPEAKER_SVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/><path d="M15.54 8.46a5 5 0 010 7.07"/><path d="M19.07 4.93a10 10 0 010 14.14"/></svg>`;

let currentCategory = null;
let currentDifficulty = null;
let progress = loadProgress();
let tolTtsAvailable = false;


/* ====== AUDIO ENGINE ====== */

let _currentTolAudio = null;

async function checkTolTts() {
  try {
    const r = await fetch("/api/tts-status");
    const d = await r.json();
    tolTtsAvailable = d.available === true;
  } catch { tolTtsAvailable = false; }
}

function speakKnown(text) {
  if (!text || !window.speechSynthesis) return;
  window.speechSynthesis.cancel();
  const u = new SpeechSynthesisUtterance(text);
  if (uiLang === "en") {
    u.lang = "en-US";
    u.rate = 0.9;
    const voices = window.speechSynthesis.getVoices();
    const enVoice = voices.find(v => v.lang.startsWith("en")) || null;
    if (enVoice) u.voice = enVoice;
  } else {
    u.lang = "es-HN";
    u.rate = 0.85;
    const voices = window.speechSynthesis.getVoices();
    const esVoice = voices.find(v => v.lang.startsWith("es")) || null;
    if (esVoice) u.voice = esVoice;
  }
  window.speechSynthesis.speak(u);
}

async function speakTol(text, btnEl) {
  if (!text) return;
  if (!tolTtsAvailable) return;

  if (_currentTolAudio) {
    _currentTolAudio.pause();
    _currentTolAudio = null;
  }

  if (btnEl) btnEl.classList.add("audio-loading");

  try {
    const resp = await fetch("/api/tts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
    if (!resp.ok) {
      if (btnEl) btnEl.classList.remove("audio-loading");
      return;
    }
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const audio = new Audio(url);
    _currentTolAudio = audio;
    audio.onended = () => {
      URL.revokeObjectURL(url);
      _currentTolAudio = null;
      if (btnEl) btnEl.classList.remove("audio-playing");
    };
    if (btnEl) {
      btnEl.classList.remove("audio-loading");
      btnEl.classList.add("audio-playing");
    }
    audio.play();
  } catch {
    if (btnEl) btnEl.classList.remove("audio-loading");
  }
}

function audioBtn(lang, text, extraClass) {
  const cls = extraClass ? `audio-btn ${extraClass}` : "audio-btn";
  const escaped = esc(text).replace(/'/g, "&#39;");
  if (lang === "known") {
    return `<button class="${cls} audio-es" onclick="event.stopPropagation();speakKnown('${escaped}')" title="${esc(t().listenSpa)}">${SPEAKER_SVG}</button>`;
  }
  if (lang === "tol") {
    if (!tolTtsAvailable) {
      return `<button class="${cls} audio-tol audio-unavailable" title="${esc(t().tolSoon)}" disabled>${SPEAKER_SVG}</button>`;
    }
    return `<button class="${cls} audio-tol" onclick="event.stopPropagation();speakTol('${escaped}', this)" title="${esc(t().listenTol)}">${SPEAKER_SVG}</button>`;
  }
  return "";
}


/* ====== PROGRESS ====== */

function loadProgress() {
  try {
    return JSON.parse(localStorage.getItem("tol_learn_progress")) || {
      totalCorrect: 0, totalAttempted: 0, streak: 0, bestStreak: 0,
      wordsLearned: [], lastSession: null, xp: 0, level: 1,
    };
  } catch { return { totalCorrect: 0, totalAttempted: 0, streak: 0, bestStreak: 0, wordsLearned: [], lastSession: null, xp: 0, level: 1 }; }
}

function saveProgress() {
  progress.lastSession = new Date().toISOString();
  localStorage.setItem("tol_learn_progress", JSON.stringify(progress));
}

function addXP(amount) {
  progress.xp += amount;
  const needed = progress.level * 100;
  if (progress.xp >= needed) {
    progress.xp -= needed;
    progress.level++;
  }
  saveProgress();
}

function esc(s) {
  if (!s) return "";
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}


/* ====== HOME SCREEN ====== */

async function showHome() {
  let cats = {};
  try {
    const resp = await fetch("/api/learn/categories");
    const data = await resp.json();
    cats = data.categories || {};
  } catch (e) { console.error(e); }

  const L = t();
  const xpNeeded = progress.level * 100;
  const accuracy = progress.totalAttempted > 0
    ? Math.round(progress.totalCorrect / progress.totalAttempted * 100) : 0;

  const tolBadge = tolTtsAvailable
    ? `<span style="font-size:.65rem;background:rgba(255,255,255,.2);padding:.15rem .4rem;border-radius:8px;">${L.tolAudioActive}</span>`
    : `<span style="font-size:.65rem;background:rgba(255,255,255,.15);padding:.15rem .4rem;border-radius:8px;opacity:.7;">${L.tolAudioTraining}</span>`;

  let html = `<div class="learn-home">

    <div class="streak-bar">
      <div class="streak-item">
        <span class="streak-num">⭐ ${progress.level}</span>
        <span class="streak-label">${L.level}</span>
      </div>
      <div class="streak-item">
        <span class="streak-num">🔥 ${progress.streak}</span>
        <span class="streak-label">${L.streak}</span>
      </div>
      <div class="streak-item">
        <span class="streak-num">💎 ${progress.xp}/${xpNeeded}</span>
        <span class="streak-label">XP</span>
      </div>
      <div class="streak-item">
        <span class="streak-num">🎯 ${accuracy}%</span>
        <span class="streak-label">${L.precision}</span>
      </div>
    </div>
    <div style="text-align:center;margin-bottom:1rem;">${tolBadge}</div>

    <div class="activity-grid">
      <div class="activity-card quiz" onclick="startQuiz('spa_to_tol')">
        <span class="act-icon">🧠</span>
        <span class="act-title">${L.quizSpaTol}</span>
        <span class="act-desc">${L.quizSpaDesc}</span>
      </div>
      <div class="activity-card quiz" onclick="startQuiz('tol_to_spa')">
        <span class="act-icon">🔤</span>
        <span class="act-title">${L.quizTolSpa}</span>
        <span class="act-desc">${L.quizTolDesc}</span>
      </div>
      <div class="activity-card match" onclick="startMatch()">
        <span class="act-icon">🃏</span>
        <span class="act-title">${L.memory}</span>
        <span class="act-desc">${L.memoryDesc}</span>
      </div>
      <div class="activity-card explore" onclick="startExplore()">
        <span class="act-icon">📚</span>
        <span class="act-title">${L.explore}</span>
        <span class="act-desc">${L.exploreDesc}</span>
      </div>
    </div>

    <h3 style="text-align:center;color:var(--text-secondary);font-size:.9rem;margin-bottom:.8rem;">${L.categories}</h3>
    <div class="category-pills">
      <span class="cat-pill ${!currentCategory ? 'active' : ''}" onclick="setCategory(null)">${L.all}</span>`;

  const sortedCats = Object.entries(cats).sort((a, b) => b[1].count - a[1].count);
  for (const [cat, info] of sortedCats) {
    const icon = CATEGORY_ICONS[cat] || "📚";
    const name = catName(cat);
    html += `<span class="cat-pill ${currentCategory === cat ? 'active' : ''}" onclick="setCategory('${cat}')">
      ${icon} ${name} <span class="pill-count">(${info.count})</span>
    </span>`;
  }

  html += `</div>

    <h3 style="text-align:center;color:var(--text-secondary);font-size:.9rem;margin-bottom:.8rem;">${L.difficulty}</h3>
    <div class="diff-selector">
      <button class="diff-btn easy ${currentDifficulty === 1 ? 'active' : ''}" onclick="setDifficulty(1)">🌱 ${L.easy}</button>
      <button class="diff-btn medium ${currentDifficulty === 2 ? 'active' : ''}" onclick="setDifficulty(2)">🌿 ${L.medium}</button>
      <button class="diff-btn hard ${currentDifficulty === 3 ? 'active' : ''}" onclick="setDifficulty(3)">🌳 ${L.hard}</button>
      <button class="diff-btn ${!currentDifficulty ? 'active' : ''}" onclick="setDifficulty(null)" style="${!currentDifficulty ? 'background:var(--learn-green);color:#fff;border-color:var(--learn-green);' : ''}">${L.all}</button>
    </div>

  </div>`;

  MAIN.innerHTML = html;
}

function setCategory(cat) { currentCategory = cat; showHome(); }
function setDifficulty(diff) { currentDifficulty = diff; showHome(); }


/* ====== QUIZ ====== */

let quizData = null;
let quizIndex = 0;
let quizCorrect = 0;
let quizStartTime = 0;

async function startQuiz(mode) {
  const params = new URLSearchParams({ mode, count: 10 });
  if (currentCategory) params.set("category", currentCategory);
  if (currentDifficulty) params.set("difficulty", currentDifficulty);

  try {
    const resp = await fetch(`/api/learn/quiz?${params}`);
    if (!resp.ok) {
      const err = await resp.json();
      alert(err.detail || "No hay suficientes palabras para este quiz");
      return;
    }
    quizData = await resp.json();
    quizIndex = 0;
    quizCorrect = 0;
    quizStartTime = Date.now();
    showQuizQuestion();
  } catch (e) {
    console.error(e);
    alert("Error cargando el quiz");
  }
}

function showQuizQuestion() {
  if (quizIndex >= quizData.questions.length) {
    showQuizResults();
    return;
  }

  const L = t();
  const q = quizData.questions[quizIndex];
  const pct = (quizIndex / quizData.questions.length) * 100;
  const isSpaTol = quizData.mode === "spa_to_tol";

  const promptText = isSpaTol
    ? knownTextFromRaw(q.prompt, q.prompt_english)
    : q.prompt;
  const promptAudioLang = isSpaTol ? "known" : "tol";
  const optionAudioLang = isSpaTol ? "tol" : "known";
  const langLabel = isSpaTol ? L.langLabel : L.tolLabel;
  const answerLabel = isSpaTol ? L.howToSay : L.whatMeans;

  let html = `<div class="quiz-screen">
    <div class="quiz-header">
      <button class="quiz-back" onclick="showHome()" title="${L.back}">←</button>
      <div class="quiz-progress-wrap">
        <div class="quiz-progress">
          <div class="quiz-progress-fill" style="width:${pct}%"></div>
        </div>
      </div>
      <div class="quiz-score">${quizCorrect}/${quizIndex}</div>
    </div>

    <div class="quiz-card">
      <div class="q-label">${esc(langLabel)}</div>
      <div class="q-prompt">
        ${esc(promptText)}
        ${audioBtn(promptAudioLang, promptText, "q-audio")}
      </div>
      <div class="q-hint">${esc(answerLabel)}</div>

      <div class="quiz-options" id="quiz-options">`;

  for (let i = 0; i < q.options.length; i++) {
    const rawOpt = q.options[i];
    let displayOpt;
    if (isSpaTol) {
      displayOpt = rawOpt;
    } else if (uiLang === "en" && q.options_english && q.options_english[i]) {
      displayOpt = q.options_english[i];
    } else {
      displayOpt = rawOpt;
    }
    const optEsc = JSON.stringify(rawOpt).replace(/"/g, '&quot;');
    const corEsc = JSON.stringify(q.correct).replace(/"/g, '&quot;');
    html += `<button class="quiz-opt" onclick="checkAnswer(this, ${corEsc}, ${optEsc})" data-raw="${esc(rawOpt)}">
      <span class="opt-text">${esc(displayOpt)}</span>
      ${audioBtn(optionAudioLang, displayOpt, "opt-audio")}
    </button>`;
  }

  html += `</div>
      <div id="quiz-feedback"></div>
    </div>
  </div>`;

  MAIN.innerHTML = html;

  if (promptAudioLang === "known") {
    setTimeout(() => speakKnown(promptText), 400);
  } else if (promptAudioLang === "tol" && tolTtsAvailable) {
    setTimeout(() => speakTol(promptText), 400);
  }
}

function checkAnswer(btn, correct, chosen) {
  const opts = document.querySelectorAll(".quiz-opt");
  opts.forEach(o => o.classList.add("disabled"));

  const isCorrect = chosen === correct;
  const fb = document.getElementById("quiz-feedback");
  const q = quizData.questions[quizIndex];
  const isSpaTol = quizData.mode === "spa_to_tol";
  const correctLang = isSpaTol ? "tol" : "es";

  if (isCorrect) {
    btn.classList.add("correct", "highlight");
    quizCorrect++;
    progress.totalCorrect++;
    progress.streak++;
    if (progress.streak > progress.bestStreak) progress.bestStreak = progress.streak;
    addXP(10);
    const correctAudioLang = isSpaTol ? "tol" : "known";
    const msg = pickRandom(t().encouragements);
    fb.innerHTML = `<div class="quiz-feedback correct-fb">${msg}</div>`;
    if (correctAudioLang === "known") speakKnown(correct);
    else if (tolTtsAvailable) speakTol(correct);
  } else {
    btn.classList.add("wrong");
    progress.streak = 0;
    opts.forEach(o => {
      const raw = o.getAttribute("data-raw");
      if (raw === correct) o.classList.add("correct", "highlight");
    });
    const correctAudioLang = isSpaTol ? "tol" : "known";
    const msg = pickRandom(t().consolations);
    const q = quizData.questions[quizIndex];
    let displayCorrect = correct;
    if (!isSpaTol && uiLang === "en" && q.correct_english) {
      displayCorrect = q.correct_english;
    }
    fb.innerHTML = `<div class="quiz-feedback wrong-fb">
      ${msg}<br>
      <span style="font-size:.85rem;font-weight:400;">${t().answerIs} <strong>${esc(displayCorrect)}</strong>
        ${audioBtn(correctAudioLang, displayCorrect, "fb-audio")}
      </span>
    </div>`;
    addXP(2);
  }

  progress.totalAttempted++;
  saveProgress();

  fb.innerHTML += `<button class="quiz-next-btn" onclick="nextQuizQuestion()">${t().next}</button>`;
}

function nextQuizQuestion() {
  quizIndex++;
  showQuizQuestion();
}

function showQuizResults() {
  const L = t();
  const elapsed = Math.round((Date.now() - quizStartTime) / 1000);
  const total = quizData.questions.length;
  const pct = Math.round(quizCorrect / total * 100);
  let emoji, title;
  if (pct >= 90) { emoji = "🏆"; title = L.incredible; }
  else if (pct >= 70) { emoji = "🌟"; title = L.greatJob; }
  else if (pct >= 50) { emoji = "💪"; title = L.goodWork; }
  else { emoji = "🌱"; title = L.keepPracticing; }

  let html = `<div class="results-screen">
    <div class="results-card">
      <div class="results-emoji">${emoji}</div>
      <div class="results-title">${title}</div>
      <div class="results-subtitle">${quizCorrect} ${L.of} ${total} ${L.correct.toLowerCase()}</div>
      <div class="results-stats">
        <div class="results-stat correct"><span class="rs-num">${quizCorrect}</span><span class="rs-label">${L.correct}</span></div>
        <div class="results-stat wrong"><span class="rs-num">${total - quizCorrect}</span><span class="rs-label">${L.incorrect}</span></div>
        <div class="results-stat time"><span class="rs-num">${elapsed}s</span><span class="rs-label">${L.time}</span></div>
      </div>
    </div>
    <div class="results-actions">
      <button class="results-btn primary" onclick="startQuiz('${quizData.mode}')">${L.playAgain}</button>
      <button class="results-btn secondary" onclick="showHome()">${L.goHome}</button>
    </div>
  </div>`;

  MAIN.innerHTML = html;
}


/* ====== MATCH GAME ====== */

let matchPairs = [];
let matchCards = [];
let matchSelected = null;
let matchedCount = 0;
let matchMoves = 0;
let matchTimerStart = 0;
let matchTimerInterval = null;

async function startMatch() {
  const params = new URLSearchParams({ count: 6 });
  if (currentCategory) params.set("category", currentCategory);
  if (currentDifficulty) params.set("difficulty", currentDifficulty);

  try {
    const resp = await fetch(`/api/learn/match?${params}`);
    if (!resp.ok) {
      const err = await resp.json();
      alert(err.detail || "No hay suficientes palabras");
      return;
    }
    const data = await resp.json();
    matchPairs = data.pairs;
    matchedCount = 0;
    matchMoves = 0;
    matchSelected = null;
    matchTimerStart = Date.now();
    if (matchTimerInterval) clearInterval(matchTimerInterval);

    matchCards = [];
    for (const p of matchPairs) {
      const knownWord = knownTextFromRaw(p.spanish, p.english);
      matchCards.push({ text: p.tol, lang: "Tol", pairId: matchCards.length / 2 | 0 });
      matchCards.push({ text: knownWord, lang: t().langLabel, pairId: (matchCards.length - 1) / 2 | 0 });
    }
    shuffleArray(matchCards);
    for (let i = 0; i < matchCards.length; i++) matchCards[i].idx = i;

    renderMatch();
    matchTimerInterval = setInterval(updateMatchTimer, 1000);
  } catch (e) {
    console.error(e);
    alert("Error cargando el juego");
  }
}

function shuffleArray(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
}

function renderMatch() {
  const elapsed = Math.round((Date.now() - matchTimerStart) / 1000);
  let html = `<div class="quiz-screen">
    <div class="match-header">
      <button class="quiz-back" onclick="stopMatch();showHome()" title="Volver">←</button>
      <span class="match-timer" id="match-timer">⏱ ${elapsed}s</span>
      <span class="match-moves">Intentos: ${matchMoves}</span>
    </div>
    <div class="match-grid">`;

  for (const card of matchCards) {
    const isMatched = card.matched;
    const isSelected = matchSelected && matchSelected.idx === card.idx;
    let cls = "match-card";
    if (isMatched) cls += " matched";
    if (isSelected) cls += " selected";
    const audioLang = card.lang === "Tol" ? "tol" : "known";
    html += `<div class="${cls}" onclick="selectMatchCard(${card.idx})" data-idx="${card.idx}">
      <div>
        <span class="mc-lang">${card.lang}</span>
        ${esc(card.text)}
        <span class="mc-audio">${audioBtn(audioLang, card.text, "match-audio")}</span>
      </div>
    </div>`;
  }

  html += `</div></div>`;
  MAIN.innerHTML = html;
}

function updateMatchTimer() {
  const el = document.getElementById("match-timer");
  if (el) {
    const elapsed = Math.round((Date.now() - matchTimerStart) / 1000);
    el.textContent = `⏱ ${elapsed}s`;
  }
}

function stopMatch() {
  if (matchTimerInterval) { clearInterval(matchTimerInterval); matchTimerInterval = null; }
}

function selectMatchCard(idx) {
  const card = matchCards.find(c => c.idx === idx);
  if (!card || card.matched) return;

  if (card.lang === "Tol") { if (tolTtsAvailable) speakTol(card.text); }
  else speakKnown(card.text);

  if (!matchSelected) {
    matchSelected = card;
    renderMatch();
    return;
  }

  if (matchSelected.idx === idx) {
    matchSelected = null;
    renderMatch();
    return;
  }

  matchMoves++;

  if (matchSelected.pairId === card.pairId && matchSelected.lang !== card.lang) {
    matchSelected.matched = true;
    card.matched = true;
    matchedCount++;
    matchSelected = null;
    progress.totalCorrect++;
    progress.totalAttempted++;
    progress.streak++;
    if (progress.streak > progress.bestStreak) progress.bestStreak = progress.streak;
    addXP(15);
    saveProgress();
    renderMatch();

    if (matchedCount === matchPairs.length) {
      stopMatch();
      setTimeout(showMatchResults, 500);
    }
  } else {
    progress.totalAttempted++;
    progress.streak = 0;
    saveProgress();

    const prevSelected = matchSelected;
    matchSelected = null;

    const allCards = document.querySelectorAll(".match-card");
    allCards.forEach(el => {
      const i = parseInt(el.dataset.idx);
      if (i === prevSelected.idx || i === card.idx) {
        el.classList.add("wrong-match");
      }
    });

    setTimeout(() => renderMatch(), 600);
  }
}

function showMatchResults() {
  const L = t();
  const elapsed = Math.round((Date.now() - matchTimerStart) / 1000);
  const total = matchPairs.length;
  let emoji, title;
  if (matchMoves <= total + 2) { emoji = "🏆"; title = L.perfectMemory; }
  else if (matchMoves <= total * 2) { emoji = "🌟"; title = L.greatJob; }
  else { emoji = "💪"; title = L.goodWork; }

  let html = `<div class="results-screen">
    <div class="results-card">
      <div class="results-emoji">${emoji}</div>
      <div class="results-title">${title}</div>
      <div class="results-subtitle">${total} ${L.pairsFound}</div>
      <div class="results-stats">
        <div class="results-stat correct"><span class="rs-num">${total}</span><span class="rs-label">${L.pairs}</span></div>
        <div class="results-stat wrong"><span class="rs-num">${matchMoves}</span><span class="rs-label">${L.attempts}</span></div>
        <div class="results-stat time"><span class="rs-num">${elapsed}s</span><span class="rs-label">${L.time}</span></div>
      </div>
    </div>
    <div class="results-actions">
      <button class="results-btn primary" onclick="startMatch()">${L.playAgain}</button>
      <button class="results-btn secondary" onclick="showHome()">${L.goHome}</button>
    </div>
  </div>`;

  MAIN.innerHTML = html;
}


/* ====== EXPLORE / FLASHCARDS ====== */

let flashcards = [];
let flashIdx = 0;
let flashFlipped = false;

async function startExplore() {
  const params = new URLSearchParams({ limit: 30, offset: Math.floor(Math.random() * 100) });
  if (currentCategory) params.set("category", currentCategory);
  if (currentDifficulty) params.set("difficulty", currentDifficulty);

  try {
    const resp = await fetch(`/api/learn/words?${params}`);
    const data = await resp.json();
    flashcards = data.words;
    flashIdx = 0;
    flashFlipped = false;
    if (!flashcards.length) { alert("No hay palabras en esta categoría"); return; }
    shuffleArray(flashcards);
    renderFlashcard();
  } catch (e) {
    console.error(e);
  }
}

function renderFlashcard() {
  if (!flashcards.length) return;
  const L = t();
  const w = flashcards[flashIdx];
  const cat = catName(w.category);
  const icon = CATEGORY_ICONS[w.category] || "📚";
  const frontWord = knownText(w);
  const otherLangWord = uiLang === "en" ? w.spanish : w.english;

  let html = `<div class="quiz-screen">
    <div class="quiz-header">
      <button class="quiz-back" onclick="showHome()" title="${L.back}">←</button>
      <div class="fc-counter">${flashIdx + 1} / ${flashcards.length} &nbsp;·&nbsp; ${icon} ${cat}</div>
      <div></div>
    </div>

    <div class="flashcard-wrap" onclick="flipCard(event)">
      <div class="flashcard ${flashFlipped ? 'flipped' : ''}" style="min-height:240px;position:relative;">
        <div class="flashcard-face flashcard-front">
          <div class="fc-lang">${L.langLabel}</div>
          <div class="fc-word">${esc(frontWord)}</div>
          <div class="fc-audio-row">
            ${audioBtn("known", frontWord, "fc-audio-btn")}
          </div>
          ${otherLangWord ? `<div class="fc-extra">${esc(otherLangWord)}</div>` : ""}
          <div class="fc-hint">${L.tapToSeeTol}</div>
        </div>
        <div class="flashcard-face flashcard-back">
          <div class="fc-lang">Tol</div>
          <div class="fc-word">${esc(w.tol)}</div>
          <div class="fc-audio-row">
            ${audioBtn("tol", w.tol, "fc-audio-btn")}
            ${audioBtn("known", frontWord, "fc-audio-btn")}
          </div>
          <div class="fc-extra">${esc(frontWord)}${otherLangWord ? ` (${esc(otherLangWord)})` : ""}</div>
          <div class="fc-hint">${L.tapToFlip}</div>
        </div>
      </div>
    </div>

    <div class="fc-nav">
      <button class="fc-nav-btn" onclick="prevCard()" ${flashIdx === 0 ? "disabled" : ""}>${L.prev}</button>
      <button class="fc-nav-btn" onclick="autoPlayCard()">${L.listen}</button>
      <button class="fc-nav-btn" onclick="shuffleFlashcards()">${L.shuffle}</button>
      <button class="fc-nav-btn primary" onclick="nextCard()">${L.next}</button>
    </div>
  </div>`;

  MAIN.innerHTML = html;
}

function flipCard(event) {
  if (event && event.target.closest(".audio-btn")) return;
  flashFlipped = !flashFlipped;
  renderFlashcard();
}

function autoPlayCard() {
  if (!flashcards.length) return;
  const w = flashcards[flashIdx];
  speakKnown(knownText(w));
  if (tolTtsAvailable) {
    setTimeout(() => speakTol(w.tol), 1500);
  }
}

function nextCard() {
  flashIdx = (flashIdx + 1) % flashcards.length;
  flashFlipped = false;
  renderFlashcard();
}

function prevCard() {
  flashIdx = (flashIdx - 1 + flashcards.length) % flashcards.length;
  flashFlipped = false;
  renderFlashcard();
}

function shuffleFlashcards() {
  shuffleArray(flashcards);
  flashIdx = 0;
  flashFlipped = false;
  renderFlashcard();
}


/* ====== INIT ====== */

checkTolTts().then(() => showHome());

window.addEventListener("sitelangchange", (e) => {
  uiLang = e.detail.lang;
  showHome();
});

if (window.speechSynthesis) {
  window.speechSynthesis.getVoices();
  window.speechSynthesis.onvoiceschanged = () => window.speechSynthesis.getVoices();
}
