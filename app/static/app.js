/* ===== Tol Translator – Frontend Logic ===== */

const API = '';
const LANG_LABELS = { tol: 'Tol', es: 'Español', en: 'English' };
const SPEECH_LANG_CODES = { en: 'en-US', es: 'es-HN' };

let sourceLang = (localStorage.getItem("tol_site_lang") || "es") === "es" ? "es" : "en";
let targetLang = 'tol';
let isRecording = false;
let recognition = null;

/* ---- Init ---- */
document.addEventListener('DOMContentLoaded', () => {
  bindFlags('source-flags', lang => { sourceLang = lang; updateLabels(); });
  bindFlags('target-flags', lang => { targetLang = lang; updateLabels(); });
  document.getElementById('swap-btn').addEventListener('click', swapLangs);
  document.getElementById('translate-btn').addEventListener('click', doTranslate);
  const tfl = document.getElementById('translator-feedback-link');
  if (tfl) tfl.addEventListener('click', goTranslatorFeedback);
  document.getElementById('mic-btn').addEventListener('click', toggleRecording);
  document.getElementById('clear-btn').addEventListener('click', clearInput);
  document.getElementById('speak-btn').addEventListener('click', speakOutput);
  document.getElementById('copy-btn').addEventListener('click', copyOutput);
  document.getElementById('dict-btn').addEventListener('click', dictSearch);
  document.getElementById('input-text').addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doTranslate(); }
  });
  document.getElementById('dict-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') dictSearch();
  });
  activateFlag('source-flags', sourceLang);
  updateLabels();
  loadStats();

  window.addEventListener("sitelangchange", () => {
    const l = window.siteLang();
    document.getElementById("input-text").placeholder = l === "es" ? "Escribe o habla…" : "Type or speak…";
    document.getElementById("dict-input").placeholder = l === "es" ? "Buscar palabra…" : "Search word…";

    const newSource = l === "es" ? "es" : "en";
    if (sourceLang === "es" || sourceLang === "en") {
      sourceLang = newSource;
      activateFlag("source-flags", sourceLang);
      updateLabels();
    }
  });
});

/* ---- Flag binding ---- */
function bindFlags(containerId, onChange) {
  const container = document.getElementById(containerId);
  container.querySelectorAll('.flag-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      container.querySelectorAll('.flag-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      onChange(btn.dataset.lang);
    });
  });
}

function updateLabels() {
  document.getElementById('input-lang-label').textContent = LANG_LABELS[sourceLang];
  document.getElementById('output-lang-label').textContent = LANG_LABELS[targetLang];
}

function swapLangs() {
  const tmp = sourceLang;
  sourceLang = targetLang;
  targetLang = tmp;
  activateFlag('source-flags', sourceLang);
  activateFlag('target-flags', targetLang);
  updateLabels();
  const inputEl = document.getElementById('input-text');
  const outputEl = document.getElementById('output-text');
  const outputText = outputEl.textContent;
  if (outputText && !outputEl.querySelector('.placeholder')) {
    inputEl.value = outputText;
    outputEl.innerHTML = '<span class="placeholder">Translation will appear here</span>';
    document.getElementById('meta-row').className = 'meta-row';
  }
}

function activateFlag(containerId, lang) {
  const container = document.getElementById(containerId);
  container.querySelectorAll('.flag-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.lang === lang);
  });
}

/* ---- Translation ---- */
async function doTranslate() {
  const text = document.getElementById('input-text').value.trim();
  if (!text) return;

  const btn = document.getElementById('translate-btn');
  btn.textContent = 'Translating…';
  btn.disabled = true;

  try {
    const res = await fetch(`${API}/api/translate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, source_lang: sourceLang, target_lang: targetLang }),
    });
    const data = await res.json();
    displayResult(data);
  } catch (err) {
    document.getElementById('output-text').textContent = 'Error: ' + err.message;
  } finally {
    btn.innerHTML = `<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14"/><path d="M12 5l7 7-7 7"/></svg> Translate`;
    btn.disabled = false;
  }
}

function displayResult(data) {
  const outputEl = document.getElementById('output-text');
  const metaEl = document.getElementById('meta-row');

  const translations = data.translations || [{ text: data.translation, method: data.method, confidence: data.confidence }];
  const isTolTarget = targetLang === 'tol';

  if (translations.length <= 1) {
    outputEl.innerHTML = isTolTarget ? makeClickableWords(data.translation || '(no result)') : esc(data.translation || '(no result)');
  } else {
    let html = '';
    translations.forEach((t, i) => {
      const conf = Math.round((t.confidence || 0) * 100);
      const rank = i === 0 ? 'rank-best' : 'rank-alt';
      html += `<div class="translation-candidate ${rank}">`;
      html += `<span class="rank-num">${i + 1}</span>`;
      html += `<span class="rank-text">${isTolTarget ? makeClickableWords(t.text) : esc(t.text)}</span>`;
      html += `<span class="rank-meta"><span class="tag">${t.method}</span> ${conf}%</span>`;
      html += `</div>`;
    });
    outputEl.innerHTML = html;
  }

  const confidence = Math.round((data.confidence || 0) * 100);
  let metaHTML = `<span class="tag">${data.method || 'unknown'}</span>`;
  metaHTML += `Confidence: ${confidence}%`;

  if (data.details) {
    if (data.details.intermediate_spanish) {
      metaHTML += ` · via Spanish: "${data.details.intermediate_spanish}"`;
    }
    if (data.details.category) {
      metaHTML += ` · ${data.details.category}`;
    }
    if (data.details.untranslated && data.details.untranslated.length) {
      metaHTML += ` · Unknown words: ${data.details.untranslated.join(', ')}`;
    }
    if (data.details.matched) {
      metaHTML += ` · Matched: "${data.details.matched}"`;
    }
  }

  metaEl.innerHTML = metaHTML;
  metaEl.className = 'meta-row visible';
}

function getTranslatorOutputPlain() {
  const out = document.getElementById('output-text');
  if (!out || out.querySelector('.placeholder')) return '';
  const ranks = out.querySelectorAll('.translation-candidate .rank-text');
  if (ranks.length) {
    return Array.from(ranks).map((p) => p.textContent.trim()).join('\n--- alternative ---\n');
  }
  return out.innerText.trim();
}

function goTranslatorFeedback(ev) {
  if (ev && (ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.altKey || ev.button !== 0)) return;
  if (ev) ev.preventDefault();
  const input = document.getElementById('input-text').value.trim();
  const shown = getTranslatorOutputPlain();
  try {
    sessionStorage.setItem(
      'tol_feedback_prefill',
      JSON.stringify({
        from_page: 'translator',
        category: 'translation_wrong',
        source_text: input,
        shown: shown || '',
        langs: `${sourceLang} → ${targetLang}`,
      })
    );
  } catch (_) {}
  window.location.href = '/feedback';
}

/* ---- Speech Recognition ---- */
function toggleRecording() {
  if (isRecording) {
    stopRecording();
  } else {
    startRecording();
  }
}

function startRecording() {
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognition) {
    alert('Speech recognition is not supported in this browser. Try Chrome or Edge.');
    return;
  }

  if (sourceLang === 'tol') {
    alert('Browser speech recognition does not support Tol. Please type your Tol text instead.\n\nFor Spanish or English input, select the corresponding flag first.');
    return;
  }

  recognition = new SpeechRecognition();
  recognition.lang = SPEECH_LANG_CODES[sourceLang] || 'en-US';
  recognition.interimResults = true;
  recognition.continuous = true;

  recognition.onresult = (event) => {
    let transcript = '';
    for (let i = 0; i < event.results.length; i++) {
      transcript += event.results[i][0].transcript;
    }
    document.getElementById('input-text').value = transcript;
  };

  recognition.onerror = () => stopRecording();
  recognition.onend = () => stopRecording();

  recognition.start();
  isRecording = true;
  document.getElementById('mic-btn').classList.add('recording');
  document.getElementById('recording-indicator').classList.add('visible');
}

function stopRecording() {
  if (recognition) {
    recognition.stop();
    recognition = null;
  }
  isRecording = false;
  document.getElementById('mic-btn').classList.remove('recording');
  document.getElementById('recording-indicator').classList.remove('visible');
}

/* ---- TTS ---- */
let tolTtsAvailable = null;
let tolAudioCtx = null;

async function checkTolTts() {
  try {
    const res = await fetch(`${API}/api/tts-status`);
    const data = await res.json();
    tolTtsAvailable = data.available;
  } catch (_) {
    tolTtsAvailable = false;
  }
}
checkTolTts();

async function speakOutput() {
  const bestEl = document.querySelector('#output-text .rank-best .rank-text');
  const text = bestEl ? bestEl.textContent : document.getElementById('output-text').textContent;
  if (!text || text === 'Translation will appear here') return;

  if (targetLang === 'tol') {
    if (!tolTtsAvailable) {
      alert('Tol TTS model is not ready yet — still training. Check back soon!');
      return;
    }
    const btn = document.getElementById('speak-btn');
    btn.classList.add('tts-loading');
    btn.disabled = true;
    try {
      const res = await fetch(`${API}/api/tts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: 'Unknown error' }));
        alert('TTS error: ' + (err.detail || res.statusText));
        return;
      }
      const arrayBuf = await res.arrayBuffer();
      if (!tolAudioCtx) tolAudioCtx = new (window.AudioContext || window.webkitAudioContext)();
      const audioBuf = await tolAudioCtx.decodeAudioData(arrayBuf);
      const source = tolAudioCtx.createBufferSource();
      source.buffer = audioBuf;
      source.connect(tolAudioCtx.destination);
      source.start(0);
    } catch (e) {
      alert('TTS playback error: ' + e.message);
    } finally {
      btn.classList.remove('tts-loading');
      btn.disabled = false;
    }
    return;
  }

  const utterance = new SpeechSynthesisUtterance(text);
  utterance.lang = SPEECH_LANG_CODES[targetLang] || 'en-US';
  utterance.rate = 0.9;
  speechSynthesis.cancel();
  speechSynthesis.speak(utterance);
}

/* ---- Utility ---- */
function clearInput() {
  document.getElementById('input-text').value = '';
  document.getElementById('output-text').innerHTML = '<span class="placeholder">Translation will appear here</span>';
  document.getElementById('meta-row').className = 'meta-row';
}

function copyOutput() {
  const bestEl = document.querySelector('#output-text .rank-best .rank-text');
  const text = bestEl ? bestEl.textContent : document.getElementById('output-text').textContent;
  if (text && text !== 'Translation will appear here') {
    navigator.clipboard.writeText(text).then(() => {
      const btn = document.getElementById('copy-btn');
      btn.style.color = 'var(--accent)';
      setTimeout(() => btn.style.color = '', 1000);
    });
  }
}

/* ---- Dictionary ---- */
async function dictSearch() {
  const query = document.getElementById('dict-input').value.trim();
  const lang = document.getElementById('dict-lang').value;
  if (!query) return;

  try {
    const res = await fetch(`${API}/api/dictionary`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, lang }),
    });
    const data = await res.json();
    displayDictResults(data.results);
  } catch (err) {
    document.getElementById('dict-results').textContent = 'Error: ' + err.message;
  }
}

function displayDictResults(results) {
  const el = document.getElementById('dict-results');
  if (!results.length) {
    el.innerHTML = '<p style="color:var(--text-muted);padding:.5rem 0;">No entries found.</p>';
    return;
  }
  el.innerHTML = results.map(r => `
    <div class="dict-entry">
      <div><div class="lang-label">Tol</div>${esc(r.tol)}</div>
      <div><div class="lang-label">Español</div>${esc(r.spanish)}</div>
      <div><div class="lang-label">English</div>${esc(r.english) || '—'}</div>
      <div class="cat">${esc(r.category) || '—'}</div>
    </div>
  `).join('');
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s || '';
  return d.innerHTML;
}

function makeClickableWords(text) {
  if (!text) return '';
  return text.split(/(\s+)/).map(token => {
    if (/^\s+$/.test(token)) return token;
    const clean = token.replace(/^[[\]()]+|[[\]()]+$/g, '');
    if (!clean || clean.startsWith('[')) return esc(token);
    const encoded = encodeURIComponent(clean.toLowerCase());
    return `<a href="/dictionary?word=${encoded}" class="tol-word-link" onclick="event.preventDefault(); window.open('/dictionary?word=${encoded}', '_blank')" title="Look up '${esc(clean)}' in the dictionary">${esc(token)}</a>`;
  }).join('');
}

/* ---- Stats ---- */
async function loadStats() {
  try {
    const res = await fetch(`${API}/api/stats`);
    const data = await res.json();
    const parts = [
      `${(data.dictionary_entries || 0).toLocaleString()} words`,
      `${(data.direct_en_tol || 0).toLocaleString()} direct En→Tol`,
      `${(data.phrase_translations || 0).toLocaleString()} phrases`,
      `${(data.verb_conjugations || 0).toLocaleString()} verbs`,
      `${(data.parallel_sentences || 0).toLocaleString()} sentences`,
    ];
    document.getElementById('stats').innerHTML =
      `<strong>${(data.total || 0).toLocaleString()}</strong> total data points · ${parts.join(' · ')} · <a href="/dictionary" style="color:var(--accent)">Dictionary</a> · <a href="/test" style="color:var(--accent)">Grammar Tests</a>`;
  } catch (_) {}
}
