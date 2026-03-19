/**
 * Shared i18n toggle — switches all static UI text between Spanish and English.
 * Reads/writes localStorage key "tol_site_lang" ("es" | "en").
 * Each page marks translatable elements with data-es="..." data-en="...".
 * The toggle button is injected into the <header> as a small flag pill.
 */

(function () {
  const KEY = "tol_site_lang";
  let lang = localStorage.getItem(KEY) || "es";

  const FLAG_ES = '<svg viewBox="0 0 36 24" width="20" height="14" style="vertical-align:middle"><rect width="36" height="24" fill="#fff"/><rect width="36" height="8" fill="#0051AB"/><rect y="16" width="36" height="8" fill="#0051AB"/></svg>';
  const FLAG_EN = '<svg viewBox="0 0 36 24" width="20" height="14" style="vertical-align:middle"><rect width="36" height="24" fill="#B31942"/><rect y="1.85" width="36" height="1.85" fill="#fff"/><rect y="5.54" width="36" height="1.85" fill="#fff"/><rect y="9.23" width="36" height="1.85" fill="#fff"/><rect y="12.92" width="36" height="1.85" fill="#fff"/><rect y="16.62" width="36" height="1.85" fill="#fff"/><rect y="20.31" width="36" height="1.85" fill="#fff"/><rect width="14.4" height="12.92" fill="#002868"/></svg>';

  function applyLang() {
    document.querySelectorAll("[data-es][data-en]").forEach(el => {
      el.textContent = el.getAttribute("data-" + lang);
    });
    document.querySelectorAll("[data-es-html][data-en-html]").forEach(el => {
      el.innerHTML = el.getAttribute("data-" + lang + "-html");
    });
    if (btn) {
      btn.innerHTML = lang === "es" ? FLAG_ES : FLAG_EN;
      btn.title = lang === "es" ? "Cambiar a English" : "Switch to Español";
    }
    document.documentElement.lang = lang;
    localStorage.setItem(KEY, lang);
    window.dispatchEvent(new CustomEvent("sitelangchange", { detail: { lang } }));
  }

  function toggle() {
    lang = lang === "es" ? "en" : "es";
    applyLang();
  }

  const btn = document.createElement("button");
  btn.id = "site-lang-toggle";
  btn.addEventListener("click", toggle);

  const style = document.createElement("style");
  style.textContent = `
    #site-lang-toggle {
      position: fixed; top: .55rem; right: .7rem; z-index: 9999;
      background: var(--surface, #fff); border: 1px solid var(--border, #ddd);
      border-radius: 8px; padding: 4px 8px; cursor: pointer;
      box-shadow: 0 1px 4px rgba(0,0,0,.1); transition: .15s ease;
      line-height: 1;
    }
    #site-lang-toggle:hover { transform: scale(1.08); box-shadow: 0 2px 8px rgba(0,0,0,.15); }
  `;
  document.head.appendChild(style);
  document.body.appendChild(btn);

  window.siteLang = () => lang;
  window.setSiteLang = (l) => { lang = l; applyLang(); };

  applyLang();
})();
