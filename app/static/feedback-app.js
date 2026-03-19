/**
 * Feedback form — builds a Cursor-friendly block and POSTs to /api/feedback.
 */
(function () {
  const API = "";
  const MAIL = "tooling_village_30@icloud.com";
  const STORAGE_KEY = "tol_feedback_prefill";

  const $ = (id) => document.getElementById(id);

  function gatherStructured() {
    return {
      source_text: ($("fb-source-text").value || "").trim(),
      shown: ($("fb-shown").value || "").trim(),
      expected: ($("fb-expected").value || "").trim(),
      langs: ($("fb-langs").value || "").trim(),
      steps: ($("fb-steps").value || "").trim(),
      user_agent: typeof navigator !== "undefined" ? navigator.userAgent : "",
      page_url: typeof location !== "undefined" ? location.href : "",
    };
  }

  function buildCursorBlock() {
    const cat = $("fb-category").value;
    const page = $("fb-page").value;
    const notes = ($("fb-notes").value || "").trim();
    const s = gatherStructured();
    const lines = [
      "# Tol web app — feedback for Cursor / maintainers",
      "",
      "## Summary",
      `- **Category:** \`${cat}\``,
      `- **App area:** \`${page}\``,
      `- **Captured at (ISO):** ${new Date().toISOString()}`,
      "",
      "## Translation / dictionary content",
      `- **Languages / direction:** ${s.langs || "(not specified)"}`,
      "",
      "### Source text (what I typed or searched)",
      s.source_text || "(empty)",
      "",
      "### What the app showed",
      s.shown || "(empty)",
      "",
      "### What it should say (correction / expected)",
      s.expected || "(empty)",
      "",
      "## Bug or missing feature",
      s.steps || "(n/a)",
      "",
      "## Extra notes",
      notes || "(none)",
      "",
      "## Suggested next steps (checklist — edit as needed)",
      "- [ ] Reproduce in local app (`cd app && uvicorn server:app --port 8080`)",
      "- [ ] Inspect `tol.db` — `dictionary` / `parallel_sentences` / related tables",
      "- [ ] Adjust `translator.py` rules or data pipeline scripts under `scripts/`",
      "- [ ] Update `static/*.js` or `server.py` if UI/API issue",
      "",
      "## Raw context",
      `- URL: ${s.page_url}`,
      "",
    ];
    return lines.join("\n");
  }

  function refreshBlock() {
    $("cursor-block").value = buildCursorBlock();
  }

  function applyPrefill() {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const p = JSON.parse(raw);
      if (p.from_page) $("fb-page").value = p.from_page;
      if (p.category) $("fb-category").value = p.category;
      if (p.source_text != null) $("fb-source-text").value = p.source_text;
      if (p.shown != null) $("fb-shown").value = p.shown;
      if (p.expected != null) $("fb-expected").value = p.expected;
      if (p.langs) $("fb-langs").value = p.langs;
      if (p.notes) $("fb-notes").value = p.notes;
      sessionStorage.removeItem(STORAGE_KEY);
    } catch (_) {}
  }

  function showStatus(kind, html) {
    const el = $("feedback-status");
    el.className = "show " + (kind === "ok" ? "ok" : kind === "warn" ? "warn" : "err");
    el.innerHTML = html;
  }

  function mailtoLink() {
    const subject = encodeURIComponent(
      `[Tol app feedback] ${$("fb-category").value} — ${$("fb-page").value}`
    );
    const body = encodeURIComponent($("cursor-block").value || buildCursorBlock());
    return `mailto:${MAIL}?subject=${subject}&body=${body}`;
  }

  document.addEventListener("DOMContentLoaded", () => {
    applyPrefill();
    refreshBlock();

    [
      "fb-category",
      "fb-page",
      "fb-source-text",
      "fb-shown",
      "fb-expected",
      "fb-langs",
      "fb-steps",
      "fb-notes",
    ].forEach((id) => {
      $(id).addEventListener("input", refreshBlock);
      $(id).addEventListener("change", refreshBlock);
    });

    $("btn-copy-cursor").addEventListener("click", async () => {
      refreshBlock();
      try {
        await navigator.clipboard.writeText($("cursor-block").value);
        showStatus("ok", window.siteLang() === "es" ? "Copiado al portapapeles." : "Copied to clipboard.");
      } catch (_) {
        showStatus("warn", window.siteLang() === "es" ? "No se pudo copiar automáticamente; selecciona el texto manualmente." : "Could not auto-copy; select the text manually.");
      }
    });

    $("btn-refresh-block").addEventListener("click", refreshBlock);

    $("btn-mailto").addEventListener("click", () => {
      refreshBlock();
      window.location.href = mailtoLink();
    });

    $("feedback-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const btn = $("btn-submit");
      btn.disabled = true;
      refreshBlock();
      const payload = {
        category: $("fb-category").value,
        from_page: $("fb-page").value,
        contact: ($("fb-contact").value || "").trim() || null,
        notes: ($("fb-notes").value || "").trim(),
        structured: gatherStructured(),
        cursor_block: $("cursor-block").value,
        website: document.querySelector('input[name="website"]').value,
      };
      try {
        const res = await fetch(`${API}/api/feedback`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || res.statusText);
        if (data.emailed) {
          showStatus(
            "ok",
            window.siteLang() === "es"
              ? "<strong>Enviado.</strong> Gracias — el mensaje se envió por correo."
              : "<strong>Sent.</strong> Thanks — your message was emailed."
          );
        } else {
          showStatus(
            "warn",
            (window.siteLang() === "es"
              ? "<strong>Guardado en el servidor.</strong> El correo automático no está configurado en este entorno; usa «Abrir correo» o copia el bloque de abajo."
              : "<strong>Saved on the server.</strong> Automatic email is not configured here; use “Open email” or copy the block below.") +
              (data.email_error ? `<br/><small>${String(data.email_error)}</small>` : "")
          );
        }
      } catch (err) {
        showStatus(
          "err",
          (window.siteLang() === "es"
            ? "<strong>Sin conexión al servidor.</strong> Usa «Abrir correo» o copia el bloque."
            : "<strong>Could not reach the server.</strong> Use “Open email” or copy the block.") +
            `<br/><small>${String(err.message || err)}</small>`
        );
      } finally {
        btn.disabled = false;
      }
    });

    window.addEventListener("sitelangchange", refreshBlock);
  });

  /** Call from Translator / Dictionary before navigating to /feedback */
  window.tolOpenFeedback = function (prefill) {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(prefill || {}));
    } catch (_) {}
    window.location.href = "/feedback";
  };
})();
