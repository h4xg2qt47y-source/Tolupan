/**
 * Feedback form — quick 2-field flow; optional advanced fields + Cursor block.
 */
(function () {
  const API = "";
  const MAIL = "tooling_village_30@icloud.com";
  const STORAGE_KEY = "tol_feedback_prefill";

  const $ = (id) => document.getElementById(id);

  const QUICK_PH = {
    es: "Ej.: Escribí «agua» en español y la traducción al tol no cuadra. Debería ser…",
    en: "E.g.: I typed “water” in Spanish and the Tol translation looks wrong. It should be…",
  };

  function parseTopic() {
    const v = ($("fb-topic") && $("fb-topic").value) || "other|other";
    const [category, from_page] = v.split("|");
    return {
      category: category || "other",
      from_page: from_page || "other",
    };
  }

  function gatherStructured() {
    const quick = ($("fb-quick") && $("fb-quick").value || "").trim();
    return {
      quick_message: quick,
      source_text: ($("fb-source-text") && $("fb-source-text").value || "").trim(),
      shown: ($("fb-shown") && $("fb-shown").value || "").trim(),
      expected: ($("fb-expected") && $("fb-expected").value || "").trim(),
      langs: ($("fb-langs") && $("fb-langs").value || "").trim(),
      steps: ($("fb-steps") && $("fb-steps").value || "").trim(),
      extra_notes: ($("fb-notes") && $("fb-notes").value || "").trim(),
      user_agent: typeof navigator !== "undefined" ? navigator.userAgent : "",
      page_url: typeof location !== "undefined" ? location.href : "",
    };
  }

  function buildCursorBlock() {
    const { category, from_page } = parseTopic();
    const s = gatherStructured();
    const lines = [
      "# Tol web app — feedback for Cursor / maintainers",
      "",
      "## User message (primary)",
      s.quick_message || "(empty)",
      "",
      "## Summary",
      `- **Category:** \`${category}\``,
      `- **App area:** \`${from_page}\``,
      `- **Captured at (ISO):** ${new Date().toISOString()}`,
      "",
    ];

    const hasAdvanced =
      s.source_text ||
      s.shown ||
      s.expected ||
      s.langs ||
      s.steps ||
      s.extra_notes;
    if (hasAdvanced) {
      lines.push("## Structured fields (optional / advanced)");
      lines.push(`- **Languages / direction:** ${s.langs || "(not specified)"}`);
      lines.push("");
      lines.push("### Source text");
      lines.push(s.source_text || "(empty)");
      lines.push("");
      lines.push("### What the app showed");
      lines.push(s.shown || "(empty)");
      lines.push("");
      lines.push("### What it should say");
      lines.push(s.expected || "(empty)");
      lines.push("");
      lines.push("### Bug steps");
      lines.push(s.steps || "(n/a)");
      lines.push("");
      lines.push("### Extra notes");
      lines.push(s.extra_notes || "(none)");
      lines.push("");
    }

    lines.push("## Suggested next steps (checklist — edit as needed)");
    lines.push("- [ ] Reproduce in local app (`cd app && uvicorn server:app --port 8080`)");
    lines.push("- [ ] Inspect `tol.db` — `dictionary` / `parallel_sentences` / related tables");
    lines.push("- [ ] Adjust `translator.py` rules or data pipeline scripts under `scripts/`");
    lines.push("- [ ] Update `static/*.js` or `server.py` if UI/API issue");
    lines.push("");
    lines.push("## Raw context");
    lines.push(`- URL: ${s.page_url}`);
    lines.push("");
    return lines.join("\n");
  }

  function refreshBlock() {
    const el = $("cursor-block");
    if (el) el.value = buildCursorBlock();
  }

  function syncQuickPlaceholder() {
    const ta = $("fb-quick");
    if (!ta || !QUICK_PH.es) return;
    const lang = typeof window.siteLang === "function" ? window.siteLang() : "es";
    ta.placeholder = lang === "en" ? QUICK_PH.en : QUICK_PH.es;
  }

  function syncTopicLabels() {
    const sel = $("fb-topic");
    if (!sel) return;
    const lang = typeof window.siteLang === "function" ? window.siteLang() : "es";
    sel.querySelectorAll("option").forEach((opt) => {
      const t = opt.getAttribute("data-" + lang);
      if (t) opt.textContent = t;
    });
  }

  function applyPrefill() {
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const p = JSON.parse(raw);
      const parts = [];
      if (p.source_text) parts.push(`${p.source_text}`);
      if (p.shown) parts.push(`App showed: ${p.shown}`);
      if (p.langs) parts.push(`(${p.langs})`);
      if (p.expected) parts.push(`Should be: ${p.expected}`);
      if (p.notes) parts.push(p.notes);
      const merged = parts.join("\n").trim();
      if (merged && $("fb-quick")) $("fb-quick").value = merged;

      const top = $("fb-topic");
      if (top) {
        if (p.category === "translation_wrong") top.value = "translation_wrong|translator";
        else if (p.category === "dictionary_entry") top.value = "dictionary_entry|dictionary";
        else if (p.from_page === "translator") top.value = "translation_wrong|translator";
        else if (p.from_page === "dictionary") top.value = "dictionary_entry|dictionary";
      }

      if ($("fb-source-text") && p.source_text) $("fb-source-text").value = p.source_text;
      if ($("fb-shown") && p.shown) $("fb-shown").value = p.shown;
      if ($("fb-langs") && p.langs) $("fb-langs").value = p.langs;

      sessionStorage.removeItem(STORAGE_KEY);
    } catch (_) {}
  }

  function showStatus(kind, html) {
    const el = $("feedback-status");
    el.className = "show " + (kind === "ok" ? "ok" : kind === "warn" ? "warn" : "err");
    el.innerHTML = html;
  }

  function mailtoLink() {
    const { category, from_page } = parseTopic();
    const subject = encodeURIComponent(`[Tol app feedback] ${category} — ${from_page}`);
    const body = encodeURIComponent($("cursor-block").value || buildCursorBlock());
    return `mailto:${MAIL}?subject=${subject}&body=${body}`;
  }

  function bindRefreshers() {
    const ids = [
      "fb-quick",
      "fb-topic",
      "fb-source-text",
      "fb-shown",
      "fb-expected",
      "fb-langs",
      "fb-steps",
      "fb-notes",
    ];
    ids.forEach((id) => {
      const el = $(id);
      if (!el) return;
      el.addEventListener("input", refreshBlock);
      el.addEventListener("change", refreshBlock);
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    applyPrefill();
    syncQuickPlaceholder();
    syncTopicLabels();
    refreshBlock();
    bindRefreshers();

    const copyBtn = $("btn-copy-cursor");
    if (copyBtn) {
      copyBtn.addEventListener("click", async () => {
        refreshBlock();
        try {
          await navigator.clipboard.writeText($("cursor-block").value);
          showStatus("ok", window.siteLang() === "es" ? "Copiado al portapapeles." : "Copied to clipboard.");
        } catch (_) {
          showStatus(
            "warn",
            window.siteLang() === "es"
              ? "No se pudo copiar automáticamente; selecciona el texto manualmente."
              : "Could not auto-copy; select the text manually."
          );
        }
      });
    }

    const refBtn = $("btn-refresh-block");
    if (refBtn) refBtn.addEventListener("click", refreshBlock);

    const mailBtn = $("btn-mailto");
    if (mailBtn) {
      mailBtn.addEventListener("click", () => {
        refreshBlock();
        window.location.href = mailtoLink();
      });
    }

    $("feedback-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const quick = ($("fb-quick").value || "").trim();
      if (!quick) {
        showStatus(
          "err",
          window.siteLang() === "es" ? "Escribe un mensaje breve arriba." : "Please write a short message above."
        );
        return;
      }

      const btn = $("btn-submit");
      btn.disabled = true;
      refreshBlock();
      const { category, from_page } = parseTopic();
      const s = gatherStructured();
      const extra = s.extra_notes;
      const notes = extra ? `${quick}\n\n---\n${extra}` : quick;

      const payload = {
        category,
        from_page,
        contact: ($("fb-contact").value || "").trim() || null,
        notes,
        structured: s,
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
              ? "<strong>Enviado.</strong> Gracias."
              : "<strong>Sent.</strong> Thank you."
          );
        } else {
          showStatus(
            "warn",
            (window.siteLang() === "es"
              ? "<strong>Guardado.</strong> Si no hay correo automático en el servidor, usa «Abrir correo»."
              : "<strong>Saved.</strong> If email is not configured on the server, use “Open email”.") +
              (data.email_error ? `<br/><small>${String(data.email_error)}</small>` : "")
          );
        }
      } catch (err) {
        showStatus(
          "err",
          (window.siteLang() === "es"
            ? "<strong>Sin conexión.</strong> Usa «Abrir correo»."
            : "<strong>Could not reach the server.</strong> Use “Open email”.") + `<br/><small>${String(err.message || err)}</small>`
        );
      } finally {
        btn.disabled = false;
      }
    });

    window.addEventListener("sitelangchange", () => {
      syncQuickPlaceholder();
      syncTopicLabels();
      refreshBlock();
    });
  });

  window.tolOpenFeedback = function (prefill) {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(prefill || {}));
    } catch (_) {}
    window.location.href = "/feedback";
  };
})();
