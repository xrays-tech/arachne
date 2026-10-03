/* ============================================================================
   Arachne — documentation site script (shared by all pages)
   - active nav highlighting
   - per-page table of contents (built from h2/h3) with scrollspy
   - copy buttons on code blocks
   - lightweight syntax coloring for Rust / TOML / console (progressive only)
   ========================================================================== */
(function () {
  "use strict";

  /* ------------------------------------------------------------------ nav -- */
  function initNav() {
    var page = document.body.getAttribute("data-page");
    if (!page) return;
    var links = document.querySelectorAll(".nav-links a[data-nav]");
    links.forEach(function (a) {
      if (a.getAttribute("data-nav") === page) a.classList.add("active");
    });
    var footerLinks = document.querySelectorAll(".site-footer a[data-nav]");
    footerLinks.forEach(function (a) {
      if (a.getAttribute("data-nav") === page) a.classList.add("active");
    });
  }

  /* ------------------------------------------------------------------ toc -- */
  function slugify(text) {
    return text
      .toLowerCase()
      .replace(/[^a-z0-9\u4e00-\u9fff]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

  function buildToc() {
    var article = document.querySelector("article");
    var list = document.getElementById("toc-list");
    if (!article || !list) return;

    var headings = article.querySelectorAll("h2, h3");
    var items = [];

    headings.forEach(function (h) {
      if (!h.id) h.id = slugify(h.textContent) || "section-" + Math.random().toString(36).slice(2, 7);
      var li = document.createElement("li");
      var a = document.createElement("a");
      a.href = "#" + h.id;
      a.className = "toc-link is-" + h.tagName.toLowerCase();
      a.textContent = h.textContent;
      li.appendChild(a);
      list.appendChild(li);
      items.push({ el: h, link: a });
    });

    /* scrollspy */
    if ("IntersectionObserver" in window && items.length) {
      var spy = new IntersectionObserver(
        function (entries) {
          entries.forEach(function (entry) {
            if (!entry.isIntersecting) return;
            items.forEach(function (item) {
              var on = item.el === entry.target;
              item.link.classList.toggle("active", on);
            });
          });
        },
        { rootMargin: "-20% 0px -70% 0px" }
      );
      items.forEach(function (item) { spy.observe(item.el); });
    }

    /* mobile toggle */
    var toggle = document.getElementById("toc-toggle");
    var sidebar = document.getElementById("sidebar");
    if (toggle && sidebar) {
      toggle.addEventListener("click", function () {
        var open = sidebar.classList.toggle("mobile-open");
        toggle.setAttribute("aria-expanded", open ? "true" : "false");
        toggle.textContent = open ? "隐藏本页目录" : "显示本页目录";
      });
    }
  }

  /* --------------------------------------------------------------- copy ---- */
  function initCopy() {
    document.querySelectorAll(".code-block").forEach(function (block) {
      var head = block.querySelector(".code-head");
      if (!head) return;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "code-copy";
      btn.textContent = "复制";
      btn.setAttribute("aria-label", "复制代码");
      head.appendChild(btn);

      var code = block.querySelector("pre code");
      btn.addEventListener("click", function () {
        var text = code ? code.textContent : block.querySelector("pre").textContent;
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(done, fallback);
        } else {
          fallback();
        }
        function fallback() {
          var ta = document.createElement("textarea");
          ta.value = text;
          document.body.appendChild(ta);
          ta.select();
          try { document.execCommand("copy"); } catch (e) { /* ignore */ }
          document.body.removeChild(ta);
          done();
        }
        function done() {
          btn.textContent = "已复制";
          btn.classList.add("copied");
          setTimeout(function () {
            btn.textContent = "复制";
            btn.classList.remove("copied");
          }, 1600);
        }
      });
    });
  }

  /* ---------------------------------------------------------------- tokens - */
  var RUST_KEYWORDS = [
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else",
    "enum", "extern", "false", "fn", "for", "if", "impl", "in", "let", "loop",
    "match", "mod", "move", "mut", "pub", "ref", "return", "self", "Self",
    "static", "struct", "super", "trait", "true", "type", "typeof", "unsafe",
    "use", "where", "while"
  ];
  var RUST_TYPES = [
    "Result", "Option", "Vec", "Box", "Arc", "String", "HashMap", "Duration",
    "Instant", "u64", "u32", "u8", "i64", "i32", "i8", "f64", "f32", "usize",
    "bool", "S", "T", "TonicRx", "TonicTransport", "WalStorage", "KvStateMachine",
    "RuntimeThread", "ArachneError", "WalConfig", "WalOptions", "Handle",
    "RaftNode", "NodeId", "RaftId", "Logger", "PathBuf", "tokio", "std", "slog",
    "FsyncPolicy"
  ];

  var RULES = [
    /* console: leading $ prompt, leading # as comment */
    { lang: ["console", "sh", "text"], re: /(\$[^\n]*)/g, wrap: "tk-cmd" },
    /* single + block comments */
    { lang: ["rust", "toml", "console", "sh"], re: /(\/\/[^\n]*|#[^\n]*|\/\*[\s\S]*?\*\/)/g, wrap: "tk-comment" },
    /* byte / char / string literals */
    { lang: ["rust", "toml", "sh"], re: /(b?r?#?"(\\.|[^"\\])*"|b?'(\\'|[^'])*')/g, wrap: "tk-string" },
    /* numbers */
    { lang: ["rust", "toml"], re: /\b(\d[\d_]*(?:\.\d+)?(?:e[+-]?\d+)?)\b/gi, wrap: "tk-number" },
    /* lifetimes + attributes */
    { lang: ["rust"], re: /(#[a-z_!]*\[[^\]]*\]|'[a-zA-Z_]\w*)/g, wrap: "tk-attr" },
    /* function call names */
    { lang: ["rust"], re: /\b([a-z_]\w*)(?=\s*\()/g, wrap: "tk-func" },
    /* keywords */
    { lang: ["rust"], re: new RegExp("\\b(" + RUST_KEYWORDS.join("|") + ")\\b", "g"), wrap: "tk-keyword" },
    /* known types / prelude */
    { lang: ["rust"], re: new RegExp("\\b(" + RUST_TYPES.join("|") + ")\\b", "g"), wrap: "tk-type" }
  ];

  var ESC = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
  function esc(s) { return s.replace(/[&<>"']/g, function (c) { return ESC[c]; }); }

  /* Tokenize in one pass so tokens never nest inside each other. */
  function tokenize(text, rules) {
    var out = "";
    var pos = 0;
    var tokens = [];
    rules.forEach(function (rule) {
      rule.re.lastIndex = 0;
      var m;
      while ((m = rule.re.exec(text)) !== null) {
        tokens.push({ start: m.index, end: m.index + m[0].length, cls: rule.wrap, text: m[1] });
        if (m.index === rule.re.lastIndex) rule.re.lastIndex++;
      }
    });
    tokens.sort(function (a, b) {
      return a.start - b.start || b.end - a.end;
    });
    var lastEnd = 0;
    tokens.forEach(function (t) {
      if (t.start < pos) return;
      out += esc(text.slice(lastEnd, t.start));
      out += '<span class="' + t.cls + '">' + esc(t.text) + "</span>";
      lastEnd = t.end;
      pos = t.end;
    });
    out += esc(text.slice(lastEnd));
    return out;
  }

  function initHighlight() {
    document.querySelectorAll("pre code[data-lang]").forEach(function (code) {
      var lang = code.getAttribute("data-lang");
      if (code.getAttribute("data-highlighted")) return;
      var rules = RULES.filter(function (r) { return r.lang.indexOf(lang) !== -1; });
      if (!rules.length) return;
      var text = code.textContent;
      code.innerHTML = tokenize(text, rules);
      code.setAttribute("data-highlighted", "1");
      code.classList.add("hl");
    });
  }

  /* ----------------------------------------------------------------- boot -- */
  function boot() {
    initNav();
    buildToc();
    initCopy();
    initHighlight();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
