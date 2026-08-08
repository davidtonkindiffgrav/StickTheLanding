(function () {
  var PATH = window.location.pathname;

  function active(slug) {
    return (slug === '/' ? PATH === '/' : PATH.startsWith(slug)) ? ' active' : '';
  }

  function a(href, label, extraCls) {
    return '<a class="stl-nav-link' + (extraCls || '') + active(href) + '" href="' + href + '">' + label + '</a>';
  }

  // ── THEME ──────────────────────────────────────────────────────────────────

  function stlPreferred() {
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  function stlApply(theme) {
    document.documentElement.dataset.theme = theme;
    var icon = document.getElementById('stl-theme-icon');
    if (icon) icon.className = theme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
    var btn = document.getElementById('stl-theme-btn');
    if (btn) btn.title = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
  }

  function stlInit() {
    var saved = localStorage.getItem('stl-theme');
    stlApply(saved || stlPreferred());
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function (e) {
      if (!localStorage.getItem('stl-theme')) {
        stlApply(e.matches ? 'dark' : 'light');
      }
    });
  }

  // ── NAV HTML ───────────────────────────────────────────────────────────────

  document.body.insertAdjacentHTML('afterbegin',
    '<nav class="stl-nav" id="stl-global-nav" aria-label="Site navigation">' +
      // LEFT: brand
      '<a class="stl-nav-brand" href="/">' +
        '<img src="/assets/favicons/android-chrome-512x512.png" alt="Stick The Landing" width="36" height="36" />' +
        '<div class="stl-nav-brand-text">' +
          '<span class="stl-nav-brand-title">Stick The <strong>Landing</strong></span>' +
          '<span class="stl-nav-brand-sub">Victorian Gymnastics · 2026 Season</span>' +
        '</div>' +
      '</a>' +
      // CENTER: date + live badge
      '<div class="stl-nav-center">' +
        '<span id="v3-date" class="stl-nav-date"></span>' +
        '<span class="stl-nav-live" id="v3-live-badge"></span>' +
      '</div>' +
      // RIGHT: nav links + theme toggle + find athlete + hamburger
      '<div class="stl-nav-end">' +
        '<div class="stl-nav-links" id="stl-links">' +
          a('/', 'Home') +
          '<a class="stl-nav-link wag" href="/#wag" id="stl-wag">WAG</a>' +
          '<a class="stl-nav-link mag" href="/#mag" id="stl-mag">MAG</a>' +
          a('/news/', 'News') +
          a('/galleries/', 'Galleries') +
          a('/about/', 'About') +
          '<div class="stl-nav-divider"></div>' +
          a('/contact/', 'Contact') +
          a('/privacy/', 'Privacy') +
          '<div class="stl-nav-divider"></div>' +
          '<a class="stl-nav-link" href="https://www.facebook.com/StickTheLandingGymnastics" target="_blank" rel="noopener" aria-label="Stick The Landing on Facebook"><i class="fab fa-facebook"></i></a>' +
        '</div>' +
        '<div class="stl-nav-divider"></div>' +
        '<button class="stl-theme-btn" id="stl-theme-btn" aria-label="Toggle theme" title="Toggle dark/light mode">' +
          '<i id="stl-theme-icon" class="fas fa-moon"></i>' +
        '</button>' +
        '<div class="stl-nav-divider"></div>' +
        '<button class="stl-nav-search" id="stl-search-btn" aria-label="Find Athlete">' +
          '<i class="fas fa-search"></i>' +
          '<span class="stl-nav-search-text">Find Athlete</span>' +
        '</button>' +
      '<button class="stl-nav-hamburger" id="stl-ham" aria-label="Toggle navigation" aria-expanded="false">' +
        '<i class="fas fa-bars"></i>' +
      '</button>' +
      '</div>' +
    '</nav>'
  );

  // ── EVENT LISTENERS ────────────────────────────────────────────────────────

  var ham = document.getElementById('stl-ham');
  var linksEl = document.getElementById('stl-links');

  ham.addEventListener('click', function () {
    var open = linksEl.classList.toggle('open');
    ham.setAttribute('aria-expanded', open);
    document.body.classList.toggle('stl-nav-open', open);
  });

  linksEl.querySelectorAll('.stl-nav-link').forEach(function (el) {
    el.addEventListener('click', function () {
      linksEl.classList.remove('open');
      ham.setAttribute('aria-expanded', 'false');
      document.body.classList.remove('stl-nav-open');
    });
  });

  var searchBtn = document.getElementById('stl-search-btn');
  if (searchBtn) {
    searchBtn.addEventListener('click', function () {
      if (PATH === '/' && typeof openSearchFromLanding === 'function') {
        openSearchFromLanding();
      } else {
        window.location.href = '/#search';
      }
    });
  }

  var themeBtn = document.getElementById('stl-theme-btn');
  if (themeBtn) {
    themeBtn.addEventListener('click', function () {
      var current = document.documentElement.dataset.theme || stlPreferred();
      var next = current === 'dark' ? 'light' : 'dark';
      localStorage.setItem('stl-theme', next);
      stlApply(next);
    });
  }

  if (PATH === '/') {
    ['wag', 'mag'].forEach(function (s) {
      var el = document.getElementById('stl-' + s);
      if (el) {
        el.addEventListener('click', function (e) {
          if (typeof enterSport === 'function') {
            e.preventDefault();
            enterSport(s.toUpperCase());
          }
        });
      }
    });
  }

  stlInit();
})();
