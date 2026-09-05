(function () {
  'use strict';

  var APP = [
    { code: 'VT', slug: 'vault', name: 'Vault', short: 'Vault', file: 'vt', colorVar: '--cop-vault' },
    { code: 'UB', slug: 'bars', name: 'Uneven Bars', short: 'Bars', file: 'ub', colorVar: '--cop-bars' },
    { code: 'BB', slug: 'beam', name: 'Balance Beam', short: 'Beam', file: 'bb', colorVar: '--cop-beam' },
    { code: 'FX', slug: 'floor', name: 'Floor Exercise', short: 'Floor', file: 'fx', colorVar: '--cop-floor' }
  ];
  var ROMAN = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII'];

  function byCode(code) { return APP.filter(function (a) { return a.code === code; })[0]; }
  function icon(a) { return '/assets/cop/images/apparatus/' + a.slug + '.png'; }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function romanIndex(g) { return ROMAN.indexOf(String(g).replace('Group ', '')); }

  function badgeClass(rating) { return rating ? '' : ' missing'; }
  function badgeLabel(rating) { return rating || '-'; }
  function scoreLabel(score) { return score == null ? '-' : score.toFixed(1); }

  // ── Home page ────────────────────────────────────────────────────────────

  function initHome() {
    var grid = document.getElementById('cop-app-grid');
    if (!grid) return;
    fetch('/data/cop/summary.json').then(function (r) { return r.json(); })
      .then(function (summary) { renderHome(grid, summary); })
      .catch(function () {
        grid.innerHTML = '<p class="cop-loading">Could not load Code of Points data.</p>';
      });
  }

  function renderHome(grid, summary) {
    grid.innerHTML = APP.map(function (a) {
      var s = summary[a.code] || { count: 0, groups: 0 };
      return (
        '<a class="cop-app-card" href="/code-of-points/wag/' + a.slug + '/">' +
          '<span class="cop-app-card-icon-well"><span class="cop-app-card-icon" style="background-image:url(' + icon(a) + ')" role="img" aria-label="' + esc(a.name) + '"></span></span>' +
          '<span class="cop-app-card-body">' +
            '<span class="cop-app-card-row1"><span class="cop-app-name">' + esc(a.name) + '</span><span class="cop-app-code cop-mono">' + a.code + '</span></span>' +
            '<span class="cop-app-card-row2 cop-mono"><span style="color:var(' + a.colorVar + ')">' + s.count + ' elements</span><span class="cop-divider-v"></span><span>' + s.groups + (s.groups === 1 ? ' group' : ' groups') + '</span></span>' +
          '</span>' +
        '</a>'
      );
    }).join('');
  }

  // ── Apparatus page ───────────────────────────────────────────────────────

  function initApparatus(code) {
    var app = byCode(code);
    if (!app) return;
    var els = {
      quicknav: document.getElementById('cop-quicknav'),
      headerIcon: document.getElementById('cop-header-icon'),
      headerMeta: document.getElementById('cop-header-meta'),
      search: document.getElementById('cop-q'),
      searchClear: document.getElementById('cop-q-clear'),
      sortNum: document.getElementById('cop-sort-number'),
      sortDiff: document.getElementById('cop-sort-difficulty'),
      ratingChips: document.getElementById('cop-rating-chips'),
      groupChips: document.getElementById('cop-group-chips'),
      resultCount: document.getElementById('cop-result-count'),
      resetBtn: document.getElementById('cop-reset'),
      namedToggle: document.getElementById('cop-named-toggle'),
      expandToggle: document.getElementById('cop-expand-toggle'),
      sections: document.getElementById('cop-sections'),
      modalBackdrop: document.getElementById('cop-modal-backdrop'),
      modalPanel: document.getElementById('cop-modal-panel')
    };

    var params = new URLSearchParams(location.search);
    var state = {
      data: null,
      descs: {},
      q: params.get('q') || '',
      diffs: (params.get('rating') || '').split(',').filter(Boolean),
      groups: (params.get('group') || '').split(',').filter(Boolean).map(function (g) { return 'Group ' + g; }),
      sort: params.get('sort') === 'difficulty' ? 'difficulty' : 'number',
      named: params.get('named') === '1',
      expand: params.get('expand') === '1'
    };

    // Header + quicknav (static per apparatus, render once)
    els.headerIcon.style.backgroundImage = 'url(' + icon(app) + ')';
    els.quicknav.innerHTML = APP.filter(function (a) { return a.code !== app.code; }).map(function (a) {
      return '<a class="cop-quicknav-btn" href="/code-of-points/wag/' + a.slug + '/" title="' + esc(a.name) + '">' +
        '<span class="cop-quicknav-icon" style="background-image:url(' + icon(a) + ')" role="img" aria-label="' + esc(a.name) + '"></span><span>' + esc(a.short) + '</span></a>';
    }).join('') + '<span class="cop-quicknav-divider"></span><a class="cop-back-btn" href="/code-of-points/wag/">← All apparatus</a>';

    els.search.value = state.q;
    updateSearchClear();
    updateSortButtons();
    updateNamedToggle();
    updateExpandToggle();

    Promise.all([
      fetch('/data/cop/' + app.file + '.json').then(function (r) { return r.json(); }),
      fetch('/data/cop/group_descriptions.json').then(function (r) { return r.json(); })
    ]).then(function (results) {
      state.data = results[0];
      state.descs = results[1] || {};
      els.headerMeta.textContent = app.code + ' · ' + state.data.length + ' elements';
      buildChips();
      render();
      maybeOpenFromHash();
    }).catch(function () {
      els.sections.innerHTML = '<p class="cop-loading">Could not load element data.</p>';
    });

    var searchTimer;
    els.search.addEventListener('input', function () {
      updateSearchClear();
      clearTimeout(searchTimer);
      searchTimer = setTimeout(function () { state.q = els.search.value; syncUrl(); render(); }, 150);
    });
    els.searchClear.addEventListener('click', function () {
      state.q = '';
      els.search.value = '';
      els.search.focus();
      updateSearchClear();
      syncUrl();
      render();
    });
    els.sortNum.addEventListener('click', function () { state.sort = 'number'; updateSortButtons(); syncUrl(); render(); });
    els.sortDiff.addEventListener('click', function () { state.sort = 'difficulty'; updateSortButtons(); syncUrl(); render(); });
    els.resetBtn.addEventListener('click', function () {
      state.q = ''; state.diffs = []; state.groups = []; state.named = false;
      els.search.value = '';
      updateSearchClear();
      buildChips();
      updateNamedToggle();
      syncUrl();
      render();
    });
    els.namedToggle.addEventListener('click', function () {
      state.named = !state.named;
      updateNamedToggle();
      syncUrl();
      render();
    });
    els.expandToggle.addEventListener('click', function () {
      state.expand = !state.expand;
      updateExpandToggle();
      syncUrl();
      render();
    });

    els.modalBackdrop.addEventListener('click', function (e) { if (e.target === els.modalBackdrop) closeModal(); });
    els.modalPanel.addEventListener('click', function (e) { e.stopPropagation(); });
    document.getElementById('cop-modal-close').addEventListener('click', closeModal);
    window.addEventListener('keydown', function (e) { if (e.key === 'Escape') closeModal(); });

    function updateSearchClear() { els.searchClear.style.display = els.search.value.length ? '' : 'none'; }
    function updateSortButtons() {
      els.sortNum.classList.toggle('active', state.sort === 'number');
      els.sortDiff.classList.toggle('active', state.sort === 'difficulty');
    }
    function updateNamedToggle() { els.namedToggle.classList.toggle('active', state.named); }
    function updateExpandToggle() { els.expandToggle.classList.toggle('active', state.expand); }

    function buildChips() {
      var ratings = uniqueSorted(state.data.map(function (e) { return e.difficulty_rating; }).filter(Boolean));
      var groups = uniqueSorted(state.data.map(function (e) { return e.group; })).sort(function (a, b) { return romanIndex(a) - romanIndex(b); });

      els.ratingChips.innerHTML = ratings.map(function (r) {
        var on = state.diffs.indexOf(r) !== -1;
        return '<button type="button" class="cop-chip' + (on ? ' active' : '') + '" data-rating="' + esc(r) + '">' + esc(r) + '</button>';
      }).join('');
      els.groupChips.innerHTML = groups.map(function (g) {
        var on = state.groups.indexOf(g) !== -1;
        var label = g.replace('Group ', '');
        return '<button type="button" class="cop-chip group-chip' + (on ? ' active' : '') + '" data-group="' + esc(g) + '">' + esc(label) + '</button>';
      }).join('');

      Array.prototype.forEach.call(els.ratingChips.querySelectorAll('.cop-chip'), function (btn) {
        btn.addEventListener('click', function () {
          var r = btn.dataset.rating;
          var i = state.diffs.indexOf(r);
          if (i === -1) state.diffs.push(r); else state.diffs.splice(i, 1);
          btn.classList.toggle('active');
          syncUrl();
          render();
        });
      });
      Array.prototype.forEach.call(els.groupChips.querySelectorAll('.cop-chip'), function (btn) {
        btn.addEventListener('click', function () {
          var g = btn.dataset.group;
          var i = state.groups.indexOf(g);
          if (i === -1) state.groups.push(g); else state.groups.splice(i, 1);
          btn.classList.toggle('active');
          syncUrl();
          render();
        });
      });
    }

    function uniqueSorted(arr) {
      var seen = {}, out = [];
      arr.forEach(function (v) { if (!seen[v]) { seen[v] = true; out.push(v); } });
      return out.sort(function (a, b) { return String(a).localeCompare(String(b)); });
    }

    function syncUrl() {
      var p = new URLSearchParams();
      if (state.q) p.set('q', state.q);
      if (state.diffs.length) p.set('rating', state.diffs.join(','));
      if (state.groups.length) p.set('group', state.groups.map(function (g) { return g.replace('Group ', ''); }).join(','));
      if (state.sort !== 'number') p.set('sort', state.sort);
      if (state.named) p.set('named', '1');
      if (state.expand) p.set('expand', '1');
      var qs = p.toString();
      var url = location.pathname + (qs ? '?' + qs : '') + location.hash;
      history.replaceState(null, '', url);
    }

    function matchRecord(e) {
      if (state.diffs.length && state.diffs.indexOf(e.difficulty_rating) === -1) return false;
      if (state.groups.length && state.groups.indexOf(e.group) === -1) return false;
      if (state.named && !e.aka) return false;
      if (!state.q.trim()) return true;
      var q = state.q.trim().toLowerCase();
      return (e.description || '').toLowerCase().indexOf(q) !== -1 ||
        String(e.number_float).indexOf(q) !== -1 ||
        String(e.number_raw || '').toLowerCase().indexOf(q) !== -1 ||
        (e.aka || '').toLowerCase().indexOf(q) !== -1;
    }

    function toCard(e) {
      return {
        raw: e,
        number: e.number_float,
        description: e.description,
        score: scoreLabel(e.difficulty_score),
        rating: badgeLabel(e.difficulty_rating),
        badgeCls: badgeClass(e.difficulty_rating),
        aka: e.aka || '',
        img: '/assets/cop/' + e.image_path
      };
    }

    function render() {
      var isFiltered = state.q.length > 0 || state.diffs.length > 0 || state.groups.length > 0 || state.named;
      els.resetBtn.style.display = isFiltered ? '' : 'none';

      var groups = uniqueSorted(state.data.map(function (e) { return e.group; })).sort(function (a, b) { return romanIndex(a) - romanIndex(b); });

      var rows = [];
      if (state.expand) {
        // Every matching record (base or variant) is its own card - no nesting.
        state.data.filter(matchRecord).forEach(function (e) {
          var card = toCard(e);
          card.variants = [];
          rows.push({ card: card, sortNum: e.number_float_value, sortDiff: e.difficulty_score == null ? 99 : e.difficulty_score, group: e.group });
        });
      } else {
        var families = {};
        var order = [];
        state.data.forEach(function (e) {
          var base = String(e.number_float).split('_')[0];
          if (!families[base]) { families[base] = []; order.push(base); }
          families[base].push(e);
        });

        order.forEach(function (key) {
          var members = families[key];
          var kept = members.filter(matchRecord);
          if (!kept.length) return;
          var baseRec = kept.filter(function (e) { return !e.number_variant; })[0] || kept[0];
          var variants = kept.filter(function (e) { return e !== baseRec; });
          var card = toCard(baseRec);
          card.variants = variants.map(toCard);
          rows.push({ card: card, sortNum: baseRec.number_float_value, sortDiff: baseRec.difficulty_score == null ? 99 : baseRec.difficulty_score, group: baseRec.group });
        });
      }

      var cmp = state.sort === 'number'
        ? function (a, b) { return a.sortNum - b.sortNum; }
        : function (a, b) { return (a.sortDiff - b.sortDiff) || (a.sortNum - b.sortNum); };

      var sections = groups.map(function (g) {
        var items = rows.filter(function (r) { return r.group === g; }).sort(cmp);
        if (!items.length) return null;
        return {
          label: g,
          countLabel: items.length + (items.length === 1 ? ' skill' : ' skills'),
          desc: state.descs[app.code + '|' + g] || '',
          items: items.map(function (r) { return r.card; })
        };
      }).filter(Boolean);

      var total = rows.reduce(function (n, r) { return n + 1 + r.card.variants.length; }, 0);
      els.resultCount.textContent = total + ' of ' + state.data.length + ' shown';

      if (!sections.length) {
        els.sections.innerHTML = (
          '<div class="cop-empty"><span class="cop-empty-text">No elements match those filters.</span>' +
          '<button type="button" class="cop-empty-reset" id="cop-empty-reset">Reset filters</button></div>'
        );
        var er = document.getElementById('cop-empty-reset');
        if (er) er.addEventListener('click', function () { els.resetBtn.click(); });
        return;
      }

      els.sections.innerHTML = sections.map(renderSection).join('');
      bindCardEvents();
    }

    function renderSection(sec) {
      return (
        '<section class="cop-section">' +
          '<div class="cop-section-head"><h2>' + esc(sec.label) + '</h2><span class="cop-section-count cop-mono">' + sec.countLabel + '</span></div>' +
          (sec.desc ? '<p class="cop-section-desc">' + esc(sec.desc) + '</p>' : '') +
          '<div class="cop-card-grid">' + sec.items.map(renderCard).join('') + '</div>' +
        '</section>'
      );
    }

    function renderCard(card) {
      return (
        '<div class="cop-card">' +
          '<button type="button" class="cop-card-diagram" data-key="' + esc(card.number) + '"><img src="' + esc(card.img) + '" alt="' + esc(card.description) + '" loading="lazy" /></button>' +
          '<div class="cop-card-body">' +
            '<div class="cop-card-meta"><span class="cop-card-meta-left"><span class="cop-card-number cop-mono">' + esc(card.number) + '</span>' +
            (card.aka ? '<span class="cop-card-aka">' + esc(card.aka) + '</span>' : '') + '</span>' +
            '<span class="cop-card-meta-right"><span class="cop-card-score cop-mono">' + card.score + '</span>' +
            '<span class="cop-badge' + card.badgeCls + ' cop-mono">' + esc(card.rating) + '</span></span></div>' +
            '<p class="cop-card-desc">' + esc(card.description) + '</p>' +
            (card.variants.length ? renderVariants(card.variants) : '') +
          '</div>' +
        '</div>'
      );
    }

    function renderVariants(variants) {
      return (
        '<div class="cop-variants"><span class="cop-variants-label">Variants</span>' +
        variants.map(function (v) {
          var suffix = (v.raw.number_variant || '').toUpperCase();
          return '<button type="button" class="cop-variant-btn" data-key="' + esc(v.number) + '" title="Variant ' + esc(suffix) + '">' + esc(suffix) + '</button>';
        }).join('') +
        '</div>'
      );
    }

    var cardIndex = {};
    function bindCardEvents() {
      cardIndex = {};
      state.data.forEach(function (e) { cardIndex[e.number_float] = e; });
      Array.prototype.forEach.call(els.sections.querySelectorAll('[data-key]'), function (btn) {
        btn.addEventListener('click', function () { openModal(cardIndex[btn.dataset.key]); });
      });
    }

    function openModal(e) {
      if (!e) return;
      var badgeCls = badgeClass(e.difficulty_rating);
      document.getElementById('cop-modal-kicker').textContent = app.name + ' · ' + e.group;
      document.getElementById('cop-modal-number').textContent = e.number_float;
      var akaEl = document.getElementById('cop-modal-aka');
      akaEl.textContent = e.aka || '';
      akaEl.style.display = e.aka ? '' : 'none';
      document.getElementById('cop-modal-score').textContent = 'Value ' + scoreLabel(e.difficulty_score);
      var badgeEl = document.getElementById('cop-modal-badge');
      badgeEl.className = 'cop-modal-badge cop-mono' + badgeCls;
      badgeEl.textContent = badgeLabel(e.difficulty_rating);
      var diagramEl = document.getElementById('cop-modal-diagram');
      diagramEl.style.backgroundImage = 'url(/assets/cop/' + e.image_path + ')';
      diagramEl.setAttribute('aria-label', e.description);
      document.getElementById('cop-modal-desc').textContent = e.description;
      els.modalBackdrop.style.display = 'grid';
      document.body.style.overflow = 'hidden';
      history.replaceState(null, '', location.pathname + location.search + '#' + encodeURIComponent(e.number_float));
    }

    function closeModal() {
      if (els.modalBackdrop.style.display === 'none' || !els.modalBackdrop.style.display) return;
      els.modalBackdrop.style.display = 'none';
      document.body.style.overflow = '';
      history.replaceState(null, '', location.pathname + location.search);
    }

    function maybeOpenFromHash() {
      var key = decodeURIComponent((location.hash || '').replace('#', ''));
      if (key && cardIndex[key]) openModal(cardIndex[key]);
    }
  }

  document.addEventListener('DOMContentLoaded', function () {
    var appCode = document.body.getAttribute('data-cop-app');
    if (appCode) initApparatus(appCode); else initHome();
  });
})();
