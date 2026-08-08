// Team Picker, private beta tool. WAG only for v1.
//
// Queries data/stick_WAG.db directly in the browser via sql.js (same approach
// as the main site SPA), no backend, no persistence, results only ever live
// in this tab until copied to clipboard.

const APPS = ['VT', 'UB', 'BB', 'FX'];
const APP_COL = { VT: 'vault', UB: 'bars', BB: 'beam', FX: 'floor' };
const APP_LABEL = { VT: 'Vault', UB: 'Bars', BB: 'Beam', FX: 'Floor' };
const APP_CLASS = { VT: 'vt', UB: 'ub', BB: 'bb', FX: 'fx' };
const BASIS_LABEL = { recent3: 'Recent (last 3)', season: 'This season', allTime: 'All-time', pb: 'PB' };
const TEAM_SIZE = 5;

function appIcon(app) {
  return `<span class="tp-app-icon ${APP_CLASS[app]}" title="${APP_LABEL[app]}"></span>`;
}

let clubInfo = null;
let dbHandle = null;
let athletesByLevel = new Map();   // level -> array of athlete objects
let currentLevel = null;
let currentBasis = 'recent3';
let showCounts = true;
let predictionBasis = 'allTime';   // 'allTime' (Average) or 'pb' (Best), independent of currentBasis
let team = [];                     // array of athlete objects, order matters

// ─────────────────────────────────────────────────────────────────────────
// Access gate
// ─────────────────────────────────────────────────────────────────────────
function checkAccess() {
  const params = new URLSearchParams(location.search);
  const club = (params.get('club') || '').toUpperCase();
  const key = params.get('key') || '';
  const entry = window.TEAM_PICKER_CLUBS[club];
  if (!entry || key !== entry.key) return null;
  return { code: club, name: entry.name };
}

function renderAccessDenied() {
  document.getElementById('app').innerHTML = `
    <div class="tp-denied">
      <h1>This tool isn't available</h1>
      <p>Check the link you were given, or contact Stick The Landing if you think this is a mistake.</p>
      <p><a href="https://stickthelanding.com.au/">stickthelanding.com.au</a></p>
    </div>`;
}

// ─────────────────────────────────────────────────────────────────────────
// DB loading (root-relative paths, this page lives at /team-picker/, not /)
// ─────────────────────────────────────────────────────────────────────────
// Downloads the whole WAG .db file once and opens it as an in-memory sql.js
// database. See index.html's loadSportDbHandle for why this isn't a lazy
// Range-based loader anymore (GitHub Pages corrupts partial byte-range reads
// of these files).
let _sqlJsPromise = null;
function _getSqlJs() {
  if (!_sqlJsPromise) {
    _sqlJsPromise = initSqlJs({ locateFile: (f) => new URL('/sql/' + f + '?v=1.14.1', location.href).toString() });
  }
  return _sqlJsPromise;
}

async function loadWagDbHandle() {
  const cfgResp = await fetch(new URL('/data/dbconfig_WAG.json', location.href).toString(), { cache: 'no-cache' });
  const dbCfg = await cfgResp.json();
  const dbUrl = new URL(`/data/stick_WAG.db?v=${dbCfg.fileLength}.${dbCfg.rev || 0}`, location.href).toString();

  const [SQL, resp] = await Promise.all([_getSqlJs(), fetch(dbUrl)]);
  if (!resp.ok) throw new Error('Failed to fetch database: ' + resp.status);
  const buf = await resp.arrayBuffer();
  const rawDb = new SQL.Database(new Uint8Array(buf));
  return { exec: (sql, params) => rawDb.exec(sql, params || []) };
}

function _execRows(resultSet) {
  if (!resultSet || !resultSet.length) return [];
  return resultSet[0].values || [];
}

async function fetchClubWagHistory(handle, clubCode) {
  const sql = `
    SELECT r.athlete, r.vault, r.bars, r.beam, r.floor, r.total,
           e.level, e.division, c.season,
           COALESCE(c.date, c.season || '-06-01') AS d, c.name
    FROM results r
    JOIN events e ON e.id = r.event_id
    JOIN competitions c ON c.id = e.competition_id
    WHERE r.club = ? AND c.sport = 'WAG' AND e.event_type = 'AA'
      AND e.level BETWEEN 1 AND 10
      AND r.athlete IS NOT NULL AND r.athlete != ''
    ORDER BY d DESC
  `;
  const raw = _execRows(await handle.exec(sql, [clubCode]));
  return raw.map(([athlete, vault, bars, beam, floor, total, level, division, season, d, comp]) => ({
    athlete, vault, bars, beam, floor, total, level, division, season, d, comp,
  }));
}

// ─────────────────────────────────────────────────────────────────────────
// Grouping + stats
// ─────────────────────────────────────────────────────────────────────────
function levelDivDisplay(level, division) {
  return division ? `L${level} Div ${division}` : `L${level}`;
}

function buildAthleteIndex(historyRows) {
  const byAthlete = new Map();
  for (const r of historyRows) {
    if (!byAthlete.has(r.athlete)) byAthlete.set(r.athlete, []);
    byAthlete.get(r.athlete).push(r);
  }
  const athletes = [];
  for (const [name, rows] of byAthlete) {
    const currentLvl = rows[0].level;
    athletes.push({
      name,
      currentLevel: currentLvl,
      currentDivision: rows[0].division,
      allRows: rows,
      levelRows: rows.filter(r => r.level === currentLvl),
    });
  }
  return athletes;
}

function avg(arr) {
  return arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null;
}

function computeAthleteStats(athlete, currentSeason) {
  const stats = {};
  for (const key of [...APPS, 'AA']) {
    const col = key === 'AA' ? 'total' : APP_COL[key];
    const vals = athlete.levelRows
      .map(r => ({ v: r[col], season: r.season }))
      .filter(x => x.v != null && x.v > 0);
    const recent3 = vals.slice(0, 3);
    const seasonVals = vals.filter(x => x.season === currentSeason);
    stats[key] = {
      recent3: avg(recent3.map(x => x.v)),
      season: avg(seasonVals.map(x => x.v)),
      allTime: avg(vals.map(x => x.v)),
      pb: vals.length ? Math.max(...vals.map(x => x.v)) : null,
      n: vals.length,
    };
  }
  return stats;
}

function organiseByLevel(historyRows) {
  const athletes = buildAthleteIndex(historyRows);
  const currentSeason = historyRows.reduce((max, r) => (r.season > max ? r.season : max), '');
  for (const a of athletes) a.stats = computeAthleteStats(a, currentSeason);

  const byLevel = new Map();
  for (const a of athletes) {
    if (!byLevel.has(a.currentLevel)) byLevel.set(a.currentLevel, []);
    byLevel.get(a.currentLevel).push(a);
  }
  for (const list of byLevel.values()) list.sort((a, b) => a.name.localeCompare(b.name));
  return byLevel;
}

// ─────────────────────────────────────────────────────────────────────────
// Autofill
// ─────────────────────────────────────────────────────────────────────────
function autoFillBestAA(pool) {
  return [...pool]
    .filter(a => a.stats.AA.pb != null)
    .sort((a, b) => b.stats.AA.pb - a.stats.AA.pb)
    .slice(0, TEAM_SIZE);
}

function autoFillBestAverage(pool) {
  return [...pool]
    .filter(a => a.stats.AA.allTime != null)
    .sort((a, b) => b.stats.AA.allTime - a.stats.AA.allTime)
    .slice(0, TEAM_SIZE);
}

function autoFillApparatusStrength(pool, basis) {
  const picked = [];
  const pickedNames = new Set();
  let i = 0;
  let guard = 0;
  const guardLimit = APPS.length * pool.length + 20;
  while (picked.length < TEAM_SIZE && guard < guardLimit) {
    const app = APPS[i % APPS.length];
    const candidate = pool
      .filter(a => !pickedNames.has(a.name) && a.stats[app][basis] != null)
      .sort((a, b) => b.stats[app][basis] - a.stats[app][basis])[0];
    if (candidate) {
      picked.push(candidate);
      pickedNames.add(candidate.name);
    }
    i++;
    guard++;
  }
  return picked;
}

// ─────────────────────────────────────────────────────────────────────────
// Predicted score (5 up, 3 count)
// ─────────────────────────────────────────────────────────────────────────
function computeTeamPrediction(currentTeam, basis) {
  const perApp = {};
  let predictedTeamScore = 0;
  let anyMissing = currentTeam.length < TEAM_SIZE;

  for (const app of APPS) {
    const entries = currentTeam
      .map(a => ({ name: a.name, v: a.stats[app][basis] }))
      .filter(e => e.v != null)
      .sort((a, b) => b.v - a.v);
    if (entries.length < currentTeam.length) anyMissing = true;
    const counting = entries.slice(0, 3);
    const subtotal = counting.reduce((s, e) => s + e.v, 0);
    perApp[app] = { subtotal, counting };
    predictedTeamScore += subtotal;
  }

  const aaEntries = currentTeam
    .map(a => ({ name: a.name, v: a.stats.AA[basis] }))
    .filter(e => e.v != null)
    .sort((a, b) => b.v - a.v);
  const aaCounting = aaEntries.slice(0, 3);
  const predictedAA = aaCounting.reduce((s, e) => s + e.v, 0);

  return { perApp, predictedTeamScore, predictedAA, aaCounting, incomplete: anyMissing };
}

// ─────────────────────────────────────────────────────────────────────────
// Rendering
// ─────────────────────────────────────────────────────────────────────────
function fmtScore(v) {
  return v == null ? '-' : v.toFixed(3);
}

function statCell(stats, key, basis) {
  const s = stats[key];
  const val = fmtScore(s[basis]);
  if (val === '-' || !showCounts || s.n >= 3) return val;
  const plural = s.n === 1 ? '' : 's';
  return `${val} <span class="tp-n tp-n-low" title="Based on only ${s.n} result${plural} at this level">(n=${s.n})</span>`;
}

function renderLevelSelect() {
  const sel = document.getElementById('tp-level-select');
  const levels = [...athletesByLevel.keys()].sort((a, b) => a - b);
  sel.innerHTML = '<option value="">Select a level…</option>' +
    levels.map(lvl => `<option value="${lvl}">Level ${lvl} (${athletesByLevel.get(lvl).length} gymnasts)</option>`).join('');
}

function renderBasisSelect() {
  const sel = document.getElementById('tp-basis-select');
  sel.innerHTML = Object.entries(BASIS_LABEL)
    .map(([k, label]) => `<option value="${k}"${k === currentBasis ? ' selected' : ''}>${label}</option>`)
    .join('');
}

function renderAll() {
  renderPool();
  renderTeam();
}

function renderPool() {
  const body = document.getElementById('tp-pool-body');
  if (currentLevel === null) {
    body.innerHTML = `<tr><td colspan="7" class="tp-empty">Select a level above to load its gymnasts.</td></tr>`;
    return;
  }
  const pool = athletesByLevel.get(currentLevel) || [];
  const teamNames = new Set(team.map(a => a.name));
  if (!pool.length) {
    body.innerHTML = `<tr><td colspan="7" class="tp-empty">No gymnasts found at this level.</td></tr>`;
    return;
  }
  body.innerHTML = pool.map(a => {
    const inTeam = teamNames.has(a.name);
    const full = team.length >= TEAM_SIZE;
    return `
      <tr data-name="${escAttr(a.name)}">
        <td>${escHtml(a.name)}<div class="tp-sub">${escHtml(levelDivDisplay(a.currentLevel, a.currentDivision))}</div></td>
        ${APPS.map(app => `<td>${statCell(a.stats, app, currentBasis)}</td>`).join('')}
        <td>${statCell(a.stats, 'AA', currentBasis)}</td>
        <td><button class="tp-btn tp-btn-add" ${inTeam || full ? 'disabled' : ''} data-action="add">${inTeam ? 'In Team' : 'Add'}</button></td>
      </tr>`;
  }).join('');

  body.querySelectorAll('button[data-action="add"]').forEach(btn => {
    btn.addEventListener('click', () => {
      const name = btn.closest('tr').dataset.name;
      addToTeam(name);
    });
  });
}

function renderTeam() {
  const body = document.getElementById('tp-team-body');
  const countEl = document.getElementById('tp-team-count');
  countEl.textContent = `${team.length} of ${TEAM_SIZE}`;

  if (!team.length) {
    body.innerHTML = `<tr><td colspan="7" class="tp-empty">No gymnasts picked yet. Add from the pool, or use an autofill button below.</td></tr>`;
  } else {
    const maxByApp = {};
    for (const app of APPS) {
      const vals = team.map(a => a.stats[app][currentBasis]).filter(v => v != null);
      maxByApp[app] = vals.length ? Math.max(...vals) : null;
    }

    body.innerHTML = team.map((a, idx) => `
      <tr data-name="${escAttr(a.name)}">
        <td>${escHtml(a.name)}</td>
        ${APPS.map(app => {
          const v = a.stats[app][currentBasis];
          const isTop = team.length > 1 && maxByApp[app] != null && v === maxByApp[app];
          return `<td class="${isTop ? 'tp-specialist' : ''}" title="${isTop ? `Best ${APP_LABEL[app]} in this lineup` : ''}">${statCell(a.stats, app, currentBasis)}</td>`;
        }).join('')}
        <td>${statCell(a.stats, 'AA', currentBasis)}</td>
        <td class="tp-team-actions">
          <button class="tp-btn tp-icon" data-action="up" ${idx === 0 ? 'disabled' : ''} title="Move up">↑</button>
          <button class="tp-btn tp-icon" data-action="down" ${idx === team.length - 1 ? 'disabled' : ''} title="Move down">↓</button>
          <button class="tp-btn tp-icon tp-btn-remove" data-action="remove" title="Remove">×</button>
        </td>
      </tr>`).join('');
  }

  body.querySelectorAll('button').forEach(btn => {
    btn.addEventListener('click', () => {
      const name = btn.closest('tr').dataset.name;
      const action = btn.dataset.action;
      if (action === 'remove') removeFromTeam(name);
      else if (action === 'up') moveTeamMember(name, -1);
      else if (action === 'down') moveTeamMember(name, 1);
    });
  });

  renderPrediction();
}

function contributorList(entries) {
  return entries.map(e => `${e.name} ${fmtScore(e.v)}`).join(', ');
}

function renderPrediction() {
  const el = document.getElementById('tp-prediction');
  if (!team.length) {
    el.innerHTML = '';
    return;
  }
  const pred = computeTeamPrediction(team, predictionBasis);
  const appRows = APPS.map(app => `
    <div class="tp-pred-row">
      <span class="tp-pred-label">${appIcon(app)}${APP_LABEL[app]}<span class="tp-pred-who">${contributorList(pred.perApp[app].counting)}</span></span>
      <span class="tp-pred-val">${fmtScore(pred.perApp[app].subtotal)}</span>
    </div>`).join('');

  el.innerHTML = `
    <div class="tp-pred-toggle">
      <span class="tp-sub">Predicted scores based on:</span>
      <button class="tp-btn tp-pred-toggle-btn${predictionBasis === 'allTime' ? ' active' : ''}" data-basis="allTime">Average</button>
      <button class="tp-btn tp-pred-toggle-btn${predictionBasis === 'pb' ? ' active' : ''}" data-basis="pb">Best</button>
    </div>

    <div class="tp-pred-block">
      <h3>Team score <span class="tp-sub">(best 3 of ${team.length}, counted separately on each apparatus)</span></h3>
      ${appRows}
      <div class="tp-pred-row tp-pred-total">
        <span class="tp-pred-label">Total</span>
        <span class="tp-pred-val">${fmtScore(pred.predictedTeamScore)}</span>
      </div>
    </div>

    <div class="tp-pred-block tp-pred-alt">
      <h3>All-around comparison <span class="tp-sub">(same 3 gymnasts, all 4 events)</span></h3>
      <div class="tp-pred-row">
        <span class="tp-pred-label">${contributorList(pred.aaCounting)}</span>
        <span class="tp-pred-val">${fmtScore(pred.predictedAA)}</span>
      </div>
      <p class="tp-note">This locks in your 3 strongest all-arounders across every event. The team score above can score higher when a gymnast outside that trio is still one of the best on a specific apparatus and gets rotated in there instead.</p>
    </div>

    ${pred.incomplete ? '<p class="tp-warn">Team incomplete or missing scores on some apparatus, predictions are based on what data is available.</p>' : ''}
  `;

  el.querySelectorAll('.tp-pred-toggle-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      predictionBasis = btn.dataset.basis;
      renderPrediction();
    });
  });
}

function escHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function escAttr(s) {
  return escHtml(s);
}

// ─────────────────────────────────────────────────────────────────────────
// Team mutation
// ─────────────────────────────────────────────────────────────────────────
function findInPool(name) {
  const pool = athletesByLevel.get(currentLevel) || [];
  return pool.find(a => a.name === name);
}

function addToTeam(name) {
  if (team.length >= TEAM_SIZE) return;
  if (team.some(a => a.name === name)) return;
  const a = findInPool(name);
  if (!a) return;
  team.push(a);
  renderAll();
}

function removeFromTeam(name) {
  team = team.filter(a => a.name !== name);
  renderAll();
}

function moveTeamMember(name, dir) {
  const idx = team.findIndex(a => a.name === name);
  const swapIdx = idx + dir;
  if (idx < 0 || swapIdx < 0 || swapIdx >= team.length) return;
  [team[idx], team[swapIdx]] = [team[swapIdx], team[idx]];
  renderTeam();
}

// ─────────────────────────────────────────────────────────────────────────
// Clipboard
// ─────────────────────────────────────────────────────────────────────────
function buildClipboardText() {
  const lvl = currentLevel === null ? '' : `Level ${currentLevel}`;
  const lines = [
    `Team Picker, ${clubInfo.name}, ${lvl}, Basis: ${BASIS_LABEL[currentBasis]}`,
    '',
  ];
  team.forEach((a, idx) => {
    const parts = APPS.map(app => `${app} ${fmtScore(a.stats[app][currentBasis])}`).join('  ');
    lines.push(`${idx + 1}. ${a.name}  ${parts}  AA ${fmtScore(a.stats.AA[currentBasis])}`);
  });
  if (team.length) {
    const pred = computeTeamPrediction(team, predictionBasis);
    lines.push('');
    lines.push(`Team score (best 3 of ${team.length}, counted separately on each apparatus, predicted using: ${predictionBasis === 'pb' ? 'Best' : 'Average'}):`);
    for (const app of APPS) {
      lines.push(`  ${APP_LABEL[app]}: ${contributorList(pred.perApp[app].counting)} = ${fmtScore(pred.perApp[app].subtotal)}`);
    }
    lines.push(`  Total: ${fmtScore(pred.predictedTeamScore)}`);
    lines.push('');
    lines.push(`All-around comparison (same 3 gymnasts, all 4 events): ${contributorList(pred.aaCounting)} = ${fmtScore(pred.predictedAA)}`);
  }
  return lines.join('\n');
}

function fallbackCopy(text) {
  const ta = Object.assign(document.createElement('textarea'), {
    value: text, style: 'position:fixed;opacity:0',
  });
  document.body.appendChild(ta);
  ta.select();
  try { document.execCommand('copy'); showToast('Copied!'); }
  catch { prompt('Copy this text:', text); }
  document.body.removeChild(ta);
}

function showToast(msg) {
  const toast = document.getElementById('tp-toast');
  toast.textContent = msg;
  toast.classList.add('show');
  setTimeout(() => toast.classList.remove('show'), 2400);
}

function buildNamesOnlyText() {
  return team.map((a, idx) => `${idx + 1}. ${a.name}`).join('\n');
}

function copyText(text) {
  if (navigator.clipboard) {
    navigator.clipboard.writeText(text).then(() => showToast('Copied!')).catch(() => fallbackCopy(text));
  } else {
    fallbackCopy(text);
  }
}

function copyTeamToClipboard() {
  copyText(buildClipboardText());
}

function copyNamesToClipboard() {
  copyText(buildNamesOnlyText());
}

// ─────────────────────────────────────────────────────────────────────────
// Wiring
// ─────────────────────────────────────────────────────────────────────────
function showLoading(msg) {
  document.getElementById('tp-status').textContent = msg;
}

async function init() {
  clubInfo = checkAccess();
  if (!clubInfo) {
    renderAccessDenied();
    return;
  }

  document.getElementById('tp-club-name').textContent = clubInfo.name;
  showLoading('Loading database…');

  try {
    dbHandle = await loadWagDbHandle();
    const historyRows = await fetchClubWagHistory(dbHandle, clubInfo.code);
    athletesByLevel = organiseByLevel(historyRows);
    showLoading('');
  } catch (err) {
    console.error(err);
    showLoading('Could not load data, please refresh to try again.');
    return;
  }

  renderLevelSelect();
  renderBasisSelect();
  renderAll();

  document.getElementById('tp-level-select').addEventListener('change', e => {
    const val = e.target.value;
    currentLevel = val === '' ? null : Number(val);
    team = [];
    renderAll();
  });

  document.getElementById('tp-basis-select').addEventListener('change', e => {
    currentBasis = e.target.value;
    renderAll();
  });

  document.getElementById('tp-toggle-counts').addEventListener('change', e => {
    showCounts = e.target.checked;
    renderAll();
  });

  document.getElementById('tp-fill-aa').addEventListener('click', () => {
    if (currentLevel === null) return;
    team = autoFillBestAA(athletesByLevel.get(currentLevel) || []);
    renderAll();
  });
  document.getElementById('tp-fill-avg').addEventListener('click', () => {
    if (currentLevel === null) return;
    team = autoFillBestAverage(athletesByLevel.get(currentLevel) || []);
    renderAll();
  });
  document.getElementById('tp-fill-apparatus-peak').addEventListener('click', () => {
    if (currentLevel === null) return;
    team = autoFillApparatusStrength(athletesByLevel.get(currentLevel) || [], 'pb');
    renderAll();
  });
  document.getElementById('tp-fill-apparatus-avg').addEventListener('click', () => {
    if (currentLevel === null) return;
    team = autoFillApparatusStrength(athletesByLevel.get(currentLevel) || [], 'allTime');
    renderAll();
  });
  document.getElementById('tp-copy').addEventListener('click', copyTeamToClipboard);
  document.getElementById('tp-copy-names').addEventListener('click', copyNamesToClipboard);
}

window.addEventListener('DOMContentLoaded', init);
