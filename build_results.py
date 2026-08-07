#!/usr/bin/env python3
"""
build_results.py — Generate static HTML result pages for competitions.

Usage:
    python build_results.py                          # all sports, all comps
    python build_results.py --sport WAG              # all WAG comps
    python build_results.py --sport WAG --comp senior-victorian-championships-2026
    python build_results.py --sport MAG --comp senior-victorian-championships-2026
"""

import argparse
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL     = "https://stickthelanding.com.au"
DATA_DIR     = Path("data")
RESULTS_DIR  = Path("results")
CSS_PATH     = RESULTS_DIR / "results.css"
INDEX_PATH   = DATA_DIR / "results_index.json"   # consumed by generate_sitemap.py
BUILD_VER    = datetime.now().strftime("%Y%m%d%H%M")

WAG_INT_LEVELS = {
    101: "Developing Int",
    102: "Future Int",
    103: "Junior Int",
    104: "Senior Int",
    105: "Developing Int 16+",
}

MAG_INT_LEVELS = {
    104: "Senior",
}

# (db_column, display_label)
WAG_AA_COLS  = [("vault","VT"), ("bars","UB"), ("beam","BB"), ("floor","FX"), ("total","Total")]
MAG_AA_COLS  = [("floor","FX"), ("pommel","PH"), ("rings","SR"), ("vault","VT"), ("pbars","PB"), ("hbar","HB"), ("total","Total")]
ACRO_AA_COLS = [("diff","D"), ("exec_score","E"), ("art","Art"), ("pen","Pen"), ("total","Total")]

# For MAG apparatus-specific events: which DB column holds the score
MAG_APP_COL  = {"FX":"floor","PH":"pommel","SR":"rings","VT":"vault","PB":"pbars","HB":"hbar"}

EVENT_ORDER  = {"AA":0,"FX":1,"PH":2,"SR":3,"VT":4,"PB":5,"HB":6,"Team":99}
EVENT_LABEL  = {
    "AA":"All Around","Team":"Team",
    "FX":"Floor Exercise","PH":"Pommel Horse","SR":"Still Rings",
    "VT":"Vault","PB":"Parallel Bars","HB":"High Bar",
}

SPORT_COLOUR = {"WAG":"#c9a4ff","MAG":"#5ee6a8","ACRO":"#fb923c"}
SPORT_FULL   = {"WAG":"Women's Artistic Gymnastics","MAG":"Men's Artistic Gymnastics","ACRO":"Acrobatic Gymnastics"}

# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_conn(sport: str) -> sqlite3.Connection:
    path = DATA_DIR / f"stick_{sport}.db"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def fetch_competitions(con, comp_id):
    if comp_id:
        rows = con.execute("SELECT * FROM competitions WHERE id = ?", (comp_id,)).fetchall()
        if not rows:
            raise SystemExit(f"Competition '{comp_id}' not found in DB.")
        return rows
    return con.execute("SELECT * FROM competitions ORDER BY date DESC, name").fetchall()


def fetch_rows(con, comp_id: str, sport: str):
    if sport == "ACRO":
        return con.execute("""
            SELECT e.level, e.category, e.event_type,
                   r.rank, r.athletes, r.club,
                   r.diff, r.exec_score, r.art, r.pen, r.total, r.bal, r.dyn, r.com
            FROM events e JOIN results r ON r.event_id = e.id
            WHERE e.competition_id = ?
            ORDER BY e.level, e.category, e.event_type, r.rank
        """, (comp_id,)).fetchall()
    return con.execute("""
        SELECT e.level, e.division, e.event_type,
               r.rank, r.athlete, r.club, r.team_name,
               r.vault, r.bars, r.beam, r.floor, r.total,
               r.pommel, r.rings, r.pbars, r.hbar
        FROM events e JOIN results r ON r.event_id = e.id
        WHERE e.competition_id = ?
        ORDER BY e.level, e.division, e.event_type, r.rank
    """, (comp_id,)).fetchall()


def fetch_club_names(con) -> dict:
    try:
        return {r["code"]: r["name"] for r in con.execute("SELECT code, name FROM clubs")}
    except sqlite3.OperationalError:
        return {}


# ── Data grouping ─────────────────────────────────────────────────────────────

def build_tree(rows, sport: str) -> dict:
    """Returns {level: {div_key: {event_type: [row_dicts]}}}"""
    tree = {}
    for row in rows:
        r = dict(row)
        level   = r["level"]
        div_key = r.get("category") if sport == "ACRO" else r.get("division")
        etype   = r["event_type"]
        tree.setdefault(level, {}).setdefault(div_key, {}).setdefault(etype, []).append(r)
    return tree


def sorted_levels(tree: dict, sport: str) -> list:
    keys = [k for k in tree if k is not None]
    if sport == "ACRO":
        return sorted(keys, key=str)
    return sorted(keys)


def sorted_divs(level_tree: dict) -> list:
    return sorted(level_tree.keys(), key=lambda x: (x is not None, x or 0))


def sorted_etypes(div_tree: dict) -> list:
    return sorted(div_tree.keys(), key=lambda e: EVENT_ORDER.get(e, 50))


# ── Formatting ─────────────────────────────────────────────────────────────────

def _is_mixed_club(code: str) -> bool:
    import re
    return "/" in code or bool(re.match(r"^MX\d", code, re.I)) or bool(re.match(r"^MIX$", code, re.I))


def fmt(v) -> str:
    if v is None:
        return "-"
    try:
        return f"{float(v):.3f}"
    except (TypeError, ValueError):
        return str(v)


def level_label(level, sport: str) -> str:
    if sport == "ACRO":
        return str(level)
    mapping = WAG_INT_LEVELS if sport == "WAG" else MAG_INT_LEVELS if sport == "MAG" else {}
    if level in mapping:
        return mapping[level]
    return f"Level {level}"


def fmt_date(date_str: str) -> str:
    if not date_str:
        return ""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{d.day} {d.strftime('%B')} {d.year}"
    except Exception:
        return date_str


def safe_id(value) -> str:
    return re.sub(r"[^a-z0-9]", "-", str(value).lower()).strip("-")


# ── Table columns ─────────────────────────────────────────────────────────────

def table_cols(event_type: str, sport: str) -> list:
    if sport == "ACRO":
        return ACRO_AA_COLS
    if event_type == "AA":
        return WAG_AA_COLS if sport == "WAG" else MAG_AA_COLS
    if event_type == "Team":
        return WAG_AA_COLS if sport == "WAG" else MAG_AA_COLS
    if sport == "MAG" and event_type in MAG_APP_COL:
        col = MAG_APP_COL[event_type]
        return [(col, event_type), ("total", "Total")]
    return [("total", "Total")]


# ── HTML rendering ─────────────────────────────────────────────────────────────

def render_table(rows: list, event_type: str, sport: str, club_names: dict = None) -> str:
    club_names = club_names or {}
    cols       = table_cols(event_type, sport)
    is_team    = event_type == "Team"
    is_acro    = sport == "ACRO"
    athlete_key = "athletes" if is_acro else "athlete"

    # Top-3 per apparatus column (skip 'total' — rank # already covers it)
    col_top3 = {}
    for col, _ in cols:
        if col == "total":
            continue
        vals = []
        for r in rows:
            v = r.get(col)
            if v is not None:
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    pass
        medals = ("gold", "silver", "bronze")
        col_top3[col] = {val: medals[i] for i, val in enumerate(sorted(set(vals), reverse=True)[:3])}

    # Column indices for sort: rank=0, name=1, club=2 (non-team), scores start at offset
    col_offset = 2 if is_team else 3
    head_cells = "".join(
        f'<th class="h-score sortable" data-col="{col_offset + i}" onclick="sortTable(this)">'
        f'{lbl} <span class="sort-arrow">⇅</span></th>'
        for i, (col, lbl) in enumerate(cols)
    )
    name_th    = "Club" if is_team else "Athlete"
    rank_th    = '<th class="sortable" data-col="0" onclick="sortTable(this)"># <span class="sort-arrow">⇅</span></th>'
    name_th_el = f'<th class="sortable" data-col="1" onclick="sortTable(this)">{name_th} <span class="sort-arrow">⇅</span></th>'
    club_th    = "" if is_team else '<th class="sortable" data-col="2" onclick="sortTable(this)">Club <span class="sort-arrow">⇅</span></th>'

    body_rows = []
    for r in rows:
        rank     = r.get("rank")
        rank_cls = {1: " gold", 2: " silver", 3: " bronze"}.get(rank, "")
        rank_cell = f'<td class="rank{rank_cls}">{rank or "-"}</td>'

        if is_team:
            club_code = r.get("club") or ""
            full_name = club_names.get(club_code, club_code) or r.get(athlete_key) or "-"
            squad     = r.get("team_name")
            if squad and squad != full_name:
                name_cell = f'<td class="name">{squad}<span class="name-sub">{full_name}</span></td>'
            else:
                name_cell = f'<td class="name">{full_name}</td>'
            club_cell = ""
        else:
            club_code = r.get("club") or ""
            full_name = club_names.get(club_code, club_code) or "-"
            name_cell = f'<td class="name">{r.get(athlete_key) or "-"}</td>'
            club_cell = f'<td class="club">{full_name}</td>'

        score_cells = []
        for col, _ in cols:
            raw    = r.get(col)
            medal  = ""
            if col != "total" and raw is not None:
                try:
                    medal = col_top3.get(col, {}).get(float(raw), "")
                except (TypeError, ValueError):
                    pass
            cls = f'score{" " + medal if medal else ""}'
            score_cells.append(f'<td class="{cls}">{fmt(raw)}</td>')

        body_rows.append(f"<tr>{rank_cell}{name_cell}{club_cell}{''.join(score_cells)}</tr>")

    return (
        '<div class="table-wrap"><table class="results-table">'
        f'<thead><tr>{rank_th}{name_th_el}{club_th}{head_cells}</tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        '</table></div>'
    )


def render_page(comp: sqlite3.Row, tree: dict, sport: str, club_names: dict = None) -> str:
    comp_id   = comp["id"]
    name      = comp["name"]
    date_str  = comp["date"] or ""
    colour    = SPORT_COLOUR.get(sport, "#c9a4ff")
    date_disp = fmt_date(date_str)

    levels = sorted_levels(tree, sport)

    # ── Tab buttons ──
    tab_btns = []
    for i, lvl in enumerate(levels):
        active = " active" if i == 0 else ""
        lbl    = level_label(lvl, sport)
        tab_btns.append(
            f'<button class="rtab{active}" data-panel="lvl-{safe_id(lvl)}"'
            f' onclick="switchTab(this)">{lbl}</button>'
        )

    # ── Panels ──
    panels = []
    for i, lvl in enumerate(levels):
        active     = " active" if i == 0 else ""
        panel_id   = f"lvl-{safe_id(lvl)}"
        level_tree = tree[lvl]
        divs       = sorted_divs(level_tree)

        sections = []
        for div_key in divs:
            div_tree = level_tree[div_key]
            etypes   = sorted_etypes(div_tree)

            div_heading = (
                f'<h2 class="div-heading">Division {div_key}</h2>'
                if div_key is not None else ""
            )

            blocks = []
            for etype in etypes:
                block_id = f"{panel_id}-{safe_id(div_key or 'x')}-{safe_id(etype)}"
                lbl      = EVENT_LABEL.get(etype, etype)
                rows_to_render = div_tree[etype]
                if etype == "Team":
                    rows_to_render = [r for r in rows_to_render
                                      if not _is_mixed_club(r.get("club") or "")]
                if etype == "Team" and isinstance(lvl, int) and lvl in (3, 4, 5):
                    rows_to_render = [r for r in rows_to_render if (r.get("total") or 0) >= 40]
                tbl      = render_table(rows_to_render, etype, sport, club_names)
                blocks.append(
                    f'<div class="event-block" id="{block_id}">'
                    f'<h3 class="event-heading">{lbl}</h3>{tbl}</div>'
                )

            sections.append(
                f'<div class="div-section">{div_heading}{"".join(blocks)}</div>'
            )

        panels.append(
            f'<div class="level-panel{active}" id="{panel_id}">'
            f'{"".join(sections)}</div>'
        )

    meta_desc = f"{SPORT_FULL.get(sport, sport)} results for {name}" + \
                (f", {date_disp}" if date_disp else "") + "."

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="color-scheme" content="light dark" />
  <script>!function(){{document.documentElement.dataset.theme=localStorage.getItem('stl-theme')||(matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light')}}()</script>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{name} Results — Stick The Landing</title>
  <meta name="description" content="{meta_desc}" />
  <meta property="og:type"        content="website" />
  <meta property="og:site_name"   content="Stick The Landing" />
  <meta property="og:title"       content="{name} Results — Stick The Landing" />
  <meta property="og:description" content="{meta_desc}" />
  <meta property="og:url"         content="{BASE_URL}/results/{sport.lower()}/{comp_id}/" />
  <meta property="og:image"       content="{BASE_URL}/assets/favicons/android-chrome-512x512.png" />
  <meta name="twitter:card"       content="summary" />
  <link rel="canonical"           href="{BASE_URL}/results/{sport.lower()}/{comp_id}/" />
  <link rel="apple-touch-icon" sizes="180x180" href="/assets/favicons/apple-touch-icon.png" />
  <link rel="icon" type="image/png" sizes="32x32"  href="/assets/favicons/favicon-32x32.png" />
  <link rel="icon" type="image/png" sizes="16x16"  href="/assets/favicons/favicon-16x16.png" />
  <link rel="manifest" href="/assets/favicons/site.webmanifest" />
  <link rel="shortcut icon" href="/assets/favicons/favicon.ico" />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer" />
  <link rel="stylesheet" href="/assets/nav.css?v={BUILD_VER}" />
  <link rel="stylesheet" href="/results/results.css?v={BUILD_VER}" />
</head>
<body class="results-page">

<div class="comp-header">
  <a class="comp-back" href="/competitions/{sport.lower()}/"><i class="fas fa-arrow-left"></i> Back to Competitions</a>
  <div class="comp-header-inner">
    <span class="comp-sport-badge" style="color:{colour};border-color:{colour}">{sport}</span>
    <h1 class="comp-name">{name}</h1>
    {"" if not date_disp else f'<span class="comp-date">{date_disp}</span>'}
  </div>
</div>

<main class="results-main">
<div class="tabs-bar">
  <div class="tabs-inner">{"".join(tab_btns)}</div>
</div>
{"".join(panels)}
</main>

<footer class="results-footer">
  <span>© 2026 Stick The Landing</span>
  &nbsp;·&nbsp;<a href="/about/">About</a>
  &nbsp;·&nbsp;<a href="/privacy/">Privacy Policy</a>
</footer>

<script>
function switchTab(btn) {{
  document.querySelectorAll('.rtab').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.level-panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById(btn.dataset.panel).classList.add('active');
  window.scrollTo({{top: 0, behavior: 'smooth'}});
}}
function sortTable(th) {{
  const table = th.closest('table');
  const tbody = table.querySelector('tbody');
  const col   = +th.dataset.col;
  const asc   = th.dataset.dir !== 'asc';
  table.querySelectorAll('th[data-col]').forEach(h => {{
    delete h.dataset.dir;
    h.querySelector('.sort-arrow').textContent = '⇅';
  }});
  th.dataset.dir = asc ? 'asc' : 'desc';
  th.querySelector('.sort-arrow').textContent = asc ? '↑' : '↓';
  Array.from(tbody.querySelectorAll('tr'))
    .sort((a, b) => {{
      const av = a.cells[col].textContent.trim();
      const bv = b.cells[col].textContent.trim();
      if (av === '-' && bv === '-') return 0;
      if (av === '-') return 1;
      if (bv === '-') return -1;
      const an = parseFloat(av), bn = parseFloat(bv);
      if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
      return asc ? av.localeCompare(bv) : bv.localeCompare(av);
    }})
    .forEach(r => tbody.appendChild(r));
}}
</script>
<script src="/assets/nav.js?v={BUILD_VER}" defer></script>
</body>
</html>"""


# ── Results index (for sitemap) ────────────────────────────────────────────────

def load_results_index() -> list:
    if INDEX_PATH.exists():
        return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    return []


def save_results_index(entries: list) -> None:
    INDEX_PATH.parent.mkdir(exist_ok=True)
    INDEX_PATH.write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")


def upsert_index(entries: list, sport: str, comp: sqlite3.Row) -> None:
    key = (sport.lower(), comp["id"])
    entries[:] = [e for e in entries if (e["sport"], e["comp_id"]) != key]
    entries.append({
        "sport":   sport.lower(),
        "comp_id": comp["id"],
        "name":    comp["name"],
        "date":    comp["date"] or "",
    })
    entries.sort(key=lambda e: (e["date"] or "", e["sport"]), reverse=True)


# ── Competitions index page ───────────────────────────────────────────────────

COMPS_DIR = Path("competitions")

def build_competitions_index(sport: str, con: sqlite3.Connection, results_index: list) -> Path:
    colour    = SPORT_COLOUR.get(sport, "#c9a4ff")
    sport_full = SPORT_FULL.get(sport, sport)
    built_ids = {e["comp_id"] for e in results_index if e["sport"] == sport.lower()}

    rows = con.execute(
        "SELECT id, name, date, season FROM competitions ORDER BY date DESC, name"
    ).fetchall()

    # PDF files per (competition name, year) {(name, year): [(file_path, source_url), ...]}.
    # Keyed by year too, not just name, so recurring competitions (e.g. "Casey Cup"
    # held every year) don't have every year's PDFs lumped under every row. Deduped
    # by source_url: older ingestion runs stored file_path without a year prefix,
    # creating literal duplicate rows for the same file under a different primary
    # key (competition_name, file_path) - source_url is always the full
    # "pdfs/<year>/..." repo path actually used as the href, so it's the reliable
    # identity *and* year source here, regardless of which file_path format a row has.
    pdf_map = {}
    seen_urls = set()
    try:
        pdf_rows = con.execute(
            "SELECT competition_name, file_path, source_url FROM pdf_manifest "
            "ORDER BY competition_name, file_path"
        ).fetchall()
        for name, fp, src in pdf_rows:
            dedupe_key = (name, src)
            if src:
                if dedupe_key in seen_urls:
                    continue
                seen_urls.add(dedupe_key)
            m = re.match(r"^pdfs/(\d{4})/", src or "")
            year = m.group(1) if m else None
            pdf_map.setdefault((name, year), []).append((fp, src))
    except Exception:
        pass

    # Group by season (year)
    from collections import defaultdict
    by_season = defaultdict(list)
    for row in rows:
        season = row["season"] or (row["date"][:4] if row["date"] else "Unknown")
        by_season[str(season)].append(row)

    seasons_sorted = sorted(by_season.keys(), reverse=True)
    sections = []
    for season_idx, season in enumerate(seasons_sorted):
        rows_html = []
        for c in by_season[season]:
            date_disp = fmt_date(c["date"] or "")
            pdfs      = pdf_map.get((c["name"], season), [])
            n_pdfs    = len(pdfs)

            if pdfs:
                pdf_items = "".join(
                    f'<li><a href="/{(src or "pdfs/" + fp).replace(chr(92), "/")}" target="_blank" rel="noopener">'
                    f'{Path(fp).name.replace("_ignore", "")}</a></li>'
                    for fp, src in pdfs
                    if fp
                )
                pdf_block = (
                    f'<details class="ci-pdfs">'
                    f'<summary>Original PDFs ({n_pdfs})</summary>'
                    f'<ul class="ci-pdf-list">{pdf_items}</ul>'
                    f'</details>'
                )
            else:
                pdf_block = ""

            if c["id"] in built_ids:
                rows_html.append(
                    f'<div class="ci-row">'
                    f'<a class="ci-result-link" href="/results/{sport.lower()}/{c["id"]}/">'
                    f'<span class="ci-name">{c["name"]}</span>'
                    f'<span class="ci-right">'
                    f'<span class="ci-date">{date_disp}</span>'
                    f'<i class="fas fa-chevron-right ci-arrow"></i>'
                    f'</span></a>'
                    f'{pdf_block}'
                    f'</div>'
                )
            else:
                rows_html.append(
                    f'<div class="ci-row ci-no-page">'
                    f'<div class="ci-result-link">'
                    f'<span class="ci-name">{c["name"]}</span>'
                    f'<span class="ci-right">'
                    f'<span class="ci-date">{date_disp}</span>'
                    f'</span></div>'
                    f'{pdf_block}'
                    f'</div>'
                )
        open_attr = " open" if season_idx == 0 else ""
        sections.append(
            f'<details class="ci-season-group"{open_attr}>'
            f'<summary class="ci-season">'
            f'<span class="ci-season-label">{season}</span>'
            f'<span class="ci-season-count">{len(by_season[season])}</span>'
            f'</summary>'
            f'<div class="ci-list">{"".join(rows_html)}</div>'
            f'</details>'
        )

    years     = sorted(by_season.keys())
    year_range = f"{years[0]}–{years[-1]}" if len(years) > 1 else years[0]
    n_comps   = sum(len(v) for v in by_season.values())
    og_image = {
        "WAG": f"{BASE_URL}/assets/images/wagresults.jpg",
    }.get(sport, f"{BASE_URL}/assets/favicons/android-chrome-512x512.png")
    meta_desc = (
        f"Results from {n_comps} {sport_full} competitions across the "
        f"{year_range} Victorian season — invitational, regional championships, "
        f"state trials, and Victorian Championships."
    )
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="color-scheme" content="light dark" />
  <script>!function(){{document.documentElement.dataset.theme=localStorage.getItem('stl-theme')||(matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light')}}()</script>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{sport_full} Competitions — Stick The Landing</title>
  <meta name="description" content="{meta_desc}" />
  <meta property="og:type"        content="website" />
  <meta property="og:site_name"   content="Stick The Landing" />
  <meta property="og:title"       content="{sport_full} Competitions — Stick The Landing" />
  <meta property="og:description" content="{meta_desc}" />
  <meta property="og:url"         content="{BASE_URL}/competitions/{sport.lower()}/" />
  <meta property="og:image"       content="{og_image}" />
  <meta name="twitter:card"       content="summary" />
  <link rel="canonical"           href="{BASE_URL}/competitions/{sport.lower()}/" />
  <link rel="apple-touch-icon" sizes="180x180" href="/assets/favicons/apple-touch-icon.png" />
  <link rel="icon" type="image/png" sizes="32x32"  href="/assets/favicons/favicon-32x32.png" />
  <link rel="icon" type="image/png" sizes="16x16"  href="/assets/favicons/favicon-16x16.png" />
  <link rel="manifest" href="/assets/favicons/site.webmanifest" />
  <link rel="shortcut icon" href="/assets/favicons/favicon.ico" />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer" />
  <link rel="stylesheet" href="/assets/nav.css?v={BUILD_VER}" />
  <link rel="stylesheet" href="/results/results.css?v={BUILD_VER}" />
</head>
<body class="results-page">

<div class="comp-header">
  <a class="comp-back" href="/#{ sport.lower() }"><i class="fas fa-arrow-left"></i> Back</a>
  <div class="comp-header-inner">
    <span class="comp-sport-badge" style="color:{colour};border-color:{colour}">{sport}</span>
    <h1 class="comp-name">{sport_full} Competitions</h1>
  </div>
</div>

<main class="ci-main">
{"".join(sections)}
</main>

<footer class="results-footer">
  <span>© 2026 Stick The Landing</span>
  &nbsp;·&nbsp;<a href="/about/">About</a>
  &nbsp;·&nbsp;<a href="/privacy/">Privacy Policy</a>
</footer>

<script src="/assets/nav.js?v={BUILD_VER}" defer></script>
</body>
</html>"""

    out_dir = COMPS_DIR / sport.lower()
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "index.html"
    out.write_text(html, encoding="utf-8")
    return out


# ── Main ──────────────────────────────────────────────────────────────────────

def build_one(comp: sqlite3.Row, sport: str, con: sqlite3.Connection) -> Path:
    rows = fetch_rows(con, comp["id"], sport)
    if not rows:
        print(f"  [SKIP] {comp['id']} — no results in DB")
        return None

    tree = build_tree(rows, sport)
    club_names = fetch_club_names(con)
    html = render_page(comp, tree, sport, club_names)

    out_dir = RESULTS_DIR / sport.lower() / comp["id"]
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "index.html"
    out.write_text(html, encoding="utf-8")
    return out


def main():
    parser = argparse.ArgumentParser(description="Build static result pages.")
    parser.add_argument("--sport", choices=["WAG", "MAG", "ACRO"],
                        help="Sport to build (default: all)")
    parser.add_argument("--comp", metavar="COMP_ID",
                        help="Build a single competition by ID")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild all pages even if they already exist")
    args = parser.parse_args()

    sports = [args.sport] if args.sport else ["WAG", "MAG", "ACRO"]

    # Write CSS once
    CSS_PATH.write_text(RESULTS_CSS, encoding="utf-8")

    results_index = load_results_index()
    built = []

    for sport in sports:
        try:
            con = get_conn(sport)
        except FileNotFoundError as e:
            print(f"  [SKIP] {e}")
            continue

        comps = fetch_competitions(con, args.comp)
        print(f"\n{sport}: {len(comps)} competition(s) to process")

        for comp in comps:
            out_path = RESULTS_DIR / sport.lower() / comp["id"] / "index.html"
            if out_path.exists() and not args.force and not args.comp:
                print(f"  [SKIP] {out_path.relative_to(Path('.'))}")
                continue
            out = build_one(comp, sport, con)
            if out:
                upsert_index(results_index, sport, comp)
                rel = out.relative_to(Path("."))
                print(f"  [OK]   {rel}")
                built.append(f"{BASE_URL}/results/{sport.lower()}/{comp['id']}/")

        # Rebuild the competitions index whenever we touch a full sport (not single --comp)
        if not args.comp:
            ci = build_competitions_index(sport, con, results_index)
            print(f"  [IDX]  {ci.relative_to(Path('.'))}")

        con.close()

    save_results_index(results_index)

    if built:
        print(f"\nBuilt {len(built)} page(s). Run generate_sitemap.py to update sitemap.xml.")
    else:
        print("\nNothing built.")


# ── CSS ───────────────────────────────────────────────────────────────────────

RESULTS_CSS = """\
/* ── RESULTS PAGE ──────────────────────────────────────────────────────────── */

body.results-page {
  background: #0a0b10;
  color: #f2f3fa;
  font-family: 'Space Grotesk', system-ui, sans-serif;
  font-size: 15px;
  line-height: 1.45;
  margin: 0;
  min-height: 100vh;
}

/* ── COMPETITION HEADER ─────────────────────────────────────────────────────── */

.comp-header {
  background: #13141f;
  border-bottom: 1px solid #232944;
  padding: 14px 24px;
  display: flex;
  align-items: center;
  gap: 20px;
}

.comp-back {
  color: #8d92ac;
  text-decoration: none;
  font: 600 0.8rem 'Space Grotesk', system-ui, sans-serif;
  white-space: nowrap;
  flex-shrink: 0;
  transition: color 0.12s;
}
.comp-back:hover { color: #f2f3fa; }

.comp-header-inner {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  min-width: 0;
}

.comp-sport-badge {
  font: 700 10px/1 'JetBrains Mono', monospace;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  border: 1px solid;
  padding: 4px 8px;
  border-radius: 4px;
  flex-shrink: 0;
}

.comp-name {
  font-size: 1.15rem;
  font-weight: 700;
  color: #f2f3fa;
  margin: 0;
  letter-spacing: -0.02em;
  line-height: 1.2;
}

.comp-date {
  font: 600 10px/1 'JetBrains Mono', monospace;
  color: #8d92ac;
  letter-spacing: 0.06em;
  white-space: nowrap;
}

/* ── LEVEL TABS ─────────────────────────────────────────────────────────────── */

.tabs-bar {
  margin-bottom: 24px;
  background: #13141f;
  border: 1px solid #232944;
  border-radius: 10px;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  scrollbar-width: none;
}
.tabs-bar::-webkit-scrollbar { display: none; }

.tabs-inner {
  display: flex;
  gap: 4px;
  padding: 8px;
  min-width: max-content;
}

.rtab {
  background: none;
  border: 1px solid transparent;
  border-radius: 6px;
  color: #8d92ac;
  font: 600 0.78rem 'Space Grotesk', system-ui, sans-serif;
  padding: 7px 16px;
  cursor: pointer;
  white-space: nowrap;
  transition: color 0.12s, background 0.12s;
}
.rtab:hover {
  color: #f2f3fa;
  background: rgba(255,255,255,0.06);
}
.rtab.active {
  color: #c9a4ff;
  background: rgba(201,164,255,0.1);
  border-color: rgba(201,164,255,0.3);
}

/* ── MAIN CONTENT ───────────────────────────────────────────────────────────── */

.results-main {
  max-width: 1100px;
  margin: 0 auto;
  padding: 28px 24px 64px;
}

.level-panel         { display: none; }
.level-panel.active  { display: block; }

.div-section { margin-bottom: 44px; }

.div-heading {
  font: 700 0.7rem/1 'JetBrains Mono', monospace;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: #5d6285;
  margin: 0 0 14px;
  padding-bottom: 8px;
  border-bottom: 1px solid #1e2135;
}

.event-block { margin-bottom: 32px; }

.event-heading {
  font-size: 1rem;
  font-weight: 700;
  color: #f2f3fa;
  margin: 0 0 10px;
  letter-spacing: -0.01em;
}

/* ── RESULTS TABLE ──────────────────────────────────────────────────────────── */

.table-wrap {
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
  border-radius: 10px;
  border: 1px solid #232944;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.875rem;
  white-space: nowrap;
}

.results-table thead th {
  background: #161927;
  color: #5d6285;
  font: 700 0.68rem/1 'JetBrains Mono', monospace;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  padding: 10px 12px;
  text-align: left;
  border-bottom: 1px solid #232944;
}
.results-table thead th.h-score { text-align: right; }
.results-table thead th.sortable { cursor: pointer; user-select: none; white-space: nowrap; }
.results-table thead th.sortable:hover { color: #9da3c8; }
.results-table thead th.sortable[data-dir="asc"],
.results-table thead th.sortable[data-dir="desc"] { color: #c9a4ff; }
.sort-arrow { font-size: 0.65rem; opacity: 0.45; margin-left: 3px; }
.sortable[data-dir] .sort-arrow { opacity: 1; }

.results-table tbody tr {
  border-bottom: 1px solid #13141f;
  transition: background 0.1s;
}
.results-table tbody tr:last-child { border-bottom: none; }
.results-table tbody tr:hover      { background: rgba(255,255,255,0.025); }

.results-table td {
  padding: 8px 12px;
  background: #0d0e1a;
  color: #c5c9e0;
}

.results-table td.rank {
  font: 700 0.78rem/1 'JetBrains Mono', monospace;
  color: #3a3f5c;
  width: 36px;
  text-align: center;
}
.results-table td.rank.gold   { color: #ffd24a; }
.results-table td.rank.silver { color: #b0bec5; }
.results-table td.rank.bronze { color: #cd7f32; }

.results-table td.name {
  font-weight: 600;
  color: #f2f3fa;
  min-width: 160px;
}

.results-table td.name .name-sub {
  display: block;
  font-size: 0.72rem;
  font-weight: 500;
  color: #8d92ac;
  margin-top: 2px;
}

.results-table td.club {
  color: #8d92ac;
  font-size: 0.82rem;
  min-width: 80px;
}

.results-table td.score {
  font: 500 0.875rem/1 'JetBrains Mono', monospace;
  text-align: right;
  min-width: 64px;
  color: #5a6080;
}
.results-table td.score:last-child {
  font-weight: 700;
  color: #f2f3fa;
}
.results-table td.score.gold   { color: #ffd24a; font-weight: 700; }
.results-table td.score.silver { color: #dce8f5; font-weight: 700; }
.results-table td.score.bronze { color: #cd7f32; font-weight: 700; }

/* ── COMPETITIONS INDEX ─────────────────────────────────────────────────────── */

.ci-main {
  max-width: 860px;
  margin: 0 auto;
  padding: 32px 24px 64px;
}

.ci-season-group { margin: 28px 0 0; }
.ci-season-group:first-child { margin-top: 0; }

.ci-season {
  display: flex;
  align-items: center;
  gap: 8px;
  font: 700 0.85rem/1 'JetBrains Mono', monospace;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: #c5c9e0;
  margin: 0 0 10px;
  padding-bottom: 8px;
  border-bottom: 1px solid #1e2135;
  cursor: pointer;
  user-select: none;
  list-style: none;
}
.ci-season::-webkit-details-marker { display: none; }
.ci-season::before {
  content: '▸';
  font-size: 0.7rem;
  color: #5d6285;
  flex-shrink: 0;
  transition: transform 0.15s;
}
.ci-season-group[open] > .ci-season::before { transform: rotate(90deg); }
.ci-season:hover { color: #f2f3fa; }
.ci-season-label { flex: 1; }
.ci-season-count {
  font: 600 0.68rem/1 'JetBrains Mono', monospace;
  letter-spacing: normal;
  text-transform: none;
  color: #5d6285;
  background: #14162a;
  border: 1px solid #1e2135;
  border-radius: 10px;
  padding: 2px 8px;
}

.ci-list { display: flex; flex-direction: column; gap: 4px; }

.ci-row {
  background: #0d0e1a;
  border: 1px solid #1e2135;
  border-radius: 8px;
  overflow: hidden;
  transition: border-color 0.12s;
}
.ci-row:hover { border-color: #3a3f5c; }

.ci-result-link {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 13px 16px;
  text-decoration: none;
  color: inherit;
  transition: background 0.12s;
}
.ci-result-link:hover { background: #111425; }

.ci-no-page { opacity: 0.45; }
.ci-no-page > .ci-result-link { pointer-events: none; }

.ci-name {
  font-size: 0.9rem;
  font-weight: 600;
  color: #f2f3fa;
}

.ci-right {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-shrink: 0;
}

.ci-date {
  font: 500 0.72rem/1 'JetBrains Mono', monospace;
  color: #5d6285;
}

.ci-arrow { color: #3a3f5c; font-size: 0.78rem; transition: color 0.12s; }
.ci-result-link:hover .ci-arrow { color: #c9a4ff; }

.ci-pdfs {
  margin: 8px 0 2px;
  padding: 0 16px;
}
.ci-pdfs summary {
  font: 600 0.72rem/1 'JetBrains Mono', monospace;
  color: #5d6285;
  cursor: pointer;
  user-select: none;
  list-style: none;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 4px 0;
}
.ci-pdfs summary::-webkit-details-marker { display: none; }
.ci-pdfs summary::before {
  content: '▸';
  font-size: 0.6rem;
  transition: transform 0.15s;
}
.ci-pdfs[open] summary::before { transform: rotate(90deg); }
.ci-pdfs summary:hover { color: #8d92ac; }
.ci-pdf-list {
  margin: 6px 0 4px 12px;
  padding: 0;
  list-style: none;
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.ci-pdf-list a {
  font: 500 0.75rem/1.4 'JetBrains Mono', monospace;
  color: #8d92ac;
  text-decoration: none;
  word-break: break-all;
}
.ci-pdf-list a:hover { color: #c9a4ff; text-decoration: underline; }

@media (max-width: 600px) {
  .ci-main { padding: 20px 12px 48px; }
  .ci-name  { font-size: 0.83rem; }
}

/* ── FOOTER ─────────────────────────────────────────────────────────────────── */

.results-footer {
  background: #0d0e1a;
  color: #5a5e7a;
  text-align: center;
  padding: 20px 24px;
  font-size: 0.78rem;
  border-top: 1px solid #1a1d2e;
}
.results-footer a { color: #7a7e9a; text-decoration: none; }
.results-footer a:hover { color: #f2f3fa; }

/* ── MOBILE ─────────────────────────────────────────────────────────────────── */

@media (max-width: 600px) {
  .comp-header       { padding: 12px 16px; gap: 12px; }
  .comp-name         { font-size: 0.95rem; }
  .results-main      { padding: 16px 12px 48px; }
  .div-section       { margin-bottom: 28px; }
  .event-block       { margin-bottom: 24px; }
}

/* ── LIGHT THEME ───────────────────────────────────────────────────────────── */

@media (prefers-color-scheme: light) {
  html:not([data-theme="dark"]) body.results-page { background: #f4f4f8; color: #1a1a2a; }
  html:not([data-theme="dark"]) .comp-header { background: #ffffff; border-bottom-color: #d4d4e8; }
  html:not([data-theme="dark"]) .comp-back { color: #5a5a78; }
  html:not([data-theme="dark"]) .comp-back:hover { color: #1a1a2a; }
  html:not([data-theme="dark"]) .comp-name { color: #1a1a2a; }
  html:not([data-theme="dark"]) .comp-date { color: #5a5a78; }
  html:not([data-theme="dark"]) .tabs-bar { background: #ffffff; border-color: #d4d4e8; }
  html:not([data-theme="dark"]) .rtab { color: #5a5a78; }
  html:not([data-theme="dark"]) .rtab:hover { color: #1a1a2a; background: rgba(0,0,0,0.05); }
  html:not([data-theme="dark"]) .rtab.active { color: #6b5ce7; background: rgba(107,92,231,0.1); border-color: rgba(107,92,231,0.3); }
  html:not([data-theme="dark"]) .div-heading { color: #5a5a78; border-bottom-color: #d4d4e8; }
  html:not([data-theme="dark"]) .event-heading { color: #1a1a2a; }
  html:not([data-theme="dark"]) .table-wrap { border-color: #d4d4e8; }
  html:not([data-theme="dark"]) .results-table thead th { background: #eef0f7; color: #5a5a78; border-bottom-color: #d4d4e8; }
  html:not([data-theme="dark"]) .results-table thead th.sortable:hover { color: #1a1a2a; }
  html:not([data-theme="dark"]) .results-table thead th.sortable[data-dir="asc"],
  html:not([data-theme="dark"]) .results-table thead th.sortable[data-dir="desc"] { color: #6b5ce7; }
  html:not([data-theme="dark"]) .results-table tbody tr { border-bottom-color: #eceef5; }
  html:not([data-theme="dark"]) .results-table tbody tr:hover { background: rgba(0,0,0,0.03); }
  html:not([data-theme="dark"]) .results-table td { background: #ffffff; color: #3a3a52; }
  html:not([data-theme="dark"]) .results-table td.rank { color: #b0b4cc; }
  html:not([data-theme="dark"]) .results-table td.rank.gold   { color: #b8860b; }
  html:not([data-theme="dark"]) .results-table td.rank.silver { color: #78828c; }
  html:not([data-theme="dark"]) .results-table td.rank.bronze { color: #a0522d; }
  html:not([data-theme="dark"]) .results-table td.name { color: #1a1a2a; }
  html:not([data-theme="dark"]) .results-table td.name .name-sub { color: #7a7e96; }
  html:not([data-theme="dark"]) .results-table td.club { color: #5a5a78; }
  html:not([data-theme="dark"]) .results-table td.score { color: #9296b4; }
  html:not([data-theme="dark"]) .results-table td.score:last-child { color: #1a1a2a; }
  html:not([data-theme="dark"]) .results-table td.score.gold   { color: #b8860b; }
  html:not([data-theme="dark"]) .results-table td.score.silver { color: #3d5a76; }
  html:not([data-theme="dark"]) .results-table td.score.bronze { color: #a0522d; }
  html:not([data-theme="dark"]) .ci-season { color: #4a4a68; border-bottom-color: #d4d4e8; }
  html:not([data-theme="dark"]) .ci-season::before { color: #9296b4; }
  html:not([data-theme="dark"]) .ci-season:hover { color: #1a1a2a; }
  html:not([data-theme="dark"]) .ci-season-count { color: #5a5a78; background: #eef0f7; border-color: #d4d4e8; }
  html:not([data-theme="dark"]) .ci-row { background: #ffffff; border-color: #d4d4e8; }
  html:not([data-theme="dark"]) .ci-row:hover { border-color: #b0b4cc; }
  html:not([data-theme="dark"]) .ci-result-link:hover { background: #f4f4f8; }
  html:not([data-theme="dark"]) .ci-name { color: #1a1a2a; }
  html:not([data-theme="dark"]) .ci-date { color: #5a5a78; }
  html:not([data-theme="dark"]) .ci-arrow { color: #b0b4cc; }
  html:not([data-theme="dark"]) .ci-result-link:hover .ci-arrow { color: #6b5ce7; }
  html:not([data-theme="dark"]) .ci-pdfs summary { color: #5a5a78; }
  html:not([data-theme="dark"]) .ci-pdfs summary:hover { color: #1a1a2a; }
  html:not([data-theme="dark"]) .ci-pdf-list a { color: #5a5a78; }
  html:not([data-theme="dark"]) .ci-pdf-list a:hover { color: #6b5ce7; }
  html:not([data-theme="dark"]) .results-footer { background: #ffffff; color: #5a5a78; border-top-color: #d4d4e8; }
  html:not([data-theme="dark"]) .results-footer a { color: #5a5a78; }
  html:not([data-theme="dark"]) .results-footer a:hover { color: #1a1a2a; }
  html:not([data-theme="dark"]) .comp-sport-badge[style*="#c9a4ff"] { color: #7c3aed !important; border-color: #7c3aed !important; }
  html:not([data-theme="dark"]) .comp-sport-badge[style*="#5ee6a8"] { color: #1a8e63 !important; border-color: #1a8e63 !important; }
  html:not([data-theme="dark"]) .comp-sport-badge[style*="#fb923c"] { color: #c2650f !important; border-color: #c2650f !important; }
}

html[data-theme="light"] body.results-page { background: #f4f4f8; color: #1a1a2a; }
html[data-theme="light"] .comp-header { background: #ffffff; border-bottom-color: #d4d4e8; }
html[data-theme="light"] .comp-back { color: #5a5a78; }
html[data-theme="light"] .comp-back:hover { color: #1a1a2a; }
html[data-theme="light"] .comp-name { color: #1a1a2a; }
html[data-theme="light"] .comp-date { color: #5a5a78; }
html[data-theme="light"] .tabs-bar { background: #ffffff; border-color: #d4d4e8; }
html[data-theme="light"] .rtab { color: #5a5a78; }
html[data-theme="light"] .rtab:hover { color: #1a1a2a; background: rgba(0,0,0,0.05); }
html[data-theme="light"] .rtab.active { color: #6b5ce7; background: rgba(107,92,231,0.1); border-color: rgba(107,92,231,0.3); }
html[data-theme="light"] .div-heading { color: #5a5a78; border-bottom-color: #d4d4e8; }
html[data-theme="light"] .event-heading { color: #1a1a2a; }
html[data-theme="light"] .table-wrap { border-color: #d4d4e8; }
html[data-theme="light"] .results-table thead th { background: #eef0f7; color: #5a5a78; border-bottom-color: #d4d4e8; }
html[data-theme="light"] .results-table thead th.sortable:hover { color: #1a1a2a; }
html[data-theme="light"] .results-table thead th.sortable[data-dir="asc"],
html[data-theme="light"] .results-table thead th.sortable[data-dir="desc"] { color: #6b5ce7; }
html[data-theme="light"] .results-table tbody tr { border-bottom-color: #eceef5; }
html[data-theme="light"] .results-table tbody tr:hover { background: rgba(0,0,0,0.03); }
html[data-theme="light"] .results-table td { background: #ffffff; color: #3a3a52; }
html[data-theme="light"] .results-table td.rank { color: #b0b4cc; }
html[data-theme="light"] .results-table td.rank.gold   { color: #b8860b; }
html[data-theme="light"] .results-table td.rank.silver { color: #78828c; }
html[data-theme="light"] .results-table td.rank.bronze { color: #a0522d; }
html[data-theme="light"] .results-table td.name { color: #1a1a2a; }
html[data-theme="light"] .results-table td.name .name-sub { color: #7a7e96; }
html[data-theme="light"] .results-table td.club { color: #5a5a78; }
html[data-theme="light"] .results-table td.score { color: #9296b4; }
html[data-theme="light"] .results-table td.score:last-child { color: #1a1a2a; }
html[data-theme="light"] .results-table td.score.gold   { color: #b8860b; }
html[data-theme="light"] .results-table td.score.silver { color: #3d5a76; }
html[data-theme="light"] .results-table td.score.bronze { color: #a0522d; }
html[data-theme="light"] .ci-season { color: #4a4a68; border-bottom-color: #d4d4e8; }
html[data-theme="light"] .ci-season::before { color: #9296b4; }
html[data-theme="light"] .ci-season:hover { color: #1a1a2a; }
html[data-theme="light"] .ci-season-count { color: #5a5a78; background: #eef0f7; border-color: #d4d4e8; }
html[data-theme="light"] .ci-row { background: #ffffff; border-color: #d4d4e8; }
html[data-theme="light"] .ci-row:hover { border-color: #b0b4cc; }
html[data-theme="light"] .ci-result-link:hover { background: #f4f4f8; }
html[data-theme="light"] .ci-name { color: #1a1a2a; }
html[data-theme="light"] .ci-date { color: #5a5a78; }
html[data-theme="light"] .ci-arrow { color: #b0b4cc; }
html[data-theme="light"] .ci-result-link:hover .ci-arrow { color: #6b5ce7; }
html[data-theme="light"] .ci-pdfs summary { color: #5a5a78; }
html[data-theme="light"] .ci-pdfs summary:hover { color: #1a1a2a; }
html[data-theme="light"] .ci-pdf-list a { color: #5a5a78; }
html[data-theme="light"] .ci-pdf-list a:hover { color: #6b5ce7; }
html[data-theme="light"] .results-footer { background: #ffffff; color: #5a5a78; border-top-color: #d4d4e8; }
html[data-theme="light"] .results-footer a { color: #5a5a78; }
html[data-theme="light"] .results-footer a:hover { color: #1a1a2a; }
html[data-theme="light"] .comp-sport-badge[style*="#c9a4ff"] { color: #7c3aed !important; border-color: #7c3aed !important; }
html[data-theme="light"] .comp-sport-badge[style*="#5ee6a8"] { color: #1a8e63 !important; border-color: #1a8e63 !important; }
html[data-theme="light"] .comp-sport-badge[style*="#fb923c"] { color: #c2650f !important; border-color: #c2650f !important; }
"""

if __name__ == "__main__":
    main()
