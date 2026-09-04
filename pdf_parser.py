"""
pdf_parser.py — Parses WAG results PDFs into structured JSON.

Supports three ProScore layouts and a generic table fallback:
  1. Old ProScore  : "Meet Results - Level X Division Y ..." one-line athlete records
  2. New ProScore  : BTYC/Knox multi-line records anchored on "Final:" lines
  3. Generic table : pdfplumber table extraction (last resort)

Team Results PDFs are skipped in all cases.
"""

import datetime
import json
import re
import sys
from pathlib import Path

import pdfplumber

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Multi-round ProScore header: "Meet Results - Multi Men / 8U / All Ages"
PROSCORE_MULTI_HDR = re.compile(r"Meet Results\s*-\s*Multi", re.IGNORECASE)

# Scoreholder.com footer
SCOREHOLDER_RE = re.compile(r"scoreholder\.com", re.IGNORECASE)

# Old ProScore header: "Meet Results - Level 6 Division 1 Women / 6D1"
PROSCORE_MEET_HDR = re.compile(
    r"Meet Results\s*[-\u2013]\s*Level\s+(\d+)\s+Division\s+(\d+)\s+(\w+)",
    re.IGNORECASE,
)

# Detects "Team Results" even when letters are space-separated (BTYC font issue)
TEAM_RESULTS_RE = re.compile(
    r"T\s*e\s*a\s*m\s+R\s*e\s*s\s*u\s*l\s*t\s*s"
    r"|DAY\s+1\s+STANDINGS\s+TEAM",
    re.IGNORECASE,
)

# Detects "Meet Results" in either clean or spaced form
MEET_RESULTS_RE = re.compile(
    r"M\s*e\s*e\s*t\s+R\s*e\s*s\s*u\s*l\s*t\s*s",
    re.IGNORECASE,
)

# Detects ProScore "Day 1 Standings" format (no apparatus column headers)
DAY1_STANDINGS_RE = re.compile(r"DAY\s+1\s+STANDINGS", re.IGNORECASE)

# Detects ProScore "Event Results" individual apparatus finals format (spaced title)
EVENT_RESULTS_RE = re.compile(r"E\s*v\s*e\s*n\s*t\s+R\s*e\s*s\s*u\s*l\s*t\s*s", re.IGNORECASE)

# Row in Event Results format: rank bib name gym diff exec s+ nd score out
# diff/exec/score: \d+\.\d{3} or __.___ placeholders; s+/nd: -?\d+\.\d or _._
_EVNT_ROW_RE = re.compile(
    r"^(\d+[TF*]?)\s+(\d+)\s+(.+?)\s+"
    r"([A-Z]{2,8}(?:/[A-Z]{2,8})?(?:\s+\([A-Z/]{2,12}\))?)\s+"
    r"([\d_]+\.[\d_]+)\s+([\d_]+\.[\d_]+)\s+"
    r"(-?[\d_.]+)\s+(-?[\d_.]+)\s+"
    r"([\d_]+\.[\d_]+)\s+([\d.]+)\s*$"
)

# Score token: real score OR blank placeholder
_S = r"(?:[\d]+\.[\d]+|_+\._*)"

# Old-format athlete line: "1 315 Elisha SPITERI 9.200 9.325 9.200 9.425 37.150"
ATHLETE_LINE_AA = re.compile(
    rf"^(\d+[T]?)\s+(\d+)\s+(.+?)\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s*$"
)
ATHLETE_LINE_APP = re.compile(
    rf"^(\d+[T]?)\s+(\d+)\s+(.+?)\s+({_S})\s*$"
)

CLUB_LINE = re.compile(r"^([A-Za-z]{2,6})\s+[\d]+[T*]?(?:\s+[\d]+[T*]?){1,5}\s*$")
APP_CODE = re.compile(r"\b(VT|UB|BB|FX|PH|SR|PB|HB)\b", re.IGNORECASE)

# MAG apparatus code → results dict key
MAG_APPARATUS_MAP = {
    "FX": "floor",  "VT": "vault",
    "PH": "pommel", "SR": "rings",
    "PB": "pbars",  "HB": "hbar",
}

# Age bracket inferred from level when only "U"/"Under" is present in filename
_MAG_LEVEL_AGE = {7: "U13", 8: "U14"}

# "Meet Results Women / 5A / All Ages" — captures numeric level and optional letter division
# Handles spaced characters: "M e e t R e s u lts W omen / 5B / ..."
PROSCORE_SIMPLE_HDR = re.compile(
    r"M\s*e\s*e\s*t\s+R\s*e\s*s\s*u\s*l\s*t\s*s"
    r"[^\n/]*"
    r"/\s*(\d+)\s*(D\d+|[A-Za-z]?)\s*(?:/|$)",
    re.IGNORECASE | re.MULTILINE,
)

# 6-score variant: rank bib name v ub bb fx spare total  (spare is ___.___ placeholder)
ATHLETE_LINE_AA_SPARE = re.compile(
    rf"^(\d+[T]?)\s+(\d+)\s+(.+?)\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s*$"
)

# 7-score variant: rank bib name v ub bb fx spare spare total (two spare/placeholder
# columns, seen in BTYC-style Level 7 "All Ages" sheets). Must be tried before the
# 6-score variant above, or the extra trailing number gets absorbed into the name.
ATHLETE_LINE_AA_SPARE2 = re.compile(
    rf"^(\d+[T]?)\s+(\d+)\s+(.+?)\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s*$"
)

# MAG AA: rank bib name + 6 apparatus scores + total (7 numeric tokens after name)
ATHLETE_LINE_MAG_AA = re.compile(
    rf"^(\d+[T]?)\s+(\d+)\s+(.+?)\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s*$"
)

# Club + ranks line after an athlete: "WVG 5 1 2 3 1" / "CAS CS 1 2 5 1 0T 1" / "GUN (HPP) 2 1..." / "HPP/PIT 1 2..."
# First-word cap of 14 covers spelled-out club names some hosts print instead of a
# short code (e.g. "MILLICENT"), verified against the whole existing WAG/MAG corpus
# to introduce no regressions.
_CLUB_RANKS_LINE = re.compile(r"^(?:[A-Za-z]{2,6}/)?([A-Za-z]{2,14}(?:\s+[A-Za-z]{2,6})*)(?:\s+\([A-Za-z/]+\))?\s+[\dT]")

# Lines to filter when building the cleaned line list for new-format parsing
_HEADER_SKIP = re.compile(
    r"(?:ProScore|^Printed:|^Session:|Page:\s*\d|"
    r"^Judge|^Gym\s*$|^AA\s*$|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d|"
    r"^Rank\s*Num\s+Name|^RankNum\s+Name\s+Gym|^RankNum\s+Name|^Rank\s+Gym\s+Team|"
    r"^Rank\s+Num\s+Name)",
    re.IGNORECASE,
)

# International level codes (101-105) — no division, no numeric level in filename
# More-specific keywords must appear before the generic ones (first match wins).
_INT_LEVEL_KEYWORDS = [
    ("developing international 16", 105),  # most specific — check before generic DI
    ("developing international",     101),
    ("developing open",              105),  # DO = Developing Open (16+ age group)
    ("developing 16",                105),  # filename pattern "Developing 16+" or "Developing 16-18"
    ("developing 13",                101),  # filename pattern "Developing 13-15yrs"
    ("future international",         102),
    ("junior international",         103),
    ("senior international",         104),
]
INT_LEVEL_LABELS = {101: "Dev Int", 102: "Fut Int", 103: "Jun Int", 104: "Sen Int", 105: "Dev Open"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NAME_PARTICLES = {"van", "der", "de", "von", "le", "la", "du", "den"}


def _parse_rank(s):
    try:
        return int(re.sub(r"[^0-9]", "", s))
    except ValueError:
        return None


def _clean_name(name):
    """Strip ProScore annotation characters and rejoin words broken by PDF spacing.

    ProScore glues footnote markers straight onto the name with no separating
    delimiter: "*" flags a tied placing, "#" flags a guest/non-scoring entry.
    Both are stripped here so they never end up parsed as part of the name.
    """
    name = re.sub(r"^[\*#\s]+|[\*#\s]+$", "", name)
    # Some result sheets render hyphenated surnames as "Word - Word" instead of
    # "Word-Word" (e.g. "Brand - Starkey"). Collapse it back to a tight hyphen.
    name = re.sub(r"(?<=[A-Za-z])\s+-\s+(?=[A-Za-z])", "-", name)
    # PDF text extraction sometimes inserts spaces mid-word. A token starting with
    # a lowercase letter is usually a broken fragment — join it to the previous
    # token, UNLESS it's a real lowercase surname particle ("van Praag", "de
    # Fazio"), which must stay a separate word for _normalise_name to recognise.
    tokens = name.split(" ")
    joined = [tokens[0]] if tokens else []
    for tok in tokens[1:]:
        if tok and tok[0].islower() and tok.lower() not in _NAME_PARTICLES:
            joined[-1] += tok
        else:
            joined.append(tok)
    return _normalise_name(" ".join(joined))


def _normalise_name(name):
    """Normalise athlete name to consistent title case.

    Handles: Mc/Mac prefixes, O'/D' prefixes, hyphens, parenthetical
    nicknames, and lowercase Dutch/French particles (van, der, de, von, le, la).
    """
    if not name or name.startswith("["):
        return name
    _PARTICLES = _NAME_PARTICLES
    def _cap(part):
        if not part:
            return part
        # Parenthetical nickname: (judy) -> (Judy)
        if part.startswith("(") and part.endswith(")") and len(part) > 2:
            return "(" + _cap(part[1:-1]) + ")"
        up = part.upper()
        if up.startswith("D'") and len(part) > 2:
            return "D'" + part[2:].capitalize()
        if up.startswith("O'") and len(part) > 2:
            return "O'" + part[2:].capitalize()
        # Mac prefix: require at least 3 chars after "Mac" (avoids Macy, Macey, Macie)
        if up.startswith("MAC") and len(part) > 5:
            return "Mac" + part[3:].capitalize()
        if up.startswith("MC") and len(part) > 2:
            return "Mc" + part[2:].capitalize()
        return part.capitalize()
    words = name.split()
    result = []
    for word in words:
        if word.lower() in _PARTICLES:
            result.append(word.lower())
        elif "-" in word:
            result.append("-".join(_cap(p) for p in word.split("-")))
        else:
            result.append(_cap(word))
    return " ".join(result)


def _parse_score(s):
    if s is None or "_" in str(s):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _div_from_letter(letter):
    """'A'→1, 'B'→2, 'C'→3, ''→None; non-A-E letters (e.g. 'U' = Under) return None."""
    if not letter or letter.upper() not in "ABCDE":
        return None
    return ord(letter.upper()) - ord("A") + 1


def _parse_final_line(line):
    """'Final: 13.4 11.8 11.4 11.8 48.4' → ([13.4, 11.8, 11.4, 11.8], 48.4)
    Variable number of apparatus scores; last token is always the total."""
    tokens = line.split()  # first token is "Final:"
    nums = [_parse_score(t) for t in tokens[1:]]
    if len(nums) >= 5:
        return nums[:-1], nums[-1]
    return [], None


# Apparatus column order for score-positional mapping
_WAG_COL_ORDER = ["vault", "bars", "beam", "floor"]
_MAG_COL_ORDER = ["floor", "pommel", "rings", "vault", "pbars", "hbar"]


def _build_app_scores(totals, d_scores, e_scores, sport):
    """Return flat dict of apparatus score columns for a result row."""
    cols = _MAG_COL_ORDER if sport == "MAG" else _WAG_COL_ORDER
    row = {}
    for i, col in enumerate(cols):
        row[col]           = totals[i]   if i < len(totals)   else None
        row[f"{col}_d"]    = d_scores[i] if i < len(d_scores) else None
        row[f"{col}_e"]    = e_scores[i] if i < len(e_scores) else None
    return row


# ---------------------------------------------------------------------------
# Old ProScore parser (one-line athlete records)
# ---------------------------------------------------------------------------

_PROSCORE_SKIP = ("ND:", "Final:", "Place:", "D/E:", "Diff:", "DN/DE:", "Exec:")


def _parse_new_proscore_page(lines, page_results, sport="WAG"):
    """Parse one page of new ProScore format (multi-line records anchored on 'Final:')."""
    n_app = 6 if sport == "MAG" else 4
    for i, line in enumerate(lines):
        if not line.startswith("Final:"):
            continue
        scores, total = _parse_final_line(line)
        if total is None:
            continue

        rank = bib = name = club = None
        d_scores = []
        e_scores = []

        for offset in range(1, 7):
            j = i - offset
            if j < 0:
                break
            l = lines[j]

            if not d_scores:
                diff_m = re.search(r"\bDiff:\s+([\d.]+(?:\s+[\d.]+)*)", l)
                if diff_m:
                    d_scores = [_parse_score(x) for x in diff_m.group(1).split()][:n_app]

            if not e_scores:
                exec_m = re.search(r"\bExec:\s+([\d.]+(?:\s+[\d.]+)*)", l)
                if exec_m:
                    e_scores = [_parse_score(x) for x in exec_m.group(1).split()][:n_app]

            if club is None and not l.startswith(_PROSCORE_SKIP):
                m_c = re.match(r"^(?:[A-Za-z]{2,6}\s*/\s*)?([A-Za-z]{2,12})(?:\s+.*)?$", l)
                if m_c:
                    club = m_c.group(1).upper()

            if rank is None:
                left = re.split(r"\s+(?:Diff:|D/E:|DN/DE::?)", l)[0].strip()
                m_r = re.match(r"^(\d+[T]?)\s+(\d+)\s+(.+)$", left)
                if m_r:
                    rank = _parse_rank(m_r.group(1))
                    bib = m_r.group(2)
                    name = _clean_name(m_r.group(3))

        if rank is not None and name and club and total is not None:
            d = (d_scores + [None] * n_app)[:n_app] if d_scores else [None] * n_app
            e = (e_scores + [None] * n_app)[:n_app] if e_scores else [None] * n_app
            app_totals = (scores + [None] * n_app)[:n_app]
            row = {"rank": rank, "bib": bib, "athlete": name, "club": club, "total": total}
            row.update(_build_app_scores(app_totals, d, e, sport))
            page_results.append(row)


def parse_proscore_text(text_pages, sport="WAG"):
    events_by_key = {}  # (level, div, event_type) -> results list
    for text in text_pages:
        if not text or TEAM_RESULTS_RE.search(text):
            continue
        lines = [l.rstrip() for l in text.splitlines() if l.strip()]

        level = div = None
        event_type = "AA"
        for line in lines:
            m = PROSCORE_MEET_HDR.search(line)
            if m:
                level = int(m.group(1))
                div = int(m.group(2))
                app_m = APP_CODE.search(line)
                if app_m:
                    event_type = app_m.group(1).upper()
                break

        if level is None:
            continue

        key = (level, div, event_type)
        page_results = events_by_key.setdefault(key, [])

        # New ProScore format: multi-line records anchored on "Final:"
        if "Final:" in text:
            _parse_new_proscore_page(lines, page_results, sport=sport)
            continue

        prev_athlete = None
        for line in lines:
            m = ATHLETE_LINE_AA.match(line)
            if m:
                rank_str, bib, name, v, ub, bb, fx, total = m.groups()
                prev_athlete = {
                    "rank": _parse_rank(rank_str),
                    "bib": bib.strip(),
                    "athlete": _clean_name(name),
                    "club": None,
                    "vault": _parse_score(v),
                    "bars": _parse_score(ub),
                    "beam": _parse_score(bb),
                    "floor": _parse_score(fx),
                    "total": _parse_score(total),
                }
                page_results.append(prev_athlete)
                continue

            m = ATHLETE_LINE_APP.match(line)
            if m:
                rank_str, bib, name, score = m.groups()
                rec = {
                    "rank": _parse_rank(rank_str),
                    "bib": bib.strip(),
                    "athlete": _clean_name(name),
                    "club": None,
                    "total": _parse_score(score),
                }
                if event_type == "VT":
                    rec["vault"] = _parse_score(score)
                elif event_type == "UB":
                    rec["bars"] = _parse_score(score)
                elif event_type == "BB":
                    rec["beam"] = _parse_score(score)
                elif event_type == "FX":
                    rec["floor"] = _parse_score(score)
                page_results.append(rec)
                prev_athlete = rec
                continue

            if prev_athlete and CLUB_LINE.match(line):
                prev_athlete["club"] = line.split()[0].upper()
                prev_athlete = None

    return [
        {"level": lvl, "division": div, "event_type": et, "results": results}
        for (lvl, div, et), results in events_by_key.items()
        if results
    ]


# ---------------------------------------------------------------------------
# New ProScore parser (BTYC / Knox multi-line records, anchored on "Final:")
# ---------------------------------------------------------------------------

def parse_new_proscore(text_pages, pdf_path, sport="WAG"):
    """
    Parse BTYC and Knox style ProScore PDFs.

    Both formats have athlete records that end with:
        Final: f1 f2 ... fN total
        Place:  p1 p2 ... pN overall

    BTYC layout (3 lines before Final):
        {rank} {bib} {name} Diff: ...
        {club} Exec: ...
        ND: ...
        Final: ...

    Knox layout (3 lines before Final):
        {rank} {bib} {name} D/E: ...
        ND: ...
        {club}
        Final: ...
    """
    meta = parse_filename_meta(pdf_path)

    # Collect all non-header/non-footer lines across pages into a flat list
    clean_lines = []
    for text in text_pages:
        if not text:
            continue
        for raw in text.splitlines():
            l = raw.strip()
            if not l:
                continue
            if _HEADER_SKIP.search(l):
                continue
            if TEAM_RESULTS_RE.search(l) or MEET_RESULTS_RE.search(l):
                continue
            clean_lines.append(l)

    results = []
    for i, line in enumerate(clean_lines):
        if not line.startswith("Final:"):
            continue

        scores, total = _parse_final_line(line)
        if total is None:
            continue

        # Some formats (e.g. MYC proscore) place the club on the line immediately
        # after Final: rather than before it. Check forward first so the backward
        # scan does not pick up the previous athlete's club instead.
        rank = bib = name = club = None
        d_scores = []
        e_scores = []
        _club_re = re.compile(
            r"^(?:[A-Za-z]{2,6}\s*/\s*)?([A-Za-z]{2,12}(?:\s+[A-Za-z]{1,12}){0,2})(?:\s+\([A-Za-z/]+\))?(?:\d+)?(?:\s+(?:Exec:|ExNe[A-Za-z]*::?)|\s*$)"
        )
        _skip_starts = ("ND:", "Final:", "Place:", "D/E:", "Diff:", "DN/DE:")
        if i + 1 < len(clean_lines):
            nxt = clean_lines[i + 1]
            if not nxt.startswith(_skip_starts):
                m_fwd = _club_re.match(nxt)
                if m_fwd:
                    club = m_fwd.group(1).upper()

        # Scan backward up to 5 lines for club (if not found forward), rank+name, and D/E scores
        for offset in range(1, 6):
            j = i - offset
            if j < 0:
                break
            l = clean_lines[j]

            n_app = 6 if sport == "MAG" else 4

            # D scores from "Diff: d1 d2 ... dN" (BTYC rank line suffix)
            if not d_scores:
                diff_m = re.search(r"\bDiff:\s+([\d.]+(?:\s+[\d.]+)*)", l)
                if diff_m:
                    d_scores = [_parse_score(x) for x in diff_m.group(1).split()][:n_app]

            # D/E from slash-separated pairs: "D/E: 2.5 / 9.000 ..." or "DN/DE:: ..." variant
            if not d_scores:
                de_m = re.search(r"\b(?:D/E:|DN/DE::?)\s+(.+)", l)
                if de_m:
                    rest = de_m.group(1)
                    slots = re.findall(r'(?:(\d+\.?\d*)|_+\.[\d_]*)\s*/\s*(?:(\d+\.?\d*)|_+\.[\d_]*)', rest)
                    if slots:
                        d_scores = [_parse_score(s[0]) if s[0] else None for s in slots[:n_app]]
                        e_scores = [_parse_score(s[1]) if s[1] else None for s in slots[:n_app]]
                    else:
                        nums = [n for n in [_parse_score(x) for x in re.findall(r'\d+\.?\d*', rest)] if n is not None]
                        if len(nums) >= n_app * 2:
                            d_scores = nums[0::2][:n_app]
                            e_scores = nums[1::2][:n_app]
                        elif len(nums) >= n_app:
                            d_scores = nums[:n_app]

            # E scores from "CLUB Exec: e1 e2 ..." (BTYC club line)
            if not e_scores:
                exec_m = re.search(r"\bExec:\s+([\d.]+(?:\s+[\d.]+)*)", l)
                if exec_m:
                    e_scores = [_parse_score(x) for x in exec_m.group(1).split()][:n_app]

            # Club: club code optionally followed by HPP/team annotations then Exec line
            # Handles: "PIT Exec:" / "MYC (HPP)01 Exec:" / "HPP/PIT Exec:" / "EKGA ExNeDc::" / "BTY"
            if club is None:
                m_club = re.match(
                    r"^(?:[A-Za-z]{2,6}\s*/\s*)?([A-Za-z]{2,12}(?:\s+[A-Za-z]{1,12}){0,2})(?:\s+\([A-Za-z/]+\))?(?:\d+)?(?:\s+(?:Exec:|ExNe[A-Za-z]*::?)|\s*$)", l
                )
                if m_club and not l.startswith(("ND:", "Final:", "Place:", "D/E:", "Diff:", "DN/DE:")):
                    club = m_club.group(1).upper()

            # Rank + bib + name: strip Diff:/D/E: suffix then match leading digits
            if rank is None:
                left = re.split(r"\s+(?:Diff:|D/E:|DN/DE::?)", l)[0].strip()
                m_rank = re.match(r"^(\d+[T]?)\s+(\d+)\s+(.+)$", left)
                if m_rank:
                    rank = _parse_rank(m_rank.group(1))
                    bib = m_rank.group(2)
                    name = _clean_name(m_rank.group(3))

        if rank is not None and name and club and total is not None:
            n_app = 6 if sport == "MAG" else 4
            _level = meta.get("level")
            _has_de = not (sport == "WAG" and _level is not None and _level < 7)
            d = (d_scores + [None] * n_app)[:n_app] if (d_scores and _has_de) else [None] * n_app
            e = (e_scores + [None] * n_app)[:n_app] if (e_scores and _has_de) else [None] * n_app
            app_totals = [(scores[i] if i < len(scores) else None) for i in range(n_app)]
            # Extrapolate missing D or E component from apparatus total
            for idx in range(n_app):
                if app_totals[idx] is not None:
                    if d[idx] is not None and e[idx] is None:
                        e[idx] = round(app_totals[idx] - d[idx], 3)
                    elif e[idx] is not None and d[idx] is None:
                        d[idx] = round(app_totals[idx] - e[idx], 3)
            row = {
                "rank":    rank,
                "bib":     bib,
                "athlete": name,
                "club":    club,
                "total":   total,
            }
            row.update(_build_app_scores(app_totals, d, e, sport))
            results.append(row)

    if not results:
        return []

    return [{**meta, "results": results}]


# ---------------------------------------------------------------------------
# Generic table parser (last-resort fallback)
# ---------------------------------------------------------------------------

HEADER_KEYWORDS = {
    "rank": ["rank", "pl", "place", "#", "pos"],
    "athlete": ["name", "athlete", "gymnast", "competitor"],
    "club": ["club", "gym", "team", "association"],
    "vault": ["vault", "vt", "v"],
    "bars": ["bars", "ub", "b"],
    "beam": ["beam", "bb"],
    "floor": ["floor", "fx", "f"],
    "total": ["total", "aa", "all around", "score", "sum"],
}


def parse_generic_tables(pdf_path):
    results = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                rows = _clean_table(table)
                headers, data_rows = _find_headers(rows)
                if headers is None:
                    continue
                for row in data_rows:
                    rec = _parse_table_row(row, headers)
                    if rec:
                        results.append(rec)
    return results


def _clean_table(table):
    cleaned = []
    for row in table:
        cr = [str(c).strip() if c is not None else "" for c in row]
        if any(cr):
            cleaned.append(cr)
    return cleaned


def _find_headers(rows):
    for i, row in enumerate(rows[:6]):
        row_lower = [c.lower() for c in row]
        matched = {}
        for col, kws in HEADER_KEYWORDS.items():
            for j, cell in enumerate(row_lower):
                if any(kw in cell for kw in kws):
                    matched[col] = j
                    break
        if "athlete" in matched and ("total" in matched or "vault" in matched):
            return matched, rows[i + 1:]
    return None, []


def _parse_table_row(row, headers):
    def get(key, default=""):
        idx = headers.get(key)
        return row[idx].strip() if idx is not None and idx < len(row) else default

    athlete = get("athlete")
    if not athlete or athlete.lower() in ("name", "athlete", "gymnast"):
        return None
    # Reject rows where the name cell looks like a header or is purely numeric
    if re.search(r"\b(?:rank|num|name|gym|club|team|score|total)\b", athlete, re.IGNORECASE):
        return None
    if re.match(r"^\d+\.?\d*$", athlete.strip()):
        return None

    def to_float(s):
        try:
            return float(s.replace(",", "."))
        except (ValueError, AttributeError):
            return None

    rank_str = get("rank")
    try:
        rank = int(re.sub(r"[^\d]", "", rank_str)) if rank_str else None
    except ValueError:
        rank = None

    return {
        "rank": rank,
        "athlete": athlete,
        "club": get("club"),
        "vault": to_float(get("vault")),
        "bars": to_float(get("bars")),
        "beam": to_float(get("beam")),
        "floor": to_float(get("floor")),
        "total": to_float(get("total")),
    }


# ---------------------------------------------------------------------------
# Filename metadata extraction
# ---------------------------------------------------------------------------

def parse_filename_meta(path, sport=None):
    # Auto-detect sport from path if not supplied
    if sport is None:
        parts = [p.upper() for p in Path(path).parts]
        sport = "MAG" if "MAG" in parts else "WAG"
    name = path.stem

    # Detect international category before any numeric level parsing
    path_text = " ".join(str(p) for p in Path(path).parts).lower()
    int_level = None
    for keyword, lvl in _INT_LEVEL_KEYWORDS:
        if keyword in path_text:
            int_level = lvl
            break

    level_m = re.search(r"(?:level|lvl|alp|(?<![a-zA-Z])L)[_\s-]*(\d+)", name, re.IGNORECASE)
    div_m   = re.search(r"(?:div(?:ision)?|D)[_\s-]*(\d+)", name, re.IGNORECASE)
    level   = int(level_m.group(1)) if level_m else None

    # Senior level code: LSNR, LSN, SNR, LSR, SR → level 104; LSR is an alternate abbreviation
    # Split on underscores/spaces to avoid partial matches (e.g. _LSNR_ not caught by \b)
    if level is None and any(re.fullmatch(r"L?S(?:NR?|R)", seg, re.IGNORECASE) for seg in re.split(r"[_\s]", name)):
        level = 104

    # MAG fallback: "5O", "5U", "6P" style codes (e.g. SGC format); also bare leading number "10 and SR"
    if level is None and sport == "MAG":
        ag_code_m = re.search(r"\b(\d+)[OUP]", name, re.IGNORECASE)
        if ag_code_m:
            level = int(ag_code_m.group(1))
    if level is None and sport == "MAG":
        bare_m = re.match(r"^(\d+)\b", name)
        if bare_m:
            level = int(bare_m.group(1))

    # Parent folder fallback: "Level 3/3 Open Meet Results.pdf" style
    if level is None:
        for part in path.parts[:-1]:
            folder_m = re.search(r"(?:level|lvl|(?<![a-zA-Z])L)[_\s-]*(\d+)", part, re.IGNORECASE)
            if folder_m:
                level = int(folder_m.group(1))
                break

    # Event type: Team → skip later; apparatus codes; AA by default.
    # Multi-word MAG apparatus names come before generic "bars"/"beam" to win alternation.
    type_m = re.search(
        r"\b(AA|all.?around|VT|UB|BB|FX|PH|SR|PB|HB|vault"
        r"|p[\s-]bars|parallel[\s-]bars|pbars|hbars?"
        r"|high[\s-]bar|h\.?bar"
        r"|pommel|rings"
        r"|bars|beam|floor|team)\b",
        name, re.IGNORECASE,
    )
    event_type = "AA"
    if type_m:
        raw = re.sub(r"[\s-]", "", type_m.group(1)).upper()
        event_type = {
            "ALLAROUND": "AA", "AA": "AA",
            "VT": "VT", "VAULT": "VT",
            "UB": "UB", "BARS": "UB",          # WAG bars = UB
            "BB": "BB", "BEAM": "BB",
            "FX": "FX", "FLOOR": "FX",
            "PH": "PH", "POMMEL": "PH",
            "SR": "SR", "RINGS": "SR",
            "PB": "PB", "PBARS": "PB", "PARALLELBARS": "PB",  # P-Bars / Parallel Bars
            "HB": "HB", "HBAR": "HB", "HBARS": "HB",          # High Bar
            "HIGHBAR": "HB", "HBAR": "HB",
            "TEAM": "Team",
        }.get(raw, raw)

    if re.search(r"meet.results", name, re.IGNORECASE):
        event_type = "AA"
    if re.search(r"\bAA\b", name, re.IGNORECASE):
        event_type = "AA"
    # EVNT_ files: apparatus is the 3rd underscore-delimited token (e.g. EVNT_Men_Floor_S8_L10)
    # The \bAA\b check above may fire on "AAll" suffix — fix by re-extracting from token
    if re.match(r"EVNT_", name, re.IGNORECASE):
        evnt_m = re.search(r"_(Floor|Vault|HBar|PBars|Pommel|Rings|Bars|Beam)_", name, re.IGNORECASE)
        if evnt_m:
            event_type = {
                "floor": "FX", "vault": "VT", "hbar": "HB", "pbars": "PB",
                "pommel": "PH", "rings": "SR", "bars": "UB", "beam": "BB",
            }.get(evnt_m.group(1).lower(), event_type)
    if re.search(r"team.results|team", name, re.IGNORECASE) and "Team" not in event_type:
        # (?<![a-zA-Z])team(?![a-zA-Z]) handles "TEAM_" prefix (underscore is \w so \bteam\b fails)
        if re.search(r"(?<![a-zA-Z])team(?![a-zA-Z])", name, re.IGNORECASE):
            event_type = "Team"

    # MAG age group parsing (order matters: specific patterns before generic)
    # Handles both word-separated ("Level 7 Open") and digit-attached ("Level 7O", "Level 9U15")
    age_group = None
    if sport == "MAG":
        if re.search(r"U15|Under\s*15", name, re.IGNORECASE):
            age_group = "U15"
        elif re.search(r"U18|Under\s*18", name, re.IGNORECASE):
            age_group = "U18"
        elif re.search(r"U13|Under\s*13", name, re.IGNORECASE):
            age_group = "U13"
        elif re.search(r"U14|Under\s*14", name, re.IGNORECASE):
            age_group = "U14"
        elif re.search(r"Open|(?<=\d)O\b", name, re.IGNORECASE):
            age_group = "Open"
        elif re.search(r"Under|(?<=\d)U(?![a-zA-Z\d])", name, re.IGNORECASE):
            age_group = _MAG_LEVEL_AGE.get(level, "Under")
        elif re.search(r"Optional|(?<=\d)P\b", name, re.IGNORECASE):
            age_group = "Optional"

    if int_level is not None:
        return {
            "level":      int_level,
            "division":   None,
            "age_group":  None,
            "event_type": event_type,
        }

    # WAG "Under" files (e.g. "L5U") are an age-split half of Division 1, not a
    # separate division: some comps run Division 1 as Over/Under instead of a
    # numbered split. Team events already got this via the "LAll" sibling
    # fallback below; apply the same convention here for individual results.
    division = int(div_m.group(1)) if div_m else None
    if sport == "WAG" and division is None and re.search(r"Under|(?<=\d)U(?![a-zA-Z\d])", name, re.IGNORECASE):
        division = 1

    return {
        "level":      level,
        "division":   division,
        "age_group":  age_group,
        "event_type": event_type,
    }


def infer_competition_name(pdf_path):
    parts = pdf_path.parts
    if len(parts) >= 5 and parts[-2] not in ("WAG", "pdfs") and not re.match(r"^\d{4}$", parts[-2]):
        return parts[-2].replace("-", " ").replace("_", " ").title()
    return pdf_path.stem.replace("-", " ").title()


# ---------------------------------------------------------------------------
# Per-file entry point
# ---------------------------------------------------------------------------

# Team rank line: "1 DGC BLU 104.900 26.700 26.850 24.725 26.625 0.000"
# Format: rank gym_name TOTAL V UB BB FX [SPARE]
# Spare column is optional (ProScore adds it in some exports).
_TEAM_RANK_RE = re.compile(
    r"^(\d+)\s+(.+?)\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})(?:\s+[\d]+\.[\d]{3})?\s*$"
)
# MAG variant: total + 6 apparatus (FX PH SR VT PB HB) — same order as individual results
_TEAM_RANK_RE_MAG = re.compile(
    r"^(\d+)\s+(.+?)\s+([\d]+\.[\d]{3})"
    r"\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})"
    r"\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})\s+([\d]+\.[\d]{3})\s*$"
)
# Sub-rank line like "1 1 1 2" — all numbers, ignore
_SUBRANK_RE = re.compile(r"^[\dT\s]+$")


def _gym_code_from_team_name(raw):
    """Extract gym code from a ProScore team name field.

    ProScore encodes team results as GYM_CODE + TEAM_DESIGNATOR where the team
    designator is always 3 characters (e.g. 'BLU', 'RED', 'ATB', 'FUN').
    Spaced fonts may insert spaces within each code ('CAS CS BLU' → 'CASCS BLU').
    Collapse all whitespace, then strip the trailing 3-char team designator.
    """
    # Strip literal "Team " prefix some PDFs inject before the gym code
    clean = re.sub(r"(?i)^team\s+", "", raw.strip())
    collapsed = re.sub(r"\s+", "", clean).upper()
    return collapsed[:-3] if len(collapsed) > 3 else collapsed


def _normalise_club(raw):
    """Extract club abbreviation from raw team name field.

    Handles two formats:
      'Cham ford CHA'  → 'CHA'  (name + 3-5 char code at end)
      'A TH A TB'      → 'ATHA' (spaced club code, collapse and take first chars)
    """
    raw = re.sub(r"(?i)^team\s+", "", raw.strip())
    tokens = raw.split()
    # "Name CODE" format: last token is a 3-5 char uppercase abbreviation
    if len(tokens) >= 2:
        last = tokens[-1]
        if last.isupper() and 3 <= len(last) <= 5:
            return last
    # Spaced-char format (BTYC style): collapse and take first 2-5 alpha chars
    collapsed = re.sub(r"\s+", "", raw)
    m = re.match(r"([A-Za-z]{2,5})", collapsed)
    return m.group(1).upper() if m else collapsed[:5].upper()


def _mag_team_club(raw):
    """Extract gym code from a MAG team name field.

    Handles:
      'BTY BTU'      → 'BTY'   Knox: gym_code team_designator
      'BTY 4'        → 'BTY'   SGC: gym_code n_athletes
      'EKGA EKG'     → 'EKGA'  Knox: long gym code + short team name
      'B5O B5O'      → 'B5O'   BTYC: level+age team code repeated
      'BA L BA L'    → 'BAL'   BTYC: spaced club code repeated
      'A TH A TH'    → 'ATH'   BTYC: spaced club code
      'Team 3 3'     → None    SGC numbered teams — no real gym code
    """
    raw = re.sub(r"(?i)^team\s+", "", raw.strip())
    tokens = raw.split()
    if not tokens:
        return None
    # Try repeated-pattern: collapse spaces and check if first half == second half
    # Handles "BA L BA L" → "BALBAL" → "BAL", "B5O B5O" → "B5OB5O" → "B5O"
    collapsed = re.sub(r"\s+", "", raw).upper()
    for n in range(2, len(collapsed) // 2 + 1):
        if collapsed[:n] == collapsed[n : 2 * n]:
            return collapsed[:n]
    first = tokens[0]
    # If first token is purely alphabetic (2–6 chars) → gym code (Knox / SGC formats)
    if re.match(r"^[A-Za-z]{2,6}$", first):
        return first.upper()
    # First token is a plain digit → numbered team, no gym code
    if first.isdigit():
        return None
    # Spaced / alphanumeric code (BTYC "B5O B5O", "A TH A TH"):
    # collapse all tokens, strip trailing 3-char designator
    result = collapsed[:-3] if len(collapsed) > 3 else collapsed
    # Reject if result is pure digits
    return result if not result.isdigit() else None


# Matches "Team Results Women / 31 / All Ages" style header and captures the code
_COMBINED_HDR_RE = re.compile(
    r"T\s*e\s*a\s*m\s+R\s*e\s*s\s*u\s*l\s*t\s*s"  # "Team Results"
    r"[^\n/]*"                                       # gender/category (no slash, no newline)
    r"/\s*(\d+(?:D\d+)?(?:[\d\s]{0,3})?)\s*(?:/|$)",  # "/ 31 /", "/ 32", "/ 3D1"
    re.IGNORECASE | re.MULTILINE,
)


def _split_combined_level_div(code):
    """'31' → (3, 1), '101' → (10, 1), '10' → (10, None), '3D1' → (3, 1)."""
    code = re.sub(r"\s+", "", str(code)).upper()
    m = re.match(r"^(\d+)D(\d+)$", code)
    if m:
        return int(m.group(1)), int(m.group(2))
    if len(code) == 1:
        return int(code), None
    if len(code) == 2:
        return (10, None) if code == "10" else (int(code[0]), int(code[1]))
    if len(code) == 3 and code[:2] == "10":
        return 10, int(code[2])
    return None, None


def parse_team_results(text_pages, pdf_path, sport="WAG"):
    """
    Parse Team Results ProScore PDFs.

    Supports single-event files (level/division from filename) and multi-event
    files where each page has its own header code like '/ 31 /' (L3 D1).

    For MAG: uses 6-apparatus regex and captures age_group from filename.
    Returns one event dict per level+division+age_group found.
    """
    file_meta = parse_filename_meta(pdf_path)
    events_by_ld = {}

    has_team_header_anywhere = any(t and TEAM_RESULTS_RE.search(t) for t in text_pages)

    for text in text_pages:
        if not text:
            continue
        # In mixed AA+Team PDFs, only process pages that are team results pages.
        # Pure team-only PDFs always have TEAM_RESULTS_RE on every page, so this
        # filter is safe for both cases.
        if has_team_header_anywhere and not TEAM_RESULTS_RE.search(text):
            continue

        page_level = file_meta.get("level")
        page_div   = file_meta.get("division")
        page_age   = file_meta.get("age_group")

        if sport != "MAG":
            # WAG: try to read level/division from combined header code
            code_m = _COMBINED_HDR_RE.search(text)
            if code_m:
                pl, pd = _split_combined_level_div(code_m.group(1))
                if pl is not None:
                    page_level, page_div = pl, pd

        # Fallback: parse "Level N Division N" from page header text
        # Handles files with LAll in filename (e.g. TEAM_Women_S2_LAll_AAll.pdf)
        if page_level is None:
            hdr_m = re.search(r"Level\s+(\d+)\s+Division\s+(\d+)", text, re.IGNORECASE)
            if hdr_m:
                page_level, page_div = int(hdr_m.group(1)), int(hdr_m.group(2))
            else:
                hdr_m = re.search(r"Level\s+(\d+)", text, re.IGNORECASE)
                if hdr_m:
                    page_level = int(hdr_m.group(1))

        # Fallback: "/ All Levels" team file with no level anywhere in its own
        # content (e.g. TEAM_Women_S3A_LAll.pdf combining D1 + Under into one
        # team ranking). Borrow level/division from sibling MEET_*.pdf files in
        # the same folder — Under siblings have division=None so they're
        # excluded, matching the "combined team counts as the numbered
        # division" convention already used for "L7D1,7U" style filenames.
        if page_level is None and re.search(r"all\s*levels", text, re.IGNORECASE):
            sib_pairs = set()
            for sib in Path(pdf_path).parent.glob("MEET_*.pdf"):
                sib_meta = parse_filename_meta(sib, sport=sport)
                if sib_meta.get("level") is not None and sib_meta.get("division") is not None:
                    sib_pairs.add((sib_meta["level"], sib_meta["division"]))
            if len(sib_pairs) == 1:
                page_level, page_div = sib_pairs.pop()

        if page_level is None:
            continue

        key = (page_level, page_div, page_age)
        if key not in events_by_ld:
            events_by_ld[key] = []

        for raw in text.splitlines():
            l = raw.strip()
            if not l or _HEADER_SKIP.search(l) or _SUBRANK_RE.match(l):
                continue

            if sport == "MAG":
                m = _TEAM_RANK_RE_MAG.match(l)
                if not m:
                    continue
                rank_str, raw_name, total, s1, s2, s3, s4, s5, s6 = m.groups()
                club = _mag_team_club(raw_name)
                if not club:
                    continue
                events_by_ld[key].append({
                    "rank":      _parse_rank(rank_str),
                    "club":      club,
                    "team_name": raw_name.strip(),
                    "floor":  _parse_score(s1),
                    "pommel": _parse_score(s2),
                    "rings":  _parse_score(s3),
                    "vault":  _parse_score(s4),
                    "pbars":  _parse_score(s5),
                    "hbar":   _parse_score(s6),
                    "total":  _parse_score(total),
                })
            else:
                m = _TEAM_RANK_RE.match(l)
                if not m:
                    continue
                rank_str, raw_name, total, s1, s2, s3, s4 = m.groups()
                events_by_ld[key].append({
                    "rank":      _parse_rank(rank_str),
                    "club":      _gym_code_from_team_name(raw_name),
                    "team_name": raw_name.strip(),
                    "vault": _parse_score(s1),
                    "bars":  _parse_score(s2),
                    "beam":  _parse_score(s3),
                    "floor": _parse_score(s4),
                    "total": _parse_score(total),
                })

    return [
        {"level": lvl, "division": div, "age_group": ag, "event_type": "Team", "results": results}
        for (lvl, div, ag), results in events_by_ld.items()
        if results
    ]


def parse_scoreholder(text_pages, pdf_path, sport="WAG"):
    """Parse scoreholder.com PDFs.

    Athlete line:  rank name score(rank) ×6 total
    Next line:     club/gym full name (to be alias-mapped later)
    Remaining lines: detail rows (SV B E ND breakdown) — ignored.

    Team pages ("Level N - Team" header, no All-around) list one row per
    club squad (e.g. "1 PIT L1 Team ... 161.700") matched by the same row
    regex, followed by that squad's club name and then each athlete's own
    per-apparatus contribution ending in "-" instead of a numeric total —
    those per-athlete rows don't match the row regex so they're skipped
    automatically, leaving just the squad totals.
    """
    file_meta = parse_filename_meta(pdf_path, sport=sport)

    # Level + age_group from page header e.g. "Level 2 - All-around > Open"
    _hdr_re = re.compile(
        r"Level\s+(\d+)\s*[-–]\s*All.?[Aa]round\s*[>|]\s*(\w+)", re.IGNORECASE
    )
    # Team page header e.g. "Level 1 - Team"
    _team_hdr_re = re.compile(r"Level\s+(\d+)\s*[-–]\s*Team\b", re.IGNORECASE)
    # Score token with rank annotation: "9.200 (1)" / "8.900 (1=)" / "9.400 (3T)"
    _SH = r"[\d.]+\s*\(\d+[=T]?\)"
    _athlete_re = re.compile(
        rf"^(\d+[=T]?)\s+(.+?)\s+({_SH})\s+({_SH})\s+({_SH})\s+({_SH})\s+({_SH})\s+({_SH})\s+([\d.]+)\s*$"
    )
    # Lines that are never club names
    _skip_line = re.compile(
        r"^(?:SV\b|Rk\.|Generated\s|Level\s|\d{2}/\d{2}/|\d{4}\s|[\d\s.\-–()+]+$)",
        re.IGNORECASE
    )

    events_by_key      = {}   # (level, age_group) → list of AA results
    team_events_by_lvl = {}   # level → list of Team results

    for text in text_pages:
        if not text:
            continue

        level     = file_meta.get("level")
        age_group = file_meta.get("age_group")
        is_team   = file_meta.get("event_type") == "Team"

        hdr_m = _hdr_re.search(text)
        if hdr_m:
            level = int(hdr_m.group(1))
            ag_raw = hdr_m.group(2).strip().lower()
            age_group = "Open" if ag_raw == "open" else "Under" if "under" in ag_raw else ag_raw.title()
            is_team = False
        else:
            team_hdr_m = _team_hdr_re.search(text)
            if team_hdr_m:
                level = int(team_hdr_m.group(1))
                is_team = True

        if level is None:
            continue

        if is_team:
            target = team_events_by_lvl.setdefault(level, [])
        else:
            target = events_by_key.setdefault((level, age_group), [])

        lines = [l.rstrip() for l in text.splitlines() if l.strip()]
        pending = None

        for line in lines:
            # Clean CID encoding artifacts (e.g. "(cid:9)" for tab)
            line = re.sub(r"\(cid:\d+\)", " ", line).strip()
            if not line:
                continue

            m = _athlete_re.match(line)
            if m:
                rank_str = m.group(1).rstrip("=T")
                raw_scores = [re.match(r"([\d.]+)", g).group(1) for g in m.groups()[2:8]]
                scores = [_parse_score(s) for s in raw_scores]
                total = _parse_score(m.group(9))
                row = {
                    "rank":  _parse_rank(rank_str),
                    "bib":   None,
                    "club":  None,
                    "total": total,
                }
                if is_team:
                    row["team_name"] = m.group(2).strip()
                else:
                    row["athlete"] = _clean_name(m.group(2))
                row.update(_build_app_scores(scores, [], [], sport))
                target.append(row)
                pending = row
                continue

            if pending is not None and pending["club"] is None:
                if not _skip_line.match(line):
                    pending["club"] = line.strip()
                    pending = None

    events = [
        {"level": lvl, "division": None, "age_group": ag, "event_type": "AA", "results": results}
        for (lvl, ag), results in events_by_key.items()
        if results
    ]
    events += [
        {"level": lvl, "division": None, "age_group": None, "event_type": "Team", "results": results}
        for lvl, results in team_events_by_lvl.items()
        if results
    ]
    return events


def parse_scoreholder_wag(text_pages, pdf_path):
    """Parse WAG scoreholder.com PDFs (Ballarat Winterfest style).

    Format differs from the MAG scoreholder layout:
      - 4 apparatus (VT, UB, BB, FX), not 6
      - Division embedded in page header: "WAG Level 5 Div 3"
      - Club line has D/E breakdown: "Club Name 1 D E [P] D E [P] ..."
      - Pass-2 vault line follows (skipped)

    Club stored as full name; update.py maps to code via aliases.
    D/E only extracted and stored for levels 7+ (E-score levels → NULL).
    """
    file_meta = parse_filename_meta(pdf_path, sport="WAG")
    level_from_file = file_meta.get("level")

    # Page header: "WAG Level 5 Div 3 Div 3" / "WAG Level 8"
    _page_hdr_re = re.compile(
        r"WAG\s+Level\s+(\d+)(?:\s+Div\s+([\d/& ]+))?",
        re.IGNORECASE,
    )

    # Athlete AA line: rank  name  score(rank) ×4  total
    _SH = r"[\d.]+\s*\(\d+[=T]?\)"
    _athlete_re = re.compile(
        rf"^(\d+[=T]?)\s+(.+?)\s+({_SH})\s+({_SH})\s+({_SH})\s+({_SH})\s+([\d.]+)\s*$"
    )

    # Club line: "Club Name 1 D E [P] ..." (Ballarat — has pass marker)
    _club_re = re.compile(r"^(.+?)\s+1\s+(?=[\d-])")
    # Club line fallback: "EKGA 10.000 -0.550 ..." (no pass marker — D/E follow immediately)
    _club_re_nopass = re.compile(r"^(.+?)\s+(?=\d{2,}\.\d+(?:\s|$)|-?\d)")

    # Exit current section when apparatus sub-rankings begin
    _section_end_re = re.compile(
        r"^#\s+Org\b|^(?:Vault|U-Bars|Beam|Floor)\s*$",
        re.IGNORECASE,
    )

    # Noise lines to skip inside the AA section
    _noise_re = re.compile(
        r"^(?:#\s+Name\b|D\s+E\s+P\b|Generated\s+by\b|WAG\s+Level\b)",
        re.IGNORECASE,
    )

    IDLE, AWAIT_CLUB, AWAIT_PASS2 = 0, 1, 2
    aa_by_key   = {}   # (level, division) → [AA result dicts]
    team_by_key = {}   # (level, division) → [Team result dicts]

    # Ballarat format has two vault passes ("Pass" column); EKGA does not
    _full_concat = "\n".join(t for t in text_pages if t)
    _has_pass = bool(re.search(r"#\s+Name\s+Pass\s+Vault", _full_concat, re.IGNORECASE))

    for text in text_pages:
        if not text:
            continue

        # Determine level and division for this page
        page_level = level_from_file
        page_div = None
        hdr_m = _page_hdr_re.search(text)
        if hdr_m:
            page_level = int(hdr_m.group(1))
            div_raw = (hdr_m.group(2) or "").strip()
            if div_raw:
                first_num = re.search(r"\d+", div_raw)
                page_div = int(first_num.group()) if first_num else None

        if page_level is None:
            continue

        in_aa   = False
        in_team = False
        state   = IDLE
        pending = None

        for raw_line in text.splitlines():
            line = re.sub(r"\(cid:\d+\)", " ", raw_line).strip()
            if not line:
                continue

            # Section control
            if "All-Around Results" in line:
                in_aa = True; in_team = False
                state = IDLE; pending = None
                continue

            if "Team Results" in line:
                in_team = True; in_aa = False
                state = IDLE; pending = None
                continue

            if _section_end_re.match(line):
                in_aa = False; in_team = False
                state = IDLE; pending = None
                continue

            if _noise_re.match(line):
                continue

            # ── Team section ──────────────────────────────────────────
            if in_team:
                m = _athlete_re.match(line)
                if m:
                    rank_str = m.group(1)
                    team_name = m.group(2).strip()
                    scores = [_parse_score(re.match(r"[\d.]+", g).group()) for g in m.groups()[2:6]]
                    total = _parse_score(m.group(7))
                    row = {
                        "rank":    _parse_rank(rank_str.rstrip("=T")),
                        "athlete": None,
                        "club":    team_name,
                        "total":   total,
                    }
                    row.update(_build_app_scores(scores, [], [], "WAG"))
                    team_by_key.setdefault((page_level, page_div), []).append(row)
                # All other lines in team section (member names, D/E rows) are skipped
                continue

            # ── AA section ────────────────────────────────────────────
            if not in_aa:
                continue

            # Waiting for pass-2 vault line
            if state == AWAIT_PASS2:
                if re.match(r"^2(?:\s|$)", line):
                    state = IDLE
                    continue
                state = IDLE  # no pass-2 found — fall through

            # Waiting for club name line
            if state == AWAIT_CLUB:
                cm = _club_re.match(line) or _club_re_nopass.match(line)
                if cm:
                    pending["club"] = cm.group(1).strip()
                    # Extract D/E for levels 7+ only
                    if page_level >= 7:
                        after = line[cm.end():].strip()
                        nums = []
                        for tok in re.split(r"\s+", after):
                            try:
                                nums.append(float(tok))
                            except ValueError:
                                pass
                        d_list, e_list = [], []
                        idx = 0
                        for _ in range(4):
                            if idx + 1 >= len(nums):
                                break
                            d = nums[idx]; idx += 1
                            e = nums[idx]; idx += 1
                            # Optional penalty (negative) — fold into E
                            if idx < len(nums) and nums[idx] < 0:
                                e = round(e + nums[idx], 3); idx += 1
                            d_list.append(d)
                            e_list.append(e)
                        for i, col in enumerate(_WAG_COL_ORDER[:len(d_list)]):
                            pending[f"{col}_d"] = d_list[i]
                            pending[f"{col}_e"] = e_list[i] if i < len(e_list) else None
                    # Ballarat has a pass-2 vault line; EKGA goes straight to next athlete
                    state = AWAIT_PASS2 if _has_pass else IDLE
                    pending = None
                    continue
                # Not a club line — abandon pending row and fall through
                state = IDLE
                pending = None

            # Try athlete rank line
            m = _athlete_re.match(line)
            if m:
                rank_str = m.group(1)
                name = _clean_name(m.group(2))
                scores = [_parse_score(re.match(r"[\d.]+", g).group()) for g in m.groups()[2:6]]
                total = _parse_score(m.group(7))
                row = {
                    "rank":    _parse_rank(rank_str.rstrip("=T")),
                    "bib":     None,
                    "athlete": name,
                    "club":    None,
                    "total":   total,
                }
                row.update(_build_app_scores(scores, [], [], "WAG"))
                aa_by_key.setdefault((page_level, page_div), []).append(row)
                pending = row
                state = AWAIT_CLUB

    events = []
    for (lvl, div), results in aa_by_key.items():
        if results:
            events.append({"level": lvl, "division": div, "age_group": None, "event_type": "AA", "results": results})
    for (lvl, div), results in team_by_key.items():
        if results:
            events.append({"level": lvl, "division": div, "age_group": None, "event_type": "Team", "results": results})
    return events


def parse_proscore_multi(text_pages, pdf_path, sport="WAG"):
    """Parse 'Meet Results - Multi' ProScore PDFs (Prelims + Finals + Combined Total per athlete)."""
    file_meta = parse_filename_meta(pdf_path, sport=sport)
    events_by_ld = {}

    _prelims_re = re.compile(
        rf"^(\d+[T]?)\s+(\d+)\s+(.+?)\s+Prelims\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s+({_S})\s*$"
    )
    _club_finals_re = re.compile(
        r"^([A-Za-z]{2,8}(?:/[A-Za-z]{2,8})?)\s*(?:\([^)]+\))?\s+Finals\s"
    )
    _combined_re = re.compile(r"^Combined Total:\s*([\d.]+)")

    for text in text_pages:
        if not text:
            continue

        page_level = file_meta.get("level")
        page_div = file_meta.get("division")
        hdr_m = PROSCORE_SIMPLE_HDR.search(text)
        if hdr_m:
            pl = int(hdr_m.group(1))
            suffix = hdr_m.group(2).upper()
            if suffix.startswith("D") and len(suffix) > 1:
                pd = int(suffix[1:])
            else:
                pd = _div_from_letter(suffix)
            page_level = pl
            if pd is not None:
                page_div = pd

        if page_level is None:
            continue

        key = (page_level, page_div)
        if key not in events_by_ld:
            events_by_ld[key] = []

        lines = [l.rstrip() for l in text.splitlines() if l.strip()]
        pending = None

        for line in lines:
            m = _prelims_re.match(line)
            if m:
                rank_str, bib, name, s1, s2, s3, s4, s5, s6, _pt = m.groups()
                app = [_parse_score(x) for x in (s1, s2, s3, s4, s5, s6)]
                row = {
                    "rank":    _parse_rank(rank_str),
                    "bib":     bib.strip(),
                    "athlete": _clean_name(name),
                    "club":    None,
                    "total":   None,
                }
                row.update(_build_app_scores(app, [], [], sport))
                events_by_ld[key].append(row)
                pending = row
                continue

            if pending is not None:
                if pending["club"] is None:
                    cm = _club_finals_re.match(line)
                    if cm:
                        pending["club"] = cm.group(1).strip().upper()
                        continue
                ct = _combined_re.match(line)
                if ct:
                    pending["total"] = _parse_score(ct.group(1))
                    pending = None

    return [
        {"level": lvl, "division": div, "event_type": "AA", "results": results}
        for (lvl, div), results in events_by_ld.items()
        if results
    ]


def parse_event_results(text_pages, pdf_path, sport="WAG"):
    """Parse ProScore 'Event Results' individual apparatus finals format.

    One line per athlete: rank bib name gym diff exec s+ nd score out
    Level and apparatus come from filename metadata.
    """
    file_meta = parse_filename_meta(pdf_path, sport=sport)
    level     = file_meta.get("level")
    age_group = file_meta.get("age_group")
    event_type = file_meta.get("event_type")

    if level is None or event_type in (None, "AA", "Team"):
        return []

    wag_app_map = {"VT": "vault", "UB": "bars", "BB": "beam", "FX": "floor"}
    app_col = MAG_APPARATUS_MAP.get(event_type) if sport == "MAG" else wag_app_map.get(event_type)
    if app_col is None:
        return []

    results = []
    for text in text_pages:
        if not text:
            continue
        for line in text.splitlines():
            m = _EVNT_ROW_RE.match(line.strip())
            if not m:
                continue
            rank_str, bib, name, gym, diff, exec_s, _, _, score, _ = m.groups()
            total = _parse_score(score)
            if total is None:
                continue
            row = {
                "rank":    _parse_rank(rank_str),
                "bib":     bib,
                "athlete": _clean_name(name),
                "club":    gym.strip().split()[0].upper(),
                "total":   total,
                app_col:   total,
            }
            d_val = _parse_score(diff)
            e_val = _parse_score(exec_s)
            if d_val is not None:
                row[f"{app_col}_d"] = d_val
            if e_val is not None:
                row[f"{app_col}_e"] = e_val
            results.append(row)

    if not results:
        return []
    return [{"level": level, "age_group": age_group, "event_type": event_type, "results": results}]


def parse_proscore_simple(text_pages, pdf_path, sport="WAG"):
    """
    Parse 'Meet Results Women / 5A' style ProScore PDFs.

    Two-line athlete records:
        {rank} {bib} {name} {v} {ub} {bb} {fx} [spare] {total}
        {gym} {ranks...}

    Level from numeric part of code, division from letter (A=1, B=2, C=3...).
    Falls back to filename metadata if no header code found on a page.
    """
    file_meta = parse_filename_meta(pdf_path)
    events_by_ld = {}

    for text in text_pages:
        if not text or TEAM_RESULTS_RE.search(text):
            continue

        page_level = file_meta.get("level")
        page_div = file_meta.get("division")
        hdr_m = PROSCORE_SIMPLE_HDR.search(text)
        if hdr_m:
            raw_num = hdr_m.group(1)
            suffix = hdr_m.group(2).upper()
            if suffix.startswith("D") and len(suffix) > 1:
                pl, pd = int(raw_num), int(suffix[1:])   # "D1" → 1, "D2" → 2
            elif suffix and not suffix.isdigit():
                pl, pd = int(raw_num), _div_from_letter(suffix)  # "A" → 1, "B" → 2
            else:
                # No letter suffix — numeric code may encode level+div (e.g. "51" = L5 D1)
                pl, pd = _split_combined_level_div(raw_num)
            page_level = pl
            if pd is not None:
                page_div = pd

        if page_level is None:
            continue

        key = (page_level, page_div)
        if key not in events_by_ld:
            events_by_ld[key] = []

        lines = [l.rstrip() for l in text.splitlines() if l.strip()]
        prev_athlete = None

        for line in lines:
            matched = False

            if sport == "MAG":
                # MAG: rank bib name + 6 apparatus scores + total
                m = ATHLETE_LINE_MAG_AA.match(line)
                if m:
                    rank_str, bib, name, s1, s2, s3, s4, s5, s6, total = m.groups()
                    app = [_parse_score(x) for x in (s1, s2, s3, s4, s5, s6)]
                    row = {
                        "rank":    _parse_rank(rank_str),
                        "bib":     bib.strip(),
                        "athlete": _clean_name(name),
                        "club":    None,
                        "total":   _parse_score(total),
                    }
                    row.update(_build_app_scores(app, [], [], sport))
                    events_by_ld[key].append(row)
                    prev_athlete = row
                    matched = True
            else:
                # WAG: try longest (2-spare) variant first, then 1-spare, then plain
                # 4-score. Trying the shorter variants first would let their fixed
                # trailing-group count absorb an extra numeric token into the name.
                m = ATHLETE_LINE_AA_SPARE2.match(line)
                if m:
                    rank_str, bib, name, v, ub, bb, fx, _spare1, _spare2, total = m.groups()
                    prev_athlete = {
                        "rank": _parse_rank(rank_str),
                        "bib": bib.strip(),
                        "athlete": _clean_name(name),
                        "club": None,
                        "vault": _parse_score(v),
                        "bars": _parse_score(ub),
                        "beam": _parse_score(bb),
                        "floor": _parse_score(fx),
                        "total": _parse_score(total),
                    }
                    events_by_ld[key].append(prev_athlete)
                    matched = True
                else:
                    m = ATHLETE_LINE_AA_SPARE.match(line)
                    if m:
                        rank_str, bib, name, v, ub, bb, fx, _spare, total = m.groups()
                        prev_athlete = {
                            "rank": _parse_rank(rank_str),
                            "bib": bib.strip(),
                            "athlete": _clean_name(name),
                            "club": None,
                            "vault": _parse_score(v),
                            "bars": _parse_score(ub),
                            "beam": _parse_score(bb),
                            "floor": _parse_score(fx),
                            "total": _parse_score(total),
                        }
                        events_by_ld[key].append(prev_athlete)
                        matched = True
                    else:
                        m = ATHLETE_LINE_AA.match(line)
                        if m:
                            rank_str, bib, name, v, ub, bb, fx, total = m.groups()
                            prev_athlete = {
                                "rank": _parse_rank(rank_str),
                                "bib": bib.strip(),
                                "athlete": _clean_name(name),
                                "club": None,
                                "vault": _parse_score(v),
                                "bars": _parse_score(ub),
                                "beam": _parse_score(bb),
                                "floor": _parse_score(fx),
                                "total": _parse_score(total),
                            }
                            events_by_ld[key].append(prev_athlete)
                            matched = True

            if not matched and prev_athlete is not None and prev_athlete["club"] is None:
                cm = _CLUB_RANKS_LINE.match(line)
                if cm:
                    prev_athlete["club"] = cm.group(1).strip().upper()
                    prev_athlete = None

    return [
        {"level": lvl, "division": div, "event_type": "AA", "results": results}
        for (lvl, div), results in events_by_ld.items()
        if results
    ]


def _parse_gymp_level_div(full_text, file_meta):
    """Extract level and division from GymPro header line: 'Level: Level 7 Div. 2'"""
    m = re.search(r"Level:\s*(?:Level\s+)?(\d+)\s+Div\.?\s*(\d+)", full_text)
    if m:
        return int(m.group(1)), int(m.group(2))
    return file_meta.get("level"), file_meta.get("division")



# ---------------------------------------------------------------------------
# GymPro individual-results wrap fixup (addon, not part of the main parser)
#
# Normally a wrapped Club/Team cell drops onto its own continuation line
# below the row (e.g. just "Club" or "Australia Red"), and the row still
# parses fine on the first line since the athlete's first+last name and the
# start of the club name are already present. That dropped continuation just
# leaves the club column truncated, which club-alias matching absorbs fine.
#
# It breaks when the athlete's SURNAME is long enough to wrap too, landing on
# the same continuation line as the wrapped club text (e.g.
# "Chindarattanavorakul Australia Red"). Flattened text then has no way to
# tell which word belongs to the Gymnast column vs the Club column, and the
# per-line splitter mis-attributes a club word as the surname. This is rare
# enough (2 rows out of ~250 in one real ingest) that it isn't worth
# rewriting the whole parser to be coordinate-based — instead we detect the
# ambiguous case by checking whether the continuation line contains any word
# that isn't a known "safe" club/colour fragment, and only for those flagged
# rows fall back to re-deriving the columns from word coordinates.
# ---------------------------------------------------------------------------

_GYMP_WRAP_WHITELIST = {
    "CLUB", "IND.", "IND", "CENTRE", "CENTER", "PROGRAM",
    "GYMNASTICS", "GYMNASTIC", "GYMANSTICS",
    "AUSTRALIA", "WARRNAMBOOL", "NUNAWADING",
    "WHITE", "BLUE", "RED", "SILVER", "GOLD", "BLACK", "NAVY", "TEAL",
    "PURPLE", "GREEN", "PINK", "YELLOW", "ORANGE", "AQUA", "MAROON",
    "GREY", "GRAY", "BRONZE", "COPPER", "PLATINUM", "INDIGO", "VIOLET",
    "CRIMSON", "SCARLET", "RUBY", "JADE", "AMBER", "CORAL", "LIME",
}

_GYMP_IGNORE_LINE_RE = re.compile(r"^(continued on page \d+|around)$", re.IGNORECASE)


def _gymp_wrap_is_suspicious(line):
    """True if a GymPro continuation line carries a word that isn't a known
    safe club/colour fragment — the signal that it also holds a surname
    overflow the per-line splitter can't place correctly."""
    l = line.strip()
    if not l or _GYMP_IGNORE_LINE_RE.match(l):
        return False
    return not all(t.upper() in _GYMP_WRAP_WHITELIST for t in l.split())


def _fix_gymp_row_via_coords(pdf_path, page_idx, bib_id):
    """Re-derive one flagged row's Gymnast/Club columns from word coordinates
    instead of flattened text. Only invoked for rows _gymp_wrap_is_suspicious
    flagged; returns (name, club) or None if the layout can't be resolved."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            words = pdf.pages[page_idx].extract_words()
    except Exception:
        return None

    id_x0 = gymnast_x0 = club_x0 = score_x0 = None
    for w in words:
        if id_x0 is None and w["text"] == "ID":
            id_x0 = w["x0"]
        elif gymnast_x0 is None and w["text"] == "Gymnast":
            gymnast_x0 = w["x0"]
        elif club_x0 is None and w["text"].startswith("Club"):
            club_x0 = w["x0"]
        elif score_x0 is None and club_x0 is not None and w["text"] == "D":
            score_x0 = w["x0"]
    if None in (id_x0, gymnast_x0, club_x0, score_x0):
        return None

    # Data x-positions sit a few points left of their header label's x0, so
    # use midpoints between header columns as the split points rather than
    # each label's own (slightly-too-far-right) x0.
    id_name_boundary   = (id_x0 + gymnast_x0) / 2
    name_club_boundary = (gymnast_x0 + club_x0) / 2
    club_score_boundary = (club_x0 + score_x0) / 2

    bib_words = [w for w in words if w["text"] == str(bib_id) and w["x0"] < id_name_boundary]
    if not bib_words:
        return None
    row_top = bib_words[0]["top"]
    later_bibs = sorted(
        w["top"] for w in words
        if w["text"].isdigit() and w["x0"] < id_name_boundary and w["top"] > row_top + 1
    )
    row_bottom = later_bibs[0] if later_bibs else float("inf")

    row_words = [w for w in words if row_top - 1 <= w["top"] < row_bottom]
    name_words = sorted(
        (w for w in row_words if id_name_boundary <= w["x0"] < name_club_boundary),
        key=lambda w: (w["top"], w["x0"]),
    )
    club_words = sorted(
        (w for w in row_words if name_club_boundary <= w["x0"] < club_score_boundary),
        key=lambda w: (w["top"], w["x0"]),
    )
    if not name_words or not club_words:
        return None

    name = " ".join(w["text"] for w in name_words)
    club = " ".join(w["text"] for w in club_words).upper()
    return name, club


# ---------------------------------------------------------------------------
# GymPro individual-results name/club split
#
# The Gymnast and Club/Team columns are just two runs of space-separated
# words with no delimiter between them, so the naive assumption "name is
# exactly the first 2 words" breaks for compound surnames ("Da Costa", "van
# Praag", "Al Makdissi"), a 3-word surname ("Lindor McFadden"), or a double
# given name ("Hailey Isabella"). In every one of those cases the extra name
# word was silently swallowed into the club field instead. Since every real
# club name is already known (clubs.json), the fix is to grow the name past
# 2 words only when doing so leaves a remainder that's actually a recognised
# club — otherwise fall back to the original 2-word split unchanged.
# ---------------------------------------------------------------------------

_CLUB_ALIASES_CACHE = None

_GYMP_COLOUR_SUFFIXES = {
    "WHITE", "BLUE", "RED", "SILVER", "GOLD", "BLACK", "NAVY", "TEAL",
    "PURPLE", "GREEN", "PINK", "YELLOW", "ORANGE", "AQUA", "MAROON",
    "GREY", "GRAY", "BRONZE", "COPPER", "PLATINUM", "INDIGO", "VIOLET",
    "CRIMSON", "SCARLET", "RUBY", "JADE", "AMBER", "CORAL", "LIME",
}


def _load_club_aliases():
    global _CLUB_ALIASES_CACHE
    if _CLUB_ALIASES_CACHE is None:
        cache = {}
        try:
            with open("data/clubs.json", encoding="utf-8") as f:
                data = json.load(f)
            for club in data.get("clubs", []):
                cache[club["code"].upper()] = club["code"]
                for alias in club.get("aliases", []):
                    cache[alias.upper()] = club["code"]
        except Exception:
            cache = {}
        _CLUB_ALIASES_CACHE = cache
    return _CLUB_ALIASES_CACHE


def _is_known_club_text(text):
    aliases = _load_club_aliases()
    if text in aliases:
        return True
    parts = text.rsplit(" ", 1)
    return len(parts) == 2 and parts[1] in _GYMP_COLOUR_SUFFIXES and parts[0] in aliases


def _split_gymp_name_club(middle):
    """Return (name, club) from the Gymnast+Club tokens of a GymPro row,
    extending the name past the default first+last split whenever that's
    needed to keep the remaining club text a recognised club."""
    best_split = 2
    for split in range(3, len(middle)):
        if _is_known_club_text(" ".join(middle[split:]).upper()):
            best_split = split
    name = " ".join(middle[:best_split])
    club = " ".join(middle[best_split:]).upper() if len(middle) > best_split else ""
    return name, club


def parse_gymp_individual(text_pages, level, division, pdf_path):
    """Parse GymPro individual results. Each row:
       ID First Last Club... D score rank D score rank D score rank D score rank total rank
       = 14 trailing numeric tokens (4 apparatus with difficulty + total + rank)
    """
    results = []
    for page_idx, page_text in enumerate(text_pages):
        last_rec = None
        last_bib = None
        for line in (page_text or "").splitlines():
            tokens = line.split()
            if not tokens or not tokens[0].isdigit() or len(tokens) < 7:
                # Not a data row — check if it's a suspicious wrap continuation
                # of the row we just parsed.
                if last_rec is not None and _gymp_wrap_is_suspicious(line):
                    fixed = _fix_gymp_row_via_coords(pdf_path, page_idx, last_bib)
                    if fixed:
                        name, club = fixed
                        last_rec["athlete"] = _clean_name(name)
                        last_rec["club"] = club
                    last_rec = None  # only ever attempt one fixup per row
                continue
            # Try to peel off 14 trailing tokens (with D scores)
            for n_trail in (14, 10):
                if len(tokens) < 1 + 2 + n_trail:  # bib + 2 name words + scores
                    continue
                trail = tokens[-n_trail:]
                middle = tokens[1:-n_trail]  # first last [club...]
                if len(middle) < 2:
                    continue
                try:
                    total = float(trail[-2])
                    rank  = int(trail[-1])
                except ValueError:
                    continue
                name, club = _split_gymp_name_club(middle)
                rec = {"rank": rank, "athlete": _clean_name(name), "club": club, "total": total}
                try:
                    if n_trail == 14:
                        # D score, combined score, rank per apparatus
                        for i, app in enumerate(_WAG_COL_ORDER):
                            rec[f"{app}_d"] = float(trail[3 * i])
                            rec[app] = float(trail[3 * i + 1])
                    else:
                        # combined score, rank per apparatus (no D column)
                        for i, app in enumerate(_WAG_COL_ORDER):
                            rec[app] = float(trail[2 * i])
                except ValueError:
                    pass
                results.append(rec)
                last_rec = rec
                last_bib = tokens[0]
                break
    return results


def parse_gymp_team(full_text, level, division):
    """Parse GymPro team results. Each row:
       Club... score rank score rank score rank score rank total rank
       = 10 trailing numeric tokens (4 apparatus scores+ranks + total + rank)
    """
    results = []
    for line in full_text.splitlines():
        tokens = line.split()
        if not tokens or len(tokens) < 3:
            continue
        # First token must not be a number (club names start with letters)
        if tokens[0][0].isdigit():
            continue
        # Need at least 10 trailing numeric tokens
        if len(tokens) < 11:
            continue
        trail = tokens[-10:]
        try:
            vals = [float(trail[i]) if i % 2 == 0 else int(trail[i]) for i in range(10)]
            total = vals[-2]
            rank  = int(vals[-1])
        except ValueError:
            continue
        raw_name = " ".join(tokens[:-10])
        club = raw_name.upper()
        rec = {"rank": rank, "club": club, "team_name": raw_name.strip(), "total": total, "athlete": None}
        for i, app in enumerate(_WAG_COL_ORDER):
            rec[app] = vals[2 * i]
        results.append(rec)
    return results


# ---------------------------------------------------------------------------
# WG scoring program parser (Natimuk Invitational style)
# ---------------------------------------------------------------------------

# Detection: column header present on every page
WG_HDR_RE = re.compile(
    r"Team\s+Name\s+Vault\s+Result\s+Bars\s+Result",
    re.IGNORECASE,
)


def _wg_expand_tokens(raw_tokens):
    """Pre-process WG middle tokens:
    - Strip parenthetical annotations: (P), (BL), (Tilly), etc.
    - Split camelCase boundaries so 'PortlandMilla' → ['Portland', 'Milla'].
    """
    result = []
    for tok in raw_tokens:
        if re.match(r'^\(.*\)$', tok):
            continue
        # Require 2+ lowercase chars before uppercase to avoid splitting Mc/Mac prefixes.
        expanded = re.sub(r'([a-z]{2,})([A-Z])', r'\1 \2', tok)
        result.extend(expanded.split())
    return result


def parse_wg(text_pages, pdf_path):
    """Parse WG scoring program PDFs.

    Each athlete row: ClubWord(s)... FirstName [particles] LastName
                      vScore vRank ubScore ubRank bbScore bbRank fxScore fxRank total rank
    Level and division always come from the filename.
    Blue/White Rotation files contain different athletes (rotation groups) — both
    are imported; athlete-level dedup in the UI handles any overlap.
    """
    meta = parse_filename_meta(pdf_path)
    if meta.get("level") is None:
        return []

    results = []
    for text in text_pages:
        if not text:
            continue
        for line in text.splitlines():
            tokens = line.split()
            # Minimum: 1 club word + first + last + 10 numeric = 13
            if len(tokens) < 13:
                continue

            trail = tokens[-10:]
            try:
                vault   = float(trail[0]); v_rank  = int(trail[1])
                bars    = float(trail[2]); b_rank  = int(trail[3])
                beam    = float(trail[4]); bm_rank = int(trail[5])
                floor   = float(trail[6]); f_rank  = int(trail[7])
                total   = float(trail[8])
            except ValueError:
                continue
            try:
                rank = int(trail[9])
            except ValueError:
                rank = None  # #N/A or missing final rank

            # Strip annotations and split camelCase-joined tokens
            middle = _wg_expand_tokens(tokens[:-10])
            if len(middle) < 3:
                continue

            # Parse name from the right: last name, optional lowercase particles, first name
            name_parts = [middle[-1]]
            idx = len(middle) - 2
            while idx >= 0 and middle[idx][0].islower():
                name_parts.insert(0, middle[idx])
                idx -= 1
            if idx < 0 or not middle[idx][0].isupper():
                continue  # no first name found
            name_parts.insert(0, middle[idx])
            idx -= 1

            club_parts = middle[:idx + 1]
            if not club_parts:
                continue

            results.append({
                "rank":    rank,
                "athlete": _normalise_name(" ".join(name_parts)),
                "club":    " ".join(club_parts).upper(),
                "vault":   vault,
                "bars":    bars,
                "beam":    beam,
                "floor":   floor,
                "total":   total,
            })

    if not results:
        return []
    return [{**meta, "results": results}]


def parse_gymp(text_pages, pdf_path):
    """Entry point for GymPro format PDFs."""
    full_text = "\n".join(t for t in text_pages if t)
    file_meta = parse_filename_meta(pdf_path)
    level, division = _parse_gymp_level_div(full_text, file_meta)
    if level is None:
        return []

    if "GymPro - Team Results" in full_text:
        results = parse_gymp_team(full_text, level, division)
        event_type = "Team"
    else:
        results = parse_gymp_individual(text_pages, level, division, pdf_path)
        event_type = "AA"

    if not results:
        return []
    return [{"level": level, "division": division, "event_type": event_type, "results": results}]


def _inject_age_group(events, age_group):
    """Stamp age_group from filename onto every event that doesn't already have one."""
    if age_group is None:
        return events
    for ev in events:
        if not ev.get("age_group"):
            ev["age_group"] = age_group
    return events


def parse_pdf(pdf_path, sport="WAG"):
    """Returns (events_list, method_string)."""
    # MMUL/TMUL/EMUL files are combined Prelims+Finals totals — never ingest
    if re.search(r"\b[MTE]MUL_", Path(pdf_path).name, re.IGNORECASE):
        return [], "multi-session-skip"

    with pdfplumber.open(pdf_path) as pdf:
        text_pages = [page.extract_text() for page in pdf.pages]

    full_text = "\n".join(t for t in text_pages if t)

    meta = parse_filename_meta(pdf_path, sport=sport)
    age_group = meta.get("age_group")

    # Scoreholder.com format — WAG Winterfest style vs MAG PIT style differ in column headers
    if SCOREHOLDER_RE.search(full_text):
        if sport == "WAG" and re.search(r"#\s+Name\s+(?:Pass\s+)?Vault\b", full_text, re.IGNORECASE):
            events = parse_scoreholder_wag(text_pages, pdf_path)
        else:
            events = parse_scoreholder(text_pages, pdf_path, sport=sport)
        return (events, "scoreholder") if events else ([], "scoreholder-empty")

    # WG scoring program (Natimuk style)
    if WG_HDR_RE.search(full_text):
        events = parse_wg(text_pages, pdf_path)
        return (_inject_age_group(events, age_group), "wg") if events else ([], "wg-empty")

    # GymPro format (Eclipse-style)
    if "GymPro" in full_text:
        events = parse_gymp(text_pages, pdf_path)
        return (_inject_age_group(events, age_group), "gymp") if events else ([], "gymp-empty")

    # ProScore AA parsers — check BEFORE team routing because mixed AA+Team PDFs must not
    # be swallowed by the team-only gate.  Each parser already skips team pages internally;
    # we then merge team results from the same file afterward.

    # Old ProScore format (Meet Results - Level X Division Y)
    has_proscore_meet = any(t and PROSCORE_MEET_HDR.search(t) for t in text_pages)
    if has_proscore_meet:
        events = parse_proscore_text(text_pages, sport=sport)
        if events:
            if TEAM_RESULTS_RE.search(full_text):
                team_events = parse_team_results(text_pages, pdf_path, sport=sport)
                if team_events:
                    events = events + team_events
            return _inject_age_group(events, age_group), "proscore"

    # Simple ProScore format (Meet Results Women / 5A / ...) — also handles spaced characters
    has_proscore_simple = any(t and PROSCORE_SIMPLE_HDR.search(t) for t in text_pages)
    if has_proscore_simple:
        events = parse_proscore_simple(text_pages, pdf_path, sport=sport)
        if events:
            if TEAM_RESULTS_RE.search(full_text):
                team_events = parse_team_results(text_pages, pdf_path, sport=sport)
                if team_events:
                    events = events + team_events
            return _inject_age_group(events, age_group), "proscore-simple"

    # Route Team Results to dedicated parser (level may come from page headers, not filename)
    if TEAM_RESULTS_RE.search(full_text) or meta.get("event_type") == "Team":
        events = parse_team_results(text_pages, pdf_path, sport=sport)
        return (_inject_age_group(events, age_group), "team") if events else ([], "team-empty")

    if meta.get("level") is None:
        return [], "no-level-skip"

    # New ProScore format: has "Final:" anchor lines + Diff: / D/E: / DN/DE:
    if "Final:" in full_text and re.search(r"(?:Diff:|D/E:|DN/DE:)", full_text):
        events = parse_new_proscore(text_pages, pdf_path, sport=sport)
        if events:
            return _inject_age_group(events, age_group), "proscore-v2"

    # Multi-round ProScore (Prelims + Finals + Combined Total) — skip entirely;
    # combined totals are not valid single-session scores for the leaderboard.
    if PROSCORE_MULTI_HDR.search(full_text):
        return [], "multi-session-skip"

    # Simple ProScore format (Meet Results Women / 5A / All Ages)
    # Also catches "/ All Levels /" headers where level comes from filename,
    # and ProScore "Day 1 Standings" format (no apparatus column headers).
    if any(t and PROSCORE_SIMPLE_HDR.search(t) for t in text_pages) or (
        meta.get("level") is not None and any(t and MEET_RESULTS_RE.search(t) for t in text_pages)
    ) or (
        meta.get("level") is not None and sport == "MAG"
        and any(t and DAY1_STANDINGS_RE.search(t) for t in text_pages)
    ):
        events = parse_proscore_simple(text_pages, pdf_path, sport=sport)
        if events:
            return _inject_age_group(events, age_group), "proscore-simple"

    # ProScore "Event Results" apparatus finals (spaced title, Diff/Exec/Score columns)
    if any(t and EVENT_RESULTS_RE.search(t) for t in text_pages):
        events = parse_event_results(text_pages, pdf_path, sport=sport)
        if events:
            return _inject_age_group(events, age_group), "event-results"

    # Fallback: table
    results = parse_generic_tables(pdf_path)
    events_fallback = []
    if results:
        events_fallback.append({**meta, "results": results})
    return _inject_age_group(events_fallback, age_group), "table"


# ---------------------------------------------------------------------------
# Grouping into competition objects
# ---------------------------------------------------------------------------

def group_into_competitions(all_entries, sport="WAG"):
    comp_map = {}
    for entry in all_entries:
        comp_name = entry["competition"]
        season = entry.get("season") or str(datetime.date.today().year)
        map_key = (comp_name, season)
        if map_key not in comp_map:
            comp_map[map_key] = {
                "id": re.sub(r"[^a-z0-9]+", "-", comp_name.lower()).strip("-") + f"-{season}",
                "name": comp_name,
                "season": season,
                "sport": sport,
                "events": [],
            }
        for ev in entry["events"]:
            ev_entry = {
                "level":      ev.get("level"),
                "division":   ev.get("division") if (ev.get("division") is not None and sport == "WAG") else None,
                "age_group":  ev.get("age_group"),
                "event_type": ev.get("event_type", "AA"),
                "source_file": entry["source_file"],
                "results":    ev.get("results", []),
            }
            for r in ev_entry["results"]:
                r.pop("bib", None)
            comp_map[map_key]["events"].append(ev_entry)
    return list(comp_map.values())
