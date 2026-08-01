"""
pdf_parser.py — Parses WAG results PDFs into structured JSON.

Supports three ProScore layouts and a generic table fallback:
  1. Old ProScore  : "Meet Results - Level X Division Y ..." one-line athlete records
  2. New ProScore  : BTYC/Knox multi-line records anchored on "Final:" lines
  3. Generic table : pdfplumber table extraction (last resort)

Team Results PDFs are skipped in all cases.
"""

import datetime
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

CLUB_LINE = re.compile(r"^([A-Za-z]{2,6})\s+[\d]+[T]?(?:\s+[\d]+[T]?){1,5}\s*$")
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
_CLUB_RANKS_LINE = re.compile(r"^(?:[A-Za-z]{2,6}/)?([A-Za-z]{2,8}(?:\s+[A-Za-z]{2,4})*)(?:\s+\([A-Za-z/]+\))?\s+[\dT]")

# Lines to filter when building the cleaned line list for new-format parsing
_HEADER_SKIP = re.compile(
    r"(?:ProScore|^Printed:|^Session:|Page:\s*\d|"
    r"^Judge|^Gym\s*$|^AA\s*$|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d|"
    r"^Rank\s*Num\s+Name|^RankNum\s+Name\s+Gym|^RankNum\s+Name|^Rank\s+Gym\s+Team|"
    r"^Rank\s+Num\s+Name)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# GymACRO custom scoring system constants (Victorian State Trials / Vic Champs)
# ---------------------------------------------------------------------------

# Detection: "Final Results" followed by an ACRO category line
GYM_ACRO_DETECT_RE = re.compile(
    r"Final Results\s*\n\s*"
    r"(?:Level\s+\d+|Junior\s+\d+-\d+|Senior|Adult)\s+"
    r"(?:Women|Womens|Men|Mens|Mixed)",
    re.IGNORECASE | re.MULTILINE,
)

# Category header: "Level 6 Mixed Pair" / "Junior 11-16 Women's Pair" / "Senior Mixed Pair"
_GYM_ACRO_CATEGORY_RE = re.compile(
    r"^(Level\s+\d+|Junior\s+\d+-\d+|Senior|Adult)"
    r"\s+(Women'?s?|Womens?|Men'?s?|Mens?|Mixed)"
    r"\s+(Pair|Trio|Group)\s*$",
    re.IGNORECASE,
)

# Exercise line: "Balance ...", "Dynamic ...", "Combined ..."
_GYM_ACRO_EXERCISE_RE = re.compile(r"^(Balance|Dynamic|Combined)\s+", re.IGNORECASE)

# Lines to skip in GymACRO format
_GYM_ACRO_SKIP_RE = re.compile(
    r"^(?:Date:|Time:|Page:|# End|Total Final Overall|"
    r"Rank Diff|E1\s+E2|A1\s+A2|Perform Penalties)",
    re.IGNORECASE,
)


def _gym_acro_normalise_category(gender_raw: str, group_raw: str) -> str:
    g_map = {
        "women": "Women's", "womens": "Women's", "women's": "Women's",
        "men":   "Men's",   "mens":   "Men's",   "men's":   "Men's",
        "mixed": "Mixed",
    }
    t_map = {"pair": "Pair", "trio": "Group", "group": "Group"}
    g_str = g_map.get(gender_raw.strip().lower(), gender_raw.strip().title())
    t_str = t_map.get(group_raw.strip().lower(), group_raw.strip().title())
    return f"{g_str} {t_str}"


def _gym_acro_parse_athlete_line(line: str):
    """Parse: '{bib} {club words} {First Last}[, {First Last}]... [{total}]'

    Returns (bib, club, athletes_list, total_or_None).
    """
    tokens = line.split()
    if not tokens or not tokens[0].isdigit():
        return None, None, None, None

    bib = tokens[0]
    rest = line[len(bib):].strip()

    # Try to strip trailing float (grand total)
    total = None
    m = re.match(r"^(.+?)\s+(\d+\.\d+)\s*$", rest)
    if m:
        try:
            total = float(m.group(2))
            rest = m.group(1).strip()
        except ValueError:
            pass

    # rest = "{club words} {a1_first} {a1_last}, {a2_first} {a2_last}[, ...]"
    comma_idx = rest.find(", ")
    if comma_idx == -1:
        words = rest.split()
        if len(words) >= 3:
            return bib, " ".join(words[:-2]), [" ".join(words[-2:])], total
        return bib, "", [rest.strip()], total

    first_seg = rest[:comma_idx]
    remaining = [s.strip() for s in rest[comma_idx + 2:].split(", ") if s.strip()]
    first_words = first_seg.split()
    if len(first_words) >= 3:
        athlete1 = first_words[-2] + " " + first_words[-1]
        club = " ".join(first_words[:-2])
    elif len(first_words) == 2:
        athlete1 = " ".join(first_words)
        club = ""
    else:
        athlete1 = first_words[0] if first_words else ""
        club = ""

    return bib, club, [athlete1] + remaining, total


def parse_gym_acro(text_pages, pdf_path):
    """Parse GymACRO custom scoring PDFs (Victorian State Trials / Senior Vic Champs).

    Each page has one category with ranked athlete blocks. Each block:
      - rank line (bare integer)
      - athlete line: bib club athletes... [total]
      - optional standalone total line
      - Balance / Dynamic / Combined exercise lines
    """
    all_lines = []
    for text in text_pages:
        if not text:
            continue
        for line in text.splitlines():
            s = line.strip()
            if s:
                all_lines.append(s)

    if not all_lines:
        return []

    events = []
    current_level = None
    current_category = None
    current_results = []

    p_rank = p_athletes = p_club = p_total = None
    p_bal = p_dyn = p_com = None
    awaiting_total = False
    in_results = False
    comp_title = None

    def _flush_entry():
        if p_rank is not None and p_athletes:
            current_results.append({
                "rank":     p_rank,
                "athletes": "|".join(_normalise_name(a) for a in p_athletes),
                "club":     p_club or "",
                "total":    p_total,
                "bal":      p_bal,
                "dyn":      p_dyn,
                "com":      p_com,
            })

    def _emit_category(level, category, results):
        if not results:
            return
        events.append({"level": level, "category": category, "event_type": "All-Around", "results": results})
        for key, name in [("bal", "Balance"), ("dyn", "Dynamic"), ("com", "Combined")]:
            ex = [r for r in results if r.get(key) is not None]
            if not ex:
                continue
            ex_sorted = sorted(ex, key=lambda r: -(r[key] or 0))
            ex_results = [
                {"rank": i + 1, "athletes": r["athletes"], "club": r["club"],
                 "total": r[key], "bal": None, "dyn": None, "com": None}
                for i, r in enumerate(ex_sorted)
            ]
            events.append({"level": level, "category": category, "event_type": name, "results": ex_results})

    for line in all_lines:
        if _GYM_ACRO_SKIP_RE.match(line):
            continue

        if comp_title is None:
            comp_title = line
            continue

        if re.match(r"^Final Results$", line, re.IGNORECASE):
            in_results = True
            continue

        if not in_results:
            continue

        # Category header → start new event, flush previous
        cat_m = _GYM_ACRO_CATEGORY_RE.match(line)
        if cat_m:
            _flush_entry()
            _emit_category(current_level, current_category, current_results)
            level_raw = cat_m.group(1).strip()
            lw = level_raw.split()
            current_level = ("Level " + lw[-1]) if lw[0].lower() == "level" else " ".join(w.title() for w in lw)
            current_category = _gym_acro_normalise_category(cat_m.group(2), cat_m.group(3))
            current_results = []
            p_rank = p_athletes = p_club = p_total = None
            p_bal = p_dyn = p_com = None
            awaiting_total = False
            continue

        if current_level is None:
            continue

        # Standalone total on its own line (follows athlete line with no inline total)
        if awaiting_total:
            try:
                p_total = float(line)
                awaiting_total = False
                continue
            except ValueError:
                awaiting_total = False

        # Exercise line
        if _GYM_ACRO_EXERCISE_RE.match(line) and p_athletes is not None:
            floats = re.findall(r"\d+\.\d+", line)
            if floats:
                score = float(floats[-1])
                et = line.split()[0].lower()
                if et == "balance":
                    p_bal = score
                elif et == "dynamic":
                    p_dyn = score
                elif et == "combined":
                    p_com = score
            continue

        # Rank line (bare integer)
        if re.match(r"^\d+$", line):
            _flush_entry()
            p_rank = int(line)
            p_athletes = p_club = p_total = None
            p_bal = p_dyn = p_com = None
            awaiting_total = False
            continue

        # Athlete line
        if p_rank is not None and p_athletes is None:
            bib, club, athletes, total = _gym_acro_parse_athlete_line(line)
            if bib is not None:
                p_athletes = athletes
                p_club = club
                p_total = total
                if total is None:
                    awaiting_total = True

    # Final flush
    _flush_entry()
    _emit_category(current_level, current_category, current_results)

    return events


# ---------------------------------------------------------------------------
# ScoreExpress ACRO constants
# ---------------------------------------------------------------------------

# PUA font: digits 0-9 encoded as 0xE44F-0xE458; null byte (0x00) = decimal point
_SE_FONT = {
    0xe44f: "0", 0xe450: "1", 0xe451: "2", 0xe452: "3", 0xe453: "4",
    0xe454: "5", 0xe455: "6", 0xe456: "7", 0xe457: "8", 0xe458: "9",
    0x00: ".",
}

SCOREEXPRESS_RE = re.compile(r"created with ScoreExpress", re.IGNORECASE)

# "LEVEL 6 WOMEN'S GROUP - ALL-AROUND" / "LEVEL 4 JUNIOR - BALANCE" / "LEVEL 5 - ALL-AROUND"
# Also handles "JNR XX-XX" short form used by some ScoreExpress versions
_SE_SECTION_RE = re.compile(
    r"^(?P<level>LEVEL\s+\d+|(?:JUNIOR|JNR)\s+\d+-\d+|SENIOR)"
    r"(?:\s+(?P<qualifier>JUNIOR|SENIOR))?"
    r"(?:\s+(?P<category>(?:WOMEN'?S|MEN'?S|MIXED)\s+(?:GROUP|PAIR)))?"
    r"\s*-\s*"
    r"(?P<et>BALANCE|DYNAMIC|COMBINED|ALL[\s-]AROUND)$",
    re.IGNORECASE,
)

_SE_CATEGORY_MAP = {
    "WOMEN'S GROUP": "Women's Group", "WOMENS GROUP": "Women's Group",
    "WOMEN'S PAIR":  "Women's Pair",  "WOMENS PAIR":  "Women's Pair",
    "MEN'S GROUP":   "Men's Group",   "MENS GROUP":   "Men's Group",
    "MEN'S PAIR":    "Men's Pair",    "MENS PAIR":    "Men's Pair",
    "MIXED PAIR":    "Mixed Pair",
}

_SE_ET_MAP = {
    "BALANCE": "Balance", "DYNAMIC": "Dynamic",
    "COMBINED": "Combined", "ALL-AROUND": "All-Around", "ALL AROUND": "All-Around",
}


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
    # a lowercase letter is always a broken fragment — join it to the previous token.
    tokens = name.split(" ")
    joined = [tokens[0]] if tokens else []
    for tok in tokens[1:]:
        if tok and tok[0].islower():
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
    _PARTICLES = {"van", "der", "de", "von", "le", "la", "du", "den"}
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
            r"^(?:[A-Za-z]{2,6}\s*/\s*)?([A-Za-z]{2,12})(?:\s+\([A-Za-z/]+\))?(?:\d+)?(?:\s+(?:Exec:|ExNe[A-Za-z]*::?)|\s*$)"
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
                    r"^(?:[A-Za-z]{2,6}\s*/\s*)?([A-Za-z]{2,12})(?:\s+\([A-Za-z/]+\))?(?:\d+)?(?:\s+(?:Exec:|ExNe[A-Za-z]*::?)|\s*$)", l
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
                    "rank":   _parse_rank(rank_str),
                    "club":   club,
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
                    "rank":  _parse_rank(rank_str),
                    "club":  _gym_code_from_team_name(raw_name),
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
    """
    file_meta = parse_filename_meta(pdf_path, sport=sport)

    # Level + age_group from page header e.g. "Level 2 - All-around > Open"
    _hdr_re = re.compile(
        r"Level\s+(\d+)\s*[-–]\s*All.?[Aa]round\s*[>|]\s*(\w+)", re.IGNORECASE
    )
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

    events_by_key = {}   # (level, age_group) → list of results

    for text in text_pages:
        if not text:
            continue

        level = file_meta.get("level")
        age_group = file_meta.get("age_group")

        hdr_m = _hdr_re.search(text)
        if hdr_m:
            level = int(hdr_m.group(1))
            ag_raw = hdr_m.group(2).strip().lower()
            age_group = "Open" if ag_raw == "open" else "Under" if "under" in ag_raw else ag_raw.title()

        if level is None:
            continue

        key = (level, age_group)
        if key not in events_by_key:
            events_by_key[key] = []

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
                name = _clean_name(m.group(2))
                raw_scores = [re.match(r"([\d.]+)", g).group(1) for g in m.groups()[2:8]]
                scores = [_parse_score(s) for s in raw_scores]
                total = _parse_score(m.group(9))
                row = {
                    "rank":    _parse_rank(rank_str),
                    "bib":     None,
                    "athlete": name,
                    "club":    None,
                    "total":   total,
                }
                row.update(_build_app_scores(scores, [], [], sport))
                events_by_key[key].append(row)
                pending = row
                continue

            if pending is not None and pending["club"] is None:
                if not _skip_line.match(line):
                    pending["club"] = line.strip()
                    pending = None

    return [
        {"level": lvl, "division": None, "age_group": ag, "event_type": "AA", "results": results}
        for (lvl, ag), results in events_by_key.items()
        if results
    ]


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


def parse_gymp_individual(full_text, level, division):
    """Parse GymPro individual results. Each row:
       ID First Last Club... D score rank D score rank D score rank D score rank total rank
       = 14 trailing numeric tokens (4 apparatus with difficulty + total + rank)
    """
    results = []
    for line in full_text.splitlines():
        tokens = line.split()
        # Must start with a numeric bib and have enough tokens
        if not tokens or not tokens[0].isdigit() or len(tokens) < 7:
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
            name = middle[0] + " " + middle[1]
            club = " ".join(middle[2:]).upper() if len(middle) > 2 else ""
            results.append({"rank": rank, "athlete": _clean_name(name), "club": club, "total": total})
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
        club = " ".join(tokens[:-10]).upper()
        results.append({"rank": rank, "club": club, "total": total, "athlete": None})
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
        results = parse_gymp_individual(full_text, level, division)
        event_type = "AA"

    if not results:
        return []
    return [{"level": level, "division": division, "event_type": event_type, "results": results}]


# ---------------------------------------------------------------------------
# ScoreExpress ACRO parser
# ---------------------------------------------------------------------------

def _decode_se(s: str) -> str:
    return "".join(_SE_FONT.get(ord(c), c) for c in s)


def _se_normalise_level(s: str) -> str:
    u = s.strip().upper()
    if u.startswith("LEVEL"):
        return "Level " + u.split()[-1]
    if u.startswith("JUNIOR") or u.startswith("JNR"):
        return "Junior " + s.strip().split()[-1]
    if u == "SENIOR":
        return "Senior"
    return s.strip().title()


def _se_parse_aa_results(lines, group_size=None):
    """Parse an All-Around section. group_size=None means auto-detect per block."""
    FLOATS = re.compile(r"\d+\.\d+")
    results = []
    i = 0
    while i < len(lines):
        m = re.match(r"^(\d+)\s+(.+)", lines[i])
        if not m:
            i += 1
            continue
        rank = int(m.group(1))
        rest0 = m.group(2)
        # "a1 diff X.XXX [diff X.XXX ...]"
        diff_parts = re.split(r"\s+diff\b", rest0)
        if len(diff_parts) < 2:
            i += 1
            continue
        a1 = diff_parts[0].strip()
        if i + 4 >= len(lines):
            break
        l1, l2, l3, l4 = lines[i + 1], lines[i + 2], lines[i + 3], lines[i + 4]
        athletes = [a1]
        # Auto-detect per block: l3 = "pen X.XXX" means pair; "Club pen X.XXX" means group
        gs = group_size if group_size is not None else (
            2 if re.match(r"^pen\s", l3.strip(), re.IGNORECASE) else 3
        )
        # Detect block size: some formats include a bib number between the last data line
        # and the totals summary line, making a 6-line block instead of 5.
        l4_stripped = l4.strip()
        if re.match(r"^\d+$", l4_stripped):
            # l4 is a bib number (pure integer) — totals are on l5
            totals_line = lines[i + 5] if i + 5 < len(lines) else ""
            advance = 6
        elif re.match(r"^[\d\s.]+$", l4_stripped) and "." in l4_stripped:
            # l4 is already the totals line (all digits/spaces/dots, has a decimal)
            totals_line = l4
            advance = 5
        else:
            # l4 is the next rank block — no separate totals for this block
            totals_line = ""
            advance = 4
        if gs == 3:
            # l1: a2+art, l2: a3+exec, l3: club+pen
            a2_p = re.split(r"\s+art\b", l1)
            athletes.append(a2_p[0].strip())
            a3_p = re.split(r"\s+exec\b", l2)
            athletes.append(a3_p[0].strip())
            club_p = re.split(r"\s+pen\b", l3)
            club = club_p[0].strip()
        else:
            # l1: a2+art, l2: club+exec, l3: pen line
            # Some PDFs (ScoreExpress bug) put the athlete name on the exec row and
            # the club on the pen row instead. Detect by checking if l3 has a non-bib
            # alphabetic prefix before "pen".
            a2_p = re.split(r"\s+art\b", l1)
            athletes.append(a2_p[0].strip())
            pen_p = re.split(r"\s+pen\b", l3, maxsplit=1)
            pen_prefix = pen_p[0].strip() if pen_p else ""
            if pen_prefix and not pen_prefix.isdigit():
                club = pen_prefix
            else:
                club_p = re.split(r"\s+exec\b", l2)
                club = club_p[0].strip()
        totals = [float(v) for v in FLOATS.findall(totals_line)]
        grand_total = totals[-1] if totals else None
        rt = totals[:-1]
        results.append({
            "rank":     rank,
            "athletes": "|".join(athletes),
            "club":     club,
            "total":    grand_total,
            "bal":      rt[0] if len(rt) > 0 else None,
            "dyn":      rt[1] if len(rt) > 1 else None,
            "com":      rt[2] if len(rt) > 2 else None,
        })
        i += advance
    return results


def _se_parse_labelled_routine_results(lines):
    """Parse Balance/Dynamic/Combined in the labelled-per-line ScoreExpress format.

    Each 5-line block:
      pair:  rank+a1+diff, a2+art, club+exec, pen, totals
      group: rank+a1+diff, a2+art, a3+exec,  club+pen, totals
    Pair vs group auto-detected: l3 starts with 'pen' → pair.
    """
    FLOAT_RE = re.compile(r"\d+\.\d+")
    results = []
    i = 0
    while i + 4 < len(lines):
        m = re.match(r"^(\d+)\s+(.+)", lines[i])
        if not m:
            i += 1
            continue
        rank = int(m.group(1))
        rest0 = m.group(2)
        diff_p = re.split(r"\s+diff\b", rest0, maxsplit=1)
        if len(diff_p) < 2:
            i += 1
            continue
        a1 = diff_p[0].strip()
        diff_m = FLOAT_RE.search(diff_p[1])
        diff = float(diff_m.group()) if diff_m else None

        l1, l2, l3, l4 = lines[i+1], lines[i+2], lines[i+3], lines[i+4]
        is_pair = bool(re.match(r"^pen\s", l3.strip(), re.IGNORECASE))

        if is_pair:
            art_p = re.split(r"\s+art\b", l1, maxsplit=1)
            a2 = art_p[0].strip()
            art_m = FLOAT_RE.search(art_p[1]) if len(art_p) > 1 else None
            art = float(art_m.group()) if art_m else None
            exec_p = re.split(r"\s+exec\b", l2, maxsplit=1)
            club = exec_p[0].strip()
            exec_m = FLOAT_RE.search(exec_p[1]) if len(exec_p) > 1 else None
            exec_s = float(exec_m.group()) if exec_m else None
            pen_m = FLOAT_RE.search(l3)
            pen = float(pen_m.group()) if pen_m else 0.0
            athletes = [a1, a2]
        else:
            art_p = re.split(r"\s+art\b", l1, maxsplit=1)
            a2 = art_p[0].strip()
            art_m = FLOAT_RE.search(art_p[1]) if len(art_p) > 1 else None
            art = float(art_m.group()) if art_m else None
            exec_p = re.split(r"\s+exec\b", l2, maxsplit=1)
            a3 = exec_p[0].strip()
            exec_m = FLOAT_RE.search(exec_p[1]) if len(exec_p) > 1 else None
            exec_s = float(exec_m.group()) if exec_m else None
            pen_p = re.split(r"\s+pen\b", l3, maxsplit=1)
            club = pen_p[0].strip()
            pen_m = FLOAT_RE.search(pen_p[1]) if len(pen_p) > 1 else None
            pen = float(pen_m.group()) if pen_m else 0.0
            athletes = [a1, a2, a3]

        totals = [float(v) for v in FLOAT_RE.findall(l4)]
        total = totals[-1] if totals else None
        results.append({
            "rank":       rank,
            "athletes":   "|".join(athletes),
            "club":       club,
            "diff":       diff,
            "art":        art,
            "exec_score": exec_s,
            "pen":        pen,
            "total":      total,
        })
        i += 5
    return results


def _se_parse_routine_results(lines, group_size):
    """Parse a Balance/Dynamic/Combined section. block_size = group_size + 2."""
    results = []
    block_size = group_size + 2
    i = 0
    while i + group_size + 1 <= len(lines):  # need rank line + (n-1) athletes + club at minimum
        m = re.match(r"^(\d+)\s+(.+)", lines[i])
        if not m:
            i += 1
            continue
        rank = int(m.group(1))
        rest0 = m.group(2)
        # Name = tokens before first float (X.Y... pattern)
        tokens = rest0.split()
        score_start = next(
            (j for j, t in enumerate(tokens) if re.match(r"^\d+\.", t)), len(tokens)
        )
        a1 = " ".join(tokens[:score_start])
        scores = [float(t) for t in tokens[score_start:] if re.match(r"^\d+\.?\d*$", t)]
        if not a1 or len(scores) < 5:
            i += 1
            continue
        # 6 values: P1, D, P, E, A, T — take [1:]; 5 values: D, P, E, A, T
        if len(scores) >= 6:
            diff, pen, exec_s, art, total = scores[1], scores[2], scores[3], scores[4], scores[5]
        else:
            diff, pen, exec_s, art, total = scores[0], scores[1], scores[2], scores[3], scores[4]
        athletes = [a1]
        for j in range(1, group_size):
            idx = i + j
            if idx < len(lines):
                athletes.append(lines[idx].strip())
        club_idx = i + group_size
        club = lines[club_idx].strip() if club_idx < len(lines) else ""
        results.append({
            "rank":       rank,
            "athletes":   "|".join(athletes),
            "club":       club,
            "diff":       diff,
            "pen":        pen,
            "exec_score": exec_s,
            "art":        art,
            "total":      total,
        })
        i += block_size
    return results


def parse_scoreexpress_acro(text_pages, pdf_path):
    """Parse ScoreExpress ACRO PDFs. Returns list of event dicts."""
    decoded = [_decode_se(p) for p in text_pages if p]
    if not decoded:
        return []
    first_lines = decoded[0].split("\n")
    comp_title = next((l.strip() for l in first_lines if l.strip()), "")
    title_re = re.compile(r"^" + re.escape(comp_title) + r"$", re.IGNORECASE) if comp_title else None

    _NOISE = re.compile(
        r"created with ScoreExpress|^�|"
        r"^\d{2}/\d{2}/\d{4}|"
        r"^PLACE\s+PARTICIPANT",
        re.IGNORECASE,
    )

    all_lines = []
    for page_text in decoded:
        for line in page_text.split("\n"):
            s = line.strip()
            if not s:
                continue
            if _NOISE.search(s):
                continue
            if title_re and title_re.match(s):
                continue
            all_lines.append(s)

    sections = []
    current_match = None
    current_lines = []
    for line in all_lines:
        m = _SE_SECTION_RE.match(line)
        if m:
            if current_match and current_lines:
                sections.append((current_match, current_lines))
            current_match = m
            current_lines = []
        elif current_match is not None:
            current_lines.append(line)
    if current_match and current_lines:
        sections.append((current_match, current_lines))

    events = []
    for hdr_m, sec_lines in sections:
        level_raw  = hdr_m.group("level")
        cat_raw    = hdr_m.group("category") or ""
        qualifier  = hdr_m.group("qualifier") or ""
        type_raw   = hdr_m.group("et")
        level      = _se_normalise_level(level_raw)
        if qualifier and cat_raw:
            level  = f"{level} {qualifier.title()}"
        if cat_raw:
            category   = _SE_CATEGORY_MAP.get(cat_raw.strip().upper(), cat_raw.strip().title())
            group_size = 3 if "GROUP" in cat_raw.upper() else 2
        elif qualifier:
            category   = qualifier.title()   # "Junior" or "Senior" stored as category
            group_size = None
        else:
            category   = None
            group_size = None
        et_key     = type_raw.upper().replace(" ", "-")
        event_type = _SE_ET_MAP.get(et_key, type_raw.title())
        is_aa      = event_type == "All-Around"
        if is_aa:
            results = _se_parse_aa_results(sec_lines, group_size)
        else:
            # Labelled format: rank line contains "diff" keyword; inline: scores only on rank line
            first_rank = next((l for l in sec_lines if re.match(r"^\d+\s+", l)), None)
            if first_rank and re.search(r"\bdiff\b", first_rank, re.IGNORECASE):
                results = _se_parse_labelled_routine_results(sec_lines)
            else:
                results = _se_parse_routine_results(sec_lines, group_size or 2)
        if results:
            events.append({
                "level":      level,
                "category":   category,
                "event_type": event_type,
                "results":    results,
            })
    return events


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

    # GymACRO custom scoring system (Victorian State Trials / Vic Champs format)
    if sport == "ACRO" and GYM_ACRO_DETECT_RE.search(full_text):
        events = parse_gym_acro(text_pages, pdf_path)
        return (events, "gym-acro") if events else ([], "gym-acro-empty")

    # ScoreExpress ACRO — check before filename-meta guard (ACRO files have no level in name)
    if sport == "ACRO" and SCOREEXPRESS_RE.search(full_text):
        events = parse_scoreexpress_acro(text_pages, pdf_path)
        return (events, "scoreexpress-acro") if events else ([], "scoreexpress-acro-empty")

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
            if sport == "ACRO":
                ev_entry["category"] = ev.get("category")
            for r in ev_entry["results"]:
                r.pop("bib", None)
            comp_map[map_key]["events"].append(ev_entry)
    return list(comp_map.values())
