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

# Old ProScore header: "Meet Results - Level 6 Division 1 Women / 6D1"
PROSCORE_MEET_HDR = re.compile(
    r"Meet Results\s*[-\u2013]\s*Level\s+(\d+)\s+Division\s+(\d+)\s+(\w+)",
    re.IGNORECASE,
)

# Detects "Team Results" even when letters are space-separated (BTYC font issue)
TEAM_RESULTS_RE = re.compile(
    r"T\s*e\s*a\s*m\s+R\s*e\s*s\s*u\s*l\s*t\s*s",
    re.IGNORECASE,
)

# Detects "Meet Results" in either clean or spaced form
MEET_RESULTS_RE = re.compile(
    r"M\s*e\s*e\s*t\s+R\s*e\s*s\s*u\s*l\s*t\s*s",
    re.IGNORECASE,
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


# International level codes (101-104) — no division, no numeric level in filename
_INT_LEVEL_KEYWORDS = [
    ("developing international", 101),
    ("future international",     102),
    ("junior international",     103),
    ("senior international",     104),
]
INT_LEVEL_LABELS = {101: "Dev Int", 102: "Fut Int", 103: "Jun Int", 104: "Sen Int"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_rank(s):
    try:
        return int(re.sub(r"[^0-9]", "", s))
    except ValueError:
        return None


def _clean_name(name):
    """Strip ProScore annotation characters and rejoin words broken by PDF spacing."""
    name = re.sub(r"^[\*\s]+|[\*\s]+$", "", name)
    # PDF text extraction sometimes inserts spaces mid-word. A token starting with
    # a lowercase letter is always a broken fragment — join it to the previous token.
    tokens = name.split(" ")
    joined = [tokens[0]] if tokens else []
    for tok in tokens[1:]:
        if tok and tok[0].islower():
            joined[-1] += tok
        else:
            joined.append(tok)
    return " ".join(joined)


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

def parse_proscore_text(text_pages):
    events = []
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

        results = []
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
                results.append(prev_athlete)
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
                results.append(rec)
                prev_athlete = rec
                continue

            if prev_athlete and CLUB_LINE.match(line):
                prev_athlete["club"] = line.split()[0].upper()
                prev_athlete = None

        if results:
            events.append({"level": level, "division": div, "event_type": event_type, "results": results})

    return events


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

        # Scan backward up to 5 lines for club, rank+name, and D/E scores
        rank = bib = name = club = None
        d_scores = []
        e_scores = []
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
                    r"^(?:[A-Z]{2,6}/)?([A-Z]{2,12})(?:\s+\([A-Z/]+\))?(?:\d+)?(?:\s+(?:Exec:|ExNe[A-Za-z]*::?)|\s*$)", l
                )
                if m_club and not l.startswith(("ND:", "Final:", "Place:", "D/E:", "Diff:", "DN/DE:")):
                    club = m_club.group(1)

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
            d = (d_scores + [None] * n_app)[:n_app] if d_scores else [None] * n_app
            e = (e_scores + [None] * n_app)[:n_app] if e_scores else [None] * n_app
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

    level_m = re.search(r"(?:level|lvl|alp|L)[_\s-]*(\d+)", name, re.IGNORECASE)
    div_m   = re.search(r"(?:div(?:ision)?|D)[_\s-]*(\d+)", name, re.IGNORECASE)
    level   = int(level_m.group(1)) if level_m else None

    # Event type: Team → skip later; apparatus codes; AA by default
    type_m = re.search(
        r"\b(AA|all.?around|VT|UB|BB|FX|PH|SR|PB|HB|vault|bars|beam|floor|team)\b",
        name, re.IGNORECASE,
    )
    event_type = "AA"
    if type_m:
        raw = type_m.group(1).upper().replace("-", "")
        event_type = {
            "ALLAROUND": "AA", "AA": "AA",
            "VT": "VT", "VAULT": "VT",
            "UB": "UB", "BARS": "UB",
            "BB": "BB", "BEAM": "BB",
            "FX": "FX", "FLOOR": "FX",
            "PH": "PH", "SR": "SR", "PB": "PB", "HB": "HB",
            "TEAM": "Team",
        }.get(raw, raw)

    if re.search(r"meet.results", name, re.IGNORECASE):
        event_type = "AA"
    if re.search(r"team.results|team", name, re.IGNORECASE) and "Team" not in event_type:
        if re.search(r"\bteam\b", name, re.IGNORECASE):
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
        elif re.search(r"Under|(?<=\d)U\b", name, re.IGNORECASE):
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
    return {
        "level":      level,
        "division":   int(div_m.group(1)) if div_m else None,
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
# MAG variant: total + 6 apparatus (VT FX PH SR PB HB)
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
    r"/\s*([\d][\d\s]{0,3})\s*/",                   # "/ 31 /"
    re.IGNORECASE,
)


def _split_combined_level_div(code):
    """'31' → (3, 1), '101' → (10, 1), '10' → (10, None)."""
    code = re.sub(r"\s+", "", str(code))
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

    for text in text_pages:
        if not text:
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
                    "vault":  _parse_score(s1),
                    "floor":  _parse_score(s2),
                    "pommel": _parse_score(s3),
                    "rings":  _parse_score(s4),
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
            pl = int(hdr_m.group(1))
            suffix = hdr_m.group(2).upper()
            if suffix.startswith("D") and len(suffix) > 1:
                pd = int(suffix[1:])       # "D1" → 1, "D2" → 2
            else:
                pd = _div_from_letter(suffix)  # "A" → 1, "B" → 2, "" → None
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
                # WAG: try 5-score (spare) first, then standard 4-score
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
                "athlete": " ".join(name_parts),
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
    with pdfplumber.open(pdf_path) as pdf:
        text_pages = [page.extract_text() for page in pdf.pages]

    full_text = "\n".join(t for t in text_pages if t)

    meta = parse_filename_meta(pdf_path, sport=sport)
    age_group = meta.get("age_group")

    # WG scoring program (Natimuk style)
    if WG_HDR_RE.search(full_text):
        events = parse_wg(text_pages, pdf_path)
        return (_inject_age_group(events, age_group), "wg") if events else ([], "wg-empty")

    # GymPro format (Eclipse-style)
    if "GymPro" in full_text:
        events = parse_gymp(text_pages, pdf_path)
        return (_inject_age_group(events, age_group), "gymp") if events else ([], "gymp-empty")

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

    # Old ProScore format (Meet Results - Level X Division Y)
    if any(t and PROSCORE_MEET_HDR.search(t) for t in text_pages):
        events = parse_proscore_text(text_pages)
        if events:
            return _inject_age_group(events, age_group), "proscore"

    # Simple ProScore format (Meet Results Women / 5A / All Ages)
    if any(t and PROSCORE_SIMPLE_HDR.search(t) for t in text_pages):
        events = parse_proscore_simple(text_pages, pdf_path, sport=sport)
        if events:
            return _inject_age_group(events, age_group), "proscore-simple"

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
