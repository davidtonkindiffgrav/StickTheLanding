"""
update.py — Ingest result PDFs into sport-specific databases.

Drop PDFs into pdfs/<year>/<SPORT>/<competition>/.
Competition name is inferred from the immediate parent folder.

Usage:
    python update.py                     # ingest new WAG PDFs
    python update.py --sport MAG         # ingest new MAG PDFs
    python update.py --resolve-urls      # populate GymVic URLs for WAG manifest entries
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import db
from pdf_parser import parse_pdf, group_into_competitions

PDF_ROOT       = Path("pdfs")
CLUBS_FILE     = Path("data/clubs.json")
OVERRIDES_FILE = Path("data/overrides.json")
URL_MAP_FILE   = Path("data/url_map.json")

SP_BASE = "https://gymnasticsvictoria.sharepoint.com"
SP_SITE = "/sites/GymnasticsVictoriaHub"


# ---------------------------------------------------------------------------
# Club / override loading (unchanged)
# ---------------------------------------------------------------------------

def load_club_aliases() -> dict:
    if not CLUBS_FILE.exists():
        return {}
    with open(CLUBS_FILE, encoding="utf-8") as f:
        data = json.load(f)
    lookup = {}
    for club in data.get("clubs", []):
        code = club["code"]
        for alias in club.get("aliases", []):
            lookup[alias.upper()] = code
    return lookup


def load_overrides() -> dict:
    if not OVERRIDES_FILE.exists():
        return {}
    with open(OVERRIDES_FILE, encoding="utf-8") as f:
        data = json.load(f)
    by_athlete = [
        e for e in data.get("by_athlete", [])
        if "competition" in e and "athlete" in e and "club" in e
    ]
    return {
        "hosts": {k: v for k, v in data.get("hosts", {}).items() if not k.startswith("_")},
        "by_competition": {k: v for k, v in data.get("by_competition", {}).items() if not k.startswith("_")},
        "by_athlete": by_athlete,
    }


BARE_CODES = {
    "WHI", "BLU", "RED", "SIL", "NAV", "GOL", "BLA",
    "GRN", "PNK", "PUR", "YEL", "ORG", "ORA", "TEA",
    "MX1", "MX2", "MX3", "MX4",
}

COLOUR_SUFFIXES = {
    "WHITE", "BLUE", "RED", "SILVER", "GOLD", "BLACK", "NAVY", "TEAL",
    "PURPLE", "GREEN", "PINK", "YELLOW", "ORANGE", "AQUA", "MAROON",
    "GREY", "GRAY", "BRONZE", "COPPER", "PLATINUM", "INDIGO", "VIOLET",
    "CRIMSON", "SCARLET", "RUBY", "JADE", "AMBER", "CORAL", "LIME",
    "INDIVIDUAL",
}


def _strip_colour(raw: str, aliases: dict):
    parts = raw.rsplit(" ", 1)
    if len(parts) == 2 and parts[1] in COLOUR_SUFFIXES:
        base = parts[0]
        if base in aliases:
            return aliases[base]
    return None


def normalise_clubs(competitions: list, aliases: dict, overrides: dict) -> None:
    by_comp    = overrides.get("by_competition", {})
    hosts      = overrides.get("hosts", {})
    by_athlete = {
        (e["competition"], e["athlete"]): e["club"]
        for e in overrides.get("by_athlete", [])
    }

    for comp in competitions:
        comp_name     = comp.get("name", "")
        comp_overrides = by_comp.get(comp_name, {})
        host          = hosts.get(comp_name)
        for ev in comp.get("events", []):
            for r in ev.get("results", []):
                raw = (r.get("club") or "").upper()
                if raw in aliases:
                    r["club"] = aliases[raw]
                elif _strip_colour(raw, aliases):
                    r["club"] = _strip_colour(raw, aliases)
                resolved = r.get("club") or raw
                if host and resolved in BARE_CODES:
                    r["club"] = host
                resolved = r.get("club") or raw
                if resolved in comp_overrides:
                    r["club"] = comp_overrides[resolved]
                athlete_key = (comp_name, r.get("athlete", ""))
                if athlete_key in by_athlete:
                    r["club"] = by_athlete[athlete_key]


# ---------------------------------------------------------------------------
# PDF helpers (unchanged logic)
# ---------------------------------------------------------------------------

def source_key(pdf: Path) -> str:
    rel = pdf.relative_to(PDF_ROOT)
    # Include year and competition name so same-named files in different years/comps are distinct
    prefix = str(Path(*rel.parts[:3]))  # e.g. "2026/WAG/Senior Judges Invitational"
    suffix = str(Path(*rel.parts[3:])) if len(rel.parts) > 4 else rel.parts[-1]
    return f"{prefix}/{suffix}"


def comp_name(pdf: Path) -> str:
    rel = pdf.relative_to(PDF_ROOT)
    return rel.parts[2] if len(rel.parts) >= 3 else pdf.parent.name


def comp_year(pdf: Path) -> str:
    rel = pdf.relative_to(PDF_ROOT)
    return rel.parts[0] if rel.parts and re.match(r"^\d{4}$", rel.parts[0]) else str(__import__("datetime").date.today().year)


# ---------------------------------------------------------------------------
# URL resolution
# ---------------------------------------------------------------------------

def load_url_map() -> dict:
    if URL_MAP_FILE.exists():
        with open(URL_MAP_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_url_map(url_map: dict) -> None:
    URL_MAP_FILE.parent.mkdir(exist_ok=True)
    with open(URL_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(url_map, f, indent=2, ensure_ascii=False)


NOT_FOUND_MARKER = "__not_found__"

# Module-level cache so GymVic page is fetched at most once per process
_gymvic_comps_cache = None
_gymvic_session_cache = None


def _token_overlap(a: str, b: str) -> int:
    ta = set(re.sub(r"[^a-z0-9]", " ", a.lower()).split())
    tb = set(re.sub(r"[^a-z0-9]", " ", b.lower()).split())
    return len(ta & tb)


def _load_gymvic_comps():
    """Fetch GymVic results page once; return (session, comps_list) or (None, None)."""
    global _gymvic_comps_cache, _gymvic_session_cache
    if _gymvic_comps_cache is not None:
        return _gymvic_session_cache, _gymvic_comps_cache
    try:
        import requests
        import scraper as _sc
    except ImportError:
        return None, None
    session = requests.Session()
    session.headers.update(_sc.HEADERS)
    try:
        import datetime
        year = datetime.date.today().year
        html = _sc.fetch_results_page(session)
        nuxt = _sc.parse_nuxt_data(html)
        wag_html = _sc.find_wag_year_html(nuxt, year)
        comps = _sc.extract_competitions(wag_html)
        _gymvic_comps_cache = comps
        _gymvic_session_cache = session
        print(f"  [GymVic] fetched {len(comps)} competition entries from results page")
        return session, comps
    except Exception as exc:
        print(f"  [WARN] GymVic fetch failed: {exc}")
        _gymvic_comps_cache = []
        return None, []


def _resolve_comp_all_files(comp_name_str: str, url_map: dict) -> None:
    """
    Resolve ALL files for a competition in one network call and cache them in url_map.
    After calling this, url_map[comp_name_str][source_key] will be set for every file
    found on GymVic (or NOT_FOUND_MARKER if the competition isn't on the page).
    """
    from urllib.parse import urlparse, parse_qs, unquote
    try:
        import scraper as _sc
    except ImportError:
        return

    session, comps = _load_gymvic_comps()
    if not comps:
        url_map.setdefault(comp_name_str, {})["__comp__"] = NOT_FOUND_MARKER
        return

    best_comp = max(comps, key=lambda c: _token_overlap(comp_name_str, c["name"]), default=None)
    score = _token_overlap(comp_name_str, best_comp["name"]) if best_comp else 0
    if score < 2:  # require at least 2 matching tokens to avoid false positives
        url_map.setdefault(comp_name_str, {})["__comp__"] = NOT_FOUND_MARKER
        return

    print(f"  [URL]  '{comp_name_str}' → '{best_comp['name']}' (score={score})")
    url = best_comp["url"]
    ltype = best_comp["link_type"]

    if ltype == "pdf":
        dl_url = url if "download=1" in url else url + ("&" if "?" in url else "?") + "download=1"
        url_map.setdefault(comp_name_str, {})["__pdf__"] = dl_url
        return

    # Folder — list all files and cache each by filename (used as source_key)
    try:
        r = session.get(url, timeout=30, allow_redirects=True)
        parsed = urlparse(r.url)
        qs = parse_qs(parsed.query)
        folder_id = qs.get("id", [None])[0]
        if not folder_id:
            url_map.setdefault(comp_name_str, {})["__comp__"] = NOT_FOUND_MARKER
            return
        sp_path = unquote(folder_id)
        pdf_files = _sc.list_sharepoint_folder(sp_path, session)
        comp_map = url_map.setdefault(comp_name_str, {})
        for pf in pdf_files:
            # Store Viewing.aspx URL — the download URL returns Access Denied
            view_url = _sc.pdf_view_url(pf["ServerRelativeUrl"])
            comp_map[pf["Name"]] = view_url
        if not pdf_files:
            comp_map["__comp__"] = NOT_FOUND_MARKER
        time.sleep(0.3)
    except Exception as exc:
        print(f"  [WARN] Folder resolve failed for '{comp_name_str}': {exc}")
        url_map.setdefault(comp_name_str, {})["__comp__"] = NOT_FOUND_MARKER


_TYPE_MAP = {
    "team": "team", "teams": "team",
    "meet": "meet", "individual": "meet",
    "aa": "meet", "apparatus": "meet",
    # "results" alone is too ambiguous — handled separately below
}


def _parse_pdf_attrs(filename: str) -> dict:
    """
    Extract structured attributes from a PDF filename:
    sessions, levels, divs, alps, types.
    Handles both verbose ("Session 3", "Level 4", "Division 2") and
    abbreviated ("S3", "L4", "D2", "L4D2") formats.
    """
    name = Path(filename).name
    s = re.sub(r"[^a-z0-9]", " ", name.lower())
    tokens = s.split()

    sessions, levels, divs, alps, types = set(), set(), set(), set(), set()

    for i, t in enumerate(tokens):
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""

        # "session N" or "session Na"
        if t == "session" and nxt and nxt[0].isdigit():
            m = re.match(r"(\d+)", nxt)
            if m:
                sessions.add(m.group(1))

        # Standalone "S3" or "S3a" token
        m = re.fullmatch(r"s(\d+)[a-z]?", t)
        if m:
            sessions.add(m.group(1))

        # "level N" with keyword
        if t in ("level",) and nxt and nxt.isdigit():
            levels.add(nxt)

        # Standalone "L4" token (but not "l" alone)
        m = re.fullmatch(r"l(\d+)", t)
        if m and len(t) >= 2:
            levels.add(m.group(1))

        # Compact "L4D2" token
        m = re.fullmatch(r"l(\d+)d(\d+)", t)
        if m:
            levels.add(m.group(1))
            divs.add(m.group(2))

        # "div N" or "division N"
        if t in ("div", "division") and nxt and nxt[0].isdigit():
            m = re.match(r"(\d+)", nxt)
            if m:
                divs.add(m.group(1))

        # Standalone "D1" token (but not "d" alone)
        m = re.fullmatch(r"d(\d+)", t)
        if m and len(t) >= 2:
            divs.add(m.group(1))

        # "ALP N" or standalone "ALP5"
        if t == "alp" and nxt and nxt[0].isdigit():
            alps.add(nxt[0])
        m = re.fullmatch(r"alp(\d+)[a-z]?", t)
        if m:
            alps.add(m.group(1))

        # Type keyword
        if t in _TYPE_MAP:
            types.add(_TYPE_MAP[t])

    # "results" alone (without "team") implies a meet/individual sheet
    name_lower = name.lower()
    if "result" in name_lower and "team" not in name_lower:
        types.add("meet")

    return {"sessions": sessions, "levels": levels, "divs": divs,
            "alps": alps, "types": types}


def _fuzzy_url_match(local_key: str, comp_map: dict):
    """
    Fallback filename match using structured attribute comparison
    (session, level, division, ALP, type).
    Returns a URL only when the match is unambiguous.
    """
    lp = _parse_pdf_attrs(local_key)

    candidates = []
    for gymvic_name, url in comp_map.items():
        if gymvic_name.startswith("__") or url == NOT_FOUND_MARKER:
            continue
        gp = _parse_pdf_attrs(gymvic_name)

        score = 0
        conflict = False

        def _check(la, ga, weight):
            nonlocal score, conflict
            if la and ga:
                shared = la & ga
                if shared:
                    score += len(shared) * weight
                else:
                    conflict = True

        _check(lp["sessions"], gp["sessions"], 3)
        _check(lp["levels"],   gp["levels"],   3)
        _check(lp["alps"],     gp["alps"],      3)
        _check(lp["divs"],     gp["divs"],      2)
        _check(lp["types"],    gp["types"],     1)

        # Levels and ALPs are different gymnastics programs — reject cross-system matches
        if lp["levels"] and gp["alps"] and not gp["levels"]:
            conflict = True
        if lp["alps"] and gp["levels"] and not gp["alps"]:
            conflict = True

        if not conflict and score > 0:
            candidates.append((score, gymvic_name, url))

    if not candidates:
        return None

    candidates.sort(key=lambda x: -x[0])
    # Require a clear winner (no tie at top score)
    if len(candidates) == 1 or candidates[0][0] > candidates[1][0]:
        return candidates[0][2]
    return None


def resolve_pdf_url(comp_name_str: str, source_key_str: str, url_map: dict):
    """Return SharePoint download URL for a PDF, or None if not found."""
    comp_cache = url_map.get(comp_name_str, {})

    # Already cached (exact source_key match)
    cached = comp_cache.get(source_key_str)
    if cached and cached != NOT_FOUND_MARKER:
        return cached
    if cached == NOT_FOUND_MARKER:
        return None

    # Check by filename only (folder links cache by bare filename)
    filename = Path(source_key_str).name
    cached = comp_cache.get(filename)
    if cached and cached != NOT_FOUND_MARKER:
        return cached

    # Check __pdf__ marker (single-PDF comps)
    if "__pdf__" in comp_cache:
        url = comp_cache["__pdf__"]
        return url if url != NOT_FOUND_MARKER else None

    # Competition already attempted and not found
    if "__comp__" in comp_cache:
        return None

    # First time seeing this competition — resolve all files at once
    _resolve_comp_all_files(comp_name_str, url_map)

    # Re-check after resolution
    comp_cache = url_map.get(comp_name_str, {})
    cached = comp_cache.get(source_key_str) or comp_cache.get(filename) or comp_cache.get("__pdf__")
    if cached and cached != NOT_FOUND_MARKER:
        return cached

    # Fuzzy fallback: match by session/level/division identifiers
    return _fuzzy_url_match(source_key_str, comp_cache)


# ---------------------------------------------------------------------------
# DB write helpers
# ---------------------------------------------------------------------------

def store_competitions(con, new_comps: list) -> None:
    for comp in new_comps:
        db.upsert_competition(con, comp)
        print(f"  [NEW]  {comp['name']} — {len(comp['events'])} event(s)")
        for ev in comp["events"]:
            event_id = db.insert_event(con, comp["id"], ev)
            for r in ev.get("results", []):
                db.insert_result(con, event_id, r)


def update_existing_competition(con, comp: dict) -> None:
    existing_keys = set()
    rows = con.execute(
        "SELECT level, division, event_type, source_file FROM events WHERE competition_id = ?",
        (comp["id"],),
    ).fetchall()
    for row in rows:
        existing_keys.add((row["level"], row["division"], row["event_type"], row["source_file"]))

    added = 0
    for ev in comp["events"]:
        key = (ev.get("level"), ev.get("division"), ev.get("event_type"), ev.get("source_file"))
        if key not in existing_keys:
            event_id = db.insert_event(con, comp["id"], ev)
            for r in ev.get("results", []):
                db.insert_result(con, event_id, r)
            added += 1

    label = f"+{added} new event(s)" if added else "already up to date"
    print(f"  [UPD]  {comp['name']} — {label}")


def merge_to_db(con, new_comps: list) -> None:
    for comp in new_comps:
        exists = con.execute(
            "SELECT 1 FROM competitions WHERE id = ?", (comp["id"],)
        ).fetchone()
        if not exists:
            store_competitions(con, [comp])
        else:
            update_existing_competition(con, comp)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolve-urls", action="store_true",
                        help="Populate GymVic URLs for all pdf_manifest entries missing a URL")
    parser.add_argument("--sport", default="WAG", choices=["WAG", "MAG", "ACRO"],
                        help="Which sport to ingest (default: WAG)")
    args = parser.parse_args()

    sport = args.sport
    db.DB_PATH = Path(f"data/stick_{sport}.db")
    dbconfig_file = Path(f"data/dbconfig_{sport}.json")

    con = db.get_conn(db.DB_PATH)
    db.create_schema(con)

    if args.resolve_urls:
        _cmd_resolve_urls(con)
        return

    already_ingested = db.get_processed_files(con)

    # Only walk the sport-specific subtree: pdfs/<year>/<SPORT>/
    all_pdfs = sorted(
        p for p in PDF_ROOT.rglob("*.pdf")
        if len(p.relative_to(PDF_ROOT).parts) >= 2
        and p.relative_to(PDF_ROOT).parts[1].upper() == sport
    )
    print(f"Found {len(all_pdfs)} PDF(s) under {PDF_ROOT}/**/{sport}/")

    to_parse = []
    for pdf in all_pdfs:
        key = source_key(pdf)
        if "countinggymnasts" in pdf.name.lower():
            # Skip only if a non-countinggymnasts version exists (duplicate)
            base = re.sub(r"\.countinggymnasts", "", pdf.name, flags=re.IGNORECASE)
            base_s = re.sub(r"\.pdf$", "s.pdf", base, flags=re.IGNORECASE)  # handle "Team"→"Teams"
            if (pdf.parent / base).exists() or (pdf.parent / base_s).exists():
                continue
        if key in already_ingested or pdf.name in already_ingested:
            print(f"  [SKIP] {key}")
        else:
            print(f"  [NEW]  {key}")
            to_parse.append(pdf)

    if not to_parse:
        print("\nNothing new to ingest.")
        _finalize(con, dbconfig_file, sport)
        return

    print(f"\nParsing {len(to_parse)} new PDF(s)...")
    new_entries = []
    newly_processed = []

    url_map = load_url_map()

    for pdf in to_parse:
        key   = source_key(pdf)
        cname = comp_name(pdf)
        rel   = pdf.relative_to(PDF_ROOT)
        if any("_ignore" in p.lower() for p in rel.parts) or \
           "special olympics" in pdf.name.lower() or \
           "developing international" in pdf.name.lower():
            print(f"  [IGNORE] {key}")
            db.add_processed_file(con, key, cname)
            con.commit()
            continue
        try:
            events, method = parse_pdf(pdf, sport=sport)
            total = sum(len(e.get("results", [])) for e in events)
            print(f"  {key}: {len(events)} event(s), {total} athletes [{method}]")
            newly_processed.append((key, cname))
            if events:
                new_entries.append({
                    "competition": cname,
                    "season": comp_year(pdf),
                    "source_file": key,
                    "events": events,
                })
        except Exception as exc:
            print(f"  [ERR]  {pdf.name}: {exc}")

    print(f"\nUpdating data/stick_{sport}.db...")
    aliases   = load_club_aliases()
    overrides = load_overrides()
    new_comps = group_into_competitions(new_entries, sport=sport)
    normalise_clubs(new_comps, aliases, overrides)
    merge_to_db(con, new_comps)

    # Record processed files
    for key, cname in newly_processed:
        db.add_processed_file(con, key, cname)

    # Build pdf_manifest entries — source_url is the repo-relative path for direct hosting
    by_comp: dict[str, list] = {}
    for pdf in all_pdfs:
        cname = comp_name(pdf)
        key   = source_key(pdf)
        src_url = str(pdf).replace("\\", "/")
        by_comp.setdefault(cname, []).append({"file_path": key, "source_url": src_url})

    for cname, files in by_comp.items():
        db.upsert_pdf_manifest(con, cname, files)

    _finalize(con, dbconfig_file, sport)

    total_athletes = con.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    total_comps    = con.execute("SELECT COUNT(*) FROM competitions").fetchone()[0]
    print(f"\nDone — {total_comps} competition(s), {total_athletes} total athletes.")
    print("Refresh http://localhost:8080 to see updates.")


def _cmd_resolve_urls(con) -> None:
    """Backfill source_url for all pdf_manifest rows using local repo-relative paths."""
    # Build index of all local PDFs: (comp_name, source_key) -> relative path
    pdf_index = {}
    for pdf in sorted(PDF_ROOT.rglob("*.pdf")):
        cname = comp_name(pdf)
        key   = source_key(pdf)
        pdf_index[(cname, key)] = str(pdf).replace("\\", "/")

    rows = con.execute(
        "SELECT competition_name, file_path, source_url FROM pdf_manifest"
    ).fetchall()

    updated = 0
    for row in rows:
        cname = row["competition_name"]
        fpath = row["file_path"]
        current = row["source_url"]
        local_url = pdf_index.get((cname, fpath))
        if local_url and local_url != current:
            db.update_manifest_url(con, cname, fpath, local_url)
            updated += 1
            print(f"  [OK]   {cname}/{fpath}")
        elif not local_url:
            print(f"  [MISS] {cname}/{fpath} — PDF not found locally")

    con.commit()
    print(f"\nDone — {updated} URL(s) updated.")


def _finalize(con, dbconfig_file: Path, sport: str = "WAG") -> None:
    db.sync_clubs(con, CLUBS_FILE)
    db.vacuum(con)
    file_size = db.DB_PATH.stat().st_size
    with open(dbconfig_file, "w", encoding="utf-8") as f:
        json.dump({
            "serverMode": "full",
            "requestChunkSize": 1024,
            "fileLength": file_size,
        }, f)
    print(f"  dbconfig_{sport}.json updated (fileSize: {file_size:,} bytes)")


if __name__ == "__main__":
    main()
