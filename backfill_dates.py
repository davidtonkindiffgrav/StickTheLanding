"""
backfill_dates.py — One-off script to populate the 'date' column on existing competitions.

For each competition with no date:
  1. Searches through its associated PDFs (from pdf_manifest) for a date string.
  2. Falls back to data/calendar.json name-match.
  3. Reports anything still unresolved so it can be entered manually.

Run once, then delete this file.
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

from date_extractor import resolve_comp_date

DATABASES = [
    ("data/stick_WAG.db", "WAG"),
    ("data/stick_MAG.db", "MAG"),
]

PDF_ROOT = Path("pdfs")


def backfill(db_path: str, sport: str) -> Tuple[int, List[str]]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Add date column if the DB pre-dates the schema change
    try:
        con.execute("ALTER TABLE competitions ADD COLUMN date TEXT")
        con.commit()
    except Exception:
        pass

    comps = con.execute(
        "SELECT id, name, season FROM competitions WHERE date IS NULL"
    ).fetchall()

    if not comps:
        print(f"  {sport}: all competitions already have dates.")
        con.close()
        return 0, []

    # Build a lookup: (comp_name, season_year) -> list of PDF paths on disk.
    # The season year is extracted from the path (pdfs/<year>/...) to avoid
    # mixing PDFs from different seasons that share the same competition name.
    manifest_rows = con.execute(
        "SELECT competition_name, source_url FROM pdf_manifest"
    ).fetchall()
    comp_to_pdfs: Dict[tuple, List[Path]] = {}
    for row in manifest_rows:
        cname = row["competition_name"]
        src = row["source_url"]
        if not src:
            continue
        p = Path(src)
        if not p.exists():
            continue
        parts = p.parts
        # Path is pdfs/<year>/<sport>/... — year is at index 1
        year = parts[1] if len(parts) > 1 else None
        comp_to_pdfs.setdefault((cname, year), []).append(p)

    updated = 0
    missing = []

    for comp in comps:
        comp_id = comp["id"]
        comp_name = comp["name"]
        season = comp["season"]
        pdfs = comp_to_pdfs.get((comp_name, season), [])
        date_val = resolve_comp_date(comp_name, pdfs)
        if date_val:
            con.execute(
                "UPDATE competitions SET date = ? WHERE id = ? AND date IS NULL",
                (date_val, comp_id),
            )
            print(f"  {sport}: {comp_name!r} = {date_val}")
            updated += 1
        else:
            missing.append(comp_name)

    con.commit()
    con.close()
    return updated, missing


def main():
    total_updated = 0
    all_missing: List[str] = []

    for db_path, sport in DATABASES:
        print(f"\n=== {sport} ({db_path}) ===")
        updated, missing = backfill(db_path, sport)
        total_updated += updated
        all_missing.extend(f"{name} ({sport})" for name in missing)

    print(f"\n{'='*50}")
    print(f"Updated {total_updated} competition(s) with dates.")

    if all_missing:
        print("\nCould not find dates for the following competitions — please enter manually:")
        for name in all_missing:
            print(f"  - {name}")
        print(
            "\nTo set a date manually, run:\n"
            "  python -c \"import sqlite3; conn=sqlite3.connect('data/stick_WAG.db'); "
            "conn.execute(\\\"UPDATE competitions SET date='YYYY-MM-DD' WHERE name='Comp Name'\\\"); "
            "conn.commit()\""
        )
    else:
        print("All competitions have dates.")


if __name__ == "__main__":
    main()
