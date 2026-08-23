"""
redact_athlete.py — flag/unflag an athlete so their name renders masked on the
live site (first/last initial + block characters for the rest) and drops out
of the /find search suggestions.

This does NOT delete or alter the underlying name in the database - it only
sets athletes.redacted, which the front end (index.html) checks after loading
each sport's .db and masks accordingly. Fully reversible with --unflag.

Note on --club: athlete profiles/search are keyed by name only, not name+club, so
this can't do a true "mask only this club's rows" split - a shared name is one
identity site-wide. --club instead acts as a safety gate: the flag is only
applied if that name's results actually match the club given, and refused (not
silently applied to everyone) if the same name also appears under a different
club, so you don't accidentally redact an unrelated same-named kid elsewhere.

Usage:
    python redact_athlete.py "Emma Smith"                     # flag in both WAG + MAG
    python redact_athlete.py "Emma Smith" --sport WAG          # flag in one sport only
    python redact_athlete.py "Emma Smith" --club STA           # only if STA is their only club
    python redact_athlete.py "Emma Smith" --club STA --force   # flag anyway despite other clubs
    python redact_athlete.py "Emma Smith" --unflag             # reverse it
"""

import argparse
import json
import sqlite3
from pathlib import Path

import db

REDACTED_JSON = Path("data/redacted.json")


def load_redacted_json() -> dict:
    if REDACTED_JSON.exists():
        with open(REDACTED_JSON, encoding="utf-8") as f:
            return json.load(f)
    return {"WAG": [], "MAG": []}


def save_redacted_json(data: dict) -> None:
    data["WAG"] = sorted(set(data.get("WAG", [])))
    data["MAG"] = sorted(set(data.get("MAG", [])))
    with open(REDACTED_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def apply_flag(sport: str, name: str, flag: bool, club: str = None, force: bool = False) -> bool:
    """Returns True if an athlete row was found and updated in this sport's db."""
    db_path = Path(f"data/stick_{sport}.db")
    if not db_path.exists():
        return False

    con = db.get_conn(db_path)
    db.create_schema(con)  # idempotent - ensures the redacted column exists

    row = con.execute("SELECT id FROM athletes WHERE name = ?", (name,)).fetchone()
    if not row:
        con.close()
        return False

    clubs = [r["club"] for r in con.execute(
        "SELECT DISTINCT club FROM results WHERE athlete = ? AND club IS NOT NULL", (name,)
    ).fetchall()]

    if club:
        if club not in clubs:
            print(f"  [{sport}] Skipped '{name}': no results found under club '{club}'"
                  + (f" (found: {', '.join(clubs)})" if clubs else " (no results at all)") + ".")
            con.close()
            return False
        if len(clubs) > 1 and not force:
            print(f"  [{sport}] Refused '{name}': also has results under {', '.join(c for c in clubs if c != club)} "
                  f"besides '{club}'. Flagging would mask ALL of them (name is one identity site-wide) - "
                  f"pass --force if that's really the same athlete, otherwise resolve the ambiguity manually.")
            con.close()
            return False

    con.execute("UPDATE athletes SET redacted = ? WHERE id = ?", (1 if flag else 0, row["id"]))
    con.commit()
    con.close()

    action = "Flagged" if flag else "Unflagged"
    print(f"  [{sport}] {action} '{name}'.")
    if clubs:
        print(f"           Results found under club(s): {', '.join(clubs)}"
              + ("  <-- more than one club: double-check this is one athlete, not a name collision" if len(clubs) > 1 else ""))
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("name", help="Exact athlete name as stored in the DB (case-sensitive)")
    parser.add_argument("--sport", choices=["WAG", "MAG"], default=None,
                         help="Limit to one sport's DB. Default: check both.")
    parser.add_argument("--club", default=None,
                         help="Only flag if this name's results are under this club "
                              "(refuses if the name also appears under another club, unless --force).")
    parser.add_argument("--force", action="store_true",
                         help="With --club, flag anyway even if the name also appears under other clubs.")
    parser.add_argument("--unflag", action="store_true", help="Reverse a previous redaction")
    args = parser.parse_args()

    sports = [args.sport] if args.sport else ["WAG", "MAG"]
    flag = not args.unflag

    redacted_json = load_redacted_json()
    found_any = False
    for sport in sports:
        if apply_flag(sport, args.name, flag, club=args.club, force=args.force):
            found_any = True
            names = redacted_json.setdefault(sport, [])
            if flag:
                if args.name not in names:
                    names.append(args.name)
            else:
                if args.name in names:
                    names.remove(args.name)

    if not found_any:
        print(f"No athlete named '{args.name}' found in {' or '.join(sports)}. No changes made.")
        return

    save_redacted_json(redacted_json)
    print(f"Updated {REDACTED_JSON} - commit and push data/stick_*.db + data/redacted.json to publish.")


if __name__ == "__main__":
    main()
