"""
db.py — SQLite database layer for StickTheLanding.

Schema is created on first access via create_schema(). All write operations
use INSERT OR REPLACE / INSERT OR IGNORE for idempotency.
"""

import datetime
import json
import sqlite3
from pathlib import Path

DB_PATH = Path("data/stick_WAG.db")


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = DELETE")
    con.execute("PRAGMA foreign_keys = ON")
    return con


def create_schema(con: sqlite3.Connection) -> None:
    con.executescript("""
        PRAGMA page_size = 1024;

        CREATE TABLE IF NOT EXISTS clubs (
            code         TEXT PRIMARY KEY,
            name         TEXT NOT NULL,
            region       TEXT,
            logo         TEXT,
            founded      TEXT,
            description  TEXT,
            links        TEXT
        );

        CREATE TABLE IF NOT EXISTS club_venues (
            club_code   TEXT NOT NULL REFERENCES clubs(code),
            name        TEXT,
            address     TEXT
        );

        CREATE TABLE IF NOT EXISTS competitions (
            id      TEXT PRIMARY KEY,
            name    TEXT NOT NULL,
            season  TEXT NOT NULL,
            sport   TEXT NOT NULL,
            date    TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            competition_id  TEXT NOT NULL REFERENCES competitions(id),
            level           INTEGER,
            division        INTEGER,
            age_group       TEXT,
            event_type      TEXT,
            source_file     TEXT
        );

        CREATE TABLE IF NOT EXISTS results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id    INTEGER NOT NULL REFERENCES events(id),
            rank        INTEGER,
            athlete     TEXT,
            club        TEXT,
            vault       REAL,
            vault_d     REAL,
            vault_e     REAL,
            bars        REAL,
            bars_d      REAL,
            bars_e      REAL,
            beam        REAL,
            beam_d      REAL,
            beam_e      REAL,
            floor       REAL,
            floor_d     REAL,
            floor_e     REAL,
            total       REAL
        );

        CREATE TABLE IF NOT EXISTS processed_files (
            source_key          TEXT PRIMARY KEY,
            competition_name    TEXT,
            processed_at        TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS pdf_manifest (
            competition_name    TEXT NOT NULL,
            file_path           TEXT NOT NULL,
            source_url          TEXT,
            PRIMARY KEY (competition_name, file_path)
        );

        CREATE TABLE IF NOT EXISTS athletes (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT NOT NULL UNIQUE,
            nickname  TEXT,
            image     TEXT
        );

        CREATE TABLE IF NOT EXISTS club_teams (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            club        TEXT NOT NULL REFERENCES clubs(code),
            label       TEXT,
            notes       TEXT,
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_results_event      ON results(event_id);
        CREATE INDEX IF NOT EXISTS idx_events_comp       ON events(competition_id);
        CREATE INDEX IF NOT EXISTS idx_events_level      ON events(level, division, event_type);
        CREATE INDEX IF NOT EXISTS idx_results_club      ON results(club);
        CREATE INDEX IF NOT EXISTS idx_results_athlete   ON results(athlete);
        CREATE INDEX IF NOT EXISTS idx_comp_sport_season ON competitions(sport, season);
        CREATE INDEX IF NOT EXISTS idx_athletes_name     ON athletes(name);
    """)
    con.commit()
    # Idempotent migrations
    for col in ("vault_d", "vault_e", "bars_d", "bars_e", "beam_d", "beam_e", "floor_d", "floor_e",
                "pommel", "pommel_d", "pommel_e", "rings", "rings_d", "rings_e",
                "pbars", "pbars_d", "pbars_e", "hbar", "hbar_d", "hbar_e"):
        try:
            con.execute(f"ALTER TABLE results ADD COLUMN {col} REAL")
        except Exception:
            pass
    for col in ("age_group TEXT",):
        try:
            con.execute(f"ALTER TABLE events ADD COLUMN {col}")
        except Exception:
            pass
    for col in ("team_name TEXT",):
        try:
            con.execute(f"ALTER TABLE results ADD COLUMN {col}")
        except Exception:
            pass
    for col in ("team_id INTEGER REFERENCES club_teams(id)",):
        try:
            con.execute(f"ALTER TABLE results ADD COLUMN {col}")
        except Exception:
            pass
    # club_teams.label was originally NOT NULL with a UNIQUE(club, label) constraint.
    # Both are relaxed: a team may be tagged before its display name is known, and a
    # promoted team can end up sharing a colour/label with another team already
    # sitting in its new division (two physical teams, one ambiguous-looking label -
    # the team_id split still keeps their stats correctly separated either way).
    # Rebuild only if the old constraints are still present.
    label_col = next((c for c in con.execute("PRAGMA table_info(club_teams)") if c["name"] == "label"), None)
    has_unique_idx = any(
        idx["unique"] for idx in con.execute("PRAGMA index_list(club_teams)")
    ) if label_col is not None else False
    if label_col is not None and (label_col["notnull"] or has_unique_idx):
        con.commit()
        con.execute("PRAGMA foreign_keys = OFF")
        con.executescript("""
            CREATE TABLE club_teams_new (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                club        TEXT NOT NULL REFERENCES clubs(code),
                label       TEXT,
                notes       TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );
            INSERT INTO club_teams_new SELECT * FROM club_teams;
            DROP TABLE club_teams;
            ALTER TABLE club_teams_new RENAME TO club_teams;
        """)
        con.commit()
        con.execute("PRAGMA foreign_keys = ON")
    for col in ("age_bracket TEXT", "age_bracket_rank INTEGER"):
        try:
            con.execute(f"ALTER TABLE results ADD COLUMN {col}")
        except Exception:
            pass
    try:
        con.execute("ALTER TABLE competitions ADD COLUMN date TEXT")
    except Exception:
        pass
    for col in ("founded TEXT", "description TEXT", "links TEXT"):
        try:
            con.execute(f"ALTER TABLE clubs ADD COLUMN {col}")
        except Exception:
            pass
    con.commit()


def sync_athletes(con: sqlite3.Connection) -> None:
    """Upsert all distinct athlete names from results into the athletes table.
    Uses INSERT OR IGNORE so manually set nickname/image values are never overwritten."""
    con.execute("""
        INSERT OR IGNORE INTO athletes(name)
        SELECT DISTINCT athlete FROM results
        WHERE athlete IS NOT NULL AND athlete != ''
    """)
    con.commit()


def sync_clubs(con: sqlite3.Connection, clubs_path: Path) -> None:
    if not clubs_path.exists():
        return
    with open(clubs_path, encoding="utf-8") as f:
        data = json.load(f)
    clubs = data.get("clubs", [])
    rows = [
        (
            c["code"], c["name"], c.get("region"), c.get("logo"),
            c.get("founded"), c.get("description"),
            json.dumps(c["links"]) if c.get("links") else None,
        )
        for c in clubs
    ]
    con.executemany(
        "INSERT OR REPLACE INTO clubs (code, name, region, logo, founded, description, links) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

    # club_venues is fully rebuilt from clubs.json each sync (it's the source of truth)
    con.execute("DELETE FROM club_venues")
    venue_rows = [
        (c["code"], v.get("name"), v.get("address"))
        for c in clubs
        for v in c.get("venues", [])
        if v.get("address")
    ]
    con.executemany(
        "INSERT INTO club_venues (club_code, name, address) VALUES (?, ?, ?)",
        venue_rows,
    )
    con.commit()


def get_processed_files(con: sqlite3.Connection) -> set:
    rows = con.execute("SELECT source_key FROM processed_files").fetchall()
    return {r["source_key"] for r in rows}


def add_processed_file(con: sqlite3.Connection, key: str, comp_name: str) -> None:
    con.execute(
        "INSERT OR IGNORE INTO processed_files (source_key, competition_name) VALUES (?, ?)",
        (key, comp_name),
    )


def upsert_competition(con: sqlite3.Connection, comp: dict) -> None:
    con.execute(
        "INSERT OR IGNORE INTO competitions (id, name, season, sport, date) VALUES (?, ?, ?, ?, ?)",
        (comp["id"], comp["name"], comp.get("season", str(datetime.date.today().year)), comp.get("sport", "WAG"), comp.get("date")),
    )


def set_competition_date(con: sqlite3.Connection, comp_id: str, date_str: str) -> None:
    """Set date on a competition only if it has no date yet (never overwrites a manual entry)."""
    con.execute(
        "UPDATE competitions SET date = ? WHERE id = ? AND date IS NULL",
        (date_str, comp_id),
    )


def insert_event(con: sqlite3.Connection, competition_id: str, ev: dict) -> int:
    cur = con.execute(
        "INSERT INTO events (competition_id, level, division, age_group, event_type, source_file) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            competition_id,
            ev.get("level"),
            ev.get("division"),
            ev.get("age_group"),
            ev.get("event_type"),
            ev.get("source_file"),
        ),
    )
    return cur.lastrowid


def insert_result(con: sqlite3.Connection, event_id: int, r: dict) -> None:
    con.execute(
        "INSERT INTO results "
        "(event_id, rank, athlete, club, team_name, vault, vault_d, vault_e, bars, bars_d, bars_e, "
        "beam, beam_d, beam_e, floor, floor_d, floor_e, total, "
        "pommel, pommel_d, pommel_e, rings, rings_d, rings_e, "
        "pbars, pbars_d, pbars_e, hbar, hbar_d, hbar_e) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id,
            r.get("rank"),
            r.get("athlete"),
            r.get("club"),
            r.get("team_name"),
            r.get("vault"),  r.get("vault_d"),  r.get("vault_e"),
            r.get("bars"),   r.get("bars_d"),   r.get("bars_e"),
            r.get("beam"),   r.get("beam_d"),   r.get("beam_e"),
            r.get("floor"),  r.get("floor_d"),  r.get("floor_e"),
            r.get("total"),
            r.get("pommel"), r.get("pommel_d"), r.get("pommel_e"),
            r.get("rings"),  r.get("rings_d"),  r.get("rings_e"),
            r.get("pbars"),  r.get("pbars_d"),  r.get("pbars_e"),
            r.get("hbar"),   r.get("hbar_d"),   r.get("hbar_e"),
        ),
    )


def get_or_create_club_team(con: sqlite3.Connection, club: str, label: str, notes: str = None) -> int:
    """Return the id of an existing club_teams row for (club, label), or create one.

    (club, label) is not a database-enforced unique key - two different physical teams
    can legitimately end up sharing a label (e.g. a promoted team landing in a division
    that already has a team with the same colour). This only reuses the first existing
    match it finds; call create_club_team() directly when you specifically want a new
    row even though a matching label already exists.
    """
    row = con.execute(
        "SELECT id FROM club_teams WHERE club = ? AND label IS ?", (club, label)
    ).fetchone()
    if row is not None:
        return row["id"]
    return create_club_team(con, club, label, notes)


def create_club_team(con: sqlite3.Connection, club: str, label: str = None, notes: str = None) -> int:
    """Always insert a new club_teams row and return its id (no reuse-by-label check)."""
    cur = con.execute(
        "INSERT INTO club_teams (club, label, notes) VALUES (?, ?, ?)",
        (club, label, notes),
    )
    return cur.lastrowid


def set_result_team_id(con: sqlite3.Connection, result_id: int, team_id: int) -> None:
    con.execute("UPDATE results SET team_id = ? WHERE id = ?", (team_id, result_id))


def upsert_pdf_manifest(con: sqlite3.Connection, comp_name: str, files: list) -> None:
    """files: list of dicts with 'file_path' and optional 'source_url'."""
    # Insert new rows; ignore conflicts to avoid overwriting already-resolved URLs
    con.executemany(
        "INSERT OR IGNORE INTO pdf_manifest (competition_name, file_path, source_url) "
        "VALUES (?, ?, ?)",
        [(comp_name, f["file_path"], f.get("source_url")) for f in files],
    )
    # Update source_url only when we have a resolved URL (never write NULL over a real URL)
    with_url = [
        (f["source_url"], comp_name, f["file_path"])
        for f in files if f.get("source_url")
    ]
    if with_url:
        con.executemany(
            "UPDATE pdf_manifest SET source_url = ? WHERE competition_name = ? AND file_path = ?",
            with_url,
        )


def update_manifest_url(con: sqlite3.Connection, comp_name: str, file_path: str, url: str) -> None:
    con.execute(
        "UPDATE pdf_manifest SET source_url = ? WHERE competition_name = ? AND file_path = ?",
        (url, comp_name, file_path),
    )


def vacuum(con: sqlite3.Connection) -> None:
    con.commit()
    con.execute("VACUUM")

