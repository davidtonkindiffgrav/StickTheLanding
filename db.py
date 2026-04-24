"""
db.py — SQLite database layer for StickTheLanding.

Schema is created on first access via create_schema(). All write operations
use INSERT OR REPLACE / INSERT OR IGNORE for idempotency.
"""

import json
import sqlite3
from pathlib import Path

DB_PATH = Path("data/stick.db")


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
            code    TEXT PRIMARY KEY,
            name    TEXT NOT NULL,
            region  TEXT,
            logo    TEXT
        );

        CREATE TABLE IF NOT EXISTS competitions (
            id      TEXT PRIMARY KEY,
            name    TEXT NOT NULL,
            season  TEXT NOT NULL,
            sport   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            competition_id  TEXT NOT NULL REFERENCES competitions(id),
            level           INTEGER,
            division        INTEGER,
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
            bars        REAL,
            beam        REAL,
            floor       REAL,
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

        CREATE INDEX IF NOT EXISTS idx_results_event   ON results(event_id);
        CREATE INDEX IF NOT EXISTS idx_events_comp     ON events(competition_id);
        CREATE INDEX IF NOT EXISTS idx_events_level    ON events(level, division, event_type);
        CREATE INDEX IF NOT EXISTS idx_results_club    ON results(club);
        CREATE INDEX IF NOT EXISTS idx_results_athlete ON results(athlete);
    """)
    con.commit()


def sync_clubs(con: sqlite3.Connection, clubs_path: Path) -> None:
    if not clubs_path.exists():
        return
    with open(clubs_path, encoding="utf-8") as f:
        data = json.load(f)
    rows = [
        (c["code"], c["name"], c.get("region"), c.get("logo"))
        for c in data.get("clubs", [])
    ]
    con.executemany(
        "INSERT OR REPLACE INTO clubs (code, name, region, logo) VALUES (?, ?, ?, ?)",
        rows,
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
        "INSERT OR IGNORE INTO competitions (id, name, season, sport) VALUES (?, ?, ?, ?)",
        (comp["id"], comp["name"], comp.get("season", "2025"), comp.get("sport", "WAG")),
    )


def insert_event(con: sqlite3.Connection, competition_id: str, ev: dict) -> int:
    cur = con.execute(
        "INSERT INTO events (competition_id, level, division, event_type, source_file) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            competition_id,
            ev.get("level"),
            ev.get("division"),
            ev.get("event_type"),
            ev.get("source_file"),
        ),
    )
    return cur.lastrowid


def insert_result(con: sqlite3.Connection, event_id: int, r: dict) -> None:
    con.execute(
        "INSERT INTO results (event_id, rank, athlete, club, vault, bars, beam, floor, total) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            event_id,
            r.get("rank"),
            r.get("athlete"),
            r.get("club"),
            r.get("vault"),
            r.get("bars"),
            r.get("beam"),
            r.get("floor"),
            r.get("total"),
        ),
    )


def upsert_pdf_manifest(con: sqlite3.Connection, comp_name: str, files: list) -> None:
    """files: list of dicts with 'file_path' and optional 'source_url'."""
    con.executemany(
        "INSERT OR REPLACE INTO pdf_manifest (competition_name, file_path, source_url) "
        "VALUES (?, ?, ?)",
        [(comp_name, f["file_path"], f.get("source_url")) for f in files],
    )


def update_manifest_url(con: sqlite3.Connection, comp_name: str, file_path: str, url: str) -> None:
    con.execute(
        "UPDATE pdf_manifest SET source_url = ? WHERE competition_name = ? AND file_path = ?",
        (url, comp_name, file_path),
    )


def vacuum(con: sqlite3.Connection) -> None:
    con.commit()
    con.execute("VACUUM")
