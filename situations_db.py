#!/usr/bin/env python3.13
"""
SQLite schema + connection helpers for the situations index.

One row per (player, sampled tick). `source` partitions pro-demo data from
user-demo data so the matcher can filter one against the other.
"""

import sqlite3
from pathlib import Path

import polars as pl

DB_PATH = Path("situations.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS situations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    source            TEXT    NOT NULL,
    demo_id           TEXT    NOT NULL,
    round_num         INTEGER NOT NULL,
    tick              INTEGER NOT NULL,
    source_event      TEXT    NOT NULL,
    player_steamid    INTEGER NOT NULL,
    player_name       TEXT,
    player_side       TEXT    NOT NULL,
    player_place      TEXT,
    player_x          REAL,
    player_y          REAL,
    player_z          REAL,
    balance           INTEGER,
    active_weapon     INTEGER,
    economy_bucket    TEXT,
    alive_ct          INTEGER,
    alive_t           INTEGER,
    phase             TEXT,
    time_remaining_s  REAL,
    smokes_active     INTEGER,
    mollies_active    INTEGER,
    clip_start_tick   INTEGER,
    clip_end_tick     INTEGER
);
"""

# Matching query hits (source, place, side, alive_ct, alive_t, phase, economy);
# demo_id index supports resume-safety lookups during batch ingest.
INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_match "
    "ON situations(source, player_place, player_side, alive_ct, alive_t, phase, economy_bucket);",
    "CREATE INDEX IF NOT EXISTS idx_demo ON situations(demo_id);",
]

INSERT_COLS = [
    "source", "demo_id", "round_num", "tick", "source_event",
    "player_steamid", "player_name", "player_side", "player_place",
    "player_x", "player_y", "player_z", "balance", "active_weapon",
    "economy_bucket", "alive_ct", "alive_t", "phase", "time_remaining_s",
    "smokes_active", "mollies_active", "clip_start_tick", "clip_end_tick",
]


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    for idx in INDEXES:
        conn.execute(idx)
    conn.commit()


def processed_demos(conn: sqlite3.Connection, source: str = "pro") -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT demo_id FROM situations WHERE source = ?", (source,)
    ).fetchall()
    return {r[0] for r in rows}


def insert_dataframe(conn: sqlite3.Connection, df: pl.DataFrame) -> None:
    rows = df.select(INSERT_COLS).rows()
    placeholders = ",".join(["?"] * len(INSERT_COLS))
    conn.executemany(
        f"INSERT INTO situations ({','.join(INSERT_COLS)}) VALUES ({placeholders})",
        rows,
    )
    conn.commit()
