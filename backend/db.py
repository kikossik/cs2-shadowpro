"""
Unified database layer — all tables, one connect(), one schema init.
Replaces the old pipeline/situations_db.py + backend/db_users.py split.
"""

import sqlite3
import time
from pathlib import Path

import polars as pl

from backend.config import DB_PATH

_SCHEMA = """
CREATE TABLE IF NOT EXISTS matches (
    demo_id           TEXT    PRIMARY KEY,
    source            TEXT    NOT NULL,
    steam_id          TEXT,
    map               TEXT,
    date_ts           INTEGER,
    round_count       INTEGER,
    score_ct          INTEGER,
    score_t           INTEGER,
    user_side_first   TEXT,
    user_result       TEXT,
    kills             INTEGER,
    deaths            INTEGER,
    assists           INTEGER,
    hs_pct            INTEGER,
    situations_count  INTEGER,
    processed_at      INTEGER NOT NULL
);

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

CREATE TABLE IF NOT EXISTS users (
    steam_id           TEXT PRIMARY KEY,
    match_auth_code    TEXT,
    last_share_code    TEXT,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);
"""

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_sit_match "
    "ON situations(source, player_place, player_side, alive_ct, alive_t, phase, economy_bucket);",
    "CREATE INDEX IF NOT EXISTS idx_sit_demo ON situations(demo_id);",
]

_INSERT_COLS = [
    "source", "demo_id", "round_num", "tick", "source_event",
    "player_steamid", "player_name", "player_side", "player_place",
    "player_x", "player_y", "player_z", "balance", "active_weapon",
    "economy_bucket", "alive_ct", "alive_t", "phase", "time_remaining_s",
    "smokes_active", "mollies_active", "clip_start_tick", "clip_end_tick",
]


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    for idx in _INDEXES:
        conn.execute(idx)
    conn.commit()


# ── situations ─────────────────────────────────────────────────────────────────

def insert_situations(conn: sqlite3.Connection, df: pl.DataFrame) -> None:
    rows = df.select(_INSERT_COLS).rows()
    placeholders = ",".join(["?"] * len(_INSERT_COLS))
    conn.executemany(
        f"INSERT INTO situations ({','.join(_INSERT_COLS)}) VALUES ({placeholders})",
        rows,
    )
    conn.commit()


def processed_demos(conn: sqlite3.Connection, source: str = "pro") -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT demo_id FROM situations WHERE source = ?", (source,)
    ).fetchall()
    return {r[0] for r in rows}


# ── matches ────────────────────────────────────────────────────────────────────

def upsert_match(conn: sqlite3.Connection, **fields) -> None:
    fields.setdefault("processed_at", int(time.time()))
    cols = list(fields.keys())
    conn.execute(
        f"INSERT OR REPLACE INTO matches ({','.join(cols)}) "
        f"VALUES ({','.join(['?'] * len(cols))})",
        list(fields.values()),
    )
    conn.commit()


# ── users ──────────────────────────────────────────────────────────────────────

def upsert_user(
    conn: sqlite3.Connection,
    steam_id: str,
    match_auth_code: str | None = None,
    last_share_code: str | None = None,
) -> None:
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO users (steam_id, match_auth_code, last_share_code, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(steam_id) DO UPDATE SET
            match_auth_code = COALESCE(excluded.match_auth_code, match_auth_code),
            last_share_code = COALESCE(excluded.last_share_code, last_share_code),
            updated_at      = excluded.updated_at
        """,
        (steam_id, match_auth_code, last_share_code, now, now),
    )
    conn.commit()


def get_user(conn: sqlite3.Connection, steam_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM users WHERE steam_id = ?", (steam_id,)).fetchone()
    return dict(row) if row else None


def get_all_users(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM users WHERE match_auth_code IS NOT NULL"
    ).fetchall()
    return [dict(r) for r in rows]
