"""
Users table — per-user Match Auth Code and share code cursor for auto-sync.
"""

import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "situations.db"

USERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    steam_id           TEXT PRIMARY KEY,
    match_auth_code    TEXT,
    last_share_code    TEXT,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL
);
"""


def init_users_table(conn: sqlite3.Connection) -> None:
    conn.execute(USERS_SCHEMA)
    conn.commit()


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
    row = conn.execute(
        "SELECT * FROM users WHERE steam_id = ?", (steam_id,)
    ).fetchone()
    return dict(row) if row else None


def get_all_users(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM users WHERE match_auth_code IS NOT NULL"
    ).fetchall()
    return [dict(r) for r in rows]
