"""PostgreSQL connection pool and typed async query helpers."""
from __future__ import annotations

import asyncpg

from backend import config

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=2,
            max_size=10,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


# ── Maps ───────────────────────────────────────────────────────────────────────

async def get_maps() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT map_name, display_name, pos_x, pos_y, map_scale, "
        "       has_lower_level, lower_level_max_z "
        "FROM maps ORDER BY display_name"
    )
    return [dict(r) for r in rows]


# ── Users ──────────────────────────────────────────────────────────────────────

async def get_user(steam_id: str) -> dict | None:
    pool = await get_pool()
    row  = await pool.fetchrow(
        "SELECT * FROM users WHERE steam_id = $1", steam_id
    )
    return dict(row) if row else None


async def upsert_user(
    steam_id: str,
    match_auth_code: str | None = None,
    last_share_code: str | None = None,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO users (steam_id, match_auth_code, last_share_code, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (steam_id) DO UPDATE
            SET match_auth_code = COALESCE($2, users.match_auth_code),
                last_share_code = COALESCE($3, users.last_share_code),
                updated_at      = NOW()
        """,
        steam_id, match_auth_code, last_share_code,
    )


async def update_last_share_code(steam_id: str, code: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE users SET last_share_code = $2, updated_at = NOW() WHERE steam_id = $1",
        steam_id, code,
    )


async def get_all_users() -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT * FROM users ORDER BY created_at")
    return [dict(r) for r in rows]


# ── User matches ───────────────────────────────────────────────────────────────

async def upsert_user_match(demo_id: str, **kwargs) -> None:
    """Insert or replace a user match record. kwargs must match column names."""
    pool = await get_pool()
    cols   = ['demo_id'] + list(kwargs.keys())
    params = [demo_id]   + list(kwargs.values())
    placeholders = ', '.join(f'${i+1}' for i in range(len(params)))
    col_list     = ', '.join(cols)
    updates      = ', '.join(
        f"{c} = EXCLUDED.{c}" for c in kwargs if c != 'processed_at'
    )
    await pool.execute(
        f"""
        INSERT INTO user_matches ({col_list}) VALUES ({placeholders})
        ON CONFLICT (demo_id) DO UPDATE SET {updates}
        """,
        *params,
    )


async def get_user_matches(steam_id: str, limit: int = 30) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT demo_id, map_name, match_date, score_ct, score_t,
               user_side_first, user_result, kills, deaths, assists,
               hs_pct, round_count
        FROM user_matches
        WHERE steam_id = $1
        ORDER BY match_date DESC NULLS LAST, processed_at DESC
        LIMIT $2
        """,
        steam_id, limit,
    )
    return [dict(r) for r in rows]


async def get_match_parquet_dir(demo_id: str) -> tuple[str, str] | None:
    """Return (parquet_dir, map_name) for either a user or pro match."""
    pool = await get_pool()
    row  = await pool.fetchrow(
        "SELECT parquet_dir, map_name FROM user_matches WHERE demo_id = $1",
        demo_id,
    )
    if row and row['parquet_dir']:
        return row['parquet_dir'], row['map_name']
    row = await pool.fetchrow(
        "SELECT parquet_dir, map_name FROM pro_matches WHERE match_id = $1",
        demo_id,
    )
    if row and row['parquet_dir']:
        return row['parquet_dir'], row['map_name']
    return None


async def get_match_source_record(match_id: str) -> dict | None:
    """Return a normalized match record for either a user or pro match."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT *, demo_id AS source_match_id, 'user' AS source_type "
        "FROM user_matches WHERE demo_id = $1",
        match_id,
    )
    if row:
        return dict(row)

    row = await pool.fetchrow(
        "SELECT *, match_id AS source_match_id, 'pro' AS source_type "
        "FROM pro_matches WHERE match_id = $1",
        match_id,
    )
    return dict(row) if row else None


# ── Pro matches ────────────────────────────────────────────────────────────────

async def upsert_pro_match(match_id: str, **kwargs) -> None:
    """Insert or replace a pro match record. kwargs must match column names."""
    pool = await get_pool()
    cols   = ['match_id'] + list(kwargs.keys())
    params = [match_id]   + list(kwargs.values())
    placeholders = ', '.join(f'${i+1}' for i in range(len(params)))
    col_list     = ', '.join(cols)
    updates      = ', '.join(
        f"{c} = EXCLUDED.{c}" for c in kwargs if c != 'ingested_at'
    )
    await pool.execute(
        f"""
        INSERT INTO pro_matches ({col_list}) VALUES ({placeholders})
        ON CONFLICT (match_id) DO UPDATE SET {updates}
        """,
        *params,
    )


async def get_ingested_pro_match_ids() -> set[str]:
    """Return all match_ids already in pro_matches (for idempotency)."""
    pool = await get_pool()
    rows = await pool.fetch("SELECT match_id FROM pro_matches")
    return {r['match_id'] for r in rows}


async def get_pro_matches(limit: int | None = None) -> list[dict]:
    pool = await get_pool()
    query = (
        "SELECT match_id, map_name, parquet_dir, match_date, event_name, "
        "       hltv_url, team_ct, team_t, ingested_at "
        "FROM pro_matches "
        "ORDER BY ingested_at DESC"
    )
    params: tuple = ()
    if limit is not None:
        query += " LIMIT $1"
        params = (limit,)
    rows = await pool.fetch(query, *params)
    return [dict(r) for r in rows]


# ── Event windows ──────────────────────────────────────────────────────────────

async def upsert_event_window(window_id: str, **kwargs) -> None:
    """Insert or replace an event-window record. kwargs must match column names."""
    pool = await get_pool()
    cols = ['window_id'] + list(kwargs.keys())
    params = [window_id] + list(kwargs.values())
    placeholders = ', '.join(f'${i+1}' for i in range(len(params)))
    col_list = ', '.join(cols)
    updates = ', '.join(
        f"{c} = EXCLUDED.{c}" for c in kwargs if c != 'created_at'
    )
    await pool.execute(
        f"""
        INSERT INTO event_windows ({col_list}) VALUES ({placeholders})
        ON CONFLICT (window_id) DO UPDATE SET {updates}
        """,
        *params,
    )


async def get_event_window(window_id: str) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM event_windows WHERE window_id = $1",
        window_id,
    )
    return dict(row) if row else None


async def list_event_window_candidates(
    *,
    source_type: str = "pro",
    map_name: str | None = None,
    phase: str | None = None,
    side_to_query: str | None = None,
    feature_version: str | None = None,
    limit: int = 250,
) -> list[dict]:
    pool = await get_pool()

    filters = ["source_type = $1"]
    params: list = [source_type]

    if map_name is not None:
        params.append(map_name)
        filters.append(f"map_name = ${len(params)}")
    if phase is not None:
        params.append(phase)
        filters.append(f"phase = ${len(params)}")
    if side_to_query is not None:
        params.append(side_to_query)
        filters.append(f"(side_to_query = ${len(params)} OR side_to_query IS NULL)")
    if feature_version is not None:
        params.append(feature_version)
        filters.append(f"feature_version = ${len(params)}")

    params.append(limit)
    query = (
        "SELECT * FROM event_windows "
        f"WHERE {' AND '.join(filters)} "
        "ORDER BY created_at DESC "
        f"LIMIT ${len(params)}"
    )
    rows = await pool.fetch(query, *params)
    return [dict(r) for r in rows]


# ── Job runs ───────────────────────────────────────────────────────────────────

async def start_job_run(job_name: str) -> int:
    pool = await get_pool()
    row  = await pool.fetchrow(
        "INSERT INTO job_runs (job_name, started_at, status) "
        "VALUES ($1, NOW(), 'running') RETURNING id",
        job_name,
    )
    return row['id']


async def finish_job_run(
    run_id: int,
    status: str,
    items_processed: int | None = None,
    error_message: str | None = None,
    stats: dict | None = None,
) -> None:
    import json
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE job_runs
        SET finished_at = NOW(), status = $2,
            items_processed = $3, error_message = $4, stats_json = $5
        WHERE id = $1
        """,
        run_id, status, items_processed, error_message,
        json.dumps(stats) if stats else None,
    )
