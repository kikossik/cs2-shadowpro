"""PostgreSQL connection pool and typed async query helpers."""
from __future__ import annotations

import asyncpg
import json

from backend import config
from pipeline.features.vectorize import VECTOR_DIM

_pool: asyncpg.Pool | None = None


def _normalize_path_fields(row: dict | None, *fields: str) -> dict | None:
    if row is None:
        return None
    payload = dict(row)
    for field in fields:
        if field in payload and payload[field] is not None:
            payload[field] = config.resolve_managed_path(payload[field])
    return payload


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


async def delete_user_match(demo_id: str) -> None:
    pool = await get_pool()
    await pool.execute("DELETE FROM user_matches WHERE demo_id = $1", demo_id)


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
        return config.resolve_managed_path(row['parquet_dir']), row['map_name']
    row = await pool.fetchrow(
        "SELECT parquet_dir, map_name FROM pro_matches WHERE match_id = $1",
        demo_id,
    )
    if row and row['parquet_dir']:
        return config.resolve_managed_path(row['parquet_dir']), row['map_name']
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
        return _normalize_path_fields(dict(row), "parquet_dir", "artifact_path")

    row = await pool.fetchrow(
        "SELECT *, match_id AS source_match_id, 'pro' AS source_type "
        "FROM pro_matches WHERE match_id = $1",
        match_id,
    )
    return _normalize_path_fields(dict(row), "parquet_dir", "artifact_path") if row else None


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
        "SELECT match_id, map_name, parquet_dir, artifact_path, match_date, event_name, "
        "       hltv_url, team_ct, team_t, ingested_at "
        "FROM pro_matches "
        "ORDER BY ingested_at DESC"
    )
    params: tuple = ()
    if limit is not None:
        query += " LIMIT $1"
        params = (limit,)
    rows = await pool.fetch(query, *params)
    return [_normalize_path_fields(dict(r), "parquet_dir", "artifact_path") for r in rows]


# ── Artifact path helpers ──────────────────────────────────────────────────────

async def set_match_artifact_path(source_type: str, source_match_id: str, artifact_path: str) -> None:
    """Store the artifact_path for a pro or user match."""
    managed = config.to_managed_path(artifact_path)
    pool = await get_pool()
    if source_type == "pro":
        await pool.execute(
            "UPDATE pro_matches SET artifact_path = $1 WHERE match_id = $2",
            managed, source_match_id,
        )
    else:
        await pool.execute(
            "UPDATE user_matches SET artifact_path = $1 WHERE demo_id = $2",
            managed, source_match_id,
        )


async def count_event_windows(
    source_type: str,
    source_match_id: str,
    *,
    feature_version: str | None = None,
) -> int:
    """Return the number of indexed event windows for one match."""
    pool = await get_pool()
    if feature_version is None:
        return int(await pool.fetchval(
            "SELECT COUNT(*) FROM event_windows "
            "WHERE source_type = $1 AND source_match_id = $2",
            source_type,
            source_match_id,
        ) or 0)
    return int(await pool.fetchval(
        "SELECT COUNT(*) FROM event_windows "
        "WHERE source_type = $1 AND source_match_id = $2 AND feature_version = $3",
        source_type,
        source_match_id,
        feature_version,
    ) or 0)


# ── Event windows ──────────────────────────────────────────────────────────────

def _format_embedding(embedding: list[float]) -> str:
    return "[" + ",".join(str(v) for v in embedding) + "]"


async def upsert_event_window(window_id: str, **kwargs) -> None:
    """Insert or replace an event-window record. kwargs must match column names."""
    pool = await get_pool()

    # embedding is a pgvector type; pass as a formatted string with an explicit cast.
    embedding = kwargs.pop("embedding", None)

    cols = ['window_id'] + list(kwargs.keys())
    params: list = [window_id] + list(kwargs.values())
    placeholders = [f'${i+1}' for i in range(len(params))]

    if embedding is not None:
        cols.append('embedding')
        params.append(_format_embedding(embedding))
        placeholders.append(f'${len(params)}::vector({VECTOR_DIM})')

    col_list = ', '.join(cols)
    placeholder_str = ', '.join(placeholders)
    updates = ', '.join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in ('window_id', 'created_at')
    )
    await pool.execute(
        f"""
        INSERT INTO event_windows ({col_list}) VALUES ({placeholder_str})
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
    return _normalize_path_fields(dict(row), "feature_path") if row else None


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
    return [_normalize_path_fields(dict(r), "feature_path") for r in rows]


async def ann_search_event_windows(
    embedding: list[float],
    *,
    source_type: str = "pro",
    map_name: str | None = None,
    feature_version: str | None = None,
    limit: int = 200,
) -> list[dict]:
    """Return up to `limit` event windows nearest to `embedding` via HNSW cosine search."""
    pool = await get_pool()
    embedding_str = _format_embedding(embedding)

    filters = ["source_type = $2", "embedding IS NOT NULL"]
    params: list = [embedding_str, source_type]

    if map_name is not None:
        params.append(map_name)
        filters.append(f"map_name = ${len(params)}")
    if feature_version is not None:
        params.append(feature_version)
        filters.append(f"feature_version = ${len(params)}")

    params.append(limit)
    query = (
        "SELECT * FROM event_windows "
        f"WHERE {' AND '.join(filters)} "
        f"ORDER BY embedding <=> $1::vector({VECTOR_DIM}) "
        f"LIMIT ${len(params)}"
    )
    rows = await pool.fetch(query, *params)
    return [_normalize_path_fields(dict(r), "feature_path") for r in rows]


# ── Round analysis cache ───────────────────────────────────────────────────────

async def get_round_analysis_result(cache_key: str) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM round_analysis_results WHERE cache_key = $1",
        cache_key,
    )
    return dict(row) if row else None


async def get_round_analysis_result_state(
    *,
    demo_id: str,
    round_num: int,
    logic: str,
    matcher_version: str,
    pro_corpus_version: str,
) -> dict:
    cache_key = f"{demo_id}:{round_num}:{logic}:{pro_corpus_version}:{matcher_version}"
    exact = await get_round_analysis_result(cache_key)
    if exact is not None:
        if exact.get("invalidated_at") is None:
            return {
                "cache_key": cache_key,
                "cache_status": "pending" if exact.get("status") == "pending" else "fresh",
                "result": exact,
                "stale_result": None,
            }
        return {
            "cache_key": cache_key,
            "cache_status": "stale",
            "result": None,
            "stale_result": exact,
        }

    pool = await get_pool()
    row = await pool.fetchrow(
        """
        SELECT *
        FROM round_analysis_results
        WHERE demo_id = $1 AND round_num = $2 AND logic = $3
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """,
        demo_id,
        round_num,
        logic,
    )
    if row is not None:
        return {
            "cache_key": cache_key,
            "cache_status": "stale",
            "result": None,
            "stale_result": dict(row),
        }

    return {
        "cache_key": cache_key,
        "cache_status": "missing",
        "result": None,
        "stale_result": None,
    }


async def upsert_round_analysis_result(
    *,
    cache_key: str,
    demo_id: str,
    round_num: int,
    logic: str,
    matcher_version: str,
    pro_corpus_version: str,
    status: str,
    result_payload: dict | None = None,
    error_message: str | None = None,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO round_analysis_results (
            cache_key, demo_id, round_num, logic, matcher_version, pro_corpus_version,
            status, result_json, error_message, invalidated_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, NOW())
        ON CONFLICT (cache_key)
        DO UPDATE SET
            matcher_version = EXCLUDED.matcher_version,
            pro_corpus_version = EXCLUDED.pro_corpus_version,
            status = EXCLUDED.status,
            result_json = EXCLUDED.result_json,
            error_message = EXCLUDED.error_message,
            invalidated_at = NULL,
            updated_at = NOW()
        """,
        cache_key,
        demo_id,
        round_num,
        logic,
        matcher_version,
        pro_corpus_version,
        status,
        json.dumps(result_payload) if result_payload is not None else None,
        error_message,
    )


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
