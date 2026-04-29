"""PostgreSQL connection pool and typed async query helpers."""
from __future__ import annotations

import asyncpg
from datetime import date, datetime, time, timezone
import hashlib
import json
import re

from backend import config

_pool: asyncpg.Pool | None = None
_MATCH_TYPES = {"unknown", "premier", "competitive", "faceit", "hltv"}
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _normalize_path_fields(row: dict | None, *fields: str) -> dict | None:
    if row is None:
        return None
    payload = dict(row)
    for field in fields:
        if field in payload and payload[field] is not None:
            payload[field] = config.resolve_managed_path(payload[field])
    return payload


def _normalized_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def _stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(_normalized_name(value).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _coerce_timestamptz(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    return value


def _coerce_match_type(value: str | None) -> str:
    normalized = (value or "unknown").strip().lower()
    return normalized if normalized in _MATCH_TYPES else "unknown"


def _search_path() -> str:
    schema = config.DB_SCHEMA.strip()
    if not _IDENTIFIER_RE.fullmatch(schema):
        raise ValueError(f"Invalid DB_SCHEMA: {config.DB_SCHEMA!r}")
    return f"{schema}, public"


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=config.DATABASE_URL,
            min_size=2,
            max_size=10,
            server_settings={"search_path": _search_path()},
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


# ── Flat game model ───────────────────────────────────────────────────────────

async def upsert_event_dimension(
    *,
    event_name: str | None,
    source_type: str = "hltv",
    source_event_id: str | None = None,
) -> str | None:
    """Upsert a tournament/event dimension and return event_id.

    source_type/source_event_id are accepted for compatibility with older
    ingestion call sites; the simplified schema deduplicates by normalized name.
    """
    if not event_name or not event_name.strip():
        return None
    event_id = _stable_id("event", event_name)
    normalized = _normalized_name(event_name)
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO events (event_id, event_name, normalized_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (normalized_name) DO UPDATE
            SET event_name = EXCLUDED.event_name
        RETURNING event_id
        """,
        event_id,
        event_name.strip(),
        normalized,
    )
    return row["event_id"] if row else event_id


async def upsert_team_dimension(
    *,
    team_name: str | None,
    source_type: str = "hltv",
    source_team_id: str | None = None,
) -> str | None:
    """Upsert a team dimension and return team_id.

    source_type/source_team_id are accepted for compatibility with older
    ingestion call sites; the simplified schema deduplicates by normalized name.
    """
    if not team_name or not team_name.strip():
        return None
    normalized = _normalized_name(team_name)
    team_id = _stable_id("team", team_name)
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO teams (team_id, team_name, normalized_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (normalized_name) DO UPDATE
            SET team_name = EXCLUDED.team_name
        RETURNING team_id
        """,
        team_id,
        team_name.strip(),
        normalized,
    )
    return row["team_id"] if row else team_id


async def upsert_game(game_id: str, **kwargs) -> None:
    """Insert/update one parsed map/demo row in the flat games table."""
    pool = await get_pool()
    payload = dict(kwargs)
    for key in ("played_at", "ingested_at"):
        if key in payload:
            payload[key] = _coerce_timestamptz(payload[key])
    if "match_type" in payload:
        payload["match_type"] = _coerce_match_type(payload["match_type"])
    for key in ("parquet_dir",):
        if payload.get(key) is not None:
            payload[key] = config.to_managed_path(payload[key])
    payload["updated_at"] = datetime.now(timezone.utc)

    cols = ["game_id"] + list(payload.keys())
    params = [game_id] + list(payload.values())
    placeholders = ", ".join(f"${i+1}" for i in range(len(params)))
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in payload if c != "created_at"
    )
    await pool.execute(
        f"""
        INSERT INTO games ({', '.join(cols)}) VALUES ({placeholders})
        ON CONFLICT (game_id) DO UPDATE SET {updates}
        """,
        *params,
    )


async def upsert_pro_game(
    *,
    game_id: str,
    map_name: str,
    hltv_match_id: str | None,
    hltv_url: str | None,
    source_slug: str | None,
    event_name: str | None,
    team1_name: str | None,
    team2_name: str | None,
    match_date,
    parquet_dir: str,
    ct_round_wins: int | None,
    t_round_wins: int | None,
    round_count: int | None,
    demo_path: str | None = None,
    map_number: int | None = None,
    tick_rate: int | None = None,
    parser_version: str | None = None,
) -> None:
    """Upsert one HLTV-sourced map demo into the flat games table."""
    external_match_id = str(hltv_match_id or game_id.split("_", 1)[0])
    event_id = await upsert_event_dimension(event_name=event_name, source_type="hltv")
    team1_id = await upsert_team_dimension(team_name=team1_name, source_type="hltv")
    team2_id = await upsert_team_dimension(team_name=team2_name, source_type="hltv")

    await upsert_game(
        game_id,
        source_type="pro",
        match_type="hltv",
        map_name=map_name,
        played_at=match_date,
        external_match_id=external_match_id,
        source_url=hltv_url,
        source_slug=source_slug,
        event_id=event_id,
        team1_id=team1_id,
        team2_id=team2_id,
        map_number=map_number,
        demo_stem=game_id,
        parquet_dir=str(parquet_dir),
        ct_round_wins=ct_round_wins,
        t_round_wins=t_round_wins,
        round_count=round_count,
        tick_rate=tick_rate,
        parser_version=parser_version,
        ingest_status="ready",
        ingest_error=None,
        ingested_at=datetime.now(timezone.utc),
        steam_id=None,
        share_code=None,
        user_side_first=None,
        user_result=None,
        user_rounds_won=None,
        user_rounds_lost=None,
        user_kills=None,
        user_deaths=None,
        user_assists=None,
        user_hs_pct=None,
    )


async def upsert_user_game(
    *,
    game_id: str,
    steam_id: str,
    map_name: str,
    match_type: str = "unknown",
    share_code: str | None = None,
    match_date=None,
    parquet_dir: str,
    demo_path: str | None = None,
    round_count: int | None,
    ct_round_wins: int | None = None,
    t_round_wins: int | None = None,
    tick_rate: int | None = None,
    user_side_first: str | None = None,
    user_result: str | None = None,
    user_rounds_won: int | None = None,
    user_rounds_lost: int | None = None,
    user_kills: int | None = None,
    user_deaths: int | None = None,
    user_assists: int | None = None,
    user_hs_pct: float | None = None,
) -> None:
    """Upsert one user-imported map demo into the flat games table."""
    await upsert_game(
        game_id,
        source_type="user",
        match_type=match_type or "unknown",
        map_name=map_name,
        played_at=match_date,
        demo_stem=game_id,
        parquet_dir=str(parquet_dir),
        ct_round_wins=ct_round_wins,
        t_round_wins=t_round_wins,
        round_count=round_count,
        tick_rate=tick_rate,
        ingest_status="ready",
        ingest_error=None,
        ingested_at=datetime.now(timezone.utc),
        steam_id=steam_id,
        share_code=share_code,
        user_side_first=user_side_first,
        user_result=user_result,
        user_rounds_won=user_rounds_won,
        user_rounds_lost=user_rounds_lost,
        user_kills=user_kills,
        user_deaths=user_deaths,
        user_assists=user_assists,
        user_hs_pct=user_hs_pct,
        external_match_id=None,
        source_url=None,
        source_slug=None,
        event_id=None,
        team1_id=None,
        team2_id=None,
        map_number=None,
    )


# ── User game queries ──────────────────────────────────────────────────────────

async def get_user_matches(steam_id: str, limit: int = 30) -> list[dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT
            g.game_id        AS demo_id,
            g.map_name,
            g.match_type,
            g.played_at      AS match_date,
            COALESCE(g.user_rounds_won, g.ct_round_wins)   AS score_ct,
            COALESCE(g.user_rounds_lost, g.t_round_wins)   AS score_t,
            g.user_side_first,
            g.user_result,
            g.user_kills     AS kills,
            g.user_deaths    AS deaths,
            g.user_assists   AS assists,
            g.user_hs_pct    AS hs_pct,
            g.round_count
        FROM games g
        WHERE g.steam_id = $1 AND g.source_type = 'user'
        ORDER BY g.played_at DESC NULLS LAST, g.ingested_at DESC
        LIMIT $2
        """,
        steam_id, limit,
    )
    return [dict(r) for r in rows]


async def delete_user_game(game_id: str) -> None:
    """Delete a user game and its child rows."""
    pool = await get_pool()
    await pool.execute(
        "DELETE FROM games WHERE game_id = $1 AND source_type = 'user'",
        game_id,
    )


async def get_match_parquet_dir(demo_id: str) -> tuple[str, str, int | None] | None:
    """Return (parquet_dir, map_name, tick_rate) for a user or pro game."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT parquet_dir, map_name, tick_rate FROM games WHERE game_id = $1",
        demo_id,
    )
    if row and row["parquet_dir"]:
        return config.resolve_managed_path(row["parquet_dir"]), row["map_name"], row["tick_rate"]
    return None


async def get_match_source_record(match_id: str) -> dict | None:
    """Return a normalized match record for a user or pro game."""
    records = await get_match_source_records([match_id])
    return records.get(match_id)


async def get_match_source_records(match_ids: list[str]) -> dict[str, dict]:
    """Return normalized match records for multiple user/pro game IDs."""
    if not match_ids:
        return {}
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT
            g.*,
            g.game_id        AS source_match_id,
            g.source_url     AS hltv_url,
            g.played_at      AS match_date,
            e.event_name,
            t1.team_name     AS team1_name,
            t2.team_name     AS team2_name,
            t1.team_name     AS team_ct,
            t2.team_name     AS team_t
        FROM games g
        LEFT JOIN events e ON e.event_id = g.event_id
        LEFT JOIN teams t1 ON t1.team_id = g.team1_id
        LEFT JOIN teams t2 ON t2.team_id = g.team2_id
        WHERE g.game_id = ANY($1::text[])
        """,
        match_ids,
    )
    return {
        row["source_match_id"]: _normalize_path_fields(dict(row), "parquet_dir")
        for row in rows
    }


# ── Pro game queries ───────────────────────────────────────────────────────────

async def get_ingested_pro_match_ids() -> set[str]:
    """Return all ready pro game IDs for idempotency."""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT game_id FROM games WHERE source_type = 'pro' AND ingest_status = 'ready'"
    )
    return {r["game_id"] for r in rows}


async def get_pro_matches(limit: int | None = None) -> list[dict]:
    pool = await get_pool()
    query = """
        SELECT
            g.game_id        AS match_id,
            g.map_name,
            g.parquet_dir,
            g.played_at      AS match_date,
            e.event_name,
            g.source_url     AS hltv_url,
            t1.team_name     AS team1_name,
            t2.team_name     AS team2_name,
            t1.team_name     AS team_ct,
            t2.team_name     AS team_t,
            g.ct_round_wins,
            g.t_round_wins,
            g.round_count,
            g.tick_rate,
            g.ingested_at
        FROM games g
        LEFT JOIN events e ON e.event_id = g.event_id
        LEFT JOIN teams t1 ON t1.team_id = g.team1_id
        LEFT JOIN teams t2 ON t2.team_id = g.team2_id
        WHERE g.source_type = 'pro'
        ORDER BY g.ingested_at DESC NULLS LAST, g.updated_at DESC
    """
    params: tuple = ()
    if limit is not None:
        query += " LIMIT $1"
        params = (limit,)
    rows = await pool.fetch(query, *params)
    return [_normalize_path_fields(dict(r), "parquet_dir") for r in rows]


# ── Round analysis cache ───────────────────────────────────────────────────────

def _round_analysis_row(row) -> dict:
    payload = dict(row)
    payload["demo_id"] = payload["game_id"]
    return payload


async def get_round_analysis_result(demo_id: str, round_num: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM round_analysis_cache WHERE game_id = $1 AND round_num = $2",
        demo_id,
        round_num,
    )
    return _round_analysis_row(row) if row else None


async def get_round_analysis_result_state(
    *,
    demo_id: str,
    round_num: int,
) -> dict:
    """Return one of: fresh, pending, missing — with the cached row if any."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM round_analysis_cache WHERE game_id = $1 AND round_num = $2",
        demo_id,
        round_num,
    )
    if row is None:
        return {"cache_status": "missing", "result": None}
    payload = _round_analysis_row(row)
    cache_status = "pending" if payload.get("status") == "pending" else "fresh"
    return {"cache_status": cache_status, "result": payload}


async def upsert_round_analysis_result(
    *,
    demo_id: str,
    round_num: int,
    status: str,
    result_payload: dict | None = None,
    error_message: str | None = None,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO round_analysis_cache (
            game_id, round_num, status, result_json, error_message, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (game_id, round_num) DO UPDATE SET
            status        = EXCLUDED.status,
            result_json   = EXCLUDED.result_json,
            error_message = EXCLUDED.error_message,
            updated_at    = NOW()
        """,
        demo_id,
        round_num,
        status,
        json.dumps(result_payload) if result_payload is not None else None,
        error_message,
    )


# ── Demo import job queue ─────────────────────────────────────────────────────

async def create_demo_job(
    *,
    job_id: str,
    steam_id: str,
    demo_path: str,
    demo_id: str,
    match_type: str,
) -> None:
    pool = await get_pool()
    coerced_match_type = _coerce_match_type(match_type)
    if coerced_match_type == "hltv":
        coerced_match_type = "unknown"
    await pool.execute(
        """
        INSERT INTO demo_jobs (job_id, steam_id, demo_path, game_id, match_type)
        VALUES ($1, $2, $3, $4, $5)
        """,
        job_id,
        steam_id,
        config.to_managed_path(demo_path),
        demo_id,
        coerced_match_type,
    )


async def claim_demo_job() -> dict | None:
    """Atomically claim one pending job. Returns the claimed row or None."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        UPDATE demo_jobs
        SET status = 'processing', updated_at = NOW()
        WHERE job_id = (
            SELECT job_id FROM demo_jobs
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *, game_id AS demo_id
        """
    )
    return dict(row) if row else None


async def finish_demo_job(
    job_id: str,
    *,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    pool = await get_pool()
    status = "error" if error else "done"
    await pool.execute(
        """
        UPDATE demo_jobs
        SET status = $2, result_json = $3, error = $4, updated_at = NOW()
        WHERE job_id = $1
        """,
        job_id,
        status,
        json.dumps(result) if result is not None else None,
        error,
    )


async def get_demo_job(job_id: str) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT *, game_id AS demo_id FROM demo_jobs WHERE job_id = $1", job_id
    )
    return dict(row) if row else None


# ── Job runs ───────────────────────────────────────────────────────────────────

async def start_job_run(job_name: str) -> int:
    pool = await get_pool()
    row  = await pool.fetchrow(
        "INSERT INTO job_runs (job_name, started_at, status) "
        "VALUES ($1, NOW(), 'running') RETURNING id",
        job_name,
    )
    return row["id"]


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
