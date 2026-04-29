"""PostgreSQL connection pool and typed async query helpers."""
from __future__ import annotations

import asyncpg
from datetime import date, datetime, time, timezone
import hashlib
import json
import re

from backend import config
from pipeline.features.vectorize import VECTOR_DIM

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
    for key in ("parquet_dir", "artifact_path"):
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
    artifact_path: str | None,
    ct_round_wins: int | None,
    t_round_wins: int | None,
    round_count: int | None,
    demo_path: str | None = None,
    map_number: int | None = None,
    tick_rate: int = 64,
    parser_version: str | None = None,
    artifact_version: str | None = None,
    window_feature_version: str | None = None,
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
        artifact_path=artifact_path,
        ct_round_wins=ct_round_wins,
        t_round_wins=t_round_wins,
        round_count=round_count,
        tick_rate=tick_rate,
        parser_version=parser_version,
        artifact_version=artifact_version,
        feature_version=window_feature_version,
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
    artifact_path: str | None,
    demo_path: str | None = None,
    round_count: int | None,
    ct_round_wins: int | None = None,
    t_round_wins: int | None = None,
    tick_rate: int = 64,
    artifact_version: str | None = None,
    window_feature_version: str | None = None,
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
        artifact_path=artifact_path,
        ct_round_wins=ct_round_wins,
        t_round_wins=t_round_wins,
        round_count=round_count,
        tick_rate=tick_rate,
        artifact_version=artifact_version,
        feature_version=window_feature_version,
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


async def get_match_parquet_dir(demo_id: str) -> tuple[str, str] | None:
    """Return (parquet_dir, map_name) for a user or pro game."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT parquet_dir, map_name FROM games WHERE game_id = $1",
        demo_id,
    )
    if row and row["parquet_dir"]:
        return config.resolve_managed_path(row["parquet_dir"]), row["map_name"]
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
            g.feature_version AS window_feature_version,
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
        row["source_match_id"]: _normalize_path_fields(dict(row), "parquet_dir", "artifact_path")
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
            g.artifact_path,
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
    return [_normalize_path_fields(dict(r), "parquet_dir", "artifact_path") for r in rows]


# ── Artifact path helpers ──────────────────────────────────────────────────────

async def set_match_artifact_path(source_type: str, source_match_id: str, artifact_path: str) -> None:
    """Store the current match artifact path on the game row."""
    from pipeline.steps.build_artifact import ARTIFACT_VERSION
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE games
        SET artifact_path = $1,
            artifact_version = $2,
            updated_at = NOW()
        WHERE source_type = $3 AND game_id = $4
        """,
        config.to_managed_path(artifact_path),
        ARTIFACT_VERSION,
        source_type,
        source_match_id,
    )


# ── Event windows ──────────────────────────────────────────────────────────────

def _format_embedding(embedding: list[float]) -> str:
    return "[" + ",".join(str(v) for v in embedding) + "]"


async def count_event_windows(
    game_id: str,
    *,
    feature_version: str | None = None,
) -> int:
    """Return the number of indexed event windows for one game."""
    pool = await get_pool()
    if feature_version is None:
        return int(await pool.fetchval(
            "SELECT COUNT(*) FROM event_windows WHERE game_id = $1",
            game_id,
        ) or 0)
    return int(await pool.fetchval(
        "SELECT COUNT(*) FROM event_windows WHERE game_id = $1 AND feature_version = $2",
        game_id,
        feature_version,
    ) or 0)


async def upsert_event_windows_batch(windows: list[dict]) -> None:
    """Batch upsert event windows in one executemany call instead of N round trips."""
    if not windows:
        return
    pool = await get_pool()
    records = []
    for w in windows:
        w = dict(w)
        w.pop("source_type", None)
        w.pop("source_match_id", None)
        w.pop("steam_id", None)
        embedding = w.pop("embedding", None)
        records.append((
            w.get("window_id"),
            w.get("game_id"),
            w.get("map_name"),
            w.get("round_num"),
            w.get("start_tick"),
            w.get("anchor_tick"),
            w.get("end_tick"),
            w.get("side_to_query"),
            w.get("phase"),
            w.get("site"),
            w.get("anchor_kind"),
            w.get("alive_ct"),
            w.get("alive_t"),
            w.get("feature_version"),
            config.to_managed_path(w.get("feature_path")) if w.get("feature_path") else None,
            _format_embedding(embedding) if embedding is not None else None,
        ))
    await pool.executemany(
        """
        INSERT INTO event_windows (
            window_id, game_id, map_name, round_num,
            start_tick, anchor_tick, end_tick, side_to_query,
            phase, site, anchor_kind, alive_ct, alive_t,
            feature_version, feature_path, embedding
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15, $16::vector(54)
        )
        ON CONFLICT (window_id) DO UPDATE SET
            game_id         = EXCLUDED.game_id,
            map_name        = EXCLUDED.map_name,
            round_num       = EXCLUDED.round_num,
            start_tick      = EXCLUDED.start_tick,
            anchor_tick     = EXCLUDED.anchor_tick,
            end_tick        = EXCLUDED.end_tick,
            side_to_query   = EXCLUDED.side_to_query,
            phase           = EXCLUDED.phase,
            site            = EXCLUDED.site,
            anchor_kind     = EXCLUDED.anchor_kind,
            alive_ct        = EXCLUDED.alive_ct,
            alive_t         = EXCLUDED.alive_t,
            feature_version = EXCLUDED.feature_version,
            feature_path    = EXCLUDED.feature_path,
            embedding       = EXCLUDED.embedding
        """,
        records,
    )


async def upsert_event_window(window_id: str, **kwargs) -> None:
    """Insert or replace an event-window record. kwargs must match column names."""
    pool = await get_pool()

    # Drop legacy fields that were removed from the schema.
    kwargs.pop("source_type", None)
    kwargs.pop("source_match_id", None)
    kwargs.pop("steam_id", None)

    embedding = kwargs.pop("embedding", None)
    if kwargs.get("feature_path"):
        kwargs["feature_path"] = config.to_managed_path(kwargs["feature_path"])

    cols = ["window_id"] + list(kwargs.keys())
    params: list = [window_id] + list(kwargs.values())
    placeholders = [f"${i+1}" for i in range(len(params))]

    if embedding is not None:
        cols.append("embedding")
        params.append(_format_embedding(embedding))
        placeholders.append(f"${len(params)}::vector({VECTOR_DIM})")

    col_list = ", ".join(cols)
    placeholder_str = ", ".join(placeholders)
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in cols if c not in ("window_id", "created_at")
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

    filters = ["g.source_type = $1"]
    params: list = [source_type]

    if map_name is not None:
        params.append(map_name)
        filters.append(f"ew.map_name = ${len(params)}")
    if phase is not None:
        params.append(phase)
        filters.append(f"ew.phase = ${len(params)}")
    if side_to_query is not None:
        params.append(side_to_query)
        filters.append(f"(ew.side_to_query = ${len(params)} OR ew.side_to_query IS NULL)")
    if feature_version is not None:
        params.append(feature_version)
        filters.append(f"ew.feature_version = ${len(params)}")

    params.append(limit)
    query = (
        "SELECT ew.*, ew.game_id AS source_match_id, g.source_type "
        "FROM event_windows ew "
        "JOIN games g ON g.game_id = ew.game_id "
        f"WHERE {' AND '.join(filters)} "
        "ORDER BY ew.created_at DESC "
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

    filters = ["g.source_type = $2", "ew.embedding IS NOT NULL"]
    params: list = [embedding_str, source_type]

    if map_name is not None:
        params.append(map_name)
        filters.append(f"ew.map_name = ${len(params)}")
    if feature_version is not None:
        params.append(feature_version)
        filters.append(f"ew.feature_version = ${len(params)}")

    params.append(limit)
    query = (
        "SELECT ew.*, ew.game_id AS source_match_id, g.source_type, "
        f"       (ew.embedding <=> $1::vector({VECTOR_DIM})) AS cosine_distance "
        "FROM event_windows ew "
        "JOIN games g ON g.game_id = ew.game_id "
        f"WHERE {' AND '.join(filters)} "
        f"ORDER BY ew.embedding <=> $1::vector({VECTOR_DIM}) "
        f"LIMIT ${len(params)}"
    )
    rows = await pool.fetch(query, *params)
    return [_normalize_path_fields(dict(r), "feature_path") for r in rows]


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
