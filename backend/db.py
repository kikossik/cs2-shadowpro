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


def hltv_match_key(hltv_match_id: str | int | None, fallback: str) -> str:
    """Stable dimensional match key for an HLTV match page."""
    external_id = str(hltv_match_id or "").strip()
    if not external_id:
        external_id = fallback.split("_", 1)[0]
    return f"hltv_{external_id}"


def user_match_key(demo_id: str) -> str:
    """Stable dimensional match key for a user-imported game."""
    return f"user_{demo_id}"


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


# ── Dimensional match/game model ──────────────────────────────────────────────

async def upsert_event_dimension(
    *,
    event_name: str | None,
    source_type: str = "hltv",
    source_event_id: str | None = None,
) -> str | None:
    """Upsert a source event/tournament dimension and return event_id."""
    if not event_name or not event_name.strip():
        return None
    event_id = (
        f"{source_type}_event_{source_event_id}"
        if source_event_id
        else _stable_id(f"{source_type}_event", event_name)
    )
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO events (event_id, source_type, source_event_id, event_name, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (event_id) DO UPDATE
            SET source_event_id = COALESCE(EXCLUDED.source_event_id, events.source_event_id),
                event_name = EXCLUDED.event_name,
                updated_at = NOW()
        """,
        event_id,
        source_type,
        source_event_id,
        event_name.strip(),
    )
    return event_id


async def upsert_team_dimension(
    *,
    team_name: str | None,
    source_type: str = "hltv",
    source_team_id: str | None = None,
) -> str | None:
    """Upsert a team dimension and return team_id."""
    if not team_name or not team_name.strip():
        return None
    normalized = _normalized_name(team_name)
    team_id = (
        f"{source_type}_team_{source_team_id}"
        if source_team_id
        else _stable_id("team", team_name)
    )
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO teams (
            team_id, source_type, source_team_id, team_name, normalized_name, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (team_id) DO UPDATE
            SET source_team_id = COALESCE(EXCLUDED.source_team_id, teams.source_team_id),
                team_name = EXCLUDED.team_name,
                normalized_name = EXCLUDED.normalized_name,
                updated_at = NOW()
        """,
        team_id,
        source_type,
        source_team_id,
        team_name.strip(),
        normalized,
    )
    return team_id


async def upsert_match_dimension(match_id: str, **kwargs) -> None:
    """Insert/update one source match container row."""
    pool = await get_pool()
    payload = dict(kwargs)
    if "played_at" in payload:
        payload["played_at"] = _coerce_timestamptz(payload["played_at"])
    if "match_type" in payload:
        payload["match_type"] = _coerce_match_type(payload["match_type"])
    payload["updated_at"] = datetime.now(timezone.utc)

    cols = ["match_id"] + list(payload.keys())
    params = [match_id] + list(payload.values())
    placeholders = ", ".join(f"${i+1}" for i in range(len(params)))
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in payload if c != "created_at"
    )
    await pool.execute(
        f"""
        INSERT INTO matches ({', '.join(cols)}) VALUES ({placeholders})
        ON CONFLICT (match_id) DO UPDATE SET {updates}
        """,
        *params,
    )


async def link_match_team(
    *,
    match_id: str,
    team_slot: int,
    team_id: str | None,
    source_team_name: str | None,
) -> None:
    if team_id is None:
        return
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO match_teams (match_id, team_slot, team_id, source_team_name)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (match_id, team_slot) DO UPDATE
            SET team_id = EXCLUDED.team_id,
                source_team_name = EXCLUDED.source_team_name
        """,
        match_id,
        team_slot,
        team_id,
        source_team_name,
    )


async def upsert_game(game_id: str, **kwargs) -> None:
    """Insert/update one parsed map/demo row."""
    pool = await get_pool()
    payload = dict(kwargs)
    if "ingested_at" in payload:
        payload["ingested_at"] = _coerce_timestamptz(payload["ingested_at"])
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


async def link_game_team(
    *,
    game_id: str,
    team_id: str | None,
    source_team_slot: int,
    side_first: str | None = None,
    score: int | None = None,
    won: bool | None = None,
) -> None:
    if team_id is None:
        return
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO game_teams (game_id, team_id, source_team_slot, side_first, score, won)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (game_id, team_id) DO UPDATE
            SET source_team_slot = EXCLUDED.source_team_slot,
                side_first = COALESCE(EXCLUDED.side_first, game_teams.side_first),
                score = COALESCE(EXCLUDED.score, game_teams.score),
                won = COALESCE(EXCLUDED.won, game_teams.won)
        """,
        game_id,
        team_id,
        source_team_slot,
        side_first,
        score,
        won,
    )


async def upsert_game_artifact(
    *,
    game_id: str,
    kind: str,
    version: str,
    path: str,
    content_hash: str | None = None,
) -> None:
    pool = await get_pool()
    managed = config.to_managed_path(path)
    await pool.execute(
        """
        INSERT INTO game_artifacts (game_id, kind, version, path, content_hash, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (game_id, kind, version) DO UPDATE
            SET path = EXCLUDED.path,
                content_hash = COALESCE(EXCLUDED.content_hash, game_artifacts.content_hash),
                updated_at = NOW()
        """,
        game_id,
        kind,
        version,
        managed,
        content_hash,
    )


async def upsert_rounds(game_id: str, rows: list[dict]) -> None:
    """Upsert round fact rows for a parsed game."""
    if not rows:
        return
    pool = await get_pool()
    records = [
        (
            game_id,
            row.get("round_num"),
            row.get("start_tick"),
            row.get("freeze_end_tick"),
            row.get("end_tick"),
            row.get("official_end_tick"),
            row.get("winner_side"),
            row.get("reason"),
            row.get("bomb_plant_tick"),
            row.get("bomb_site"),
            row.get("duration_ticks"),
        )
        for row in rows
        if row.get("round_num") is not None
    ]
    await pool.executemany(
        """
        INSERT INTO rounds (
            game_id, round_num, start_tick, freeze_end_tick, end_tick,
            official_end_tick, winner_side, reason, bomb_plant_tick,
            bomb_site, duration_ticks, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW())
        ON CONFLICT (game_id, round_num) DO UPDATE
            SET start_tick = EXCLUDED.start_tick,
                freeze_end_tick = EXCLUDED.freeze_end_tick,
                end_tick = EXCLUDED.end_tick,
                official_end_tick = EXCLUDED.official_end_tick,
                winner_side = EXCLUDED.winner_side,
                reason = EXCLUDED.reason,
                bomb_plant_tick = EXCLUDED.bomb_plant_tick,
                bomb_site = EXCLUDED.bomb_site,
                duration_ticks = EXCLUDED.duration_ticks,
                updated_at = NOW()
        """,
        records,
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
    artifact_version: str | None = None,
    window_feature_version: str | None = None,
) -> None:
    """Upsert canonical dimensions/facts for one HLTV-sourced map demo."""
    match_id = hltv_match_key(hltv_match_id, game_id)
    external_match_id = str(hltv_match_id or game_id.split("_", 1)[0])
    event_id = await upsert_event_dimension(event_name=event_name, source_type="hltv")
    await upsert_match_dimension(
        match_id,
        source_type="pro",
        match_type="hltv",
        external_match_id=external_match_id,
        source_url=hltv_url,
        source_slug=source_slug,
        event_id=event_id,
        played_at=match_date,
    )

    team1_id = await upsert_team_dimension(team_name=team1_name, source_type="hltv")
    team2_id = await upsert_team_dimension(team_name=team2_name, source_type="hltv")
    await link_match_team(match_id=match_id, team_slot=1, team_id=team1_id, source_team_name=team1_name)
    await link_match_team(match_id=match_id, team_slot=2, team_id=team2_id, source_team_name=team2_name)

    await upsert_game(
        game_id,
        match_id=match_id,
        source_type="pro",
        map_name=map_name,
        map_number=map_number,
        demo_stem=game_id,
        demo_path=demo_path,
        parquet_dir=str(parquet_dir),
        artifact_path=artifact_path,
        ct_round_wins=ct_round_wins,
        t_round_wins=t_round_wins,
        round_count=round_count,
        tick_rate=tick_rate,
        artifact_version=artifact_version,
        window_feature_version=window_feature_version,
        ingest_status="ready",
        ingest_error=None,
        ingested_at=datetime.now(timezone.utc),
    )
    await link_game_team(game_id=game_id, team_id=team1_id, source_team_slot=1)
    await link_game_team(game_id=game_id, team_id=team2_id, source_team_slot=2)

    if artifact_path and artifact_version:
        await upsert_game_artifact(
            game_id=game_id,
            kind="match_artifact",
            version=artifact_version,
            path=artifact_path,
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
    round_count: int | None,
    tick_rate: int = 64,
    artifact_version: str | None = None,
    window_feature_version: str | None = None,
) -> None:
    """Upsert canonical dimensions/facts for one user-imported map demo."""
    match_id = user_match_key(game_id)
    await upsert_match_dimension(
        match_id,
        source_type="user",
        match_type=match_type or "unknown",
        external_match_id=game_id,
        share_code=share_code,
        steam_id=steam_id,
        played_at=match_date,
    )
    await upsert_game(
        game_id,
        match_id=match_id,
        source_type="user",
        map_name=map_name,
        demo_stem=game_id,
        parquet_dir=str(parquet_dir),
        artifact_path=artifact_path,
        round_count=round_count,
        tick_rate=tick_rate,
        artifact_version=artifact_version,
        window_feature_version=window_feature_version,
        ingest_status="ready",
        ingest_error=None,
        ingested_at=datetime.now(timezone.utc),
    )
    if artifact_path and artifact_version:
        await upsert_game_artifact(
            game_id=game_id,
            kind="match_artifact",
            version=artifact_version,
            path=artifact_path,
        )


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
        SELECT demo_id, map_name, match_type, match_date, score_ct, score_t,
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
        "SELECT parquet_dir, map_name FROM games WHERE game_id = $1",
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
        """
        SELECT
            g.*,
            g.game_id AS source_match_id,
            m.match_type,
            m.external_match_id,
            m.source_url AS hltv_url,
            m.source_slug,
            m.share_code,
            m.steam_id,
            m.played_at AS match_date,
            e.event_name,
            t1.team_name AS team1_name,
            t2.team_name AS team2_name,
            -- Legacy aliases kept for existing frontend/tests while UI migrates.
            t1.team_name AS team_ct,
            t2.team_name AS team_t
        FROM games g
        JOIN matches m ON m.match_id = g.match_id
        LEFT JOIN events e ON e.event_id = m.event_id
        LEFT JOIN match_teams mt1 ON mt1.match_id = m.match_id AND mt1.team_slot = 1
        LEFT JOIN teams t1 ON t1.team_id = mt1.team_id
        LEFT JOIN match_teams mt2 ON mt2.match_id = m.match_id AND mt2.team_slot = 2
        LEFT JOIN teams t2 ON t2.team_id = mt2.team_id
        WHERE g.game_id = $1
        """,
        match_id,
    )
    if row:
        return _normalize_path_fields(dict(row), "parquet_dir", "artifact_path")

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
    """Return all ready pro game IDs for idempotency."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT game_id AS match_id FROM games
        WHERE source_type = 'pro' AND ingest_status = 'ready'
        UNION
        SELECT match_id FROM pro_matches
        """
    )
    return {r["match_id"] for r in rows}


async def get_pro_matches(limit: int | None = None) -> list[dict]:
    pool = await get_pool()
    query = (
        """
        SELECT
            g.game_id AS match_id,
            g.map_name,
            g.parquet_dir,
            g.artifact_path,
            m.played_at AS match_date,
            e.event_name,
            m.source_url AS hltv_url,
            t1.team_name AS team1_name,
            t2.team_name AS team2_name,
            t1.team_name AS team_ct,
            t2.team_name AS team_t,
            g.ct_round_wins,
            g.t_round_wins,
            g.round_count,
            g.ingested_at
        FROM games g
        JOIN matches m ON m.match_id = g.match_id
        LEFT JOIN events e ON e.event_id = m.event_id
        LEFT JOIN match_teams mt1 ON mt1.match_id = m.match_id AND mt1.team_slot = 1
        LEFT JOIN teams t1 ON t1.team_id = mt1.team_id
        LEFT JOIN match_teams mt2 ON mt2.match_id = m.match_id AND mt2.team_slot = 2
        LEFT JOIN teams t2 ON t2.team_id = mt2.team_id
        WHERE g.source_type = 'pro'
        ORDER BY g.ingested_at DESC NULLS LAST, g.updated_at DESC
        """
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
    await pool.execute(
        "UPDATE games SET artifact_path = $1, updated_at = NOW() WHERE game_id = $2",
        managed,
        source_match_id,
    )
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
