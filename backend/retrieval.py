"""Situation-retrieval helpers for matching user windows against pro windows."""
from __future__ import annotations

import json
import math
from pathlib import Path

from backend import db
from pipeline.features.extract_windows import load_match_frames
from pipeline.features.featurize_windows import (
    DEFAULT_WINDOW_POST_TICKS,
    DEFAULT_WINDOW_PRE_TICKS,
    FEATURE_VERSION,
    build_window_features,
)

_WEAPON_COMPATIBILITY = {
    ("sniper", "sniper"): 1.0,
    ("sniper", "rifle"): 0.82,
    ("sniper", "smg"): 0.45,
    ("sniper", "shotgun"): 0.25,
    ("sniper", "heavy"): 0.2,
    ("sniper", "pistol"): 0.2,
    ("rifle", "rifle"): 1.0,
    ("rifle", "smg"): 0.62,
    ("rifle", "shotgun"): 0.32,
    ("rifle", "heavy"): 0.28,
    ("rifle", "pistol"): 0.35,
    ("smg", "smg"): 1.0,
    ("smg", "shotgun"): 0.4,
    ("smg", "heavy"): 0.24,
    ("smg", "pistol"): 0.48,
    ("shotgun", "shotgun"): 1.0,
    ("shotgun", "heavy"): 0.42,
    ("shotgun", "pistol"): 0.28,
    ("heavy", "heavy"): 1.0,
    ("heavy", "pistol"): 0.22,
    ("pistol", "pistol"): 1.0,
}


def _load_feature_blob(feature_path: str) -> dict | None:
    path = Path(feature_path)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _numeric_similarity(query: dict, candidate: dict, keys: tuple[str, ...]) -> float:
    scores: list[float] = []
    for key in keys:
        if key not in query or key not in candidate:
            continue
        qv = float(query[key])
        cv = float(candidate[key])
        denom = max(abs(qv), abs(cv), 1.0)
        scores.append(max(0.0, 1.0 - abs(qv - cv) / denom))
    return sum(scores) / len(scores) if scores else 0.0


def _time_similarity(query: dict, candidate: dict) -> float:
    q_time = query.get("time_since_freeze_end_s")
    c_time = candidate.get("time_since_freeze_end_s")
    if q_time is None or c_time is None:
        return 0.0

    freeze_score = max(0.0, 1.0 - abs(float(q_time) - float(c_time)) / 25.0)
    q_planted = bool(query.get("planted"))
    c_planted = bool(candidate.get("planted"))
    if q_planted != c_planted:
        return 0.0
    if not q_planted:
        return freeze_score

    q_plant = query.get("time_since_bomb_plant_s")
    c_plant = candidate.get("time_since_bomb_plant_s")
    if q_plant is None or c_plant is None:
        return freeze_score * 0.5
    plant_score = max(0.0, 1.0 - abs(float(q_plant) - float(c_plant)) / 12.0)
    return 0.4 * freeze_score + 0.6 * plant_score


def _path_similarity(query_path: list[list[float]], candidate_path: list[list[float]]) -> float:
    if not query_path or not candidate_path:
        return 0.0

    pairs = zip(query_path, candidate_path)
    distances = [
        math.dist((left[0], left[1]), (right[0], right[1]))
        for left, right in pairs
    ]
    if not distances:
        return 0.0
    mean_distance = sum(distances) / len(distances)
    return 1.0 / (1.0 + mean_distance / 450.0)


def _jaccard_similarity(left: list[str], right: list[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set and not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _dict_overlap_similarity(left: dict | None, right: dict | None) -> float:
    left = left or {}
    right = right or {}
    keys = set(left) | set(right)
    if not keys:
        return 0.0
    numerator = sum(min(int(left.get(key, 0)), int(right.get(key, 0))) for key in keys)
    denominator = sum(max(int(left.get(key, 0)), int(right.get(key, 0))) for key in keys)
    return numerator / denominator if denominator else 0.0


def _weapon_compatibility(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    pair = tuple(sorted((left, right)))
    return _WEAPON_COMPATIBILITY.get(pair, 0.15)


def _weapon_similarity(query: dict, candidate: dict) -> float:
    profile_score = (
        _dict_overlap_similarity(query.get("ct_weapon_profile"), candidate.get("ct_weapon_profile"))
        + _dict_overlap_similarity(query.get("t_weapon_profile"), candidate.get("t_weapon_profile"))
        + _dict_overlap_similarity(query.get("shots_weapon_profile"), candidate.get("shots_weapon_profile"))
    ) / 3.0

    focus_score = _weapon_compatibility(
        query.get("focus_weapon_family"),
        candidate.get("focus_weapon_family"),
    )
    return max(profile_score * 0.8 + focus_score * 0.2, focus_score)


def _situation_similarity(query: dict, candidate: dict) -> float:
    tag_score = _jaccard_similarity(
        list(query.get("situation_tags") or []),
        list(candidate.get("situation_tags") or []),
    )
    primary_match = 1.0 if query.get("primary_situation") == candidate.get("primary_situation") else 0.0
    planted_match = 1.0 if bool(query.get("planted")) == bool(candidate.get("planted")) else 0.0
    return (0.5 * tag_score) + (0.3 * primary_match) + (0.2 * planted_match)


def _state_similarity(query: dict, candidate: dict) -> float:
    return _numeric_similarity(
        query["vector"],
        candidate["vector"],
        (
            "alive_ct",
            "alive_t",
            "alive_diff",
            "hp_ct_sum",
            "hp_t_sum",
            "defuser_ct_count",
            "deaths_ct",
            "deaths_t",
            "shots_ct",
            "shots_t",
            "utility_total",
            "seconds_remaining_s",
        ),
    )


def _spatial_similarity(query: dict, candidate: dict) -> float:
    site_score = 0.0
    if query.get("site") and candidate.get("site"):
        site_score = 1.0 if query["site"] == candidate["site"] else 0.0

    place_score = (
        _dict_overlap_similarity(query.get("ct_place_profile"), candidate.get("ct_place_profile"))
        + _dict_overlap_similarity(query.get("t_place_profile"), candidate.get("t_place_profile"))
        + _jaccard_similarity(query.get("ct_top_places", []), candidate.get("ct_top_places", []))
        + _jaccard_similarity(query.get("t_top_places", []), candidate.get("t_top_places", []))
    ) / 4.0

    centroid_score = _numeric_similarity(
        query["vector"],
        candidate["vector"],
        (
            "ct_centroid_x",
            "ct_centroid_y",
            "t_centroid_x",
            "t_centroid_y",
            "ct_spread",
            "t_spread",
        ),
    )
    return (0.25 * site_score) + (0.4 * place_score) + (0.35 * centroid_score)


def _movement_similarity(query: dict, candidate: dict) -> float:
    path_score = (
        _path_similarity(query.get("ct_centroid_path", []), candidate.get("ct_centroid_path", []))
        + _path_similarity(query.get("t_centroid_path", []), candidate.get("t_centroid_path", []))
    ) / 2.0

    distance_score = _numeric_similarity(
        query["vector"],
        candidate["vector"],
        ("ct_path_distance", "t_path_distance"),
    )
    return (path_score + distance_score) / 2.0


def _situation_weights(query: dict) -> dict[str, float]:
    tags = set(query.get("situation_tags") or [])
    if {"fight", "trade_window", "clutch", "retake", "post_plant"} & tags:
        return {
            "time": 0.12,
            "situation": 0.22,
            "state": 0.24,
            "spatial": 0.18,
            "movement": 0.08,
            "weapons": 0.16,
        }
    if {"setup", "rotate", "default", "mid_round"} & tags:
        return {
            "time": 0.18,
            "situation": 0.18,
            "state": 0.16,
            "spatial": 0.26,
            "movement": 0.12,
            "weapons": 0.10,
        }
    return {
        "time": 0.15,
        "situation": 0.20,
        "state": 0.20,
        "spatial": 0.22,
        "movement": 0.10,
        "weapons": 0.13,
    }


def _score_candidate(query: dict, candidate: dict) -> float:
    if bool(query.get("planted")) != bool(candidate.get("planted")):
        return 0.0

    weights = _situation_weights(query)
    parts = {
        "time": _time_similarity(query, candidate),
        "situation": _situation_similarity(query, candidate),
        "state": _state_similarity(query, candidate),
        "spatial": _spatial_similarity(query, candidate),
        "movement": _movement_similarity(query, candidate),
        "weapons": _weapon_similarity(query, candidate),
    }
    return sum(parts[key] * weight for key, weight in weights.items())


def _build_reason(query: dict, candidate: dict) -> str:
    parts: list[str] = []

    if query.get("primary_situation") and query.get("primary_situation") == candidate.get("primary_situation"):
        parts.append(f"same {query['primary_situation'].replace('_', ' ')}")
    elif query.get("situation_tags") and candidate.get("situation_tags"):
        overlap = sorted(set(query["situation_tags"]) & set(candidate["situation_tags"]))
        if overlap:
            parts.append(f"shared tags: {', '.join(tag.replace('_', ' ') for tag in overlap[:2])}")

    if query.get("site") and query.get("site") == candidate.get("site"):
        parts.append(f"same {query['site'].upper()} site")

    if candidate["vector"].get("alive_ct") == query["vector"].get("alive_ct") and candidate["vector"].get("alive_t") == query["vector"].get("alive_t"):
        parts.append(f"same alive state {query['vector']['alive_ct']}v{query['vector']['alive_t']}")

    q_weapon = query.get("focus_weapon_family")
    c_weapon = candidate.get("focus_weapon_family")
    if q_weapon and c_weapon:
        if q_weapon == c_weapon:
            parts.append(f"same focal weapon family: {q_weapon}")
        elif _weapon_compatibility(q_weapon, c_weapon) >= 0.75:
            parts.append(f"compatible focal weapons: {q_weapon} to {c_weapon}")

    if query.get("planted") and candidate.get("planted"):
        q_plant = query.get("time_since_bomb_plant_s")
        c_plant = candidate.get("time_since_bomb_plant_s")
        if q_plant is not None and c_plant is not None:
            parts.append(f"similar plant timing ({round(float(c_plant), 1)}s after plant)")

    return "; ".join(parts[:3]) or "closest situation window in the current pro corpus"


async def build_query_window(
    demo_id: str,
    round_num: int,
    anchor_tick: int,
    *,
    pre_ticks: int = DEFAULT_WINDOW_PRE_TICKS,
    post_ticks: int = DEFAULT_WINDOW_POST_TICKS,
) -> dict:
    """Create an in-memory query feature blob from an existing match parquet set."""
    record = await db.get_match_source_record(demo_id)
    if not record or not record.get("parquet_dir"):
        raise ValueError(f"Unknown match or missing parquet_dir: {demo_id}")

    parquet_dir = Path(record["parquet_dir"])
    frames = load_match_frames(parquet_dir, demo_id)

    feature = build_window_features(
        ticks=frames["ticks"],
        rounds=frames["rounds"],
        shots=frames["shots"],
        smokes=frames["smokes"],
        infernos=frames["infernos"],
        flashes=frames["flashes"],
        grenade_paths=frames["grenade_paths"],
        round_num=round_num,
        start_tick=max(0, anchor_tick - pre_ticks),
        anchor_tick=anchor_tick,
        end_tick=anchor_tick + post_ticks,
        user_steam_id=record.get("steam_id"),
        anchor_kind="manual_query",
    )
    feature["map_name"] = record.get("map_name")
    feature["source_match_id"] = record.get("source_match_id")
    feature["source_type"] = record.get("source_type")
    return feature


async def retrieve_similar_pro_windows(
    demo_id: str,
    round_num: int,
    anchor_tick: int,
    *,
    limit: int = 10,
    candidate_limit: int = 2000,
) -> list[dict]:
    """Return top-N similar pro situation windows for a user-selected query window."""
    query_feature = await build_query_window(demo_id, round_num, anchor_tick)
    if not query_feature.get("queryable", True):
        return []

    map_name = query_feature.get("map_name")
    candidates = await db.list_event_window_candidates(
        source_type="pro",
        map_name=map_name,
        feature_version=FEATURE_VERSION,
        limit=candidate_limit,
    )
    if not candidates:
        return []

    scored: list[dict] = []
    for candidate in candidates:
        feature_blob = _load_feature_blob(candidate["feature_path"])
        if feature_blob is None or not feature_blob.get("queryable", True):
            continue

        score = _score_candidate(query_feature, feature_blob)
        if score <= 0:
            continue

        scored.append({
            "window_id": candidate["window_id"],
            "source_match_id": candidate["source_match_id"],
            "map_name": candidate.get("map_name"),
            "round_num": candidate["round_num"],
            "anchor_tick": candidate["anchor_tick"],
            "start_tick": candidate["start_tick"],
            "end_tick": candidate["end_tick"],
            "phase": candidate.get("phase"),
            "site": candidate.get("site"),
            "anchor_kind": candidate.get("anchor_kind"),
            "score": round(score, 4),
            "reason": _build_reason(query_feature, feature_blob),
            "feature_path": candidate["feature_path"],
            "primary_situation": feature_blob.get("primary_situation"),
            "situation_tags": feature_blob.get("situation_tags", []),
            "focus_weapon_family": feature_blob.get("focus_weapon_family"),
            "time_since_freeze_end_s": feature_blob.get("time_since_freeze_end_s"),
            "time_since_bomb_plant_s": feature_blob.get("time_since_bomb_plant_s"),
            "planted": feature_blob.get("planted"),
        })

    scored.sort(key=lambda row: row["score"], reverse=True)
    return scored[:limit]


async def get_best_pro_mapping(
    demo_id: str,
    round_num: int,
    anchor_tick: int,
) -> dict:
    """Return one best-match mapping payload for a user round anchor."""
    query_feature = await build_query_window(demo_id, round_num, anchor_tick)
    matches = await retrieve_similar_pro_windows(
        demo_id,
        round_num,
        anchor_tick,
        limit=1,
    )

    best = matches[0] if matches else None
    best_match_record = (
        await db.get_match_source_record(best["source_match_id"])
        if best is not None
        else None
    )

    return {
        "query": {
            "demo_id": demo_id,
            "round_num": round_num,
            "anchor_tick": anchor_tick,
            "start_tick": query_feature["start_tick"],
            "end_tick": query_feature["end_tick"],
            "phase": query_feature.get("phase"),
            "site": query_feature.get("site"),
            "side_to_query": query_feature.get("side_to_query"),
            "primary_situation": query_feature.get("primary_situation"),
            "situation_tags": query_feature.get("situation_tags", []),
            "focus_weapon_family": query_feature.get("focus_weapon_family"),
            "time_since_freeze_end_s": query_feature.get("time_since_freeze_end_s"),
            "time_since_bomb_plant_s": query_feature.get("time_since_bomb_plant_s"),
            "queryable": query_feature.get("queryable", True),
            "skip_reason": query_feature.get("skip_reason"),
        },
        "best_match": (
            {
                **best,
                "map_name": best_match_record.get("map_name") if best_match_record else None,
                "event_name": best_match_record.get("event_name") if best_match_record else None,
                "team_ct": best_match_record.get("team_ct") if best_match_record else None,
                "team_t": best_match_record.get("team_t") if best_match_record else None,
                "match_date": (
                    best_match_record.get("match_date").isoformat()
                    if best_match_record and best_match_record.get("match_date")
                    else None
                ),
            }
            if best is not None
            else None
        ),
    }
