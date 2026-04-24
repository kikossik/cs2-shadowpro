"""Situation-retrieval helpers for matching user windows against pro windows."""
from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path

import polars as pl

from backend import db
from pipeline.features.extract_windows import list_match_anchor_specs, load_match_frames
from pipeline.features.featurize_windows import (
    DEFAULT_WINDOW_POST_TICKS,
    DEFAULT_WINDOW_PRE_TICKS,
    FEATURE_VERSION,
    build_window_features,
)
from pipeline.features.vectorize import feature_blob_to_vector

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


def _finalize_query_feature(feature: dict, record: dict) -> dict:
    payload = dict(feature)
    payload["map_name"] = record.get("map_name")
    payload["source_match_id"] = record.get("source_match_id")
    payload["source_type"] = record.get("source_type")
    return payload


def _build_query_window_from_frames(
    *,
    frames: dict,
    record: dict,
    round_num: int,
    start_tick: int,
    anchor_tick: int,
    end_tick: int,
    anchor_kind: str = "manual_query",
) -> dict:
    feature = build_window_features(
        ticks=frames["ticks"],
        rounds=frames["rounds"],
        shots=frames["shots"],
        smokes=frames["smokes"],
        infernos=frames["infernos"],
        flashes=frames["flashes"],
        grenade_paths=frames["grenade_paths"],
        round_num=round_num,
        start_tick=start_tick,
        anchor_tick=anchor_tick,
        end_tick=end_tick,
        user_steam_id=record.get("steam_id"),
        anchor_kind=anchor_kind,
    )
    return _finalize_query_feature(feature, record)


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
    return _build_query_window_from_frames(
        frames=frames,
        record=record,
        round_num=round_num,
        start_tick=max(0, anchor_tick - pre_ticks),
        anchor_tick=anchor_tick,
        end_tick=anchor_tick + post_ticks,
    )


def _score_loaded_candidate(
    *,
    query_feature: dict,
    candidate: dict,
    candidate_feature: dict | None,
) -> dict | None:
    if candidate_feature is None or not candidate_feature.get("queryable", True):
        return None

    score = _score_candidate(query_feature, candidate_feature)
    if score <= 0:
        return None

    return {
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
        "reason": _build_reason(query_feature, candidate_feature),
        "feature_path": candidate["feature_path"],
        "primary_situation": candidate_feature.get("primary_situation"),
        "situation_tags": candidate_feature.get("situation_tags", []),
        "focus_weapon_family": candidate_feature.get("focus_weapon_family"),
        "time_since_freeze_end_s": candidate_feature.get("time_since_freeze_end_s"),
        "time_since_bomb_plant_s": candidate_feature.get("time_since_bomb_plant_s"),
        "planted": candidate_feature.get("planted"),
    }


def _score_candidates_for_query(
    *,
    query_feature: dict,
    candidates: list[dict],
    candidate_features: dict[str, dict | None],
    limit: int,
) -> list[dict]:
    scored: list[dict] = []
    for candidate in candidates:
        match = _score_loaded_candidate(
            query_feature=query_feature,
            candidate=candidate,
            candidate_feature=candidate_features.get(candidate["window_id"]),
        )
        if match is None:
            continue
        scored.append(match)

    scored.sort(key=lambda row: row["score"], reverse=True)
    return scored[:limit]


def _query_payload(demo_id: str, round_num: int, query_feature: dict) -> dict:
    return {
        "demo_id": demo_id,
        "round_num": round_num,
        "anchor_tick": query_feature["anchor_tick"],
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
    }


def _fallback_round_query(demo_id: str, round_num: int, frames: dict) -> dict:
    round_rows = frames["rounds"].filter(pl.col("round_num") == round_num)
    tick_rows = frames["ticks"].filter(pl.col("round_num") == round_num)

    anchor_tick = 0
    if round_rows.height > 0 and "freeze_end" in round_rows.columns:
        anchor_tick = int(round_rows["freeze_end"][0] or 0)
    elif tick_rows.height > 0 and "tick" in tick_rows.columns:
        anchor_tick = int(tick_rows["tick"].min())

    end_tick = anchor_tick
    if round_rows.height > 0:
        if "official_end" in round_rows.columns and round_rows["official_end"][0] is not None:
            end_tick = int(round_rows["official_end"][0])
        elif "end" in round_rows.columns and round_rows["end"][0] is not None:
            end_tick = int(round_rows["end"][0])
    elif tick_rows.height > 0 and "tick" in tick_rows.columns:
        end_tick = int(tick_rows["tick"].max())

    return {
        "demo_id": demo_id,
        "round_num": round_num,
        "anchor_tick": anchor_tick,
        "start_tick": anchor_tick,
        "end_tick": end_tick,
        "phase": None,
        "site": None,
        "side_to_query": None,
        "primary_situation": None,
        "situation_tags": [],
        "focus_weapon_family": None,
        "time_since_freeze_end_s": None,
        "time_since_bomb_plant_s": None,
        "queryable": False,
        "skip_reason": "no queryable anchor windows for round",
    }


async def _ann_candidates(
    query_feature: dict,
    *,
    candidate_limit: int,
) -> tuple[list[dict], dict[str, dict | None]]:
    """Return (candidates, candidate_features) via pgvector ANN search."""
    embedding = feature_blob_to_vector(query_feature)
    candidates = await db.ann_search_event_windows(
        embedding,
        source_type="pro",
        map_name=query_feature.get("map_name"),
        feature_version=FEATURE_VERSION,
        limit=candidate_limit,
    )
    candidate_features = {
        c["window_id"]: _load_feature_blob(c["feature_path"])
        for c in candidates
    }
    return candidates, candidate_features


async def retrieve_similar_pro_windows(
    demo_id: str,
    round_num: int,
    anchor_tick: int,
    *,
    limit: int = 10,
    candidate_limit: int = 200,
) -> list[dict]:
    """Return top-N similar pro situation windows for a user-selected query window."""
    query_feature = await build_query_window(demo_id, round_num, anchor_tick)
    if not query_feature.get("queryable", True):
        return []

    candidates, candidate_features = await _ann_candidates(
        query_feature, candidate_limit=candidate_limit
    )
    if not candidates:
        return []

    return _score_candidates_for_query(
        query_feature=query_feature,
        candidates=candidates,
        candidate_features=candidate_features,
        limit=limit,
    )


def collapse_window_hits_to_candidate_rounds(
    *,
    window_hits: list[dict],
    total_query_windows: int,
    limit: int = 10,
) -> list[dict]:
    """Group window-level hits into ranked pro-round shortlist candidates."""
    grouped: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for hit in window_hits:
        key = (hit["source_match_id"], int(hit["round_num"]))
        grouped[key].append(hit)

    candidates: list[dict] = []
    for (source_match_id, pro_round_num), hits in grouped.items():
        ranked_hits = sorted(
            hits,
            key=lambda row: (
                row["score"],
                row.get("query_anchor_tick", -1),
                row.get("anchor_tick", -1),
            ),
            reverse=True,
        )
        best_hit = ranked_hits[0]
        distinct_queries = {
            (hit.get("query_anchor_tick"), hit.get("query_anchor_kind"))
            for hit in ranked_hits
        }
        top_scores = [float(hit["score"]) for hit in ranked_hits[:3]]
        avg_top_score = sum(top_scores) / len(top_scores)
        coverage = (
            min(len(distinct_queries), total_query_windows) / total_query_windows
            if total_query_windows > 0
            else 0.0
        )
        candidate_score = round((0.7 * avg_top_score) + (0.3 * coverage), 4)

        candidates.append({
            "source_match_id": source_match_id,
            "round_num": pro_round_num,
            "map_name": best_hit.get("map_name"),
            "score": candidate_score,
            "best_window_score": best_hit["score"],
            "coverage": round(coverage, 4),
            "supporting_window_hits": len(ranked_hits),
            "matched_query_windows": len(distinct_queries),
            "top_window": best_hit,
            "window_hits": ranked_hits[:5],
            "query_anchor_kinds": sorted({
                kind for _, kind in distinct_queries if kind
            }),
        })

    candidates.sort(
        key=lambda row: (
            row["score"],
            row["best_window_score"],
            row["matched_query_windows"],
        ),
        reverse=True,
    )
    return candidates[:limit]


def _empty_shortlist_result(query: dict, query_windows: list | None = None) -> dict:
    return {
        "query": query,
        "query_windows": query_windows or [],
        "window_hits": [],
        "shortlist": [],
    }


async def build_pro_round_shortlist(
    demo_id: str,
    round_num: int,
    *,
    per_query_limit: int = 4,
    limit: int = 8,
    candidate_limit: int = 200,
) -> dict:
    """Use the fast event-window corpus as Stage 1 retrieval for one round."""
    record = await db.get_match_source_record(demo_id)
    if not record or not record.get("parquet_dir"):
        raise ValueError(f"Unknown match or missing parquet_dir: {demo_id}")

    parquet_dir = Path(record["parquet_dir"])
    frames = load_match_frames(parquet_dir, demo_id)
    anchors = [
        anchor
        for anchor in list_match_anchor_specs(frames=frames)
        if int(anchor["round_num"]) == int(round_num)
    ]
    if not anchors:
        return _empty_shortlist_result(_fallback_round_query(demo_id, round_num, frames))

    query_windows: list[dict] = []
    for anchor in anchors:
        feature = _build_query_window_from_frames(
            frames=frames,
            record=record,
            round_num=round_num,
            start_tick=anchor["start_tick"],
            anchor_tick=anchor["anchor_tick"],
            end_tick=anchor["end_tick"],
            anchor_kind=anchor["anchor_kind"],
        )
        if not feature.get("queryable", True):
            continue
        query_windows.append(feature)

    if not query_windows:
        return _empty_shortlist_result(_fallback_round_query(demo_id, round_num, frames))

    # ANN search per query window; union results so each anchor contributes candidates.
    candidate_by_id: dict[str, dict] = {}
    per_query_ann = max(50, candidate_limit // max(len(query_windows), 1))
    for query_feature in query_windows:
        ann_hits, _ = await _ann_candidates(query_feature, candidate_limit=per_query_ann)
        for c in ann_hits:
            candidate_by_id.setdefault(c["window_id"], c)

    candidates = list(candidate_by_id.values())
    if not candidates:
        return _empty_shortlist_result(_query_payload(demo_id, round_num, query_windows[0]), query_windows)

    candidate_features = {
        c["window_id"]: _load_feature_blob(c["feature_path"])
        for c in candidates
    }

    window_hits: list[dict] = []
    for query_feature in query_windows:
        query_hits = _score_candidates_for_query(
            query_feature=query_feature,
            candidates=candidates,
            candidate_features=candidate_features,
            limit=per_query_limit,
        )
        for hit in query_hits:
            window_hits.append({
                **hit,
                "query_anchor_tick": query_feature["anchor_tick"],
                "query_start_tick": query_feature["start_tick"],
                "query_end_tick": query_feature["end_tick"],
                "query_anchor_kind": query_feature.get("anchor_kind"),
                "query_phase": query_feature.get("phase"),
                "query_site": query_feature.get("site"),
                "query_primary_situation": query_feature.get("primary_situation"),
            })

    shortlist = collapse_window_hits_to_candidate_rounds(
        window_hits=window_hits,
        total_query_windows=len(query_windows),
        limit=limit,
    )

    representative_query = None
    if shortlist:
        top_window = shortlist[0]["top_window"]
        matched_query_feature = next(
            (
                feature
                for feature in query_windows
                if int(feature["anchor_tick"]) == int(top_window["query_anchor_tick"])
                and feature.get("anchor_kind") == top_window.get("query_anchor_kind")
            ),
            query_windows[0],
        )
        representative_query = {
            "demo_id": demo_id,
            "round_num": round_num,
            "anchor_tick": top_window["query_anchor_tick"],
            "start_tick": top_window["query_start_tick"],
            "end_tick": top_window["query_end_tick"],
            "phase": top_window.get("query_phase"),
            "site": top_window.get("query_site"),
            "side_to_query": matched_query_feature.get("side_to_query"),
            "primary_situation": top_window.get("query_primary_situation"),
            "situation_tags": matched_query_feature.get("situation_tags", []),
            "focus_weapon_family": matched_query_feature.get("focus_weapon_family"),
            "time_since_freeze_end_s": matched_query_feature.get("time_since_freeze_end_s"),
            "time_since_bomb_plant_s": matched_query_feature.get("time_since_bomb_plant_s"),
            "queryable": True,
            "skip_reason": None,
        }
    else:
        representative_query = _query_payload(demo_id, round_num, query_windows[0])

    return {
        "query": representative_query,
        "query_windows": query_windows,
        "window_hits": window_hits,
        "shortlist": shortlist,
    }


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
        "query": _query_payload(demo_id, round_num, query_feature),
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
