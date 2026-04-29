"""Stage-1 retrieval: build query windows for a user round, run ANN search
against the pro event-window corpus, collapse window hits into pro-round
candidates.

No per-feature similarity scoring is done here — the cosine similarity from
pgvector's HNSW index is the entire signal. Per-round score is the best window
cosine score, lightly boosted by anchor coverage.
"""
from __future__ import annotations

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


# ── Query window construction ─────────────────────────────────────────────────

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
    record = await db.get_match_source_record(demo_id)
    if not record or not record.get("parquet_dir"):
        raise ValueError(f"Unknown match or missing parquet_dir: {demo_id}")
    frames = load_match_frames(Path(record["parquet_dir"]), demo_id)
    return _build_query_window_from_frames(
        frames=frames,
        record=record,
        round_num=round_num,
        start_tick=max(0, anchor_tick - pre_ticks),
        anchor_tick=anchor_tick,
        end_tick=anchor_tick + post_ticks,
    )


# ── Round shortlist ───────────────────────────────────────────────────────────

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
        "queryable": False,
        "skip_reason": "no queryable anchor windows for round",
    }


def _empty_shortlist_result(query: dict) -> dict:
    return {"query": query, "shortlist": []}


def _collapse_to_round_candidates(
    *,
    window_hits: list[dict],
    total_query_windows: int,
    limit: int,
) -> list[dict]:
    """Group window hits by (source_match_id, round_num); score each round by
    its best per-window cosine, lightly boosted by anchor coverage."""
    grouped: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for hit in window_hits:
        grouped[(hit["source_match_id"], int(hit["round_num"]))].append(hit)

    candidates: list[dict] = []
    for (source_match_id, pro_round_num), hits in grouped.items():
        ranked = sorted(hits, key=lambda h: h["score"], reverse=True)
        best = ranked[0]
        distinct_query_anchors = {h.get("query_anchor_tick") for h in ranked}
        coverage = (
            min(len(distinct_query_anchors), total_query_windows) / total_query_windows
            if total_query_windows > 0 else 0.0
        )
        candidates.append({
            "source_match_id": source_match_id,
            "round_num": pro_round_num,
            "map_name": best.get("map_name"),
            "score": round(best["score"] * (0.85 + 0.15 * coverage), 4),
            "best_window_score": best["score"],
            "coverage": round(coverage, 4),
            "supporting_window_hits": len(ranked),
            "matched_query_windows": len(distinct_query_anchors),
            "top_window": best,
        })

    candidates.sort(key=lambda c: c["score"], reverse=True)
    return candidates[:limit]


async def build_pro_round_shortlist(
    demo_id: str,
    round_num: int,
    *,
    per_query_limit: int = 4,
    limit: int = 8,
    candidate_limit: int = 200,
) -> dict:
    """Return a ranked list of pro rounds plausibly similar to one user round.

    Pipeline: anchor windows → ANN per anchor → collapse hits per pro round.
    The score on each shortlist row is purely cosine-derived; deep scoring is
    delegated to backend/round_mapper.py (currently a placeholder)."""
    record = await db.get_match_source_record(demo_id)
    if not record or not record.get("parquet_dir"):
        raise ValueError(f"Unknown match or missing parquet_dir: {demo_id}")

    frames = load_match_frames(Path(record["parquet_dir"]), demo_id)
    anchors = [
        a for a in list_match_anchor_specs(frames=frames)
        if int(a["round_num"]) == int(round_num)
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
        if feature.get("queryable", True):
            query_windows.append(feature)

    if not query_windows:
        return _empty_shortlist_result(_fallback_round_query(demo_id, round_num, frames))

    per_query_ann = max(50, candidate_limit // max(len(query_windows), 1))
    window_hits: list[dict] = []
    for query_feature in query_windows:
        embedding = feature_blob_to_vector(query_feature)
        ann_hits = await db.ann_search_event_windows(
            embedding,
            source_type="pro",
            map_name=query_feature.get("map_name"),
            feature_version=FEATURE_VERSION,
            limit=per_query_ann,
        )
        for hit in ann_hits[:per_query_limit]:
            distance = float(hit.get("cosine_distance") or 0.0)
            window_hits.append({
                **hit,
                "score": round(max(0.0, 1.0 - distance), 4),
                "query_anchor_tick": query_feature["anchor_tick"],
                "query_anchor_kind": query_feature.get("anchor_kind"),
            })

    if not window_hits:
        return _empty_shortlist_result(_query_payload(demo_id, round_num, query_windows[0]))

    shortlist = _collapse_to_round_candidates(
        window_hits=window_hits,
        total_query_windows=len(query_windows),
        limit=limit,
    )

    return {
        "query": _query_payload(demo_id, round_num, query_windows[0]),
        "shortlist": shortlist,
    }
