"""Shared round-analysis computation and caching.

Extracted from backend/main.py so both the FastAPI web server and the
background worker can call compute_and_cache_round without importing the
full FastAPI app.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from backend import db
from backend.log import get_logger

log = get_logger("ROUND_ANALYSIS")

MATCHER_VERSION = "clean-v3"
PRO_CORPUS_VERSION = "event-windows-v1"

_MAP_DISPLAY: dict[str, str] = {
    "de_ancient": "Ancient", "de_anubis": "Anubis", "de_dust2": "Dust 2",
    "de_inferno": "Inferno", "de_mirage": "Mirage", "de_nuke": "Nuke",
    "de_overpass": "Overpass",
}


def map_display(map_name: str | None) -> dict:
    name    = map_name or "unknown"
    display = _MAP_DISPLAY.get(name, name.replace("de_", "").title())
    return {"key": name.replace("de_", ""), "name": name, "display": display}


def _read_json_file(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _artifact_is_current(path: str, expected_version: str) -> bool:
    try:
        payload = _read_json_file(path)
    except (OSError, json.JSONDecodeError):
        return False
    return payload.get("artifact_version") == expected_version


async def _load_round_artifact(match_record: dict, round_num: int) -> dict:
    from pipeline.steps.build_artifact import ARTIFACT_VERSION, build_match_artifact

    artifact_path = match_record.get("artifact_path")

    if (
        not artifact_path
        or not Path(artifact_path).exists()
        or not _artifact_is_current(artifact_path, ARTIFACT_VERSION)
    ):
        parquet_dir = match_record.get("parquet_dir")
        if not parquet_dir:
            raise ValueError(f"No parquet_dir for {match_record.get('source_match_id')}")
        artifact_path = build_match_artifact(
            source_type=match_record["source_type"],
            source_match_id=match_record["source_match_id"],
            parquet_dir=Path(parquet_dir),
            stem=match_record["source_match_id"],
            map_name=match_record["map_name"],
            steam_id=match_record.get("steam_id"),
        )
        await db.set_match_artifact_path(
            match_record["source_type"],
            match_record["source_match_id"],
            artifact_path,
        )

    match_artifact = _read_json_file(artifact_path)
    rounds = match_artifact.get("rounds", {})
    round_artifact = rounds.get(str(round_num)) or rounds.get(round_num)
    if round_artifact is None:
        raise ValueError(
            f"Round {round_num} not found in artifact for {match_record.get('source_match_id')}"
        )
    return round_artifact


def _inject_map(item: dict) -> dict:
    out = dict(item)
    if out.get("map") is None:
        out["map"] = map_display(out.get("map_name"))
    return out


def normalize_round_analysis_result(result: dict | None) -> dict | None:
    if result is None:
        return None
    payload = dict(result)
    if payload.get("best_match") is not None:
        payload["best_match"] = _inject_map(payload["best_match"])
    payload["shortlist"] = [_inject_map(c) for c in payload.get("shortlist") or []]
    payload["matches"]   = [_inject_map(m) for m in payload.get("matches") or []]
    if payload.get("selected_match") is not None:
        payload["selected_match"] = _inject_map(payload["selected_match"])
    return payload


def _build_round_analysis_payload(
    *,
    logic: str,
    shortlist_result: dict,
    enriched_shortlist: list[dict],
    deep_analysis: dict,
) -> dict:
    matches = deep_analysis.get("matches", [])
    selected_match = deep_analysis.get("selected_match")
    best = None
    if selected_match:
        top_window = dict(selected_match["top_window"])
        best = {
            **top_window,
            "score":                  selected_match["deep_score"],
            "map_name":               selected_match.get("map_name"),
            "event_name":             selected_match.get("event_name"),
            "team1_name":             selected_match.get("team1_name"),
            "team2_name":             selected_match.get("team2_name"),
            "team_ct":                selected_match.get("team_ct"),
            "team_t":                 selected_match.get("team_t"),
            "match_date":             selected_match.get("match_date"),
            "candidate_score":        selected_match["deep_score"],
            "retrieval_score":        selected_match["score"],
            "coverage":               selected_match["coverage"],
            "matched_query_windows":  selected_match["matched_query_windows"],
            "supporting_window_hits": selected_match["supporting_window_hits"],
            "shortlist_rank":         selected_match["shortlist_rank"],
            "logic":                  logic,
            "break_event":            selected_match.get("break_event"),
            "shared_prefix":          selected_match.get("shared_prefix"),
            "divergence":             selected_match.get("divergence"),
            "timeline_sync":          selected_match.get("timeline_sync"),
            "pro_time_offset_s":      selected_match.get("pro_time_offset_s"),
            "divergence_start_sec":   selected_match.get("divergence_start_sec"),
            "divergence_end_sec":     selected_match.get("divergence_end_sec"),
            "summary":                selected_match.get("summary"),
        }

    return normalize_round_analysis_result({
        "query":         shortlist_result["query"],
        "best_match":    best,
        "shortlist":     enriched_shortlist,
        "retrieval": {
            "query_window_count":    len(shortlist_result.get("query_windows", [])),
            "window_hit_count":      len(shortlist_result.get("window_hits", [])),
            "candidate_round_count": len(enriched_shortlist),
            "stage":                 "event_windows_shortlist",
        },
        "logic":          logic,
        "matches":        matches,
        "selected_match": selected_match,
    })


async def _compute_round_analysis_payload(
    demo_id: str,
    round_num: int,
    logic: str,
    match_record: dict,
) -> dict:
    from backend.retrieval import build_pro_round_shortlist
    from backend.round_analysis import analyze_shortlisted_rounds

    shortlist_result = await build_pro_round_shortlist(demo_id, round_num)
    shortlist = shortlist_result.get("shortlist", [])

    user_artifact_coro = _load_round_artifact(match_record, round_num)
    candidate_records, user_artifact = await asyncio.gather(
        asyncio.gather(*[db.get_match_source_record(c["source_match_id"]) for c in shortlist]),
        user_artifact_coro,
    )

    valid = [
        (idx + 1, candidate, record)
        for idx, (candidate, record) in enumerate(zip(shortlist, candidate_records))
        if record is not None and record.get("parquet_dir")
    ]

    pro_artifacts = await asyncio.gather(
        *[_load_round_artifact(record, int(candidate["round_num"]))
          for _, candidate, record in valid]
    )

    enriched_shortlist: list[dict] = []
    for (idx, candidate, candidate_record), pro_artifact in zip(valid, pro_artifacts):
        enriched_shortlist.append({
            "source_match_id":        candidate["source_match_id"],
            "round_num":              candidate["round_num"],
            "map_name":               candidate_record.get("map_name"),
            "event_name":             candidate_record.get("event_name"),
            "team1_name":             candidate_record.get("team1_name"),
            "team2_name":             candidate_record.get("team2_name"),
            "team_ct":                candidate_record.get("team_ct"),
            "team_t":                 candidate_record.get("team_t"),
            "match_date":             (
                candidate_record["match_date"].isoformat()
                if candidate_record.get("match_date") else None
            ),
            "score":                  candidate["score"],
            "best_window_score":      candidate["best_window_score"],
            "coverage":               candidate["coverage"],
            "supporting_window_hits": candidate["supporting_window_hits"],
            "matched_query_windows":  candidate["matched_query_windows"],
            "query_anchor_kinds":     candidate.get("query_anchor_kinds", []),
            "shortlist_rank":         idx,
            "top_window":             dict(candidate["top_window"]),
            "window_hits":            candidate.get("window_hits", []),
            "artifact":               pro_artifact,
        })

    deep_analysis = analyze_shortlisted_rounds(
        query=shortlist_result.get("query"),
        user_artifact=user_artifact,
        candidates=enriched_shortlist,
        logic=logic,
    )
    shortlist_without_artifacts = [
        {k: v for k, v in c.items() if k != "artifact"}
        for c in enriched_shortlist
    ]
    return _build_round_analysis_payload(
        logic=logic,
        shortlist_result=shortlist_result,
        enriched_shortlist=shortlist_without_artifacts,
        deep_analysis=deep_analysis,
    )


async def compute_and_cache_round(
    demo_id: str,
    round_num: int,
    logic: str,
    match_record: dict,
) -> dict:
    """Compute round analysis and persist as done. Raises on error — caller stores the error."""
    cache_key = f"{demo_id}:{round_num}:{logic}:{PRO_CORPUS_VERSION}:{MATCHER_VERSION}"
    result_payload = await _compute_round_analysis_payload(demo_id, round_num, logic, match_record)
    await db.upsert_round_analysis_result(
        cache_key=cache_key,
        demo_id=demo_id,
        round_num=round_num,
        logic=logic,
        matcher_version=MATCHER_VERSION,
        pro_corpus_version=PRO_CORPUS_VERSION,
        status="done",
        result_payload=result_payload,
    )
    return result_payload
