"""Round-level artifact matching logic used by round analysis caching.

Phase 6 ports the "stronger" round matcher into library functions that consume
prebuilt round artifacts and a Phase 5 shortlist, rather than doing replay
discovery inline in the request path.
"""
from __future__ import annotations

import math
import itertools
from typing import Any


_DEFAULT_SAMPLE_STEP_S = 2.0
_NAV_LOGIC = "nav"
_ORIGINAL_LOGIC = "original"
_BOTH_LOGIC = "both"


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _round2(value: float) -> float:
    return round(float(value), 4)


def _round_seconds(value: float) -> float:
    return round(float(value), 2)


def _relative_seconds(artifact: dict, tick: int | None) -> float | None:
    if tick is None:
        return None
    origin = int(artifact.get("timing", {}).get("timeline_origin_tick", 0) or 0)
    tick_rate = float(artifact.get("timing", {}).get("tick_rate", 64) or 64)
    return (int(tick) - origin) / tick_rate


def _round_duration_s(artifact: dict) -> float:
    timing = artifact.get("timing", {})
    origin = int(timing.get("timeline_origin_tick", 0) or 0)
    end_tick = int(timing.get("round_end_tick", origin) or origin)
    tick_rate = float(timing.get("tick_rate", 64) or 64)
    return max(0.0, (end_tick - origin) / tick_rate)


def _freeze_end_tick(artifact: dict) -> int:
    return int(artifact.get("timing", {}).get("timeline_origin_tick", 0) or 0)


def _tick_rate(artifact: dict) -> float:
    return float(artifact.get("timing", {}).get("tick_rate", 64) or 64)


def _coerce_seconds(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _window_relative_time(window: dict | None) -> float | None:
    if not window:
        return None
    for key in (
        "time_since_freeze_end_s",
        "window_start_sec_from_freeze",
        "match_window_start_sec_from_freeze",
        "user_window_start_sec_from_freeze",
    ):
        if window.get(key) is not None:
            return _coerce_seconds(window.get(key))
    return None


def _pro_time_offset_s(query: dict | None, candidate: dict | None) -> float:
    """Offset to apply to user elapsed seconds to get pro elapsed seconds."""
    user_time = _window_relative_time(query)
    pro_time = _window_relative_time((candidate or {}).get("top_window"))
    if user_time is None or pro_time is None:
        return 0.0
    return _round_seconds(pro_time - user_time)


def _divergence_end_s(
    *,
    user_artifact: dict,
    start_s: float,
    break_event: dict | None,
) -> float:
    round_end = _round_duration_s(user_artifact)
    event_time = _coerce_seconds((break_event or {}).get("user_time_s"), start_s)
    end_s = max(start_s, event_time)
    if end_s <= start_s:
        end_s = start_s + 4.0
    if round_end > 0:
        end_s = min(end_s, round_end)
    return _round_seconds(max(start_s, end_s))


def _attach_timeline_contract(
    result: dict,
    *,
    user_artifact: dict,
    pro_artifact: dict,
    pro_time_offset_s: float,
) -> dict:
    """Attach explicit freeze-end-relative sync fields to one logic result."""
    payload = dict(result)
    divergence = dict(payload.get("divergence") or {})
    start_s = _coerce_seconds(divergence.get("start_s"))
    divergence["start_s"] = _round_seconds(start_s)
    divergence["end_s"] = _round_seconds(
        _coerce_seconds(
            divergence.get("end_s"),
            _divergence_end_s(
                user_artifact=user_artifact,
                start_s=start_s,
                break_event=payload.get("break_event"),
            ),
        )
    )
    payload["divergence"] = divergence

    shared_prefix = dict(payload.get("shared_prefix") or {})
    shared_prefix["duration_s"] = _round_seconds(_coerce_seconds(shared_prefix.get("duration_s")))
    shared_prefix["end_s"] = shared_prefix["duration_s"]
    payload["shared_prefix"] = shared_prefix

    sync = {
        "time_base": "freeze_end_relative_seconds",
        "user_freeze_end_tick": _freeze_end_tick(user_artifact),
        "pro_freeze_end_tick": _freeze_end_tick(pro_artifact),
        "user_tick_rate": _tick_rate(user_artifact),
        "pro_tick_rate": _tick_rate(pro_artifact),
        "pro_time_offset_s": _round_seconds(pro_time_offset_s),
        "shared_prefix_end_sec": shared_prefix["duration_s"],
        "divergence_start_sec": divergence["start_s"],
        "divergence_end_sec": divergence["end_s"],
    }
    payload["timeline_sync"] = sync
    payload["pro_time_offset_s"] = sync["pro_time_offset_s"]
    payload["divergence_start_sec"] = sync["divergence_start_sec"]
    payload["divergence_end_sec"] = sync["divergence_end_sec"]
    return payload


def _safe_divide(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _sample_times(max_time_s: float, step_s: float = _DEFAULT_SAMPLE_STEP_S) -> list[float]:
    if max_time_s <= 0:
        return [0.0]
    samples: list[float] = []
    current = 0.0
    while current < max_time_s:
        samples.append(_round_seconds(current))
        current += step_s
    if not samples or samples[-1] < max_time_s:
        samples.append(_round_seconds(max_time_s))
    return samples


def _jaccard_similarity(left: list[str], right: list[str]) -> float:
    left_set = {item for item in left if item}
    right_set = {item for item in right if item}
    if not left_set and not right_set:
        return 1.0
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _numeric_similarity(left: float | int | None, right: float | int | None, *, scale: float) -> float:
    if left is None or right is None:
        return 0.0
    return _clamp(1.0 - (abs(float(left) - float(right)) / max(scale, 1.0)))


def _dict_overlap_similarity(left: dict | None, right: dict | None) -> float:
    left = left or {}
    right = right or {}
    keys = set(left) | set(right)
    if not keys:
        return 1.0
    numerator = 0.0
    denominator = 0.0
    for key in keys:
        lv = float(left.get(key, 0) or 0)
        rv = float(right.get(key, 0) or 0)
        numerator += min(lv, rv)
        denominator += max(lv, rv)
    return _safe_divide(numerator, denominator)


def _path_similarity(left: list[list[float]], right: list[list[float]]) -> float:
    if not left or not right:
        return 0.0
    pair_count = min(len(left), len(right))
    if pair_count == 0:
        return 0.0
    distances = [
        math.dist((left[idx][0], left[idx][1]), (right[idx][0], right[idx][1]))
        for idx in range(pair_count)
    ]
    mean_distance = sum(distances) / pair_count
    return 1.0 / (1.0 + mean_distance / 450.0)


def _lcs_similarity(left: list[str], right: list[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0

    rows = len(left) + 1
    cols = len(right) + 1
    dp = [[0] * cols for _ in range(rows)]
    for i, left_item in enumerate(left, start=1):
        for j, right_item in enumerate(right, start=1):
            if left_item == right_item:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
    return _safe_divide(dp[-1][-1], max(len(left), len(right)))


def _dominant_segments(artifact: dict, side: str) -> list[dict]:
    timing = artifact.get("timing", {})
    origin = int(timing.get("timeline_origin_tick", 0) or 0)
    end_tick = int(timing.get("round_end_tick", origin) or origin)
    trace = artifact.get("teams", {}).get(side, {}).get("nav_trace", {})
    segments = trace.get("dominant_place_segments", [])

    clipped: list[dict] = []
    for segment in segments:
        start_tick = max(int(segment.get("start_tick", origin) or origin), origin)
        segment_end = min(int(segment.get("end_tick", end_tick) or end_tick), end_tick)
        if segment_end < origin or segment_end < start_tick:
            continue
        clipped.append({
            **segment,
            "start_tick": start_tick,
            "end_tick": segment_end,
            "start_s": _round_seconds(_relative_seconds(artifact, start_tick) or 0.0),
            "end_s": _round_seconds(_relative_seconds(artifact, segment_end) or 0.0),
        })
    return clipped


def _centroid_trace(artifact: dict, side: str) -> list[dict]:
    timing = artifact.get("timing", {})
    origin = int(timing.get("timeline_origin_tick", 0) or 0)
    trace = artifact.get("teams", {}).get(side, {}).get("nav_trace", {}).get("centroid_trace", [])
    payload: list[dict] = []
    for point in trace:
        tick = int(point.get("tick", origin) or origin)
        if tick < origin:
            continue
        payload.append({
            "time_s": _round_seconds(_relative_seconds(artifact, tick) or 0.0),
            "x": float(point.get("x", 0.0) or 0.0),
            "y": float(point.get("y", 0.0) or 0.0),
            "alive_players": int(point.get("alive_players", 0) or 0),
        })
    return payload


def _windows(artifact: dict, *, side: str | None = None) -> list[dict]:
    if side is None:
        windows = artifact.get("windows", [])
    else:
        windows = artifact.get("teams", {}).get(side, {}).get("windows", [])

    payload: list[dict] = []
    for window in windows:
        if window.get("queryable", True) is False:
            continue
        anchor_tick = window.get("anchor_tick")
        time_s = window.get("time_since_freeze_end_s")
        if time_s is None:
            time_s = _relative_seconds(artifact, anchor_tick)
        payload.append({
            **window,
            "time_s": _round_seconds(time_s or 0.0),
        })
    return sorted(payload, key=lambda row: row["time_s"])


def _find_segment_at_time(segments: list[dict], time_s: float) -> dict | None:
    for segment in segments:
        if segment["start_s"] <= time_s <= segment["end_s"]:
            return segment
    return None


def _find_window_at_time(windows: list[dict], time_s: float, max_delta_s: float = 3.5) -> dict | None:
    if not windows:
        return None
    best = min(windows, key=lambda row: abs(float(row["time_s"]) - time_s))
    if abs(float(best["time_s"]) - time_s) > max_delta_s:
        return None
    return best


def _place_timeline(artifact: dict, side: str, *, step_s: float = _DEFAULT_SAMPLE_STEP_S) -> list[dict]:
    segments = _dominant_segments(artifact, side)
    if not segments:
        return []
    max_time_s = min(
        _round_duration_s(artifact),
        max(float(segment["end_s"]) for segment in segments),
    )
    payload: list[dict] = []
    for time_s in _sample_times(max_time_s, step_s=step_s):
        segment = _find_segment_at_time(segments, time_s)
        if segment is None:
            continue
        payload.append({
            "time_s": _round_seconds(time_s),
            "place": segment.get("place") or "unknown",
            "alive_players": int(segment.get("alive_players", 0) or 0),
        })
    return payload


def _alive_timeline(artifact: dict, side: str, *, step_s: float = _DEFAULT_SAMPLE_STEP_S) -> list[int]:
    timeline = _place_timeline(artifact, side, step_s=step_s)
    return [int(point["alive_players"]) for point in timeline]


def _event_timing_vector(artifact: dict) -> dict[str, Any]:
    events = artifact.get("events", {})
    death_times = [
        _round_seconds(_relative_seconds(artifact, int(tick)) or 0.0)
        for tick in list(events.get("death_ticks") or [])[:4]
    ]
    return {
        "first_shot_s": _relative_seconds(artifact, events.get("first_shot_tick")),
        "first_utility_s": _relative_seconds(artifact, events.get("first_utility_tick")),
        "bomb_plant_s": _relative_seconds(artifact, events.get("bomb_plant_tick")),
        "death_times_s": death_times,
    }


def _event_timing_similarity(user_artifact: dict, pro_artifact: dict) -> float:
    left = _event_timing_vector(user_artifact)
    right = _event_timing_vector(pro_artifact)
    scores = [
        _numeric_similarity(left.get("first_shot_s"), right.get("first_shot_s"), scale=12.0),
        _numeric_similarity(left.get("first_utility_s"), right.get("first_utility_s"), scale=12.0),
        _numeric_similarity(left.get("bomb_plant_s"), right.get("bomb_plant_s"), scale=10.0),
    ]
    left_deaths = left.get("death_times_s", [])
    right_deaths = right.get("death_times_s", [])
    if left_deaths or right_deaths:
        pair_count = min(len(left_deaths), len(right_deaths))
        if pair_count > 0:
            scores.append(
                sum(
                    _numeric_similarity(left_deaths[idx], right_deaths[idx], scale=7.0)
                    for idx in range(pair_count)
                ) / pair_count
            )
        else:
            scores.append(0.0)
    filtered = [score for score in scores if score is not None]
    return sum(filtered) / len(filtered) if filtered else 0.0


def _shared_prefix(user_timeline: list[dict], pro_timeline: list[dict], *, step_s: float = _DEFAULT_SAMPLE_STEP_S) -> dict:
    if not user_timeline or not pro_timeline:
        return {
            "duration_s": 0.0,
            "ratio": 0.0,
            "user_place": None,
            "pro_place": None,
        }

    pair_count = min(len(user_timeline), len(pro_timeline))
    duration_s = 0.0
    last_user_place = user_timeline[0]["place"]
    last_pro_place = pro_timeline[0]["place"]

    for idx in range(pair_count):
        user_point = user_timeline[idx]
        pro_point = pro_timeline[idx]
        last_user_place = user_point["place"]
        last_pro_place = pro_point["place"]
        if user_point["place"] != pro_point["place"]:
            break
        duration_s = user_point["time_s"] + step_s

    comparable_duration = max(
        user_timeline[min(pair_count - 1, len(user_timeline) - 1)]["time_s"] if user_timeline else 0.0,
        pro_timeline[min(pair_count - 1, len(pro_timeline) - 1)]["time_s"] if pro_timeline else 0.0,
        step_s,
    )
    return {
        "duration_s": _round_seconds(duration_s),
        "ratio": _clamp(_safe_divide(duration_s, comparable_duration)),
        "user_place": last_user_place,
        "pro_place": last_pro_place,
    }


def _alive_similarity(user_artifact: dict, pro_artifact: dict, side: str) -> float:
    left = _alive_timeline(user_artifact, side)
    right = _alive_timeline(pro_artifact, side)
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    pair_count = min(len(left), len(right))
    return sum(
        _numeric_similarity(left[idx], right[idx], scale=5.0)
        for idx in range(pair_count)
    ) / pair_count


def _centroid_similarity(user_artifact: dict, pro_artifact: dict, side: str) -> float:
    left = _centroid_trace(user_artifact, side)
    right = _centroid_trace(pro_artifact, side)
    return _path_similarity(
        [[point["x"], point["y"]] for point in left],
        [[point["x"], point["y"]] for point in right],
    )


def _route_similarity(user_artifact: dict, pro_artifact: dict, side: str) -> tuple[float, dict]:
    user_timeline = _place_timeline(user_artifact, side)
    pro_timeline = _place_timeline(pro_artifact, side)
    if not user_timeline or not pro_timeline:
        return 0.0, {
            "duration_s": 0.0,
            "ratio": 0.0,
            "user_place": None,
            "pro_place": None,
        }

    shared = _shared_prefix(user_timeline, pro_timeline)
    user_places = [point["place"] for point in user_timeline]
    pro_places = [point["place"] for point in pro_timeline]
    lcs_score = _lcs_similarity(user_places, pro_places)
    set_score = _jaccard_similarity(user_places, pro_places)
    route_score = (0.45 * shared["ratio"]) + (0.35 * lcs_score) + (0.20 * set_score)
    return route_score, shared


def _infer_query_side(query: dict | None, user_artifact: dict) -> str:
    query = query or {}
    side = query.get("side_to_query")
    if side in {"ct", "t"}:
        return side

    counts: dict[str, int] = {"ct": 0, "t": 0}
    for window in user_artifact.get("windows", []):
        candidate = window.get("side_to_query")
        if candidate in counts:
            counts[candidate] += 1
    if counts["t"] >= counts["ct"]:
        return "t"
    return "ct"


def _next_event_after(artifact: dict, time_s: float) -> tuple[str, float | None]:
    events = _event_timing_vector(artifact)
    candidates: list[tuple[str, float]] = []
    for label, value in (
        ("first_shot", events.get("first_shot_s")),
        ("first_utility", events.get("first_utility_s")),
        ("bomb_plant", events.get("bomb_plant_s")),
    ):
        if value is not None and value >= time_s:
            candidates.append((label, float(value)))
    for death_time in events.get("death_times_s", []):
        if death_time >= time_s:
            candidates.append(("death", float(death_time)))
    if not candidates:
        return "round_end", None
    return min(candidates, key=lambda item: item[1])


def _classify_break_event(
    *,
    divergence_s: float,
    user_artifact: dict,
    pro_artifact: dict,
    query_side: str,
    mode: str,
) -> dict:
    user_segments = _dominant_segments(user_artifact, query_side)
    pro_segments = _dominant_segments(pro_artifact, query_side)
    user_place = (_find_segment_at_time(user_segments, divergence_s) or {}).get("place")
    pro_place = (_find_segment_at_time(pro_segments, divergence_s) or {}).get("place")

    user_window = _find_window_at_time(_windows(user_artifact), divergence_s)
    pro_window = _find_window_at_time(_windows(pro_artifact), divergence_s)
    user_next_label, user_next_time = _next_event_after(user_artifact, divergence_s)
    pro_next_label, pro_next_time = _next_event_after(pro_artifact, divergence_s)

    event_type = "tempo_split"
    label = "Tempo split"
    reason = "The rounds drift apart in timing before a single stronger break signal appears."

    if user_place and pro_place and user_place != pro_place:
        if user_window and pro_window and user_window.get("site") and pro_window.get("site") and user_window.get("site") != pro_window.get("site"):
            event_type = "site_commit"
            label = "Site commit"
            reason = f"The route diverges into different site commitments: user {user_window.get('site')} versus pro {pro_window.get('site')}."
        else:
            event_type = "route_split"
            label = "Route split"
            reason = f"The {query_side.upper()} route stops matching at {user_place} versus {pro_place}."
    elif user_window and pro_window and (
        int(user_window.get("alive_ct", 0) or 0) != int(pro_window.get("alive_ct", 0) or 0)
        or int(user_window.get("alive_t", 0) or 0) != int(pro_window.get("alive_t", 0) or 0)
    ):
        event_type = "fight_outcome"
        label = "Fight outcome"
        reason = "The alive-state changes no longer line up, suggesting a different duel or trade result."
    elif user_next_label != pro_next_label:
        if "utility" in {user_next_label, pro_next_label}:
            event_type = "utility_tempo"
            label = "Utility tempo"
            reason = "The next meaningful utility timing differs from the pro round."
        elif "death" in {user_next_label, pro_next_label}:
            event_type = "fight_outcome"
            label = "Fight outcome"
            reason = "The next frag timing differs enough to bend the round in a different direction."

    if mode == _ORIGINAL_LOGIC and event_type == "route_split":
        event_type = "phase_shift"
        label = "Phase shift"
        reason = "The round sequence leaves the same phase/site shape even before the route fully separates."

    return {
        "type": event_type,
        "label": label,
        "reason": reason,
        "user_time_s": _round_seconds(user_next_time if user_next_time is not None else divergence_s),
        "pro_time_s": _round_seconds(pro_next_time if pro_next_time is not None else divergence_s),
        "user_place": user_place,
        "pro_place": pro_place,
    }


def _nav_summary(query_side: str, shared: dict, break_event: dict) -> str:
    if shared["duration_s"] > 0:
        return (
            f"Matches the {query_side.upper()} route for about {shared['duration_s']:.1f}s "
            f"before a {break_event['label'].lower()}."
        )
    return f"Only lightly overlaps the {query_side.upper()} route before a {break_event['label'].lower()}."


def _extract_player_seqs(artifact: dict, side: str) -> dict[str, dict]:
    """Return player_nav_sequences for one side from a round artifact."""
    return (
        artifact.get("teams", {})
        .get(side, {})
        .get("nav_trace", {})
        .get("player_nav_sequences", {})
    )


def _nav_summary_real(query_side: str, best_pair: dict) -> str:
    dur = float(best_pair.get("prefix_duration_sec", 0.0))
    label = str(best_pair.get("break_event_label", "divergence"))
    if dur > 0:
        return f"Matches the {query_side.upper()} route for about {dur:.1f}s before a {label}."
    return f"Only lightly overlaps the {query_side.upper()} route before a {label}."


def match_nav_rounds(
    *,
    query: dict | None,
    user_artifact: dict,
    pro_artifact: dict,
) -> dict:
    """Match using per-player nav-mesh area sequences (ported from groundup)."""
    from backend.nav_matching import score_player_pair

    map_name: str = user_artifact.get("map_name") or pro_artifact.get("map_name") or ""
    query_side = _infer_query_side(query, user_artifact)

    user_steamid = user_artifact.get("user_steamid")
    user_seqs = _extract_player_seqs(user_artifact, query_side)

    # Prefer user's specific player; fall back to any player on the side
    if user_steamid and user_steamid in user_seqs:
        user_candidates = {user_steamid: user_seqs[user_steamid]}
    else:
        user_candidates = user_seqs

    pro_seqs = _extract_player_seqs(pro_artifact, query_side)

    _fallback_result = {
        "logic": _NAV_LOGIC,
        "score": 0.0,
        "query_side": query_side,
        "components": {"prefix": 0.0, "coach": 0.0},
        "shared_prefix": {"duration_s": 0.0, "ratio": 0.0, "user_place": None, "pro_place": None},
        "divergence": {"start_s": 0.0, "user_place": None, "pro_place": None},
        "break_event": {
            "type": "no_nav_sequences", "label": "No nav sequences",
            "reason": "Nav sequences missing from artifact (rebuild required).",
            "user_time_s": 0.0, "pro_time_s": 0.0, "user_place": None, "pro_place": None,
        },
        "survival_gap_s": _round_seconds(_round_duration_s(user_artifact) - _round_duration_s(pro_artifact)),
        "summary": "Nav sequences not available for this artifact version.",
    }

    if not user_candidates or not pro_seqs:
        return _fallback_result

    # Try every user player × every pro player on the same side, pick best
    best: dict | None = None
    best_score = -1.0
    best_user_steamid: str | None = None
    best_pro_steamid: str | None = None
    for u_steamid, u_seq in user_candidates.items():
        for p_steamid, p_seq in pro_seqs.items():
            pair = score_player_pair(u_seq, p_seq, map_name)
            if pair is not None and float(pair["round_score"]) > best_score:
                best_score = float(pair["round_score"])
                best = pair
                best_user_steamid = str(u_steamid)
                best_pro_steamid = str(p_steamid)

    if best is None:
        return _fallback_result

    prefix_dur = float(best.get("prefix_duration_sec", 0.0))
    survival_gap = float(best.get("survival_gap_sec", 0.0))

    # Reconstruct break_event in the shape the rest of round_analysis expects
    break_event = {
        "type": best["break_event_type"],
        "label": best["break_event_label"],
        "reason": f"Break at t={best['break_time_sec']:.1f}s — {best['break_event_label']}.",
        "user_time_s": _round_seconds(best["break_time_sec"]),
        "pro_time_s": _round_seconds(best["break_time_sec"]),
        "user_place": None,
        "pro_place": None,
    }
    max_round_dur = max(
        float(user_artifact.get("timing", {}).get("round_end_tick", 0) or 0)
        - float(user_artifact.get("timing", {}).get("timeline_origin_tick", 0) or 0),
        1.0,
    ) / float(user_artifact.get("timing", {}).get("tick_rate", 64) or 64)
    shared_prefix_ratio = _clamp(prefix_dur / max(max_round_dur, 1.0))

    return {
        "logic": _NAV_LOGIC,
        "score": _round2(best_score),
        "query_side": query_side,
        "components": {
            "prefix": _round2(float(best.get("prefix_score", 0.0))),
            "coach": _round2(float(best.get("coach_value", 0.0))),
        },
        "shared_prefix": {
            "duration_s": _round_seconds(prefix_dur),
            "ratio": _round2(shared_prefix_ratio),
            "user_place": None,
            "pro_place": None,
        },
        "divergence": {
            "start_s": _round_seconds(prefix_dur),
            "user_place": None,
            "pro_place": None,
        },
        "break_event": break_event,
        "survival_gap_s": _round_seconds(survival_gap),
        "summary": _nav_summary_real(query_side, best),
        "user_focal_steamid": best_user_steamid,
        "matched_pro_steamid": best_pro_steamid,
    }


_ORIGINAL_SCALES = {
    "position": 900.0,
    "path_relative": 650.0,
    "spacing": 450.0,
    "speed": 240.0,
    "yaw_deg": 90.0,
    "path_length": 700.0,
}

_WEAPON_COMPAT = {
    ("rifle", "sniper"): 0.82,
    ("rifle", "smg"): 0.62,
    ("rifle", "shotgun"): 0.32,
    ("rifle", "heavy"): 0.28,
    ("pistol", "rifle"): 0.35,
    ("pistol", "sniper"): 0.20,
    ("pistol", "smg"): 0.48,
    ("pistol", "shotgun"): 0.28,
    ("pistol", "heavy"): 0.22,
    ("smg", "sniper"): 0.45,
    ("smg", "shotgun"): 0.40,
    ("heavy", "sniper"): 0.20,
    ("heavy", "smg"): 0.24,
    ("heavy", "shotgun"): 0.42,
    ("shotgun", "sniper"): 0.25,
}


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _point_distance(left: list[float], right: list[float]) -> float:
    return math.dist((float(left[0]), float(left[1])), (float(right[0]), float(right[1])))


def _series_distance(left: list[list[float]], right: list[list[float]]) -> float:
    n = min(len(left), len(right))
    if n == 0:
        return 0.0
    return _mean([_point_distance(left[idx], right[idx]) for idx in range(n)])


def _series_abs_gap(left: list[float], right: list[float]) -> float:
    n = min(len(left), len(right))
    if n == 0:
        return 0.0
    return _mean([abs(float(left[idx]) - float(right[idx])) for idx in range(n)])


def _angle_gap(left: list[float], right: list[float]) -> float:
    n = min(len(left), len(right))
    if n == 0:
        return 0.0
    gaps: list[float] = []
    for idx in range(n):
        diff = abs(float(left[idx]) - float(right[idx])) % 360.0
        gaps.append(min(diff, 360.0 - diff))
    return _mean(gaps)


def _track_relative_path(track: dict) -> list[list[float]]:
    positions = [[float(p[0]), float(p[1])] for p in track.get("positions", [])]
    if not positions:
        return []
    origin = positions[0]
    return [[p[0] - origin[0], p[1] - origin[1]] for p in positions]


def _track_components(user_track: dict, pro_track: dict) -> dict[str, float]:
    s = _ORIGINAL_SCALES
    position_gap = _series_distance(user_track.get("positions", []), pro_track.get("positions", [])) / s["position"]
    path_gap = _series_distance(_track_relative_path(user_track), _track_relative_path(pro_track)) / s["path_relative"]
    spacing_gap = _series_distance(user_track.get("relative_to_team", []), pro_track.get("relative_to_team", [])) / s["spacing"]
    speed_gap = _series_abs_gap(user_track.get("speeds", []), pro_track.get("speeds", [])) / s["speed"]
    yaw_gap = _angle_gap(user_track.get("yaws", []), pro_track.get("yaws", [])) / s["yaw_deg"]
    path_length_gap = abs(float(user_track.get("path_length", 0.0) or 0.0) - float(pro_track.get("path_length", 0.0) or 0.0)) / s["path_length"]
    total = (
        0.25 * position_gap
        + 0.25 * path_gap
        + 0.20 * spacing_gap
        + 0.15 * speed_gap
        + 0.10 * yaw_gap
        + 0.05 * path_length_gap
    )
    return {
        "position_gap": position_gap,
        "path_gap": path_gap,
        "spacing_gap": spacing_gap,
        "speed_gap": speed_gap,
        "yaw_gap": yaw_gap,
        "path_length_gap": path_length_gap,
        "total": total,
    }


def _track_assignment_cost(user_track: dict, pro_track: dict) -> float:
    s = _ORIGINAL_SCALES
    position_cost = _series_distance(user_track.get("positions", []), pro_track.get("positions", [])) / s["position"]
    path_cost = _series_distance(_track_relative_path(user_track), _track_relative_path(pro_track)) / s["path_relative"]
    spacing_cost = _series_distance(user_track.get("relative_to_team", []), pro_track.get("relative_to_team", [])) / s["spacing"]
    speed_cost = _series_abs_gap(user_track.get("speeds", []), pro_track.get("speeds", [])) / s["speed"]
    return 0.35 * position_cost + 0.35 * path_cost + 0.20 * spacing_cost + 0.10 * speed_cost


def _best_track_assignment(user_tracks: dict[str, dict], pro_tracks: dict[str, dict]) -> tuple[list[dict], float] | None:
    if len(user_tracks) != len(pro_tracks):
        return None
    if not user_tracks:
        return [], 0.0

    user_ids = list(user_tracks)
    pro_ids = list(pro_tracks)
    best_pairs: list[dict] | None = None
    best_cost = float("inf")
    for permutation in itertools.permutations(pro_ids, len(user_ids)):
        pairs: list[dict] = []
        costs: list[float] = []
        for user_id, pro_id in zip(user_ids, permutation):
            cost = _track_assignment_cost(user_tracks[user_id], pro_tracks[pro_id])
            costs.append(cost)
            pairs.append({
                "user_steamid": user_id,
                "user_name": user_tracks[user_id].get("name", ""),
                "pro_steamid": pro_id,
                "pro_name": pro_tracks[pro_id].get("name", ""),
                "side": user_tracks[user_id].get("side"),
                "alignment_cost": cost,
            })
        mean_cost = _mean(costs)
        if mean_cost < best_cost:
            best_cost = mean_cost
            best_pairs = pairs
    return best_pairs or [], best_cost


def _player_side_for_original_window(window: dict, steamid: str) -> str | None:
    if steamid in {str(value) for value in window.get("t_alive_steamids", [])}:
        return "t"
    if steamid in {str(value) for value in window.get("ct_alive_steamids", [])}:
        return "ct"
    return None


def _original_team_catalog(artifact: dict) -> list[dict]:
    return list(artifact.get("team_window_catalog") or [])


def _count_overlap_similarity(left: dict | None, right: dict | None) -> float:
    left = left or {}
    right = right or {}
    keys = {key for key in (set(left) | set(right)) if key is not None}
    if not keys:
        return 0.0
    numerator = sum(min(float(left.get(key, 0) or 0), float(right.get(key, 0) or 0)) for key in keys)
    denominator = sum(max(float(left.get(key, 0) or 0), float(right.get(key, 0) or 0)) for key in keys)
    return _safe_divide(numerator, denominator)


def _weapon_compat(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.2
    if left == right:
        return 1.0
    return _WEAPON_COMPAT.get((left, right), _WEAPON_COMPAT.get((right, left), 0.2))


def _context_sample_distance(user_window: dict, pro_window: dict) -> float:
    user_samples = list(user_window.get("context_samples") or [])
    pro_samples = list(pro_window.get("context_samples") or [])
    n = min(len(user_samples), len(pro_samples))
    if n == 0:
        return 1.0
    gaps: list[float] = []
    for idx in range(n):
        user_sample = user_samples[idx]
        pro_sample = pro_samples[idx]
        for side in ("t", "ct"):
            u_side = user_sample.get(side, {})
            p_side = pro_sample.get(side, {})
            gaps.extend([
                _numeric_similarity(u_side.get("centroid_x"), p_side.get("centroid_x"), scale=900.0),
                _numeric_similarity(u_side.get("centroid_y"), p_side.get("centroid_y"), scale=900.0),
                _numeric_similarity(u_side.get("spread"), p_side.get("spread"), scale=600.0),
                _numeric_similarity(u_side.get("pairwise"), p_side.get("pairwise"), scale=650.0),
                _numeric_similarity(u_side.get("mean_speed"), p_side.get("mean_speed"), scale=300.0),
            ])
        gaps.append(_numeric_similarity(user_sample.get("team_gap"), pro_sample.get("team_gap"), scale=900.0))
    return 1.0 - _mean(gaps)


def _score_original_context(user_window: dict, pro_window: dict) -> dict:
    geom_distance = _context_sample_distance(user_window, pro_window)
    time_penalty = 0.08 * abs(
        float(user_window.get("window_start_sec_from_freeze", 0.0) or 0.0)
        - float(pro_window.get("window_start_sec_from_freeze", 0.0) or 0.0)
    )
    place_sim = 0.5 * (
        _count_overlap_similarity(user_window.get("t_place_profile"), pro_window.get("t_place_profile"))
        + _count_overlap_similarity(user_window.get("ct_place_profile"), pro_window.get("ct_place_profile"))
    )
    weapon_profile_sim = 0.5 * (
        _count_overlap_similarity(user_window.get("t_weapon_profile"), pro_window.get("t_weapon_profile"))
        + _count_overlap_similarity(user_window.get("ct_weapon_profile"), pro_window.get("ct_weapon_profile"))
    )
    weapon_focus_sim = 0.5 * (
        _weapon_compat(user_window.get("t_focus_weapon_family"), pro_window.get("t_focus_weapon_family"))
        + _weapon_compat(user_window.get("ct_focus_weapon_family"), pro_window.get("ct_focus_weapon_family"))
    )
    weapon_sim = 0.5 * (weapon_profile_sim + weapon_focus_sim)
    plant_penalty = 0.0
    if user_window.get("time_since_plant_s") is not None and pro_window.get("time_since_plant_s") is not None:
        plant_penalty = 0.05 * abs(float(user_window["time_since_plant_s"]) - float(pro_window["time_since_plant_s"]))

    total_distance = (
        geom_distance
        + time_penalty
        + 0.6 * (1.0 - place_sim)
        + 0.4 * (1.0 - weapon_sim)
        + plant_penalty
    )
    return {
        "context_distance": total_distance,
        "context_match": 1.0 / (1.0 + total_distance),
        "context_feature_distance": geom_distance,
        "context_place_penalty": 0.6 * (1.0 - place_sim),
        "context_weapon_penalty": 0.4 * (1.0 - weapon_sim),
        "context_plant_penalty": plant_penalty,
    }


def _retrieve_original_team_windows(user_window: dict, pro_windows: list[dict], *, top_k: int = 20) -> list[dict]:
    candidates = [
        window for window in pro_windows
        if window.get("map_name") == user_window.get("map_name")
        and int(window.get("alive_t", -1) or -1) == int(user_window.get("alive_t", -2) or -2)
        and int(window.get("alive_ct", -1) or -1) == int(user_window.get("alive_ct", -2) or -2)
        and bool(window.get("planted")) == bool(user_window.get("planted"))
    ]
    if user_window.get("planted") and user_window.get("site"):
        same_site = [window for window in candidates if window.get("site") == user_window.get("site")]
        if same_site:
            candidates = same_site
    same_phase = [window for window in candidates if window.get("phase") == user_window.get("phase")]
    if len(same_phase) >= min(top_k, 5):
        candidates = same_phase

    scored: list[dict] = []
    for candidate in candidates:
        scores = _score_original_context(user_window, candidate)
        scored.append({**candidate, **scores})
    scored.sort(key=lambda row: float(row["context_distance"]))
    return scored[:top_k]


def _score_original_assignments(user_window: dict, pro_window: dict, *, user_steamid: str) -> list[dict]:
    user_side = _player_side_for_original_window(user_window, user_steamid)
    if user_side is None:
        return []
    other_side = "ct" if user_side == "t" else "t"
    if int(user_window.get("alive_t", -1) or -1) != int(pro_window.get("alive_t", -2) or -2):
        return []
    if int(user_window.get("alive_ct", -1) or -1) != int(pro_window.get("alive_ct", -2) or -2):
        return []

    user_tracks_by_side = user_window.get("tracks_by_side") or {}
    pro_tracks_by_side = pro_window.get("tracks_by_side") or {}
    user_side_tracks = {str(k): v for k, v in (user_tracks_by_side.get(user_side) or {}).items()}
    pro_side_tracks = {str(k): v for k, v in (pro_tracks_by_side.get(user_side) or {}).items()}
    user_other_tracks = {str(k): v for k, v in (user_tracks_by_side.get(other_side) or {}).items()}
    pro_other_tracks = {str(k): v for k, v in (pro_tracks_by_side.get(other_side) or {}).items()}
    if user_steamid not in user_side_tracks:
        return []

    enemy_assignment = _best_track_assignment(user_other_tracks, pro_other_tracks)
    if enemy_assignment is None:
        return []
    enemy_pairs, _ = enemy_assignment

    results: list[dict] = []
    context_match = float(pro_window.get("context_match", 0.0) or 0.0)
    for candidate_pro_steamid, candidate_pro_track in pro_side_tracks.items():
        teammate_user_tracks = {
            sid: track for sid, track in user_side_tracks.items()
            if sid != user_steamid
        }
        teammate_pro_tracks = {
            sid: track for sid, track in pro_side_tracks.items()
            if sid != candidate_pro_steamid
        }
        teammate_assignment = _best_track_assignment(teammate_user_tracks, teammate_pro_tracks)
        if teammate_assignment is None:
            continue
        teammate_pairs, _ = teammate_assignment
        non_user_pairs = teammate_pairs + enemy_pairs
        non_user_cost = _mean([float(pair["alignment_cost"]) for pair in non_user_pairs])
        non_user_alignment = 1.0 / (1.0 + non_user_cost)

        user_components = _track_components(user_side_tracks[user_steamid], candidate_pro_track)
        user_alignment = 1.0 / (1.0 + float(user_components["total"]))
        user_pro_difference = float(user_components["total"]) / (1.0 + float(user_components["total"]))
        n_non_user = len(non_user_pairs)
        all_player_alignment = (
            (non_user_alignment * n_non_user + user_alignment) / (n_non_user + 1)
            if n_non_user > 0
            else user_alignment
        )
        results.append({
            "user_focal_steamid": user_steamid,
            "user_round_num": int(user_window.get("round_num", 0) or 0),
            "user_window_start_tick": int(user_window["window_start_tick"]),
            "user_window_end_tick": int(user_window["window_end_tick"]),
            "user_window_start_sec_from_freeze": float(user_window.get("window_start_sec_from_freeze", 0.0) or 0.0),
            "user_window_end_sec_from_freeze": float(user_window.get("window_end_sec_from_freeze", 0.0) or 0.0),
            "user_phase": str(user_window.get("phase") or ""),
            "user_side": user_side,
            "match_demo_id": str(pro_window.get("source_match_id") or ""),
            "match_round_num": int(pro_window.get("round_num", 0) or 0),
            "match_window_start_tick": int(pro_window["window_start_tick"]),
            "match_window_end_tick": int(pro_window["window_end_tick"]),
            "match_window_start_sec_from_freeze": float(pro_window.get("window_start_sec_from_freeze", 0.0) or 0.0),
            "match_window_end_sec_from_freeze": float(pro_window.get("window_end_sec_from_freeze", 0.0) or 0.0),
            "match_phase": str(pro_window.get("phase") or ""),
            "matched_pro_steamid": candidate_pro_steamid,
            "matched_pro_player": str(candidate_pro_track.get("name") or ""),
            "context_match": context_match,
            "context_distance": float(pro_window.get("context_distance", 0.0) or 0.0),
            "non_user_alignment": non_user_alignment,
            "non_user_cost": non_user_cost,
            "user_pro_difference": user_pro_difference,
            "user_difference_cost": float(user_components["total"]),
            "user_alignment": user_alignment,
            "round_similarity": context_match * all_player_alignment,
        })
    return results


def _longest_original_streak(rows: list[dict], expected_step: int = 128) -> int:
    starts = sorted({int(row["user_window_start_tick"]) for row in rows})
    if not starts:
        return 0
    best = current = 1
    previous = starts[0]
    for value in starts[1:]:
        if value - previous == expected_step:
            current += 1
        else:
            current = 1
        best = max(best, current)
        previous = value
    return best


def _original_divergence(window_scores: list[dict]) -> dict:
    if not window_scores:
        return {
            "shared_prefix": {"duration_s": 0.0, "ratio": 0.0, "user_phase": None, "pro_phase": None},
            "divergence": {"start_s": 0.0, "user_phase": None, "pro_phase": None},
        }
    ordered = sorted(window_scores, key=lambda row: float(row["user_window_start_sec_from_freeze"]))
    shared_duration = 0.0
    last_shared = ordered[0]
    for row in ordered:
        last_shared = row
        if float(row.get("round_similarity", 0.0)) < 0.55:
            break
        shared_duration = max(shared_duration, float(row.get("user_window_end_sec_from_freeze", 0.0) or 0.0))

    divergence_row = None
    consecutive = 0
    for row in ordered:
        signal = float(row.get("user_pro_difference", 0.0)) * float(row.get("non_user_alignment", 0.0))
        if signal >= 0.15:
            consecutive += 1
            if consecutive >= 2:
                divergence_row = row
                break
        else:
            consecutive = 0
    if divergence_row is None:
        divergence_row = last_shared

    max_time = max(float(row.get("user_window_end_sec_from_freeze", 0.0) or 0.0) for row in ordered)
    return {
        "shared_prefix": {
            "duration_s": _round_seconds(shared_duration),
            "ratio": _round2(_safe_divide(shared_duration, max(max_time, 1.0))),
            "user_phase": str(last_shared.get("user_phase") or "") or None,
            "pro_phase": str(last_shared.get("match_phase") or "") or None,
        },
        "divergence": {
            "start_s": _round_seconds(float(divergence_row.get("user_window_start_sec_from_freeze", shared_duration) or shared_duration)),
            "user_phase": str(divergence_row.get("user_phase") or "") or None,
            "pro_phase": str(divergence_row.get("match_phase") or "") or None,
        },
    }


def _candidate_user_ids(user_windows: list[dict], query_side: str, user_steamid: str | None) -> list[str]:
    if user_steamid:
        return [str(user_steamid)]
    key = f"{query_side}_alive_steamids"
    ids = sorted({str(value) for window in user_windows for value in window.get(key, [])})
    return ids


def _select_user_original_windows(windows: list[dict], user_steamid: str, *, min_windows: int = 2) -> list[dict]:
    with_user = [
        window for window in windows
        if _player_side_for_original_window(window, user_steamid) is not None
    ]
    if not with_user:
        return []
    for cutoff in (20.0, 10.0, 5.0, 0.0):
        selected = [
            window for window in with_user
            if float(window.get("window_start_sec_from_freeze", 0.0) or 0.0) >= cutoff
        ]
        if len(selected) >= min_windows:
            return selected
    return with_user


def _match_original_team_windows(query: dict | None, user_artifact: dict, pro_artifact: dict) -> dict | None:
    user_windows = _original_team_catalog(user_artifact)
    pro_windows = _original_team_catalog(pro_artifact)
    if not user_windows or not pro_windows:
        return None

    query_side = _infer_query_side(query, user_artifact)
    user_steamid = user_artifact.get("user_steamid")
    all_scored: list[dict] = []
    total_windows_by_user: dict[str, int] = {}

    for focal_user in _candidate_user_ids(user_windows, query_side, str(user_steamid) if user_steamid else None):
        selected_user_windows = _select_user_original_windows(user_windows, focal_user)
        if not selected_user_windows:
            continue
        total_windows_by_user[focal_user] = len(selected_user_windows)
        for user_window in selected_user_windows:
            retrieved = _retrieve_original_team_windows(user_window, pro_windows, top_k=20)
            for pro_window in retrieved:
                all_scored.extend(_score_original_assignments(user_window, pro_window, user_steamid=focal_user))

    if not all_scored:
        return None

    dedup: dict[tuple, dict] = {}
    for row in all_scored:
        key = (
            row["user_focal_steamid"],
            row["user_window_start_tick"],
            row["match_demo_id"],
            row["match_round_num"],
            row["matched_pro_steamid"],
        )
        if key not in dedup or float(row["round_similarity"]) > float(dedup[key]["round_similarity"]):
            dedup[key] = row
    scored = list(dedup.values())

    groups: dict[tuple, list[dict]] = {}
    for row in scored:
        key = (
            row["user_focal_steamid"],
            row["match_demo_id"],
            row["match_round_num"],
            row["matched_pro_steamid"],
            row["matched_pro_player"],
        )
        groups.setdefault(key, []).append(row)

    rankings: list[dict] = []
    for (focal_user, match_demo_id, match_round_num, matched_pro_steamid, matched_pro_player), rows in groups.items():
        total_user_windows = max(total_windows_by_user.get(focal_user, len(rows)), 1)
        window_count = len(rows)
        coverage = min(1.0, window_count / total_user_windows)
        longest_streak = _longest_original_streak(rows)
        mean_round_similarity = _mean([float(row["round_similarity"]) for row in rows])
        mean_non_user_alignment = _mean([float(row["non_user_alignment"]) for row in rows])
        mean_context_match = _mean([float(row["context_match"]) for row in rows])
        mean_user_pro_difference = _mean([float(row["user_pro_difference"]) for row in rows])
        round_score = (
            mean_round_similarity
            * (1.0 + 0.15 * min(max(longest_streak - 1, 0), 6))
            * (0.70 + 0.30 * coverage)
        )
        if window_count < 2:
            continue
        rankings.append({
            "user_focal_steamid": focal_user,
            "user_round_num": int(user_artifact.get("round_num", 0) or 0),
            "match_demo_id": match_demo_id,
            "match_round_num": int(match_round_num),
            "matched_pro_steamid": matched_pro_steamid,
            "matched_pro_player": matched_pro_player,
            "mean_round_similarity": mean_round_similarity,
            "mean_non_user_alignment": mean_non_user_alignment,
            "mean_context_match": mean_context_match,
            "mean_user_pro_difference": mean_user_pro_difference,
            "window_count": window_count,
            "coverage": coverage,
            "longest_streak": longest_streak,
            "round_score": round_score,
        })

    if not rankings:
        return None
    rankings.sort(key=lambda row: float(row["round_score"]), reverse=True)
    best = rankings[0]
    best_window_scores = sorted(
        [
            row for row in scored
            if row["user_focal_steamid"] == best["user_focal_steamid"]
            and row["match_demo_id"] == best["match_demo_id"]
            and int(row["match_round_num"]) == int(best["match_round_num"])
            and row["matched_pro_steamid"] == best["matched_pro_steamid"]
        ],
        key=lambda row: float(row["user_window_start_sec_from_freeze"]),
    )
    divergence_payload = _original_divergence(best_window_scores)
    divergence_s = float(divergence_payload["divergence"]["start_s"])
    break_event = _classify_break_event(
        divergence_s=divergence_s,
        user_artifact=user_artifact,
        pro_artifact=pro_artifact,
        query_side=query_side,
        mode=_ORIGINAL_LOGIC,
    )
    return {
        "logic": _ORIGINAL_LOGIC,
        "score": _round2(best["round_score"]),
        "components": {
            "window_alignment": _round2(best["mean_round_similarity"]),
            "context": _round2(best["mean_context_match"]),
            "non_user_alignment": _round2(best["mean_non_user_alignment"]),
            "user_difference": _round2(best["mean_user_pro_difference"]),
            "coverage": _round2(best["coverage"]),
            "longest_streak": float(best["longest_streak"]),
        },
        "shared_prefix": divergence_payload["shared_prefix"],
        "divergence": divergence_payload["divergence"],
        "break_event": break_event,
        "survival_gap_s": _round_seconds(_round_duration_s(user_artifact) - _round_duration_s(pro_artifact)),
        "summary": _original_summary(divergence_payload["shared_prefix"], break_event),
        "matched_pro_steamid": best["matched_pro_steamid"],
        "matched_pro_player": best["matched_pro_player"],
        "window_count": best["window_count"],
        "coverage": _round2(best["coverage"]),
        "longest_streak": best["longest_streak"],
        "all_round_rankings": rankings[:10],
        "window_scores": best_window_scores,
    }


def _window_summary_similarity(left: dict, right: dict) -> float:
    left_summary = left.get("window_summary", {})
    right_summary = right.get("window_summary", {})
    keys = (
        "shots_count",
        "smokes_count",
        "infernos_count",
        "flashes_count",
        "he_count",
        "deaths_ct",
        "deaths_t",
    )
    scores = [
        _numeric_similarity(left_summary.get(key), right_summary.get(key), scale=12.0)
        for key in keys
    ]
    return sum(scores) / len(scores) if scores else 0.0


def _overall_window_similarity(left: dict, right: dict) -> float:
    phase_score = 1.0 if (left.get("phase") and left.get("phase") == right.get("phase")) else 0.0
    primary_score = 1.0 if (
        left.get("primary_situation")
        and left.get("primary_situation") == right.get("primary_situation")
    ) else 0.0
    site_score = 1.0 if left.get("site") and left.get("site") == right.get("site") else 0.0
    tag_score = _jaccard_similarity(
        list(left.get("situation_tags") or []),
        list(right.get("situation_tags") or []),
    )
    alive_score = (
        _numeric_similarity(left.get("alive_ct"), right.get("alive_ct"), scale=5.0)
        + _numeric_similarity(left.get("alive_t"), right.get("alive_t"), scale=5.0)
    ) / 2.0
    timing_score = _numeric_similarity(left.get("time_s"), right.get("time_s"), scale=10.0)
    summary_score = _window_summary_similarity(left, right)
    return (
        (0.18 * phase_score)
        + (0.20 * primary_score)
        + (0.10 * site_score)
        + (0.12 * tag_score)
        + (0.20 * alive_score)
        + (0.10 * timing_score)
        + (0.10 * summary_score)
    )


def _round_outcome_similarity(user_artifact: dict, pro_artifact: dict) -> float:
    left_round = user_artifact.get("round", {})
    right_round = pro_artifact.get("round", {})
    scores = [
        1.0 if left_round.get("winner") == right_round.get("winner") else 0.0,
        1.0 if left_round.get("reason") == right_round.get("reason") else 0.0,
        1.0 if left_round.get("bomb_site") == right_round.get("bomb_site") else 0.0,
    ]
    return sum(scores) / len(scores)


def _original_shared_prefix(user_windows: list[dict], pro_windows: list[dict]) -> dict:
    if not user_windows or not pro_windows:
        return {"duration_s": 0.0, "ratio": 0.0, "user_window": None, "pro_window": None}

    pair_count = min(len(user_windows), len(pro_windows))
    duration_s = 0.0
    last_user = user_windows[0]
    last_pro = pro_windows[0]
    for idx in range(pair_count):
        user_window = user_windows[idx]
        pro_window = pro_windows[idx]
        last_user = user_window
        last_pro = pro_window
        if _overall_window_similarity(user_window, pro_window) < 0.55:
            break
        duration_s = max(duration_s, float(user_window["time_s"]))
    comparable = max(
        float(user_windows[min(pair_count - 1, len(user_windows) - 1)]["time_s"]) if user_windows else 0.0,
        float(pro_windows[min(pair_count - 1, len(pro_windows) - 1)]["time_s"]) if pro_windows else 0.0,
        1.0,
    )
    return {
        "duration_s": _round_seconds(duration_s),
        "ratio": _clamp(_safe_divide(duration_s, comparable)),
        "user_window": last_user,
        "pro_window": last_pro,
    }


def _original_summary(shared: dict, break_event: dict) -> str:
    if shared["duration_s"] > 0:
        return (
            f"Tracks the same round shape for about {shared['duration_s']:.1f}s "
            f"before a {break_event['label'].lower()}."
        )
    return f"The round structure diverges early with a {break_event['label'].lower()}."


def match_original_rounds(
    *,
    query: dict | None,
    user_artifact: dict,
    pro_artifact: dict,
) -> dict:
    full_match = _match_original_team_windows(query, user_artifact, pro_artifact)
    if full_match is not None:
        return full_match

    user_windows = _windows(user_artifact)
    pro_windows = _windows(pro_artifact)
    if not user_windows or not pro_windows:
        return {
            "logic": _ORIGINAL_LOGIC,
            "score": 0.0,
            "components": {
                "window_alignment": 0.0,
                "events": 0.0,
                "outcome": 0.0,
            },
            "shared_prefix": {"duration_s": 0.0, "ratio": 0.0},
            "divergence": {"start_s": 0.0},
            "break_event": {
                "type": "missing_windows",
                "label": "Missing windows",
                "reason": "One of the artifacts does not have queryable round windows.",
                "user_time_s": 0.0,
                "pro_time_s": 0.0,
                "user_place": None,
                "pro_place": None,
            },
            "survival_gap_s": _round_seconds(_round_duration_s(user_artifact) - _round_duration_s(pro_artifact)),
            "summary": "One artifact is missing usable round windows.",
        }

    pair_count = min(len(user_windows), len(pro_windows), 12)
    per_window_scores = [
        _overall_window_similarity(user_windows[idx], pro_windows[idx])
        for idx in range(pair_count)
    ]
    window_alignment = sum(per_window_scores) / pair_count if pair_count else 0.0
    event_score = _event_timing_similarity(user_artifact, pro_artifact)
    outcome_score = _round_outcome_similarity(user_artifact, pro_artifact)
    score = (0.55 * window_alignment) + (0.25 * event_score) + (0.20 * outcome_score)

    shared = _original_shared_prefix(user_windows, pro_windows)
    divergence_s = shared["duration_s"]
    query_side = _infer_query_side(None, user_artifact)
    break_event = _classify_break_event(
        divergence_s=divergence_s,
        user_artifact=user_artifact,
        pro_artifact=pro_artifact,
        query_side=query_side,
        mode=_ORIGINAL_LOGIC,
    )
    return {
        "logic": _ORIGINAL_LOGIC,
        "score": _round2(score),
        "components": {
            "window_alignment": _round2(window_alignment),
            "events": _round2(event_score),
            "outcome": _round2(outcome_score),
        },
        "shared_prefix": {
            "duration_s": _round_seconds(shared["duration_s"]),
            "ratio": _round2(shared["ratio"]),
            "user_phase": (shared.get("user_window") or {}).get("phase"),
            "pro_phase": (shared.get("pro_window") or {}).get("phase"),
        },
        "divergence": {
            "start_s": _round_seconds(divergence_s),
            "user_phase": (shared.get("user_window") or {}).get("phase"),
            "pro_phase": (shared.get("pro_window") or {}).get("phase"),
        },
        "break_event": break_event,
        "survival_gap_s": _round_seconds(_round_duration_s(user_artifact) - _round_duration_s(pro_artifact)),
        "summary": _original_summary(shared, break_event),
    }


def _combined_summary(nav_result: dict, original_result: dict) -> str:
    if float(nav_result.get("score", 0.0)) >= float(original_result.get("score", 0.0)):
        return nav_result.get("summary") or "Combined round analysis prefers the nav match."
    return original_result.get("summary") or "Combined round analysis prefers the original match."


def analyze_shortlisted_rounds(
    *,
    query: dict | None,
    user_artifact: dict,
    candidates: list[dict],
    logic: str,
) -> dict:
    """Run deep scoring over a shortlist of preloaded candidate round artifacts."""
    matches: list[dict] = []

    for candidate in candidates:
        pro_artifact = candidate.get("artifact")
        if not isinstance(pro_artifact, dict):
            continue

        nav_result = match_nav_rounds(
            query=query,
            user_artifact=user_artifact,
            pro_artifact=pro_artifact,
        )
        original_result = match_original_rounds(
            query=query,
            user_artifact=user_artifact,
            pro_artifact=pro_artifact,
        )
        pro_offset = _pro_time_offset_s(query, candidate)
        nav_result = _attach_timeline_contract(
            nav_result,
            user_artifact=user_artifact,
            pro_artifact=pro_artifact,
            pro_time_offset_s=pro_offset,
        )
        original_result = _attach_timeline_contract(
            original_result,
            user_artifact=user_artifact,
            pro_artifact=pro_artifact,
            pro_time_offset_s=pro_offset,
        )
        combined_score = _round2(
            (0.55 * float(nav_result.get("score", 0.0)))
            + (0.45 * float(original_result.get("score", 0.0)))
        )

        if logic == _NAV_LOGIC:
            selected_score = float(nav_result.get("score", 0.0))
            summary = nav_result.get("summary")
            selected_break_event = nav_result.get("break_event")
            selected_shared_prefix = nav_result.get("shared_prefix")
            selected_divergence = nav_result.get("divergence")
            selected_timeline_sync = nav_result.get("timeline_sync")
        elif logic == _ORIGINAL_LOGIC:
            selected_score = float(original_result.get("score", 0.0))
            summary = original_result.get("summary")
            selected_break_event = original_result.get("break_event")
            selected_shared_prefix = original_result.get("shared_prefix")
            selected_divergence = original_result.get("divergence")
            selected_timeline_sync = original_result.get("timeline_sync")
        else:
            nav_wins = float(nav_result.get("score", 0.0)) >= float(original_result.get("score", 0.0))
            selected_score = combined_score
            summary = _combined_summary(nav_result, original_result)
            selected_break_event = nav_result.get("break_event") if nav_wins else original_result.get("break_event")
            selected_shared_prefix = nav_result.get("shared_prefix") if nav_wins else original_result.get("shared_prefix")
            selected_divergence = nav_result.get("divergence") if nav_wins else original_result.get("divergence")
            selected_timeline_sync = nav_result.get("timeline_sync") if nav_wins else original_result.get("timeline_sync")

        matches.append({
            **{key: value for key, value in candidate.items() if key != "artifact"},
            "deep_score": _round2(selected_score),
            "logic": logic,
            "logic_scores": {
                "nav": nav_result.get("score", 0.0),
                "original": original_result.get("score", 0.0),
                "both": combined_score,
            },
            "nav": nav_result,
            "original": original_result,
            "summary": summary,
            "break_event": selected_break_event,
            "shared_prefix": selected_shared_prefix,
            "divergence": selected_divergence,
            "timeline_sync": selected_timeline_sync,
            "pro_time_offset_s": pro_offset,
            "divergence_start_sec": (selected_divergence or {}).get("start_s"),
            "divergence_end_sec": (selected_divergence or {}).get("end_s"),
        })

    matches.sort(
        key=lambda row: (
            float(row.get("deep_score", 0.0)),
            float(row.get("score", 0.0)),
            -int(row.get("shortlist_rank", 9999)),
        ),
        reverse=True,
    )
    selected_match = matches[0] if matches else None
    return {
        "logic": logic,
        "matches": matches,
        "selected_match": selected_match,
    }
