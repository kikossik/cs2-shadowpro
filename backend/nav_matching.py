"""Nav-mesh matching logic ported from cs2-shadowpro-groundup.

Operates on pre-computed player nav sequences stored in round artifacts rather
than live DemoArtifacts objects, so it runs at scoring time without re-parsing.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Sequence

_NAV_CACHE: dict[str, Any] = {}
_NAV_AREA_CACHE: dict[str, list[dict]] = {}
_NAV_GRID_CACHE: dict[str, dict[tuple[int, int], list[int]]] = {}
_NAV_POINT_CACHE: dict[tuple, int | None] = {}

_CELL_SIZE = 512.0
_Z_TOLERANCE = 130.0


# ── Nav mesh loading ───────────────────────────────────────────────────────────

def _load_nav_mesh(map_name: str) -> Any | None:
    if map_name in _NAV_CACHE:
        return _NAV_CACHE[map_name]

    try:
        import awpy.data
        import awpy.nav
        nav_path = awpy.data.NAVS_DIR / f"{map_name}.json"
        if not nav_path.exists():
            _NAV_CACHE[map_name] = None
            return None
        nav_mesh = awpy.nav.Nav.from_json(nav_path)
        _NAV_CACHE[map_name] = nav_mesh
        return nav_mesh
    except Exception:
        _NAV_CACHE[map_name] = None
        return None


def _nav_area_records(map_name: str) -> list[dict]:
    if map_name in _NAV_AREA_CACHE:
        return _NAV_AREA_CACHE[map_name]

    nav_mesh = _load_nav_mesh(map_name)
    if nav_mesh is None:
        _NAV_AREA_CACHE[map_name] = []
        return []

    records: list[dict] = []
    grid: dict[tuple[int, int], list[int]] = {}
    for area in nav_mesh.areas.values():
        points_xy = [(float(c.x), float(c.y)) for c in area.corners]
        if not points_xy:
            continue
        xs = [p[0] for p in points_xy]
        ys = [p[1] for p in points_xy]
        z_mean = float(sum(float(c.z) for c in area.corners) / len(area.corners))
        centroid = area.centroid
        record = {
            "area_id": int(area.area_id),
            "points_xy": points_xy,
            "min_x": min(xs),
            "max_x": max(xs),
            "min_y": min(ys),
            "max_y": max(ys),
            "z_mean": z_mean,
            "centroid_x": float(centroid.x),
            "centroid_y": float(centroid.y),
        }
        idx = len(records)
        records.append(record)
        min_cx = int(math.floor(record["min_x"] / _CELL_SIZE))
        max_cx = int(math.floor(record["max_x"] / _CELL_SIZE))
        min_cy = int(math.floor(record["min_y"] / _CELL_SIZE))
        max_cy = int(math.floor(record["max_y"] / _CELL_SIZE))
        for cx in range(min_cx, max_cx + 1):
            for cy in range(min_cy, max_cy + 1):
                grid.setdefault((cx, cy), []).append(idx)

    _NAV_AREA_CACHE[map_name] = records
    _NAV_GRID_CACHE[map_name] = grid
    return records


def _point_in_polygon_xy(x: float, y: float, polygon: Sequence[tuple[float, float]]) -> bool:
    inside = False
    n = len(polygon)
    if n < 3:
        return False
    for i in range(n):
        x1, y1 = polygon[i]
        x2, y2 = polygon[(i + 1) % n]
        if (y1 > y) != (y2 > y):
            slope_x = (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-9) + x1
            if x < slope_x:
                inside = not inside
    return inside


def lookup_nav_area_id(map_name: str, x: float, y: float, z: float) -> int | None:
    """Map a world position to the nearest containing nav area ID."""
    cache_key = (map_name, int(round(x / 16.0)), int(round(y / 16.0)), int(round(z / 32.0)))
    if cache_key in _NAV_POINT_CACHE:
        return _NAV_POINT_CACHE[cache_key]

    records = _nav_area_records(map_name)
    grid = _NAV_GRID_CACHE.get(map_name, {})
    center_cell = (int(math.floor(x / _CELL_SIZE)), int(math.floor(y / _CELL_SIZE)))
    candidate_indexes: set[int] = set()
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            candidate_indexes.update(grid.get((center_cell[0] + dx, center_cell[1] + dy), []))
    if not candidate_indexes:
        candidate_indexes = set(range(len(records)))

    best_id: int | None = None
    best_dist = float("inf")
    fallback_id: int | None = None
    fallback_dist = float("inf")

    for idx in candidate_indexes:
        area = records[idx]
        if x < area["min_x"] - 8.0 or x > area["max_x"] + 8.0:
            continue
        if y < area["min_y"] - 8.0 or y > area["max_y"] + 8.0:
            continue
        z_gap = abs(z - area["z_mean"])
        cdist = math.dist((x, y), (area["centroid_x"], area["centroid_y"]))
        if z_gap <= _Z_TOLERANCE and cdist < fallback_dist:
            fallback_dist = cdist
            fallback_id = int(area["area_id"])
        if z_gap > _Z_TOLERANCE:
            continue
        if not _point_in_polygon_xy(x, y, area["points_xy"]):
            continue
        if cdist < best_dist:
            best_dist = cdist
            best_id = int(area["area_id"])

    resolved = best_id if best_id is not None else fallback_id
    _NAV_POINT_CACHE[cache_key] = resolved
    return resolved


# ── Area similarity ────────────────────────────────────────────────────────────

def nav_area_similarity(map_name: str, left_id: int | None, right_id: int | None) -> float:
    if left_id is None or right_id is None:
        return 0.0
    if int(left_id) == int(right_id):
        return 1.0
    nav_mesh = _load_nav_mesh(map_name)
    if nav_mesh is None:
        return 0.0
    left_area = nav_mesh.areas.get(int(left_id))
    right_area = nav_mesh.areas.get(int(right_id))
    if left_area is None or right_area is None:
        return 0.0
    if int(right_id) in left_area.connected_areas or int(left_id) in right_area.connected_areas:
        return 0.82
    gap = math.dist(
        (float(left_area.centroid.x), float(left_area.centroid.y)),
        (float(right_area.centroid.x), float(right_area.centroid.y)),
    )
    if gap <= 250.0:
        return 0.75
    if gap <= 500.0:
        return 0.55
    if gap <= 900.0:
        return 0.30
    return 0.0


# ── Route step collapse (also used by artifact builder) ───────────────────────

def collapse_route_steps(
    sample_times: list[float],
    area_ids: list[int | None],
    support_counts: list[int],
    enemy_counts: list[int],
    bomb_flags: list[bool],
) -> list[dict]:
    steps: list[dict] = []
    for idx, area_id in enumerate(area_ids):
        if area_id is None:
            continue
        t = float(sample_times[idx])
        sup = int(support_counts[idx])
        en = int(enemy_counts[idx])
        bom = bool(bomb_flags[idx])
        if steps and steps[-1]["area_id"] == int(area_id):
            steps[-1]["end_sec"] = t
            steps[-1]["_sup"].append(sup)
            steps[-1]["_en"].append(en)
            steps[-1]["_bom"].append(1.0 if bom else 0.0)
            continue
        steps.append({
            "area_id": int(area_id),
            "start_sec": t,
            "end_sec": t,
            "_sup": [sup],
            "_en": [en],
            "_bom": [1.0 if bom else 0.0],
        })
    for step in steps:
        sup = step.pop("_sup")
        en = step.pop("_en")
        bom = step.pop("_bom")
        step["support_count"] = sum(sup) / max(len(sup), 1)
        step["enemy_count"] = sum(en) / max(len(en), 1)
        step["bomb_planted"] = (sum(bom) / max(len(bom), 1)) >= 0.5
    return steps


# ── Shared route prefix ────────────────────────────────────────────────────────

def _count_route_steps_through(route_steps: list[dict], t_sec: float) -> int:
    return sum(1 for step in route_steps if float(step["start_sec"]) <= t_sec + 1e-6)


def shared_route_prefix(
    user_seq: dict,
    pro_seq: dict,
    map_name: str,
    *,
    min_step_similarity: float = 0.60,
    max_timing_gap_sec: float = 5.0,
    min_context_alignment: float = 0.38,
) -> dict:
    user_times = user_seq["sample_times"]
    pro_times = pro_seq["sample_times"]
    n = min(len(user_times), len(pro_times))
    _empty = {
        "shared_steps": 0.0, "prefix_similarity": 0.0, "timing_alignment": 0.0,
        "support_alignment": 0.0, "local_context_alignment": 0.0,
        "bomb_state_alignment": 0.0, "prefix_duration_sec": 0.0,
        "user_prefix_end_sec": 0.0, "pro_prefix_end_sec": 0.0,
        "matched_sample_count": 0.0, "break_index": 0.0,
    }
    if n == 0:
        return _empty

    user_ids = user_seq["area_ids"]
    pro_ids = pro_seq["area_ids"]
    user_sup = user_seq["support_counts"]
    pro_sup = pro_seq["support_counts"]
    user_en = user_seq["enemy_counts"]
    pro_en = pro_seq["enemy_counts"]
    user_bom = user_seq["bomb_planted_flags"]
    pro_bom = pro_seq["bomb_planted_flags"]

    matched: list[dict] = []
    break_index = n
    for idx in range(n):
        timing_gap = abs(float(user_times[idx]) - float(pro_times[idx]))
        step_sim = nav_area_similarity(map_name, user_ids[idx], pro_ids[idx])
        sup_align = 1.0 / (1.0 + abs(float(user_sup[idx]) - float(pro_sup[idx])))
        en_align = 1.0 / (1.0 + abs(float(user_en[idx]) - float(pro_en[idx])))
        bom_align = 1.0 if bool(user_bom[idx]) == bool(pro_bom[idx]) else 0.0
        ctx_align = 0.40 * sup_align + 0.35 * en_align + 0.25 * bom_align
        if step_sim < min_step_similarity or timing_gap > max_timing_gap_sec or ctx_align < min_context_alignment:
            break_index = idx
            break
        matched.append({
            "step_similarity": step_sim,
            "timing_gap": timing_gap,
            "sup_alignment": sup_align,
            "en_alignment": en_align,
            "bom_alignment": bom_align,
            "ctx_alignment": ctx_align,
        })

    if not matched:
        return {**_empty, "break_index": float(break_index)}

    last_idx = len(matched) - 1
    user_end_sec = float(user_times[last_idx])
    pro_end_sec = float(pro_times[last_idx])
    n_matched = len(matched)
    return {
        "shared_steps": float(min(
            _count_route_steps_through(user_seq["route_steps"], user_end_sec),
            _count_route_steps_through(pro_seq["route_steps"], pro_end_sec),
        )),
        "prefix_similarity": sum(r["step_similarity"] for r in matched) / n_matched,
        "timing_alignment": 1.0 / (1.0 + sum(r["timing_gap"] for r in matched) / n_matched / 4.0),
        "support_alignment": sum(r["sup_alignment"] for r in matched) / n_matched,
        "local_context_alignment": sum(r["ctx_alignment"] for r in matched) / n_matched,
        "bomb_state_alignment": sum(r["bom_alignment"] for r in matched) / n_matched,
        "prefix_duration_sec": float(min(user_end_sec, pro_end_sec)),
        "user_prefix_end_sec": user_end_sec,
        "pro_prefix_end_sec": pro_end_sec,
        "matched_sample_count": float(n_matched),
        "break_index": float(break_index),
    }


# ── Post-break local conversion ────────────────────────────────────────────────

def post_break_local_conversion(user_seq: dict, pro_seq: dict, break_index: int, lookahead: int = 3) -> float:
    n = min(len(user_seq["sample_times"]), len(pro_seq["sample_times"]))
    if n == 0:
        return 0.0
    start = max(0, min(int(break_index), n - 1))
    stop = min(n, start + lookahead)

    def seg_mean(vals: list, s: int, e: int) -> float:
        seg = [float(v) for v in vals[s:e]]
        return sum(seg) / len(seg) if seg else 0.0

    user_score = (
        seg_mean(user_seq["support_counts"], start, stop)
        - seg_mean(user_seq["enemy_counts"], start, stop)
        - 0.75 * seg_mean(user_seq.get("nearby_teammate_deaths", []), start, stop)
    )
    pro_score = (
        seg_mean(pro_seq["support_counts"], start, stop)
        - seg_mean(pro_seq["enemy_counts"], start, stop)
        - 0.75 * seg_mean(pro_seq.get("nearby_teammate_deaths", []), start, stop)
    )
    return max(0.0, min(1.0, (pro_score - user_score + 2.0) / 4.0))


# ── Break event classification ─────────────────────────────────────────────────

def classify_route_break(
    user_seq: dict,
    pro_seq: dict,
    map_name: str,
    prefix: dict,
) -> dict:
    labels = {
        "route_deviation": "route deviation",
        "user_death": "user death",
        "local_teammate_collapse": "local teammate collapse",
        "bomb_state_divergence": "bomb-state divergence",
        "round_outcome_collapse": "round outcome collapse",
        "local_context_divergence": "local context divergence",
        "shared_prefix_complete": "shared path complete",
    }
    n = min(len(user_seq["sample_times"]), len(pro_seq["sample_times"]))
    _empty_break = {"break_event_type": "shared_prefix_complete", "break_event_label": labels["shared_prefix_complete"], "break_time_sec": 0.0, "survival_gap_sec": 0.0}
    if n == 0:
        return _empty_break

    user_survival = float(user_seq["death_sec"]) if user_seq.get("death_sec") is not None else float(user_seq.get("round_end_sec", user_seq["sample_times"][-1]))
    pro_survival = float(pro_seq["death_sec"]) if pro_seq.get("death_sec") is not None else float(pro_seq.get("round_end_sec", pro_seq["sample_times"][-1]))
    survival_gap = max(0.0, pro_survival - user_survival)
    break_index = min(max(int(prefix.get("break_index", n)), 0), n)

    if break_index >= n:
        event_type = "shared_prefix_complete"
        break_time = float(prefix.get("prefix_duration_sec", min(user_survival, pro_survival)))
        if not bool(user_seq.get("team_won", False)) and bool(pro_seq.get("team_won", False)):
            event_type = "round_outcome_collapse"
        elif survival_gap > 0.5 and user_seq.get("death_sec") is not None:
            event_type = "user_death"
            break_time = float(user_seq["death_sec"])
        return {"break_event_type": event_type, "break_event_label": labels[event_type], "break_time_sec": break_time, "survival_gap_sec": survival_gap}

    break_time = float(user_seq["sample_times"][break_index])
    user_alive = bool(user_seq.get("alive_flags", [user_seq["area_ids"][break_index] is not None] * n)[break_index])
    pro_alive = bool(pro_seq.get("alive_flags", [pro_seq["area_ids"][break_index] is not None] * n)[break_index])
    bomb_mismatch = bool(user_seq["bomb_planted_flags"][break_index]) != bool(pro_seq["bomb_planted_flags"][break_index])
    user_local_collapse = (
        int(user_seq.get("nearby_teammate_deaths", [0] * n)[break_index])
        > int(pro_seq.get("nearby_teammate_deaths", [0] * n)[break_index])
        and float(user_seq.get("local_balance", [0] * n)[break_index])
        < float(pro_seq.get("local_balance", [0] * n)[break_index])
    )
    route_sim = nav_area_similarity(map_name, user_seq["area_ids"][break_index], pro_seq["area_ids"][break_index])

    if not user_alive and pro_alive:
        event_type = "user_death"
    elif user_local_collapse:
        event_type = "local_teammate_collapse"
    elif bomb_mismatch:
        event_type = "bomb_state_divergence"
    elif route_sim < 0.60:
        event_type = "route_deviation"
    elif not bool(user_seq.get("team_won", False)) and bool(pro_seq.get("team_won", False)):
        event_type = "round_outcome_collapse"
    else:
        event_type = "local_context_divergence"

    return {"break_event_type": event_type, "break_event_label": labels[event_type], "break_time_sec": break_time, "survival_gap_sec": survival_gap}


# ── Score one user/pro player pair ─────────────────────────────────────────────

def score_player_pair(user_seq: dict, pro_seq: dict, map_name: str, *, min_shared_steps: int = 2) -> dict | None:
    """Score a user player against one pro player. Returns None if not a viable match."""
    if not user_seq.get("route_steps") or not pro_seq.get("route_steps"):
        return None

    user_start_area = user_seq.get("start_area_id") or user_seq["route_steps"][0].get("area_id")
    pro_start_area = pro_seq.get("start_area_id") or pro_seq["route_steps"][0].get("area_id")
    start_similarity = nav_area_similarity(map_name, user_start_area, pro_start_area)
    if start_similarity < 0.55:
        return None

    start_support_alignment = 1.0 / (
        1.0 + abs(float(user_seq["support_counts"][0]) - float(pro_seq["support_counts"][0]))
    )
    start_enemy_alignment = 1.0 / (
        1.0 + abs(float(user_seq["enemy_counts"][0]) - float(pro_seq["enemy_counts"][0]))
    )
    start_context_alignment = 0.55 * start_support_alignment + 0.45 * start_enemy_alignment
    if start_context_alignment < 0.34:
        return None

    # Gate on bomb state agreement at the start
    if user_seq["bomb_planted_flags"] and pro_seq["bomb_planted_flags"]:
        if bool(user_seq["bomb_planted_flags"][0]) != bool(pro_seq["bomb_planted_flags"][0]):
            return None

    prefix = shared_route_prefix(user_seq, pro_seq, map_name)
    if float(prefix["shared_steps"]) < min_shared_steps:
        return None

    n_user_steps = _count_route_steps_through(user_seq["route_steps"], float(user_seq["sample_times"][-1]) if user_seq["sample_times"] else 0.0)
    prefix_duration_norm = min(1.0, float(prefix["prefix_duration_sec"]) / max(float(user_seq["sample_times"][-1]) if user_seq["sample_times"] else 1.0, 1.0))
    shared_steps_norm = min(1.0, float(prefix["shared_steps"]) / max(n_user_steps, 1))

    user_survival = float(user_seq["death_sec"]) if user_seq.get("death_sec") is not None else float(user_seq["sample_times"][-1] if user_seq["sample_times"] else 0.0)
    pro_survival = float(pro_seq["death_sec"]) if pro_seq.get("death_sec") is not None else float(pro_seq["sample_times"][-1] if pro_seq["sample_times"] else 0.0)
    survival_gap = max(0.0, pro_survival - user_survival)
    survived_longer = min(1.0, survival_gap / 12.0)

    # Coach value: did pro survive longer and win?
    pro_won = bool(pro_seq.get("team_won", False))
    user_won = bool(user_seq.get("team_won", False))
    round_outcome_advantage = 1.0 if (pro_won and not user_won) else (0.5 if pro_won else 0.0)
    plc = post_break_local_conversion(user_seq, pro_seq, int(prefix["break_index"]))

    prefix_score = (
        0.35 * prefix_duration_norm
        + 0.25 * shared_steps_norm
        + 0.15 * float(prefix["prefix_similarity"])
        + 0.10 * float(prefix["timing_alignment"])
        + 0.15 * float(prefix["local_context_alignment"])
    )
    coach_value = 0.40 * survived_longer + 0.35 * round_outcome_advantage + 0.25 * plc
    round_score = 0.75 * prefix_score + 0.25 * coach_value

    break_event = classify_route_break(user_seq, pro_seq, map_name, prefix)

    return {
        "round_score": round(round_score, 4),
        "prefix_score": round(prefix_score, 4),
        "coach_value": round(coach_value, 4),
        "prefix_duration_sec": round(float(prefix["prefix_duration_sec"]), 2),
        "shared_steps": int(prefix["shared_steps"]),
        "prefix_similarity": round(float(prefix["prefix_similarity"]), 4),
        "timing_alignment": round(float(prefix["timing_alignment"]), 4),
        "local_context_alignment": round(float(prefix["local_context_alignment"]), 4),
        "start_similarity": round(start_similarity, 4),
        "start_context_alignment": round(start_context_alignment, 4),
        "survived_longer": round(survived_longer, 4),
        "survival_gap_sec": round(survival_gap, 2),
        "break_event_type": break_event["break_event_type"],
        "break_event_label": break_event["break_event_label"],
        "break_time_sec": round(float(break_event["break_time_sec"]), 2),
        "prefix": prefix,
    }
